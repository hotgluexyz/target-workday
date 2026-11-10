"""SOAP transport, with no automatic replay of ambiguous financial writes."""

from urllib.parse import quote, urlsplit
from xml.etree import ElementTree as ET

import requests
from defusedxml.ElementTree import fromstring
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

WD = "urn:com.workday/bsvc"
SOAP = "http://schemas.xmlsoap.org/soap/envelope/"
WSSE = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd"
NS = {"wd": WD, "soap": SOAP}


def element(parent, name, value=None, **attributes):
    """Append a Workday XML element, escaping data through ElementTree."""
    child = ET.SubElement(
        parent,
        f"{{{WD}}}{name}",
        {f"{{{WD}}}{key}": str(value) for key, value in attributes.items()},
    )
    if value is not None:
        child.text = str(value)
    return child


def reference(parent, name, value, id_type, **attributes):
    """Write a typed, optionally account-set-scoped Workday reference."""
    return element(element(parent, name), "ID", value, type=id_type, **attributes)


def ids(parent, path):
    """Read reference IDs by type rather than response order."""
    return {node.get(f"{{{WD}}}type"): node.text for node in parent.findall(path + "/wd:ID", NS)}


class WorkdayError(RuntimeError):
    """A sanitized SOAP failure retaining structured validation details."""

    def __init__(self, message, validation_errors=()):
        super().__init__(message)
        self.validation_errors = validation_errors


class WorkdayRetry(Retry):
    """Do not let Retry-After implicitly enable retries of 503 writes."""

    RETRY_AFTER_STATUS_CODES = frozenset({429})


class WorkdayClient:
    """WS-Security UsernameToken client for Financial_Management."""

    def __init__(self, config):
        self.config = config
        host = config["host"].rstrip("/")
        parsed = urlsplit(host)
        if (
            parsed.scheme != "https"
            or not parsed.netloc
            or parsed.path
            or parsed.query
            or parsed.fragment
            or parsed.username
        ):
            raise ValueError("host must be an HTTPS origin without a path or credentials")
        self.url = (
            f"{host}/ccx/service/{quote(config['tenant'], safe='')}/"
            f"Financial_Management/{quote(config['api_version'], safe='')}"
        )

    def envelope(self, request):
        envelope = ET.Element(f"{{{SOAP}}}Envelope")
        security = ET.SubElement(
            ET.SubElement(envelope, f"{{{SOAP}}}Header"),
            f"{{{WSSE}}}Security",
            {f"{{{SOAP}}}mustUnderstand": "1"},
        )
        token = ET.SubElement(security, f"{{{WSSE}}}UsernameToken")
        ET.SubElement(token, f"{{{WSSE}}}Username").text = self.config["username"]
        ET.SubElement(
            token,
            f"{{{WSSE}}}Password",
            {
                "Type": "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText",
            },
        ).text = self.config["password"]
        ET.SubElement(envelope, f"{{{SOAP}}}Body").append(request)
        return ET.tostring(envelope, encoding="utf-8", xml_declaration=True)

    def request(self, operation, write=False):
        return ET.Element(
            f"{{{WD}}}{operation}_Request",
            {
                f"{{{WD}}}version": self.config["api_version"],
                **({f"{{{WD}}}Add_Only": "true"} if write else {}),
            },
        )

    def call(self, operation, request, write=False):
        # A 429 is explicitly rejected. Never replay a write on a timeout or 5xx:
        # the server may already have committed it. The sink reconciles by ID.
        retry = WorkdayRetry(
            total=4,
            connect=0 if write else None,
            read=0 if write else None,
            other=0,
            backoff_factor=1,
            backoff_jitter=1,
            status_forcelist=[429] if write else [429, 502, 503, 504],
            allowed_methods=["POST"],
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        with requests.Session() as session:
            session.mount("https://", HTTPAdapter(max_retries=retry))
            try:
                result = session.post(
                    self.url,
                    data=self.envelope(request),
                    headers={"Content-Type": "text/xml;charset=UTF-8"},
                    timeout=self.config["request_timeout"],
                    allow_redirects=False,
                )
            except requests.RequestException:
                raise WorkdayError(
                    f"{operation}: transport failure; write outcome may be unknown"
                ) from None
        try:
            root = fromstring(result.content)
        except ET.ParseError:
            raise WorkdayError(
                f"{operation}: non-XML response (HTTP {result.status_code})"
            ) from None
        fault = root.find("soap:Body/soap:Fault", NS)
        if fault is not None:
            message = fault.findtext("faultstring") or "SOAP fault"
            # Do not log SOAP bodies, which can contain credentials and financial data.
            for secret in (self.config["password"], self.config["username"]):
                message = message.replace(secret, "[REDACTED]")
            errors = tuple(
                (
                    node.findtext("wd:Message", namespaces=NS),
                    node.findtext("wd:Xpath", namespaces=NS),
                )
                for node in fault.findall(".//wd:Validation_Error", NS)
            )
            raise WorkdayError(f"{operation}: {message}", errors)
        if not 200 <= result.status_code < 300:
            raise WorkdayError(f"{operation}: HTTP {result.status_code}")
        response = root.find(f"soap:Body/wd:{operation}_Response", NS)
        if response is None:
            raise WorkdayError(f"{operation}: missing SOAP response element")
        return response

    def find_journal(self, journal_id):
        request = self.request("Get_Journals")
        reference(
            element(request, "Request_References"),
            "JournalEntryReference",
            journal_id,
            "Accounting_Journal_ID",
        )
        try:
            response = self.call("Get_Journals", request)
        except WorkdayError as error:
            # Verified against the sandbox. Only this exact, reference-specific
            # validation fault means absent; permissions/other faults must fail.
            expected = f"Invalid ID value.  '{journal_id}' is not a valid ID value for type = 'Accounting_Journal_ID'"
            if error.validation_errors and all(
                message == expected
                and path
                and "Get_Journals_Request[1]" in path
                and path.endswith("JournalEntryReference[1]")
                for message, path in error.validation_errors
            ):
                return None
            raise
        journals = response.findall("wd:Response_Data/wd:Journal_Entry", NS)
        if len(journals) != 1:
            raise WorkdayError("Get_Journals: exact reference lookup did not return one journal")
        journal = journals[0]
        ref = ids(journal, "wd:Journal_Entry_Reference")
        data = journal.find("wd:Journal_Entry_Data", NS)
        if ref.get("Accounting_Journal_ID") != journal_id or not ref.get("WID") or data is None:
            raise WorkdayError("Get_Journals: incomplete or mismatched journal identity")
        return {
            "id": ref["WID"],
            "company_id": ids(data, "wd:Company_Reference").get("Company_Reference_ID"),
            "externalId": data.findtext("wd:External_Reference_ID", namespaces=NS),
        }
