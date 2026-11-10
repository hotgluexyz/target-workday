"""Exercise journal exports through Singer input and emitted Hotglue state."""

import copy
import hashlib
import io
import json
from html import escape
from xml.etree import ElementTree as ET

import pytest
import requests
import responses

from target_workday.client import NS, SOAP, WD
from target_workday.sinks import JOURNAL_SCHEMA
from target_workday.target import TargetWorkday

CONFIG = {
    "host": "https://example.workday.com",
    "tenant": "demo",
    "username": "isu@demo",
    "password": "secret<&",
}
URL = CONFIG["host"] + "/ccx/service/demo/Financial_Management/v48.0"
RECORD = {
    "externalId": "corpay-1001",
    "company_id": "Company_US",
    "currency": "USD",
    "ledger_type_id": "ACTUALS",
    "journal_source_id": "CORPAY",
    "accounting_date": "2026-11-10",
    "memo": "Card purchase <travel> & meals",
    "lines": [
        {
            "account_set_id": "Corporate",
            "account_number": "6100",
            "debit": "0.10",
            "cost_center_id": "CC001",
            "spend_category_id": "Travel",
            "supplier_id": "Vendor1",
        },
        {"account_set_id": "Corporate", "account_number": "6200", "debit": "0.20"},
        {"account_set_id": "Corporate", "account_number": "2550", "credit": "0.30"},
    ],
}


def journal_id(record=RECORD):
    """Independent construction of the documented remote identity."""
    scope = json.dumps(["demo", record["company_id"], record["externalId"]], separators=(",", ":"))
    return "HG_" + hashlib.sha256(scope.encode()).hexdigest()


def soap(content):
    return f'<s:Envelope xmlns:s="{SOAP}" xmlns:w="{WD}"><s:Body>{content}</s:Body></s:Envelope>'


def not_found(remote_id=None):
    message = escape(
        f"Invalid ID value.  '{remote_id or journal_id()}' is not a valid ID value for type = 'Accounting_Journal_ID'"
    )
    return soap(f"""<s:Fault><faultstring>{message}</faultstring><detail><w:Validation_Fault>
    <w:Validation_Error><w:Message>{message}</w:Message>
    <w:Xpath>/ns2:Get_Journals_Request[1]/ns2:Request_References[1]/ns2:JournalEntryReference[1]</w:Xpath>
    </w:Validation_Error></w:Validation_Fault></detail></s:Fault>""")


def found(record=RECORD, wid="journal-wid", company=None):
    return soap(f"""<w:Get_Journals_Response><w:Response_Data><w:Journal_Entry>
    <w:Journal_Entry_Reference><w:ID w:type="Accounting_Journal_ID">{journal_id(record)}</w:ID>
    <w:ID w:type="WID">{wid}</w:ID></w:Journal_Entry_Reference><w:Journal_Entry_Data>
    <w:Company_Reference><w:ID w:type="Company_Reference_ID">{company or record["company_id"]}</w:ID></w:Company_Reference>
    <w:External_Reference_ID>{record["externalId"]}</w:External_Reference_ID>
    </w:Journal_Entry_Data></w:Journal_Entry></w:Response_Data></w:Get_Journals_Response>""")


def created():
    return soap(f"""<w:Submit_Accounting_Journal_Response><w:Accounting_Journal_Reference>
    <w:ID w:type="Accounting_Journal_ID">{journal_id()}</w:ID><w:ID w:type="WID">journal-wid</w:ID>
    </w:Accounting_Journal_Reference></w:Submit_Accounting_Journal_Response>""")


@pytest.fixture
def run_target(tmp_path, monkeypatch, capsys):
    """Isolate SDK state paths while using its real Singer processing path."""
    import hotglue_singer_sdk.target_sdk.target as sdk_target
    from hotglue_singer_sdk.target_sdk.client import HotglueBaseSink

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sdk_target, "SNAPSHOT_DIR", str(tmp_path))
    monkeypatch.setattr(sdk_target, "flow_id", "demo")
    monkeypatch.setattr(HotglueBaseSink, "processed_hashes", [])
    monkeypatch.setattr(
        TargetWorkday, "incremental_target_state_path", str(tmp_path / "prior.json")
    )

    def run(records=None, prior=None, snapshot=None, config=None):
        path = tmp_path / "prior.json"
        if prior is None:
            path.unlink(missing_ok=True)
        else:
            path.write_text(json.dumps(prior))
        if snapshot:
            (tmp_path / "accounting_journals_demo.snapshot.csv").write_text(snapshot)
        target = TargetWorkday(config={**CONFIG, **(config or {})})
        messages = [
            {
                "type": "SCHEMA",
                "stream": "accounting_journals",
                "schema": JOURNAL_SCHEMA,
                "key_properties": ["externalId"],
            }
        ]
        messages += [
            {"type": "RECORD", "stream": "accounting_journals", "record": record}
            for record in (records if records is not None else [copy.deepcopy(RECORD)])
        ]
        capsys.readouterr()
        try:
            target.listen(
                io.StringIO("\n".join(json.dumps(message) for message in messages) + "\n")
            )
        finally:
            target._shutdown_requested.set()
        states = [
            json.loads(line)
            for line in capsys.readouterr().out.splitlines()
            if line.startswith("{")
        ]
        assert states, "Singer target must emit its per-record state"
        return states[-1]

    return run


@responses.activate
def test_create_balanced_journal_and_preserve_source_remote_ids(run_target):
    responses.post(URL, status=500, body=not_found())
    responses.post(URL, body=created())
    state = run_target()
    (bookmark,) = state["bookmarks"]["accounting_journals"]
    assert bookmark["id"] == "journal-wid"
    assert bookmark["externalId"] == "corpay-1001"
    assert bookmark["success"] is True
    assert state["summary"]["accounting_journals"] == {
        "success": 1,
        "fail": 0,
        "existing": 0,
        "updated": 0,
    }
    request = ET.fromstring(responses.calls[1].request.body)
    submit = request.find("soap:Body/wd:Submit_Accounting_Journal_Request", NS)
    assert submit.get(f"{{{WD}}}Add_Only") == "true"
    assert (
        submit.findtext("wd:Business_Process_Parameters/wd:Auto_Complete", namespaces=NS) == "false"
    )
    data = submit.find("wd:Accounting_Journal_Data", NS)
    assert data.findtext("wd:Submit", namespaces=NS) == "false"
    assert data.findtext("wd:Accounting_Journal_ID", namespaces=NS) == journal_id()
    assert data.findtext("wd:External_Reference_ID", namespaces=NS) == RECORD["externalId"]
    assert data.findtext("wd:Journal_Entry_Memo", namespaces=NS) == RECORD["memo"]
    lines = data.findall("wd:Journal_Entry_Line_Replacement_Data", NS)
    assert [line.findtext("wd:Debit_Amount", namespaces=NS) for line in lines] == [
        "0.10",
        "0.20",
        None,
    ]
    assert lines[2].findtext("wd:Credit_Amount", namespaces=NS) == "0.30"
    ledger = lines[0].find("wd:Ledger_Account_Reference/wd:ID", NS)
    assert ledger.text == "6100"
    assert ledger.get(f"{{{WD}}}parent_id") == "Corporate"
    assert ledger.get(f"{{{WD}}}parent_type") == "Account_Set_ID"
    assert [
        (node.get(f"{{{WD}}}type"), node.text)
        for node in lines[0].findall("wd:Worktags_Reference/wd:ID", NS)
    ] == [
        ("Cost_Center_Reference_ID", "CC001"),
        ("Spend_Category_ID", "Travel"),
        ("Supplier_Reference_ID", "Vendor1"),
    ]
    assert CONFIG["password"] not in json.dumps(state)


@responses.activate
def test_existing_untracked_or_changed_journal_is_skipped(run_target):
    changed = copy.deepcopy(RECORD)
    changed["memo"] = "Changed but must not overwrite the existing financial record"
    responses.post(URL, body=found())
    state = run_target([changed])
    assert state["summary"]["accounting_journals"]["existing"] == 1
    assert state["bookmarks"]["accounting_journals"][0]["id"] == "journal-wid"
    assert len(responses.calls) == 1


@responses.activate
def test_duplicate_same_run_and_prior_state_skip(run_target):
    responses.post(URL, status=500, body=not_found())
    responses.post(URL, body=created())
    state = run_target([copy.deepcopy(RECORD), copy.deepcopy(RECORD)])
    assert state["summary"]["accounting_journals"] == {
        "success": 1,
        "existing": 1,
        "fail": 0,
        "updated": 0,
    }
    assert len(responses.calls) == 2
    # A new process/job reuses successful SDK hashes, not a target-specific cache.
    state = run_target(prior=state)
    assert state["summary"]["accounting_journals"]["fail"] == 0
    assert len(responses.calls) == 2


@pytest.mark.parametrize("snapshot_id", ["journal-wid", "wrong-wid"])
@responses.activate
def test_snapshot_id_requires_matching_company_and_source(run_target, snapshot_id):
    responses.post(URL, body=found())
    state = run_target(snapshot=f"InputId,RemoteId\ncorpay-1001,{snapshot_id}\n")
    summary = state["summary"]["accounting_journals"]
    assert summary["existing"] == (1 if snapshot_id == "journal-wid" else 0)
    assert summary["fail"] == (0 if snapshot_id == "journal-wid" else 1)
    assert len(responses.calls) == 1


@responses.activate
def test_wrong_company_fails_without_submit(run_target):
    responses.post(URL, body=found(company="OtherCompany"))
    state = run_target()
    assert state["summary"]["accounting_journals"]["fail"] == 1
    assert "company" in state["bookmarks"]["accounting_journals"][0]["error"]
    assert len(responses.calls) == 1


@pytest.mark.parametrize("recovered", [True, False])
@responses.activate
def test_lost_write_response_reconciles_without_reposting(run_target, recovered):
    responses.post(URL, status=500, body=not_found())
    responses.post(URL, body=requests.exceptions.ReadTimeout("connection lost"))
    responses.post(
        URL, body=found() if recovered else not_found(), status=200 if recovered else 500
    )
    state = run_target()
    summary = state["summary"]["accounting_journals"]
    assert summary["existing"] == int(recovered)
    assert summary["fail"] == int(not recovered)
    if recovered:
        assert state["bookmarks"]["accounting_journals"][0]["id"] == "journal-wid"
    operations = [
        ET.fromstring(call.request.body).find("soap:Body", NS)[0].tag for call in responses.calls
    ]
    assert operations.count(f"{{{WD}}}Submit_Accounting_Journal_Request") == 1
    assert len(operations) == 3


@responses.activate
def test_permission_fault_does_not_become_not_found(run_target):
    responses.post(
        URL,
        status=500,
        body=soap(
            """<s:Fault><faultstring>The task submitted is not authorized.</faultstring></s:Fault>"""
        ),
    )
    state = run_target()
    assert state["summary"]["accounting_journals"]["fail"] == 1
    assert "not authorized" in state["bookmarks"]["accounting_journals"][0]["error"]
    assert len(responses.calls) == 1


@responses.activate
def test_rate_limited_submission_retries_with_stable_id(run_target):
    responses.post(URL, status=500, body=not_found())
    responses.post(URL, status=429, headers={"Retry-After": "0"})
    responses.post(URL, body=created())
    state = run_target(config={"submit_for_approval": True})
    assert state["summary"]["accounting_journals"]["success"] == 1
    assert responses.calls[1].request.body == responses.calls[2].request.body
    request = ET.fromstring(responses.calls[2].request.body)
    assert request.findtext(".//wd:Submit", namespaces=NS) == "true"
    assert request.findtext(".//wd:Auto_Complete", namespaces=NS) == "false"


@responses.activate
def test_503_retry_after_reconciles_instead_of_reposting(run_target):
    responses.post(URL, status=500, body=not_found())
    responses.post(URL, status=503, headers={"Retry-After": "0"})
    responses.post(URL, body=found())
    state = run_target()
    assert state["summary"]["accounting_journals"]["existing"] == 1
    operations = [
        ET.fromstring(call.request.body).find("soap:Body", NS)[0].tag for call in responses.calls
    ]
    assert operations == [
        f"{{{WD}}}{name}_Request"
        for name in ("Get_Journals", "Submit_Accounting_Journal", "Get_Journals")
    ]


@responses.activate
def test_previous_failed_record_remains_retryable(run_target):
    responses.post(
        URL, status=500, body=soap("<s:Fault><faultstring>Not authorized</faultstring></s:Fault>")
    )
    failed = run_target()
    assert failed["summary"]["accounting_journals"]["fail"] == 1
    responses.post(URL, status=500, body=not_found())
    responses.post(URL, body=created())
    state = run_target(prior=failed)
    assert state["summary"]["accounting_journals"]["success"] == 1
    assert state["summary"]["accounting_journals"]["fail"] == 0
    assert state["bookmarks"]["accounting_journals"][0]["id"] == "journal-wid"


@responses.activate
def test_large_totals_cannot_hide_a_micro_unit_imbalance(run_target):
    record = copy.deepcopy(RECORD)
    line = {"account_set_id": "Corporate", "account_number": "6100"}
    record["lines"] = [
        {**line, side: "99999999999999999999.999999"}
        for side in ("debit", "credit")
        for _ in range(101)
    ] + [{**line, "debit": "0.000001"}, {**line, "credit": "0.000002"}]
    state = run_target([record])
    assert "balance exactly" in state["bookmarks"]["accounting_journals"][0]["error"]
    assert not responses.calls


@pytest.mark.parametrize(
    "change",
    [
        {"credit": "0.31"},
        {"credit": "0.0000001"},
        {"credit": "NaN"},
        {"credit": "-0.30"},
        {"credit": "0.30", "debit": "0.30"},
        {"credit": "0"},
    ],
)
@responses.activate
def test_invalid_amounts_fail_before_network(run_target, change):
    record = copy.deepcopy(RECORD)
    record["lines"][-1].update(change)
    state = run_target([record])
    assert state["summary"]["accounting_journals"]["fail"] == 1
    assert state["bookmarks"]["accounting_journals"][0]["externalId"] == RECORD["externalId"]
    assert not responses.calls
