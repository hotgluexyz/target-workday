"""Create-only balanced journals, with safe remote duplicate reconciliation."""

import hashlib
import json
from datetime import date
from decimal import Decimal

import jsonschema
from hotglue_singer_sdk.target_sdk.client import HotglueSink

from target_workday.client import WorkdayClient, WorkdayError, element, ids, reference

MONEY = {"type": ["string", "number"]}
STRING = {"type": "string", "minLength": 1}
LINE_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["account_set_id", "account_number"],
    "properties": {
        "account_set_id": STRING,
        "account_number": STRING,
        "debit": MONEY,
        "credit": MONEY,
        "memo": {"type": "string"},
        "cost_center_id": STRING,
        "spend_category_id": STRING,
        "supplier_id": STRING,
    },
}
JOURNAL_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "externalId",
        "company_id",
        "currency",
        "ledger_type_id",
        "journal_source_id",
        "accounting_date",
        "lines",
    ],
    "properties": {
        **{
            key: STRING
            for key in (
                "externalId",
                "company_id",
                "currency",
                "ledger_type_id",
                "journal_source_id",
                "accounting_date",
                "id",
            )
        },
        "memo": {"type": "string"},
        "lines": {"type": "array", "minItems": 2, "items": LINE_SCHEMA},
    },
}


class AccountingJournalsSink(HotglueSink):
    """One journal per source transaction; existing journals are never edited."""

    name = "accounting_journals"
    endpoint = ""
    unified_schema = None
    allows_externalid = ["accounting_journals"]

    @property
    def base_url(self):
        return self.config["host"]

    def preprocess_record(self, record, context):
        jsonschema.validate(record, JOURNAL_SCHEMA)
        if date.fromisoformat(record["accounting_date"]).isoformat() != record["accounting_date"]:
            raise ValueError("accounting_date must use YYYY-MM-DD")
        debit_total = 0
        credit_total = 0
        for line in record["lines"]:
            amounts = []
            for key in ("debit", "credit"):
                value = Decimal(str(line.get(key, 0)))
                if (
                    not value.is_finite()
                    or value < 0
                    or value.as_tuple().exponent < -6
                    or value >= Decimal("1e20")
                ):
                    raise ValueError(
                        "Amounts must be nonnegative finite decimals with at most 6 decimal places and 20 integer digits"
                    )
                amounts.append(value)
            debit, credit = amounts
            if (debit > 0) == (credit > 0):
                raise ValueError("Each line must have exactly one positive debit or credit")
            # Integer micro-units keep large totals exact beyond Decimal's context precision.
            debit_total += int(debit * 1_000_000)
            credit_total += int(credit * 1_000_000)
        if debit_total != credit_total:
            raise ValueError("Journal debits and credits must balance exactly")
        return record

    def journal_id(self, record):
        scope = [self.config["tenant"], record["company_id"], record["externalId"]]
        return "HG_" + hashlib.sha256(json.dumps(scope, separators=(",", ":")).encode()).hexdigest()

    def journal_request(self, client, record, journal_id):
        request = client.request("Submit_Accounting_Journal", write=True)
        element(element(request, "Business_Process_Parameters"), "Auto_Complete", "false")
        data = element(request, "Accounting_Journal_Data")
        element(data, "Accounting_Journal_ID", journal_id)
        element(data, "Submit", str(self.config["submit_for_approval"]).lower())
        reference(data, "Company_Reference", record["company_id"], "Company_Reference_ID")
        reference(data, "Currency_Reference", record["currency"], "Currency_ID")
        reference(data, "Ledger_Type_Reference", record["ledger_type_id"], "Ledger_Type_ID")
        element(data, "Accounting_Date", record["accounting_date"])
        reference(
            data, "Journal_Source_Reference", record["journal_source_id"], "Journal_Source_ID"
        )
        if record.get("memo") is not None:
            element(data, "Journal_Entry_Memo", record["memo"])
        element(data, "External_Reference_ID", record["externalId"])
        for index, line in enumerate(record["lines"], 1):
            entry = element(data, "Journal_Entry_Line_Replacement_Data")
            element(entry, "Line_Order", index)
            reference(
                entry,
                "Ledger_Account_Reference",
                line["account_number"],
                "Ledger_Account_ID",
                parent_id=line["account_set_id"],
                parent_type="Account_Set_ID",
            )
            for key in ("debit", "credit"):
                if Decimal(str(line.get(key, 0))) > 0:
                    element(entry, f"{key.title()}_Amount", format(Decimal(str(line[key])), "f"))
            if line.get("memo") is not None:
                element(entry, "Memo", line["memo"])
            for key, id_type in (
                ("cost_center_id", "Cost_Center_Reference_ID"),
                ("spend_category_id", "Spend_Category_ID"),
                ("supplier_id", "Supplier_Reference_ID"),
            ):
                if key in line:
                    reference(entry, "Worktags_Reference", line[key], id_type)
        return request

    def existing_result(self, existing, record):
        if (
            existing["company_id"] != record["company_id"]
            or existing["externalId"] != record["externalId"]
        ):
            raise WorkdayError(
                "Existing journal company or external reference does not match; refusing to write"
            )
        if record.get("id") and record["id"] != existing["id"]:
            raise WorkdayError(
                "Snapshot/explicit ID does not match the source-scoped journal; refusing to write"
            )
        return existing["id"], True, {"existing": True}

    def upsert_record(self, record, context):
        client = WorkdayClient(self.config)
        journal_id = self.journal_id(record)
        existing = client.find_journal(journal_id)
        if existing:
            return self.existing_result(existing, record)
        if record.get("id"):
            raise WorkdayError(
                "Snapshot/explicit ID has no matching source-scoped journal; refusing to create"
            )
        request = self.journal_request(client, record, journal_id)
        try:
            response = client.call("Submit_Accounting_Journal", request, write=True)
            remote = ids(response, "wd:Accounting_Journal_Reference")
            if not remote.get("WID"):
                raise WorkdayError(
                    "Submit_Accounting_Journal: no WID returned; outcome may be unknown"
                )
            if remote.get("Accounting_Journal_ID", journal_id) != journal_id:
                raise WorkdayError("Submit_Accounting_Journal: unexpected journal reference")
            return remote["WID"], True, {}
        except WorkdayError as error:
            # Covers a concurrent create or a commit whose HTTP response was lost.
            # Never retry submission here; a fresh run uses the same Add_Only ID.
            try:
                existing = client.find_journal(journal_id)
            except WorkdayError:
                raise WorkdayError(
                    f"{error}; reconciliation failed, inspect Workday before retrying"
                ) from None
            if existing:
                return self.existing_result(existing, record)
            raise error
