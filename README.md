# target-workday

Hotglue Singer target for **create-only Workday Finance accounting journals**, using
the SOAP `Financial_Management` service and WS-Security UsernameToken authentication.
Built for the Corpay demo in HGI-11627. No REST API or OAuth setup is needed.

## Scope and safety

- Accepts one stream, `accounting_journals`: one balanced journal per source transaction.
- Uses `Submit_Accounting_Journal`, not the Expenses credit-card-feed import. Journal
  lines support ledger accounts, cost centers, spend categories, and supplier worktags.
- **Creates drafts by default** (`Submit=false`, `Auto_Complete=false`). Setting
  `submit_for_approval=true` submits through Workday's configured approval process;
  this target never bypasses approval or automatically completes the business process.
- Existing journals are **skipped, never updated**, even if the source content changes.
  Corrections/reversals must be handled separately with new source IDs.
- This is a single-company, single-currency journal interface. Use the company's
  ledger currency; FX conversion and intercompany balancing are not implemented.
- Invalid or unbalanced records fail before making API calls. Amounts must be finite,
  nonnegative, less than 1e20, and have at most six decimal places. Each line must have
  exactly one positive debit or credit; total debits and credits must match exactly.

## Install

Requires Python 3.10, matching the pinned Hotglue SDK.

```bash
uv venv --python 3.10 --seed .venv
uv pip install --python .venv/bin/python -e .
```

## Configuration

Keep configuration, credentials, snapshots, input, and state in ignored `.secrets/`.
Example `.secrets/config.json` (replace placeholders locally):

```json
{
  "host": "https://your-workday-services-host.myworkday.com",
  "tenant": "your_tenant",
  "username": "integration_user@your_tenant",
  "password": "<password>",
  "api_version": "v48.0",
  "request_timeout": 120,
  "submit_for_approval": false
}
```

`host`, `tenant`, `username`, and `password` are required. The other values above are
the defaults. `host` is the HTTPS origin only, without a service path. The endpoint is
`{host}/ccx/service/{tenant}/Financial_Management/{api_version}`.

The integration user needs permission to **read accounting journals**, create them
with `Submit_Accounting_Journal`, and reference the selected company, ledger, accounts,
journal source, and worktags. Read access is required for duplicate reconciliation,
not merely for reporting. Permission failures stop the record; they are not treated
as an absent journal.

## Input contract

Every record requires `externalId`, `company_id`, `currency`, `ledger_type_id`,
`journal_source_id`, `accounting_date` (YYYY-MM-DD), and at least two `lines`.
Unknown fields are rejected so coding is not silently discarded.

Example `.secrets/journal.json`:

```json
{
  "externalId": "corpay-card-transaction-1001",
  "company_id": "Company_US",
  "currency": "USD",
  "ledger_type_id": "ACTUALS",
  "journal_source_id": "CORPAY",
  "accounting_date": "2026-11-10",
  "memo": "Corpay card purchase",
  "lines": [
    {
      "account_set_id": "Corporate_Accounts",
      "account_number": "6100",
      "debit": "125.50",
      "memo": "Travel",
      "cost_center_id": "CC001",
      "spend_category_id": "Travel"
    },
    {
      "account_set_id": "Corporate_Accounts",
      "account_number": "2550",
      "credit": "125.50"
    }
  ]
}
```

The example IDs are illustrative, not guaranteed to exist in your tenant. Choose
valid Workday references from `tap-workday` output or tenant setup:

| Input | Workday ID type |
| --- | --- |
| `company_id` | `Company_Reference_ID` |
| `currency` | `Currency_ID` |
| `ledger_type_id` | `Ledger_Type_ID` |
| `journal_source_id` | `Journal_Source_ID` |
| Line `account_number` | `Ledger_Account_ID`, scoped by `account_set_id` (`Account_Set_ID`) |
| Line `cost_center_id` | `Cost_Center_Reference_ID` |
| Line `spend_category_id` | `Spend_Category_ID` |
| Line `supplier_id` | `Supplier_Reference_ID` |

Line worktags and both memo fields are optional. Use decimal strings for money to
avoid upstream floating-point rounding. GL accounts must include their account set,
because an account number alone is not a globally unique Workday reference. Nested
line references are already-native Workday IDs; the target does not resolve arbitrary
source-system IDs or maintain a second reference map.

## Singer execution and state

Generate a Singer SCHEMA and RECORD from the example, using the target's input schema:

```bash
.venv/bin/python - <<'PY' > .secrets/data.singer
import json
from target_workday.sinks import JOURNAL_SCHEMA

print(json.dumps({"type": "SCHEMA", "stream": "accounting_journals",
                  "schema": JOURNAL_SCHEMA, "key_properties": ["externalId"]}))
with open(".secrets/journal.json") as source:
    print(json.dumps({"type": "RECORD", "stream": "accounting_journals",
                      "record": json.load(source)}))
PY
```

**The next command writes to Workday.** Run only in an approved test tenant with
validated company/accounts/worktags and authorization to create the example draft:

```bash
(cd .secrets && ../.venv/bin/target-workday --config config.json \
  < data.singer > target-state.json)
```

Logs go to stderr; Hotglue state goes to stdout. Inspect both `summary` and per-record
`bookmarks`; a zero process exit code alone does not mean every record succeeded.
Successful bookmarks retain the source `externalId` and Workday journal WID as `id`.

The pinned SDK reuses successful hashes from `incremental_target_state.json` in this
working directory. Set `FLOW` and `SNAPSHOT_DIR` **before launching** to reuse native
`accounting_journals_<FLOW>.snapshot.csv`/`.parquet` files (`InputId` → `RemoteId`).
Explicit/snapshot `id` values are Workday WIDs and must agree with the remotely
reconciled source/company identity; missing or conflicting references fail closed.
Failed records remain retryable.

### Duplicate protection and uncertain writes

The target generates a deterministic `Accounting_Journal_ID`:
`HG_` plus SHA-256 of the compact JSON array `[tenant, company_id, externalId]`.
Keep these identifiers stable across reruns. It exact-looks-up this ID through
`Get_Journals` before creating, and submits with `Add_Only=true`. A matching remote
journal is reported as existing rather than overwritten, including when local state
was lost. Remote identity must include the same company and external reference.

Reads retry bounded 429/502/503/504 responses. Writes retry only explicit 429
rejections, honoring `Retry-After` or bounded exponential backoff. A
timeout, connection error, malformed response, or 5xx **never automatically replays
the submission**: the target first reconciles remotely by the stable journal ID.
An unresolved outcome is a failed bookmark, not an assumed success. Inspect Workday
before retrying such a record; retries retain the same create-only ID.

## Development and validation status

```bash
uv pip install --python .venv/bin/python -e . --group dev
.venv/bin/pytest -q
.venv/bin/ruff check target_workday tests
.venv/bin/ruff format --check target_workday tests
```

Tests run real Singer input through the pinned Hotglue SDK with mocked SOAP responses,
covering source/remote IDs, drafts and approval mode, worktags, exact money validation,
state and snapshot reuse, lost responses, permissions, and bounded retry behavior.

The generated submission was additionally validated against Workday's published v47.0
Financial Management XSD (the public v48.0 schema URL was unavailable). A read-only
v48.0 sandbox lookup verified the specific structured missing-journal fault used by
duplicate detection. **No live journal has been created by this implementation's
validation, and sandbox write permissions/payload acceptance remain unverified.**
The target has not been deployed to Hotglue; an approved one-draft sandbox test and
then a deployed-job test are the remaining integration validation gates.
