"""Singer entry point for Workday Finance journal exports."""

from hotglue_singer_sdk.target_sdk.target import TargetHotglue

from target_workday.sinks import AccountingJournalsSink


class TargetWorkday(TargetHotglue):
    name = "target-workday"
    SINK_TYPES = [AccountingJournalsSink]
    MAX_PARALLELISM = 1
    config_jsonschema = {
        "type": "object",
        "required": ["host", "tenant", "username", "password"],
        "properties": {
            **{key: {"type": "string", "minLength": 1} for key in ("host", "tenant", "username")},
            "password": {"type": "string", "minLength": 1, "secret": True, "writeOnly": True},
            "api_version": {"type": "string", "pattern": "^v[0-9]+\\.[0-9]+$", "default": "v48.0"},
            "request_timeout": {"type": "integer", "minimum": 1, "default": 120},
            "submit_for_approval": {"type": "boolean", "default": False},
        },
    }

    def get_sink_class(self, stream_name):
        if stream_name != AccountingJournalsSink.name:
            raise ValueError(f"Unsupported stream: {stream_name}; expected accounting_journals")
        return AccountingJournalsSink


if __name__ == "__main__":
    TargetWorkday.cli()
