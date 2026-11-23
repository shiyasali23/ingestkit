import json
import logging
import sys
from datetime import UTC, datetime

ROOT_LOGGER_NAME = "ingestkit"
CONTEXT_FIELD_NAMES = ("run_id", "batch_id", "source_path", "checksum", "file_status", "attempt_number")


class JsonLogFormatter(logging.Formatter):
    def format(self, log_record: logging.LogRecord) -> str:
        log_payload = {
            "timestamp": datetime.fromtimestamp(log_record.created, UTC).isoformat(),
            "level": log_record.levelname,
            "logger_name": log_record.name,
            "message": log_record.getMessage(),
        }
        for field_name in CONTEXT_FIELD_NAMES:
            field_value = getattr(log_record, field_name, None)
            if field_value is not None:
                log_payload[field_name] = field_value
        if log_record.exc_info:
            log_payload["traceback"] = self.formatException(log_record.exc_info)
        return json.dumps(log_payload)


class ContextLogger(logging.LoggerAdapter):
    def process(self, log_message, keyword_arguments):
        merged_context = dict(self.extra)
        merged_context.update(keyword_arguments.pop("extra", None) or {})
        keyword_arguments["extra"] = merged_context
        return log_message, keyword_arguments


def configure_pipeline_logging(log_level: str = "INFO") -> None:
    root_logger = logging.getLogger(ROOT_LOGGER_NAME)
    root_logger.setLevel(log_level.upper())
    root_logger.propagate = False
    already_configured = any(
        isinstance(log_handler.formatter, JsonLogFormatter) for log_handler in root_logger.handlers
    )
    if already_configured:
        return
    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setFormatter(JsonLogFormatter())
    root_logger.addHandler(stream_handler)


def get_pipeline_logger(logger_name: str, **context_fields) -> ContextLogger:
    return ContextLogger(logging.getLogger(logger_name), context_fields)
