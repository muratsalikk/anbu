from __future__ import annotations

import json
from pathlib import Path
import re
from typing import Iterable

HEADER_ORDER = [
    "TARGET_NAME",
    "DESCRIPTION",
    "DOCUMENT_URL",
    "DASHBOARD_URL",
    "TAG_LIST",
    "IS_ACTIVE",
    "IS_MUTED",
    "DATA_SOURCE",
    "SQL_TIMEOUT_SEC",
    "SQL_JITTER_SEC",
    "SQL_MODE",
    "SCHEDULE_CRON",
    "MUTE_BETWEEN_ENABLED",
    "MUTE_BETWEEN_RULES",
    "MUTE_UNTIL_ENABLED",
    "MUTE_UNTIL",
    "QUERY_FILE",
]

METRIC_FIELD_ORDER = [
    "NAME",
    "VALUE",
    "NORMAL_ACTION",
    "NORMAL_MSG",
]

SEVERITY_LEVELS = ["CRITICAL", "MAJOR", "MINOR"]
CONDITION_FIELDS = ["OPERATOR", "VAL", "ACTION", "MSG", "TIMEFRAME"]

_ENV_LINE_RE = re.compile(r"^([^=]+)=(.*)$")


def parse_env_text(text: str) -> dict[str, str]:
    data: dict[str, str] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        match = _ENV_LINE_RE.match(line)
        if not match:
            continue
        key = match.group(1).strip()
        value = match.group(2).strip()
        if value.startswith(("'", '"')) and len(value) >= 2 and value[-1] == value[0]:
            quote = value[0]
            value = value[1:-1]
            if quote == '"':
                value = value.replace('\\"', '"')
            elif quote == "'":
                value = value.replace("\\'", "'")
        data[key] = value
    return data


def read_env(path: str | Path) -> dict[str, str]:
    file_path = Path(path)
    if not file_path.exists():
        return {}
    return parse_env_text(file_path.read_text(encoding="utf-8"))


def _format_value(value: object) -> str:
    if value is None:
        return ""
    text = str(value)
    if text == "":
        return ""
    needs_quote = any(ch.isspace() for ch in text) or any(ch in text for ch in ["#", "="])
    if needs_quote or text.startswith(("'", '"')):
        text = text.replace('"', '\\"')
        return f'"{text}"'
    return text


def _lines_for_header(header: dict[str, object]) -> Iterable[str]:
    for key in HEADER_ORDER:
        if key in header:
            yield f"{key}={_format_value(header[key])}"


def _format_condition_value(field: str, value: object) -> str:
    if field == "TIMEFRAME":
        if value in ("", None, []):
            return ""
        if isinstance(value, str):
            return _format_value(value)
        return _format_value(json.dumps(value, separators=(",", ":")))
    return _format_value(value)


def write_rule_env(
    path: str | Path,
    header: dict[str, object],
    mapping: dict[str, str],
    metrics: list[dict[str, object]],
) -> None:
    lines: list[str] = []
    lines.extend(_lines_for_header(header))

    mapping_lines: list[str] = []
    for i in range(1, 11):
        alias = mapping.get(f"VAL{i}", "")
        if alias:
            mapping_lines.append(f"MAP_VAL{i}={_format_value(alias)}")
    if mapping_lines:
        lines.append("")
        lines.extend(mapping_lines)

    lines.append("")
    lines.append(f"METRIC_COUNT={len(metrics)}")
    for index, metric in enumerate(metrics, start=1):
        for field in METRIC_FIELD_ORDER:
            key = f"METRIC_{index}_{field}"
            value = metric.get(field, "")
            lines.append(f"{key}={_format_value(value)}")
        for level in SEVERITY_LEVELS:
            conditions = metric.get(level, [])
            if not isinstance(conditions, list):
                continue
            for cond_index, condition in enumerate(conditions, start=1):
                if not isinstance(condition, dict):
                    continue
                if not any(condition.get(field) for field in CONDITION_FIELDS):
                    continue
                for field in CONDITION_FIELDS:
                    value = condition.get(field, "")
                    if value in ("", None):
                        continue
                    key = f"METRIC_{index}_{level}_{cond_index}_{field}"
                    lines.append(f"{key}={_format_condition_value(field, value)}")

    Path(path).write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
