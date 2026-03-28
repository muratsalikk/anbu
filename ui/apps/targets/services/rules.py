from __future__ import annotations

from datetime import datetime, time
import json
from pathlib import Path
import re
from typing import Any

import anbu_validators as validators
import storage_env
import storage_sql


_OPERATOR_OPTIONS = ["", "=", "<", ">", "=<", "=>"]
_OPERATOR_NORMALIZE = {
    "==": "=",
    "<=": "=<",
    ">=": "=>",
}
_BETWEEN_DAY_VALUES = ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]


def parse_bool(value: object) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def parse_int(value: object, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except ValueError:
        return default


def _normalize_operator(operator: str) -> str:
    return _OPERATOR_NORMALIZE.get(operator.strip(), operator.strip())


def _parse_legacy_condition(expr: str) -> tuple[str, str]:
    pattern = r"^\s*[A-Z0-9_]+\s*(?P<op>>=|<=|==|!=|=|>|<)\s*(?P<val>[A-Z0-9_]+|\d+)\s*$"
    match = re.match(pattern, expr or "")
    if not match:
        return "", ""
    op = _normalize_operator(match.group("op"))
    value = match.group("val")
    if op not in _OPERATOR_OPTIONS:
        return "", ""
    return op, value


def _coerce_between_rules(value: object) -> list[dict[str, object]]:
    if isinstance(value, str):
        return _parse_between_rules_json(value)
    return _normalize_between_rules(value)


def _parse_conditions(raw: dict[str, str], index: int, severity: str) -> list[dict[str, object]]:
    conditions: dict[int, dict[str, object]] = {}
    pattern = re.compile(
        rf"^METRIC_{index}_{severity}_(\d+)_(OPERATOR|VAL|ACTION|MSG|TIMEFRAME)$"
    )
    for key, value in raw.items():
        match = pattern.match(key)
        if match:
            cond_index = int(match.group(1))
            field = match.group(2)
            parsed_value: object = value
            if field == "TIMEFRAME":
                parsed_value = _parse_between_rules_json(value)
            conditions.setdefault(cond_index, {})[field] = parsed_value

    if conditions:
        ordered = [conditions[i] for i in sorted(conditions)]
        for cond in ordered:
            if cond.get("OPERATOR"):
                cond["OPERATOR"] = _normalize_operator(str(cond["OPERATOR"]))
            cond["TIMEFRAME"] = _coerce_between_rules(cond.get("TIMEFRAME", []))
        return ordered

    legacy = {
        "OPERATOR": raw.get(f"METRIC_{index}_{severity}_OPERATOR", ""),
        "VAL": raw.get(f"METRIC_{index}_{severity}_VAL", ""),
        "ACTION": raw.get(f"METRIC_{index}_{severity}_ACTION", ""),
        "MSG": raw.get(f"METRIC_{index}_{severity}_MSG", ""),
        "TIMEFRAME": _parse_between_rules_json(
            raw.get(f"METRIC_{index}_{severity}_TIMEFRAME", "")
        ),
    }
    if any(legacy.values()):
        if legacy.get("OPERATOR"):
            legacy["OPERATOR"] = _normalize_operator(str(legacy["OPERATOR"]))
        return [legacy]

    legacy_if = raw.get(f"METRIC_{index}_{severity}_IF", "")
    op, val = _parse_legacy_condition(legacy_if)
    if op:
        return [
            {
                "OPERATOR": op,
                "VAL": val,
                "ACTION": raw.get(f"METRIC_{index}_{severity}_ACTION", ""),
                "MSG": raw.get(f"METRIC_{index}_{severity}_MSG", ""),
                "TIMEFRAME": _parse_between_rules_json(
                    raw.get(f"METRIC_{index}_{severity}_TIMEFRAME", "")
                ),
            }
        ]
    return []


def _parse_hhmm(value: str) -> tuple[int, int] | None:
    if not value:
        return None
    parts = value.strip().split(":")
    if len(parts) < 2:
        return None
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except ValueError:
        return None
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return None
    return hour, minute


def _normalize_between_days(value: object) -> list[str]:
    if isinstance(value, str):
        raw_items = [part.strip().upper() for part in value.split(",")]
    elif isinstance(value, list):
        raw_items = [str(item).strip().upper() for item in value]
    else:
        raw_items = []
    days: list[str] = []
    for item in raw_items:
        if item in _BETWEEN_DAY_VALUES and item not in days:
            days.append(item)
    return days


def _normalize_between_rules(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []
    rules: list[dict[str, object]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        start_raw = str(item.get("start", "")).strip()
        end_raw = str(item.get("end", "")).strip()
        start_hm = _parse_hhmm(start_raw)
        end_hm = _parse_hhmm(end_raw)
        if not start_hm or not end_hm:
            continue
        days = _normalize_between_days(item.get("days", []))
        if not days:
            days = _BETWEEN_DAY_VALUES.copy()
        rules.append(
            {
                "start": f"{start_hm[0]:02d}:{start_hm[1]:02d}",
                "end": f"{end_hm[0]:02d}:{end_hm[1]:02d}",
                "days": days,
            }
        )
    return rules


def _parse_between_rules_json(value: str) -> list[dict[str, object]]:
    raw = (value or "").strip()
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return []
    return _normalize_between_rules(parsed)


def _dump_between_rules_json(value: object) -> str:
    rules = _normalize_between_rules(value)
    if not rules:
        return ""
    return json.dumps(rules, separators=(",", ":"))


def _parse_datetime(value: str) -> datetime | None:
    if not value:
        return None
    text = value.strip().replace("T", " ")
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        try:
            return datetime.strptime(text, "%Y-%m-%d %H:%M")
        except ValueError:
            try:
                return datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None


def dump_between_rules_json(value: object) -> str:
    return _dump_between_rules_json(value)


def is_in_mute_policy(rule: dict[str, Any], now: datetime | None = None) -> bool:
    current = now or datetime.now()
    mute_between_enabled = bool(rule.get("mute_between_enabled", False))
    mute_until_enabled = bool(rule.get("mute_until_enabled", False))
    muted = False

    if mute_between_enabled:
        between_rules = _normalize_between_rules(rule.get("mute_between_rules", []))
        if not between_rules:
            legacy_start = str(rule.get("mute_between_start", "") or "")
            legacy_end = str(rule.get("mute_between_end", "") or "")
            if legacy_start or legacy_end:
                between_rules = _normalize_between_rules(
                    [
                        {
                            "start": legacy_start,
                            "end": legacy_end,
                            "days": _BETWEEN_DAY_VALUES,
                        }
                    ]
                )
        now_t = current.time()
        weekday = _BETWEEN_DAY_VALUES[current.weekday()]
        for between in between_rules:
            days = _normalize_between_days(between.get("days", []))
            if days and weekday not in days:
                continue
            start_hm = _parse_hhmm(str(between.get("start", "")))
            end_hm = _parse_hhmm(str(between.get("end", "")))
            if not start_hm or not end_hm:
                continue
            start_t = time(start_hm[0], start_hm[1])
            end_t = time(end_hm[0], end_hm[1])
            if start_t == end_t:
                muted = True
            elif start_t < end_t:
                if start_t <= now_t < end_t:
                    muted = True
            else:
                if now_t >= start_t or now_t < end_t:
                    muted = True
            if muted:
                break

    if mute_until_enabled:
        until_raw = str(rule.get("mute_until", "") or "")
        until_dt = _parse_datetime(until_raw)
        if until_dt and current <= until_dt:
            muted = True
    return muted


def parse_rule(rule_name: str, raw: dict[str, str]) -> dict[str, Any]:
    mapping = {f"VAL{i}": raw.get(f"MAP_VAL{i}", "") for i in range(1, 11)}
    metric_count = parse_int(raw.get("METRIC_COUNT"), 0)
    if metric_count <= 0:
        indices: list[int] = []
        for key in raw.keys():
            if key.startswith("METRIC_") and key.endswith("_NAME"):
                part = key.split("_")[1]
                if part.isdigit():
                    indices.append(int(part))
        metric_count = max(indices) if indices else 0
    metrics: list[dict[str, object]] = []
    for index in range(1, metric_count + 1):
        metric: dict[str, object] = {}
        for field in storage_env.METRIC_FIELD_ORDER:
            metric[field] = raw.get(f"METRIC_{index}_{field}", "")
        for severity in storage_env.SEVERITY_LEVELS:
            metric[severity] = _parse_conditions(raw, index, severity)
        metrics.append(metric)

    tag_list = raw.get("TAG_LIST", "")
    tags = [tag.strip() for tag in tag_list.split(",") if tag.strip()]
    query_file = raw.get("QUERY_FILE") or f"{rule_name}.sql"
    sql_mode = raw.get("SQL_MODE", "single").strip().lower()
    if sql_mode not in ("single", "multiline"):
        sql_mode = "single"
    mute_between_rules = _parse_between_rules_json(raw.get("MUTE_BETWEEN_RULES", ""))
    legacy_between_start = raw.get("MUTE_BETWEEN_START", "")
    legacy_between_end = raw.get("MUTE_BETWEEN_END", "")
    if not mute_between_rules and (legacy_between_start or legacy_between_end):
        mute_between_rules = _normalize_between_rules(
            [
                {
                    "start": legacy_between_start,
                    "end": legacy_between_end,
                    "days": _BETWEEN_DAY_VALUES,
                }
            ]
        )
    mute_between_enabled = parse_bool(raw.get("MUTE_BETWEEN_ENABLED", ""))
    mute_until = raw.get("MUTE_UNTIL", "")
    mute_until_enabled = parse_bool(raw.get("MUTE_UNTIL_ENABLED", ""))
    if not mute_between_enabled and mute_between_rules:
        mute_between_enabled = True
    if not mute_until_enabled and mute_until:
        mute_until_enabled = True
    return {
        "rule_name": rule_name,
        "target_name": rule_name,
        "description": raw.get("DESCRIPTION", ""),
        "document_url": raw.get("DOCUMENT_URL", ""),
        "dashboard_url": raw.get("DASHBOARD_URL", ""),
        "schedule_cron": raw.get("SCHEDULE_CRON", ""),
        "sql_mode": sql_mode,
        "mute_between_enabled": mute_between_enabled,
        "mute_between_rules": mute_between_rules,
        "mute_until_enabled": mute_until_enabled,
        "mute_until": mute_until,
        "tag_list": tag_list,
        "tags": tags,
        "is_active": parse_bool(raw.get("IS_ACTIVE", "false")),
        "is_muted": parse_bool(raw.get("IS_MUTED", "false")),
        "data_source": raw.get("DATA_SOURCE", ""),
        "sql_timeout_sec": parse_int(raw.get("SQL_TIMEOUT_SEC", 0), 0),
        "sql_jitter_sec": parse_int(raw.get("SQL_JITTER_SEC", 0), 0),
        "query_file": query_file,
        "mapping": mapping,
        "metrics": metrics,
    }


def empty_metric() -> dict[str, object]:
    metric: dict[str, object] = {field: "" for field in storage_env.METRIC_FIELD_ORDER}
    for severity in storage_env.SEVERITY_LEVELS:
        metric[severity] = []
    return metric


def empty_rule() -> dict[str, Any]:
    return {
        "rule_name": "",
        "target_name": "",
        "description": "",
        "document_url": "",
        "dashboard_url": "",
        "schedule_cron": "",
        "sql_mode": "single",
        "mute_between_enabled": False,
        "mute_between_rules": [],
        "mute_until_enabled": False,
        "mute_until": "",
        "tag_list": "",
        "tags": [],
        "is_active": True,
        "is_muted": False,
        "data_source": "",
        "sql_timeout_sec": 60,
        "sql_jitter_sec": 0,
        "query_file": "",
        "mapping": {f"VAL{i}": "" for i in range(1, 11)},
        "metrics": [empty_metric()],
    }


def load_rules(rules_dir: Path) -> list[dict[str, Any]]:
    rules: list[dict[str, Any]] = []
    if not rules_dir.exists():
        return rules
    for env_path in sorted(rules_dir.glob("*.env")):
        raw = storage_env.read_env(env_path)
        rules.append(parse_rule(env_path.stem, raw))
    return rules


def load_rule_for_edit(rules_dir: Path, rule_name: str) -> tuple[dict[str, Any], str]:
    raw = storage_env.read_env(rules_dir / f"{rule_name}.env")
    if not raw:
        raise FileNotFoundError(f"Rule not found: {rule_name}")
    rule = parse_rule(rule_name, raw)
    sql_text = ""
    if rule["query_file"]:
        sql_text = storage_sql.read_sql(rules_dir / str(rule["query_file"]))
    return rule, sql_text


def rule_to_form_initial(rule: dict[str, Any], sql_text: str) -> dict[str, Any]:
    initial: dict[str, Any] = {
        "target_name": rule.get("target_name", ""),
        "description": rule.get("description", ""),
        "document_url": rule.get("document_url", ""),
        "dashboard_url": rule.get("dashboard_url", ""),
        "tag_list": rule.get("tag_list", ""),
        "is_active": bool(rule.get("is_active", False)),
        "is_muted": bool(rule.get("is_muted", False)),
        "data_source": rule.get("data_source", ""),
        "sql_timeout_sec": int(rule.get("sql_timeout_sec", 0) or 0),
        "sql_jitter_sec": int(rule.get("sql_jitter_sec", 0) or 0),
        "schedule_cron": rule.get("schedule_cron", ""),
        "sql_mode": rule.get("sql_mode", "single"),
        "mute_between_enabled": bool(rule.get("mute_between_enabled", False)),
        "mute_between_rules": _dump_between_rules_json(rule.get("mute_between_rules", [])),
        "mute_until_enabled": bool(rule.get("mute_until_enabled", False)),
        "mute_until": rule.get("mute_until", ""),
        "metrics_json": json.dumps(rule.get("metrics", []), indent=2),
        "sql_text": sql_text or "",
        "original_name": rule.get("rule_name", ""),
        "original_query_file": rule.get("query_file", ""),
    }
    mapping = rule.get("mapping", {}) or {}
    for i in range(1, 11):
        initial[f"map_val{i}"] = mapping.get(f"VAL{i}", "")
    return initial


def validate_rule_data(
    rule_name: str,
    header: dict[str, str],
    mapping: dict[str, str],
    metrics: list[dict[str, object]],
    sql_text: str,
) -> list[str]:
    errors: list[str] = []
    errors.extend(validators.validate_target_name_upper_allow_dot(rule_name))
    if ".." in rule_name or "/" in rule_name or "\\" in rule_name:
        errors.append("TARGET_NAME cannot contain '..' or slashes.")
    if not header.get("DESCRIPTION"):
        errors.append("DESCRIPTION is required.")
    if not header.get("DATA_SOURCE"):
        errors.append("DATA_SOURCE is required.")
    if not str(header.get("SQL_TIMEOUT_SEC", "")).isdigit():
        errors.append("SQL_TIMEOUT_SEC must be an integer.")
    if not str(header.get("SQL_JITTER_SEC", "")).isdigit():
        errors.append("SQL_JITTER_SEC must be an integer.")

    sql_mode = str(header.get("SQL_MODE", "single")).strip().lower()
    if sql_mode not in {"single", "multiline"}:
        errors.append("SQL_MODE must be single or multiline.")
    schedule_cron = str(header.get("SCHEDULE_CRON", "")).strip()
    errors.extend(validators.validate_linux_cron(schedule_cron, "SCHEDULE_CRON"))

    if sql_mode == "multiline":
        if mapping.get("VAL1", "") != "METRIC_NAME":
            errors.append("SQL_MODE multiline requires MAP_VAL1 to be METRIC_NAME.")
        for key, alias in mapping.items():
            if key != "VAL1" and alias == "METRIC_NAME":
                errors.append("METRIC_NAME is reserved for MAP_VAL1 in multiline mode.")

    if parse_bool(header.get("MUTE_BETWEEN_ENABLED", "false")):
        between_rules = _parse_between_rules_json(
            str(header.get("MUTE_BETWEEN_RULES", "") or "")
        )
        if not between_rules:
            errors.append(
                "MUTE_BETWEEN_RULES must include at least one between rule when enabled."
            )
        for index, rule in enumerate(between_rules, start=1):
            days = _normalize_between_days(rule.get("days", []))
            if not days:
                errors.append(
                    f"MUTE_BETWEEN_RULES[{index}] must contain at least one day."
                )
    if parse_bool(header.get("MUTE_UNTIL_ENABLED", "false")):
        until_value = header.get("MUTE_UNTIL", "")
        if not _parse_datetime(str(until_value)):
            errors.append("MUTE_UNTIL must be a valid date-time.")

    query_file = header.get("QUERY_FILE", "")
    if not query_file:
        errors.append("QUERY_FILE is required.")
    if query_file and (".." in query_file or "/" in query_file or "\\" in query_file):
        errors.append("QUERY_FILE must be a filename without path segments.")
    if query_file and not query_file.lower().endswith(".sql"):
        errors.append("QUERY_FILE must end with .sql.")
    errors.extend(validators.validate_read_only_sql(sql_text, "SQL"))

    alias_set: set[str] = set()
    for val_key, alias in mapping.items():
        if not alias:
            continue
        errors.extend(validators.validate_upper_identifier(alias, f"MAP_{val_key}"))
        if alias in alias_set:
            errors.append(f"Duplicate mapping alias: {alias}.")
        alias_set.add(alias)

    value_ids = {f"VAL{i}" for i in range(1, 11)}
    allowed_identifiers = set(value_ids) | alias_set | {
        "BASELINE",
        "DEVIATION",
        "PREVIOUS",
        "LAST_WEEK",
        "LAST_MONTH",
        "TARGET_NAME",
        "METRIC_NAME",
        "CONDITION_VALUE",
    }
    max_metrics = 100 if sql_mode == "multiline" else 10
    if not 1 <= len(metrics) <= max_metrics:
        errors.append(f"METRIC_COUNT must be between 1 and {max_metrics}.")

    for index, metric in enumerate(metrics, start=1):
        if not isinstance(metric, dict):
            errors.append(f"METRIC_{index} must be a JSON object.")
            continue
        name = str(metric.get("NAME", "") or "").strip().upper()
        errors.extend(validators.validate_upper_identifier(name, f"METRIC_{index}_NAME"))

        value = str(metric.get("VALUE", "") or "").strip()
        if not value:
            errors.append(f"METRIC_{index}_VALUE is required.")
        elif value not in value_ids and value not in alias_set:
            errors.append(f"METRIC_{index}_VALUE must be VAL1..VAL10 or a mapped alias.")

        normal_action = str(metric.get("NORMAL_ACTION", "") or "").strip()
        normal_msg = str(metric.get("NORMAL_MSG", "") or "").strip()
        if not normal_action:
            errors.append(f"METRIC_{index}_NORMAL_ACTION is required.")
        if not normal_msg:
            errors.append(f"METRIC_{index}_NORMAL_MSG is required.")
        if len(normal_msg) > 280:
            errors.append(f"METRIC_{index}_NORMAL_MSG exceeds 280 chars.")
        errors.extend(
            validators.validate_message_placeholders(normal_msg, allowed_identifiers)
        )

        for severity in storage_env.SEVERITY_LEVELS:
            conditions = metric.get(severity, [])
            if not isinstance(conditions, list):
                errors.append(f"METRIC_{index}_{severity} must be a list.")
                continue
            if severity == "CRITICAL" and not conditions:
                errors.append(f"METRIC_{index}_CRITICAL conditions are required.")
            for cond_index, condition in enumerate(conditions, start=1):
                if not isinstance(condition, dict):
                    errors.append(
                        f"METRIC_{index}_{severity}_{cond_index} must be a JSON object."
                    )
                    continue
                operator = _normalize_operator(str(condition.get("OPERATOR", "")))
                cond_value = str(condition.get("VAL", "") or "").strip()
                action = str(condition.get("ACTION", "") or "").strip()
                msg = str(condition.get("MSG", "") or "").strip()
                timeframe_raw = condition.get("TIMEFRAME", [])
                timeframe_rules = _coerce_between_rules(timeframe_raw)

                if operator and operator not in _OPERATOR_OPTIONS:
                    errors.append(
                        f"METRIC_{index}_{severity}_{cond_index}_OPERATOR must be one of {_OPERATOR_OPTIONS[1:]}."
                    )
                if not operator:
                    errors.append(
                        f"METRIC_{index}_{severity}_{cond_index}_OPERATOR is required."
                    )
                if not cond_value:
                    errors.append(
                        f"METRIC_{index}_{severity}_{cond_index}_VAL is required."
                    )
                if not action:
                    errors.append(
                        f"METRIC_{index}_{severity}_{cond_index}_ACTION is required."
                    )
                if not msg:
                    errors.append(
                        f"METRIC_{index}_{severity}_{cond_index}_MSG is required."
                    )
                if len(msg) > 280:
                    errors.append(
                        f"METRIC_{index}_{severity}_{cond_index}_MSG exceeds 280 chars."
                    )
                errors.extend(
                    validators.validate_message_placeholders(msg, allowed_identifiers)
                )
                errors.extend(
                    validators.validate_condition_tokens(cond_value, allowed_identifiers)
                )
                if timeframe_raw not in ("", None, []) and not timeframe_rules:
                    errors.append(
                        f"METRIC_{index}_{severity}_{cond_index}_TIMEFRAME must be a valid between-rule array."
                    )
                if len(timeframe_rules) > 1:
                    errors.append(
                        f"METRIC_{index}_{severity}_{cond_index}_TIMEFRAME supports only one interval."
                    )
    return errors


def _rule_to_header(rule: dict[str, Any]) -> dict[str, object]:
    return {
        "TARGET_NAME": rule.get("target_name", ""),
        "DESCRIPTION": rule.get("description", ""),
        "DOCUMENT_URL": rule.get("document_url", ""),
        "DASHBOARD_URL": rule.get("dashboard_url", ""),
        "TAG_LIST": rule.get("tag_list", ""),
        "IS_ACTIVE": "true" if rule.get("is_active") else "false",
        "IS_MUTED": "true" if rule.get("is_muted") else "false",
        "DATA_SOURCE": rule.get("data_source", ""),
        "SQL_TIMEOUT_SEC": rule.get("sql_timeout_sec", ""),
        "SQL_JITTER_SEC": rule.get("sql_jitter_sec", ""),
        "SQL_MODE": rule.get("sql_mode", "single"),
        "SCHEDULE_CRON": rule.get("schedule_cron", ""),
        "MUTE_BETWEEN_ENABLED": "true" if rule.get("mute_between_enabled") else "false",
        "MUTE_BETWEEN_RULES": _dump_between_rules_json(rule.get("mute_between_rules", [])),
        "MUTE_UNTIL_ENABLED": "true" if rule.get("mute_until_enabled") else "false",
        "MUTE_UNTIL": rule.get("mute_until", ""),
        "QUERY_FILE": rule.get("query_file", ""),
    }


def apply_rule_updates(
    rules_dir: Path,
    rule_name: str,
    updates: dict[str, object],
) -> bool:
    env_path = rules_dir / f"{rule_name}.env"
    raw = storage_env.read_env(env_path)
    if not raw:
        return False
    rule = parse_rule(rule_name, raw)
    header = _rule_to_header(rule)
    header.update(updates)
    storage_env.write_rule_env(env_path, header, rule["mapping"], rule["metrics"])
    return True


def save_rule(
    rules_dir: Path,
    rule_name: str,
    header: dict[str, str],
    mapping: dict[str, str],
    metrics: list[dict[str, object]],
    sql_text: str,
    original_name: str = "",
    original_query_file: str = "",
) -> str:
    target_name = rule_name.strip().upper()
    if not target_name:
        raise ValueError("TARGET_NAME is required.")

    rename_from = bool(original_name and original_name != target_name)
    header = header.copy()
    header["TARGET_NAME"] = target_name
    header["QUERY_FILE"] = header.get("QUERY_FILE") or f"{target_name}.sql"

    env_path = rules_dir / f"{target_name}.env"
    if rename_from:
        old_env_path = rules_dir / f"{original_name}.env"
        if old_env_path.exists():
            old_env_path.replace(env_path)

    storage_env.write_rule_env(env_path, header, mapping, metrics)

    sql_path = rules_dir / header["QUERY_FILE"]
    storage_sql.write_sql(sql_path, sql_text or "")

    if rename_from:
        old_query = original_query_file or f"{original_name}.sql"
        if old_query and old_query != header["QUERY_FILE"]:
            old_sql_path = rules_dir / old_query
            if old_sql_path.exists() and old_sql_path != sql_path:
                old_sql_path.unlink()

    return target_name
