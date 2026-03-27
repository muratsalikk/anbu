from __future__ import annotations

import re
from typing import Iterable

RULE_NAME_RE = re.compile(r"^[A-Z0-9_]+$")
TARGET_NAME_RE = re.compile(r"^[A-Z0-9_.]+$")
IDENT_RE = re.compile(r"^[A-Z_][A-Z0-9_]*$")
NUMBER_RE = re.compile(r"^\d+(\.\d+)?$")
PLACEHOLDER_RE = re.compile(r"\{\{([A-Za-z0-9_]+)\}\}")
_SQL_FORBIDDEN_RE = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|ALTER|DROP|CREATE|GRANT|REVOKE|TRUNCATE|CALL|EXECUTE|COMMIT|ROLLBACK|COPY|DO|VACUUM|LOCK)\b",
    re.IGNORECASE,
)
_SQL_UNSAFE_FN_RE = re.compile(
    r"\b(pg_sleep|dbms_lock\.sleep)\s*\(",
    re.IGNORECASE,
)


def validate_rule_name_upper(name: str) -> list[str]:
    errors: list[str] = []
    if not name:
        errors.append("RULE_NAME is required.")
        return errors
    if ".." in name or "/" in name or "\\" in name:
        errors.append("RULE_NAME cannot contain '..' or slashes.")
    if not RULE_NAME_RE.match(name):
        errors.append("RULE_NAME must be uppercase A-Z, 0-9, underscore only.")
    return errors


def validate_target_name_upper_allow_dot(name: str) -> list[str]:
    errors: list[str] = []
    if not name:
        errors.append("TARGET_NAME is required.")
        return errors
    if not TARGET_NAME_RE.match(name):
        errors.append("TARGET_NAME must be uppercase A-Z, 0-9, underscore, dot only.")
    return errors


def validate_upper_identifier(value: str, label: str = "identifier") -> list[str]:
    errors: list[str] = []
    if not value:
        errors.append(f"{label} is required.")
        return errors
    if not IDENT_RE.match(value):
        errors.append(f"{label} must be uppercase A-Z, 0-9, underscore and start with A-Z or underscore.")
    return errors


def validate_message_placeholders(message: str, allowed_identifiers: Iterable[str]) -> list[str]:
    errors: list[str] = []
    allowed = set(allowed_identifiers)
    for match in PLACEHOLDER_RE.finditer(message or ""):
        name = match.group(1)
        if name not in allowed:
            errors.append(f"Placeholder '{{{{{name}}}}}' is not an allowed identifier.")
    cleaned = PLACEHOLDER_RE.sub("", message or "")
    if "{{" in cleaned or "}}" in cleaned:
        errors.append("Message contains invalid placeholder syntax.")
    return errors


def validate_linux_cron(expr: str, label: str = "SCHEDULE_CRON") -> list[str]:
    errors: list[str] = []
    text = (expr or "").strip()
    if not text:
        return errors
    parts = re.split(r"\s+", text)
    if len(parts) != 5:
        errors.append(f"{label} must have 5 fields (Linux cron: min hour dom month dow).")
        return errors
    minute, hour, dom, month, dow = parts

    def has_only(value: str, allowed: str) -> bool:
        return bool(value) and all(ch in allowed for ch in value)

    numeric_chars = "0123456789"
    base_chars = numeric_chars + "*/,-"
    dom_chars = base_chars + "L"
    dow_chars = base_chars + "L"
    month_chars = base_chars + "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

    if not has_only(minute, base_chars):
        errors.append(f"{label} minutes field has invalid characters.")
    if not has_only(hour, base_chars):
        errors.append(f"{label} hours field has invalid characters.")
    if not has_only(dom, dom_chars):
        errors.append(f"{label} day-of-month field has invalid characters.")
    if not has_only(month, month_chars):
        errors.append(f"{label} month field has invalid characters.")
    if not has_only(dow, dow_chars):
        errors.append(f"{label} day-of-week field has invalid characters.")
    return errors


def validate_read_only_sql(sql_text: str, label: str = "SQL") -> list[str]:
    errors: list[str] = []
    if not sql_text or not sql_text.strip():
        errors.append(f"{label} is required.")
        return errors
    text = sql_text
    text = re.sub(r"/\*.*?\*/", " ", text, flags=re.DOTALL)
    text = re.sub(r"--.*?$", " ", text, flags=re.MULTILINE)
    text = re.sub(r"'([^']|'')*'", " ", text)
    stripped = text.strip()
    if not stripped:
        errors.append(f"{label} is required.")
        return errors
    head = stripped.lstrip()
    if not re.match(r"^(WITH|SELECT)\b", head, flags=re.IGNORECASE):
        errors.append(f"{label} must start with SELECT or WITH.")
        return errors
    if ";" in stripped:
        parts = stripped.split(";")
        if any(part.strip() for part in parts[1:]):
            errors.append(f"{label} must contain a single read-only statement.")
            return errors
    if _SQL_FORBIDDEN_RE.search(stripped):
        errors.append(f"{label} must be read-only (DDL/DML not allowed).")
    if _SQL_UNSAFE_FN_RE.search(stripped):
        errors.append(f"{label} contains blocked function(s).")
    return errors


def _tokenize_condition(expr: str) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    tokens: list[str] = []
    idx = 0
    while idx < len(expr):
        ch = expr[idx]
        if ch.isspace():
            idx += 1
            continue
        two = expr[idx : idx + 2]
        if two in (">=", "<=", "==", "!="):
            tokens.append(two)
            idx += 2
            continue
        if ch in "><()+-*/,":
            tokens.append(ch)
            idx += 1
            continue
        if ch.isdigit():
            start = idx
            idx += 1
            while idx < len(expr) and (expr[idx].isdigit() or expr[idx] == "."):
                idx += 1
            tokens.append(expr[start:idx])
            continue
        if ch.isalpha() or ch == "_":
            start = idx
            idx += 1
            while idx < len(expr) and (expr[idx].isalnum() or expr[idx] == "_"):
                idx += 1
            tokens.append(expr[start:idx])
            continue
        errors.append(f"Invalid character '{ch}' in condition.")
        break
    return tokens, errors


def validate_condition_tokens(expr: str, allowed_identifiers: Iterable[str]) -> list[str]:
    if not expr:
        return []
    tokens, errors = _tokenize_condition(expr)
    if errors:
        return errors
    allowed = set(allowed_identifiers)
    for token in tokens:
        if token in (">", ">=", "<", "<=", "==", "!=", "(", ")", "+", "-", "*", "/", ","):
            continue
        lowered = token.lower()
        if lowered in ("and", "or", "not", "greatest", "least", "max", "min"):
            continue
        if NUMBER_RE.match(token):
            continue
        if not IDENT_RE.match(token):
            errors.append(f"Invalid identifier '{token}' in condition.")
            continue
        if token not in allowed:
            errors.append(f"Identifier '{token}' is not allowed in condition.")
    return errors
