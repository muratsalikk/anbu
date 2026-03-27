from __future__ import annotations

import copy
from difflib import unified_diff
from datetime import datetime
import json
import mimetypes
import os
from pathlib import Path
import platform
import socket
import subprocess
from typing import Any

import anbu_validators as validators
import storage_env
import storage_sql
from django.conf import settings
from django.contrib import messages
from django.contrib.auth import logout
from django.contrib.auth.decorators import login_required
from django.contrib.auth.views import LoginView
from django.http import FileResponse
from django.http import Http404, HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils import timezone
from django.utils.dateparse import parse_datetime
from django.views.decorators.http import require_GET, require_POST

from .forms import (
    ActionPropertyForm,
    ApplicationPropertiesForm,
    DataSourceForm,
    ExploreForm,
    TargetRuleForm,
)
from .services import explore as explore_service
from .services import property_store
from .services import results as results_service
from .services import rules as rules_service


class AppLoginView(LoginView):
    template_name = "login.html"
    redirect_authenticated_user = True


APPLICATION_PROPERTY_KEYS = [
    "RULES_DIR",
    "DS_HOST",
    "DS_PORT",
    "DS_DBNAME",
    "DS_USER",
    "DS_PASS",
    "HELPER_TEXT_FILE",
    "SAVED_QUERIES_DIR",
    "ENGINE_LOG_FILE",
    "UI_LOG_FILE",
    "BACKUP_SCRIPT",
    "APP_LOGO_FILE",
    "AI_PROMPT_FILE",
]

DEFAULT_RULES_DIR = "./rules"
DEFAULT_SAVED_QUERIES_DIR = "./saved_queries"
DEFAULT_APP_LOGO_FILE = "./ui/static/assets/ico.png"
DEFAULT_AI_PROMPT_FILE = "./prompts/target_ai_prompt.txt"
DEFAULT_AI_PROMPT_TEXT = """You are an ANBU Alarm Authoring Assistant specialized in fast target creation and legacy alarm migration.

Primary objective:
- Generate ANBU files quickly and correctly.
- Preserve detection coverage: missing or muted alarms are higher risk than extra alarms.
- Still reduce unnecessary false positives to protect workforce.
- Support monitor-only metrics (for dashboards/trends) as first-class use cases.

You produce:
1) .env (required)
2) .sql (required)
3) .hql (optional, only when history import is requested)

Operating mode:
- GREENFIELD: brand-new alarm scenario.
- MIGRATION: convert existing alarm definitions from legacy systems/scripts/queries.
- If user indicates migration, preserve existing semantics first, then optimize.

Question policy (speed-first):
- Ask follow-up questions ONLY for blocking gaps.
- Use defaults for non-blocking fields.
- If enough data exists, do not ask additional questions; generate files immediately.

Blocking inputs (must know before output):
- TARGET_NAME (uppercase, allowed chars A-Z 0-9 _ .)
- DATA_SOURCE (must match an existing datasource name)
- SQL_MODE (single or multiline)
- SQL logic/query intent
- At least one metric definition (or enough source data to derive it)
- Available action names for routing (or explicit fallback action policy)

Default values for non-blocking fields:
- DESCRIPTION: concise migration/intent summary
- IS_ACTIVE=true
- IS_MUTED=false
- SQL_TIMEOUT_SEC=60
- SQL_JITTER_SEC=0
- SCHEDULE_CRON=* * * * *
- QUERY_FILE=<TARGET_NAME>.sql
- DOCUMENT_URL=
- DASHBOARD_URL=
- TAG_LIST=source,migrated (for migration) or monitoring (for greenfield)
- MUTE_BETWEEN_ENABLED=false
- MUTE_BETWEEN_RULES=
- MUTE_UNTIL_ENABLED=false
- MUTE_UNTIL=

Strict ANBU validity rules (must satisfy):
- .env format: KEY=VALUE.
- SQL must be one read-only statement (SELECT or WITH...SELECT), no DDL/DML.
- SQL_MODE=single:
  - SQL returns exactly 1 row.
  - Up to 10 columns mapped as VAL1..VAL10.
- SQL_MODE=multiline:
  - SQL returns one row per metric instance.
  - Column 1 is metric name.
  - MAP_VAL1 must be METRIC_NAME (exactly).
  - METRIC_NAME must not be used in MAP_VAL2..MAP_VAL10.
- Use MAP_VAL aliases where helpful. VALUE may reference VALn or alias.
- METRIC_COUNT required.
- For each metric, required fields:
  - METRIC_<n>_NAME
  - METRIC_<n>_VALUE
  - METRIC_<n>_NORMAL_ACTION
  - METRIC_<n>_NORMAL_MSG
  - at least one CRITICAL condition block:
    - METRIC_<n>_CRITICAL_<m>_OPERATOR
    - METRIC_<n>_CRITICAL_<m>_VAL
    - METRIC_<n>_CRITICAL_<m>_ACTION
    - METRIC_<n>_CRITICAL_<m>_MSG
- Allowed operators: =, <, >, =<, =>
- Optional MAJOR/MINOR blocks follow same pattern.

Condition/message context references allowed:
- VAL1..VAL10
- mapped aliases (MAP_VAL*)
- BASELINE
- DEVIATION
- TARGET_NAME
- METRIC_NAME
- CONDITION_VALUE
- placeholders in messages use {{NAME}}

Monitor-only metric guidance:
- Monitor-only metrics are valid and encouraged for dashboard/trend visibility.
- Keep them in target definitions even if they should not page.
- Use non-paging action strategy:
  - NORMAL_ACTION should be a non-paging action (or agreed fallback).
  - CRITICAL conditions must still be syntactically present; use non-paging action and conservative logic so they do not create operational noise.
- Do NOT remove useful metrics only because they are noisy; classify and route correctly.

Migration guidance:
- Preserve source alarm intent, severity semantics, and message language where practical.
- Map source entities to multiline metrics when source is instance-based (host/service/queue/etc.).
- If source has mixed alarming + trend metrics, keep both:
  - alarming metrics => actionable conditions/actions
  - monitor-only metrics => non-paging route
- Include migration-safe tags (for example: source system/team/domain).

If history import requested:
- Generate .hql read-only SQL returning exactly these columns:
  - dttm
  - metric_name
  - value

Output policy:
- If blocking data is missing, ask concise numbered questions and stop.
- If data is sufficient, output only fenced code blocks in this order:
  1) <TARGET_NAME>.env
  2) <TARGET_NAME>.sql
  3) <TARGET_NAME>.hql (only when requested)
- Do not add commentary outside code blocks when generating final files.
"""


def _project_root() -> Path:
    return Path(str(getattr(settings, "BASE_DIR", Path.cwd()))).parent.resolve()


def _resolve_app_path(raw_value: str, fallback: str) -> Path:
    value = str(raw_value or "").strip() or str(fallback or "").strip()
    path = Path(value)
    if not path.is_absolute():
        path = (_project_root() / path).resolve()
    return path


def _runtime_rules_dir(properties: dict[str, str] | None = None) -> Path:
    props = properties or _load_application_properties()
    return _resolve_app_path(str(props.get("RULES_DIR", "") or ""), DEFAULT_RULES_DIR)


def _runtime_saved_queries_dir(properties: dict[str, str] | None = None) -> Path:
    props = properties or _load_application_properties()
    return _resolve_app_path(
        str(props.get("SAVED_QUERIES_DIR", "") or ""),
        DEFAULT_SAVED_QUERIES_DIR,
    )


def _runtime_app_root(properties: dict[str, str] | None = None) -> Path:
    return _runtime_rules_dir(properties).parent


def _runtime_logo_path(properties: dict[str, str] | None = None) -> Path:
    props = properties or _load_application_properties()
    return _resolve_app_path(
        str(props.get("APP_LOGO_FILE", "") or ""),
        DEFAULT_APP_LOGO_FILE,
    )


def app_logo(request: HttpRequest) -> HttpResponse:
    logo_path = _runtime_logo_path()
    if not logo_path.exists() or not logo_path.is_file():
        raise Http404("App logo file is not configured.")
    content_type, _ = mimetypes.guess_type(str(logo_path))
    return FileResponse(
        logo_path.open("rb"),
        content_type=content_type or "application/octet-stream",
    )


@login_required
def app_logout(request: HttpRequest) -> HttpResponse:
    logout(request)
    return redirect("login")


def _is_htmx(request: HttpRequest) -> bool:
    return request.headers.get("HX-Request", "").lower() == "true"


def _load_datasource_choices() -> list[str]:
    datasource_content = property_store.get_property_content(
        "datasources.properties",
        None,
    )
    return explore_service.list_datasource_names(
        datasource_definition_text=datasource_content,
        datasource_definition_file=None,
    )


def _parse_selected_targets(request: HttpRequest) -> list[str]:
    raw = str(request.POST.get("selected_targets", "") or "").strip()
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    targets: list[str] = []
    for item in parsed:
        name = str(item or "").strip().upper()
        if name and name not in targets:
            targets.append(name)
    return targets


def _apply_bulk_updates(
    rules_dir: Path,
    target_names: list[str],
    updates: dict[str, object],
) -> tuple[list[str], list[str]]:
    updated_names: list[str] = []
    failed: list[str] = []
    for target_name in target_names:
        try:
            if rules_service.apply_rule_updates(rules_dir, target_name, updates):
                updated_names.append(target_name)
            else:
                failed.append(target_name)
        except Exception:
            failed.append(target_name)
    return updated_names, failed


def _action_redirect_url(request: HttpRequest) -> str:
    return_query = str(request.POST.get("return_query", "") or "").strip()
    if return_query:
        return f"{reverse('targets:list')}?{return_query}"
    return reverse("targets:list")


def _parse_change_notes(request: HttpRequest) -> str:
    raw = str(request.POST.get("change_notes", "") or "")
    return raw.replace("\r\n", "\n").replace("\r", "\n").strip()


def _build_rule_header(rule: dict[str, Any], target_name: str) -> dict[str, str]:
    return {
        "TARGET_NAME": target_name,
        "DESCRIPTION": str(rule.get("description", "") or ""),
        "DOCUMENT_URL": str(rule.get("document_url", "") or ""),
        "DASHBOARD_URL": str(rule.get("dashboard_url", "") or ""),
        "TAG_LIST": str(rule.get("tag_list", "") or ""),
        "IS_ACTIVE": "true" if bool(rule.get("is_active")) else "false",
        "IS_MUTED": "true" if bool(rule.get("is_muted")) else "false",
        "DATA_SOURCE": str(rule.get("data_source", "") or ""),
        "SQL_TIMEOUT_SEC": str(int(rule.get("sql_timeout_sec", 0) or 0)),
        "SQL_JITTER_SEC": str(int(rule.get("sql_jitter_sec", 0) or 0)),
        "SQL_MODE": str(rule.get("sql_mode", "single") or "single").strip().lower(),
        "SCHEDULE_CRON": str(rule.get("schedule_cron", "") or ""),
        "MUTE_BETWEEN_ENABLED": "true" if bool(rule.get("mute_between_enabled")) else "false",
        "MUTE_BETWEEN_RULES": rules_service.dump_between_rules_json(
            rule.get("mute_between_rules", [])
        ),
        "MUTE_UNTIL_ENABLED": "true" if bool(rule.get("mute_until_enabled")) else "false",
        "MUTE_UNTIL": str(rule.get("mute_until", "") or ""),
        "QUERY_FILE": f"{target_name}.sql",
    }


def _application_properties_path() -> Path | None:
    return None


def _datasource_properties_path() -> Path | None:
    return None


def _action_properties_path() -> Path | None:
    return None


def _load_application_properties() -> dict[str, str]:
    data = property_store.get_property_map(
        "application.properties",
        _application_properties_path(),
    )
    ds_fallbacks = {
        "DS_HOST": "PG_HOST",
        "DS_PORT": "PG_PORT",
        "DS_DBNAME": "PG_DBNAME",
        "DS_USER": "PG_USER",
        "DS_PASS": "PG_PASS",
    }
    loaded: dict[str, str] = {}
    for key in APPLICATION_PROPERTY_KEYS:
        value = str(data.get(key, "") or "")
        if not value and key in ds_fallbacks:
            value = str(data.get(ds_fallbacks[key], "") or "")
        if not value and key == "AI_PROMPT_FILE":
            value = DEFAULT_AI_PROMPT_FILE
        loaded[key] = value
    return loaded


def _ai_prompt_path(properties: dict[str, str] | None = None) -> Path:
    props = properties or _load_application_properties()
    configured = str(props.get("AI_PROMPT_FILE", "") or "").strip()
    raw_value = configured or DEFAULT_AI_PROMPT_FILE
    path = Path(raw_value)
    if not path.is_absolute():
        path = (_runtime_app_root(props) / path).resolve()
    return path


def _load_or_create_ai_prompt() -> tuple[str, str]:
    prompt_path = _ai_prompt_path()
    prompt_text = ""
    try:
        if prompt_path.exists() and prompt_path.is_file():
            prompt_text = prompt_path.read_text(encoding="utf-8")
        if not str(prompt_text).strip():
            prompt_path.parent.mkdir(parents=True, exist_ok=True)
            prompt_text = DEFAULT_AI_PROMPT_TEXT
            prompt_path.write_text(prompt_text, encoding="utf-8")
    except Exception:
        prompt_text = DEFAULT_AI_PROMPT_TEXT
    return prompt_text, str(prompt_path)


def _build_ai_prefill_initial(
    env_text: str,
    sql_text: str,
) -> tuple[dict[str, Any], str]:
    parsed_env = storage_env.parse_env_text(str(env_text or ""))
    target_name = str(parsed_env.get("TARGET_NAME", "") or "").strip().upper()
    if not target_name:
        return {}, "TARGET_NAME is required in .env."
    rule = rules_service.parse_rule(target_name, parsed_env)
    if not rule.get("metrics"):
        rule["metrics"] = [rules_service.empty_metric()]
    normalized_sql = str(sql_text or "").replace("\r\n", "\n").replace("\r", "\n")
    initial = rules_service.rule_to_form_initial(rule, normalized_sql)
    initial["target_name"] = target_name
    initial["original_name"] = ""
    initial["original_query_file"] = ""
    return initial, ""


def _resolve_optional_path(path_value: str) -> Path | None:
    raw = str(path_value or "").strip()
    if not raw:
        return None
    path = Path(raw)
    if not path.is_absolute():
        path = (_runtime_app_root() / path).resolve()
    return path


def _read_log_file(path_value: str) -> tuple[str, str, str]:
    path = _resolve_optional_path(path_value)
    if path is None:
        return "", "", "Log file is not configured."
    resolved_path = str(path)
    if not path.exists() or not path.is_file():
        return "", resolved_path, "Log file not found."
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except Exception as exc:
        return "", resolved_path, f"Failed to read log file: {exc}"
    return text, resolved_path, ""


def _server_ipv4_addresses() -> list[str]:
    ips: set[str] = set()
    try:
        for item in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            ip = str(item[4][0] or "").strip()
            if ip and not ip.startswith("127."):
                ips.add(ip)
    except Exception:
        pass
    if not ips:
        try:
            fallback_ip = str(socket.gethostbyname(socket.gethostname()) or "").strip()
            if fallback_ip:
                ips.add(fallback_ip)
        except Exception:
            pass
    return sorted(ips)


def _tail_log_lines(text: str, max_lines: int) -> str:
    if not text or max_lines <= 0:
        return text
    lines = text.splitlines()
    if len(lines) <= max_lines:
        return text
    return "\n".join(lines[-max_lines:])


def _reverse_lines(text: str) -> str:
    lines = text.splitlines()
    lines.reverse()
    return "\n".join(lines)


def _datasource_ref_for_new_name(name: str) -> str:
    cleaned = "".join(ch.lower() if ch.isalnum() else "_" for ch in str(name or ""))
    cleaned = cleaned.strip("_") or "datasource"
    return f"./ds/{cleaned}.properties"


def _datasource_ref_path(ref: str) -> Path:
    path = Path(str(ref or "").strip())
    if not path.is_absolute():
        path = (_runtime_app_root() / path).resolve()
    return path


def _replace_name_in_order(
    existing_order: list[str],
    original_name: str,
    new_name: str,
) -> list[str]:
    if not original_name:
        return [*existing_order, new_name]
    updated_order: list[str] = []
    for key in existing_order:
        if key == original_name:
            updated_order.append(new_name)
        elif key != new_name:
            updated_order.append(key)
    if new_name not in updated_order:
        updated_order.append(new_name)
    return updated_order


def _history_import_sql_path(target_name: str) -> Path:
    rules_dir = _runtime_rules_dir()
    return rules_dir / f"{target_name}.hql"


def _load_history_import_sql(target_name: str) -> str:
    normalized = str(target_name or "").strip().upper()
    if not normalized:
        return ""
    return storage_sql.read_sql(_history_import_sql_path(normalized))


def _safe_query_file_name(target_name: str, query_file: str) -> str:
    normalized_target = str(target_name or "").strip().upper()
    default_name = f"{normalized_target}.sql"
    candidate = str(query_file or "").strip()
    if not candidate:
        return default_name
    if ".." in candidate or "/" in candidate or "\\" in candidate:
        return default_name
    if not candidate.lower().endswith(".sql"):
        return default_name
    return candidate


def _target_env_path(target_name: str) -> Path:
    rules_dir = _runtime_rules_dir()
    normalized_target = str(target_name or "").strip().upper()
    return rules_dir / f"{normalized_target}.env"


def _read_target_snapshot_from_files(target_name: str) -> dict[str, str]:
    normalized_target = str(target_name or "").strip().upper()
    env_path = _target_env_path(normalized_target)
    env_content = ""
    if env_path.exists() and env_path.is_file():
        env_content = env_path.read_text(encoding="utf-8")

    env_map = storage_env.parse_env_text(env_content)
    query_file = _safe_query_file_name(
        normalized_target,
        str(env_map.get("QUERY_FILE", "") or ""),
    )
    rules_dir = _runtime_rules_dir()
    sql_path = rules_dir / query_file
    sql_content = storage_sql.read_sql(sql_path)

    hql_path = _history_import_sql_path(normalized_target)
    hql_content = storage_sql.read_sql(hql_path)

    return {
        "target_name": normalized_target,
        "query_file": query_file,
        "env_content": env_content,
        "sql_content": sql_content,
        "hql_content": hql_content,
    }


def _save_target_audit_snapshot(
    target_name: str,
    edited_by: str,
    change_notes: str = "",
) -> None:
    snapshot = _read_target_snapshot_from_files(target_name)
    results_service.create_target_audit_entry(
        target_name=snapshot["target_name"],
        user=edited_by,
        change_notes=change_notes,
        env_content=snapshot["env_content"],
        sql_content=snapshot["sql_content"],
        hql_content=snapshot["hql_content"],
    )


def _read_target_snapshot_from_rule_name(rule_name: str) -> dict[str, str]:
    rules_dir = _runtime_rules_dir()
    file_stem = str(rule_name or "").strip()
    env_path = rules_dir / f"{file_stem}.env"
    env_content = ""
    if env_path.exists() and env_path.is_file():
        env_content = env_path.read_text(encoding="utf-8")

    env_map = storage_env.parse_env_text(env_content)
    target_name = str(env_map.get("TARGET_NAME", file_stem) or file_stem).strip().upper()
    query_file = _safe_query_file_name(
        target_name,
        str(env_map.get("QUERY_FILE", "") or ""),
    )
    sql_content = storage_sql.read_sql(rules_dir / query_file)
    hql_content = storage_sql.read_sql(_history_import_sql_path(target_name))
    return {
        "target_name": target_name,
        "query_file": query_file,
        "env_content": env_content,
        "sql_content": sql_content,
        "hql_content": hql_content,
    }


def _save_target_audit_snapshot_safe(
    target_name: str,
    edited_by: str,
    change_notes: str = "",
) -> str:
    try:
        _save_target_audit_snapshot(target_name, edited_by, change_notes=change_notes)
        return ""
    except Exception as primary_exc:
        try:
            snapshot = _read_target_snapshot_from_rule_name(target_name)
            results_service.create_target_audit_entry(
                target_name=snapshot["target_name"],
                user=edited_by,
                change_notes=change_notes,
                env_content=snapshot["env_content"],
                sql_content=snapshot["sql_content"],
                hql_content=snapshot["hql_content"],
            )
            return ""
        except Exception as fallback_exc:
            return f"{primary_exc}; {fallback_exc}"


def _decorate_audit_rows(target_name: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized_target = str(target_name or "").strip().upper()
    decorated: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        row_id = int(row.get("id") or 0)
        item = dict(row)
        item["view_url"] = reverse(
            "targets:audit_view",
            args=[normalized_target, row_id],
        )
        item["changes_url"] = reverse(
            "targets:audit_changes",
            args=[normalized_target, row_id],
        )
        item["restore_url"] = reverse(
            "targets:audit_restore",
            args=[normalized_target, row_id],
        )
        item["is_latest"] = index == 0
        decorated.append(item)
    return decorated


def _write_snapshot_to_files(
    target_name: str,
    env_content: str,
    sql_content: str,
    hql_content: str,
) -> None:
    normalized_target = str(target_name or "").strip().upper()
    rules_dir = _runtime_rules_dir()
    env_path = _target_env_path(normalized_target)

    current_snapshot = _read_target_snapshot_from_files(normalized_target)
    current_query_file = current_snapshot.get("query_file", f"{normalized_target}.sql")

    normalized_env = str(env_content or "").replace("\r\n", "\n").replace("\r", "\n")
    if normalized_env and not normalized_env.endswith("\n"):
        normalized_env += "\n"
    env_path.write_text(normalized_env, encoding="utf-8")

    parsed_env = storage_env.parse_env_text(normalized_env)
    restored_query_file = _safe_query_file_name(
        normalized_target,
        str(parsed_env.get("QUERY_FILE", "") or ""),
    )
    restored_sql_path = rules_dir / restored_query_file
    storage_sql.write_sql(restored_sql_path, sql_content)

    if current_query_file != restored_query_file:
        old_sql_path = rules_dir / current_query_file
        if old_sql_path.exists() and old_sql_path.is_file():
            old_sql_path.unlink()

    hql_path = _history_import_sql_path(normalized_target)
    if str(hql_content or "").strip():
        storage_sql.write_sql(hql_path, hql_content)
    elif hql_path.exists() and hql_path.is_file():
        hql_path.unlink()


def _build_unified_file_diff(
    previous_text: str,
    current_text: str,
    file_label: str,
) -> str:
    previous_lines = str(previous_text or "").splitlines()
    current_lines = str(current_text or "").splitlines()
    diff_lines = list(
        unified_diff(
            previous_lines,
            current_lines,
            fromfile=f"previous/{file_label}",
            tofile=f"current/{file_label}",
            lineterm="",
        )
    )
    if not diff_lines:
        return "No changes."
    return "\n".join(diff_lines)


def _parse_history_import_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value or "").strip()
        if not raw:
            return None
        parsed = parse_datetime(raw)
        if parsed is None:
            parsed = parse_datetime(raw.replace(" ", "T"))
    if parsed is None:
        return None
    if timezone.is_aware(parsed):
        parsed = timezone.localtime(parsed).replace(tzinfo=None)
    return parsed.replace(second=0, microsecond=0)


def _build_history_import_rows(
    target_name: str,
    columns: list[str],
    source_rows: list[tuple[Any, ...]],
) -> tuple[list[dict[str, Any]], str]:
    col_map = {str(name or "").strip().lower(): idx for idx, name in enumerate(columns)}
    required = {"dttm", "metric_name", "value"}
    if any(col not in col_map for col in required):
        return [], "Result must include columns: dttm, metric_name, value."

    normalized_target = str(target_name or "").strip().upper()
    import_rows: list[dict[str, Any]] = []
    for row in source_rows:
        try:
            dttm_raw = row[col_map["dttm"]]
            metric_raw = row[col_map["metric_name"]]
            value_raw = row[col_map["value"]]
        except Exception:
            return [], "Row shape mismatch."

        metric_name = str(metric_raw or "").strip()
        if not metric_name:
            return [], "metric_name cannot be empty."

        evaluated_at = _parse_history_import_datetime(dttm_raw)
        if evaluated_at is None:
            return [], f"Invalid dttm: {dttm_raw}"

        try:
            metric_value = int(value_raw)
        except Exception:
            return [], f"Invalid value (not integer): {value_raw}"

        import_rows.append(
            {
                "evaluated_at": evaluated_at,
                "target_name": normalized_target,
                "metric_name": metric_name,
                "metric_value": metric_value,
                "severity": 0,
                "state": "data_import",
                "critical_val": None,
                "major_val": None,
                "minor_val": None,
                "message": None,
                "action_name": None,
                "datasource": None,
                "scheduler_name": None,
                "tags": None,
            }
        )
    return import_rows, ""


def _parse_history_metric_selection(value: str) -> list[str]:
    raw = str(value or "").strip()
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    selected: list[str] = []
    for item in parsed:
        metric = str(item or "").strip().upper()
        if metric and metric not in selected:
            selected.append(metric)
    return selected


@login_required
def anbu_settings(request: HttpRequest) -> HttpResponse:
    initial = _load_application_properties()
    if request.method == "POST":
        form = ApplicationPropertiesForm(request.POST)
        if form.is_valid():
            cleaned = {
                key: str(form.cleaned_data.get(key, "") or "").strip()
                for key in APPLICATION_PROPERTY_KEYS
            }
            property_store.set_property_map(
                name="application.properties",
                data=cleaned,
                fallback_path=_application_properties_path(),
                key_order=APPLICATION_PROPERTY_KEYS,
            )
            messages.success(request, "Application properties saved.")
            return redirect("targets:settings")
    else:
        form = ApplicationPropertiesForm(initial=initial)

    datasource_items = property_store.get_property_map(
        "datasources.properties",
        _datasource_properties_path(),
    )
    action_items = property_store.get_property_map(
        "actions.properties",
        _action_properties_path(),
    )
    return render(
        request,
        "targets/settings.html",
        {
            "form": form,
            "datasource_count": len(datasource_items),
            "action_count": len(action_items),
        },
    )


@login_required
def datasource_list(request: HttpRequest) -> HttpResponse:
    entries = property_store.get_property_map(
        "datasources.properties",
        _datasource_properties_path(),
    )
    rows: list[dict[str, str]] = []
    for name, ref in entries.items():
        cfg_path = _datasource_ref_path(ref)
        cfg = property_store.parse_kv_text(property_store.read_file_text(cfg_path))
        rows.append(
            {
                "name": name,
                "type": str(cfg.get("TYPE", "") or ""),
                "user": str(cfg.get("USER", "") or ""),
                "dsn": str(cfg.get("DSN", "") or ""),
                "edit_url": reverse("targets:datasource_edit", args=[name]),
            }
        )
    return render(
        request,
        "targets/datasource_list.html",
        {
            "rows": rows,
        },
    )


@login_required
def datasource_edit(
    request: HttpRequest,
    source_name: str | None = None,
) -> HttpResponse:
    entries = property_store.get_property_map(
        "datasources.properties",
        _datasource_properties_path(),
    )
    normalized_source_name = str(source_name or "").strip().upper()
    if normalized_source_name and normalized_source_name not in entries:
        raise Http404(f"Data source not found: {normalized_source_name}")

    if request.method == "POST":
        form = DataSourceForm(request.POST)
        if form.is_valid():
            new_name = str(form.cleaned_data.get("datasource_name", "")).strip().upper()
            datasource_type = str(form.cleaned_data.get("datasource_type", "")).strip().upper()
            datasource_user = str(form.cleaned_data.get("datasource_user", "")).strip()
            datasource_password = str(form.cleaned_data.get("datasource_password", "")).strip()
            datasource_dsn = str(form.cleaned_data.get("datasource_dsn", "")).strip()
            original_name = str(form.cleaned_data.get("original_name", "")).strip().upper()
            if new_name in entries and new_name != original_name:
                form.add_error("datasource_name", "Data source name already exists.")
            elif not datasource_type:
                form.add_error("datasource_type", "TYPE is required.")
            elif not datasource_user:
                form.add_error("datasource_user", "USER is required.")
            elif not datasource_dsn:
                form.add_error("datasource_dsn", "DSN is required.")
            else:
                updated_entries = {**entries}
                current_ref = str(updated_entries.get(original_name, "") or "").strip()
                if not current_ref:
                    current_ref = _datasource_ref_for_new_name(new_name)
                if original_name and original_name != new_name:
                    updated_entries.pop(original_name, None)
                updated_entries[new_name] = current_ref
                key_order = _replace_name_in_order(
                    list(entries.keys()),
                    original_name,
                    new_name,
                )
                property_store.set_property_map(
                    name="datasources.properties",
                    data=updated_entries,
                    fallback_path=_datasource_properties_path(),
                    key_order=key_order,
                )
                cfg_path = _datasource_ref_path(current_ref)
                cfg_content = property_store.dump_kv_text(
                    {
                        "TYPE": datasource_type,
                        "USER": datasource_user,
                        "PASSWORD": datasource_password,
                        "DSN": datasource_dsn,
                    },
                    key_order=["TYPE", "USER", "PASSWORD", "DSN"],
                )
                cfg_path.parent.mkdir(parents=True, exist_ok=True)
                cfg_path.write_text(cfg_content, encoding="utf-8")
                messages.success(request, f"Data source saved: {new_name}")
                return redirect("targets:datasource_list")
    else:
        current_ref = str(entries.get(normalized_source_name, "") or "").strip()
        cfg_map = {}
        if current_ref:
            cfg_map = property_store.parse_kv_text(
                property_store.read_file_text(_datasource_ref_path(current_ref))
            )
        initial = {
            "datasource_name": normalized_source_name,
            "datasource_type": str(cfg_map.get("TYPE", "POSTGRES") or "POSTGRES"),
            "datasource_user": str(cfg_map.get("USER", "") or ""),
            "datasource_password": str(cfg_map.get("PASSWORD", "") or ""),
            "datasource_dsn": str(cfg_map.get("DSN", "") or ""),
            "original_name": normalized_source_name,
        }
        if not normalized_source_name:
            initial = {
                "datasource_name": "",
                "datasource_type": "POSTGRES",
                "datasource_user": "",
                "datasource_password": "",
                "datasource_dsn": "",
                "original_name": "",
            }
        form = DataSourceForm(initial=initial)

    return render(
        request,
        "targets/datasource_form.html",
        {
            "form": form,
            "page_title": "Edit Data Source"
            if normalized_source_name
            else "Add Data Source",
            "is_edit": bool(normalized_source_name),
        },
    )


@login_required
def action_list(request: HttpRequest) -> HttpResponse:
    entries = property_store.get_property_map(
        "actions.properties",
        _action_properties_path(),
    )
    rows = [
        {
            "name": name,
            "file_path": path,
            "edit_url": reverse("targets:action_edit", args=[name]),
        }
        for name, path in entries.items()
    ]
    return render(
        request,
        "targets/action_list.html",
        {
            "rows": rows,
        },
    )


@login_required
def action_edit(
    request: HttpRequest,
    action_name: str | None = None,
) -> HttpResponse:
    entries = property_store.get_property_map(
        "actions.properties",
        _action_properties_path(),
    )
    normalized_action_name = str(action_name or "").strip().upper()
    if normalized_action_name and normalized_action_name not in entries:
        raise Http404(f"Action not found: {normalized_action_name}")

    if request.method == "POST":
        form = ActionPropertyForm(request.POST)
        if form.is_valid():
            new_name = str(form.cleaned_data.get("action_name", "")).strip().upper()
            file_path = str(form.cleaned_data.get("action_file_path", "")).strip()
            original_name = str(form.cleaned_data.get("original_name", "")).strip().upper()
            if new_name in entries and new_name != original_name:
                form.add_error("action_name", "Action name already exists.")
            else:
                updated_entries = {**entries}
                if original_name and original_name != new_name:
                    updated_entries.pop(original_name, None)
                updated_entries[new_name] = file_path
                key_order = _replace_name_in_order(
                    list(entries.keys()),
                    original_name,
                    new_name,
                )
                property_store.set_property_map(
                    name="actions.properties",
                    data=updated_entries,
                    fallback_path=_action_properties_path(),
                    key_order=key_order,
                )
                messages.success(request, f"Action saved: {new_name}")
                return redirect("targets:action_list")
    else:
        initial = {
            "action_name": normalized_action_name,
            "action_file_path": entries.get(normalized_action_name, ""),
            "original_name": normalized_action_name,
        }
        if not normalized_action_name:
            initial = {
                "action_name": "",
                "action_file_path": "",
                "original_name": "",
            }
        form = ActionPropertyForm(initial=initial)

    return render(
        request,
        "targets/action_form.html",
        {
            "form": form,
            "page_title": "Edit Action"
            if normalized_action_name
            else "Add Action",
            "is_edit": bool(normalized_action_name),
        },
    )


@login_required
@require_POST
def target_new_with_ai(request: HttpRequest) -> HttpResponse:
    env_text = str(request.POST.get("env_text", "") or "")
    sql_text = str(request.POST.get("sql_text", "") or "")
    hql_text = str(request.POST.get("hql_text", "") or "")

    if not str(env_text).strip():
        messages.error(request, ".env content is required.")
        return redirect("targets:list")
    if not str(sql_text).strip():
        messages.error(request, ".sql content is required.")
        return redirect("targets:list")

    initial, parse_error = _build_ai_prefill_initial(env_text, sql_text)
    if parse_error:
        messages.error(request, parse_error)
        return redirect("targets:list")

    request.session["ai_target_prefill"] = {
        "initial": initial,
        "hql_text": str(hql_text or "").replace("\r\n", "\n").replace("\r", "\n"),
    }
    return redirect(f"{reverse('targets:new')}?ai_prefill=1")


@login_required
def targets_list(request: HttpRequest) -> HttpResponse:
    rules_dir = _runtime_rules_dir()
    rules = rules_service.load_rules(rules_dir)
    rules_by_name = {
        str(rule.get("target_name") or "").strip().upper(): rule for rule in rules
    }

    if request.method == "POST":
        action = str(request.POST.get("action", "") or "").strip()
        change_notes = _parse_change_notes(request)
        selected_targets = _parse_selected_targets(request)
        if not selected_targets and action in {
            "duplicate_selected",
            "mute_until_selected",
            "deactivate_selected",
            "activate_selected",
            "unmute_selected",
        }:
            messages.error(request, "Select at least one target.")
            return redirect(_action_redirect_url(request))

        if action == "duplicate_selected":
            if len(selected_targets) != 1:
                messages.error(request, "Duplicate requires exactly one selected target.")
                return redirect(_action_redirect_url(request))
            source_name = selected_targets[0]
            rule = rules_by_name.get(source_name)
            if not rule:
                messages.error(request, f"Target not found: {source_name}")
                return redirect(_action_redirect_url(request))
            new_name = f"{source_name}_COPY"
            if (rules_dir / f"{new_name}.env").exists():
                messages.error(request, f"Duplicate already exists: {new_name}")
                return redirect(_action_redirect_url(request))
            try:
                _, sql_text = rules_service.load_rule_for_edit(rules_dir, source_name)
                rule_copy = copy.deepcopy(rule)
                rule_copy["rule_name"] = new_name
                rule_copy["target_name"] = new_name
                rule_copy["query_file"] = f"{new_name}.sql"
                rule_copy["is_active"] = False
                header = _build_rule_header(rule_copy, new_name)
                rules_service.save_rule(
                    rules_dir=rules_dir,
                    rule_name=new_name,
                    header=header,
                    mapping=rule_copy.get("mapping", {}),
                    metrics=rule_copy.get("metrics", []),
                    sql_text=sql_text,
                )
                try:
                    _save_target_audit_snapshot(new_name, request.user.username)
                except Exception:
                    pass
                messages.success(request, f"Duplicated target: {new_name}")
                return redirect("targets:edit", target_name=new_name)
            except Exception as exc:
                messages.error(request, f"Duplicate failed: {exc}")
                return redirect(_action_redirect_url(request))

        if action == "deactivate_selected":
            updated_names, failed = _apply_bulk_updates(
                rules_dir,
                selected_targets,
                {"IS_ACTIVE": "false"},
            )
            for updated_name in updated_names:
                try:
                    _save_target_audit_snapshot(
                        updated_name,
                        request.user.username,
                        change_notes=change_notes,
                    )
                except Exception:
                    pass
            updated = len(updated_names)
            if updated:
                messages.success(request, f"Deactivated {updated} target(s).")
            if failed:
                messages.error(request, f"Failed: {', '.join(failed)}")
            return redirect(_action_redirect_url(request))

        if action == "activate_selected":
            updated_names, failed = _apply_bulk_updates(
                rules_dir,
                selected_targets,
                {"IS_ACTIVE": "true"},
            )
            for updated_name in updated_names:
                try:
                    _save_target_audit_snapshot(
                        updated_name,
                        request.user.username,
                        change_notes=change_notes,
                    )
                except Exception:
                    pass
            updated = len(updated_names)
            if updated:
                messages.success(request, f"Activated {updated} target(s).")
            if failed:
                messages.error(request, f"Failed: {', '.join(failed)}")
            return redirect(_action_redirect_url(request))

        if action == "unmute_selected":
            updated_names, failed = _apply_bulk_updates(
                rules_dir,
                selected_targets,
                {
                    "IS_MUTED": "false",
                    "MUTE_UNTIL_ENABLED": "false",
                    "MUTE_UNTIL": "",
                },
            )
            for updated_name in updated_names:
                try:
                    _save_target_audit_snapshot(
                        updated_name,
                        request.user.username,
                        change_notes=change_notes,
                    )
                except Exception:
                    pass
            updated = len(updated_names)
            if updated:
                messages.success(request, f"Unmuted {updated} target(s).")
            if failed:
                messages.error(request, f"Failed: {', '.join(failed)}")
            return redirect(_action_redirect_url(request))

        if action == "mute_until_selected":
            mute_until_raw = str(request.POST.get("mute_until", "") or "").strip()
            try:
                mute_until_dt = datetime.fromisoformat(mute_until_raw)
            except ValueError:
                messages.error(request, "Mute until datetime is invalid.")
                return redirect(_action_redirect_url(request))
            updated_names, failed = _apply_bulk_updates(
                rules_dir,
                selected_targets,
                {
                    "MUTE_UNTIL_ENABLED": "true",
                    "MUTE_UNTIL": mute_until_dt.strftime("%Y-%m-%d %H:%M:%S"),
                },
            )
            audit_errors: list[str] = []
            for updated_name in updated_names:
                audit_error = _save_target_audit_snapshot_safe(
                    updated_name,
                    request.user.username,
                    change_notes=change_notes,
                )
                if audit_error:
                    audit_errors.append(f"{updated_name}: {audit_error}")
            updated = len(updated_names)
            if updated:
                messages.success(
                    request,
                    f"Muted {updated} target(s) until {mute_until_dt.strftime('%Y-%m-%d %H:%M:%S')}.",
                )
            if audit_errors:
                preview = "; ".join(audit_errors[:3])
                if len(audit_errors) > 3:
                    preview = f"{preview}; ..."
                messages.warning(request, f"Mute applied, but audit failed for: {preview}")
            if failed:
                messages.error(request, f"Failed: {', '.join(failed)}")
            return redirect(_action_redirect_url(request))

        return redirect(_action_redirect_url(request))

    f_target_name = str(request.GET.get("f_target_name", "")).strip().lower()
    f_description = str(request.GET.get("f_description", "")).strip().lower()
    f_schedule = str(request.GET.get("f_schedule", "")).strip().lower()
    f_datasource = str(request.GET.get("f_datasource", "")).strip()
    f_tags = str(request.GET.get("f_tags", "")).strip()
    f_active = str(request.GET.get("f_active", "")).strip().lower()
    f_muted = str(request.GET.get("f_muted", "")).strip().lower()
    f_severity = str(request.GET.get("f_severity", "")).strip().upper()
    f_state = str(request.GET.get("f_state", "")).strip()
    f_last_run = str(request.GET.get("f_last_run", "")).strip().lower()
    f_edited_by = str(request.GET.get("f_edited_by", "")).strip()
    f_edited_at = str(request.GET.get("f_edited_at", "")).strip().lower()
    sort_col = str(request.GET.get("sort_col", "target_name") or "").strip().lower()
    sort_dir = str(request.GET.get("sort_dir", "asc") or "").strip().lower()
    view_mode = str(request.GET.get("view_mode", "simple") or "").strip().lower()
    if view_mode not in {"simple", "detailed"}:
        view_mode = "simple"
    is_detailed_view = view_mode == "detailed"
    if sort_dir not in {"asc", "desc"}:
        sort_dir = "asc"

    names = [str(rule.get("target_name") or "") for rule in rules if rule.get("target_name")]
    runtime_states: dict[str, dict[str, Any]] = {}
    audits: dict[str, dict[str, Any]] = {}
    try:
        runtime_states = results_service.get_runtime_states(names)
        audits = results_service.get_rule_audit(names)
    except Exception as exc:
        messages.warning(request, f"data_store connection warning: {exc}")

    severity_rank = {
        "UNKNOWN": 0,
        "NORMAL": 1,
        "MINOR": 2,
        "MAJOR": 3,
        "CRITICAL": 4,
    }
    severity_values = {
        str(state.get("severity") or "UNKNOWN").strip().upper()
        for state in runtime_states.values()
    }
    if not severity_values:
        severity_values = {"UNKNOWN"}
    severity_options = sorted(
        severity_values,
        key=lambda value: (severity_rank.get(value, -1), value),
    )
    state_options = sorted(
        {
            str(state.get("state") or "").strip()
            for state in runtime_states.values()
            if str(state.get("state") or "").strip()
        },
        key=lambda value: value.lower(),
    )
    datasource_options = sorted(
        {
            str(rule.get("data_source") or "").strip()
            for rule in rules
            if str(rule.get("data_source") or "").strip()
        },
        key=lambda value: value.lower(),
    )
    tag_options = sorted(
        {
            str(tag or "").strip()
            for rule in rules
            for tag in (rule.get("tags") or [])
            if str(tag or "").strip()
        },
        key=lambda value: value.lower(),
    )
    edited_by_options = sorted(
        {
            str(item.get("last_edited_by") or "").strip()
            for item in audits.values()
            if str(item.get("last_edited_by") or "").strip()
        },
        key=lambda value: value.lower(),
    )

    f_datasource_lower = f_datasource.lower()
    f_tags_lower = f_tags.lower()
    f_edited_by_lower = f_edited_by.lower()

    filtered: list[dict[str, Any]] = []
    for rule in rules:
        target_name = str(rule.get("target_name") or "")
        description = str(rule.get("description") or "")
        data_source = str(rule.get("data_source") or "")
        schedule_cron = str(rule.get("schedule_cron") or "")
        tag_list = str(rule.get("tag_list") or "")
        runtime_state = runtime_states.get(target_name, {})
        severity = str(runtime_state.get("severity") or "").strip().upper()
        state_text = str(runtime_state.get("state") or "").strip().lower()
        dynamic_muted = rules_service.is_in_mute_policy(rule)
        effective_muted = bool(rule.get("is_muted") or dynamic_muted)

        if f_target_name and f_target_name not in target_name.lower():
            continue
        if f_description and f_description not in description.lower():
            continue
        if f_schedule and f_schedule not in schedule_cron.lower():
            continue
        if f_datasource and f_datasource_lower != data_source.lower():
            continue
        tag_values_lower = {
            str(tag or "").strip().lower()
            for tag in (rule.get("tags") or [])
            if str(tag or "").strip()
        }
        if f_tags and f_tags_lower not in tag_values_lower:
            continue
        if f_active in {"yes", "no"}:
            is_active = bool(rule.get("is_active"))
            if (f_active == "yes" and not is_active) or (f_active == "no" and is_active):
                continue
        if f_muted in {"yes", "no"}:
            if (f_muted == "yes" and not effective_muted) or (
                f_muted == "no" and effective_muted
            ):
                continue
        if f_severity and f_severity != severity:
            continue
        if f_state and f_state.lower() != state_text:
            continue
        last_run_text = str(runtime_state.get("last_run") or "").strip().lower()
        if f_last_run and f_last_run not in last_run_text:
            continue
        editor = str(audits.get(target_name, {}).get("last_edited_by") or "")
        edited_at = str(audits.get(target_name, {}).get("last_edited_at") or "")
        if f_edited_by and f_edited_by_lower != editor.lower():
            continue
        if f_edited_at and f_edited_at not in edited_at.lower():
            continue
        filtered.append(rule)

    rows: list[dict[str, Any]] = []
    for rule in filtered:
        target_name = str(rule.get("target_name") or "")
        state = runtime_states.get(target_name, {})
        audit = audits.get(target_name, {})
        dynamic_muted = rules_service.is_in_mute_policy(rule)
        rows.append(
            {
                "target_name": target_name,
                "description": str(rule.get("description") or ""),
                "schedule_cron": str(rule.get("schedule_cron") or ""),
                "data_source": str(rule.get("data_source") or ""),
                "tags": ", ".join(rule.get("tags", [])),
                "is_active": bool(rule.get("is_active")),
                "is_muted": bool(rule.get("is_muted") or dynamic_muted),
                "severity": str(state.get("severity") or "UNKNOWN"),
                "state": str(state.get("state") or ""),
                "last_run": str(state.get("last_run") or ""),
                "last_edited_by": str(audit.get("last_edited_by") or ""),
                "last_edited_at": str(audit.get("last_edited_at") or ""),
                "detail_url": reverse("targets:detail", args=[target_name]),
            }
        )

    sort_key_map = {
        "target_name": "target_name",
        "description": "description",
        "schedule_cron": "schedule_cron",
        "data_source": "data_source",
        "tags": "tags",
        "is_active": "is_active",
        "is_muted": "is_muted",
        "severity": "severity",
        "state": "state",
        "last_run": "last_run",
        "last_edited_by": "last_edited_by",
        "last_edited_at": "last_edited_at",
    }
    if sort_col not in sort_key_map:
        sort_col = "target_name"
    reverse_sort = sort_dir == "desc"

    if sort_col == "severity":
        rows.sort(
            key=lambda row: severity_rank.get(
                str(row.get("severity") or "").strip().upper(),
                -1,
            ),
            reverse=reverse_sort,
        )
    elif sort_col in {"is_active", "is_muted"}:
        rows.sort(
            key=lambda row: bool(row.get(sort_key_map[sort_col])),
            reverse=reverse_sort,
        )
    else:
        rows.sort(
            key=lambda row: str(row.get(sort_key_map[sort_col]) or "").lower(),
            reverse=reverse_sort,
        )

    ai_prompt_text, ai_prompt_file = _load_or_create_ai_prompt()
    filter_query = request.GET.urlencode()
    context = {
        "rows": rows,
        "f_target_name": f_target_name,
        "f_description": f_description,
        "f_schedule": f_schedule,
        "f_datasource": f_datasource,
        "f_tags": f_tags,
        "f_active": f_active,
        "f_muted": f_muted,
        "f_severity": f_severity,
        "f_state": f_state,
        "f_last_run": f_last_run,
        "f_edited_by": f_edited_by,
        "f_edited_at": f_edited_at,
        "severity_options": severity_options,
        "state_options": state_options,
        "datasource_options": datasource_options,
        "tag_options": tag_options,
        "edited_by_options": edited_by_options,
        "sort_col": sort_col,
        "sort_dir": sort_dir,
        "view_mode": view_mode,
        "is_detailed_view": is_detailed_view,
        "filter_query": filter_query,
        "ai_prompt_text": ai_prompt_text,
        "ai_prompt_file": ai_prompt_file,
    }
    if _is_htmx(request):
        return render(request, "partials/targets_grid.html", context)
    return render(request, "targets/index.html", context)


@login_required
def target_detail(request: HttpRequest, target_name: str) -> HttpResponse:
    rules_dir = _runtime_rules_dir()
    try:
        rule, sql_text = rules_service.load_rule_for_edit(rules_dir, target_name)
    except FileNotFoundError as exc:
        raise Http404(str(exc)) from exc

    history_metrics: list[str] = []
    history_rows: list[dict[str, Any]] = []
    audit_rows: list[dict[str, Any]] = []
    postgres_error = ""
    metric_name = str(request.GET.get("metric_name", "")).strip() or None
    try:
        audit_rows = results_service.get_rule_audit_history(target_name, limit=100)
    except Exception:
        audit_rows = []
    try:
        history_metrics = results_service.get_target_metrics(target_name)
        history_rows = results_service.get_status_history(
            target_name,
            metric_name=metric_name,
            limit=300,
        )
    except Exception as exc:
        postgres_error = str(exc)

    document_url_value = str(rule.get("document_url", "") or "").strip()
    dashboard_url_value = str(rule.get("dashboard_url", "") or "").strip()
    normalized_target_name = str(rule.get("target_name", "") or "").strip().upper()
    audit_rows = _decorate_audit_rows(normalized_target_name, audit_rows)

    context = {
        "rule": rule,
        "sql_text": sql_text,
        "history_metrics": history_metrics,
        "selected_metric": metric_name or "",
        "history_rows": history_rows,
        "audit_rows": audit_rows,
        "postgres_error": postgres_error,
        "document_url_value": document_url_value,
        "dashboard_url_value": dashboard_url_value,
        "history_import_sql": _load_history_import_sql(normalized_target_name),
    }
    return render(request, "targets/detail.html", context)


@login_required
def target_edit(request: HttpRequest, target_name: str | None = None) -> HttpResponse:
    rules_dir = _runtime_rules_dir()
    rules_dir.mkdir(parents=True, exist_ok=True)
    datasource_choices = _load_datasource_choices()
    ai_prefill_requested = str(request.GET.get("ai_prefill", "") or "").strip().lower() in {
        "1",
        "true",
        "yes",
    }

    initial: dict[str, Any] = {}
    page_title = "Create Target"
    edit_target_name = str(target_name or "").strip().upper()
    if target_name:
        try:
            rule, sql_text = rules_service.load_rule_for_edit(rules_dir, target_name)
        except FileNotFoundError as exc:
            raise Http404(str(exc)) from exc
        initial = rules_service.rule_to_form_initial(rule, sql_text)
        page_title = f"Edit Target - {target_name}"
    else:
        if ai_prefill_requested:
            ai_prefill_data = request.session.get("ai_target_prefill")
            if isinstance(ai_prefill_data, dict):
                prefill_initial = ai_prefill_data.get("initial")
                if isinstance(prefill_initial, dict):
                    initial = copy.deepcopy(prefill_initial)
        if not initial:
            initial = rules_service.rule_to_form_initial(
                rules_service.empty_rule(),
                "",
            )
        if not ai_prefill_requested:
            request.session.pop("ai_target_prefill", None)

    selected_metric = str(request.GET.get("history_metric", "") or "").strip()
    if request.method == "POST":
        change_notes = _parse_change_notes(request)
        form = TargetRuleForm(request.POST, datasource_choices=datasource_choices)
        if form.is_valid():
            cleaned = form.cleaned_data
            rule_name = str(cleaned.get("target_name", "")).strip().upper()
            header = {
                "TARGET_NAME": rule_name,
                "DESCRIPTION": str(cleaned.get("description", "")).strip(),
                "DOCUMENT_URL": str(cleaned.get("document_url", "")).strip(),
                "DASHBOARD_URL": str(cleaned.get("dashboard_url", "")).strip(),
                "TAG_LIST": str(cleaned.get("tag_list", "")).strip(),
                "IS_ACTIVE": "true" if cleaned.get("is_active") else "false",
                "IS_MUTED": "true" if cleaned.get("is_muted") else "false",
                "DATA_SOURCE": str(cleaned.get("data_source", "")).strip(),
                "SQL_TIMEOUT_SEC": str(cleaned.get("sql_timeout_sec", 0)),
                "SQL_JITTER_SEC": str(cleaned.get("sql_jitter_sec", 0)),
                "SQL_MODE": str(cleaned.get("sql_mode", "single")).strip().lower(),
                "SCHEDULE_CRON": str(cleaned.get("schedule_cron", "")).strip(),
                "MUTE_BETWEEN_ENABLED": "true"
                if cleaned.get("mute_between_enabled")
                else "false",
                "MUTE_BETWEEN_RULES": str(cleaned.get("mute_between_rules", "")).strip(),
                "MUTE_UNTIL_ENABLED": "true" if cleaned.get("mute_until_enabled") else "false",
                "MUTE_UNTIL": str(cleaned.get("mute_until", "")).strip(),
                "QUERY_FILE": f"{rule_name}.sql",
            }
            mapping = cleaned.get("mapping", {})
            metrics = cleaned.get("metrics", [])
            sql_text = str(cleaned.get("sql_text", "") or "")

            errors = rules_service.validate_rule_data(
                rule_name=rule_name,
                header=header,
                mapping=mapping,
                metrics=metrics,
                sql_text=sql_text,
            )
            if errors:
                for err in errors:
                    form.add_error(None, err)
            else:
                original_name = str(cleaned.get("original_name", "")).strip().upper()
                original_query_file = str(cleaned.get("original_query_file", "")).strip()
                try:
                    saved_name = rules_service.save_rule(
                        rules_dir=rules_dir,
                        rule_name=rule_name,
                        header=header,
                        mapping=mapping,
                        metrics=metrics,
                        sql_text=sql_text,
                        original_name=original_name,
                        original_query_file=original_query_file,
                    )
                except Exception as exc:
                    form.add_error(None, f"Save failed: {exc}")
                else:
                    if not target_name:
                        ai_prefill_data = request.session.get("ai_target_prefill")
                        if isinstance(ai_prefill_data, dict):
                            hql_text = str(ai_prefill_data.get("hql_text", "") or "")
                            if hql_text.strip():
                                try:
                                    storage_sql.write_sql(
                                        _history_import_sql_path(saved_name),
                                        hql_text,
                                    )
                                except Exception:
                                    pass
                            request.session.pop("ai_target_prefill", None)
                    try:
                        _save_target_audit_snapshot(
                            saved_name,
                            request.user.username,
                            change_notes=change_notes,
                        )
                    except Exception:
                        pass
                    messages.success(request, f"Target saved: {saved_name}")
                    return redirect("targets:detail", target_name=saved_name)
    else:
        form = TargetRuleForm(initial=initial, datasource_choices=datasource_choices)

    audit_rows: list[dict[str, Any]] = []
    history_rows: list[dict[str, Any]] = []
    history_metrics: list[str] = []
    postgres_error = ""
    if edit_target_name:
        try:
            audit_rows = results_service.get_rule_audit_history(edit_target_name, limit=100)
        except Exception:
            audit_rows = []
        try:
            history_metrics = results_service.get_target_metrics(edit_target_name)
            if selected_metric and selected_metric not in history_metrics:
                selected_metric = ""
            selected_metric_name = selected_metric if selected_metric else None
            history_rows = results_service.get_status_history(
                edit_target_name,
                metric_name=selected_metric_name,
                limit=300,
            )
        except Exception as exc:
            postgres_error = str(exc)
    audit_rows = _decorate_audit_rows(edit_target_name, audit_rows)

    document_url_value = str(form["document_url"].value() or "").strip()
    dashboard_url_value = str(form["dashboard_url"].value() or "").strip()
    action_options = list(
        property_store.get_property_map(
            "actions.properties",
            _action_properties_path(),
        ).keys()
    )

    return render(
        request,
        "targets/form.html",
        {
            "form": form,
            "page_title": page_title,
            "is_edit": bool(target_name),
            "target_name": target_name or "",
            "edit_target_name": edit_target_name,
            "audit_rows": audit_rows,
            "history_rows": history_rows,
            "history_metrics": history_metrics,
            "selected_metric": selected_metric,
            "postgres_error": postgres_error,
            "document_url_value": document_url_value,
            "dashboard_url_value": dashboard_url_value,
            "action_options": action_options,
            "history_import_sql": _load_history_import_sql(edit_target_name),
        },
    )


@login_required
@require_GET
def target_audit_view(
    request: HttpRequest,
    target_name: str,
    audit_id: int,
) -> HttpResponse:
    normalized_target_name = str(target_name or "").strip().upper()
    entry = results_service.get_target_audit_entry(normalized_target_name, int(audit_id))
    if not entry:
        raise Http404("Audit entry not found.")
    return render(
        request,
        "targets/audit_view.html",
        {
            "target_name": normalized_target_name,
            "entry": entry,
        },
    )


@login_required
@require_GET
def target_audit_changes(
    request: HttpRequest,
    target_name: str,
    audit_id: int,
) -> HttpResponse:
    normalized_target_name = str(target_name or "").strip().upper()
    entry = results_service.get_target_audit_entry(normalized_target_name, int(audit_id))
    if not entry:
        raise Http404("Audit entry not found.")
    previous = results_service.get_previous_target_audit_entry(
        normalized_target_name,
        int(audit_id),
    )

    if previous:
        previous_env = previous.get("env_content", "")
        previous_sql = previous.get("sql_content", "")
        previous_hql = previous.get("hql_content", "")
        env_diff = _build_unified_file_diff(
            previous_env,
            entry.get("env_content", ""),
            f"{normalized_target_name}.env",
        )
        sql_diff = _build_unified_file_diff(
            previous_sql,
            entry.get("sql_content", ""),
            f"{normalized_target_name}.sql",
        )
        hql_diff = _build_unified_file_diff(
            previous_hql,
            entry.get("hql_content", ""),
            f"{normalized_target_name}.hql",
        )
    else:
        env_diff = "No previous save to compare."
        sql_diff = "No previous save to compare."
        hql_diff = "No previous save to compare."

    return render(
        request,
        "targets/audit_changes.html",
        {
            "target_name": normalized_target_name,
            "entry": entry,
            "previous": previous,
            "env_diff": env_diff,
            "sql_diff": sql_diff,
            "hql_diff": hql_diff,
        },
    )


@login_required
@require_POST
def target_audit_restore(
    request: HttpRequest,
    target_name: str,
    audit_id: int,
) -> JsonResponse:
    normalized_target_name = str(target_name or "").strip().upper()
    entry = results_service.get_target_audit_entry(normalized_target_name, int(audit_id))
    if not entry:
        raise Http404("Audit entry not found.")
    env_content = str(entry.get("env_content", "") or "")
    sql_content = str(entry.get("sql_content", "") or "")
    hql_content = str(entry.get("hql_content", "") or "")
    if not env_content and not sql_content and not hql_content:
        return JsonResponse(
            {"ok": False, "error": "Snapshot content is not available for this entry."},
            status=400,
        )
    try:
        _write_snapshot_to_files(
            normalized_target_name,
            env_content,
            sql_content,
            hql_content,
        )
        _save_target_audit_snapshot(normalized_target_name, request.user.username)
    except Exception as exc:
        return JsonResponse({"ok": False, "error": f"Restore failed: {exc}"}, status=500)
    return JsonResponse({"ok": True, "message": "Restore completed."})


@login_required
@require_GET
def target_history_instances(request: HttpRequest, target_name: str) -> JsonResponse:
    rules_dir = _runtime_rules_dir()
    try:
        rule, _ = rules_service.load_rule_for_edit(rules_dir, target_name)
    except FileNotFoundError as exc:
        raise Http404(str(exc)) from exc
    normalized_target_name = str(rule.get("target_name", "") or "").strip().upper()
    try:
        rows = results_service.get_result_instances(normalized_target_name, limit=1000)
    except Exception as exc:
        return JsonResponse({"ok": False, "error": str(exc)}, status=500)
    return JsonResponse({"ok": True, "rows": rows})


@login_required
@require_POST
def target_history_run_normal_actions(
    request: HttpRequest,
    target_name: str,
) -> JsonResponse:
    rules_dir = _runtime_rules_dir()
    try:
        rule, _ = rules_service.load_rule_for_edit(rules_dir, target_name)
    except FileNotFoundError as exc:
        raise Http404(str(exc)) from exc

    normalized_target_name = str(rule.get("target_name", "") or "").strip().upper()
    normal_action_by_metric: dict[str, str] = {}
    configured_metrics: list[str] = []
    for metric in rule.get("metrics", []):
        if not isinstance(metric, dict):
            continue
        metric_name = str(metric.get("NAME", "") or "").strip().upper()
        if not metric_name:
            continue
        if metric_name not in configured_metrics:
            configured_metrics.append(metric_name)
        normal_action = str(metric.get("NORMAL_ACTION", "") or "").strip().upper()
        if normal_action:
            normal_action_by_metric[metric_name] = normal_action

    selected_metrics = _parse_history_metric_selection(
        str(request.POST.get("metrics", "") or "")
    )
    run_all_metrics = not selected_metrics or "ALL" in selected_metrics
    if run_all_metrics:
        selected_metrics = [*configured_metrics]
    selected_metrics = [
        metric_name
        for metric_name in selected_metrics
        if metric_name in configured_metrics
    ]
    if not selected_metrics:
        return JsonResponse(
            {"ok": False, "error": "No configured metrics found for this target."},
            status=400,
        )

    action_map = property_store.get_property_map(
        "actions.properties",
        _action_properties_path(),
    )
    action_map_upper = {
        str(key).strip().upper(): str(value or "").strip()
        for key, value in action_map.items()
    }

    latest_by_metric = results_service.get_latest_metric_results(
        normalized_target_name,
        selected_metrics,
    )

    timeout_sec = int(rule.get("sql_timeout_sec", 60) or 60)
    timeout_sec = max(5, min(timeout_sec, 900))
    action_root = rules_dir.parent
    message_text = str(request.POST.get("message", "") or "")

    executed = 0
    success = 0
    failed = 0
    skipped = 0
    details: list[dict[str, Any]] = []
    output_chunks: list[str] = []

    for selected_metric in selected_metrics:
        result: dict[str, Any] = {
            "metric_name": selected_metric,
            "status": "skipped",
            "detail": "",
            "stdout": "",
            "stderr": "",
        }
        latest = latest_by_metric.get(selected_metric)
        if not latest:
            result["detail"] = "No runtime metric row found."
            skipped += 1
            details.append(result)
            continue

        try:
            severity_num = int(latest.get("severity"))
        except (TypeError, ValueError):
            severity_num = 0
        if severity_num == 1:
            result["detail"] = "Current severity is already NORMAL."
            skipped += 1
            details.append(result)
            continue

        normal_action = normal_action_by_metric.get(selected_metric, "")
        if not normal_action:
            result["detail"] = "NORMAL_ACTION is not configured for metric."
            skipped += 1
            details.append(result)
            continue

        script_ref = action_map_upper.get(normal_action, "")
        if not script_ref:
            result["detail"] = f"Action is not defined: {normal_action}"
            skipped += 1
            details.append(result)
            continue

        script_path = Path(script_ref)
        if not script_path.is_absolute():
            script_path = (action_root / script_path).resolve()
        if not script_path.exists() or not script_path.is_file():
            result["detail"] = f"Script not found: {script_path}"
            skipped += 1
            details.append(result)
            continue

        metric_name = str(latest.get("metric_name") or selected_metric)
        metric_value = latest.get("metric_value")
        args = [
            str(script_path),
            normalized_target_name,
            metric_name,
            "" if metric_value is None else str(metric_value),
            "1",
            message_text,
        ]
        executed += 1
        try:
            proc = subprocess.run(
                args,
                capture_output=True,
                text=True,
                timeout=timeout_sec,
            )
            result["stdout"] = str(proc.stdout or "")
            result["stderr"] = str(proc.stderr or "")
            if int(proc.returncode) == 0:
                result["status"] = "success"
                result["detail"] = "OK"
                success += 1
            else:
                result["status"] = "failed"
                err_text = str(proc.stderr or "").strip()
                if not err_text:
                    err_text = str(proc.stdout or "").strip()
                if not err_text:
                    err_text = f"exit status {proc.returncode}"
                result["detail"] = err_text
                failed += 1
            result["exit_status"] = int(proc.returncode)
        except Exception as exc:
            result["status"] = "failed"
            result["detail"] = str(exc)
            failed += 1
        details.append(result)

        output_lines: list[str] = [
            f"Metric: {result['metric_name']}",
            f"Status: {result['status']}",
        ]
        exit_status = result.get("exit_status")
        if exit_status is not None:
            output_lines.append(f"Exit: {exit_status}")
        detail_text = str(result.get("detail", "") or "").strip()
        if detail_text:
            output_lines.append(f"Detail: {detail_text}")
        stdout_text = str(result.get("stdout", "") or "")
        stderr_text = str(result.get("stderr", "") or "")
        if stdout_text.strip():
            output_lines.append("STDOUT:")
            output_lines.append(stdout_text.rstrip("\n"))
        if stderr_text.strip():
            output_lines.append("STDERR:")
            output_lines.append(stderr_text.rstrip("\n"))
        output_chunks.append("\n".join(output_lines))

    return JsonResponse(
        {
            "ok": True,
            "summary": {
                "requested": len(selected_metrics),
                "executed": executed,
                "success": success,
                "failed": failed,
                "skipped": skipped,
            },
            "results": details,
            "output_text": "\n\n---\n\n".join(output_chunks),
        }
    )


@login_required
@require_POST
def target_history_delete(request: HttpRequest, target_name: str) -> JsonResponse:
    rules_dir = _runtime_rules_dir()
    try:
        rule, _ = rules_service.load_rule_for_edit(rules_dir, target_name)
    except FileNotFoundError as exc:
        raise Http404(str(exc)) from exc
    normalized_target_name = str(rule.get("target_name", "") or "").strip().upper()
    try:
        deleted = results_service.delete_target_history(normalized_target_name)
    except Exception as exc:
        return JsonResponse({"ok": False, "error": str(exc)}, status=500)
    return JsonResponse({"ok": True, "deleted": deleted})


@login_required
@require_POST
def target_history_import(request: HttpRequest, target_name: str) -> JsonResponse:
    rules_dir = _runtime_rules_dir()
    try:
        rule, _ = rules_service.load_rule_for_edit(rules_dir, target_name)
    except FileNotFoundError as exc:
        raise Http404(str(exc)) from exc

    normalized_target_name = str(rule.get("target_name", "") or "").strip().upper()
    sql_text = str(request.POST.get("sql", "") or "")
    validation_errors = validators.validate_read_only_sql(sql_text, "SQL")
    if validation_errors:
        return JsonResponse(
            {"ok": False, "error": "Validation failed.", "errors": validation_errors},
            status=400,
        )

    target_datasource = str(rule.get("data_source", "") or "").strip()
    if not target_datasource:
        return JsonResponse(
            {"ok": False, "error": "Target datasource is not set."},
            status=400,
        )

    datasource_content = property_store.get_property_content(
        "datasources.properties",
        None,
    )
    max_import_rows = 20000
    columns, source_rows, truncated, run_errors = explore_service.run_query(
        app_root=_runtime_app_root(),
        datasource_name=target_datasource,
        sql_text=sql_text,
        datasource_definition_text=datasource_content,
        datasource_definition_file=None,
        timeout_sec=120,
        max_rows=max_import_rows,
    )
    if run_errors:
        return JsonResponse(
            {"ok": False, "error": run_errors[0], "errors": run_errors},
            status=400,
        )
    if truncated:
        return JsonResponse(
            {
                "ok": False,
                "error": f"Query returned more than {max_import_rows} rows. Narrow the query and retry.",
            },
            status=400,
        )
    if not source_rows:
        return JsonResponse(
            {"ok": False, "error": "Query returned no rows."},
            status=400,
        )

    import_rows, import_error = _build_history_import_rows(
        normalized_target_name,
        columns,
        source_rows,
    )
    if import_error:
        return JsonResponse({"ok": False, "error": import_error}, status=400)

    try:
        inserted = results_service.insert_import_rows(import_rows)
        storage_sql.write_sql(_history_import_sql_path(normalized_target_name), sql_text)
        _save_target_audit_snapshot(normalized_target_name, request.user.username)
    except Exception as exc:
        return JsonResponse({"ok": False, "error": f"Import failed: {exc}"}, status=500)
    return JsonResponse({"ok": True, "inserted": inserted})


@login_required
def explore(request: HttpRequest) -> HttpResponse:
    datasource_content = property_store.get_property_content(
        "datasources.properties",
        None,
    )
    saved_dir = _runtime_saved_queries_dir()

    def _sorted_unique(values: list[str]) -> list[str]:
        dedup: list[str] = []
        for value in values:
            item = str(value or "").strip()
            if item and item not in dedup:
                dedup.append(item)
        dedup.sort(key=lambda item: item.lower())
        return dedup

    configured_datasources = _load_datasource_choices()
    saved_query_entries = explore_service.list_saved_query_entries(saved_dir)
    datasource_choices = _sorted_unique(
        configured_datasources + [item.get("datasource", "") for item in saved_query_entries]
    )
    saved_query_choices = _sorted_unique([item.get("name", "") for item in saved_query_entries])
    selected_datasource = ""
    selected_saved_query = ""
    sql_text = ""
    columns: list[str] = []
    rows: list[list[Any]] = []
    run_errors: list[str] = []
    query_executed = False

    if request.method == "POST":
        action = str(request.POST.get("action", "") or "").strip().lower()
        if not action:
            action = str(request.POST.get("action_hint", "") or "").strip().lower()
        selected_datasource = str(request.POST.get("datasource", "") or "").strip()
        selected_saved_query = str(request.POST.get("saved_query", "") or "").strip()
        sql_text = str(request.POST.get("sql_text", "") or "")
        if action == "load_saved":
            if selected_saved_query:
                sql_text = explore_service.load_saved_query(
                    saved_dir,
                    selected_saved_query,
                )
                if not sql_text:
                    messages.warning(
                        request,
                        f"Saved query could not be loaded or is empty: {selected_saved_query}",
                    )
        else:
            if action == "save_query":
                query_name = str(request.POST.get("query_name", "") or "").strip().upper()
                if not query_name:
                    run_errors.append("Query name is required.")
                run_errors.extend(
                    validators.validate_target_name_upper_allow_dot(query_name)
                )
                run_errors.extend(
                    validators.validate_read_only_sql(sql_text, "SQL")
                )
                if not selected_datasource:
                    run_errors.append("Datasource is required to save a query.")
                if not run_errors:
                    explore_service.save_query(
                        saved_dir=saved_dir,
                        query_name=query_name,
                        sql_text=sql_text,
                        datasource_name=selected_datasource,
                        created_by=request.user.username,
                    )
                    messages.success(request, f"Saved query: {query_name}")
                    selected_saved_query = query_name
                    saved_query_entries = explore_service.list_saved_query_entries(saved_dir)
                    datasource_choices = _sorted_unique(
                        configured_datasources
                        + [item.get("datasource", "") for item in saved_query_entries]
                    )
                    saved_query_choices = _sorted_unique(
                        [item.get("name", "") for item in saved_query_entries]
                    )
            elif action == "run_query":
                query_executed = True
                if not selected_datasource:
                    run_errors.append("Select a datasource to run query.")
                else:
                    max_explore_rows = 1000
                    columns, raw_rows, truncated, run_errors = explore_service.run_query(
                        app_root=_runtime_app_root(),
                        datasource_name=selected_datasource,
                        sql_text=sql_text,
                        datasource_definition_text=datasource_content,
                        datasource_definition_file=None,
                        timeout_sec=120,
                        max_rows=max_explore_rows,
                    )
                    if raw_rows:
                        rows = [list(row) for row in raw_rows]
                    if truncated:
                        messages.info(request, f"Showing first {max_explore_rows} rows.")

    if selected_datasource and selected_datasource not in datasource_choices:
        datasource_choices = _sorted_unique(datasource_choices + [selected_datasource])
    if selected_saved_query and selected_saved_query not in saved_query_choices:
        saved_query_choices = _sorted_unique(saved_query_choices + [selected_saved_query])

    form = ExploreForm(
        initial={
            "datasource": selected_datasource,
            "saved_query": selected_saved_query,
            "query_name": "",
            "sql_text": sql_text,
        },
        datasource_choices=datasource_choices,
        saved_query_choices=saved_query_choices,
    )

    return render(
        request,
        "targets/explore.html",
        {
            "form": form,
            "columns": columns,
            "rows": rows,
            "run_errors": run_errors,
            "query_executed": query_executed,
            "explore_page_size": 100,
        },
    )


@login_required
def logs_page(request: HttpRequest) -> HttpResponse:
    properties = _load_application_properties()

    raw_line_count = str(request.GET.get("lines", "1000") or "1000").strip()
    try:
        line_count = int(raw_line_count)
    except ValueError:
        line_count = 1000
    line_count = max(1, min(line_count, 20000))

    active_tab = str(request.GET.get("tab", "engine") or "engine").strip().lower()
    if active_tab not in {"engine", "ui"}:
        active_tab = "engine"

    download_choice = str(request.GET.get("download_choice", active_tab) or active_tab).strip().lower()
    if download_choice not in {"engine", "ui"}:
        download_choice = "engine"

    engine_text, engine_path, engine_err = _read_log_file(
        str(properties.get("ENGINE_LOG_FILE", "") or "")
    )
    ui_text, ui_path, ui_err = _read_log_file(
        str(properties.get("UI_LOG_FILE", "") or "")
    )

    if str(request.GET.get("action", "") or "").strip().lower() == "download":
        if download_choice == "ui":
            download_text = ui_text
            download_path = ui_path
            fallback_name = "ui.log"
        else:
            download_text = engine_text
            download_path = engine_path
            fallback_name = "engine.log"
        download_name = Path(download_path).name if download_path else fallback_name
        response = HttpResponse(
            download_text or "",
            content_type="text/plain; charset=utf-8",
        )
        response["Content-Disposition"] = f'attachment; filename="{download_name}"'
        return response

    engine_view = _reverse_lines(_tail_log_lines(engine_text, line_count))
    ui_view = _tail_log_lines(ui_text, line_count)
    now_local = timezone.localtime(
        timezone.now(),
        timezone.get_default_timezone(),
    )
    tz_offset = now_local.strftime("%z")
    tz_offset = (
        f"{tz_offset[:3]}:{tz_offset[3:]}"
        if len(tz_offset) == 5
        else tz_offset
    )
    tz_name = str(timezone.get_default_timezone_name() or "").strip()
    system_tz = f"{tz_name} ({tz_offset})".strip()
    host_name = str(socket.gethostname() or "").strip() or "-"
    host_ips = _server_ipv4_addresses()

    return render(
        request,
        "targets/logs.html",
        {
            "line_count": line_count,
            "active_tab": active_tab,
            "download_choice": download_choice,
            "engine_path": engine_path,
            "engine_error": engine_err,
            "engine_view": engine_view,
            "engine_has_text": bool(engine_text),
            "ui_path": ui_path,
            "ui_error": ui_err,
            "ui_view": ui_view,
            "ui_has_text": bool(ui_text),
            "system_now": now_local.strftime("%Y-%m-%d %H:%M:%S"),
            "system_tz": system_tz,
            "system_hostname": host_name,
            "system_ips": host_ips,
            "system_pid": int(os.getpid()),
            "system_platform": platform.platform(),
            "system_python": platform.python_version(),
        },
    )


@login_required
def help_page(request: HttpRequest) -> HttpResponse:
    return render(request, "targets/help.html")


HELP_TOPIC_TEMPLATES: dict[str, str] = {
    "config": "targets/help_config.html",
    "sql": "targets/help_sql.html",
    "metrics": "targets/help_metrics.html",
    "audit": "targets/help_audit.html",
    "history": "targets/help_history.html",
    "engine": "targets/help_engine.html",
    "alarm-planning": "targets/help_alarm_planning.html",
    "ai-migration": "targets/help_ai_migration.html",
}


@login_required
def help_topic_page(request: HttpRequest, topic: str) -> HttpResponse:
    key = str(topic or "").strip().lower()
    template_name = HELP_TOPIC_TEMPLATES.get(key, "")
    if not template_name:
        raise Http404(f"Help topic not found: {topic}")
    return render(
        request,
        template_name,
        {
            "help_topic": key,
        },
    )


@login_required
def test_actions(request: HttpRequest) -> HttpResponse:
    action_map = property_store.get_property_map(
        "actions.properties",
        _action_properties_path(),
    )
    action_names = sorted(action_map.keys())

    selected_action = str(request.POST.get("action_name", "") or "").strip().upper()
    if action_names and selected_action not in action_names:
        selected_action = action_names[0]
    if not selected_action and action_names:
        selected_action = action_names[0]

    target_name = str(request.POST.get("target_name", "") or "").strip()
    metric_name = str(request.POST.get("metric_name", "") or "").strip()
    metric_value = str(request.POST.get("metric_value", "") or "").strip()
    severity = str(request.POST.get("severity", "1") or "1").strip()
    message = str(request.POST.get("message", "") or "").strip()
    timeout_raw = str(request.POST.get("timeout_sec", "60") or "60").strip()
    try:
        timeout_sec = int(timeout_raw)
    except ValueError:
        timeout_sec = 60
    timeout_sec = max(5, min(timeout_sec, 900))

    script_path = str(action_map.get(selected_action, "") or "").strip()

    resolved_script_path = ""
    exit_status: int | None = None
    stdout_text = ""
    stderr_text = ""
    run_error = ""

    if request.method == "POST":
        if not action_names:
            run_error = "No configured actions found in actions.properties."
        elif not script_path:
            run_error = "Action script is not set."
        else:
            resolved = Path(script_path)
            if not resolved.is_absolute():
                resolved = (_runtime_app_root() / resolved).resolve()
            resolved_script_path = str(resolved)
            if not resolved.exists() or not resolved.is_file():
                run_error = f"Script not found: {resolved_script_path}"
            else:
                args = [
                    resolved_script_path,
                    target_name,
                    metric_name,
                    metric_value,
                    severity,
                    message,
                ]
                try:
                    result = subprocess.run(
                        args,
                        capture_output=True,
                        text=True,
                        timeout=timeout_sec,
                    )
                    exit_status = int(result.returncode)
                    stdout_text = str(result.stdout or "")
                    stderr_text = str(result.stderr or "")
                except Exception as exc:
                    run_error = f"Failed to run action: {exc}"

    return render(
        request,
        "targets/test_actions.html",
        {
            "action_names": action_names,
            "selected_action": selected_action,
            "script_path": script_path,
            "target_name_value": target_name,
            "metric_name_value": metric_name,
            "metric_value_value": metric_value,
            "severity_value": severity or "1",
            "message_value": message,
            "timeout_sec_value": timeout_sec,
            "resolved_script_path": resolved_script_path,
            "exit_status": exit_status,
            "stdout_text": stdout_text,
            "stderr_text": stderr_text,
            "run_error": run_error,
        },
    )
