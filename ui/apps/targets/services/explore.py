from __future__ import annotations

import os
from pathlib import Path
import threading

import anbu_validators as validators
from django.db.utils import OperationalError, ProgrammingError
from eva.db import Datasource, execute_target_sql_multi_limited
import storage_sql

from apps.targets.models import SavedQuery

from .property_store import parse_kv_text, read_file_text

def _read_max_concurrency() -> int:
    raw = str(os.getenv("ANBU_ADHOC_QUERY_MAX_CONCURRENCY", "4") or "4").strip()
    try:
        parsed = int(raw)
    except ValueError:
        parsed = 4
    return max(1, parsed)


_ADHOC_QUERY_MAX_CONCURRENCY = _read_max_concurrency()
_ADHOC_QUERY_SLOT = threading.BoundedSemaphore(_ADHOC_QUERY_MAX_CONCURRENCY)


def _parse_kv_file(path: Path) -> dict[str, str]:
    return parse_kv_text(read_file_text(path))


def _resolve_ref_path(app_root: Path, ref: str) -> Path:
    path = Path(ref)
    if not path.is_absolute():
        path = (app_root / path).resolve()
    return path


def load_datasource_map(
    app_root: Path,
    datasource_definition_text: str = "",
    datasource_definition_file: Path | None = None,
) -> dict[str, Datasource]:
    refs: dict[str, str] = {}
    base_dir = app_root
    if datasource_definition_text.strip():
        refs = parse_kv_text(datasource_definition_text)
    elif datasource_definition_file:
        refs = _parse_kv_file(datasource_definition_file)
        base_dir = datasource_definition_file.parent

    ds_map: dict[str, Datasource] = {}
    for ds_name, ref in refs.items():
        cfg_path = _resolve_ref_path(base_dir, ref)
        cfg = _parse_kv_file(cfg_path)
        ds_type = cfg.get("TYPE", "").strip().upper()
        user = cfg.get("USER", "").strip()
        password = cfg.get("PASSWORD", "")
        dsn = cfg.get("DSN", "").strip()
        if not (ds_type and user and dsn):
            continue
        ds_map[ds_name] = Datasource(
            name=ds_name,
            type=ds_type,
            user=user,
            password=password,
            dsn=dsn,
        )
    return ds_map


def list_datasource_names(
    datasource_definition_text: str = "",
    datasource_definition_file: Path | None = None,
) -> list[str]:
    if datasource_definition_text.strip():
        refs = parse_kv_text(datasource_definition_text)
        return sorted(refs.keys())
    if datasource_definition_file:
        refs = _parse_kv_file(datasource_definition_file)
        return sorted(refs.keys())
    return []


def _fallback_saved_query_entries(saved_dir: Path) -> list[dict[str, str]]:
    if not saved_dir.exists():
        return []
    entries: list[dict[str, str]] = []
    for path in sorted(saved_dir.glob("*.sql")):
        entries.append(
            {
                "name": path.stem,
                "datasource": "",
                "created_by": "UNKNOWN",
            }
        )
    return entries


def list_saved_query_entries(saved_dir: Path) -> list[dict[str, str]]:
    try:
        rows = list(
            SavedQuery.objects.using("default")
            .order_by("name")
            .values("name", "datasource", "created_by")
        )
    except (OperationalError, ProgrammingError):
        rows = []

    entries: list[dict[str, str]] = []
    for row in rows:
        name = str(row.get("name") or "").strip().upper()
        if not name:
            continue
        entries.append(
            {
                "name": name,
                "datasource": str(row.get("datasource") or "").strip(),
                "created_by": str(row.get("created_by") or "").strip() or "UNKNOWN",
            }
        )
    if entries:
        return entries
    return _fallback_saved_query_entries(saved_dir)


def list_saved_queries(
    saved_dir: Path,
    datasource_name: str = "",
    created_by: str = "",
) -> list[str]:
    ds_filter = str(datasource_name or "").strip()
    author_filter = str(created_by or "").strip()
    names: list[str] = []
    for item in list_saved_query_entries(saved_dir):
        if ds_filter and str(item.get("datasource") or "").strip() != ds_filter:
            continue
        if author_filter and str(item.get("created_by") or "").strip() != author_filter:
            continue
        name = str(item.get("name") or "").strip()
        if name and name not in names:
            names.append(name)
    return sorted(names)


def load_saved_query(saved_dir: Path, query_name: str, datasource_name: str = "") -> str:
    normalized_name = str(query_name or "").strip().upper()
    if not normalized_name:
        return ""
    ds_filter = str(datasource_name or "").strip()
    try:
        queryset = SavedQuery.objects.using("default").filter(name=normalized_name)
        if ds_filter:
            queryset = queryset.filter(datasource=ds_filter)
        row = queryset.values("sql_text").first()
        if row is not None:
            return str(row.get("sql_text") or "")
    except (OperationalError, ProgrammingError):
        pass
    path = saved_dir / f"{normalized_name}.sql"
    if not path.exists():
        return ""
    return storage_sql.read_sql(path)


def save_query(
    saved_dir: Path,
    query_name: str,
    sql_text: str,
    datasource_name: str,
    created_by: str,
) -> None:
    saved_dir.mkdir(parents=True, exist_ok=True)
    normalized_name = str(query_name or "").strip().upper()
    normalized_ds = str(datasource_name or "").strip()
    normalized_user = str(created_by or "").strip() or "UNKNOWN"
    try:
        existing = (
            SavedQuery.objects.using("default")
            .filter(name=normalized_name)
            .first()
        )
    except (OperationalError, ProgrammingError):
        existing = None
    if existing is None:
        try:
            SavedQuery.objects.using("default").create(
                name=normalized_name,
                datasource=normalized_ds,
                created_by=normalized_user,
                sql_text=str(sql_text or ""),
            )
        except (OperationalError, ProgrammingError):
            pass
    else:
        existing.datasource = normalized_ds
        existing.sql_text = str(sql_text or "")
        if not str(existing.created_by or "").strip():
            existing.created_by = normalized_user
        try:
            existing.save(update_fields=["datasource", "sql_text", "created_by", "updated_at"])
        except (OperationalError, ProgrammingError):
            pass
    storage_sql.write_sql(saved_dir / f"{normalized_name}.sql", sql_text or "")


def run_query(
    app_root: Path,
    datasource_name: str,
    sql_text: str,
    datasource_definition_text: str = "",
    datasource_definition_file: Path | None = None,
    timeout_sec: int = 300,
    max_rows: int = 1000,
) -> tuple[list[str], list[tuple], bool, list[str]]:
    errors = validators.validate_read_only_sql(sql_text or "", "SQL")
    if errors:
        return [], [], False, errors

    ds_map = load_datasource_map(
        app_root=app_root,
        datasource_definition_text=datasource_definition_text,
        datasource_definition_file=datasource_definition_file,
    )
    if not ds_map:
        return [], [], False, ["No datasource definitions found."]
    if datasource_name not in ds_map:
        return [], [], False, [f"Datasource not found: {datasource_name}"]
    normalized_limit = max(1, int(max_rows or 0))
    acquired = _ADHOC_QUERY_SLOT.acquire(blocking=False)
    if not acquired:
        return (
            [],
            [],
            False,
            [
                "Too many concurrent queries. Please retry in a few seconds."
            ],
        )
    try:
        columns, rows, truncated = execute_target_sql_multi_limited(
            ds_map[datasource_name],
            sql_text,
            timeout_sec=timeout_sec,
            read_only=True,
            max_rows=normalized_limit,
        )
    except Exception as exc:
        return [], [], False, [f"Query failed: {exc}"]
    finally:
        if acquired:
            _ADHOC_QUERY_SLOT.release()
    return columns, rows, truncated, []
