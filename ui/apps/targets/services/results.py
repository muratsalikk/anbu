from __future__ import annotations

from datetime import datetime, timezone as dt_timezone
from decimal import Decimal
from typing import Any

from django.db import connections
from django.db.models import Q
from django.utils import timezone

from apps.targets.models import TargetAudit


def _rows_to_dicts(cursor) -> list[dict[str, Any]]:  # type: ignore[no-untyped-def]
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _parse_dt_value(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    text = str(value or "").strip()
    if not text:
        return None
    normalized = text
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(normalized)
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(text, fmt)
        except Exception:
            continue
    return None


def _format_dt(value: Any) -> str:
    dt_value = _parse_dt_value(value)
    if dt_value is not None:
        if dt_value.tzinfo is None:
            dt_value = dt_value.replace(tzinfo=dt_timezone.utc)
        dt_value = timezone.localtime(
            dt_value,
            timezone.get_default_timezone(),
        )
        return dt_value.strftime("%Y-%m-%d %H:%M:%S")
    return "" if value is None else str(value)


def _normalize_intish(value: Any) -> Any:
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else value
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else value
    return value


def get_runtime_states(target_names: list[str]) -> dict[str, dict[str, Any]]:
    if not target_names:
        return {}
    sql = """
        WITH latest_min AS (
            SELECT
                target_name,
                date_trunc('minute', max(evaluated_at)) AS last_run
            FROM anbu_result
            WHERE target_name = ANY(%s)
            GROUP BY target_name
        ),
        candidates AS (
            SELECT
                r.target_name,
                r.evaluated_at,
                r.severity,
                r.state,
                lm.last_run
            FROM anbu_result r
            JOIN latest_min lm
              ON r.target_name = lm.target_name
             AND date_trunc('minute', r.evaluated_at) = lm.last_run
        ),
        ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY target_name
                    ORDER BY severity DESC NULLS LAST, evaluated_at DESC
                ) AS rn
            FROM candidates
        )
        SELECT
            target_name,
            last_run,
            (ARRAY['UNKNOWN','NORMAL','MINOR','MAJOR','CRITICAL'])[COALESCE(severity, 0) + 1] AS severity,
            state
        FROM ranked
        WHERE rn = 1
    """
    with connections["data_store"].cursor() as cursor:
        cursor.execute(sql, [target_names])
        rows = _rows_to_dicts(cursor)
    states: dict[str, dict[str, Any]] = {}
    for row in rows:
        target_name = str(row.get("target_name") or "")
        states[target_name] = {
            "severity": str(row.get("severity") or ""),
            "state": str(row.get("state") or ""),
            "last_run": _format_dt(row.get("last_run")),
        }
    return states


def get_rule_audit(rule_names: list[str]) -> dict[str, dict[str, Any]]:
    if not rule_names:
        return {}
    audits: dict[str, dict[str, Any]] = {}
    queryset = (
        TargetAudit.objects.using("default")
        .filter(target_name__in=rule_names)
        .order_by("target_name", "-edited_at", "-id")
        .values("target_name", "edited_at", "edited_by")
    )
    seen: set[str] = set()
    rows = list(queryset)
    for row in rows:
        target_name = str(row.get("target_name") or "")
        if not target_name or target_name in seen:
            continue
        seen.add(target_name)
        audits[target_name] = {
            "last_edited_by": str(row.get("edited_by") or ""),
            "last_edited_at": _format_dt(row.get("edited_at")),
        }
    return audits


def get_rule_audit_history(rule_name: str, limit: int = 100) -> list[dict[str, Any]]:
    target = str(rule_name or "").strip().upper()
    if not target:
        return []
    rows = list(
        TargetAudit.objects.using("default")
        .filter(target_name=target)
        .order_by("-edited_at", "-id")
        .values("id", "edited_at", "edited_by", "change_notes")[: max(int(limit or 0), 1)]
    )
    for row in rows:
        row["last_edited_at"] = _format_dt(row.get("edited_at"))
        row["last_edited_by"] = str(row.get("edited_by") or "")
        row["change_notes"] = str(row.get("change_notes") or "")
    return rows


def get_target_metrics(target_name: str) -> list[str]:
    sql = """
        SELECT DISTINCT metric_name
        FROM anbu_result
        WHERE target_name = %s
          AND metric_name IS NOT NULL
          AND metric_name <> ''
        ORDER BY metric_name
    """
    with connections["data_store"].cursor() as cursor:
        cursor.execute(sql, [target_name])
        rows = _rows_to_dicts(cursor)
    return [str(row.get("metric_name") or "").strip() for row in rows if row.get("metric_name")]


def get_status_history(
    target_name: str,
    metric_name: str | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    sql = """
        WITH base AS (
            SELECT
                id,
                evaluated_at,
                target_name,
                metric_name,
                severity AS severity_num,
                CASE severity
                    WHEN 4 THEN 'CRITICAL'
                    WHEN 3 THEN 'MAJOR'
                    WHEN 2 THEN 'MINOR'
                    WHEN 1 THEN 'NORMAL'
                    ELSE 'UNKNOWN'
                END AS severity_txt,
                state,
                message,
                action_name,
                LAG(severity) OVER w AS prev_severity_num,
                LAG(state) OVER w AS prev_state
            FROM anbu_result
            WHERE target_name = %s
              AND (%s IS NULL OR metric_name = %s)
            WINDOW w AS (
                PARTITION BY target_name, metric_name
                ORDER BY evaluated_at
            )
        ),
        change_points AS (
            SELECT *,
                CASE
                    WHEN prev_severity_num IS DISTINCT FROM severity_num
                      OR prev_state IS DISTINCT FROM state
                    THEN 1
                    ELSE 0
                END AS is_change
            FROM base
        ),
        groups AS (
            SELECT *,
                SUM(is_change) OVER (
                    PARTITION BY target_name, metric_name
                    ORDER BY evaluated_at
                    ROWS UNBOUNDED PRECEDING
                ) AS grp_id
            FROM change_points
        ),
        intervals AS (
            SELECT
                target_name,
                metric_name,
                severity_txt AS severity,
                state,
                MIN(evaluated_at) AS started_at,
                MAX(evaluated_at) AS last_seen_at,
                (ARRAY_AGG(message ORDER BY evaluated_at DESC))[1] AS last_message,
                (ARRAY_AGG(action_name ORDER BY evaluated_at DESC))[1] AS last_action,
                grp_id
            FROM groups
            GROUP BY
                target_name,
                metric_name,
                severity_txt,
                state,
                grp_id
        ),
        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY target_name, metric_name
                    ORDER BY started_at DESC, grp_id DESC
                ) AS rn
            FROM intervals
        )
        SELECT
            target_name,
            metric_name,
            severity,
            state,
            started_at,
            CASE
                WHEN rn = 1 THEN NULL
                ELSE last_seen_at
            END AS ended_at,
            last_message,
            last_action
        FROM ranked
        ORDER BY ended_at DESC NULLS FIRST, started_at DESC
        LIMIT %s
    """
    with connections["data_store"].cursor() as cursor:
        cursor.execute(sql, [target_name, metric_name, metric_name, limit])
        rows = _rows_to_dicts(cursor)
    for row in rows:
        row["started_at"] = _format_dt(row.get("started_at"))
        row["ended_at"] = _format_dt(row.get("ended_at"))
    return rows


def get_result_instances(target_name: str, limit: int = 1000) -> list[dict[str, Any]]:
    sql = """
        SELECT
            evaluated_at,
            target_name,
            metric_name,
            metric_value,
            baseline,
            deviation,
            severity,
            state,
            message,
            action_name,
            datasource,
            scheduler_name,
            tags,
            critical_val,
            major_val,
            minor_val
        FROM anbu_result
        WHERE target_name = %s
        ORDER BY evaluated_at DESC
        LIMIT %s
    """
    with connections["data_store"].cursor() as cursor:
        cursor.execute(sql, [target_name, limit])
        rows = _rows_to_dicts(cursor)
    for row in rows:
        row["evaluated_at"] = _format_dt(row.get("evaluated_at"))
        for key in (
            "metric_value",
            "baseline",
            "deviation",
            "critical_val",
            "major_val",
            "minor_val",
        ):
            row[key] = _normalize_intish(row.get(key))
    return rows


def get_latest_metric_results(
    target_name: str,
    metric_names: list[str],
) -> dict[str, dict[str, Any]]:
    normalized_metrics = [
        str(name or "").strip()
        for name in metric_names
        if str(name or "").strip()
    ]
    if not normalized_metrics:
        return {}
    sql = """
        WITH ranked AS (
            SELECT
                metric_name,
                metric_value,
                severity,
                state,
                evaluated_at,
                ROW_NUMBER() OVER (
                    PARTITION BY metric_name
                    ORDER BY evaluated_at DESC, id DESC
                ) AS rn
            FROM anbu_result
            WHERE target_name = %s
              AND metric_name = ANY(%s)
        )
        SELECT
            metric_name,
            metric_value,
            severity,
            state,
            evaluated_at
        FROM ranked
        WHERE rn = 1
    """
    with connections["data_store"].cursor() as cursor:
        cursor.execute(sql, [target_name, normalized_metrics])
        rows = _rows_to_dicts(cursor)

    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        metric_name = str(row.get("metric_name") or "").strip()
        if not metric_name:
            continue
        latest[metric_name.upper()] = {
            "metric_name": metric_name,
            "metric_value": _normalize_intish(row.get("metric_value")),
            "severity": row.get("severity"),
            "state": str(row.get("state") or ""),
            "evaluated_at": _format_dt(row.get("evaluated_at")),
        }
    return latest


def delete_target_history(target_name: str) -> int:
    normalized_target = str(target_name or "").strip().upper()
    if not normalized_target:
        return 0
    sql = """
        DELETE FROM anbu_result
        WHERE target_name = %s
    """
    with connections["data_store"].cursor() as cursor:
        cursor.execute(sql, [normalized_target])
        return int(cursor.rowcount or 0)


def insert_import_rows(rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    sql = """
        INSERT INTO anbu_result (
            evaluated_at,
            target_name,
            metric_name,
            metric_value,
            severity,
            state,
            critical_val,
            major_val,
            minor_val,
            message,
            action_name,
            datasource,
            scheduler_name,
            tags
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """
    params = [
        (
            row.get("evaluated_at"),
            row.get("target_name"),
            row.get("metric_name"),
            row.get("metric_value"),
            row.get("severity"),
            row.get("state"),
            row.get("critical_val"),
            row.get("major_val"),
            row.get("minor_val"),
            row.get("message"),
            row.get("action_name"),
            row.get("datasource"),
            row.get("scheduler_name"),
            row.get("tags"),
        )
        for row in rows
    ]
    with connections["data_store"].cursor() as cursor:
        cursor.executemany(sql, params)
        return int(cursor.rowcount or 0)


def create_target_audit_entry(
    target_name: str,
    user: str,
    env_content: str,
    sql_content: str,
    hql_content: str,
    change_notes: str = "",
) -> None:
    TargetAudit.objects.using("default").create(
        target_name=(target_name or "").strip().upper(),
        edited_by=(user or "").strip(),
        edited_at=timezone.now(),
        change_notes=str(change_notes or ""),
        env_content=str(env_content or ""),
        sql_content=str(sql_content or ""),
        hql_content=str(hql_content or ""),
    )


def get_target_audit_entry(target_name: str, audit_id: int) -> dict[str, Any] | None:
    normalized_target = str(target_name or "").strip().upper()
    row = (
        TargetAudit.objects.using("default")
        .filter(target_name=normalized_target, id=int(audit_id))
        .values(
            "id",
            "target_name",
            "edited_at",
            "edited_by",
            "change_notes",
            "env_content",
            "sql_content",
            "hql_content",
        )
        .first()
    )
    if not row:
        return None
    return {
        "id": int(row.get("id") or 0),
        "target_name": str(row.get("target_name") or ""),
        "edited_at": _format_dt(row.get("edited_at")),
        "edited_by": str(row.get("edited_by") or ""),
        "change_notes": str(row.get("change_notes") or ""),
        "env_content": str(row.get("env_content") or ""),
        "sql_content": str(row.get("sql_content") or ""),
        "hql_content": str(row.get("hql_content") or ""),
    }


def get_previous_target_audit_entry(target_name: str, audit_id: int) -> dict[str, Any] | None:
    normalized_target = str(target_name or "").strip().upper()
    current = (
        TargetAudit.objects.using("default")
        .filter(target_name=normalized_target, id=int(audit_id))
        .values("id", "edited_at")
        .first()
    )
    if not current:
        return None
    current_id = int(current.get("id") or 0)
    current_edited_at = current.get("edited_at")
    if not current_edited_at:
        return None

    row = (
        TargetAudit.objects.using("default")
        .filter(target_name=normalized_target)
        .filter(
            Q(edited_at__lt=current_edited_at)
            | Q(edited_at=current_edited_at, id__lt=current_id)
        )
        .order_by("-edited_at", "-id")
        .values(
            "id",
            "target_name",
            "edited_at",
            "edited_by",
            "change_notes",
            "env_content",
            "sql_content",
            "hql_content",
        )
        .first()
    )
    if not row:
        return None
    return {
        "id": int(row.get("id") or 0),
        "target_name": str(row.get("target_name") or ""),
        "edited_at": _format_dt(row.get("edited_at")),
        "edited_by": str(row.get("edited_by") or ""),
        "change_notes": str(row.get("change_notes") or ""),
        "env_content": str(row.get("env_content") or ""),
        "sql_content": str(row.get("sql_content") or ""),
        "hql_content": str(row.get("hql_content") or ""),
    }
