from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

try:
    from .db import PgClient
except ImportError:  # Support running as a script from the eva directory.
    from db import PgClient


@dataclass(frozen=True)
class BaselineStats:
    baseline: Optional[int]
    deviation: Optional[int]
    sample_count: int
    previous: Optional[int]
    last_week: Optional[int]
    last_month: Optional[int]


def calculate_baseline_stats(
    *,
    main_pg: PgClient,
    target_name: str,
    metric_name: str,
    evaluated_at_iso: str,
    lookback_months: int = 3,
    max_minute_distance: int = 60,
) -> BaselineStats:
    sql = """
    WITH ref AS (
        SELECT
            %(evaluated_at)s::timestamptz AS ref_ts,
            (%(evaluated_at)s::timestamptz - INTERVAL '7 days') AS last_week_ts,
            (%(evaluated_at)s::timestamptz - INTERVAL '1 month') AS last_month_ts,
            EXTRACT(ISODOW FROM %(evaluated_at)s::timestamptz AT TIME ZONE 'UTC')::int AS ref_isodow,
            (
                EXTRACT(HOUR FROM %(evaluated_at)s::timestamptz AT TIME ZONE 'UTC')::int * 60
                + EXTRACT(MINUTE FROM %(evaluated_at)s::timestamptz AT TIME ZONE 'UTC')::int
            ) AS ref_mod,
            (
                EXTRACT(HOUR FROM (%(evaluated_at)s::timestamptz - INTERVAL '7 days') AT TIME ZONE 'UTC')::int * 60
                + EXTRACT(MINUTE FROM (%(evaluated_at)s::timestamptz - INTERVAL '7 days') AT TIME ZONE 'UTC')::int
            ) AS last_week_mod,
            (
                EXTRACT(HOUR FROM (%(evaluated_at)s::timestamptz - INTERVAL '1 month') AT TIME ZONE 'UTC')::int * 60
                + EXTRACT(MINUTE FROM (%(evaluated_at)s::timestamptz - INTERVAL '1 month') AT TIME ZONE 'UTC')::int
            ) AS last_month_mod,
            ((%(evaluated_at)s::timestamptz - INTERVAL '7 days') AT TIME ZONE 'UTC')::date AS last_week_date,
            ((%(evaluated_at)s::timestamptz - INTERVAL '1 month') AT TIME ZONE 'UTC')::date AS last_month_date
    ),
    hist AS (
        SELECT
            r.evaluated_at,
            r.metric_value::double precision AS metric_value,
            (r.evaluated_at AT TIME ZONE 'UTC')::date AS run_date,
            (
                EXTRACT(HOUR FROM r.evaluated_at AT TIME ZONE 'UTC')::int * 60
                + EXTRACT(MINUTE FROM r.evaluated_at AT TIME ZONE 'UTC')::int
            ) AS run_mod,
            ref.ref_ts
        FROM anbu_result r
        CROSS JOIN ref
        WHERE r.target_name = %(target_name)s
          AND r.metric_name = %(metric_name)s
          AND r.metric_value IS NOT NULL
          AND r.evaluated_at >= ref.ref_ts - make_interval(months => %(lookback_months)s)
          AND r.evaluated_at < ref.ref_ts
          AND EXTRACT(ISODOW FROM r.evaluated_at AT TIME ZONE 'UTC')::int = ref.ref_isodow
    ),
    ranked AS (
        SELECT
            evaluated_at,
            metric_value,
            run_date,
            ref_ts,
            LEAST(ABS(run_mod - ref.ref_mod), 1440 - ABS(run_mod - ref.ref_mod)) AS minute_distance
        FROM hist
        CROSS JOIN ref
        WHERE EXTRACT(ISODOW FROM evaluated_at AT TIME ZONE 'UTC')::int = ref.ref_isodow
    ),
    chosen AS (
        SELECT DISTINCT ON (run_date)
            metric_value,
            evaluated_at,
            CASE
                WHEN evaluated_at >= ref_ts - INTERVAL '1 month' THEN 3
                WHEN evaluated_at >= ref_ts - INTERVAL '2 months' THEN 2
                ELSE 1
            END AS sample_weight
        FROM ranked
        WHERE minute_distance <= %(max_minute_distance)s
        ORDER BY run_date, minute_distance ASC, evaluated_at DESC
    ),
    previous_row AS (
        SELECT
            r.metric_value::double precision AS metric_value,
            r.evaluated_at
        FROM anbu_result r
        CROSS JOIN ref
        WHERE r.target_name = %(target_name)s
          AND r.metric_name = %(metric_name)s
          AND r.metric_value IS NOT NULL
          AND r.evaluated_at < ref.ref_ts
        ORDER BY r.evaluated_at DESC
        LIMIT 1
    ),
    last_week_ranked AS (
        SELECT
            h.metric_value,
            h.evaluated_at,
            LEAST(ABS(h.run_mod - ref.last_week_mod), 1440 - ABS(h.run_mod - ref.last_week_mod)) AS minute_distance
        FROM hist h
        CROSS JOIN ref
        WHERE h.run_date = ref.last_week_date
    ),
    last_week_row AS (
        SELECT metric_value, evaluated_at
        FROM last_week_ranked
        WHERE minute_distance <= %(max_minute_distance)s
        ORDER BY minute_distance ASC, evaluated_at DESC
        LIMIT 1
    ),
    last_month_ranked AS (
        SELECT
            h.metric_value,
            h.evaluated_at,
            LEAST(ABS(h.run_mod - ref.last_month_mod), 1440 - ABS(h.run_mod - ref.last_month_mod)) AS minute_distance
        FROM hist h
        CROSS JOIN ref
        WHERE h.run_date = ref.last_month_date
    ),
    last_month_row AS (
        SELECT metric_value, evaluated_at
        FROM last_month_ranked
        WHERE minute_distance <= %(max_minute_distance)s
        ORDER BY minute_distance ASC, evaluated_at DESC
        LIMIT 1
    )
    SELECT 'BASELINE' AS stat_name, metric_value, sample_weight
    FROM chosen
    UNION ALL
    SELECT 'PREVIOUS' AS stat_name, metric_value, NULL::double precision AS sample_weight
    FROM previous_row
    UNION ALL
    SELECT 'LAST_WEEK' AS stat_name, metric_value, NULL::double precision AS sample_weight
    FROM last_week_row
    UNION ALL
    SELECT 'LAST_MONTH' AS stat_name, metric_value, NULL::double precision AS sample_weight
    FROM last_month_row
    """

    _, rows = main_pg.fetch_all_rows(
        sql,
        {
            "target_name": target_name,
            "metric_name": metric_name,
            "evaluated_at": evaluated_at_iso,
            "lookback_months": int(max(1, lookback_months)),
            "max_minute_distance": int(max(0, max_minute_distance)),
        },
    )

    weighted_values: list[tuple[float, float]] = []
    previous_value: Optional[int] = None
    last_week_value: Optional[int] = None
    last_month_value: Optional[int] = None

    def _to_metric_int(value: object) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(round(float(value)))
        except Exception:
            return None

    for row in rows:
        if not row or len(row) < 2:
            continue
        stat_name = str(row[0] or "").strip().upper()
        val = row[1]
        if val is None:
            continue
        if stat_name == "PREVIOUS":
            if previous_value is None:
                previous_value = _to_metric_int(val)
            continue
        if stat_name == "LAST_WEEK":
            if last_week_value is None:
                last_week_value = _to_metric_int(val)
            continue
        if stat_name == "LAST_MONTH":
            if last_month_value is None:
                last_month_value = _to_metric_int(val)
            continue
        weight = row[2] if len(row) > 2 else 1
        try:
            weight_f = float(weight)
        except Exception:
            weight_f = 1.0
        if weight_f <= 0:
            continue
        weighted_values.append((float(val), weight_f))

    if not weighted_values:
        return BaselineStats(
            baseline=None,
            deviation=None,
            sample_count=0,
            previous=previous_value,
            last_week=last_week_value,
            last_month=last_month_value,
        )

    total_weight = sum(weight for _, weight in weighted_values)
    if total_weight <= 0:
        return BaselineStats(
            baseline=None,
            deviation=None,
            sample_count=0,
            previous=previous_value,
            last_week=last_week_value,
            last_month=last_month_value,
        )

    # Recency-weighted baseline/deviation: newest month=3, prior month=2, third month=1.
    baseline_float = sum(val * weight for val, weight in weighted_values) / total_weight
    deviation_float = (
        sum(abs(val - baseline_float) * weight for val, weight in weighted_values) / total_weight
    )

    baseline_int = int(round(baseline_float))
    deviation_int = max(0, int(round(deviation_float)))
    return BaselineStats(
        baseline=baseline_int,
        deviation=deviation_int,
        sample_count=len(weighted_values),
        previous=previous_value,
        last_week=last_week_value,
        last_month=last_month_value,
    )
