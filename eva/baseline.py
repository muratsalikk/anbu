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
            EXTRACT(ISODOW FROM %(evaluated_at)s::timestamptz AT TIME ZONE 'UTC')::int AS ref_isodow,
            (
                EXTRACT(HOUR FROM %(evaluated_at)s::timestamptz AT TIME ZONE 'UTC')::int * 60
                + EXTRACT(MINUTE FROM %(evaluated_at)s::timestamptz AT TIME ZONE 'UTC')::int
            ) AS ref_mod
    ),
    hist AS (
        SELECT
            r.evaluated_at,
            r.metric_value::double precision AS metric_value,
            (r.evaluated_at AT TIME ZONE 'UTC')::date AS run_date,
            ref.ref_ts,
            ABS(
                (
                    EXTRACT(HOUR FROM r.evaluated_at AT TIME ZONE 'UTC')::int * 60
                    + EXTRACT(MINUTE FROM r.evaluated_at AT TIME ZONE 'UTC')::int
                ) - ref.ref_mod
            ) AS raw_dist
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
            LEAST(raw_dist, 1440 - raw_dist) AS minute_distance
        FROM hist
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
    )
    SELECT metric_value, sample_weight
    FROM chosen
    ORDER BY evaluated_at DESC
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
    for row in rows:
        if not row:
            continue
        val = row[0]
        if val is None:
            continue
        weight = row[1] if len(row) > 1 else 1
        try:
            weight_f = float(weight)
        except Exception:
            weight_f = 1.0
        if weight_f <= 0:
            continue
        weighted_values.append((float(val), weight_f))

    if not weighted_values:
        return BaselineStats(baseline=None, deviation=None, sample_count=0)

    total_weight = sum(weight for _, weight in weighted_values)
    if total_weight <= 0:
        return BaselineStats(baseline=None, deviation=None, sample_count=0)

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
    )
