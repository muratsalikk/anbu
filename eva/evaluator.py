from __future__ import annotations

import random
import re
import time
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional

from engine import (
    SEV_ORDER,
    SEV_TO_NUM,
    apply_operator,
    build_alias_map,
    compute_context,
    log_line,
    parse_bool,
    parse_env_file,
    parse_legacy_if,
    parse_metric_defs,
    render_message,
    resolve_value_ref,
    run_action,
    safe_eval_expr,
    is_read_only_sql,
    load_actions,
    load_engine_props,
    load_schedulers,
)
from db import (
    Datasource,
    execute_target_sql,
    execute_target_sql_multi,
    insert_result_row,
    get_result_pg_client,
    load_datasources,
)
from baseline import calculate_baseline_stats


class ConfigError(Exception):
    pass


def _insert_result_row_from_pool(**kwargs: Any) -> None:
    with get_result_pg_client() as main_pg:
        insert_result_row(main_pg=main_pg, **kwargs)


def _calculate_baseline_stats_from_pool(
    *,
    target_name: str,
    metric_name: str,
    evaluated_at_iso: str,
):
    with get_result_pg_client() as main_pg:
        return calculate_baseline_stats(
            main_pg=main_pg,
            target_name=target_name,
            metric_name=metric_name,
            evaluated_at_iso=evaluated_at_iso,
        )


def evaluate_metric(
    *,
    evaluated_at_iso: str,
    target_name: str,
    metric_name: str,
    mdef,
    sql_vals: Dict[str, Any],
    alias_map: Dict[str, str],
    is_muted: bool,
    actions: Dict[str, str],
    target_ds_key: str,
    scheduler_name: str,
    tags: str,
    sql_timeout: int,
    env: Dict[str, str],
) -> None:
    metric_value_int: Optional[int] = None
    baseline_value: Optional[int] = None
    deviation_value: Optional[int] = None
    muted_action_name = "TARGET_MUTED-NO_ACTION_TAKEN" if is_muted else None
    try:
        ctx = compute_context(target_name, metric_name, sql_vals, alias_map)

        baseline_calc_error: Optional[str] = None
        try:
            baseline_stats = _calculate_baseline_stats_from_pool(
                target_name=target_name,
                metric_name=metric_name,
                evaluated_at_iso=evaluated_at_iso,
            )
            baseline_value = baseline_stats.baseline
            deviation_value = baseline_stats.deviation
        except Exception as e:
            baseline_calc_error = f"baseline calc failed: {e}"
            baseline_value = None
            deviation_value = None

        ctx["BASELINE"] = baseline_value
        ctx["DEVIATION"] = deviation_value

        mv = resolve_value_ref(mdef.value_ref, ctx)
        if mv is None:
            msg = f"nodata: metric value missing or non-numeric for ref={mdef.value_ref}"
            if baseline_calc_error:
                msg = (msg + f" | {baseline_calc_error}").strip()
            _insert_result_row_from_pool(
                evaluated_at_iso=evaluated_at_iso,
                target_name=target_name,
                metric_name=metric_name,
                metric_value=None,
                severity=None,
                state="nodata",
                critical_val=None,
                major_val=None,
                minor_val=None,
                message=msg,
                action_name=muted_action_name,
                datasource=target_ds_key,
                scheduler_name=scheduler_name,
                tags=tags,
                baseline=baseline_value,
                deviation=deviation_value,
            )
            log_line(target_name, evaluated_at_iso, f"{metric_name} error: {msg}")
            return

        metric_value_int = int(mv)

        matched_sev = "NORMAL"
        matched_action = mdef.normal_action
        matched_msg_tmpl = mdef.normal_msg or "evaluated ok"
        matched_rhs: Optional[float] = None
        data_error = False
        data_error_notes: List[str] = []
        baseline_missing = False
        deviation_missing = False

        need_baseline = False
        need_deviation = False
        all_exprs = []
        for sev in SEV_ORDER:
            for c in mdef.conditions.get(sev, []):
                all_exprs.append(c.val_expr)
            if sev in mdef.legacy_if:
                all_exprs.append(mdef.legacy_if[sev])
        joined = " ".join(all_exprs).upper()
        if "BASELINE" in joined:
            need_baseline = True
        if "DEVIATION" in joined:
            need_deviation = True
        if baseline_calc_error and (need_baseline or need_deviation):
            data_error_notes.append(baseline_calc_error)
        if need_baseline and ctx.get("BASELINE") is None:
            baseline_missing = True
            data_error = True
            if "BASELINE missing or null" not in data_error_notes:
                data_error_notes.append("BASELINE missing or null")
        if need_deviation and ctx.get("DEVIATION") is None:
            deviation_missing = True
            data_error = True
            if "DEVIATION missing or null" not in data_error_notes:
                data_error_notes.append("DEVIATION missing or null")

        ctx["CONDITION_VALUE"] = 0

        for sev in SEV_ORDER:
            conds = mdef.conditions.get(sev, [])
            if conds:
                for c in sorted(conds, key=lambda x: x.idx):
                    try:
                        expr_upper = c.val_expr.upper()
                        if baseline_missing and "BASELINE" in expr_upper:
                            continue
                        if deviation_missing and "DEVIATION" in expr_upper:
                            continue
                        rhs = safe_eval_expr(c.val_expr, ctx)
                        if apply_operator(float(mv), c.operator, rhs):
                            matched_sev = sev
                            matched_action = c.action or matched_action
                            matched_msg_tmpl = c.msg or matched_msg_tmpl
                            matched_rhs = rhs
                            break
                    except Exception as e:
                        raise ConfigError(f"Condition eval failed: {c.val_expr}") from e
                if matched_sev == sev:
                    break
            elif sev in mdef.legacy_if:
                expr_text = mdef.legacy_if[sev]
                expr_upper = expr_text.upper()
                if (baseline_missing and "BASELINE" in expr_upper) or (deviation_missing and "DEVIATION" in expr_upper):
                    continue
                try:
                    lhs_s, op_s, rhs_s = parse_legacy_if(expr_text)
                    lhs_val = safe_eval_expr(lhs_s, ctx) if re.match(r"^[A-Z_][A-Z0-9_]*$", lhs_s.strip()) else float(lhs_s)
                    rhs_val = safe_eval_expr(rhs_s, ctx)
                    if apply_operator(lhs_val, op_s, rhs_val):
                        matched_sev = sev
                        matched_action = env.get(f"METRIC_{mdef.i}_{sev}_ACTION", matched_action) or matched_action
                        matched_msg_tmpl = env.get(f"METRIC_{mdef.i}_{sev}_MSG", matched_msg_tmpl) or matched_msg_tmpl
                        matched_rhs = rhs_val
                        break
                except Exception as e:
                    raise ConfigError(f"Legacy IF eval failed: {expr_text}") from e

        ctx["CONDITION_VALUE"] = matched_rhs if matched_rhs is not None else 0
        msg_final = render_message(matched_msg_tmpl, ctx).strip()
        if data_error_notes:
            note = "; ".join(data_error_notes)
            if msg_final:
                msg_final = (msg_final + f" | data_error: {note}").strip()
            else:
                msg_final = f"data_error: {note}"

        severity_num = SEV_TO_NUM.get(matched_sev, 1)

        critical_val = major_val = minor_val = None
        if matched_sev == "CRITICAL" and matched_rhs is not None:
            critical_val = int(matched_rhs)
        elif matched_sev == "MAJOR" and matched_rhs is not None:
            major_val = int(matched_rhs)
        elif matched_sev == "MINOR" and matched_rhs is not None:
            minor_val = int(matched_rhs)

        action_state = "ok"
        action_name_used: Optional[str] = None

        if is_muted:
            action_name_used = "TARGET_MUTED-NO_ACTION_TAKEN"
        else:
            if matched_action:
                action_name_used = matched_action
                if matched_action not in actions:
                    action_state = "config_error"
                    msg_final = (msg_final + f" | action not defined: {matched_action}").strip()
                else:
                    script = actions[matched_action]
                    ok, detail = run_action(
                        script_path=script,
                        target_name=target_name,
                        metric_name=metric_name,
                        metric_value=metric_value_int,
                        severity=severity_num,
                        message=msg_final,
                        timeout=max(5, sql_timeout),
                    )
                    if not ok:
                        action_state = "action_error"
                        msg_final = (msg_final + f" | action failed: {detail}").strip()

        final_state = "data_error" if data_error else "ok"
        if action_state in ("action_error", "config_error"):
            final_state = action_state

        _insert_result_row_from_pool(
            evaluated_at_iso=evaluated_at_iso,
            target_name=target_name,
            metric_name=metric_name,
            metric_value=metric_value_int,
            severity=severity_num,
            state=final_state,
            critical_val=critical_val,
            major_val=major_val,
            minor_val=minor_val,
            message=msg_final,
            action_name=action_name_used,
            datasource=target_ds_key,
            scheduler_name=scheduler_name,
            tags=tags,
            baseline=baseline_value,
            deviation=deviation_value,
        )
        log_line(target_name, evaluated_at_iso, msg_final or "evaluated ok")

    except ConfigError as e:
        msg = f"config error: {e}"
        _insert_result_row_from_pool(
            evaluated_at_iso=evaluated_at_iso,
            target_name=target_name,
            metric_name=metric_name,
            metric_value=metric_value_int,
            severity=None,
            state="config_error",
            critical_val=None,
            major_val=None,
            minor_val=None,
            message=msg,
            action_name=muted_action_name,
            datasource=target_ds_key,
            scheduler_name=scheduler_name,
            tags=tags,
            baseline=baseline_value,
            deviation=deviation_value,
        )
        log_line(target_name, evaluated_at_iso, f"{metric_name} error: {msg}")
    except Exception as e:
        msg = f"system error: {e}"
        _insert_result_row_from_pool(
            evaluated_at_iso=evaluated_at_iso,
            target_name=target_name,
            metric_name=metric_name,
            metric_value=metric_value_int,
            severity=None,
            state="system_error",
            critical_val=None,
            major_val=None,
            minor_val=None,
            message=msg,
            action_name=muted_action_name,
            datasource=target_ds_key,
            scheduler_name=scheduler_name,
            tags=tags,
            baseline=baseline_value,
            deviation=deviation_value,
        )
        log_line(target_name, evaluated_at_iso, f"{metric_name} error: {msg}")


def run_target(app_dir: Path, target_name: str, evaluated_at_iso: str, is_muted_override: Optional[bool] = None) -> int:
    target_started_at = time.perf_counter()

    def _finish(code: int) -> int:
        elapsed_ms = int((time.perf_counter() - target_started_at) * 1000)
        log_line(target_name, evaluated_at_iso, f"TARGET eval_ms={elapsed_ms}")
        return code

    props = load_engine_props(app_dir)

    rules_dir = props.get("RULES_DIR", "").strip()
    if not rules_dir:
        raise ValueError("engine.properties missing RULES_DIR")
    rules_dir_p = Path(rules_dir)
    if not rules_dir_p.is_absolute():
        rules_dir_p = (app_dir / rules_dir_p).resolve()

    ds_def_file = props.get("DATASOURCE_DEFINITION_FILE", "").strip()
    if not ds_def_file:
        raise ValueError("engine.properties missing DATASOURCE_DEFINITION_FILE")
    ds_def_p = Path(ds_def_file)
    if not ds_def_p.is_absolute():
        ds_def_p = (app_dir / ds_def_p).resolve()

    action_def_file = props.get("ACTION_DEFINITION_FILE", "").strip()
    if not action_def_file:
        raise ValueError("engine.properties missing ACTION_DEFINITION_FILE")
    action_def_p = Path(action_def_file)
    if not action_def_p.is_absolute():
        action_def_p = (app_dir / action_def_p).resolve()

    main_ds_key = (props.get("MAIN_POSTGRES_DATASOURCE_KEY") or props.get("RESULT_DATASOURCE_KEY") or "").strip()

    datasources = load_datasources(app_dir, ds_def_p)
    actions = load_actions(app_dir, action_def_p)

    env_path = rules_dir_p / f"{target_name}.env"
    env: Dict[str, str] = {}
    metrics: List[Any] = []
    alias_map: Dict[str, str] = {}
    is_active = False
    is_muted = False
    scheduler_name = ""
    tags = ""
    target_ds_key = ""
    sql_timeout = 0
    jitter_max = 0
    sql_mode = "single"

    try:
        env = parse_env_file(env_path)

        required = [
            "TARGET_NAME",
            "DESCRIPTION",
            "TAG_LIST",
            "IS_ACTIVE",
            "IS_MUTED",
            "DATA_SOURCE",
            "SQL_TIMEOUT_SEC",
            "SQL_JITTER_SEC",
            "QUERY_FILE",
        ]
        for rk in required:
            if rk not in env:
                raise ValueError(f"Missing required env key: {rk}")

        if env["TARGET_NAME"].strip() != target_name:
            raise ValueError(f"TARGET_NAME mismatch: arg={target_name} env={env['TARGET_NAME']}")

        is_active = parse_bool(env.get("IS_ACTIVE", "false"))
        is_muted = parse_bool(env.get("IS_MUTED", "false"))
        if is_muted_override is not None:
            is_muted = is_muted or is_muted_override
        scheduler_name = env.get("UC4_SCHEDULER", "").strip()
        tags = env.get("TAG_LIST", "").strip()
        target_ds_key = env.get("DATA_SOURCE", "").strip()
        if not main_ds_key:
            main_ds_key = target_ds_key

        sched_file = props.get("SCHEDULER_DEFINITION_FILE", "").strip()
        if sched_file:
            sp = Path(sched_file)
            if not sp.is_absolute():
                sp = (app_dir / sp).resolve()
            scheds = load_schedulers(app_dir, sp)
            if scheduler_name and scheduler_name not in scheds:
                log_line(target_name, evaluated_at_iso, f"warning: unknown UC4_SCHEDULER={scheduler_name}")

        sql_timeout = int(env.get("SQL_TIMEOUT_SEC", "0") or "0")
        jitter_max = int(env.get("SQL_JITTER_SEC", "0") or "0")
        sql_mode = env.get("SQL_MODE", "single").strip().lower()
        if sql_mode != "multiline":
            sql_mode = "single"

        metrics = parse_metric_defs(env)
        alias_map = build_alias_map(env)
    except Exception as e:
        msg = f"config error: {e}"
        if env:
            scheduler_name = scheduler_name or env.get("UC4_SCHEDULER", "").strip()
            tags = tags or env.get("TAG_LIST", "").strip()
            target_ds_key = target_ds_key or env.get("DATA_SOURCE", "").strip()
        metric_names = [m.name for m in metrics] if metrics else ["TARGET"]
        with get_result_pg_client() as main_pg:
            for metric_name in metric_names:
                insert_result_row(
                    main_pg,
                    evaluated_at_iso,
                    target_name,
                    metric_name,
                    None,
                    None,
                    "config_error",
                    None,
                    None,
                    None,
                    msg,
                    None,
                    target_ds_key,
                    scheduler_name,
                    tags,
                )
                log_line(target_name, evaluated_at_iso, f"{metric_name} error: {msg}")
        return _finish(2)

    with get_result_pg_client() as main_pg:
        if not is_active:
            for mdef in metrics:
                insert_result_row(
                    main_pg=main_pg,
                    evaluated_at_iso=evaluated_at_iso,
                    target_name=target_name,
                    metric_name=mdef.name,
                    metric_value=None,
                    severity=None,
                    state="skipped",
                    critical_val=None,
                    major_val=None,
                    minor_val=None,
                    message="IS_ACTIVE=false (skipped)",
                    action_name=None,
                    datasource=target_ds_key,
                    scheduler_name=scheduler_name,
                    tags=tags,
                )
                log_line(target_name, evaluated_at_iso, f"{mdef.name} skipped (IS_ACTIVE=false)")
            return _finish(0)

        query_file = env.get("QUERY_FILE","").strip()
        sql_path = rules_dir_p / query_file
        if not sql_path.exists():
            sql_path = rules_dir_p / f"{target_name}.sql"
        if not sql_path.exists() or not sql_path.is_file():
            msg = f"SQL file not found: {sql_path}"
            for mdef in metrics:
                insert_result_row(main_pg, evaluated_at_iso, target_name, mdef.name, None, None, "config_error",
                                  None, None, None, msg, None, target_ds_key, scheduler_name, tags)
                log_line(target_name, evaluated_at_iso, f"{mdef.name} error: {msg}")
            return _finish(3)
        sql_text = sql_path.read_text(encoding="utf-8")

        if not is_read_only_sql(sql_text):
            msg = "sql_write_not_allowed"
            state = "data_error"
            action_name = "TARGET_MUTED-NO_ACTION_TAKEN" if is_muted else None
            for mdef in metrics:
                insert_result_row(
                    main_pg=main_pg,
                    evaluated_at_iso=evaluated_at_iso,
                    target_name=target_name,
                    metric_name=mdef.name,
                    metric_value=None,
                    severity=None,
                    state=state,
                    critical_val=None,
                    major_val=None,
                    minor_val=None,
                    message=msg,
                    action_name=action_name,
                    datasource=target_ds_key,
                    scheduler_name=scheduler_name,
                    tags=tags,
                )
                log_line(target_name, evaluated_at_iso, f"{mdef.name} error: {msg}")
            return _finish(3)

        if jitter_max > 0:
            sleep_s = random.randint(0, jitter_max)
            time.sleep(sleep_s)

        if target_ds_key not in datasources:
            msg = f"Unknown DATA_SOURCE: {target_ds_key}"
            for mdef in metrics:
                insert_result_row(main_pg, evaluated_at_iso, target_name, mdef.name, None, None, "config_error",
                                  None, None, None, msg, None, target_ds_key, scheduler_name, tags)
                log_line(target_name, evaluated_at_iso, f"{mdef.name} error: {msg}")
            return _finish(2)

        target_ds = datasources[target_ds_key]
        read_only = target_ds_key != main_ds_key
        default_metrics = [m for m in metrics if m.name.upper() == "DEFAULT"]
        non_default_metrics = [m for m in metrics if m.name.upper() != "DEFAULT"]
        metrics_by_name: Dict[str, Any] = {}
        for mdef in non_default_metrics:
            key = mdef.name.upper()
            if key not in metrics_by_name:
                metrics_by_name[key] = mdef

        try:
            if sql_mode == "multiline":
                cols, rows = execute_target_sql_multi(
                    target_ds, sql_text, timeout_sec=sql_timeout, read_only=read_only
                )
                log_line(
                    target_name,
                    evaluated_at_iso,
                    f"SQL ok: ds={target_ds_key}/{target_ds.type} cols={len(cols)} rows={len(rows)}",
                )
            else:
                cols, row = execute_target_sql(target_ds, sql_text, timeout_sec=sql_timeout, read_only=read_only)
                rows = [row]
                log_line(
                    target_name,
                    evaluated_at_iso,
                    f"SQL ok: ds={target_ds_key}/{target_ds.type} cols={len(cols)} "
                    f"row_type={type(row).__name__} row_len={len(row) if hasattr(row,'__len__') else 'NA'}"
                )
        except Exception as e:
            tb = traceback.format_exc()
            sql_head = " ".join((sql_text or "").strip().split())[:300]
            err_text = str(e)
            state = "system_error"
            msg = f"SQL error: {err_text}"
            if isinstance(e, ValueError) and "0 rows" in err_text:
                state = "nodata"
                msg = "SQL returned 0 rows"
            elif isinstance(e, ValueError) and "multiple rows" in err_text:
                state = "data_error"
                msg = "unexpected_multilne"
            elif isinstance(e, ValueError) and "more than 10 columns" in err_text:
                state = "config_error"
            elif isinstance(e, ValueError) and err_text.startswith("Invalid DSN"):
                state = "config_error"
            elif isinstance(e, ValueError) and err_text.startswith("Unsupported datasource TYPE"):
                state = "config_error"

            log_line(
                target_name,
                evaluated_at_iso,
                f"{msg} | ds={target_ds_key}/{target_ds.type} dsn={target_ds.dsn} "
                f"| sql='{sql_head}' | tb='{tb[:1800]}'"
            )

            for mdef in metrics:
                insert_result_row(
                    main_pg, evaluated_at_iso, target_name, mdef.name, None, None, state,
                    None, None, None, msg, None, target_ds_key, scheduler_name, tags
                )
                log_line(target_name, evaluated_at_iso, f"{mdef.name} error: {msg}")
            return _finish(3)

        if sql_mode == "multiline":
            if not rows:
                msg = "SQL returned 0 rows"
                if default_metrics:
                    insert_result_row(
                        main_pg, evaluated_at_iso, target_name, default_metrics[0].name, None, None, "nodata",
                        None, None, None, msg, None, target_ds_key, scheduler_name, tags
                    )
                    log_line(target_name, evaluated_at_iso, f"{default_metrics[0].name} error: {msg}")
                else:
                    for mdef in metrics:
                        insert_result_row(
                            main_pg, evaluated_at_iso, target_name, mdef.name, None, None, "nodata",
                            None, None, None, msg, None, target_ds_key, scheduler_name, tags
                        )
                        log_line(target_name, evaluated_at_iso, f"{mdef.name} error: {msg}")
                return _finish(0)

            metric_counts: Dict[str, int] = {}
            metric_display: Dict[str, str] = {}
            metric_order: List[str] = []
            row_by_metric: Dict[str, Any] = {}
            for row in rows:
                if not row:
                    continue
                metric_name = str(row[0]).strip()
                if not metric_name:
                    continue
                key = metric_name.upper()
                if key not in metric_display:
                    metric_display[key] = metric_name
                    metric_order.append(key)
                    row_by_metric[key] = row
                metric_counts[key] = metric_counts.get(key, 0) + 1
            dup_metrics = {name for name, count in metric_counts.items() if count > 1}
            evaluated_keys: set[str] = set()

            for key in metric_order:
                if key not in dup_metrics:
                    continue
                metric_name = metric_display.get(key, key)
                msg = "unexpected_multilne"
                insert_result_row(
                    main_pg, evaluated_at_iso, target_name, metric_name, None, None, "data_error",
                    None, None, None, msg, None, target_ds_key, scheduler_name, tags
                )
                log_line(target_name, evaluated_at_iso, f"{metric_name} error: {msg}")
                evaluated_keys.add(key)

            for key in metric_order:
                if key in evaluated_keys:
                    continue
                row = row_by_metric.get(key)
                if not row:
                    continue
                if len(row) > 10:
                    msg = "SQL returned more than 10 columns (max 10 for multiline)"
                    for mdef in metrics:
                        insert_result_row(
                            main_pg, evaluated_at_iso, target_name, mdef.name, None, None, "config_error",
                            None, None, None, msg, None, target_ds_key, scheduler_name, tags
                        )
                        log_line(target_name, evaluated_at_iso, f"{mdef.name} error: {msg}")
                    return _finish(3)
                metric_name = metric_display.get(key, key)
                mdef = metrics_by_name.get(key)
                if mdef is None:
                    continue
                sql_vals: Dict[str, Any] = {}
                for idx in range(1, 11):
                    sql_vals[f"VAL{idx}"] = None
                for idx, val in enumerate(row[1:], start=2):
                    if idx > 10:
                        break
                    sql_vals[f"VAL{idx}"] = val
                evaluate_metric(
                    evaluated_at_iso=evaluated_at_iso,
                    target_name=target_name,
                    metric_name=metric_name,
                    mdef=mdef,
                    sql_vals=sql_vals,
                    alias_map=alias_map,
                    is_muted=is_muted,
                    actions=actions,
                    target_ds_key=target_ds_key,
                    scheduler_name=scheduler_name,
                    tags=tags,
                    sql_timeout=sql_timeout,
                    env=env,
                )
                evaluated_keys.add(key)

            if default_metrics:
                for default_mdef in default_metrics:
                    for key in metric_order:
                        if key in evaluated_keys:
                            continue
                        row = row_by_metric.get(key)
                        if not row:
                            continue
                        metric_name = metric_display.get(key, key)
                        sql_vals: Dict[str, Any] = {}
                        for idx in range(1, 11):
                            sql_vals[f"VAL{idx}"] = None
                        for idx, val in enumerate(row[1:], start=2):
                            if idx > 10:
                                break
                            sql_vals[f"VAL{idx}"] = val
                        evaluate_metric(
                            evaluated_at_iso=evaluated_at_iso,
                            target_name=target_name,
                            metric_name=metric_name,
                            mdef=default_mdef,
                            sql_vals=sql_vals,
                            alias_map=alias_map,
                            is_muted=is_muted,
                            actions=actions,
                            target_ds_key=target_ds_key,
                            scheduler_name=scheduler_name,
                            tags=tags,
                            sql_timeout=sql_timeout,
                            env=env,
                        )
                        evaluated_keys.add(key)

            for key in metric_order:
                if key in evaluated_keys:
                    continue
                metric_name = metric_display.get(key, key)
                msg = f"Metric not defined: {metric_name}"
                insert_result_row(
                    main_pg, evaluated_at_iso, target_name, metric_name, None, None, "config_error",
                    None, None, None, msg, None, target_ds_key, scheduler_name, tags
                )
                log_line(target_name, evaluated_at_iso, f"{metric_name} error: {msg}")
        else:
            row = rows[0] if rows else tuple()
            if len(row) > 10:
                msg = "SQL returned more than 10 columns (max 10)"
                for mdef in metrics:
                    insert_result_row(
                        main_pg, evaluated_at_iso, target_name, mdef.name, None, None, "config_error",
                        None, None, None, msg, None, target_ds_key, scheduler_name, tags
                    )
                    log_line(target_name, evaluated_at_iso, f"{mdef.name} error: {msg}")
                return _finish(3)

            sql_vals: Dict[str, Any] = {}
            for idx in range(1, 11):
                sql_vals[f"VAL{idx}"] = None
            for idx, val in enumerate(row, start=1):
                sql_vals[f"VAL{idx}"] = val

            for mdef in metrics:
                evaluate_metric(
                    evaluated_at_iso=evaluated_at_iso,
                    target_name=target_name,
                    metric_name=mdef.name,
                    mdef=mdef,
                    sql_vals=sql_vals,
                    alias_map=alias_map,
                    is_muted=is_muted,
                    actions=actions,
                    target_ds_key=target_ds_key,
                    scheduler_name=scheduler_name,
                    tags=tags,
                    sql_timeout=sql_timeout,
                    env=env,
                )

    return _finish(0)
