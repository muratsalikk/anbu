from __future__ import annotations

import os
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from queue import Empty, Queue
from typing import Dict, List

from croniter import croniter  # type: ignore

from engine import (
    BETWEEN_DAY_VALUES,
    BetweenRule,
    is_within_between_rules,
    load_engine_props,
    log_line,
    parse_bool,
    parse_between_rules_json,
    parse_datetime,
    parse_env_file,
    parse_hhmm,
    utc_now_iso,
)
from db import (
    PgConnInfo,
    ResultPgPoolConfig,
    close_result_pg_pool,
    get_main_pg_from_props,
    init_result_pg_pool,
    load_datasources,
    parse_dsn_host_port_db,
    verify_result_pg_access,
)
from evaluator import run_target

@dataclass
class RuleHeader:
    target_name: str
    schedule_cron: str
    is_active: bool
    is_muted: bool
    mute_between_enabled: bool
    mute_between_rules: List[BetweenRule]
    mute_until_enabled: bool
    mute_until: datetime | None


@dataclass
class SchedulerConfig:
    app_dir: Path
    rules_dir: Path


def get_app_dir() -> Path:
    return Path(__file__).resolve().parent


def resolve_prop_path(app_dir: Path, raw_value: str, prop_name: str) -> Path:
    value = (raw_value or "").strip()
    if not value:
        raise ValueError(f"engine.properties missing {prop_name}")
    p = Path(value)
    if not p.is_absolute():
        p = (app_dir / p).resolve()
    return p


def validate_required_files(app_dir: Path) -> None:
    props = load_engine_props(app_dir)

    rules_dir = resolve_prop_path(app_dir, props.get("RULES_DIR", ""), "RULES_DIR")
    if not rules_dir.exists() or not rules_dir.is_dir():
        raise FileNotFoundError(f"RULES_DIR not found or not a directory: {rules_dir}")

    for key in ("DATASOURCE_DEFINITION_FILE", "ACTION_DEFINITION_FILE"):
        p = resolve_prop_path(app_dir, props.get(key, ""), key)
        if not p.exists() or not p.is_file():
            raise FileNotFoundError(f"{key} not found: {p}")

    scheduler_file = (props.get("SCHEDULER_DEFINITION_FILE") or "").strip()
    if scheduler_file:
        p = resolve_prop_path(app_dir, scheduler_file, "SCHEDULER_DEFINITION_FILE")
        if not p.exists() or not p.is_file():
            raise FileNotFoundError(f"SCHEDULER_DEFINITION_FILE not found: {p}")


def parse_int_prop(props: Dict[str, str], key: str, default: int, minimum: int = 1) -> int:
    raw = (props.get(key, "") or "").strip()
    if not raw:
        return default
    try:
        val = int(raw)
    except Exception as e:
        raise ValueError(f"{key} must be an integer") from e
    if val < minimum:
        raise ValueError(f"{key} must be >= {minimum}")
    return val


def load_result_pool_config(props: Dict[str, str]) -> ResultPgPoolConfig:
    min_size = parse_int_prop(props, "RESULT_PG_POOL_MIN", 2, minimum=1)
    max_size = parse_int_prop(props, "RESULT_PG_POOL_MAX", 20, minimum=1)
    if max_size < min_size:
        raise ValueError("RESULT_PG_POOL_MAX must be >= RESULT_PG_POOL_MIN")
    connect_timeout = parse_int_prop(props, "RESULT_PG_POOL_CONNECT_TIMEOUT_SEC", 10, minimum=1)
    acquire_timeout = parse_int_prop(props, "RESULT_PG_POOL_ACQUIRE_TIMEOUT_SEC", 30, minimum=1)
    return ResultPgPoolConfig(
        min_size=min_size,
        max_size=max_size,
        connect_timeout=connect_timeout,
        acquire_timeout_sec=acquire_timeout,
    )


def resolve_result_pg_info(app_dir: Path, props: Dict[str, str]) -> PgConnInfo:
    main_ds_key = (props.get("MAIN_POSTGRES_DATASOURCE_KEY") or props.get("RESULT_DATASOURCE_KEY") or "").strip()
    if main_ds_key:
        ds_def_file = resolve_prop_path(
            app_dir,
            props.get("DATASOURCE_DEFINITION_FILE", ""),
            "DATASOURCE_DEFINITION_FILE",
        )
        datasources = load_datasources(app_dir, ds_def_file)
        if main_ds_key not in datasources:
            raise ValueError(f"MAIN/RESULT datasource not found: {main_ds_key}")
        ds = datasources[main_ds_key]
        if (ds.type or "").strip().upper() != "POSTGRES":
            raise ValueError(f"MAIN/RESULT datasource must be POSTGRES: {main_ds_key}")
        host, port, db = parse_dsn_host_port_db(ds.dsn)
        return PgConnInfo(host=host, port=port, dbname=db, user=ds.user, password=ds.password)

    return get_main_pg_from_props(props)


def load_scheduler_config(app_dir: Path) -> SchedulerConfig:
    props = load_engine_props(app_dir)
    rules_dir_p = resolve_prop_path(app_dir, props.get("RULES_DIR", ""), "RULES_DIR")

    return SchedulerConfig(
        app_dir=app_dir,
        rules_dir=rules_dir_p,
    )


def load_rule_headers(rules_dir: Path) -> List[RuleHeader]:
    rules: List[RuleHeader] = []
    for env_path in sorted(rules_dir.glob("*.env")):
        try:
            env = parse_env_file(env_path)
            target_name = env.get("TARGET_NAME", env_path.stem).strip() or env_path.stem
            schedule_cron = env.get("SCHEDULE_CRON", "").strip()
            is_active = parse_bool(env.get("IS_ACTIVE", "false"))
            is_muted = parse_bool(env.get("IS_MUTED", "false"))
            mute_between_enabled = parse_bool(env.get("MUTE_BETWEEN_ENABLED", "false"))
            mute_until_enabled = parse_bool(env.get("MUTE_UNTIL_ENABLED", "false"))
            mute_between_rules = parse_between_rules_json(
                env.get("MUTE_BETWEEN_RULES", "")
            )
            if not mute_between_rules:
                # Backward compatibility for old files.
                legacy_start = parse_hhmm(env.get("MUTE_BETWEEN_START", ""))
                legacy_end = parse_hhmm(env.get("MUTE_BETWEEN_END", ""))
                if legacy_start and legacy_end:
                    mute_between_rules = [
                        BetweenRule(
                            start=legacy_start,
                            end=legacy_end,
                            days=set(BETWEEN_DAY_VALUES),
                        )
                    ]
            mute_until = parse_datetime(env.get("MUTE_UNTIL", ""))
            if not mute_between_enabled and mute_between_rules:
                mute_between_enabled = True
            if not mute_until_enabled and mute_until:
                mute_until_enabled = True
            rules.append(
                RuleHeader(
                    target_name=target_name,
                    schedule_cron=schedule_cron,
                    is_active=is_active,
                    is_muted=is_muted,
                    mute_between_enabled=mute_between_enabled,
                    mute_between_rules=mute_between_rules,
                    mute_until_enabled=mute_until_enabled,
                    mute_until=mute_until,
                )
            )
        except Exception as e:
            log_line(env_path.stem, utc_now_iso(), f"rule load failed: {e}")
    return rules


def should_run_now(rule: RuleHeader, evaluated_at_local: datetime) -> bool:
    if not rule.schedule_cron:
        return True
    # croniter works with naive datetime; evaluate against local wall clock.
    base = (evaluated_at_local - timedelta(minutes=1)).replace(tzinfo=None)
    try:
        itr = croniter(rule.schedule_cron, base)
        next_run = itr.get_next(datetime)
        return next_run.replace(second=0, microsecond=0) == evaluated_at_local.replace(tzinfo=None)
    except Exception as e:
        log_line(rule.target_name, evaluated_at_local.isoformat(), f"schedule_cron invalid: {e}")
        return False


def compute_mute(rule: RuleHeader, next_minute_local: datetime) -> bool:
    muted = False
    if rule.mute_between_enabled and rule.mute_between_rules:
        muted = is_within_between_rules(next_minute_local, rule.mute_between_rules)
    if rule.mute_until_enabled and rule.mute_until:
        if next_minute_local <= rule.mute_until:
            muted = True
    return muted


def sleep_until_next_minute() -> None:
    now = datetime.now().astimezone()
    next_minute = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
    time.sleep(max(0, (next_minute - now).total_seconds()))


def sleep_until_end_of_minute() -> None:
    now = datetime.now()
    next_minute = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
    sleep_for = (next_minute - now).total_seconds() - 1
    if sleep_for > 0:
        time.sleep(sleep_for)


def handle_target_future(
    future: Future,
    stop_event: threading.Event,
    fatal_errors: Queue[BaseException],
    target_name: str,
    evaluated_at_iso: str,
) -> None:
    if stop_event.is_set():
        return
    exc = future.exception()
    if exc is None:
        return
    if isinstance(exc, Empty):
        log_line(target_name, evaluated_at_iso, "target skipped: result DB pool acquire timeout")
        return
    log_line(target_name, evaluated_at_iso, f"fatal target error: {exc}")
    if fatal_errors.empty():
        try:
            fatal_errors.put_nowait(exc)
        except Exception:
            pass
    stop_event.set()


def scheduler_loop(
    stop_event: threading.Event,
    config: SchedulerConfig,
    mute_state: Dict[str, bool],
    mute_lock: threading.Lock,
    executor: ThreadPoolExecutor,
    fatal_errors: Queue[BaseException],
) -> None:
    while not stop_event.is_set():
        try:
            sleep_until_next_minute()
            evaluated_at = datetime.now().astimezone().replace(second=0, microsecond=0)
            evaluated_at_iso = evaluated_at.isoformat()
            log_line("SYSTEM", evaluated_at_iso, "scheduler tick")
            rules = load_rule_headers(config.rules_dir)
            for rule in rules:
                if not should_run_now(rule, evaluated_at):
                    continue
                with mute_lock:
                    dynamic_mute = mute_state.get(rule.target_name, False)
                fut = executor.submit(
                    run_target,
                    config.app_dir,
                    rule.target_name,
                    evaluated_at_iso,
                    dynamic_mute,
                )
                fut.add_done_callback(
                    lambda f, tn=rule.target_name, eiso=evaluated_at_iso: handle_target_future(
                        f, stop_event, fatal_errors, tn, eiso
                    )
                )
        except Exception as e:
            log_line("SYSTEM", utc_now_iso(), f"scheduler error: {e}")
            if fatal_errors.empty():
                try:
                    fatal_errors.put_nowait(e)
                except Exception:
                    pass
            stop_event.set()
            return


def mute_loop(
    stop_event: threading.Event,
    config: SchedulerConfig,
    mute_state: Dict[str, bool],
    mute_lock: threading.Lock,
) -> None:
    while not stop_event.is_set():
        try:
            sleep_until_end_of_minute()
            next_minute_local = (datetime.now() + timedelta(minutes=1)).replace(second=0, microsecond=0)
            rules = load_rule_headers(config.rules_dir)
            updates: Dict[str, bool] = {}
            for rule in rules:
                updates[rule.target_name] = compute_mute(rule, next_minute_local)
            with mute_lock:
                mute_state.clear()
                mute_state.update(updates)
        except Exception as e:
            log_line("SYSTEM", utc_now_iso(), f"mute loop error: {e}")
            time.sleep(1)


def main() -> None:
    app_dir = get_app_dir()
    validate_required_files(app_dir)
    props = load_engine_props(app_dir)
    pool_config = load_result_pool_config(props)
    result_pg_info = resolve_result_pg_info(app_dir, props)
    try:
        init_result_pg_pool(result_pg_info, pool_config)
        verify_result_pg_access()
    except Exception:
        close_result_pg_pool()
        raise
    config = load_scheduler_config(app_dir)
    stop_event = threading.Event()
    fatal_errors: Queue[BaseException] = Queue(maxsize=1)
    mute_state: Dict[str, bool] = {}
    mute_lock = threading.Lock()
    executor = ThreadPoolExecutor(max_workers=max(2, (os.cpu_count() or 2)))

    scheduler_thread = threading.Thread(
        target=scheduler_loop,
        args=(stop_event, config, mute_state, mute_lock, executor, fatal_errors),
        name="anbu-scheduler",
        daemon=True,
    )
    mute_thread = threading.Thread(
        target=mute_loop,
        args=(stop_event, config, mute_state, mute_lock),
        name="anbu-mute",
        daemon=True,
    )
    scheduler_thread.start()
    mute_thread.start()
    log_line("SYSTEM", utc_now_iso(), "baseline procedure loop disabled (python per-metric baseline active)")
    log_line("SYSTEM", utc_now_iso(), "engine started")

    try:
        while True:
            try:
                fatal = fatal_errors.get(timeout=1)
            except Empty:
                if not scheduler_thread.is_alive():
                    raise RuntimeError("scheduler thread stopped unexpectedly")
                if not mute_thread.is_alive():
                    raise RuntimeError("mute thread stopped unexpectedly")
                continue
            raise fatal
    except KeyboardInterrupt:
        stop_event.set()
        scheduler_thread.join(timeout=5)
        mute_thread.join(timeout=5)
        executor.shutdown(wait=True)
        close_result_pg_pool()
    except Exception:
        stop_event.set()
        scheduler_thread.join(timeout=5)
        mute_thread.join(timeout=5)
        executor.shutdown(wait=True)
        close_result_pg_pool()
        raise


if __name__ == "__main__":
    main()
