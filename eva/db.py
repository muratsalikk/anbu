from __future__ import annotations

from dataclasses import dataclass
import re
import threading
from contextlib import contextmanager
from pathlib import Path
from queue import Empty, Queue
from typing import Any, Dict, Iterator, List, Optional, Tuple

try:
    from .engine import read_kv_file, log_line, utc_now_iso
except ImportError:  # Support running as a script from the eva directory.
    from engine import read_kv_file, log_line, utc_now_iso


@dataclass(frozen=True)
class PgConnInfo:
    host: str
    port: int
    dbname: str
    user: str
    password: str


@dataclass(frozen=True)
class Datasource:
    name: str
    type: str  # POSTGRES / ORACLE
    user: str
    password: str
    dsn: str


@dataclass(frozen=True)
class ResultPgPoolConfig:
    min_size: int
    max_size: int
    connect_timeout: int
    acquire_timeout_sec: int


class PooledPgClient:
    def __init__(self, pool: "PgConnectionPool"):
        self._pool = pool

    def _run_with_conn(self, fn):
        conn = self._pool.acquire()
        try:
            return fn(conn)
        finally:
            self._pool.release(conn)

    def execute(self, sql: str, params: Optional[Dict[str, Any]] = None) -> None:
        def _do(conn: Any) -> None:
            with conn.cursor() as cur:
                cur.execute(sql, params or {})

        self._run_with_conn(_do)

    def fetch_one_row(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Tuple[List[str], Tuple[Any, ...]]:
        def _do(conn: Any) -> Tuple[List[str], Tuple[Any, ...]]:
            with conn.cursor() as cur:
                cur.execute(sql, params or {})
                row = cur.fetchone()
                if row is None:
                    raise ValueError("SQL returned 0 rows (expected exactly 1)")
                extra = cur.fetchone()
                if extra is not None:
                    raise ValueError("SQL returned multiple rows (expected exactly 1)")
                cols = [d.name for d in cur.description] if cur.description else []
                return cols, row

        return self._run_with_conn(_do)

    def fetch_all_rows(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Tuple[List[str], List[Tuple[Any, ...]]]:
        cols, rows, _ = self.fetch_rows_limited(
            sql=sql,
            params=params,
            max_rows=None,
        )
        return cols, rows

    def fetch_rows_limited(
        self,
        sql: str,
        params: Optional[Dict[str, Any]] = None,
        max_rows: Optional[int] = None,
        fetch_size: int = 500,
    ) -> Tuple[List[str], List[Tuple[Any, ...]], bool]:
        normalized_limit: Optional[int] = None
        if max_rows is not None:
            normalized_limit = max(1, int(max_rows))
        chunk_size = max(1, int(fetch_size))

        def _do(conn: Any) -> Tuple[List[str], List[Tuple[Any, ...]], bool]:
            with conn.cursor() as cur:
                cur.execute(sql, params or {})
                cols = [d.name for d in cur.description] if cur.description else []
                rows: List[Tuple[Any, ...]] = []
                while True:
                    if normalized_limit is not None and len(rows) >= normalized_limit:
                        break
                    if normalized_limit is None:
                        batch_target = chunk_size
                    else:
                        batch_target = min(chunk_size, normalized_limit - len(rows))
                    if batch_target <= 0:
                        break
                    batch = cur.fetchmany(batch_target)
                    if not batch:
                        break
                    rows.extend(batch)
                truncated = False
                if normalized_limit is not None and len(rows) >= normalized_limit:
                    truncated = bool(cur.fetchmany(1))
                return cols, list(rows), truncated

        return self._run_with_conn(_do)


class PgConnectionPool:
    def __init__(
        self,
        *,
        host: str,
        port: int,
        dbname: str,
        user: str,
        password: str,
        config: ResultPgPoolConfig,
    ) -> None:
        if config.min_size < 1:
            raise ValueError("RESULT_PG_POOL_MIN must be >= 1")
        if config.max_size < config.min_size:
            raise ValueError("RESULT_PG_POOL_MAX must be >= RESULT_PG_POOL_MIN")

        try:
            import psycopg  # type: ignore
        except Exception as e:
            raise RuntimeError("psycopg (v3) is required for Postgres support") from e

        self._psycopg = psycopg
        self._conn_kwargs = {
            "host": host,
            "port": port,
            "dbname": dbname,
            "user": user,
            "password": password,
            "connect_timeout": config.connect_timeout,
            "autocommit": True,
        }
        self._config = config
        self._idle: Queue[Any] = Queue(maxsize=config.max_size)
        self._lock = threading.Lock()
        self._created = 0
        self._closed = False

        for _ in range(config.min_size):
            conn = self._open_conn()
            self._idle.put_nowait(conn)
            self._created += 1

    @property
    def signature(self) -> Tuple[Any, ...]:
        return (
            self._conn_kwargs["host"],
            self._conn_kwargs["port"],
            self._conn_kwargs["dbname"],
            self._conn_kwargs["user"],
            self._config.min_size,
            self._config.max_size,
            self._config.connect_timeout,
            self._config.acquire_timeout_sec,
        )

    def _open_conn(self) -> Any:
        conn = self._psycopg.connect(**self._conn_kwargs)
        try:
            log_line(
                "SYSTEM",
                utc_now_iso(),
                f"pg connected {self._conn_kwargs['host']}:{self._conn_kwargs['port']}/{self._conn_kwargs['dbname']} (pool)",
            )
        except Exception:
            pass
        return conn

    def _dec_created(self) -> None:
        with self._lock:
            if self._created > 0:
                self._created -= 1

    def acquire(self) -> Any:
        with self._lock:
            if self._closed:
                raise RuntimeError("Result DB pool is closed")

        conn = None
        try:
            conn = self._idle.get_nowait()
        except Empty:
            conn = None

        if conn is not None:
            if getattr(conn, "closed", False):
                self._dec_created()
                return self._open_replacement_or_wait()
            return conn

        return self._open_replacement_or_wait()

    def _open_replacement_or_wait(self) -> Any:
        with self._lock:
            if self._closed:
                raise RuntimeError("Result DB pool is closed")
            if self._created < self._config.max_size:
                self._created += 1
                create_new = True
            else:
                create_new = False

        if create_new:
            try:
                return self._open_conn()
            except Exception:
                self._dec_created()
                raise

        conn = self._idle.get(timeout=max(1, self._config.acquire_timeout_sec))
        if getattr(conn, "closed", False):
            self._dec_created()
            return self._open_replacement_or_wait()
        return conn

    def release(self, conn: Any) -> None:
        if conn is None:
            return
        if getattr(conn, "closed", False):
            self._dec_created()
            return

        with self._lock:
            is_closed = self._closed

        if is_closed:
            try:
                conn.close()
            finally:
                self._dec_created()
            return

        try:
            self._idle.put_nowait(conn)
        except Exception:
            try:
                conn.close()
            finally:
                self._dec_created()

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True

        while True:
            try:
                conn = self._idle.get_nowait()
            except Empty:
                break
            try:
                conn.close()
            except Exception:
                pass
            finally:
                self._dec_created()

    @contextmanager
    def client(self) -> Iterator[PooledPgClient]:
        yield PooledPgClient(self)


_RESULT_POOL_LOCK = threading.Lock()
_RESULT_POOL: Optional[PgConnectionPool] = None
_RESULT_POOL_SIGNATURE: Optional[Tuple[Any, ...]] = None


def get_main_pg_from_props(props: Dict[str, str]) -> PgConnInfo:
    host = props.get("PG_HOST", "").strip()
    port = props.get("PG_PORT", "").strip()
    dbname = props.get("PG_DBNAME", "").strip()
    user = props.get("PG_USER", "").strip()
    password = props.get("PG_PASS", "").strip()
    if not (host and port and dbname and user):
        raise ValueError("engine.properties missing PG_* settings for main result DB")
    return PgConnInfo(host=host, port=int(port), dbname=dbname, user=user, password=password)


def init_result_pg_pool(pg_info: PgConnInfo, config: ResultPgPoolConfig) -> None:
    global _RESULT_POOL, _RESULT_POOL_SIGNATURE

    with _RESULT_POOL_LOCK:
        candidate = PgConnectionPool(
            host=pg_info.host,
            port=pg_info.port,
            dbname=pg_info.dbname,
            user=pg_info.user,
            password=pg_info.password,
            config=config,
        )
        sig = candidate.signature

        if _RESULT_POOL is not None and _RESULT_POOL_SIGNATURE == sig:
            candidate.close()
            return

        old_pool = _RESULT_POOL
        _RESULT_POOL = candidate
        _RESULT_POOL_SIGNATURE = sig

    if old_pool is not None:
        old_pool.close()


def close_result_pg_pool() -> None:
    global _RESULT_POOL, _RESULT_POOL_SIGNATURE

    with _RESULT_POOL_LOCK:
        pool = _RESULT_POOL
        _RESULT_POOL = None
        _RESULT_POOL_SIGNATURE = None

    if pool is not None:
        pool.close()


@contextmanager
def get_result_pg_client() -> Iterator[PooledPgClient]:
    with _RESULT_POOL_LOCK:
        pool = _RESULT_POOL
    if pool is None:
        raise RuntimeError("Result DB pool not initialized")
    with pool.client() as client:
        yield client


def verify_result_pg_access() -> None:
    with get_result_pg_client() as client:
        client.fetch_one_row("SELECT 1")


def parse_dsn_host_port_db(dsn: str) -> Tuple[str, int, str]:
    m = re.match(r"^([^:/\s]+):(\d+)/(.*)$", dsn.strip())
    if not m:
        raise ValueError(f"Invalid DSN format (expected host:port/db): {dsn}")
    host = m.group(1)
    port = int(m.group(2))
    db = m.group(3).strip()
    if not db:
        raise ValueError(f"Invalid DSN db/service part: {dsn}")
    return host, port, db


def load_datasources(app_dir: Path, definition_file: Path) -> Dict[str, Datasource]:
    ds_map: Dict[str, Datasource] = {}
    refs = read_kv_file(definition_file)
    base_dir = definition_file.parent

    for ds_name, ref_path in refs.items():
        p = Path(ref_path)
        if not p.is_absolute():
            p = (base_dir / p).resolve()

        if p.exists() and p.is_dir():
            continue
        if not p.exists() or not p.is_file():
            continue

        cfg = read_kv_file(p)
        ds_type = cfg.get("TYPE", "").strip().upper()
        user = cfg.get("USER", "")
        password = cfg.get("PASSWORD", "")
        dsn = cfg.get("DSN", "")
        if not ds_type or not user or not dsn:
            continue

        ds_map[ds_name] = Datasource(ds_name, ds_type, user, password, dsn)

    return ds_map


class PgClient:
    def __init__(self, host: str, port: int, db: str, user: str, password: str, connect_timeout: int = 10):
        try:
            import psycopg  # type: ignore
        except Exception as e:
            raise RuntimeError("psycopg (v3) is required for Postgres support") from e
        self.psycopg = psycopg
        self.host = host
        self.port = port
        self.db = db
        self.user = user
        self.password = password
        self.connect_timeout = connect_timeout
        self.conn = None

    def __enter__(self) -> "PgClient":
        self.conn = self.psycopg.connect(
            host=self.host,
            port=self.port,
            dbname=self.db,
            user=self.user,
            password=self.password,
            connect_timeout=self.connect_timeout,
            autocommit=True,
        )
        try:
            log_line("SYSTEM", utc_now_iso(), f"pg connected {self.host}:{self.port}/{self.db}")
        except Exception:
            pass
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            if self.conn is not None:
                self.conn.close()
        finally:
            self.conn = None

    def execute(self, sql: str, params: Optional[Dict[str, Any]] = None) -> None:
        assert self.conn is not None
        with self.conn.cursor() as cur:
            cur.execute(sql, params or {})

    def fetch_one_row(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Tuple[List[str], Tuple[Any, ...]]:
        assert self.conn is not None
        with self.conn.cursor() as cur:
            cur.execute(sql, params or {})
            row = cur.fetchone()
            if row is None:
                raise ValueError("SQL returned 0 rows (expected exactly 1)")
            extra = cur.fetchone()
            if extra is not None:
                raise ValueError("SQL returned multiple rows (expected exactly 1)")
            cols = [d.name for d in cur.description] if cur.description else []
            return cols, row

    def fetch_all_rows(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Tuple[List[str], List[Tuple[Any, ...]]]:
        cols, rows, _ = self.fetch_rows_limited(
            sql=sql,
            params=params,
            max_rows=None,
        )
        return cols, rows

    def fetch_rows_limited(
        self,
        sql: str,
        params: Optional[Dict[str, Any]] = None,
        max_rows: Optional[int] = None,
        fetch_size: int = 500,
    ) -> Tuple[List[str], List[Tuple[Any, ...]], bool]:
        assert self.conn is not None
        normalized_limit: Optional[int] = None
        if max_rows is not None:
            normalized_limit = max(1, int(max_rows))
        chunk_size = max(1, int(fetch_size))
        with self.conn.cursor() as cur:
            cur.execute(sql, params or {})
            cols = [d.name for d in cur.description] if cur.description else []
            rows: List[Tuple[Any, ...]] = []
            while True:
                if normalized_limit is not None and len(rows) >= normalized_limit:
                    break
                if normalized_limit is None:
                    batch_target = chunk_size
                else:
                    batch_target = min(chunk_size, normalized_limit - len(rows))
                if batch_target <= 0:
                    break
                batch = cur.fetchmany(batch_target)
                if not batch:
                    break
                rows.extend(batch)
            truncated = False
            if normalized_limit is not None and len(rows) >= normalized_limit:
                truncated = bool(cur.fetchmany(1))
            return cols, list(rows), truncated


def insert_result_row(
    main_pg: PgClient | PooledPgClient,
    evaluated_at_iso: str,
    target_name: str,
    metric_name: str,
    metric_value: Optional[int],
    severity: Optional[int],
    state: str,
    critical_val: Optional[int],
    major_val: Optional[int],
    minor_val: Optional[int],
    message: Optional[str],
    action_name: Optional[str],
    datasource: Optional[str],
    scheduler_name: Optional[str],
    tags: Optional[str],
    baseline: Optional[int] = None,
    deviation: Optional[int] = None,
) -> None:
    sql = """
    INSERT INTO anbu_result (
        evaluated_at, target_name, metric_name, metric_value, severity, state,
        critical_val, major_val, minor_val, message, action_name, datasource, scheduler_name, tags,
        baseline, deviation
    ) VALUES (
        %(evaluated_at)s, %(target_name)s, %(metric_name)s, %(metric_value)s, %(severity)s, %(state)s,
        %(critical_val)s, %(major_val)s, %(minor_val)s, %(message)s, %(action_name)s, %(datasource)s, %(scheduler_name)s, %(tags)s,
        %(baseline)s, %(deviation)s
    )
    """
    main_pg.execute(sql, {
        "evaluated_at": evaluated_at_iso,
        "target_name": target_name,
        "metric_name": metric_name,
        "metric_value": metric_value,
        "severity": severity,
        "state": state,
        "critical_val": critical_val,
        "major_val": major_val,
        "minor_val": minor_val,
        "message": message,
        "action_name": action_name,
        "datasource": datasource,
        "scheduler_name": scheduler_name,
        "tags": tags,
        "baseline": baseline,
        "deviation": deviation,
    })


def _normalize_row(row: Any) -> Tuple[Any, ...]:
    if row is None:
        return tuple()
    if isinstance(row, tuple):
        return row
    if isinstance(row, list):
        return tuple(row)
    return (row,)


def escape_psycopg_percent_literals(sql_text: str) -> str:
    if "%" not in sql_text:
        return sql_text
    out: List[str] = []
    i = 0
    while i < len(sql_text):
        ch = sql_text[i]
        if ch == "%":
            if i + 1 < len(sql_text) and sql_text[i + 1] == "%":
                out.append("%%")
                i += 2
                continue
            out.append("%%")
            i += 1
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def execute_target_sql(
    ds: Datasource,
    sql_text: str,
    timeout_sec: int,
    read_only: bool,
) -> Tuple[List[str], Tuple[Any, ...]]:
    ds_type = (ds.type or "").strip().upper()
    timeout_sec = int(timeout_sec or 0)

    if ds_type == "POSTGRES":
        host, port, db = parse_dsn_host_port_db(ds.dsn)
        connect_timeout = 10 if timeout_sec <= 0 else min(10, max(2, timeout_sec))

        with PgClient(host, port, db, ds.user, ds.password, connect_timeout=connect_timeout) as pg:
            if read_only:
                try:
                    pg.execute("SET default_transaction_read_only = on")
                except Exception:
                    pass
            if timeout_sec > 0:
                try:
                    pg.execute("SET statement_timeout = %(ms)s", {"ms": timeout_sec * 1000})
                except Exception:
                    pass

            sql_text = escape_psycopg_percent_literals(sql_text)
            cols, row = pg.fetch_one_row(sql_text)
            return cols, _normalize_row(row)

    if ds_type == "ORACLE":
        try:
            import cx_Oracle  # type: ignore
        except Exception as e:
            raise RuntimeError("Oracle datasource requested but cx_Oracle is not available") from e

        host, port, service = parse_dsn_host_port_db(ds.dsn)
        dsn = cx_Oracle.makedsn(host, port, service_name=service)

        conn = None
        cur = None
        try:
            conn = cx_Oracle.connect(user=ds.user, password=ds.password, dsn=dsn)
            try:
                log_line("SYSTEM", utc_now_iso(), f"oracle connected {host}:{port}/{service}")
            except Exception:
                pass

            if timeout_sec > 0:
                try:
                    conn.callTimeout = timeout_sec * 1000
                except Exception:
                    pass

            cur = conn.cursor()
            if read_only:
                try:
                    cur.execute("SET TRANSACTION READ ONLY")
                except Exception:
                    pass
            cur.execute(sql_text)

            row = cur.fetchone()
            if row is None:
                raise ValueError("SQL returned 0 rows (expected exactly 1)")
            extra = cur.fetchone()
            if extra is not None:
                raise ValueError("SQL returned multiple rows (expected exactly 1)")

            cols = [d[0] for d in cur.description] if cur.description else []
            return cols, _normalize_row(row)

        finally:
            try:
                if cur is not None:
                    cur.close()
            except Exception:
                pass
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass

    raise ValueError(f"Unsupported datasource TYPE={ds.type}")


def _execute_target_sql_multi_impl(
    ds: Datasource,
    sql_text: str,
    timeout_sec: int,
    read_only: bool,
    max_rows: Optional[int] = None,
) -> Tuple[List[str], List[Tuple[Any, ...]], bool]:
    ds_type = (ds.type or "").strip().upper()
    timeout_sec = int(timeout_sec or 0)

    if ds_type == "POSTGRES":
        host, port, db = parse_dsn_host_port_db(ds.dsn)
        connect_timeout = 10 if timeout_sec <= 0 else min(10, max(2, timeout_sec))

        with PgClient(host, port, db, ds.user, ds.password, connect_timeout=connect_timeout) as pg:
            if read_only:
                try:
                    pg.execute("SET default_transaction_read_only = on")
                except Exception:
                    pass
            if timeout_sec > 0:
                try:
                    pg.execute("SET statement_timeout = %(ms)s", {"ms": timeout_sec * 1000})
                except Exception:
                    pass

            sql_text = escape_psycopg_percent_literals(sql_text)
            cols, rows, truncated = pg.fetch_rows_limited(
                sql_text,
                max_rows=max_rows,
            )
            return cols, [_normalize_row(r) for r in rows], truncated

    if ds_type == "ORACLE":
        try:
            import cx_Oracle  # type: ignore
        except Exception as e:
            raise RuntimeError("Oracle datasource requested but cx_Oracle is not available") from e

        host, port, service = parse_dsn_host_port_db(ds.dsn)
        dsn = cx_Oracle.makedsn(host, port, service_name=service)

        conn = None
        cur = None
        try:
            conn = cx_Oracle.connect(user=ds.user, password=ds.password, dsn=dsn)
            try:
                log_line("SYSTEM", utc_now_iso(), f"oracle connected {host}:{port}/{service}")
            except Exception:
                pass

            if timeout_sec > 0:
                try:
                    conn.callTimeout = timeout_sec * 1000
                except Exception:
                    pass

            cur = conn.cursor()
            if read_only:
                try:
                    cur.execute("SET TRANSACTION READ ONLY")
                except Exception:
                    pass
            cur.execute(sql_text)

            cols = [d[0] for d in cur.description] if cur.description else []
            normalized_limit: Optional[int] = None
            if max_rows is not None:
                normalized_limit = max(1, int(max_rows))
            rows: List[Tuple[Any, ...]] = []
            fetch_size = 500
            while True:
                if normalized_limit is not None and len(rows) >= normalized_limit:
                    break
                if normalized_limit is None:
                    batch_target = fetch_size
                else:
                    batch_target = min(fetch_size, normalized_limit - len(rows))
                if batch_target <= 0:
                    break
                batch = cur.fetchmany(batch_target)
                if not batch:
                    break
                rows.extend(_normalize_row(r) for r in batch)
            truncated = False
            if normalized_limit is not None and len(rows) >= normalized_limit:
                truncated = bool(cur.fetchmany(1))
            return cols, rows, truncated

        finally:
            try:
                if cur is not None:
                    cur.close()
            except Exception:
                pass
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass

    raise ValueError(f"Unsupported datasource TYPE={ds.type}")


def execute_target_sql_multi(
    ds: Datasource,
    sql_text: str,
    timeout_sec: int,
    read_only: bool,
) -> Tuple[List[str], List[Tuple[Any, ...]]]:
    cols, rows, _ = _execute_target_sql_multi_impl(
        ds=ds,
        sql_text=sql_text,
        timeout_sec=timeout_sec,
        read_only=read_only,
        max_rows=None,
    )
    return cols, rows


def execute_target_sql_multi_limited(
    ds: Datasource,
    sql_text: str,
    timeout_sec: int,
    read_only: bool,
    max_rows: int,
) -> Tuple[List[str], List[Tuple[Any, ...]], bool]:
    return _execute_target_sql_multi_impl(
        ds=ds,
        sql_text=sql_text,
        timeout_sec=timeout_sec,
        read_only=read_only,
        max_rows=max_rows,
    )
