from __future__ import annotations

import ast
import json
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime, time as dt_time
from decimal import Decimal, InvalidOperation
import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ----------------------------
# Logging / parsing helpers
# ----------------------------


def utc_now_iso() -> str:
    # Backward-compatible function name; returns local wall-clock timestamp with offset.
    return datetime.now().astimezone().replace(microsecond=0).isoformat()


def log_line(target_name: str, evaluated_at_iso: str, message: str) -> None:
    print(f"{evaluated_at_iso} - {target_name} - {message}", flush=True)


def parse_bool(value: str) -> bool:
    return str(value or "").strip().lower() in {"true", "1", "yes", "y"}


def parse_hhmm(value: str) -> Optional[dt_time]:
    if not value:
        return None
    text = value.strip()
    parts = text.split(":")
    if len(parts) < 2:
        return None
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except ValueError:
        return None
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        return None
    return datetime(2000, 1, 1, hour, minute).time()


def parse_datetime(value: str) -> Optional[datetime]:
    if not value:
        return None
    text = value.strip().replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text)
    except Exception:
        return None


BETWEEN_DAY_VALUES = ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]


@dataclass
class BetweenRule:
    start: dt_time
    end: dt_time
    days: set[str]


def normalize_between_days(value: object) -> set[str]:
    if isinstance(value, str):
        raw_items = [part.strip().upper() for part in value.split(",")]
    elif isinstance(value, list):
        raw_items = [str(item).strip().upper() for item in value]
    else:
        raw_items = []
    days: set[str] = set()
    for item in raw_items:
        if item in BETWEEN_DAY_VALUES:
            days.add(item)
    if not days:
        days = set(BETWEEN_DAY_VALUES)
    return days


def parse_between_rules_json(value: str) -> List[BetweenRule]:
    raw = (value or "").strip()
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    rules: List[BetweenRule] = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        start_hm = parse_hhmm(str(item.get("start", "")).strip())
        end_hm = parse_hhmm(str(item.get("end", "")).strip())
        if not start_hm or not end_hm:
            continue
        days = normalize_between_days(item.get("days", []))
        rules.append(
            BetweenRule(
                start=start_hm,
                end=end_hm,
                days=days,
            )
        )
    return rules


def is_within_between_rules(current_local: datetime, between_rules: List[BetweenRule]) -> bool:
    if not between_rules:
        return False
    t = current_local.time()
    weekday = BETWEEN_DAY_VALUES[current_local.weekday()]
    for between_rule in between_rules:
        if between_rule.days and weekday not in between_rule.days:
            continue
        start = between_rule.start
        end = between_rule.end
        if start == end:
            return True
        if start < end:
            if start <= t < end:
                return True
            continue
        if t >= start or t < end:
            return True
    return False


def read_kv_file(path: Path) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def parse_env_file(path: Path) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not path.exists():
        raise FileNotFoundError(f"Missing env: {path}")
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip()
        if len(v) >= 2 and ((v[0] == v[-1] == '"') or (v[0] == v[-1] == "'")):
            quote = v[0]
            body = v[1:-1]
            if quote == '"':
                body = body.replace(r"\"", '"')
            else:
                body = body.replace(r"\'", "'")
            v = body
        out[k] = v
    return out


def load_actions(app_dir: Path, action_def_file: Path) -> Dict[str, str]:
    raw = read_kv_file(action_def_file)
    out: Dict[str, str] = {}
    for k, v in raw.items():
        p = Path(v)
        if not p.is_absolute():
            p = (app_dir / p).resolve()
        out[k.strip()] = str(p)
    return out


def load_schedulers(app_dir: Path, sched_file: Path) -> Dict[str, str]:
    return read_kv_file(sched_file)


def load_engine_props(app_dir: Path) -> Dict[str, str]:
    return read_kv_file(app_dir / "engine.properties")


# ----------------------------
# Safe expression evaluation
# ----------------------------

SAFE_FUNCS = {
    "max": max,
    "min": min,
    "greatest": max,
    "least": min,
}
# Allow uppercase function names as well (e.g. GREATEST)
for _k, _v in list(SAFE_FUNCS.items()):
    SAFE_FUNCS[_k.upper()] = _v

ALLOWED_AST_NODES = (
    ast.Expression,
    ast.BinOp,
    ast.UnaryOp,
    ast.Add, ast.Sub, ast.Mult, ast.Div, ast.FloorDiv, ast.Mod, ast.Pow,
    ast.USub, ast.UAdd,
    ast.Constant,
    ast.Name,
    ast.Load,
    ast.Expr,
    ast.Call,
)


def safe_eval_expr(expr: str, ctx: Dict[str, Any]) -> float:
    expr = expr.strip()
    if expr == "":
        raise ValueError("Empty expression")

    tree = ast.parse(expr, mode="eval")
    safe_ctx = dict(ctx)
    safe_ctx.update(SAFE_FUNCS)

    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            if not isinstance(node.func, ast.Name) or node.func.id not in SAFE_FUNCS:
                raise ValueError("Disallowed function")
            if node.keywords:
                raise ValueError("Keywords not allowed")
        if isinstance(node, (ast.Attribute, ast.Subscript, ast.Lambda, ast.Dict, ast.List, ast.Tuple, ast.Set, ast.Compare, ast.IfExp)):
            raise ValueError(f"Disallowed expression element: {type(node).__name__}")
        if not isinstance(node, ALLOWED_AST_NODES):
            if node.__class__ is ast.AST:
                continue
            raise ValueError(f"Disallowed AST node: {type(node).__name__}")
        if isinstance(node, ast.Name):
            if node.id not in safe_ctx:
                raise ValueError(f"Unknown identifier: {node.id}")

    code = compile(tree, "<expr>", "eval")
    val = eval(code, {"__builtins__": {}}, safe_ctx)
    if val is None:
        raise ValueError("Expression evaluated to null")
    try:
        return float(val)
    except Exception as e:
        raise ValueError(f"Expression not numeric: {val}") from e


def normalize_operator(op: str) -> str:
    op = op.strip()
    if op == "=<":
        return "<="
    if op == "=>":
        return ">="
    if op == "=":
        return "=="
    if op in ("<", ">", "<=", ">=", "=="):
        return op
    raise ValueError(f"Unsupported operator: {op}")


def apply_operator(left: float, op: str, right: float) -> bool:
    op = normalize_operator(op)
    if op == "==":
        return left == right
    if op == "<":
        return left < right
    if op == ">":
        return left > right
    if op == "<=":
        return left <= right
    if op == ">=":
        return left >= right
    raise ValueError(f"Unsupported operator after normalize: {op}")


# ----------------------------
# Metrics / conditions parsing
# ----------------------------

SEV_ORDER = ["CRITICAL", "MAJOR", "MINOR"]
SEV_TO_NUM = {"NORMAL": 1, "MINOR": 2, "MAJOR": 3, "CRITICAL": 4}


@dataclass
class Condition:
    idx: int
    operator: str
    val_expr: str
    action: str
    msg: str
    timeframe_rules: List[BetweenRule]


@dataclass
class MetricDef:
    i: int
    name: str
    value_ref: str
    normal_action: str
    normal_msg: str
    conditions: Dict[str, List[Condition]]
    legacy_if: Dict[str, str]


def metric_key(i: int, suffix: str) -> str:
    return f"METRIC_{i}_{suffix}"


def parse_metric_defs(env: Dict[str, str]) -> List[MetricDef]:
    mc = int(env.get("METRIC_COUNT", "0") or "0")
    sql_mode = (env.get("SQL_MODE", "single") or "single").strip().lower()
    max_metrics = 100 if sql_mode == "multiline" else 10
    if mc < 1 or mc > max_metrics:
        raise ValueError(f"METRIC_COUNT must be 1..{max_metrics}")

    metrics: List[MetricDef] = []
    for i in range(1, mc + 1):
        name = env.get(metric_key(i, "NAME"), "").strip()
        value_ref = env.get(metric_key(i, "VALUE"), "").strip()
        normal_action = env.get(metric_key(i, "NORMAL_ACTION"), "").strip()
        normal_msg = env.get(metric_key(i, "NORMAL_MSG"), "").strip()
        if not name or not value_ref:
            raise ValueError(f"Metric {i} missing NAME or VALUE")

        conditions: Dict[str, List[Condition]] = {s: [] for s in SEV_ORDER}
        legacy_if: Dict[str, str] = {}

        for sev in SEV_ORDER:
            prefix = f"METRIC_{i}_{sev}_"
            idxs = set()
            for k in env.keys():
                if k.startswith(prefix):
                    m = re.match(
                        rf"^{re.escape(prefix)}(\d+)_(OPERATOR|VAL|ACTION|MSG|TIMEFRAME)$",
                        k,
                    )
                    if m:
                        idxs.add(int(m.group(1)))
            for idx in sorted(idxs):
                op = env.get(f"{prefix}{idx}_OPERATOR", "").strip()
                ve = env.get(f"{prefix}{idx}_VAL", "").strip()
                ac = env.get(f"{prefix}{idx}_ACTION", "").strip()
                ms = env.get(f"{prefix}{idx}_MSG", "").strip()
                tf = parse_between_rules_json(
                    env.get(f"{prefix}{idx}_TIMEFRAME", "").strip()
                )
                if len(tf) > 1:
                    raise ValueError(
                        f"{prefix}{idx}_TIMEFRAME supports only one interval"
                    )
                if not op or not ve:
                    continue
                conditions[sev].append(
                    Condition(
                        idx=idx,
                        operator=op,
                        val_expr=ve,
                        action=ac,
                        msg=ms,
                        timeframe_rules=tf,
                    )
                )

            if not conditions[sev]:
                op = env.get(f"METRIC_{i}_{sev}_OPERATOR", "").strip()
                ve = env.get(f"METRIC_{i}_{sev}_VAL", "").strip()
                ac = env.get(f"METRIC_{i}_{sev}_ACTION", "").strip()
                ms = env.get(f"METRIC_{i}_{sev}_MSG", "").strip()
                tf = parse_between_rules_json(
                    env.get(f"METRIC_{i}_{sev}_TIMEFRAME", "").strip()
                )
                if len(tf) > 1:
                    raise ValueError(
                        f"METRIC_{i}_{sev}_TIMEFRAME supports only one interval"
                    )
                if op and ve:
                    conditions[sev].append(
                        Condition(
                            idx=1,
                            operator=op,
                            val_expr=ve,
                            action=ac,
                            msg=ms,
                            timeframe_rules=tf,
                        )
                    )

            if not conditions[sev]:
                if_expr = env.get(f"METRIC_{i}_{sev}_IF", "").strip()
                if if_expr:
                    legacy_if[sev] = if_expr

        metrics.append(MetricDef(
            i=i,
            name=name,
            value_ref=value_ref,
            normal_action=normal_action,
            normal_msg=normal_msg,
            conditions=conditions,
            legacy_if=legacy_if,
        ))
    return metrics


def build_alias_map(env: Dict[str, str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for n in range(1, 11):
        k = f"MAP_VAL{n}"
        alias = env.get(k, "").strip()
        if alias:
            out[alias] = f"VAL{n}"
    return out


# ----------------------------
# Placeholders / legacy parsing
# ----------------------------

PLACEHOLDER_RE = re.compile(r"\{\{\s*([A-Z0-9_]+)\s*\}\}")
IF_RE = re.compile(r"^\s*([A-Z0-9_]+|\-?\d+(\.\d+)?)\s*(==|=|=<|=>|<=|>=|<|>)\s*(.+?)\s*$")
_SQL_FORBIDDEN_RE = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|ALTER|DROP|CREATE|GRANT|REVOKE|TRUNCATE|CALL|EXECUTE|COMMIT|ROLLBACK)\b",
    re.IGNORECASE,
)


def render_message(template: str, ctx: Dict[str, Any]) -> str:
    def repl(m: re.Match) -> str:
        key = m.group(1)
        val = ctx.get(key)
        return format_numeric_display(val)
    return PLACEHOLDER_RE.sub(repl, template or "")


def is_read_only_sql(sql_text: str) -> bool:
    if not sql_text or not sql_text.strip():
        return False
    text = sql_text
    text = re.sub(r"/\*.*?\*/", " ", text, flags=re.DOTALL)
    text = re.sub(r"--.*?$", " ", text, flags=re.MULTILINE)
    text = re.sub(r"'([^']|'')*'", " ", text)
    stripped = text.strip()
    if not stripped:
        return False
    if not re.match(r"^(WITH|SELECT)\b", stripped, flags=re.IGNORECASE):
        return False
    if ";" in stripped:
        parts = stripped.split(";")
        if any(part.strip() for part in parts[1:]):
            return False
    if _SQL_FORBIDDEN_RE.search(stripped):
        return False
    return True


def parse_legacy_if(expr: str) -> Tuple[str, str, str]:
    m = IF_RE.match(expr.strip())
    if not m:
        raise ValueError(f"Legacy IF format invalid: {expr}")
    lhs = m.group(1)
    op = m.group(3)
    rhs = m.group(4)
    return lhs, op, rhs


def resolve_value_ref(value_ref: str, ctx: Dict[str, Any]) -> Optional[float]:
    key = value_ref.strip()
    if key not in ctx:
        return None
    v = ctx[key]
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _coerce_decimal(value: Any) -> Optional[Decimal]:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return Decimal(str(value))
    return None


def format_numeric_display(value: Any, *, truncate: bool = False) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(value)
    number = _coerce_decimal(value)
    if number is None:
        return str(value)
    if truncate:
        try:
            return str(int(number))
        except (InvalidOperation, ValueError, OverflowError):
            return str(value)
    try:
        if number == number.to_integral_value():
            return str(int(number))
    except (InvalidOperation, ValueError, OverflowError):
        return str(value)
    return str(value)


def compute_context(
    target_name: str,
    metric_name: str,
    sql_vals: Dict[str, Any],
    alias_map: Dict[str, str],
) -> Dict[str, Any]:
    ctx: Dict[str, Any] = {}
    ctx.update(sql_vals)
    ctx["TARGET_NAME"] = target_name
    ctx["METRIC_NAME"] = metric_name
    for alias, valn in alias_map.items():
        if alias in ctx:
            continue
        ctx[alias] = sql_vals.get(valn)
    return ctx


def run_action(
    script_path: str,
    target_name: str,
    metric_name: str,
    metric_value: Optional[int],
    severity: int,
    message: str,
    timeout: int,
) -> Tuple[bool, str]:
    args = [
        script_path,
        str(target_name),
        str(metric_name),
        format_numeric_display(metric_value, truncate=True),
        str(severity),
        message or "",
    ]
    try:
        res = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
        if res.returncode != 0:
            err = (res.stderr or "").strip()
            out = (res.stdout or "").strip()
            detail = err or out or f"action exit code {res.returncode}"
            return False, detail[:2000]
        return True, "ok"
    except subprocess.TimeoutExpired:
        return False, "action timeout"
    except Exception as e:
        return False, f"action exception: {e}"
