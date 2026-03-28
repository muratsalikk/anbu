from __future__ import annotations

from pathlib import Path
import re


UI_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = UI_DIR.parent
ENGINE_DIR = PROJECT_ROOT / "eva"
UI_ENV_PATH = UI_DIR / ".env"
LEGACY_UI_PROPERTIES_PATH = UI_DIR / "application.properties"
LEGACY_ENGINE_PROPERTIES_PATH = ENGINE_DIR / "engine.properties"

APP_CONFIG_DEFAULTS: dict[str, str] = {
    "RULES_DIR": "../rules",
    "DATASOURCE_DEFINITION_FILE": "./datasources.properties",
    "ACTION_DEFINITION_FILE": "./actions.properties",
    "PG_HOST": "localhost",
    "PG_PORT": "5432",
    "PG_DBNAME": "anbudb",
    "PG_USER": "anbuuser",
    "PG_PASS": "anbupass",
    "PG_DSN": "",
    "HELPER_TEXT_FILE": "",
    "SAVED_QUERIES_DIR": "./saved_queries",
    "ENGINE_LOG_FILE": "",
    "UI_LOG_FILE": "",
    "BACKUP_SCRIPT": "",
    "APP_LOGO_FILE": "./ui/static/assets/ico.png",
    "AI_PROMPT_FILE": "./prompts/target_ai_prompt.txt",
}

APP_CONFIG_KEY_ORDER = [
    "RULES_DIR",
    "DATASOURCE_DEFINITION_FILE",
    "ACTION_DEFINITION_FILE",
    "PG_HOST",
    "PG_PORT",
    "PG_DBNAME",
    "PG_USER",
    "PG_PASS",
    "PG_DSN",
    "HELPER_TEXT_FILE",
    "SAVED_QUERIES_DIR",
    "ENGINE_LOG_FILE",
    "UI_LOG_FILE",
    "BACKUP_SCRIPT",
    "APP_LOGO_FILE",
    "AI_PROMPT_FILE",
]

_LINE_RE = re.compile(r"^([^=]+)=(.*)$")
_DS_TO_PG = {
    "DS_HOST": "PG_HOST",
    "DS_PORT": "PG_PORT",
    "DS_DBNAME": "PG_DBNAME",
    "DS_USER": "PG_USER",
    "DS_PASS": "PG_PASS",
}


def parse_kv_text(text: str) -> dict[str, str]:
    data: dict[str, str] = {}
    for raw in str(text or "").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        match = _LINE_RE.match(line)
        if not match:
            continue
        key = match.group(1).strip()
        value = match.group(2).strip()
        if value.startswith(("'", '"')) and len(value) >= 2 and value[-1] == value[0]:
            quote = value[0]
            value = value[1:-1]
            if quote == '"':
                value = value.replace('\\"', '"')
            elif quote == "'":
                value = value.replace("\\'", "'")
        data[key] = value
    return data


def read_kv_file(path: Path) -> dict[str, str]:
    if not path.exists() or not path.is_file():
        return {}
    return parse_kv_text(path.read_text(encoding="utf-8"))


def load_ui_env() -> dict[str, str]:
    loaded: dict[str, str] = {}
    for candidate in (UI_ENV_PATH, LEGACY_UI_PROPERTIES_PATH, LEGACY_ENGINE_PROPERTIES_PATH):
        loaded = read_kv_file(candidate)
        if loaded:
            break
    for legacy_key, pg_key in _DS_TO_PG.items():
        if not loaded.get(pg_key) and loaded.get(legacy_key):
            loaded[pg_key] = str(loaded.get(legacy_key) or "")
    merged = dict(loaded)
    for key, default in APP_CONFIG_DEFAULTS.items():
        merged.setdefault(key, default)
    return merged


def load_bootstrap_env() -> dict[str, str]:
    return load_ui_env()


def resolve_project_path(raw_value: str, fallback: str = "") -> Path:
    value = str(raw_value or "").strip() or str(fallback or "").strip()
    path = Path(value)
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    return path


def resolve_engine_path(raw_value: str, fallback: str = "") -> Path:
    value = str(raw_value or "").strip() or str(fallback or "").strip()
    path = Path(value)
    if not path.is_absolute():
        path = (ENGINE_DIR / path).resolve()
    return path
