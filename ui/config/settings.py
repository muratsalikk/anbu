from __future__ import annotations

from pathlib import Path
import os
import sys

import runtime_config


BASE_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = runtime_config.PROJECT_ROOT

if str(PROJECT_ROOT) not in sys.path:
    # Keep project root for eva/* imports, but prefer modules inside ui/.
    sys.path.append(str(PROJECT_ROOT))


for _key, _value in runtime_config.load_bootstrap_env().items():
    if str(_value or "").strip():
        os.environ.setdefault(_key, str(_value))


def _resolve_path(raw: str, fallback: str) -> Path:
    value = (raw or fallback).strip()
    path = Path(value)
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    return path


def _env_value(*keys: str, default: str = "") -> str:
    for key in keys:
        value = os.getenv(key, "").strip()
        if value:
            return value
    return default


SECRET_KEY = os.getenv(
    "DJANGO_SECRET_KEY",
    "unsafe-dev-secret-key-change-in-production",
)

DEBUG = os.getenv("DJANGO_DEBUG", "1").strip() in {"1", "true", "True", "yes", "YES"}
ALLOWED_HOSTS = os.getenv("DJANGO_ALLOWED_HOSTS", "*").split(",")

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "apps.targets",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "config.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "config.wsgi.application"
ASGI_APPLICATION = "config.asgi.application"

_ds_host = _env_value("PG_HOST", "DS_HOST", "PGHOST")
_ds_port = _env_value("PG_PORT", "DS_PORT", "PGPORT", default="5432")
_ds_dbname = _env_value("PG_DBNAME", "DS_DBNAME", "PGDATABASE")
_ds_user = _env_value("PG_USER", "DS_USER", "PGUSER")
_ds_pass = _env_value("PG_PASS", "DS_PASS", "PGPASSWORD")

if not (_ds_host and _ds_dbname and _ds_user):
    raise RuntimeError(
        "PostgreSQL bootstrap config is required. Set PG_HOST/PG_DBNAME/PG_USER "
        f"in {runtime_config.UI_ENV_PATH} or via environment."
    )

_postgres_db: dict[str, str | int] = {
    "ENGINE": "django.db.backends.postgresql",
    "HOST": _ds_host,
    "PORT": _ds_port,
    "NAME": _ds_dbname,
    "USER": _ds_user,
    "PASSWORD": _ds_pass,
    "CONN_MAX_AGE": 60,
}

DATABASES = {
    "default": dict(_postgres_db),
    "data_store": dict(_postgres_db),
}

DATABASE_ROUTERS = ["apps.targets.db_router.TargetsDbRouter"]

AUTH_PASSWORD_VALIDATORS = []

LANGUAGE_CODE = "en-us"
TIME_ZONE = os.getenv("ANBU_TIME_ZONE", "Europe/Istanbul").strip() or "Europe/Istanbul"
USE_I18N = True
USE_TZ = True

STATIC_URL = "/static/"
STATICFILES_DIRS = [BASE_DIR / "static"]
STATIC_ROOT = BASE_DIR / "staticfiles"

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

LOGIN_URL = "login"
LOGIN_REDIRECT_URL = "targets:list"
LOGOUT_REDIRECT_URL = "login"

ANBU_APP_CONFIG_FILE = runtime_config.UI_ENV_PATH
ANBU_RULES_DIR = runtime_config.resolve_engine_path(
    _env_value("RULES_DIR", default=runtime_config.APP_CONFIG_DEFAULTS["RULES_DIR"])
)
ANBU_DATASOURCE_DEFINITION_FILE = runtime_config.resolve_engine_path(
    _env_value(
        "DATASOURCE_DEFINITION_FILE",
        default=runtime_config.APP_CONFIG_DEFAULTS["DATASOURCE_DEFINITION_FILE"],
    )
)
ANBU_ACTION_DEFINITION_FILE = runtime_config.resolve_engine_path(
    _env_value(
        "ACTION_DEFINITION_FILE",
        default=runtime_config.APP_CONFIG_DEFAULTS["ACTION_DEFINITION_FILE"],
    )
)
ANBU_SAVED_QUERIES_DIR = _resolve_path(
    _env_value(
        "SAVED_QUERIES_DIR",
        default=runtime_config.APP_CONFIG_DEFAULTS["SAVED_QUERIES_DIR"],
    ),
    runtime_config.APP_CONFIG_DEFAULTS["SAVED_QUERIES_DIR"],
)
ANBU_APP_LOGO_FILE = _resolve_path(
    _env_value(
        "APP_LOGO_FILE",
        default=runtime_config.APP_CONFIG_DEFAULTS["APP_LOGO_FILE"],
    ),
    runtime_config.APP_CONFIG_DEFAULTS["APP_LOGO_FILE"],
)
ANBU_AI_PROMPT_FILE = _resolve_path(
    _env_value(
        "AI_PROMPT_FILE",
        default=runtime_config.APP_CONFIG_DEFAULTS["AI_PROMPT_FILE"],
    ),
    runtime_config.APP_CONFIG_DEFAULTS["AI_PROMPT_FILE"],
)
