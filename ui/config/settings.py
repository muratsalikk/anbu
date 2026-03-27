from __future__ import annotations

from pathlib import Path
import os
import sys


BASE_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = BASE_DIR.parent

if str(PROJECT_ROOT) not in sys.path:
    # Keep project root for eva/* imports, but prefer modules inside ui/.
    sys.path.append(str(PROJECT_ROOT))


def _resolve_path(raw: str, fallback: str) -> Path:
    value = (raw or fallback).strip()
    path = Path(value)
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    return path


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

_ds_host = (
    os.getenv("DS_HOST", "").strip()
    or os.getenv("PGHOST", "").strip()
)
_ds_port = (
    os.getenv("DS_PORT", "").strip()
    or os.getenv("PGPORT", "").strip()
    or "5432"
)
_ds_dbname = (
    os.getenv("DS_DBNAME", "").strip()
    or os.getenv("PGDATABASE", "").strip()
)
_ds_user = (
    os.getenv("DS_USER", "").strip()
    or os.getenv("PGUSER", "").strip()
)
_ds_pass = (
    os.getenv("DS_PASS", "").strip()
    or os.getenv("PGPASSWORD", "").strip()
)

if not (_ds_host and _ds_dbname and _ds_user):
    raise RuntimeError(
        "PostgreSQL bootstrap config is required. Set DS_HOST/DS_DBNAME/DS_USER "
        "or PGHOST/PGDATABASE/PGUSER in environment."
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

ANBU_RULES_DIR = _resolve_path("", "./rules")
ANBU_SAVED_QUERIES_DIR = _resolve_path(
    "",
    "./saved_queries",
)
ANBU_APP_LOGO_FILE = _resolve_path(
    "",
    "./ui/static/assets/ico.png",
)
ANBU_AI_PROMPT_FILE = _resolve_path(
    "",
    "./prompts/target_ai_prompt.txt",
)
# Datasource/action/application/helper properties are stored in default DB (property_file).
ANBU_DATASOURCE_DEFINITION_FILE = None
