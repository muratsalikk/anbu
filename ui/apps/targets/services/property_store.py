from __future__ import annotations

from pathlib import Path
import re


_PROP_RE = re.compile(r"^([^=]+)=(.*)$")


def parse_kv_text(text: str) -> dict[str, str]:
    data: dict[str, str] = {}
    for raw in (text or "").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        match = _PROP_RE.match(line)
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


def read_file_text(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    return path.read_text(encoding="utf-8")


def get_property_content(name: str, fallback_path: Path | None = None) -> str:
    _ = name
    return read_file_text(fallback_path) if fallback_path else ""


def get_property_map(name: str, fallback_path: Path | None = None) -> dict[str, str]:
    return parse_kv_text(get_property_content(name, fallback_path))


def dump_kv_text(data: dict[str, str], key_order: list[str] | None = None) -> str:
    keys: list[str] = []
    seen: set[str] = set()
    if key_order:
        for key in key_order:
            if key in data and key not in seen:
                keys.append(key)
                seen.add(key)
    for key in data:
        if key not in seen:
            keys.append(key)
            seen.add(key)
    lines = [f"{key}={str(data.get(key, '') or '').strip()}" for key in keys]
    if not lines:
        return ""
    return "\n".join(lines) + "\n"


def set_property_content(
    name: str,
    content: str,
    fallback_path: Path | None = None,
) -> None:
    _ = name
    normalized = str(content or "")
    if not fallback_path:
        raise ValueError("fallback_path is required for file-backed property writes")
    fallback_path.parent.mkdir(parents=True, exist_ok=True)
    fallback_path.write_text(normalized, encoding="utf-8")


def set_property_map(
    name: str,
    data: dict[str, str],
    fallback_path: Path | None = None,
    key_order: list[str] | None = None,
) -> None:
    set_property_content(
        name=name,
        content=dump_kv_text(data, key_order=key_order),
        fallback_path=fallback_path,
    )
