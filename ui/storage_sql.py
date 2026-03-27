from __future__ import annotations

from pathlib import Path


def read_sql(path: str | Path) -> str:
    file_path = Path(path)
    if not file_path.exists():
        return ""
    with file_path.open("r", encoding="utf-8", newline="") as handle:
        raw = handle.read()
    while "\r\r\n" in raw:
        raw = raw.replace("\r\r\n", "\r\n")
    return raw.replace("\r\n", "\n").replace("\r", "\n")


def write_sql(path: str | Path, text: str) -> None:
    file_path = Path(path)
    normalized = str(text or "").replace("\r\n", "\n").replace("\r", "\n")
    with file_path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(normalized)
