from __future__ import annotations

import os
from pathlib import Path


def data_dir_from_db_path(db_path: str | None = None) -> Path:
    raw = str(db_path or "").strip()
    if raw:
        parent = Path(raw).expanduser().parent
        if str(parent) not in {"", "."}:
            return parent

    env_db_path = str(os.getenv("JM_DB_PATH", "")).strip()
    if env_db_path:
        parent = Path(env_db_path).expanduser().parent
        if str(parent) not in {"", "."}:
            return parent

    mounted_data_dir = Path("/data")
    if mounted_data_dir.exists():
        return mounted_data_dir

    return Path("data")


def default_db_path() -> str:
    return str(data_dir_from_db_path() / "jellyfin_manager.db")


def banuser_log_path(db_path: str | None = None) -> Path:
    return data_dir_from_db_path(db_path) / "banuser.log"
