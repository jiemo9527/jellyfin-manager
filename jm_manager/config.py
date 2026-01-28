from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    host: str
    port: int

    db_path: str

    # Session middleware secret must be set via env at process start.
    session_secret: str


def load_settings() -> Settings:
    load_dotenv(override=False)

    host = os.getenv("JM_HOST", "0.0.0.0")
    port = int(os.getenv("JM_PORT", "18080"))

    db_path = os.getenv("JM_DB_PATH", str(Path("data") / "jellyfin_manager.db"))

    session_secret = os.getenv("JM_SESSION_SECRET", "")

    return Settings(
        host=host,
        port=port,
        db_path=db_path,
        session_secret=session_secret,
    )
