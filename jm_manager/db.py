from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Db:
    path: str


SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;
PRAGMA busy_timeout=5000;

CREATE TABLE IF NOT EXISTS users (
  jellyfin_id TEXT PRIMARY KEY,
  username TEXT NOT NULL,
  plan_id TEXT NOT NULL,
  plan_name TEXT NOT NULL,
  registration_date TEXT NOT NULL,
  expiration_date TEXT NOT NULL,
  status TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_users_expiration ON users(status, expiration_date);

CREATE TABLE IF NOT EXISTS app_settings (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ban_user_blacklists (
  username TEXT NOT NULL,
  url TEXT NOT NULL,
  PRIMARY KEY(username, url)
);

CREATE INDEX IF NOT EXISTS idx_ban_user ON ban_user_blacklists(username);

CREATE TABLE IF NOT EXISTS ban_user_overrides (
  username TEXT PRIMARY KEY,
  updated_at TEXT NOT NULL
);
"""


def connect(db: Db) -> sqlite3.Connection:
    p = Path(db.path)
    if p.parent:
        p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db.path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db: Db) -> None:
    conn = connect(db)
    try:
        conn.executescript(SCHEMA_SQL)
        conn.commit()
    finally:
        conn.close()


def has_any_user(db: Db) -> bool:
    conn = connect(db)
    try:
        row = conn.execute("SELECT 1 FROM users LIMIT 1").fetchone()
        return row is not None
    finally:
        conn.close()
