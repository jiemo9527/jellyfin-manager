from __future__ import annotations

from typing import Any

from jm_manager.db import Db, connect
from jm_manager.runtime_settings import RuntimeSettings
from jm_manager.utils import now_shanghai, to_iso


def list_blacklists(db: Db) -> dict[str, list[str]]:
    conn = connect(db)
    try:
        rows = conn.execute(
            "SELECT username, url FROM ban_user_blacklists ORDER BY username ASC, url ASC"
        ).fetchall()
    finally:
        conn.close()

    out: dict[str, list[str]] = {}
    for r in rows:
        u = str(r["username"])
        url = str(r["url"])
        out.setdefault(u, []).append(url)
    return out


def has_any_rule(db: Db) -> bool:
    conn = connect(db)
    try:
        row = conn.execute("SELECT 1 FROM ban_user_blacklists LIMIT 1").fetchone()
        return row is not None
    finally:
        conn.close()


def replace_blacklists(db: Db, rules: dict[str, list[str]]) -> None:
    conn = connect(db)
    try:
        conn.execute("DELETE FROM ban_user_blacklists")
        for username, urls in (rules or {}).items():
            su = str(username).strip()
            if not su:
                continue
            for url in urls or []:
                s = str(url).strip()
                if not s:
                    continue
                conn.execute(
                    "INSERT OR IGNORE INTO ban_user_blacklists(username, url) VALUES(?, ?)",
                    (su, s),
                )
        conn.commit()
    finally:
        conn.close()


def list_overrides(db: Db) -> set[str]:
    conn = connect(db)
    try:
        rows = conn.execute("SELECT username FROM ban_user_overrides").fetchall()
        return {str(r["username"]) for r in rows}
    finally:
        conn.close()


def replace_overrides(db: Db, usernames: list[str]) -> None:
    now = to_iso(now_shanghai())
    conn = connect(db)
    try:
        conn.execute("DELETE FROM ban_user_overrides")
        for u in usernames:
            su = str(u).strip()
            if not su:
                continue
            conn.execute(
                "INSERT OR REPLACE INTO ban_user_overrides(username, updated_at) VALUES(?, ?)",
                (su, now),
            )
        conn.commit()
    finally:
        conn.close()


def extract_blacklists(cfg: dict[str, Any]) -> dict[str, list[str]]:
    rules = cfg.get("user_blacklists")
    if not isinstance(rules, dict):
        return {}
    out: dict[str, list[str]] = {}
    for k, v in rules.items():
        if not isinstance(k, str) or not isinstance(v, list):
            continue
        urls = [str(x) for x in v if str(x).strip()]
        if urls:
            out[k] = urls
    return out


def build_ban_config(rt: RuntimeSettings, rules: dict[str, list[str]]) -> dict[str, Any]:
    return {
        "jellyfin_host": rt.jellyfin_base_url,
        "api_key": rt.jellyfin_admin_api_key,
        "user_blacklists": rules,
        "generated_at": to_iso(now_shanghai()),
    }
