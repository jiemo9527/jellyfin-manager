from __future__ import annotations

from typing import Any

from jm_manager.db import Db, connect


def list_users(db: Db) -> list[dict[str, Any]]:
    conn = connect(db)
    try:
        rows = conn.execute(
            "SELECT jellyfin_id, username, plan_id, plan_name, registration_date, expiration_date, status FROM users ORDER BY expiration_date ASC"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_user(db: Db, jellyfin_id: str) -> dict[str, Any] | None:
    conn = connect(db)
    try:
        row = conn.execute(
            "SELECT jellyfin_id, username, plan_id, plan_name, registration_date, expiration_date, status FROM users WHERE jellyfin_id=?",
            (jellyfin_id,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def upsert_user(db: Db, user: dict[str, Any]) -> None:
    conn = connect(db)
    try:
        conn.execute(
            """
            INSERT INTO users(jellyfin_id, username, plan_id, plan_name, registration_date, expiration_date, status)
            VALUES(?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(jellyfin_id) DO UPDATE SET
              username=excluded.username,
              plan_id=excluded.plan_id,
              plan_name=excluded.plan_name,
              registration_date=excluded.registration_date,
              expiration_date=excluded.expiration_date,
              status=excluded.status
            """,
            (
                str(user.get("jellyfin_id") or ""),
                str(user.get("username") or ""),
                str(user.get("plan_id") or ""),
                str(user.get("plan_name") or ""),
                str(user.get("registration_date") or ""),
                str(user.get("expiration_date") or ""),
                str(user.get("status") or "active"),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def delete_user(db: Db, jellyfin_id: str) -> None:
    conn = connect(db)
    try:
        conn.execute("DELETE FROM users WHERE jellyfin_id=?", (jellyfin_id,))
        conn.commit()
    finally:
        conn.close()
