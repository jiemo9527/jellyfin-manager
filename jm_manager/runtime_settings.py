from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from jm_manager.db import Db, connect
from jm_manager.utils import now_shanghai, to_iso


@dataclass(frozen=True)
class RuntimeSettings:
    # Jellyfin
    jellyfin_base_url: str = ""
    jellyfin_pro_url: str = ""
    jellyfin_admin_api_key: str = ""

    # Auth
    web_username: str = "admin"
    web_password: str = ""
    api_key: str = ""

    # Telegram 通知
    telegram_bot_token: str = ""
    telegram_user_id: str = ""
    telegram_enabled: bool = False
    telegram_notify_types: str = ""

    telegram_public_bot_token: str = ""
    telegram_public_user_id: str = ""
    telegram_public_enabled: bool = False
    telegram_public_notify_types: str = ""

    # Backup
    backup_enabled: bool = False
    backup_time: str = "06:00"
    backup_repo: str = ""
    backup_source_dir: str = "/srv/jellyfin"
    backup_tag: str = "jellyfin"
    backup_keep_daily: int = 7
    backup_keep_weekly: int = 4
    backup_keep_monthly: int = 2
    backup_restic_password: str = ""

    # Schedules
    user_lifecycle_enabled: bool = True
    user_lifecycle_interval_hours: int = 3
    dns_refresh_interval_minutes: float = 4
    ban_rules_enabled: bool = True
    startj_url: str = ""
    device_cleanup_enabled: bool = False
    device_cleanup_time: str = "03:30"
    device_cleanup_inactive_days: int = 40
    device_cleanup_app_keywords: str = ""



_KEYS: dict[str, str] = {
    "web_username": "web_username",
    "jellyfin_base_url": "jellyfin_base_url",
    "jellyfin_pro_url": "jellyfin_pro_url",
    "jellyfin_admin_api_key": "jellyfin_admin_api_key",
    "web_password": "web_password",
    "api_key": "api_key",
    "telegram_bot_token": "telegram_bot_token",
    "telegram_user_id": "telegram_user_id",
    "telegram_enabled": "telegram_enabled",
    "telegram_notify_types": "telegram_notify_types",
    "telegram_public_bot_token": "telegram_public_bot_token",
    "telegram_public_user_id": "telegram_public_user_id",
    "telegram_public_enabled": "telegram_public_enabled",
    "telegram_public_notify_types": "telegram_public_notify_types",
    "backup_enabled": "backup_enabled",
    "backup_time": "backup_time",
    "backup_repo": "backup_repo",
    "backup_source_dir": "backup_source_dir",
    "backup_tag": "backup_tag",
    "backup_keep_daily": "backup_keep_daily",
    "backup_keep_weekly": "backup_keep_weekly",
    "backup_keep_monthly": "backup_keep_monthly",
    "backup_restic_password": "backup_restic_password",
    "user_lifecycle_enabled": "user_lifecycle_enabled",
    "user_lifecycle_interval_hours": "user_lifecycle_interval_hours",
    "dns_refresh_interval_minutes": "dns_refresh_interval_minutes",
    "ban_rules_enabled": "ban_rules_enabled",
    "startj_url": "startj_url",
    "device_cleanup_enabled": "device_cleanup_enabled",
    "device_cleanup_time": "device_cleanup_time",
    "device_cleanup_inactive_days": "device_cleanup_inactive_days",
    "device_cleanup_app_keywords": "device_cleanup_app_keywords",
}


def load_runtime_settings(db: Db) -> RuntimeSettings:
    conn = connect(db)
    try:
        # 允许在首次迁移/测试环境中表尚未创建
        rows = conn.execute("SELECT key, value FROM app_settings").fetchall()
    except Exception:
        rows = []
    finally:
        conn.close()

    d: dict[str, str] = {str(r["key"]): str(r["value"]) for r in rows}

    def get_str(key: str) -> str:
        return str(d.get(key, "") or "")

    def get_int(key: str, default: int) -> int:
        raw = str(d.get(key, "") or "").strip()
        if not raw:
            return default
        try:
            return int(raw)
        except Exception:
            return default

    def get_float(key: str, default: float) -> float:
        raw = str(d.get(key, "") or "").strip()
        if not raw:
            return default
        try:
            return float(raw)
        except Exception:
            return default

    def get_bool(key: str, default: bool = False) -> bool:
        raw = str(d.get(key, "") or "").strip().lower()
        if not raw:
            return default
        return raw in {"1", "true", "yes", "on"}

    base = get_str("jellyfin_base_url").rstrip("/")
    pro = get_str("jellyfin_pro_url").rstrip("/")
    if not pro:
        pro = base

    web_username = get_str("web_username").strip() or "admin"

    minutes_raw = str(d.get("dns_refresh_interval_minutes") or "").strip()
    hours_raw = str(d.get("dns_refresh_interval_hours") or "").strip()
    if minutes_raw:
        dns_minutes = get_float("dns_refresh_interval_minutes", 4)
    elif hours_raw:
        hours_val = get_float("dns_refresh_interval_hours", 0.0667)
        dns_minutes = max(hours_val * 60, 0)
        save_runtime_settings(db, {"dns_refresh_interval_minutes": dns_minutes}, skip_if_blank=set())
    else:
        dns_minutes = 4

    return RuntimeSettings(
        web_username=web_username,
        jellyfin_base_url=base,
        jellyfin_pro_url=pro,
        jellyfin_admin_api_key=get_str("jellyfin_admin_api_key"),
        web_password=get_str("web_password"),
        api_key=get_str("api_key"),
        telegram_bot_token=get_str("telegram_bot_token"),
        telegram_user_id=get_str("telegram_user_id"),
        telegram_enabled=get_bool("telegram_enabled", False),
        telegram_notify_types=get_str("telegram_notify_types"),
        telegram_public_bot_token=get_str("telegram_public_bot_token"),
        telegram_public_user_id=get_str("telegram_public_user_id"),
        telegram_public_enabled=get_bool("telegram_public_enabled", False),
        telegram_public_notify_types=get_str("telegram_public_notify_types"),
        backup_enabled=get_bool("backup_enabled", False),
        backup_time=get_str("backup_time") or "06:00",
        backup_repo=get_str("backup_repo"),
        backup_source_dir=get_str("backup_source_dir") or "/srv/jellyfin",
        backup_tag=get_str("backup_tag") or "jellyfin",
        backup_keep_daily=get_int("backup_keep_daily", 7),
        backup_keep_weekly=get_int("backup_keep_weekly", 4),
        backup_keep_monthly=get_int("backup_keep_monthly", 2),
        backup_restic_password=get_str("backup_restic_password"),
        user_lifecycle_enabled=get_bool("user_lifecycle_enabled", True),
        user_lifecycle_interval_hours=get_int("user_lifecycle_interval_hours", 3),
        dns_refresh_interval_minutes=dns_minutes,
        ban_rules_enabled=get_bool("ban_rules_enabled", True),
        startj_url=get_str("startj_url").strip(),
        device_cleanup_enabled=get_bool("device_cleanup_enabled", False),
        device_cleanup_time=get_str("device_cleanup_time") or "03:30",
        device_cleanup_inactive_days=get_int("device_cleanup_inactive_days", 40),
        device_cleanup_app_keywords=get_str("device_cleanup_app_keywords"),
    )


def save_runtime_settings(
    db: Db,
    updates: dict[str, Any],
    *,
    skip_if_blank: set[str] | None = None,
) -> None:
    skip_if_blank = skip_if_blank or set()
    now = to_iso(now_shanghai())

    conn = connect(db)
    try:
        for k, v in updates.items():
            key = _KEYS.get(k)
            if not key:
                continue
            sv = str(v if v is not None else "")
            if key in skip_if_blank and not sv.strip():
                continue
            conn.execute(
                """
                INSERT INTO app_settings(key, value, updated_at)
                VALUES(?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                  value=excluded.value,
                  updated_at=excluded.updated_at
                """,
                (key, sv, now),
            )
        conn.commit()
    finally:
        conn.close()


def runtime_missing(rt: RuntimeSettings) -> list[str]:
    missing: list[str] = []
    if not rt.jellyfin_base_url:
        missing.append("JM_JELLYFIN_BASE_URL")
    if not rt.jellyfin_admin_api_key:
        missing.append("JM_JELLYFIN_ADMIN_API_KEY")
    return missing
