from __future__ import annotations

import os
import sqlite3
import platform
import sys
import time
import re
import io
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Any
import json
import hashlib
import requests

import threading

from fastapi import Depends, FastAPI, Form, HTTPException, Request
from fastapi import UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from jm_manager.config import Settings, load_settings
from jm_manager.db import Db, connect, has_any_user, init_db
from jm_manager.jellyfin_api import JellyfinApi
from jm_manager.runtime_settings import RuntimeSettings, load_runtime_settings, runtime_missing, save_runtime_settings
from jm_manager.telegram_notify import (
    notify_user_created,
    notify_user_disabled,
    notify_user_enabled,
    notify_user_extended,
    notify_user_plan_changed,
    notify_user_deleted,
    notify_user_imported,
    notify_user_auto_disabled,
    notify_user_auto_deleted,
    notify_public_backup_result,
    notify_public_user_expiring,
    notify_public_user_auto_disabled,
    notify_stream_usage_high,
)
from jm_manager.ban_rules_store import (
    build_ban_config,
    extract_blacklists,
    list_blacklists as db_list_ban_blacklists,
    list_overrides as db_list_ban_overrides,
    replace_blacklists as db_replace_ban_blacklists,
    replace_overrides as db_replace_ban_overrides,
)
from jm_manager.startj_pools import get_startj_pools
from jm_manager.users_store import delete_user as db_delete_user
from jm_manager.users_store import list_users as db_list_users
from jm_manager.users_store import upsert_user as db_upsert_user
from jm_manager.utils import SHANGHAI_TZ, now_shanghai, parse_iso, to_iso
from jm_manager.backup import BackupConfig, run_backup_once, parse_backup_time, format_shanghai, list_snapshots
from jm_manager.banuser_worker import start_banuser_worker


def get_settings() -> Settings:
    return load_settings()


def get_runtime_settings(settings: Settings = Depends(get_settings)) -> RuntimeSettings:
    return load_runtime_settings(_db(settings))


def require_session(rt: RuntimeSettings, request: Request) -> None:
    if not rt.web_password:
        return
    if not request.session.get("authed"):
        raise HTTPException(status_code=401)

    expected_user = rt.web_username or "admin"
    expected_ver = hashlib.sha256(f"{expected_user}:{rt.web_password}".encode("utf-8")).hexdigest()
    if request.session.get("auth_user") != expected_user or request.session.get("auth_ver") != expected_ver:
        request.session.clear()
        raise HTTPException(status_code=401)


def require_api_key(rt: RuntimeSettings, request: Request) -> None:
    if not rt.api_key:
        raise HTTPException(status_code=403, detail="API disabled")
    if request.headers.get("x-api-key") != rt.api_key:
        raise HTTPException(status_code=401, detail="Unauthorized")


def build_plans(rt: RuntimeSettings) -> dict[str, dict[str, Any]]:
    # 对齐 Jellyfin_bot/jflogin.py 的 1-8 套餐
    base = rt.jellyfin_base_url
    pro = rt.jellyfin_pro_url
    return {
        "1": {"name": "普通一天", "duration_days": 1, "address": base},
        "2": {"name": "普通月卡", "duration_days": 30, "address": base},
        "3": {"name": "普通季卡", "duration_days": 90, "address": base},
        "4": {"name": "普通年卡", "duration_days": 365, "address": base},
        "5": {"name": "专线一天", "duration_days": 1, "address": pro},
        "6": {"name": "专线月卡", "duration_days": 30, "address": pro},
        "7": {"name": "专线季卡", "duration_days": 90, "address": pro},
        "8": {"name": "专线年卡", "duration_days": 365, "address": pro},
    }


app = FastAPI(title="jellyfin-manager", version="0.1")
templates = Jinja2Templates(directory="templates")
templates.env.globals["app_version"] = app.version
app.add_middleware(SessionMiddleware, secret_key=os.getenv("JM_SESSION_SECRET", "dev"))
app.mount("/static", StaticFiles(directory="static"), name="static")


def _append_log(message: str, level: str = "INFO") -> None:
    ts = now_shanghai().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} [{level}] {message}"
    buf: list[str] = getattr(app.state, "log_buffer", [])
    buf.append(line)
    if len(buf) > 500:
        del buf[:100]
    app.state.log_buffer = buf


_LOG_TS_REGEX = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),(\d{3,6})\s+-\s+")
_LOG_LINE_REGEX = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \[([A-Z]+)\] (.*)$")


def _to_shanghai_line(line: str) -> str:
    m = _LOG_TS_REGEX.match(str(line))
    if not m:
        return line
    ts = f"{m.group(1)},{m.group(2)}"
    try:
        dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S,%f")
        local_tz = datetime.now().astimezone().tzinfo or SHANGHAI_TZ
        dt = dt.replace(tzinfo=local_tz).astimezone(SHANGHAI_TZ)
        new_ts = dt.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
        return new_ts + str(line)[len(ts):]
    except Exception:
        return line


def _parse_log_line(line: str) -> dict[str, str]:
    raw = str(line)
    m = _LOG_LINE_REGEX.match(raw)
    if not m:
        return {"time": "", "level": "INFO", "message": raw}
    return {"time": m.group(1), "level": m.group(2), "message": m.group(3)}


def _log_category(message: str) -> str:
    if message.startswith("[备份]"):
        return "backup"
    if message.startswith("[用户生命周期]"):
        return "lifecycle"
    if message.startswith("[设备清理]"):
        return "device_cleanup"
    if message.startswith("[同步]"):
        return "sync"
    if message.startswith("[扫描]"):
        return "scan"
    if message.startswith("[用户]"):
        return "user"
    if message.startswith("[分流]"):
        return "ban"
    if message.startswith("[系统]"):
        return "system"
    return "system"


def _normalize_url(url: str) -> str:
    return str(url).strip().rstrip("/")


def _get_servers_and_all_urls(rt: RuntimeSettings, db: Db) -> tuple[dict[str, list[str]], list[str]]:
    servers_raw = get_startj_pools(db)
    if not servers_raw:
        fallback_urls = [u for u in [rt.jellyfin_base_url, rt.jellyfin_pro_url] if u]
        servers_raw = {"直连后端": fallback_urls, "其他类型": fallback_urls}

    servers: dict[str, list[str]] = {}
    for cat, urls in (servers_raw or {}).items():
        clean: list[str] = []
        for u in urls or []:
            su = _normalize_url(str(u))
            if su and su not in clean:
                clean.append(su)
        if clean:
            servers[str(cat)] = clean

    all_urls: list[str] = []
    for urls in servers.values():
        for u in urls:
            su = _normalize_url(str(u))
            if su and su not in all_urls:
                all_urls.append(su)
    return servers, all_urls


def _get_admin_names(rt: RuntimeSettings) -> set[str]:
    admin_names: set[str] = set()
    if rt.jellyfin_base_url and rt.jellyfin_admin_api_key:
        try:
            jf = _require_jellyfin(rt)
            for ju in jf.get_users():
                if bool((ju.get("Policy") or {}).get("IsAdministrator", False)):
                    name = str(ju.get("Name") or "").strip()
                    if name:
                        admin_names.add(name)
        except Exception:
            pass
    return admin_names


def _load_stream_servers(db: Db) -> list[dict[str, str]]:
    conn = connect(db)
    try:
        row = conn.execute(
            "SELECT value FROM app_settings WHERE key = ?", ("stream_servers_json",)
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return []
    try:
        data = json.loads(str(row["value"]))
    except Exception:
        return []
    if not isinstance(data, list):
        return []
    out: list[dict[str, str]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        veid = str(item.get("veid") or "").strip()
        api_key = str(item.get("api_key") or "").strip()
        mark = str(item.get("mark") or "").strip() or veid
        if not veid or not api_key:
            continue
        out.append({"veid": veid, "api_key": api_key, "mark": mark})
    return out


def _format_gb(size_bytes: int) -> float:
    if size_bytes <= 0:
        return 0.0
    return round(size_bytes / (1024 ** 3), 2)


def _fetch_stream_item(veid: str, api_key: str, mark: str) -> dict[str, str]:
    url = "https://api.64clouds.com/v1/getServiceInfo"
    params = {"veid": veid, "api_key": api_key}
    try:
        resp = requests.get(url, params=params, timeout=10)
        data = resp.json() if resp is not None else {}
    except Exception as e:
        return {"mark": mark, "error": f"请求失败: {e}"}

    if not isinstance(data, dict):
        return {"mark": mark, "error": "返回格式异常"}

    used = int(data.get("data_counter", 0) or 0)
    total = int(data.get("plan_monthly_data", 0) or 0)
    next_reset = data.get("data_next_reset") or 0
    ip_list = data.get("ip_addresses") or []
    ip = ip_list[0] if isinstance(ip_list, list) and ip_list else data.get("ip_address")
    dc = data.get("node_datacenter") or data.get("node_location") or "-"

    reset_time = "-"
    try:
        if next_reset:
            dt = datetime.fromtimestamp(float(next_reset), tz=SHANGHAI_TZ)
            reset_time = dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        reset_time = "-"

    used_gb = _format_gb(used)
    total_gb = _format_gb(total)
    percent = 0.0
    if total_gb > 0:
        percent = min(100.0, max(0.0, used_gb * 100.0 / total_gb))

    return {
        "mark": mark,
        "ip": str(ip or "-") if ip else "-",
        "traffic": f"{used_gb:.2f} GB / {total_gb:.2f} GB",
        "percent": f"{percent:.2f}%",
        "reset_time": reset_time,
        "data_center": str(dc or "-"),
    }


def _save_stream_servers(db: Db, servers: list[dict[str, str]]) -> None:
    payload = json.dumps(servers, ensure_ascii=False, separators=(",", ":"))
    now = to_iso(now_shanghai())
    conn = connect(db)
    try:
        conn.execute(
            """
            INSERT INTO app_settings(key, value, updated_at)
            VALUES(?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
              value=excluded.value,
              updated_at=excluded.updated_at
            """,
            ("stream_servers_json", payload, now),
        )
        conn.commit()
    finally:
        conn.close()


def _load_device_cleanup_rules(db: Db) -> list[dict[str, str]]:
    conn = connect(db)
    try:
        row = conn.execute(
            "SELECT value FROM app_settings WHERE key = ?", ("device_cleanup_rules_json",)
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return []
    try:
        data = json.loads(str(row["value"]))
    except Exception:
        return []
    if not isinstance(data, list):
        return []
    out: list[dict[str, str]] = []
    for idx, item in enumerate(data):
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or f"rule-{idx + 1}").strip()
        enabled = str(item.get("enabled", "1")).strip().lower()
        time_hhmm = str(item.get("time") or "").strip()
        try:
            inactive_days = int(str(item.get("inactive_days") or "0").strip() or 0)
        except Exception:
            inactive_days = 0
        keywords = str(item.get("app_keywords") or "").strip()
        if not name:
            name = f"rule-{idx + 1}"
        out.append(
            {
                "name": name,
                "enabled": "1" if enabled in {"1", "true", "yes", "on"} else "0",
                "time": time_hhmm,
                "inactive_days": str(inactive_days),
                "app_keywords": keywords,
            }
        )
    return out


def _save_device_cleanup_rules(db: Db, rules: list[dict[str, str]]) -> None:
    payload = json.dumps(rules, ensure_ascii=False, separators=(",", ":"))
    now = to_iso(now_shanghai())
    conn = connect(db)
    try:
        conn.execute(
            """
            INSERT INTO app_settings(key, value, updated_at)
            VALUES(?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
              value=excluded.value,
              updated_at=excluded.updated_at
            """,
            ("device_cleanup_rules_json", payload, now),
        )
        conn.commit()
    finally:
        conn.close()


def _slugify(text: str, index: int = 0) -> str:
    """生成媒体库代码。支持中文名称（转为 lib-N 序号形式）。"""
    raw = str(text or "").strip().lower()
    slug = re.sub(r"[^a-z0-9]+", "-", raw).strip("-")
    if slug:
        return slug
    # 中文或其他非 ASCII 名称：使用序号
    return f"lib-{index + 1}" if index > 0 else "lib"


def _load_library_scan_items(db: Db, rt: RuntimeSettings) -> list[dict[str, str]]:
    conn = connect(db)
    try:
        row = conn.execute(
            "SELECT value FROM app_settings WHERE key = ?", ("library_scan_items_json",)
        ).fetchone()
    finally:
        conn.close()

    items: list[dict[str, str]] = []
    if row:
        try:
            data = json.loads(str(row["value"]))
        except Exception:
            data = None
        if isinstance(data, list):
            for idx, item in enumerate(data):
                if not isinstance(item, dict):
                    continue
                name = str(item.get("name") or "").strip()
                lib_id = str(item.get("id") or "").strip()
                code = str(item.get("code") or "").strip()
                if not name or not lib_id:
                    continue
                if not code:
                    code = _slugify(name, idx)
                items.append({"name": name, "id": lib_id, "code": code})

    return items


def _save_library_scan_items(db: Db, items: list[dict[str, str]]) -> None:
    payload = json.dumps(items, ensure_ascii=False, separators=(",", ":"))
    now = to_iso(now_shanghai())
    conn = connect(db)
    try:
        conn.execute(
            """
            INSERT INTO app_settings(key, value, updated_at)
            VALUES(?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
              value=excluded.value,
              updated_at=excluded.updated_at
            """,
            ("library_scan_items_json", payload, now),
        )
        conn.commit()
    finally:
        conn.close()


def _export_app_settings(db: Db) -> dict[str, Any]:
    conn = connect(db)
    try:
        rows = conn.execute("SELECT key, value FROM app_settings").fetchall()
    finally:
        conn.close()
    settings: dict[str, str] = {str(r["key"]): str(r["value"]) for r in rows}
    return {
        "exported_at": to_iso(now_shanghai()),
        "app_settings": settings,
    }


def _export_full_backup(db: Db) -> dict[str, Any]:
    app_settings = _export_app_settings(db).get("app_settings") or {}
    users = db_list_users(db)
    rules = db_list_ban_blacklists(db)
    overrides = list(db_list_ban_overrides(db))
    return {
        "version": 1,
        "exported_at": to_iso(now_shanghai()),
        "app_settings": app_settings,
        "users": users,
        "ban_user_blacklists": rules,
        "ban_user_overrides": overrides,
    }


def _clear_all_data(db: Db) -> None:
    conn = connect(db)
    try:
        conn.execute("DELETE FROM users")
        conn.execute("DELETE FROM ban_user_blacklists")
        conn.execute("DELETE FROM ban_user_overrides")
        conn.execute("DELETE FROM app_settings")
        conn.commit()
    finally:
        conn.close()


def _import_full_backup(db: Db, payload: dict[str, Any], *, replace_all: bool) -> None:
    if replace_all:
        _clear_all_data(db)

    app_settings = payload.get("app_settings")
    if isinstance(app_settings, dict):
        _import_app_settings(db, app_settings, replace_all=False)

    users = payload.get("users")
    if isinstance(users, list):
        for u in users:
            if isinstance(u, dict):
                db_upsert_user(db, u)

    rules = payload.get("ban_user_blacklists")
    if isinstance(rules, dict):
        db_replace_ban_blacklists(db, rules)

    overrides = payload.get("ban_user_overrides")
    if isinstance(overrides, list):
        db_replace_ban_overrides(db, [str(x) for x in overrides if str(x).strip()])


def _import_app_settings(db: Db, values: dict[str, Any], *, replace_all: bool) -> None:
    now = to_iso(now_shanghai())
    conn = connect(db)
    try:
        if replace_all:
            conn.execute("DELETE FROM app_settings")
        for key, value in values.items():
            k = str(key).strip()
            if not k:
                continue
            v = "" if value is None else str(value)
            conn.execute(
                """
                INSERT INTO app_settings(key, value, updated_at)
                VALUES(?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                  value=excluded.value,
                  updated_at=excluded.updated_at
                """,
                (k, v, now),
            )
        conn.commit()
    finally:
        conn.close()


def _bytes_to_gb(val: int) -> float:
    try:
        return round(float(val) / (1024**3), 2)
    except Exception:
        return 0.0


def _fetch_stream_server_info(veid: str, api_key: str) -> dict[str, str]:
    url = f"https://api.64clouds.com/v1/getServiceInfo?veid={veid}&api_key={api_key}"
    default_info = {
        "data_center": "未知",
        "ip": "未知",
        "traffic": "0 GB / 0 GB (0%)",
        "reset_time": "未知",
    }
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        node_datacenter = data.get("node_datacenter", "未知")
        ip_addresses = data.get("ip_addresses", [])

        try:
            data_used = int(data.get("data_counter", 0))
            data_total = int(data.get("plan_monthly_data", 0))
            data_reset = int(data.get("data_next_reset", 0))
        except (ValueError, TypeError):
            data_used = 0
            data_total = 0
            data_reset = 0

        used_gb = _bytes_to_gb(data_used)
        total_gb = _bytes_to_gb(data_total)
        pct = round(used_gb / total_gb * 100, 2) if total_gb > 0 else 0

        try:
            dt = datetime.fromtimestamp(data_reset, tz=timezone.utc).astimezone(SHANGHAI_TZ)
            reset_date = dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            reset_date = "未知"

        return {
            "data_center": str(node_datacenter),
            "ip": ", ".join(ip_addresses) if ip_addresses else "未知",
            "traffic": f"{used_gb} GB / {total_gb} GB ({pct}%)",
            "reset_time": reset_date,
        }
    except requests.exceptions.RequestException:
        return default_info


def _default_blacklist_normal(servers: dict[str, list[str]], all_urls: list[str]) -> list[str]:
    allowed_categories_normal = {"direct", "other"}
    allow_urls_normal: set[str] = set()
    for cat, urls in servers.items():
        c = str(cat)
        if c in allowed_categories_normal or ("direct" in c) or ("other" in c):
            for u in urls:
                su = _normalize_url(str(u))
                if su:
                    allow_urls_normal.add(su)
    return [u for u in all_urls if u not in allow_urls_normal]


def _auto_apply_ban_rules_on_user_change(db: Db, rt: RuntimeSettings) -> None:
    servers, all_urls = _get_servers_and_all_urls(rt, db)
    stored = db_list_ban_blacklists(db)
    overrides = db_list_ban_overrides(db)
    users_rows = db_list_users(db)
    users = [str(u.get("username") or "").strip() for u in users_rows if u.get("username")]
    plan_by_user = {str(u.get("username")): str(u.get("plan_id")) for u in users_rows if u.get("username")}
    admin_names = _get_admin_names(rt)

    default_blacklist_normal = _default_blacklist_normal(servers, all_urls)

    new_blacklists: dict[str, list[str]] = {}
    for username in users:
        if username in admin_names:
            continue
        if username in overrides:
            vals = [_normalize_url(str(x)) for x in stored.get(username, []) if str(x).strip()]
            if vals:
                new_blacklists[username] = vals
            continue

        pid = plan_by_user.get(username, "")
        is_pro = pid in {"5", "6", "7", "8"}
        if not is_pro:
            new_blacklists[username] = list(default_blacklist_normal)

    db_replace_ban_blacklists(db, new_blacklists)
    db_replace_ban_overrides(db, users)
    _append_log(f"[分流] 自动应用 users={len(users)} rules={len(new_blacklists)}")


def _tail_file(path: str, *, max_lines: int = 200, max_bytes: int = 128 * 1024) -> list[str]:
    try:
        p = Path(path)
        if not p.exists() or not p.is_file():
            return []
        size = p.stat().st_size
        start = max(0, size - max_bytes)
        with p.open("rb") as f:
            f.seek(start)
            data = f.read()
        text = data.decode("utf-8", errors="replace")
        lines = text.splitlines()
        return lines[-max_lines:]
    except Exception:
        return []


def _build_backup_config(rt: RuntimeSettings) -> BackupConfig:
    return BackupConfig(
        enabled=bool(rt.backup_enabled),
        time_hhmm=str(rt.backup_time or "06:00"),
        repo=str(rt.backup_repo or "").strip(),
        source_dir=str(rt.backup_source_dir or "").strip(),
        tag=str(rt.backup_tag or "jellyfin").strip(),
        keep_daily=int(rt.backup_keep_daily or 7),
        keep_weekly=int(rt.backup_keep_weekly or 4),
        keep_monthly=int(rt.backup_keep_monthly or 2),
        restic_password=str(rt.backup_restic_password or "").strip(),
    )


def _trigger_backup(db: Db, rt: RuntimeSettings, *, reason: str) -> None:
    if getattr(app.state, "backup_running", False):
        _append_log("[备份] 当前已有任务在运行，已忽略本次触发")
        return

    def _worker() -> None:
        app.state.backup_running = True
        app.state.backup_last_status = "running"
        _append_log(f"[备份] 触发方式: {reason}")
        cfg = _build_backup_config(rt)
        if reason == "manual" and not cfg.enabled:
            cfg = BackupConfig(
                enabled=True,
                time_hhmm=cfg.time_hhmm,
                repo=cfg.repo,
                source_dir=cfg.source_dir,
                tag=cfg.tag,
                keep_daily=cfg.keep_daily,
                keep_weekly=cfg.keep_weekly,
                keep_monthly=cfg.keep_monthly,
                restic_password=cfg.restic_password,
            )
        ok, info = run_backup_once(cfg, _append_log)
        duration_seconds = str(info.get("duration_seconds") or "")
        total_bytes_processed = str(info.get("total_bytes_processed") or "")
        data_added = str(info.get("data_added") or "")
        files_new = str(info.get("files_new") or "")
        files_changed = str(info.get("files_changed") or "")
        total_files_processed = str(info.get("total_files_processed") or "")
        notify_public_backup_result(
            rt,
            ok=ok,
            repo=cfg.repo,
            source_dir=cfg.source_dir,
            reason=reason,
            duration_seconds=duration_seconds,
            total_bytes_processed=total_bytes_processed,
            data_added=data_added,
            files_new=files_new,
            files_changed=files_changed,
            total_files_processed=total_files_processed,
            error=str(info.get("error") or ""),
        )
        app.state.backup_last_status = "success" if ok else "failed"
        app.state.backup_last_run = now_shanghai()
        app.state.backup_running = False

    t = threading.Thread(target=_worker, daemon=True)
    t.start()


def _start_backup_scheduler(settings: Settings) -> None:
    def _loop() -> None:
        last_date = None
        last_warn = 0.0
        while True:
            time.sleep(30)
            rt = load_runtime_settings(_db(settings))
            cfg = _build_backup_config(rt)
            if not cfg.enabled:
                continue

            hhmm = parse_backup_time(cfg.time_hhmm)
            if not hhmm:
                if time.time() - last_warn > 3600:
                    _append_log("[备份] 定时配置无效，请在 /settings 设置时间 (HH:MM)")
                    last_warn = time.time()
                continue

            if not cfg.repo or not cfg.source_dir:
                if time.time() - last_warn > 3600:
                    _append_log("[备份] 缺少备份仓库或目录，已跳过定时任务")
                    last_warn = time.time()
                continue

            now = now_shanghai()
            if (now.hour, now.minute) != hhmm:
                continue

            if last_date == now.date():
                continue

            last_date = now.date()
            _trigger_backup(_db(settings), rt, reason="schedule")

    t = threading.Thread(target=_loop, daemon=True)
    t.start()


def _trigger_device_cleanup(
    db: Db,
    rt: RuntimeSettings,
    *,
    reason: str,
    start_dt: datetime | None,
    end_dt: datetime | None,
    inactive_days: int,
    app_keywords: list[str],
) -> None:
    if getattr(app.state, "device_cleanup_running", False):
        _append_log("[设备清理] 任务正在运行，已忽略本次触发")
        return

    def _worker() -> None:
        app.state.device_cleanup_running = True
        app.state.device_cleanup_last_status = "running"
        _append_log(f"[设备清理] 触发方式: {reason}")
        try:
            devices = _device_cleanup_preview(
                rt,
                start_dt=start_dt,
                end_dt=end_dt,
                inactive_days=inactive_days,
                app_keywords=app_keywords,
            )
            if start_dt or end_dt:
                _append_log(
                    f"[设备清理] 预览条件 时间范围 start={start_dt or '-'} end={end_dt or '-'} keywords={','.join(app_keywords) or '-'}"
                )
            else:
                _append_log(
                    f"[设备清理] 预览条件 不活跃天数={inactive_days} keywords={','.join(app_keywords) or '-'}"
                )
            deleted = _device_cleanup_execute(rt, devices)
            app.state.device_cleanup_last_status = "success"
            app.state.device_cleanup_last_count = deleted
            _append_log(f"[设备清理] 完成 deleted={deleted} candidates={len(devices)}")
        except Exception as e:
            app.state.device_cleanup_last_status = "failed"
            _append_log(f"[设备清理] 失败 err={e}")
        app.state.device_cleanup_last_run = now_shanghai()
        app.state.device_cleanup_running = False

    t = threading.Thread(target=_worker, daemon=True)
    t.start()


def _start_device_cleanup_scheduler(settings: Settings) -> None:
    def _loop() -> None:
        last_date = None
        last_warn = 0.0
        while True:
            time.sleep(30)
            rt = load_runtime_settings(_db(settings))
            if not rt.device_cleanup_enabled:
                continue
            if not rt.jellyfin_base_url or not rt.jellyfin_admin_api_key:
                continue
            rules = _load_device_cleanup_rules(_db(settings))
            if rules:
                last_dates = getattr(app.state, "device_cleanup_rule_last_dates", {})
                warn_map = getattr(app.state, "device_cleanup_rule_warns", {})
                now = now_shanghai()
                for idx, rule in enumerate(rules):
                    if str(rule.get("enabled") or "1").strip() != "1":
                        continue
                    rule_name = str(rule.get("name") or f"rule-{idx + 1}").strip()
                    rule_key = rule_name or f"rule-{idx + 1}"
                    hhmm = parse_backup_time(str(rule.get("time") or ""))
                    if not hhmm:
                        last_warn_ts = float(warn_map.get(rule_key) or 0)
                        if time.time() - last_warn_ts > 3600:
                            _append_log(f"[设备清理] 规则时间无效 name={rule_key}")
                            warn_map[rule_key] = time.time()
                        continue
                    if now.hour != hhmm[0] or now.minute != hhmm[1]:
                        continue
                    if last_dates.get(rule_key) == now.date():
                        continue
                    last_dates[rule_key] = now.date()
                    app_keywords = _parse_keywords(str(rule.get("app_keywords") or ""))
                    try:
                        inactive_days = int(str(rule.get("inactive_days") or "0").strip() or 0)
                    except Exception:
                        inactive_days = 0
                    _trigger_device_cleanup(
                        _db(settings),
                        rt,
                        reason=f"schedule:{rule_key}",
                        start_dt=None,
                        end_dt=None,
                        inactive_days=inactive_days,
                        app_keywords=app_keywords,
                    )
                app.state.device_cleanup_rule_last_dates = last_dates
                app.state.device_cleanup_rule_warns = warn_map
                continue

            hhmm = parse_backup_time(str(rt.device_cleanup_time or ""))
            if not hhmm:
                if time.time() - last_warn > 3600:
                    _append_log("[设备清理] 执行时间无效，跳过")
                    last_warn = time.time()
                continue

            now = now_shanghai()
            if last_date == now.date():
                continue
            if now.hour != hhmm[0] or now.minute != hhmm[1]:
                continue

            last_date = now.date()
            app_keywords = _parse_keywords(rt.device_cleanup_app_keywords)
            inactive_days = int(rt.device_cleanup_inactive_days or 0)
            _trigger_device_cleanup(
                _db(settings),
                rt,
                reason="schedule",
                start_dt=None,
                end_dt=None,
                inactive_days=inactive_days,
                app_keywords=app_keywords,
            )

    t = threading.Thread(target=_loop, daemon=True)
    t.start()


def _start_stream_check_scheduler(settings: Settings) -> None:
    def _loop() -> None:
        last_date = None
        did_startup = False
        while True:
            time.sleep(30)
            rt = load_runtime_settings(_db(settings))
            now = now_shanghai()

            if not did_startup:
                db = _db(settings)
                servers = _load_stream_servers(db)
                items_startup: list[dict[str, str]] = []
                for s in servers:
                    veid = str(s.get("veid") or "").strip()
                    key = str(s.get("api_key") or "").strip()
                    mark = str(s.get("mark") or "").strip() or veid
                    if not veid or not key:
                        continue
                    items_startup.append(_fetch_stream_item(veid, key, mark))
                if items_startup:
                    _maybe_notify_stream_usage(rt, items_startup, db)
                did_startup = True

            if last_date == now.date():
                continue
            if now.hour != 9 or now.minute != 0:
                continue

            last_date = now.date()
            db = _db(settings)
            servers = _load_stream_servers(db)
            items_daily: list[dict[str, str]] = []
            for s in servers:
                veid = str(s.get("veid") or "").strip()
                key = str(s.get("api_key") or "").strip()
                mark = str(s.get("mark") or "").strip() or veid
                if not veid or not key:
                    continue
                items_daily.append(_fetch_stream_item(veid, key, mark))
            if items_daily:
                _maybe_notify_stream_usage(rt, items_daily, db)

    t = threading.Thread(target=_loop, daemon=True)
    t.start()


def _notify_once(cache_key: str, username: str, marker: str) -> bool:
    cache: dict[str, str] = getattr(app.state, cache_key, {})
    last_value = cache.get(username)
    if last_value == marker:
        return False
    cache[username] = marker
    setattr(app.state, cache_key, cache)
    return True


def _clear_notify_cache(cache_key: str, username: str) -> None:
    cache: dict[str, str] = getattr(app.state, cache_key, {})
    if username in cache:
        del cache[username]
        setattr(app.state, cache_key, cache)


def _load_notify_cache(db: Db, key: str) -> dict[str, str]:
    conn = connect(db)
    try:
        row = conn.execute(
            "SELECT value FROM app_settings WHERE key = ?", (f"notify_cache_{key}",)
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return {}
    try:
        data = json.loads(str(row["value"]))
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}
    return {str(k): str(v) for k, v in data.items()}


def _save_notify_cache(db: Db, key: str, cache: dict[str, str]) -> None:
    payload = json.dumps(cache, ensure_ascii=False, separators=(",", ":"))
    now = to_iso(now_shanghai())
    conn = connect(db)
    try:
        conn.execute(
            """
            INSERT INTO app_settings(key, value, updated_at)
            VALUES(?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
              value=excluded.value,
              updated_at=excluded.updated_at
            """,
            (f"notify_cache_{key}", payload, now),
        )
        conn.commit()
    finally:
        conn.close()


def _user_lifecycle_once(db: Db, rt: RuntimeSettings) -> dict[str, int]:
    if not rt.jellyfin_base_url or not rt.jellyfin_admin_api_key:
        _append_log("[用户生命周期] 缺少 Jellyfin 配置，已跳过")
        return {"disabled": 0, "deleted": 0}

    jf = _require_jellyfin(rt)
    try:
        admin_names = _get_admin_names(rt)
    except Exception:
        admin_names = set()

    users = db_list_users(db)
    now = now_shanghai()
    disabled = 0
    deleted = 0

    for u in users:
        try:
            username = str(u.get("username") or "").strip()
            if username in admin_names:
                continue
            exp = parse_iso(str(u.get("expiration_date") or ""))
            status = str(u.get("status") or "active")
            exp_key = str(u.get("expiration_date") or "").strip() or "unknown"
            today_key = now.strftime("%Y-%m-%d")

            if status == "active":
                days_left = (exp.date() - now.date()).days
                if 0 <= days_left <= 3:
                    if _notify_once("expiring_notified", username, today_key):
                        notify_public_user_expiring(
                            rt,
                            username=username,
                            expiration_date=str(u.get("expiration_date") or ""),
                            days_left=days_left,
                        )
                        _save_notify_cache(db, "expiring_notified", app.state.expiring_notified)

            if status == "active" and now > exp:
                jf.set_disabled(str(u.get("jellyfin_id")), True)
                u["status"] = "disabled"
                db_upsert_user(db, u)
                disabled += 1
                _append_log(f"[用户生命周期] 到期禁用 user={username}")
                # 发送 Telegram 通知：自动禁用
                if _notify_once("disabled_notified", username, exp_key):
                    notify_user_auto_disabled(
                        rt,
                        username,
                        str(u.get("registration_date") or ""),
                        str(u.get("expiration_date") or ""),
                    )
                    notify_public_user_auto_disabled(
                        rt,
                        username=username,
                        expiration_date=str(u.get("expiration_date") or ""),
                    )
                    _save_notify_cache(db, "disabled_notified", app.state.disabled_notified)
                continue

            if status == "disabled" and (now - exp) > timedelta(days=4):
                try:
                    jf.delete_user(str(u.get("jellyfin_id")))
                except Exception as e:
                    _append_log(f"[用户生命周期] 删除 Jellyfin 用户失败 user={username} err={e}")
                db_delete_user(db, str(u.get("jellyfin_id")))
                deleted += 1
                _append_log(f"[用户生命周期] 到期清理 user={username}")
                # 发送 Telegram 通知：自动清理
                notify_user_auto_deleted(rt, username, str(u.get("registration_date") or ""), str(u.get("expiration_date") or ""))
        except Exception as e:
            _append_log(f"[用户生命周期] 处理失败 user={u.get('username')} err={e}")

    return {"disabled": disabled, "deleted": deleted}


def _trigger_user_lifecycle(db: Db, rt: RuntimeSettings, *, reason: str) -> None:
    if getattr(app.state, "lifecycle_running", False):
        _append_log("[用户生命周期] 任务正在运行，已忽略本次触发")
        return

    def _worker() -> None:
        app.state.lifecycle_running = True
        app.state.lifecycle_last_status = "running"
        _append_log(f"[用户生命周期] 触发方式: {reason}")
        stats = _user_lifecycle_once(db, rt)
        app.state.lifecycle_last_status = "success"
        app.state.lifecycle_last_run = now_shanghai()
        app.state.lifecycle_last_stats = stats
        app.state.lifecycle_running = False

    t = threading.Thread(target=_worker, daemon=True)
    t.start()


def _start_user_lifecycle_scheduler(settings: Settings) -> None:
    def _loop() -> None:
        # 启动即跑一次
        rt = load_runtime_settings(_db(settings))
        if rt.user_lifecycle_enabled and rt.jellyfin_base_url and rt.jellyfin_admin_api_key:
            _trigger_user_lifecycle(_db(settings), rt, reason="startup")

        while True:
            time.sleep(30)
            rt = load_runtime_settings(_db(settings))
            if not rt.user_lifecycle_enabled:
                continue
            if not rt.jellyfin_base_url or not rt.jellyfin_admin_api_key:
                continue

            interval_hours = int(rt.user_lifecycle_interval_hours or 3)
            if interval_hours <= 0:
                continue

            last_run = getattr(app.state, "lifecycle_last_run", None)
            if last_run is None:
                _trigger_user_lifecycle(_db(settings), rt, reason="interval")
                continue

            delta = now_shanghai() - last_run
            if delta.total_seconds() >= interval_hours * 3600:
                _trigger_user_lifecycle(_db(settings), rt, reason="interval")

    t = threading.Thread(target=_loop, daemon=True)
    t.start()


@app.get("/ban-rules/logs")
def ban_rules_logs(
    request: Request,
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Any:
    try:
        require_session(rt, request)
    except HTTPException:
        return JSONResponse(status_code=401, content={"ok": False, "error": "unauthorized"})

    banuser_log_path = str(Path("data") / "banuser.log")
    p = Path(banuser_log_path)
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        if not p.exists():
            p.write_text("", encoding="utf-8")
    except Exception:
        pass
    exists = p.exists() and p.is_file()
    size = 0
    mtime = None
    try:
        if exists:
            st = p.stat()
            size = int(st.st_size)
            mtime = int(st.st_mtime)
    except Exception:
        pass

    lines = _tail_file(banuser_log_path, max_lines=200)
    lines = [_to_shanghai_line(str(x)) for x in lines]
    manager_lines = [
        str(x)
        for x in getattr(app.state, "log_buffer", [])
        if "[分流]" in str(x) or "ban" in str(x).lower()
    ][-80:]
    return {
        "ok": True,
        "path": banuser_log_path,
        "exists": bool(exists),
        "size": size,
        "mtime": mtime,
        "lines": lines,
        "manager_lines": manager_lines,
        "line_count": len(lines),
        "manager_line_count": len(manager_lines),
    }


@app.post("/ban-rules/logs/clear")
def ban_rules_logs_clear(
    request: Request,
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Any:
    try:
        require_session(rt, request)
    except HTTPException:
        return JSONResponse(status_code=401, content={"ok": False, "error": "unauthorized"})

    banuser_log_path = str(Path("data") / "banuser.log")
    ok = True
    try:
        p = Path(banuser_log_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        if p.exists() and p.is_file():
            p.write_text("", encoding="utf-8")
    except Exception:
        ok = False

    app.state.log_buffer = []
    return {"ok": ok}


def _db(settings: Settings) -> Db:
    return Db(path=settings.db_path)


def _require_jellyfin(rt: RuntimeSettings) -> JellyfinApi:
    if not rt.jellyfin_base_url or not rt.jellyfin_admin_api_key:
        raise HTTPException(status_code=503, detail="missing jellyfin config")
    return JellyfinApi(base_url=rt.jellyfin_base_url, api_key=rt.jellyfin_admin_api_key)


def _parse_datetime_local(value: str) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw)
    except Exception:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=SHANGHAI_TZ)
    return dt.astimezone(SHANGHAI_TZ)


def _parse_keywords(value: str) -> list[str]:
    raw = str(value or "").strip().lower()
    if not raw:
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]


def _run_nslookup(host: str) -> str:
    try:
        import socket

        infos = socket.getaddrinfo(host, None)
        seen = []
        for info in infos:
            addr = info[4][0]
            if addr not in seen:
                seen.append(addr)
        if not seen:
            return f"Name: {host}\nAddress: -"
        lines = [f"Name: {host}"]
        for addr in seen:
            lines.append(f"Address: {addr}")
        return "\n".join(lines)
    except Exception as e:
        return f"nslookup 失败: {e}"


def _maybe_notify_stream_usage(rt: RuntimeSettings, items: list[dict[str, str]], db: Db) -> None:
    if not rt.telegram_enabled:
        return
    if not rt.telegram_bot_token or not rt.telegram_user_id:
        return
    cache: dict[str, str] = getattr(app.state, "stream_usage_alerts", {})
    updated = False
    for it in items:
        mark = str(it.get("mark") or "").strip()
        if not mark:
            continue
        percent_raw = str(it.get("percent") or "0").replace("%", "").strip()
        try:
            percent = float(percent_raw)
        except Exception:
            continue
        if percent < 66.0:
            continue
        reset_time = str(it.get("reset_time") or "").strip()
        if cache.get(mark) == reset_time:
            continue
        notify_stream_usage_high(
            rt,
            mark=mark,
            ip=str(it.get("ip") or "-"),
            traffic=str(it.get("traffic") or "-"),
            percent=str(it.get("percent") or "-"),
            reset_time=reset_time or "-",
            data_center=str(it.get("data_center") or "-"),
        )
        cache[mark] = reset_time
        updated = True
    if updated:
        app.state.stream_usage_alerts = cache
        _save_notify_cache(db, "stream_usage_alerts", cache)


def _device_cleanup_preview(
    rt: RuntimeSettings,
    *,
    start_dt: datetime | None,
    end_dt: datetime | None,
    inactive_days: int,
    app_keywords: list[str],
) -> list[dict[str, str]]:
    jf = _require_jellyfin(rt)
    devices = jf.get_devices()
    now = now_shanghai()
    out: list[dict[str, str]] = []

    for d in devices:
        if not isinstance(d, dict):
            continue
        device_id = str(d.get("Id") or "").strip()
        device_name = str(d.get("Name") or "").strip() or "-"
        app_name = str(d.get("AppName") or "").strip()
        last_raw = str(d.get("DateLastActivity") or "").strip()
        last_dt = parse_iso(last_raw) if last_raw else None
        last_text = last_dt.strftime("%Y-%m-%d %H:%M") if last_dt else "-"

        reasons: list[str] = []
        app_hit: list[str] = []
        name_hit: list[str] = []
        if app_keywords:
            app_lower = app_name.lower() if app_name else ""
            name_lower = device_name.lower() if device_name else ""
            app_hit = [k for k in app_keywords if app_lower and k in app_lower]
            name_hit = [k for k in app_keywords if name_lower and k in name_lower]
            if not app_hit and not name_hit:
                continue
            if app_hit:
                reasons.append(f"应用匹配:{'/'.join(app_hit)}")
            if name_hit:
                reasons.append(f"设备名匹配:{'/'.join(name_hit)}")

        if start_dt or end_dt:
            if not last_dt:
                continue
            if start_dt and last_dt < start_dt:
                continue
            if end_dt and last_dt > end_dt:
                continue
            reasons.append("活跃时间在范围内")
        else:
            if inactive_days > 0:
                if not last_dt:
                    continue
                days_ago = (now - last_dt).days
                if days_ago < inactive_days:
                    continue
                reasons.append(f"不活跃 {days_ago} 天")

        if not reasons:
            continue

        out.append(
            {
                "id": device_id,
                "name": device_name,
                "app": app_name or "-",
                "last": last_text,
                "reason": " & ".join(reasons),
            }
        )
    return out


def _device_cleanup_execute(rt: RuntimeSettings, devices: list[dict[str, str]]) -> int:
    jf = _require_jellyfin(rt)
    deleted = 0
    for item in devices:
        device_id = str(item.get("id") or "").strip()
        if not device_id:
            continue
        try:
            ok = jf.delete_device(device_id)
            if ok:
                deleted += 1
        except Exception:
            continue
    return deleted


@app.on_event("startup")
def _startup() -> None:
    settings = load_settings()
    app.state.session_secret = settings.session_secret or os.getenv("JM_SESSION_SECRET", "dev")
    app.state.log_buffer = []
    app.state.backup_running = False
    app.state.backup_last_run = None
    app.state.backup_last_status = "idle"
    app.state.lifecycle_running = False
    app.state.lifecycle_last_run = None
    app.state.lifecycle_last_status = "idle"
    app.state.lifecycle_last_stats = {"disabled": 0, "deleted": 0}
    app.state.expiring_notified = {}
    app.state.disabled_notified = {}
    app.state.device_cleanup_running = False
    app.state.device_cleanup_last_run = None
    app.state.device_cleanup_last_status = "idle"
    app.state.device_cleanup_last_count = 0
    app.state.device_cleanup_rule_last_dates = {}
    app.state.device_cleanup_rule_warns = {}
    app.state.stream_usage_alerts = {}

    db = _db(settings)
    init_db(db)
    app.state.expiring_notified = _load_notify_cache(db, "expiring_notified")
    app.state.disabled_notified = _load_notify_cache(db, "disabled_notified")
    app.state.stream_usage_alerts = _load_notify_cache(db, "stream_usage_alerts")

    _append_log("[系统] 启动完成")
    _start_backup_scheduler(settings)
    _start_user_lifecycle_scheduler(settings)
    _start_device_cleanup_scheduler(settings)
    try:
        rt = load_runtime_settings(_db(settings))
        servers = _load_stream_servers(_db(settings))
        items_startup: list[dict[str, str]] = []
        for s in servers:
            veid = str(s.get("veid") or "").strip()
            key = str(s.get("api_key") or "").strip()
            mark = str(s.get("mark") or "").strip() or veid
            if not veid or not key:
                continue
            items_startup.append(_fetch_stream_item(veid, key, mark))
        if items_startup:
            _maybe_notify_stream_usage(rt, items_startup, db)
    except Exception:
        pass
    _start_stream_check_scheduler(settings)
    start_banuser_worker(settings.db_path)


@app.get("/login", response_class=HTMLResponse)
def login_get(
    request: Request,
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Any:
    if not rt.web_password:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("login.html", {"request": request, "error": None})


@app.post("/login")
def login_post(
    request: Request,
    rt: RuntimeSettings = Depends(get_runtime_settings),
    username: str = Form(""),
    password: str = Form(...),
) -> Any:
    if not rt.web_password:
        return RedirectResponse(url="/", status_code=302)
    expected_user = rt.web_username or "admin"
    if str(username).strip() != expected_user or password != rt.web_password:
        request.session.clear()
        return templates.TemplateResponse("login.html", {"request": request, "error": "用户名或密码错误"}, status_code=400)

    request.session["authed"] = True
    request.session["auth_user"] = expected_user
    request.session["auth_ver"] = hashlib.sha256(f"{expected_user}:{rt.web_password}".encode("utf-8")).hexdigest()
    return RedirectResponse(url="/", status_code=303)


@app.get("/logout")
def logout(request: Request, rt: RuntimeSettings = Depends(get_runtime_settings)) -> Any:
    if rt.web_password:
        request.session.clear()
    return RedirectResponse(url="/login", status_code=302)


@app.get("/settings", response_class=HTMLResponse)
def settings_get(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Any:
    try:
        require_session(rt, request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    saved = request.query_params.get("saved") == "1"
    error = request.query_params.get("error")
    missing = runtime_missing(rt)
    # 提醒：Session secret 只能通过 env 设置
    if rt.web_password and not (settings.session_secret or os.getenv("JM_SESSION_SECRET", "")):
        missing.append("JM_SESSION_SECRET")

    servers = _load_stream_servers(_db(settings))
    server_marks = [str(s.get("mark") or "") for s in servers if s.get("mark")]
    scan_items = _load_library_scan_items(_db(settings), rt)
    device_cleanup_rules = _load_device_cleanup_rules(_db(settings))

    return templates.TemplateResponse(
        "settings.html",
        {
            "request": request,
            "settings": rt,
            "saved": saved,
            "error": error,
            "missing": missing,
            "stream_server_marks": server_marks,
            "library_scan_items": scan_items,
            "device_cleanup_rules": device_cleanup_rules,
        },
    )


@app.post("/settings")
async def settings_post(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Any:
    try:
        require_session(rt, request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    form = await request.form()

    web_username = rt.web_username
    clear_web_password = False
    new_web_password = ""

    scope_raw = str(form.get("JM_SETTINGS_SCOPE") or "").strip()
    scope = {s.strip() for s in scope_raw.split(",") if s.strip()} if scope_raw else None

    def in_scope(name: str) -> bool:
        return scope is None or name in scope

    updates: dict[str, Any] = {}
    skip_if_blank = {"jellyfin_admin_api_key", "web_password", "api_key"}

    if in_scope("jellyfin"):
        clear_jellyfin_key = str(form.get("CLEAR_JM_JELLYFIN_ADMIN_API_KEY") or "").strip() != ""
        base_url = str(form.get("JM_JELLYFIN_BASE_URL") or "").strip().rstrip("/")
        pro_url = str(form.get("JM_JELLYFIN_PRO_URL") or "").strip().rstrip("/")
        if not pro_url:
            pro_url = base_url
        new_jellyfin_admin_api_key = str(form.get("JM_JELLYFIN_ADMIN_API_KEY") or "").strip()
        updates.update(
            {
                "jellyfin_base_url": base_url,
                "jellyfin_pro_url": pro_url,
                "jellyfin_admin_api_key": "" if clear_jellyfin_key else new_jellyfin_admin_api_key,
            }
        )
        if clear_jellyfin_key:
            skip_if_blank.discard("jellyfin_admin_api_key")

    if in_scope("security"):
        clear_web_password = str(form.get("CLEAR_JM_WEB_PASSWORD") or "").strip() != ""
        clear_api_key = str(form.get("CLEAR_JM_API_KEY") or "").strip() != ""
        web_username = str(form.get("JM_WEB_USERNAME") or "").strip() or "admin"
        new_web_password = str(form.get("JM_WEB_PASSWORD") or "")
        new_web_password_confirm = str(form.get("JM_WEB_PASSWORD_CONFIRM") or "")
        new_api_key = str(form.get("JM_API_KEY") or "")

        # 密码确认校验：非清空且有输入时，两次必须一致
        if not clear_web_password and new_web_password:
            if new_web_password != new_web_password_confirm:
                return RedirectResponse("/settings?msg=两次密码输入不一致&scope=security", status_code=303)

        updates.update(
            {
                "web_username": web_username,
                "web_password": "" if clear_web_password else new_web_password,
                "api_key": "" if clear_api_key else new_api_key,
            }
        )
        if clear_web_password:
            skip_if_blank.discard("web_password")
        if clear_api_key:
            skip_if_blank.discard("api_key")

    if in_scope("automation"):
        # 保留入口以兼容旧表单，但不再保存旧字段
        pass

    if in_scope("telegram"):
        clear_telegram_bot_token = str(form.get("CLEAR_JM_TELEGRAM_BOT_TOKEN") or "").strip() != ""
        telegram_enabled = "1" if str(form.get("JM_TELEGRAM_ENABLED") or "").strip() else "0"
        new_telegram_bot_token = str(form.get("JM_TELEGRAM_BOT_TOKEN") or "").strip()
        telegram_user_id = str(form.get("JM_TELEGRAM_USER_ID") or "").strip()
        clear_public_bot_token = str(form.get("CLEAR_JM_TELEGRAM_PUBLIC_BOT_TOKEN") or "").strip() != ""
        public_enabled = "1" if str(form.get("JM_TELEGRAM_PUBLIC_ENABLED") or "").strip() else "0"
        public_bot_token = str(form.get("JM_TELEGRAM_PUBLIC_BOT_TOKEN") or "").strip()
        public_user_id = str(form.get("JM_TELEGRAM_PUBLIC_USER_ID") or "").strip()
        updates.update(
            {
                "telegram_enabled": telegram_enabled,
                "telegram_bot_token": "" if clear_telegram_bot_token else new_telegram_bot_token,
                "telegram_user_id": telegram_user_id,
                "telegram_public_enabled": public_enabled,
                "telegram_public_bot_token": "" if clear_public_bot_token else public_bot_token,
                "telegram_public_user_id": public_user_id,
            }
        )
        if clear_telegram_bot_token:
            skip_if_blank.discard("telegram_bot_token")
        else:
            skip_if_blank.add("telegram_bot_token")
        if clear_public_bot_token:
            skip_if_blank.discard("telegram_public_bot_token")
        else:
            skip_if_blank.add("telegram_public_bot_token")

    if in_scope("backup"):
        clear_restic_password = str(form.get("CLEAR_JM_BACKUP_RESTIC_PASSWORD") or "").strip() != ""
        backup_enabled = "1" if str(form.get("JM_BACKUP_ENABLED") or "").strip() else "0"
        backup_time = str(form.get("JM_BACKUP_TIME") or "").strip() or "06:00"
        backup_repo = str(form.get("JM_BACKUP_REPO") or "").strip()
        backup_source_dir = str(form.get("JM_BACKUP_SOURCE_DIR") or "").strip()
        backup_tag = str(form.get("JM_BACKUP_TAG") or "").strip() or "jellyfin"
        new_restic_password = str(form.get("JM_BACKUP_RESTIC_PASSWORD") or "").strip()
        try:
            backup_keep_daily = int(str(form.get("JM_BACKUP_KEEP_DAILY") or "").strip() or 7)
        except Exception:
            backup_keep_daily = 7
        try:
            backup_keep_weekly = int(str(form.get("JM_BACKUP_KEEP_WEEKLY") or "").strip() or 4)
        except Exception:
            backup_keep_weekly = 4
        try:
            backup_keep_monthly = int(str(form.get("JM_BACKUP_KEEP_MONTHLY") or "").strip() or 2)
        except Exception:
            backup_keep_monthly = 2
        updates.update(
            {
                "backup_enabled": backup_enabled,
                "backup_time": backup_time,
                "backup_repo": backup_repo,
                "backup_source_dir": backup_source_dir,
                "backup_tag": backup_tag,
                "backup_keep_daily": backup_keep_daily,
                "backup_keep_weekly": backup_keep_weekly,
                "backup_keep_monthly": backup_keep_monthly,
                "backup_restic_password": "" if clear_restic_password else new_restic_password,
            }
        )
        if clear_restic_password:
            skip_if_blank.discard("backup_restic_password")
        else:
            skip_if_blank.add("backup_restic_password")

    if in_scope("schedules"):
        user_lifecycle_enabled = "1" if str(form.get("JM_USER_LIFECYCLE_ENABLED") or "").strip() else "0"
        try:
            user_lifecycle_interval_hours = int(
                str(form.get("JM_USER_LIFECYCLE_INTERVAL_HOURS") or "").strip() or 3
            )
        except Exception:
            user_lifecycle_interval_hours = 3
        try:
            dns_refresh_interval_minutes = float(
                str(form.get("JM_DNS_REFRESH_INTERVAL_MINUTES") or "").strip() or 4
            )
        except Exception:
            dns_refresh_interval_minutes = 4
        device_cleanup_enabled = "1" if str(form.get("JM_DEVICE_CLEANUP_ENABLED") or "").strip() else "0"
        device_cleanup_time = str(form.get("JM_DEVICE_CLEANUP_TIME") or "").strip() or "03:30"
        try:
            device_cleanup_inactive_days = int(
                str(form.get("JM_DEVICE_CLEANUP_INACTIVE_DAYS") or "").strip() or 40
            )
        except Exception:
            device_cleanup_inactive_days = 40
        device_cleanup_app_keywords = str(form.get("JM_DEVICE_CLEANUP_APP_KEYWORDS") or "").strip()
        clear_cleanup_rules = str(form.get("CLEAR_JM_DEVICE_CLEANUP_RULES") or "").strip() != ""
        cleanup_rules_raw = str(form.get("JM_DEVICE_CLEANUP_RULES_JSON") or "").strip()
        updates.update(
            {
                "user_lifecycle_enabled": user_lifecycle_enabled,
                "user_lifecycle_interval_hours": user_lifecycle_interval_hours,
                "dns_refresh_interval_minutes": dns_refresh_interval_minutes,
                "device_cleanup_enabled": device_cleanup_enabled,
                "device_cleanup_time": device_cleanup_time,
                "device_cleanup_inactive_days": device_cleanup_inactive_days,
                "device_cleanup_app_keywords": device_cleanup_app_keywords,
            }
        )
        if clear_cleanup_rules:
            _save_device_cleanup_rules(_db(settings), [])
            _append_log("[设备清理] 已清空定时规则")
        elif cleanup_rules_raw:
            try:
                parsed = json.loads(cleanup_rules_raw)
                if not isinstance(parsed, list):
                    raise ValueError("规则列表必须是 JSON 数组")
                rules: list[dict[str, str]] = []
                for idx, item in enumerate(parsed):
                    if not isinstance(item, dict):
                        continue
                    name = str(item.get("name") or f"rule-{idx + 1}").strip()
                    enabled = str(item.get("enabled", "1")).strip().lower()
                    time_hhmm = str(item.get("time") or "").strip()
                    try:
                        inactive_days = int(str(item.get("inactive_days") or "0").strip() or 0)
                    except Exception:
                        inactive_days = 0
                    keywords = str(item.get("app_keywords") or "").strip()
                    if not name:
                        name = f"rule-{idx + 1}"
                    rules.append(
                        {
                            "name": name,
                            "enabled": "1" if enabled in {"1", "true", "yes", "on"} else "0",
                            "time": time_hhmm,
                            "inactive_days": str(inactive_days),
                            "app_keywords": keywords,
                        }
                    )
                _save_device_cleanup_rules(_db(settings), rules)
                rule_names = [str(r.get("name") or "") for r in rules if r.get("name")]
                enabled_count = len([r for r in rules if str(r.get("enabled") or "") == "1"])
                _append_log(
                    f"[设备清理] 保存定时规则 total={len(rules)} enabled={enabled_count} names={','.join(rule_names)}"
                )
            except Exception as e:
                return RedirectResponse(url=f"/settings?error=设备清理规则解析失败: {e}", status_code=303)

    if updates:
        save_runtime_settings(_db(settings), updates, skip_if_blank=skip_if_blank)

    clear_stream_servers = str(form.get("CLEAR_JM_STREAM_SERVERS") or "").strip() != ""
    stream_servers_raw = str(form.get("JM_STREAM_SERVERS_JSON") or "").strip()
    if in_scope("stream") and clear_stream_servers:
        _save_stream_servers(_db(settings), [])
    elif in_scope("stream") and stream_servers_raw:
        try:
            parsed = json.loads(stream_servers_raw)
            if not isinstance(parsed, list):
                raise ValueError("服务器列表必须是 JSON 数组")
            servers: list[dict[str, str]] = []
            for item in parsed:
                if not isinstance(item, dict):
                    continue
                veid = str(item.get("veid") or "").strip()
                api_key = str(item.get("api_key") or "").strip()
                mark = str(item.get("mark") or "").strip() or veid
                if not veid or not api_key:
                    continue
                servers.append({"veid": veid, "api_key": api_key, "mark": mark})
            _save_stream_servers(_db(settings), servers)
        except Exception as e:
            return RedirectResponse(url=f"/settings?error=服务器列表解析失败: {e}", status_code=302)

    clear_library_scan_items = str(form.get("CLEAR_JM_LIBRARY_SCAN_ITEMS") or "").strip() != ""
    library_scan_items_raw = str(form.get("JM_LIBRARY_SCAN_ITEMS_JSON") or "").strip()
    if in_scope("library_scan") and clear_library_scan_items:
        _save_library_scan_items(_db(settings), [])
    elif in_scope("library_scan") and library_scan_items_raw:
        try:
            parsed = json.loads(library_scan_items_raw)
            if not isinstance(parsed, list):
                raise ValueError("媒体库列表必须是 JSON 数组")
            items: list[dict[str, str]] = []
            for idx, item in enumerate(parsed):
                if not isinstance(item, dict):
                    continue
                name = str(item.get("name") or "").strip()
                lib_id = str(item.get("id") or "").strip()
                code = str(item.get("code") or "").strip()
                if not name or not lib_id:
                    continue
                if not code:
                    code = _slugify(name, idx)
                items.append({"name": name, "id": lib_id, "code": code})
            _save_library_scan_items(_db(settings), items)
        except Exception as e:
            return RedirectResponse(url=f"/settings?error=媒体库列表解析失败: {e}", status_code=302)

    # 会话同步：仅在安全配置更新时处理
    if in_scope("security"):
        new_user = web_username
        new_pw = "" if clear_web_password else str(new_web_password)
        if str(new_pw).strip():
            request.session["authed"] = True
            request.session["auth_user"] = new_user
            request.session["auth_ver"] = hashlib.sha256(f"{new_user}:{new_pw}".encode("utf-8")).hexdigest()
        else:
            # 关闭登录：清掉会话
            request.session.clear()
    return RedirectResponse(url="/settings?saved=1", status_code=303)


@app.post("/settings/stream-servers/delete")
def settings_stream_server_delete(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
    delete_veid: str = Form(""),
    next_url: str = Form(""),
) -> Any:
    try:
        require_session(rt, request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    veid = str(delete_veid or "").strip()
    redirect_url = str(next_url or "").strip()
    if not redirect_url.startswith("/") or redirect_url.startswith("//"):
        redirect_url = "/settings?saved=1"
    if not veid:
        return RedirectResponse(url="/settings?error=缺少服务器标识", status_code=303)

    existing = _load_stream_servers(_db(settings))
    remaining = [item for item in existing if str(item.get("veid") or "").strip() != veid]
    if len(remaining) == len(existing):
        return RedirectResponse(url="/settings?error=未找到对应服务器配置", status_code=303)

    _save_stream_servers(_db(settings), remaining)
    _append_log(f"[系统] 流量监控服务器已删除 veid={veid} remaining={len(remaining)}")
    return RedirectResponse(url=redirect_url, status_code=303)


@app.post("/settings/export")
def settings_export(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
    password: str = Form(""),
) -> Any:
    try:
        require_session(rt, request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    pw = str(password or "").strip()
    if not pw:
        return RedirectResponse(url="/settings?error=导出失败: 需要设置导出密码", status_code=302)

    try:
        pyzipper = __import__("pyzipper")
    except Exception:
        return RedirectResponse(url="/settings?error=导出失败: 未安装 pyzipper", status_code=302)

    payload = _export_full_backup(_db(settings))
    data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

    buffer = io.BytesIO()
    with pyzipper.AESZipFile(buffer, "w", compression=pyzipper.ZIP_DEFLATED, encryption=pyzipper.WZ_AES) as zf:
        zf.setpassword(pw.encode("utf-8"))
        zf.writestr("jm_backup.json", data)

    filename = f"jm_backup_{now_shanghai().strftime('%Y%m%d_%H%M%S')}.zip"
    res = Response(content=buffer.getvalue(), media_type="application/zip")
    res.headers["Content-Disposition"] = f"attachment; filename={filename}"
    return res


@app.post("/settings/import")
async def settings_import(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
    file: UploadFile = File(...),
    replace_all: str = Form(""),
    password: str = Form(""),
) -> Any:
    try:
        require_session(rt, request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    pw = str(password or "").strip()
    if not pw:
        return RedirectResponse(url="/settings?error=导入失败: 需要导入密码", status_code=302)

    try:
        pyzipper = __import__("pyzipper")
    except Exception:
        return RedirectResponse(url="/settings?error=导入失败: 未安装 pyzipper", status_code=302)

    raw = await file.read()
    try:
        with pyzipper.AESZipFile(io.BytesIO(raw)) as zf:
            zf.setpassword(pw.encode("utf-8"))
            data = zf.read("jm_backup.json")
    except Exception as e:
        return RedirectResponse(url=f"/settings?error=导入失败: 解密或读取失败 {e}", status_code=302)

    try:
        payload = json.loads(data.decode("utf-8"))
    except Exception as e:
        return RedirectResponse(url=f"/settings?error=导入失败: JSON 解析错误 {e}", status_code=302)

    if not isinstance(payload, dict):
        return RedirectResponse(url="/settings?error=导入失败: 格式无效", status_code=302)

    _import_full_backup(_db(settings), payload, replace_all=str(replace_all).strip() != "")
    return RedirectResponse(url="/settings?saved=1", status_code=302)


@app.post("/settings/db-vacuum")
def settings_db_vacuum(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Any:
    try:
        require_session(rt, request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    try:
        conn = sqlite3.connect(settings.db_path, check_same_thread=False)
        try:
            conn.execute("VACUUM")
            _append_log("[系统] 数据库压缩完成")
        finally:
            conn.close()
    except Exception as e:
        _append_log(f"[系统] 数据库压缩失败: {e}")
        return RedirectResponse(url=f"/settings?error=数据库压缩失败:{e}", status_code=302)

    return RedirectResponse(url="/settings?saved=1", status_code=302)


@app.get("/", response_class=HTMLResponse)
def dashboard(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Any:
    try:
        require_session(rt, request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    users = db_list_users(_db(settings))
    now = now_shanghai()
    total = len(users)
    disabled = sum(1 for u in users if u.get("status") == "disabled")
    expiring_soon = 0
    for u in users:
        try:
            exp = parse_iso(str(u.get("expiration_date") or ""))
            if u.get("status") == "active" and 0 <= (exp.date() - now.date()).days <= 3:
                expiring_soon += 1
        except Exception:
            pass

    rules = db_list_ban_blacklists(_db(settings))

    stats = {
        "total_users": total,
        "expiring_soon": expiring_soon,
        "disabled_users": disabled,
        "active_rules": len(rules),
    }

    backup_summary = {
        "last_run": format_shanghai(getattr(app.state, "backup_last_run", None)),
        "last_status": str(getattr(app.state, "backup_last_status", "idle")),
        "running": bool(getattr(app.state, "backup_running", False)),
    }

    scan_items = _load_library_scan_items(_db(settings), rt)

    backup_info = {
        "enabled": bool(rt.backup_enabled),
        "time": str(rt.backup_time or "06:00"),
        "repo": str(rt.backup_repo or ""),
        "source_dir": str(rt.backup_source_dir or ""),
        "tag": str(rt.backup_tag or "jellyfin"),
        "keep_daily": int(rt.backup_keep_daily or 7),
        "keep_weekly": int(rt.backup_keep_weekly or 4),
        "keep_monthly": int(rt.backup_keep_monthly or 2),
        "last_run": format_shanghai(getattr(app.state, "backup_last_run", None)),
        "last_status": str(getattr(app.state, "backup_last_status", "idle")),
        "running": bool(getattr(app.state, "backup_running", False)),
    }

    recent_logs: list[dict[str, str]] = []
    for line in list(getattr(app.state, "log_buffer", []))[-8:]:
        parsed = _parse_log_line(line)
        recent_logs.append(
            {
                "time": parsed.get("time") or "-",
                "level": parsed.get("level") or "INFO",
                "message": parsed.get("message") or "",
            }
        )
    env_info = {
        "python_version": sys.version.split()[0],
        "platform": platform.platform(),
        "jellyfin_url": rt.jellyfin_base_url or "未配置",
        "server_time": now_shanghai().strftime("%Y-%m-%d %H:%M:%S"),
    }
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "stats": stats,
            "recent_logs": recent_logs,
            "backup_summary": backup_summary,
            "backup_info": backup_info,
            "scan_items": scan_items,
            "env_info": env_info,
        },
    )


@app.get("/users", response_class=HTMLResponse)
def users_page(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Any:
    try:
        require_session(rt, request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    plans = build_plans(rt)
    db = _db(settings)
    users = db_list_users(db)
    admin_ids: set[str] = set()
    admin_names: set[str] = set()
    if rt.jellyfin_base_url and rt.jellyfin_admin_api_key:
        try:
            jf = _require_jellyfin(rt)
            for ju in jf.get_users():
                uid = str(ju.get("Id") or "").strip()
                name = str(ju.get("Name") or "").strip()
                is_admin = bool((ju.get("Policy") or {}).get("IsAdministrator", False))
                if is_admin:
                    if uid:
                        admin_ids.add(uid)
                    if name:
                        admin_names.add(name)
        except Exception:
            pass
    now = now_shanghai()
    sort_key = str(request.query_params.get("sort") or "").strip()
    enriched = []
    for u in users:
        try:
            exp = parse_iso(str(u.get("expiration_date") or ""))
            days_left = (exp.date() - now.date()).days
        except Exception:
            days_left = 0
        enriched.append(
            {
                "jellyfin_id": u.get("jellyfin_id"),
                "username": u.get("username"),
                "plan_id": u.get("plan_id"),
                "plan_name": u.get("plan_name"),
                "registration_date": u.get("registration_date"),
                "expiration_date": u.get("expiration_date"),
                "status": u.get("status", "active"),
                "days_left": days_left,
                "is_admin": (str(u.get("jellyfin_id") or "").strip() in admin_ids)
                or (str(u.get("username") or "").strip() in admin_names),
            }
        )

    def _safe_dt(value: str) -> datetime:
        try:
            return parse_iso(str(value or ""))
        except Exception:
            return datetime.min.replace(tzinfo=SHANGHAI_TZ)

    if sort_key == "name_asc":
        enriched.sort(key=lambda u: str(u.get("username") or "").lower())
    elif sort_key == "name_desc":
        enriched.sort(key=lambda u: str(u.get("username") or "").lower(), reverse=True)
    elif sort_key == "reg_asc":
        enriched.sort(key=lambda u: _safe_dt(str(u.get("registration_date") or "")))
    elif sort_key == "reg_desc":
        enriched.sort(key=lambda u: _safe_dt(str(u.get("registration_date") or "")), reverse=True)
    elif sort_key == "plan_asc":
        enriched.sort(key=lambda u: str(u.get("plan_name") or "").lower())
    elif sort_key == "plan_desc":
        enriched.sort(key=lambda u: str(u.get("plan_name") or "").lower(), reverse=True)

    return templates.TemplateResponse(
        "users.html",
        {
            "request": request,
            "users": enriched,
            "plans": {k: type("P", (), v)() for k, v in plans.items()},
            "sort_key": sort_key,
        },
    )


@app.get("/users/export")
def users_export(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Response:
    try:
        require_session(rt, request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    users = db_list_users(_db(settings))
    payload = json.dumps(users, ensure_ascii=False, indent=2)
    headers = {"Content-Disposition": "attachment; filename=users.json"}
    return Response(payload, media_type="application/json; charset=utf-8", headers=headers)


@app.get("/users/import", response_class=HTMLResponse)
def users_import_get(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Any:
    try:
        require_session(rt, request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    plans = build_plans(rt)
    return templates.TemplateResponse(
        "users_import.html",
        {
            "request": request,
            "plans": {k: type("P", (), v)() for k, v in plans.items()},
            "report": None,
            "error": None,
        },
    )


@app.post("/users/import", response_class=HTMLResponse)
async def users_import_post(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
    file: UploadFile = File(...),
    default_plan_id: str = Form("2"),
) -> Any:
    try:
        require_session(rt, request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    # 确保数据库表已创建（兼容测试环境直接调用）
    init_db(_db(settings))

    plans = build_plans(rt)
    if default_plan_id not in plans:
        return templates.TemplateResponse(
            "users_import.html",
            {
                "request": request,
                "plans": {k: type("P", (), v)() for k, v in plans.items()},
                "report": None,
                "error": "默认套餐无效",
            },
            status_code=400,
        )

    raw = await file.read()
    try:
        payload = json.loads(raw.decode("utf-8"))
    except Exception as e:
        return templates.TemplateResponse(
            "users_import.html",
            {
                "request": request,
                "plans": {k: type("P", (), v)() for k, v in plans.items()},
                "report": None,
                "error": f"JSON 解析失败: {e}",
            },
            status_code=400,
        )

    if not isinstance(payload, list):
        return templates.TemplateResponse(
            "users_import.html",
            {
                "request": request,
                "plans": {k: type("P", (), v)() for k, v in plans.items()},
                "report": None,
                "error": "文件内容必须是 JSON 数组（list）",
            },
            status_code=400,
        )

    # plan_name -> plan_id 映射（尽量从当前系统匹配）
    name_to_pid = {str(v.get("name")): k for k, v in plans.items()}

    db = _db(settings)
    existing = {str(u.get("jellyfin_id")) for u in db_list_users(db) if u.get("jellyfin_id")}

    report: dict[str, Any] = {
        "total": len(payload),
        "created": 0,
        "updated": 0,
        "skipped": 0,
        "errors": [],
    }

    for idx, item in enumerate(payload, start=1):
        if not isinstance(item, dict):
            report["skipped"] += 1
            report["errors"].append(f"#{idx}: 非对象（dict），已跳过")
            continue

        jellyfin_id = str(item.get("jellyfin_id") or "").strip()
        username = str(item.get("username") or "").strip()
        registration_date = str(item.get("registration_date") or "").strip()
        expiration_date = str(item.get("expiration_date") or "").strip()
        status = str(item.get("status") or "active").strip() or "active"
        plan_id = str(item.get("plan_id") or "").strip()
        plan_name = str(item.get("plan_name") or "").strip()

        if not jellyfin_id or not username or not registration_date or not expiration_date:
            report["skipped"] += 1
            report["errors"].append(f"#{idx}: 缺少 jellyfin_id/username/registration_date/expiration_date，已跳过")
            continue

        if status not in {"active", "disabled"}:
            status = "active"

        if not plan_id:
            if plan_name and plan_name in name_to_pid:
                plan_id = name_to_pid[plan_name]
            else:
                plan_id = default_plan_id

        if plan_id not in plans:
            plan_id = default_plan_id

        if not plan_name:
            plan_name = str(plans[plan_id]["name"])

        try:
            # 校验时间可解析（保持原值写入）
            parse_iso(registration_date)
            parse_iso(expiration_date)
        except Exception:
            report["skipped"] += 1
            report["errors"].append(f"#{idx}: 时间格式无法解析（需 ISO 或类似），已跳过")
            continue

        before_exists = jellyfin_id in existing
        db_upsert_user(
            db,
            {
                "jellyfin_id": jellyfin_id,
                "username": username,
                "plan_id": plan_id,
                "plan_name": plan_name,
                "registration_date": registration_date,
                "expiration_date": expiration_date,
                "status": status,
            },
        )
        if before_exists:
            report["updated"] += 1
        else:
            report["created"] += 1
            existing.add(jellyfin_id)

    try:
        _auto_apply_ban_rules_on_user_change(db, rt)
    except Exception as e:
        _append_log(f"[分流] 自动应用失败: {e}")

    # 发送 Telegram 通知：批量导入
    total_imported = report["created"] + report["updated"]
    if total_imported > 0:
        notify_user_imported(rt, total_imported)

    return templates.TemplateResponse(
        "users_import.html",
        {
            "request": request,
            "plans": {k: type("P", (), v)() for k, v in plans.items()},
            "report": report,
            "error": None,
        },
    )


@app.post("/users/create")
def users_create(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
    username: str = Form(...),
    password: str = Form(...),
    plan_id: str = Form(...),
) -> Any:
    try:
        require_session(rt, request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    if not rt.jellyfin_base_url or not rt.jellyfin_admin_api_key:
        return RedirectResponse(url="/settings?error=请先配置 Jellyfin 连接信息", status_code=302)

    plans = build_plans(rt)
    if plan_id not in plans:
        raise HTTPException(status_code=400, detail="invalid plan")

    jf = _require_jellyfin(rt)
    created = jf.create_user(username=username, password=password)
    jellyfin_id = created.get("Id")
    if not jellyfin_id:
        raise HTTPException(status_code=500, detail="create user failed")
    jf.set_initial_policy(jellyfin_id)

    reg = now_shanghai()
    exp = reg + timedelta(days=int(plans[plan_id]["duration_days"]))

    db = _db(settings)
    db_upsert_user(
        db,
        {
            "jellyfin_id": jellyfin_id,
            "username": username,
            "plan_id": plan_id,
            "plan_name": plans[plan_id]["name"],
            "registration_date": to_iso(reg),
            "expiration_date": to_iso(exp),
            "status": "active",
        },
    )
    try:
        _auto_apply_ban_rules_on_user_change(db, rt)
    except Exception as e:
        _append_log(f"[分流] 自动应用失败: {e}")

    _append_log(f"[用户] 创建 user={username} plan={plans[plan_id]['name']}")

    # 发送 Telegram 通知：用户创建
    server_address = str(plans[plan_id].get("address") or "")
    notify_user_created(rt, username, plans[plan_id]["name"], to_iso(exp), server_address, password)

    return RedirectResponse(url="/users", status_code=303)


@app.post("/users/disable")
def users_disable(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
    user_id: str = Form(...),
) -> Any:
    try:
        require_session(rt, request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    if not rt.jellyfin_base_url or not rt.jellyfin_admin_api_key:
        return RedirectResponse(url="/settings?error=请先配置 Jellyfin 连接信息", status_code=302)

    jf = _require_jellyfin(rt)
    try:
        ju = jf.get_user(user_id)
        if bool((ju.get("Policy") or {}).get("IsAdministrator", False)):
            _append_log(f"refuse disable admin user id={user_id}")
            return RedirectResponse(url="/users", status_code=303)
    except Exception:
        pass
    jf.set_disabled(user_id, True)

    db = _db(settings)
    disabled_username = ""
    registration_date = ""
    expiration_date = ""
    for u in db_list_users(db):
        if u.get("jellyfin_id") != user_id:
            continue
        u["status"] = "disabled"
        db_upsert_user(db, u)
        disabled_username = str(u.get("username") or "")
        registration_date = str(u.get("registration_date") or "")
        expiration_date = str(u.get("expiration_date") or "")
        break

    # 发送 Telegram 通知：用户禁用
    if disabled_username:
        _append_log(f"[用户] 禁用 user={disabled_username}")
        exp_key = str(expiration_date or "").strip() or "unknown"
        if _notify_once("disabled_notified", disabled_username, exp_key):
            notify_user_disabled(rt, disabled_username, registration_date, expiration_date)
            _save_notify_cache(db, "disabled_notified", app.state.disabled_notified)

    return RedirectResponse(url="/users", status_code=303)


@app.post("/users/enable")
def users_enable(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
    user_id: str = Form(...),
) -> Any:
    try:
        require_session(rt, request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    if not rt.jellyfin_base_url or not rt.jellyfin_admin_api_key:
        return RedirectResponse(url="/settings?error=请先配置 Jellyfin 连接信息", status_code=302)

    jf = _require_jellyfin(rt)
    try:
        ju = jf.get_user(user_id)
        if bool((ju.get("Policy") or {}).get("IsAdministrator", False)):
            _append_log(f"refuse enable admin user id={user_id}")
            return RedirectResponse(url="/users", status_code=303)
    except Exception:
        pass
    jf.set_disabled(user_id, False)

    db = _db(settings)
    enabled_username = ""
    for u in db_list_users(db):
        if u.get("jellyfin_id") != user_id:
            continue
        u["status"] = "active"
        db_upsert_user(db, u)
        enabled_username = str(u.get("username") or "")
        break

    # 发送 Telegram 通知：用户启用
    if enabled_username:
        _append_log(f"[用户] 启用 user={enabled_username}")
        _clear_notify_cache("disabled_notified", enabled_username)
        _save_notify_cache(db, "disabled_notified", app.state.disabled_notified)
        notify_user_enabled(rt, enabled_username)

    return RedirectResponse(url="/users", status_code=303)


@app.post("/users/extend")
def users_extend(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
    user_id: str = Form(...),
    days: int = Form(...),
) -> Any:
    try:
        require_session(rt, request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    if days <= 0:
        raise HTTPException(status_code=400, detail="invalid days")


    db = _db(settings)
    extended_username = ""
    new_expiration_date = ""
    new_expiration_display = ""
    registration_date = ""
    for u in db_list_users(db):
        if u.get("jellyfin_id") != user_id:
            continue

        if rt.jellyfin_base_url and rt.jellyfin_admin_api_key:
            try:
                jf = _require_jellyfin(rt)
                ju = jf.get_user(user_id)
                if bool((ju.get("Policy") or {}).get("IsAdministrator", False)):
                    _append_log(f"refuse extend admin user id={user_id}")
                    return RedirectResponse(url="/users", status_code=303)
            except Exception:
                pass

        exp = parse_iso(str(u.get("expiration_date") or ""))
        new_exp = exp + timedelta(days=int(days))
        u["expiration_date"] = to_iso(new_exp)
        new_expiration_date = to_iso(new_exp)
        new_expiration_display = format_shanghai(new_exp)
        extended_username = str(u.get("username") or "")
        registration_date = str(u.get("registration_date") or "")
        # 如果是禁用状态，按旧 bot 行为：续期同时恢复启用
        if u.get("status") == "disabled":
            if rt.jellyfin_base_url and rt.jellyfin_admin_api_key:
                jf = _require_jellyfin(rt)
                jf.set_disabled(user_id, False)
            u["status"] = "active"
        db_upsert_user(db, u)
        break

    # 发送 Telegram 通知：用户续期
    if extended_username:
        _append_log(f"[用户] 续期 user={extended_username} days={days} exp={new_expiration_display}")
        notify_user_extended(rt, extended_username, days, new_expiration_date, registration_date)

    return RedirectResponse(url="/users", status_code=303)


@app.post("/users/plan")
def users_change_plan(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
    user_id: str = Form(...),
    plan_id: str = Form(...),
) -> Any:
    try:
        require_session(rt, request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    plans = build_plans(rt)
    if plan_id not in plans:
        _append_log(f"change plan invalid plan_id={plan_id} user_id={user_id}")
        return RedirectResponse(url="/users", status_code=303)

    # 管理员用户不允许操作
    if rt.jellyfin_base_url and rt.jellyfin_admin_api_key:
        try:
            jf = _require_jellyfin(rt)
            ju = jf.get_user(user_id)
            if bool((ju.get("Policy") or {}).get("IsAdministrator", False)):
                _append_log(f"refuse change plan for admin id={user_id}")
                return RedirectResponse(url="/users", status_code=303)
        except Exception:
            pass

    db = _db(settings)
    updated = False
    changed_username = ""
    old_plan_name = ""
    for u in db_list_users(db):
        if u.get("jellyfin_id") != user_id:
            continue
        old_plan_name = str(u.get("plan_name") or "")
        changed_username = str(u.get("username") or "")
        u["plan_id"] = plan_id
        u["plan_name"] = plans[plan_id]["name"]
        db_upsert_user(db, u)
        updated = True
        break
    _append_log(f"change plan user_id={user_id} plan_id={plan_id} updated={updated}")

    # 发送 Telegram 通知：套餐变更
    if updated and changed_username:
        notify_user_plan_changed(rt, changed_username, old_plan_name, plans[plan_id]["name"])

    if updated and changed_username:
        try:
            servers, all_urls = _get_servers_and_all_urls(rt, db)
            default_blacklist_normal = _default_blacklist_normal(servers, all_urls)
            stored = db_list_ban_blacklists(db)
            is_pro = plan_id in {"5", "6", "7", "8"}
            if is_pro:
                stored.pop(changed_username, None)
            else:
                stored[changed_username] = list(default_blacklist_normal)
            db_replace_ban_blacklists(db, stored)

            overrides = db_list_ban_overrides(db)
            if changed_username not in overrides:
                overrides.add(changed_username)
                db_replace_ban_overrides(db, list(overrides))

            _append_log(f"[分流] 套餐切换自动更新 user={changed_username} plan={plans[plan_id]['name']}")
        except Exception as e:
            _append_log(f"[分流] 套餐切换自动更新失败: {e}")

    return RedirectResponse(url="/users", status_code=303)


@app.post("/users/delete")
def users_delete(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
    user_id: str = Form(...),
) -> Any:
    try:
        require_session(rt, request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    if not rt.jellyfin_base_url or not rt.jellyfin_admin_api_key:
        return RedirectResponse(url="/settings?error=请先配置 Jellyfin 连接信息", status_code=302)

    jf = _require_jellyfin(rt)
    try:
        ju = jf.get_user(user_id)
        if bool((ju.get("Policy") or {}).get("IsAdministrator", False)):
            _append_log(f"refuse delete admin user id={user_id}")
            return RedirectResponse(url="/users", status_code=303)
    except Exception:
        # 用户在 Jellyfin 不存在时，继续删除 DB 记录
        pass

    try:
        jf.delete_user(user_id)
    except Exception as e:
        # Jellyfin 删除失败（含 404/网络异常）不阻断本地删除
        _append_log(f"jellyfin delete skipped id={user_id} err={e}")

    db = _db(settings)
    # 先获取用户名用于通知
    deleted_username = ""
    registration_date = ""
    expiration_date = ""
    for u in db_list_users(db):
        if u.get("jellyfin_id") == user_id:
            deleted_username = str(u.get("username") or "")
            registration_date = str(u.get("registration_date") or "")
            expiration_date = str(u.get("expiration_date") or "")
            break

    db_delete_user(db, user_id)
    try:
        _auto_apply_ban_rules_on_user_change(db, rt)
    except Exception as e:
        _append_log(f"分流规则自动应用失败: {e}")

    # 发送 Telegram 通知：用户删除
    if deleted_username:
        _append_log(f"[用户] 删除 user={deleted_username}")
        notify_user_deleted(rt, deleted_username, registration_date, expiration_date)

    return RedirectResponse(url="/users", status_code=303)


@app.get("/audit", response_class=HTMLResponse)
def audit_page(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
    sync: str | None = None,
    synced: str | None = None,
    changed: str | None = None,
) -> Any:
    try:
        require_session(rt, request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    if not rt.jellyfin_base_url or not rt.jellyfin_admin_api_key:
        return RedirectResponse(url="/settings?error=请先配置 Jellyfin 连接信息", status_code=302)

    plans = build_plans(rt)
    jf = _require_jellyfin(rt)
    jelly_users = jf.get_users()
    db = _db(settings)
    managed = db_list_users(db)
    tracked_names = {u.get("username") for u in managed}
    tracked_ids = {u.get("jellyfin_id") for u in managed}
    untracked = [u for u in jelly_users if u.get("Name") not in tracked_names]

    jelly_ids = {u.get("Id") for u in jelly_users}
    missing_on_server = [u for u in managed if u.get("jellyfin_id") and u.get("jellyfin_id") not in jelly_ids]

    sync_mode = str(sync or "").strip() == "1"
    sync_changes: list[dict[str, str]] = []
    if sync_mode:
        jelly_map = {u.get("Id"): u for u in jelly_users if u.get("Id")}
        for u in managed:
            jellyfin_id = str(u.get("jellyfin_id") or "").strip()
            if not jellyfin_id:
                continue
            su = jelly_map.get(jellyfin_id)
            if not su:
                continue
            jf_name = str(su.get("Name") or "").strip()
            jf_disabled = bool((su.get("Policy") or {}).get("IsDisabled", False))
            jf_status = "disabled" if jf_disabled else "active"
            db_name = str(u.get("username") or "").strip()
            db_status = str(u.get("status") or "active")
            if db_name != jf_name or db_status != jf_status:
                sync_changes.append(
                    {
                        "jellyfin_id": jellyfin_id,
                        "db_username": db_name,
                        "jf_username": jf_name,
                        "db_status": db_status,
                        "jf_status": jf_status,
                    }
                )
    return templates.TemplateResponse(
        "audit.html",
        {
            "request": request,
            "untracked_users": untracked,
            "missing_users": missing_on_server,
            "plans": {k: type("P", (), v)() for k, v in plans.items()},
            "sync_mode": sync_mode,
            "sync_changes": sync_changes,
            "sync_done": str(synced or "").strip() == "1",
            "sync_changed": str(changed or "").strip(),
        },
    )


@app.post("/audit/add")
def audit_add(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
    jellyfin_id: str = Form(...),
    username: str = Form(...),
    plan_id: str = Form(...),
    registration_date: str = Form(...),
) -> Any:
    try:
        require_session(rt, request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    plans = build_plans(rt)
    if plan_id not in plans:
        raise HTTPException(status_code=400, detail="invalid plan")

    # registration_date from datetime-local (no tz); treat as Shanghai local
    reg = now_shanghai().replace(
        year=int(registration_date[0:4]),
        month=int(registration_date[5:7]),
        day=int(registration_date[8:10]),
        hour=int(registration_date[11:13]),
        minute=int(registration_date[14:16]),
        second=0,
        microsecond=0,
    )
    exp = reg + timedelta(days=int(plans[plan_id]["duration_days"]))

    db = _db(settings)
    db_upsert_user(
        db,
        {
            "jellyfin_id": jellyfin_id,
            "username": username,
            "plan_id": plan_id,
            "plan_name": plans[plan_id]["name"],
            "registration_date": to_iso(reg),
            "expiration_date": to_iso(exp),
            "status": "active",
        },
    )
    try:
        _auto_apply_ban_rules_on_user_change(db, rt)
    except Exception as e:
        _append_log(f"分流规则自动应用失败: {e}")

    _append_log(f"[用户] 纳管 user={username} plan={plans[plan_id]['name']}")

    # 发送 Telegram 通知：用户纳管（等同于创建）
    server_address = str(plans[plan_id].get("address") or "")
    notify_user_created(rt, username, plans[plan_id]["name"], to_iso(exp), server_address, "")

    return RedirectResponse(url="/audit", status_code=303)


@app.post("/audit/sync/confirm")
def audit_sync_confirm(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Any:
    try:
        require_session(rt, request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    if not rt.jellyfin_base_url or not rt.jellyfin_admin_api_key:
        return RedirectResponse(url="/settings?error=请先配置 Jellyfin 连接信息", status_code=302)

    jf = _require_jellyfin(rt)
    db = _db(settings)
    changed, _ = _apply_user_sync(db, jf)
    _append_log(f"[同步] 审计页确认同步 changed={changed}")
    return RedirectResponse(url=f"/audit?sync=1&synced=1&changed={changed}", status_code=303)


@app.get("/ban-rules", response_class=HTMLResponse)
def ban_rules_page(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Any:
    try:
        require_session(rt, request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    db = _db(settings)
    servers_raw = get_startj_pools(db)
    if not servers_raw:
        # 兜底：startj 不可用时仍让页面可操作
        fallback_urls = [u for u in [rt.jellyfin_base_url, rt.jellyfin_pro_url] if u]
        servers_raw = {"直连后端": fallback_urls, "其他类型": fallback_urls}

    # 统一规范化 URL，避免保存/渲染 key 不一致
    servers: dict[str, list[str]] = {}
    for cat, urls in (servers_raw or {}).items():
        clean: list[str] = []
        for u in urls or []:
            su = str(u).strip().rstrip("/")
            if su and su not in clean:
                clean.append(su)
        if clean:
            servers[str(cat)] = clean

    all_urls: list[str] = []
    for urls in servers.values():
        for u in urls:
            su = str(u).strip().rstrip("/")
            if su and su not in all_urls:
                all_urls.append(su)

    stored = db_list_ban_blacklists(db)
    overrides = db_list_ban_overrides(db)

    users_rows = db_list_users(db)
    users = [u.get("username") for u in users_rows if u.get("username")]
    users = [str(u) for u in users]
    users.sort(key=lambda x: str(x).lower())

    # Jellyfin 管理员用户：必须全允许（不受分流规则影响）
    admin_names: set[str] = set()
    if rt.jellyfin_base_url and rt.jellyfin_admin_api_key:
        try:
            jf = _require_jellyfin(rt)
            for ju in jf.get_users():
                if bool((ju.get("Policy") or {}).get("IsAdministrator", False)):
                    name = str(ju.get("Name") or "").strip()
                    if name:
                        admin_names.add(name)
        except Exception:
            pass

    plan_by_user = {str(u.get("username")): str(u.get("plan_id")) for u in users_rows if u.get("username")}

    # startj 当前分组名为英文 key：direct=直连后端, other=其他类型
    allowed_categories_normal = {"direct", "other"}
    allow_urls_normal: set[str] = set()
    for cat, urls in servers.items():
        c = str(cat)
        if c in allowed_categories_normal or ("direct" in c) or ("other" in c):
            for u in urls:
                su = str(u).strip().rstrip("/")
                if su:
                    allow_urls_normal.add(su)
    default_blacklist_normal = [u for u in all_urls if u not in allow_urls_normal]

    effective: dict[str, list[str]] = {}
    for username in users:
        if username in admin_names:
            effective[username] = []
            continue
        pid = plan_by_user.get(username, "")
        is_pro = pid in {"5", "6", "7", "8"}
        if username in overrides:
            effective[username] = [str(x).strip().rstrip("/") for x in stored.get(username, []) if str(x).strip()]
        else:
            effective[username] = [] if is_pro else list(default_blacklist_normal)

    return templates.TemplateResponse(
        "ban_rules.html",
        {
            "request": request,
            "users": users,
            "servers": servers,
            "rules": effective,
            "ban_rules_enabled": bool(rt.ban_rules_enabled),
        },
    )


@app.post("/ban-rules/save")
async def ban_rules_save(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Any:
    try:
        require_session(rt, request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    db = _db(settings)
    servers_raw = get_startj_pools(db)
    if not servers_raw:
        fallback_urls = [u for u in [rt.jellyfin_base_url, rt.jellyfin_pro_url] if u]
        servers_raw = {"直连后端": fallback_urls, "其他类型": fallback_urls}

    servers: dict[str, list[str]] = {}
    for cat, urls in (servers_raw or {}).items():
        clean: list[str] = []
        for u in urls or []:
            su = str(u).strip().rstrip("/")
            if su and su not in clean:
                clean.append(su)
        if clean:
            servers[str(cat)] = clean

    all_urls: list[str] = []
    for urls in servers.values():
        for u in urls:
            su = str(u).strip().rstrip("/")
            if su and su not in all_urls:
                all_urls.append(su)

    users = [u.get("username") for u in db_list_users(db) if u.get("username")]
    users = [str(u) for u in users]

    admin_names: set[str] = set()
    if rt.jellyfin_base_url and rt.jellyfin_admin_api_key:
        try:
            jf = _require_jellyfin(rt)
            for ju in jf.get_users():
                if bool((ju.get("Policy") or {}).get("IsAdministrator", False)):
                    name = str(ju.get("Name") or "").strip()
                    if name:
                        admin_names.add(name)
        except Exception:
            pass

    form = await request.form()
    new_blacklists: dict[str, list[str]] = {}

    for username in users:
        if username in admin_names:
            # 管理员必须全允许：不落库
            continue
        allowed: set[str] = set()
        for url in all_urls:
            key = f"rule_{username}_{url}"
            if key in form:
                allowed.add(url)
        blocked = [url for url in all_urls if url not in allowed]
        if blocked:
            new_blacklists[username] = blocked

    db_replace_ban_blacklists(db, new_blacklists)
    db_replace_ban_overrides(db, users)
    _append_log(f"[分流] 规则已保存 users={len(new_blacklists)}")
    return RedirectResponse(url="/ban-rules", status_code=303)


@app.post("/ban-rules/monitor-toggle")
def ban_rules_toggle(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
    enabled: str = Form(""),
) -> Any:
    try:
        require_session(rt, request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    on = str(enabled or "").strip() == "1"
    save_runtime_settings(_db(settings), {"ban_rules_enabled": "1" if on else "0"})
    _append_log(f"[分流] 监控开关更新 enabled={on}")
    return RedirectResponse(url="/ban-rules", status_code=303)


@app.get("/tasks", response_class=HTMLResponse)
def tasks_page(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Any:
    try:
        require_session(rt, request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    env_info = {
        "python_version": sys.version.split()[0],
        "platform": platform.platform(),
        "jellyfin_url": rt.jellyfin_base_url,
        "server_time": now_shanghai().strftime("%Y-%m-%d %H:%M:%S"),
    }

    raw_logs = list(getattr(app.state, "log_buffer", []))
    log_groups: dict[str, list[str]] = {
        "system": [],
        "backup": [],
        "lifecycle": [],
        "device_cleanup": [],
        "sync": [],
        "scan": [],
        "user": [],
        "ban": [],
    }
    for line in raw_logs:
        parsed = _parse_log_line(line)
        cat = _log_category(parsed.get("message") or "")
        log_groups.setdefault(cat, []).append(line)

    for key in list(log_groups.keys()):
        log_groups[key] = log_groups[key][-120:]

    log_categories = [
        {"key": "system", "name": "系统"},
        {"key": "backup", "name": "备份"},
        {"key": "lifecycle", "name": "用户生命周期"},
        {"key": "device_cleanup", "name": "设备清理"},
        {"key": "sync", "name": "同步"},
        {"key": "scan", "name": "扫描"},
        {"key": "user", "name": "用户操作"},
        {"key": "ban", "name": "分流规则"},
    ]

    return templates.TemplateResponse(
        "tasks.html",
        {
            "request": request,
            "env_info": env_info,
            "log_groups": log_groups,
            "log_categories": log_categories,
        },
    )


@app.get("/device-cleanup", response_class=HTMLResponse)
def device_cleanup_page(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Any:
    try:
        require_session(rt, request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    cleanup_flag = str(request.query_params.get("cleanup") or "").strip() == "1"
    cleanup_start = str(request.query_params.get("start") or "").strip()
    cleanup_end = str(request.query_params.get("end") or "").strip()
    cleanup_days = str(request.query_params.get("days") or "").strip()
    cleanup_keywords = str(request.query_params.get("keywords") or "").strip()
    cleanup_error = str(request.query_params.get("error") or "").strip()
    cleanup_preview: list[dict[str, str]] = []
    if cleanup_flag:
        try:
            start_dt = _parse_datetime_local(cleanup_start)
            end_dt = _parse_datetime_local(cleanup_end)
            try:
                inactive_days = int(cleanup_days or 0)
            except Exception:
                inactive_days = 0
            keywords = _parse_keywords(cleanup_keywords)
            if not rt.jellyfin_base_url or not rt.jellyfin_admin_api_key:
                cleanup_error = "请先配置 Jellyfin 连接信息"
            else:
                cleanup_preview = _device_cleanup_preview(
                    rt,
                    start_dt=start_dt,
                    end_dt=end_dt,
                    inactive_days=inactive_days,
                    app_keywords=keywords,
                )
        except Exception as e:
            cleanup_error = str(e)

    return templates.TemplateResponse(
        "device_cleanup.html",
        {
            "request": request,
            "cleanup_preview": cleanup_preview,
            "cleanup_error": cleanup_error,
            "cleanup_params": {
                "start": cleanup_start,
                "end": cleanup_end,
                "days": cleanup_days,
                "keywords": cleanup_keywords,
            },
            "cleanup_done": str(request.query_params.get("cleanup_done") or "") == "1",
            "cleanup_deleted": str(request.query_params.get("cleanup_deleted") or "0"),
        },
    )


@app.post("/device-cleanup/execute")
def device_cleanup_execute(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
    start: str = Form(""),
    end: str = Form(""),
    days: str = Form(""),
    keywords: str = Form(""),
) -> Any:
    try:
        require_session(rt, request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    if not rt.jellyfin_base_url or not rt.jellyfin_admin_api_key:
        return RedirectResponse(url="/device-cleanup?cleanup=1&error=missing_jellyfin", status_code=303)

    start_dt = _parse_datetime_local(start)
    end_dt = _parse_datetime_local(end)
    try:
        inactive_days = int(str(days or "").strip() or 0)
    except Exception:
        inactive_days = 0
    app_keywords = _parse_keywords(keywords)

    try:
        candidates = _device_cleanup_preview(
            rt,
            start_dt=start_dt,
            end_dt=end_dt,
            inactive_days=inactive_days,
            app_keywords=app_keywords,
        )
        if start_dt or end_dt:
            _append_log(
                f"[设备清理] 手动条件 时间范围 start={start_dt or '-'} end={end_dt or '-'} keywords={','.join(app_keywords) or '-'}"
            )
        else:
            _append_log(
                f"[设备清理] 手动条件 不活跃天数={inactive_days} keywords={','.join(app_keywords) or '-'}"
            )
        deleted = _device_cleanup_execute(rt, candidates)
        _append_log(f"[设备清理] 手动删除 deleted={deleted} candidates={len(candidates)}")
        return RedirectResponse(
            url=f"/device-cleanup?cleanup=1&cleanup_done=1&cleanup_deleted={deleted}",
            status_code=303,
        )
    except Exception as e:
        _append_log(f"[设备清理] 手动删除失败 err={e}")
        return RedirectResponse(url="/device-cleanup?cleanup=1", status_code=303)


@app.post("/tasks/logs/clear")
def tasks_logs_clear(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
    category: str = Form("system"),
) -> Any:
    try:
        require_session(rt, request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    buf = list(getattr(app.state, "log_buffer", []))
    if category == "all":
        app.state.log_buffer = []
        return RedirectResponse(url="/tasks", status_code=303)

    kept: list[str] = []
    for line in buf:
        parsed = _parse_log_line(line)
        cat = _log_category(parsed.get("message") or "")
        if cat != category:
            kept.append(line)
    app.state.log_buffer = kept
    return RedirectResponse(url="/tasks", status_code=303)


@app.get("/server-stream", response_class=HTMLResponse)
def server_stream_page(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Any:
    try:
        require_session(rt, request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    servers = _load_stream_servers(_db(settings))
    marks = [str(s.get("mark") or "") for s in servers if s.get("mark")]
    ns_host = str(request.query_params.get("host") or "0vid.de").strip() or "0vid.de"
    nslookup_result = _run_nslookup(ns_host)
    return templates.TemplateResponse(
        "server_stream.html",
        {
            "request": request,
            "stream_servers": servers,
            "server_marks": marks,
            "nslookup_result": nslookup_result,
            "ns_host": ns_host,
        },
    )


@app.get("/api/server-stream")
def server_stream_api(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Any:
    api_key = str(request.headers.get("X-API-Key") or "").strip()
    if api_key:
        require_api_key(rt, request)
    else:
        try:
            require_session(rt, request)
        except HTTPException:
            return JSONResponse(status_code=401, content={"ok": False, "error": "unauthorized"})

    servers = _load_stream_servers(_db(settings))
    if not servers:
        return {"ok": True, "items": []}

    items: list[dict[str, str]] = []
    for s in servers:
        veid = str(s.get("veid") or "").strip()
        key = str(s.get("api_key") or "").strip()
        mark = str(s.get("mark") or "").strip() or veid
        if not veid or not key:
            continue
        items.append(_fetch_stream_item(veid, key, mark))
    _maybe_notify_stream_usage(rt, items, _db(settings))
    return {"ok": True, "items": items}


@app.post("/tasks/sync")
def tasks_sync(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Any:
    try:
        require_session(rt, request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    if not rt.jellyfin_base_url or not rt.jellyfin_admin_api_key:
        return RedirectResponse(url="/settings?error=请先配置 Jellyfin 连接信息", status_code=302)

    jf = _require_jellyfin(rt)
    db = _db(settings)
    changed, _ = _apply_user_sync(db, jf)
    _append_log(f"[同步] 同步完成 changed={changed}")
    return RedirectResponse(url="/tasks", status_code=303)


def _apply_user_sync(db: Db, jf: JellyfinApi) -> tuple[int, list[dict[str, str]]]:
    jelly_users = jf.get_users()
    jelly_map = {u.get("Id"): u for u in jelly_users if u.get("Id")}

    users = db_list_users(db)
    changed = 0
    changes: list[dict[str, str]] = []
    for u in users:
        uid = str(u.get("jellyfin_id") or "").strip()
        if not uid:
            continue
        su = jelly_map.get(uid)
        if not su:
            continue
        name = str(su.get("Name") or "").strip()
        disabled = bool((su.get("Policy") or {}).get("IsDisabled", False))
        new_status = "disabled" if disabled else "active"

        old_name = str(u.get("username") or "").strip()
        old_status = str(u.get("status") or "active")
        if old_name != name or old_status != new_status:
            changes.append(
                {
                    "jellyfin_id": uid,
                    "db_username": old_name,
                    "jf_username": name,
                    "db_status": old_status,
                    "jf_status": new_status,
                }
            )

        if name and name != u.get("username"):
            u["username"] = name
            changed += 1
        if new_status != u.get("status"):
            u["status"] = new_status
            changed += 1

    if changed:
        for u in users:
            if u.get("jellyfin_id"):
                db_upsert_user(db, u)
    return changed, changes


@app.post("/tasks/scan")
def tasks_scan(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Any:
    try:
        require_session(rt, request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    if not rt.jellyfin_base_url or not rt.jellyfin_admin_api_key:
        return RedirectResponse(url="/settings?error=请先配置 Jellyfin 连接信息", status_code=302)

    jf = _require_jellyfin(rt)
    items = _load_library_scan_items(_db(settings), rt)
    ok = 0
    for idx, item in enumerate(items):
        lib_id = str(item.get("id") or "").strip()
        if not lib_id:
            continue
        jf.refresh_library_default(lib_id)
        ok += 1
        _append_log(f"[扫描] 媒体库刷新 ok name={item.get('name')}")
        if idx < len(items) - 1:
            time.sleep(10)
    if ok == 0:
        _append_log("[扫描] 跳过：未配置媒体库")
    return RedirectResponse(url="/tasks", status_code=303)


@app.post("/tasks/library-scan/{code}")
def tasks_scan_one(
    request: Request,
    code: str,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Any:
    try:
        require_session(rt, request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    if not rt.jellyfin_base_url or not rt.jellyfin_admin_api_key:
        return RedirectResponse(url="/settings?error=请先配置 Jellyfin 连接信息", status_code=302)

    items = _load_library_scan_items(_db(settings), rt)
    found = None
    for item in items:
        if str(item.get("code") or "").strip() == code:
            found = item
            break
    if not found:
        _append_log(f"[扫描] 无效代码 code={code}")
        return RedirectResponse(url="/tasks", status_code=303)
    lib_id = str(found.get("id") or "").strip()
    if not lib_id:
        _append_log(f"[扫描] 跳过：缺少媒体库ID code={code}")
        return RedirectResponse(url="/tasks", status_code=303)

    jf = _require_jellyfin(rt)
    jf.refresh_library_default(lib_id)
    _append_log(f"[扫描] 媒体库刷新 ok name={found.get('name')}")
    return RedirectResponse(url="/tasks", status_code=303)


@app.post("/tasks/backup")
def tasks_backup(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Any:
    try:
        require_session(rt, request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    _trigger_backup(_db(settings), rt, reason="manual")
    return RedirectResponse(url="/tasks", status_code=303)


@app.post("/tasks/backup-settings")
def tasks_backup_settings(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
    backup_enabled: str = Form(""),
    backup_time: str = Form("06:00"),
    backup_keep_daily: str = Form("7"),
    backup_keep_weekly: str = Form("4"),
    backup_keep_monthly: str = Form("2"),
) -> Any:
    try:
        require_session(rt, request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    try:
        keep_daily = int(str(backup_keep_daily or "").strip() or 7)
    except Exception:
        keep_daily = 7
    try:
        keep_weekly = int(str(backup_keep_weekly or "").strip() or 4)
    except Exception:
        keep_weekly = 4
    try:
        keep_monthly = int(str(backup_keep_monthly or "").strip() or 2)
    except Exception:
        keep_monthly = 2

    updates: dict[str, Any] = {
        "backup_enabled": "1" if str(backup_enabled).strip() else "0",
        "backup_time": str(backup_time or "").strip() or "06:00",
        "backup_keep_daily": keep_daily,
        "backup_keep_weekly": keep_weekly,
        "backup_keep_monthly": keep_monthly,
    }
    save_runtime_settings(_db(settings), updates)
    _append_log("[备份] 策略已更新")
    return RedirectResponse(url="/tasks", status_code=303)


@app.get("/tasks/backup/snapshots")
def tasks_backup_snapshots(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Any:
    try:
        require_session(rt, request)
    except HTTPException:
        return JSONResponse(status_code=401, content={"ok": False, "error": "unauthorized"})

    cfg = _build_backup_config(rt)
    ok, items, err = list_snapshots(cfg)
    return {"ok": ok, "items": items, "error": err}


@app.post("/tasks/user-lifecycle")
def tasks_user_lifecycle(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Any:
    try:
        require_session(rt, request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    _trigger_user_lifecycle(_db(settings), rt, reason="manual")
    return RedirectResponse(url="/tasks", status_code=303)


@app.post("/tasks/schedules")
def tasks_update_schedules(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
    user_lifecycle_enabled: str = Form(""),
    user_lifecycle_interval_hours: str = Form("3"),
    dns_refresh_interval_minutes: str = Form("4"),
) -> Any:
    try:
        require_session(rt, request)
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

    try:
        interval_hours = int(str(user_lifecycle_interval_hours or "").strip() or 3)
    except Exception:
        interval_hours = 3

    try:
        dns_interval = float(str(dns_refresh_interval_minutes or "").strip() or 4)
    except Exception:
        dns_interval = 4

    updates: dict[str, Any] = {
        "user_lifecycle_enabled": "1" if str(user_lifecycle_enabled).strip() else "0",
        "user_lifecycle_interval_hours": interval_hours,
        "dns_refresh_interval_minutes": dns_interval,
    }
    save_runtime_settings(_db(settings), updates)
    _append_log("[系统] 定时任务配置已更新")
    return RedirectResponse(url="/tasks", status_code=303)


@app.post("/api/tasks/sync")
def api_tasks_sync(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Any:
    require_api_key(rt, request)
    try:
        jf = _require_jellyfin(rt)
    except HTTPException:
        return JSONResponse(status_code=503, content={"ok": False, "error": "missing jellyfin config"})
    db = _db(settings)
    changed, _ = _apply_user_sync(db, jf)
    return {"ok": True, "changed": changed}


@app.post("/api/tasks/library-scan")
def api_tasks_scan(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Any:
    require_api_key(rt, request)
    try:
        jf = _require_jellyfin(rt)
    except HTTPException:
        return JSONResponse(status_code=503, content={"ok": False, "error": "missing jellyfin config"})
    items = _load_library_scan_items(_db(settings), rt)
    ran = []
    for item in items:
        lib_id = str(item.get("id") or "").strip()
        if not lib_id:
            continue
        jf.refresh_library_default(lib_id)
        ran.append(str(item.get("code") or ""))
    return {"ok": True, "libraries": ran}


@app.post("/api/tasks/library-scan/{code}")
def api_tasks_scan_one(
    request: Request,
    code: str,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Any:
    require_api_key(rt, request)
    try:
        jf = _require_jellyfin(rt)
    except HTTPException:
        return JSONResponse(status_code=503, content={"ok": False, "error": "missing jellyfin config"})

    items = _load_library_scan_items(_db(settings), rt)
    found = None
    for item in items:
        if str(item.get("code") or "").strip() == code:
            found = item
            break
    if not found:
        return JSONResponse(status_code=400, content={"ok": False, "error": "invalid code"})
    lib_id = str(found.get("id") or "").strip()
    if not lib_id:
        return JSONResponse(status_code=400, content={"ok": False, "error": f"missing library id for {code}"})
    jf.refresh_library_default(lib_id)
    return {"ok": True, "libraries": [code]}


@app.post("/api/tasks/backup")
def api_tasks_backup(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Any:
    require_api_key(rt, request)
    _trigger_backup(_db(settings), rt, reason="api")
    return {"ok": True}


@app.post("/api/tasks/user-lifecycle")
def api_tasks_user_lifecycle(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Any:
    require_api_key(rt, request)
    _trigger_user_lifecycle(_db(settings), rt, reason="api")
    return {"ok": True}


# --- External API (minimal, for other programs) ---


@app.get("/api/plans")
def api_plans(request: Request, rt: RuntimeSettings = Depends(get_runtime_settings)) -> Any:
    require_api_key(rt, request)
    return build_plans(rt)


@app.get("/api/info")
def api_info(request: Request, rt: RuntimeSettings = Depends(get_runtime_settings)) -> Any:
    require_api_key(rt, request)
    return {
        "server_time": now_shanghai().strftime("%Y-%m-%d %H:%M:%S"),
        "jellyfin_base_url": rt.jellyfin_base_url,
        "jellyfin_pro_url": rt.jellyfin_pro_url,
        "has_jellyfin_admin_api_key": bool(rt.jellyfin_admin_api_key),
        "has_web_password": bool(rt.web_password),
        "api_enabled": bool(rt.api_key),
    }


@app.get("/api/users")
def api_list_users(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Any:
    require_api_key(rt, request)
    return db_list_users(_db(settings))


@app.post("/api/users")
async def api_create_user(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Any:
    require_api_key(rt, request)
    if not rt.jellyfin_base_url or not rt.jellyfin_admin_api_key:
        return JSONResponse(status_code=503, content={"ok": False, "error": "missing jellyfin config"})
    payload = await request.json()
    username = str(payload.get("username") or "").strip()
    password = str(payload.get("password") or "").strip()
    plan_id = str(payload.get("plan_id") or "").strip()
    if not username or not password or not plan_id:
        return JSONResponse(status_code=400, content={"ok": False, "error": "missing username/password/plan_id"})

    plans = build_plans(rt)
    if plan_id not in plans:
        return JSONResponse(status_code=400, content={"ok": False, "error": "invalid plan_id"})

    jf = _require_jellyfin(rt)
    created = jf.create_user(username=username, password=password)
    jellyfin_id = created.get("Id")
    if not jellyfin_id:
        return JSONResponse(status_code=500, content={"ok": False, "error": "create user failed"})
    jf.set_initial_policy(jellyfin_id)

    reg = now_shanghai()
    exp = reg + timedelta(days=int(plans[plan_id]["duration_days"]))

    db = _db(settings)
    db_upsert_user(
        db,
        {
            "jellyfin_id": jellyfin_id,
            "username": username,
            "plan_id": plan_id,
            "plan_name": plans[plan_id]["name"],
            "registration_date": to_iso(reg),
            "expiration_date": to_iso(exp),
            "status": "active",
        },
    )
    try:
        _auto_apply_ban_rules_on_user_change(db, rt)
    except Exception as e:
        _append_log(f"[分流] 自动应用失败: {e}")
    _append_log(f"[用户] 创建 user={username} plan={plans[plan_id]['name']}")

    server_address = str(plans[plan_id].get("address") or "")
    notify_user_created(rt, username, plans[plan_id]["name"], to_iso(exp), server_address, password)
    return {"ok": True, "jellyfin_id": jellyfin_id}


@app.post("/api/users/{jellyfin_id}/disable")
def api_disable_user(
    request: Request,
    jellyfin_id: str,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Any:
    require_api_key(rt, request)
    if not rt.jellyfin_base_url or not rt.jellyfin_admin_api_key:
        return JSONResponse(status_code=503, content={"ok": False, "error": "missing jellyfin config"})
    jf = _require_jellyfin(rt)
    jf.set_disabled(jellyfin_id, True)
    db = _db(settings)
    disabled_username = ""
    registration_date = ""
    expiration_date = ""
    for u in db_list_users(db):
        if u.get("jellyfin_id") != jellyfin_id:
            continue
        u["status"] = "disabled"
        db_upsert_user(db, u)
        disabled_username = str(u.get("username") or "")
        registration_date = str(u.get("registration_date") or "")
        expiration_date = str(u.get("expiration_date") or "")
        break
    if disabled_username:
        _append_log(f"[用户] 禁用 user={disabled_username}")
        exp_key = str(expiration_date or "").strip() or "unknown"
        if _notify_once("disabled_notified", disabled_username, exp_key):
            notify_user_disabled(rt, disabled_username, registration_date, expiration_date)
            _save_notify_cache(db, "disabled_notified", app.state.disabled_notified)
    return {"ok": True}


@app.post("/api/users/{jellyfin_id}/enable")
def api_enable_user(
    request: Request,
    jellyfin_id: str,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Any:
    require_api_key(rt, request)
    if not rt.jellyfin_base_url or not rt.jellyfin_admin_api_key:
        return JSONResponse(status_code=503, content={"ok": False, "error": "missing jellyfin config"})
    jf = _require_jellyfin(rt)
    jf.set_disabled(jellyfin_id, False)
    db = _db(settings)
    enabled_username = ""
    for u in db_list_users(db):
        if u.get("jellyfin_id") != jellyfin_id:
            continue
        u["status"] = "active"
        db_upsert_user(db, u)
        enabled_username = str(u.get("username") or "")
        break
    if enabled_username:
        _append_log(f"[用户] 启用 user={enabled_username}")
        _clear_notify_cache("disabled_notified", enabled_username)
        _save_notify_cache(db, "disabled_notified", app.state.disabled_notified)
        notify_user_enabled(rt, enabled_username)
    return {"ok": True}


@app.post("/api/users/{jellyfin_id}/extend")
async def api_extend_user(
    request: Request,
    jellyfin_id: str,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Any:
    require_api_key(rt, request)
    payload = await request.json()
    days = int(payload.get("days") or 0)
    if days <= 0:
        return JSONResponse(status_code=400, content={"ok": False, "error": "invalid days"})

    db = _db(settings)
    extended_username = ""
    new_expiration_date = ""
    new_expiration_display = ""
    registration_date = ""
    for u in db_list_users(db):
        if u.get("jellyfin_id") != jellyfin_id:
            continue
        exp = parse_iso(str(u.get("expiration_date") or ""))
        new_exp = exp + timedelta(days=days)
        u["expiration_date"] = to_iso(new_exp)
        new_expiration_date = to_iso(new_exp)
        new_expiration_display = format_shanghai(new_exp)
        if u.get("status") == "disabled" and rt.jellyfin_base_url and rt.jellyfin_admin_api_key:
            jf = _require_jellyfin(rt)
            jf.set_disabled(jellyfin_id, False)
            u["status"] = "active"
        db_upsert_user(db, u)
        extended_username = str(u.get("username") or "")
        registration_date = str(u.get("registration_date") or "")
        break
    if extended_username:
        _append_log(f"[用户] 续期 user={extended_username} days={days} exp={new_expiration_display}")
        notify_user_extended(rt, extended_username, days, new_expiration_date, registration_date)
    return {"ok": True}


@app.post("/api/users/{jellyfin_id}/plan")
async def api_change_plan(
    request: Request,
    jellyfin_id: str,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Any:
    require_api_key(rt, request)
    payload = await request.json()
    plan_id = str(payload.get("plan_id") or "").strip()
    plans = build_plans(rt)
    if plan_id not in plans:
        return JSONResponse(status_code=400, content={"ok": False, "error": "invalid plan_id"})

    if rt.jellyfin_base_url and rt.jellyfin_admin_api_key:
        try:
            jf = _require_jellyfin(rt)
            ju = jf.get_user(jellyfin_id)
            if bool((ju.get("Policy") or {}).get("IsAdministrator", False)):
                return JSONResponse(status_code=400, content={"ok": False, "error": "refuse admin"})
        except Exception:
            pass

    db = _db(settings)
    updated = False
    changed_username = ""
    old_plan_name = ""
    for u in db_list_users(db):
        if u.get("jellyfin_id") != jellyfin_id:
            continue
        old_plan_name = str(u.get("plan_name") or "")
        changed_username = str(u.get("username") or "")
        u["plan_id"] = plan_id
        u["plan_name"] = plans[plan_id]["name"]
        db_upsert_user(db, u)
        updated = True
        break
    _append_log(f"change plan user_id={jellyfin_id} plan_id={plan_id} updated={updated}")

    if updated and changed_username:
        notify_user_plan_changed(rt, changed_username, old_plan_name, plans[plan_id]["name"])

    if updated and changed_username:
        try:
            servers, all_urls = _get_servers_and_all_urls(rt, db)
            default_blacklist_normal = _default_blacklist_normal(servers, all_urls)
            stored = db_list_ban_blacklists(db)
            is_pro = plan_id in {"5", "6", "7", "8"}
            if is_pro:
                stored.pop(changed_username, None)
            else:
                stored[changed_username] = list(default_blacklist_normal)
            db_replace_ban_blacklists(db, stored)

            overrides = db_list_ban_overrides(db)
            if changed_username not in overrides:
                overrides.add(changed_username)
                db_replace_ban_overrides(db, list(overrides))

            _append_log(f"[分流] 套餐切换自动更新 user={changed_username} plan={plans[plan_id]['name']}")
        except Exception as e:
            _append_log(f"[分流] 套餐切换自动更新失败: {e}")

    return {"ok": True, "updated": updated}


@app.delete("/api/users/{jellyfin_id}")
def api_delete_user(
    request: Request,
    jellyfin_id: str,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Any:
    require_api_key(rt, request)
    db = _db(settings)

    deleted_username = ""
    registration_date = ""
    expiration_date = ""
    for u in db_list_users(db):
        if u.get("jellyfin_id") == jellyfin_id:
            deleted_username = str(u.get("username") or "")
            registration_date = str(u.get("registration_date") or "")
            expiration_date = str(u.get("expiration_date") or "")
            break

    # Jellyfin 不可用/用户不存在时也允许删除本地记录
    if rt.jellyfin_base_url and rt.jellyfin_admin_api_key:
        try:
            jf = _require_jellyfin(rt)
            jf.delete_user(jellyfin_id)
        except Exception as e:
            _append_log(f"api jellyfin delete skipped id={jellyfin_id} err={e}")

    db_delete_user(db, jellyfin_id)
    try:
        _auto_apply_ban_rules_on_user_change(db, rt)
    except Exception as e:
        _append_log(f"[分流] 自动应用失败: {e}")
    if deleted_username:
        _append_log(f"[用户] 删除 user={deleted_username}")
        notify_user_deleted(rt, deleted_username, registration_date, expiration_date)
    return {"ok": True}


@app.get("/api/audit/untracked-users")
def api_audit_untracked(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Any:
    require_api_key(rt, request)
    if not rt.jellyfin_base_url or not rt.jellyfin_admin_api_key:
        return JSONResponse(status_code=503, content={"ok": False, "error": "missing jellyfin config"})
    jf = _require_jellyfin(rt)
    jelly_users = jf.get_users()
    tracked = {u.get("username") for u in db_list_users(_db(settings))}
    untracked = [u for u in jelly_users if u.get("Name") not in tracked]
    return untracked


@app.post("/api/audit/add")
async def api_audit_add(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Any:
    require_api_key(rt, request)
    payload = await request.json()
    if not isinstance(payload, dict):
        return JSONResponse(status_code=400, content={"ok": False, "error": "invalid json"})

    jellyfin_id = str(payload.get("jellyfin_id") or "").strip()
    username = str(payload.get("username") or "").strip()
    plan_id = str(payload.get("plan_id") or "").strip()
    registration_date = str(payload.get("registration_date") or "").strip()
    if not jellyfin_id or not username or not plan_id or not registration_date:
        return JSONResponse(status_code=400, content={"ok": False, "error": "missing jellyfin_id/username/plan_id/registration_date"})

    plans = build_plans(rt)
    if plan_id not in plans:
        return JSONResponse(status_code=400, content={"ok": False, "error": "invalid plan_id"})

    try:
        reg = parse_iso(registration_date)
    except Exception:
        return JSONResponse(status_code=400, content={"ok": False, "error": "invalid registration_date"})

    exp = reg + timedelta(days=int(plans[plan_id]["duration_days"]))
    db = _db(settings)
    db_upsert_user(
        db,
        {
            "jellyfin_id": jellyfin_id,
            "username": username,
            "plan_id": plan_id,
            "plan_name": plans[plan_id]["name"],
            "registration_date": to_iso(reg),
            "expiration_date": to_iso(exp),
            "status": "active",
        },
    )
    _append_log(f"[用户] 纳管 user={username} plan={plans[plan_id]['name']}")
    server_address = str(plans[plan_id].get("address") or "")
    notify_user_created(rt, username, plans[plan_id]["name"], to_iso(exp), server_address, "")
    try:
        _auto_apply_ban_rules_on_user_change(db, rt)
    except Exception as e:
        _append_log(f"分流规则自动应用失败: {e}")
    return {"ok": True}


@app.get("/api/ban/config")
def api_get_ban_config(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Any:
    require_api_key(rt, request)
    db = _db(settings)
    servers = get_startj_pools(db)
    if not servers:
        fallback_urls = [u for u in [rt.jellyfin_base_url, rt.jellyfin_pro_url] if u]
        servers = {"直连后端": fallback_urls, "其他类型": fallback_urls}

    all_urls: list[str] = []
    for urls in servers.values():
        for u in urls:
            su = str(u).strip().rstrip("/")
            if su and su not in all_urls:
                all_urls.append(su)

    stored = db_list_ban_blacklists(db)
    overrides = db_list_ban_overrides(db)
    users_rows = db_list_users(db)
    plan_by_user = {str(u.get("username")): str(u.get("plan_id")) for u in users_rows if u.get("username")}

    admin_names: set[str] = set()
    if rt.jellyfin_base_url and rt.jellyfin_admin_api_key:
        try:
            jf = _require_jellyfin(rt)
            for ju in jf.get_users():
                if bool((ju.get("Policy") or {}).get("IsAdministrator", False)):
                    name = str(ju.get("Name") or "").strip()
                    if name:
                        admin_names.add(name)
        except Exception:
            pass

    allowed_categories_normal = {"direct", "other"}
    allow_urls_normal: set[str] = set()
    for cat, urls in servers.items():
        c = str(cat)
        if c in allowed_categories_normal or ("direct" in c) or ("other" in c):
            for u in urls:
                su = str(u).strip().rstrip("/")
                if su:
                    allow_urls_normal.add(su)
    default_blacklist_normal = [u for u in all_urls if u not in allow_urls_normal]

    effective: dict[str, list[str]] = {}
    for u in users_rows:
        username = str(u.get("username") or "").strip()
        if not username:
            continue
        if username in admin_names:
            effective[username] = []
            continue
        pid = plan_by_user.get(username, "")
        is_pro = pid in {"5", "6", "7", "8"}
        if username in overrides:
            effective[username] = [str(x).strip().rstrip("/") for x in stored.get(username, []) if str(x).strip()]
        else:
            effective[username] = [] if is_pro else list(default_blacklist_normal)

    try:
        ip = request.client.host if request.client else ""
    except Exception:
        ip = ""
    _append_log(f"[分流] 配置下发 ip={ip} users={len(effective)}")
    return build_ban_config(rt, effective)


@app.get("/api/server-stream")
def api_server_stream(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Any:
    try:
        require_session(rt, request)
    except HTTPException:
        return JSONResponse(status_code=401, content={"ok": False, "error": "unauthorized"})

    servers = _load_stream_servers(_db(settings))
    items: list[dict[str, str]] = []
    for s in servers:
        info = _fetch_stream_server_info(s.get("veid", ""), s.get("api_key", ""))
        items.append(
            {
                "mark": str(s.get("mark") or ""),
                "data_center": info.get("data_center", "未知"),
                "ip": info.get("ip", "未知"),
                "traffic": info.get("traffic", ""),
                "reset_time": info.get("reset_time", ""),
            }
        )
    return {"ok": True, "items": items}


@app.post("/api/ban/config")
async def api_set_ban_config(
    request: Request,
    settings: Settings = Depends(get_settings),
    rt: RuntimeSettings = Depends(get_runtime_settings),
) -> Any:
    require_api_key(rt, request)
    payload = await request.json()
    if not isinstance(payload, dict):
        return JSONResponse(status_code=400, content={"ok": False, "error": "invalid json"})
    rules = extract_blacklists(payload)
    db = _db(settings)
    db_replace_ban_blacklists(db, rules)
    db_replace_ban_overrides(db, list(rules.keys()))

    # 允许外部写入 host/key（可选）
    updates: dict[str, Any] = {}
    host = str(payload.get("jellyfin_host") or "").strip().rstrip("/")
    key = str(payload.get("api_key") or "").strip()
    if host:
        updates["jellyfin_base_url"] = host
        if not str(payload.get("jellyfin_pro_url") or "").strip():
            updates["jellyfin_pro_url"] = host
    if key:
        updates["jellyfin_admin_api_key"] = key
    if updates:
        save_runtime_settings(db, updates, skip_if_blank=set())

    return {"ok": True}
