from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

from jm_manager.users_store import list_users

from jm_manager.db import Db, connect
from jm_manager.runtime_settings import RuntimeSettings, load_runtime_settings


logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("jm_admin_bot")

TZ_UTC8 = timezone(timedelta(hours=8))
DB_PATH = os.getenv("JM_DB_PATH", str(os.path.join("data", "jellyfin_manager.db"))).strip()
POLL_TIMEOUT = 30
SETTINGS_TTL = 15

PLAN_HINT = "1=普通一天 2=普通月卡 3=普通季卡 4=普通年卡 5=专线一天 6=专线月卡 7=专线季卡 8=专线年卡"

PENDING: dict[str, dict[str, Any]] = {}


def _default_manager_url() -> str:
    url = os.getenv("JM_MANAGER_URL", "").strip()
    if url:
        return url.rstrip("/")

    host = os.getenv("JM_HOST", "").strip()
    port = os.getenv("JM_PORT", "").strip()
    if host and port:
        if host in {"0.0.0.0", "::"}:
            host = "127.0.0.1"
        return f"http://{host}:{port}".rstrip("/")

    return "http://127.0.0.1:18080"


def _load_rt_cached(state: dict[str, Any]) -> RuntimeSettings | None:
    now_ts = time.time()
    if state.get("rt") and now_ts - state.get("rt_ts", 0) < SETTINGS_TTL:
        return state["rt"]
    try:
        rt = load_runtime_settings(Db(DB_PATH))
        state["rt"] = rt
        state["rt_ts"] = now_ts
        return rt
    except Exception as e:
        logger.warning(f"读取运行时设置失败: {e}")
        state["rt"] = None
        state["rt_ts"] = now_ts
        return None


def _parse_ids(value: str) -> list[str]:
    if not value:
        return []
    return [x.strip() for x in value.split(",") if x.strip()]


def _authorized_chat_ids(rt: RuntimeSettings) -> set[str]:
    return set(_parse_ids(rt.telegram_user_id))


def _tg_post(token: str, method: str, payload: dict[str, Any]) -> dict[str, Any]:
    url = f"https://api.telegram.org/bot{token}/{method}"
    resp = requests.post(url, json=payload, timeout=15)
    try:
        return resp.json()
    except Exception:
        return {"ok": False, "error": f"http {resp.status_code}"}


def _tg_get_updates(token: str, offset: int) -> list[dict[str, Any]]:
    url = f"https://api.telegram.org/bot{token}/getUpdates"
    params = {"timeout": POLL_TIMEOUT, "offset": offset}
    resp = requests.get(url, params=params, timeout=POLL_TIMEOUT + 5)
    data = resp.json() if resp is not None else {}
    if not data or not data.get("ok"):
        return []
    return data.get("result", []) or []


def _send_message(token: str, chat_id: str, text: str, reply_markup: dict[str, Any] | None = None) -> None:
    payload: dict[str, Any] = {"chat_id": chat_id, "text": text}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    _tg_post(token, "sendMessage", payload)


def _answer_callback(token: str, callback_id: str) -> None:
    _tg_post(token, "answerCallbackQuery", {"callback_query_id": callback_id})


def _manager_request(rt: RuntimeSettings, method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    base = _default_manager_url()
    url = f"{base}{path}"
    headers = {"X-API-Key": str(rt.api_key or "").strip()}
    if not headers["X-API-Key"]:
        return {"ok": False, "error": "未配置 JM_API_KEY"}
    try:
        resp = requests.request(method, url, json=payload, headers=headers, timeout=15)
        if resp.headers.get("Content-Type", "").startswith("application/json"):
            data = resp.json()
            if isinstance(data, dict) and "ok" in data:
                return data
            return {"ok": True, "data": data}
        return {"ok": resp.ok, "status": resp.status_code, "text": resp.text[:500]}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _load_library_items() -> list[dict[str, str]]:
    conn = connect(Db(DB_PATH))
    try:
        row = conn.execute(
            "SELECT value FROM app_settings WHERE key = ?", ("library_scan_items_json",)
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
        code = str(item.get("code") or "").strip()
        name = str(item.get("name") or "").strip()
        lib_id = str(item.get("id") or "").strip()
        if code and lib_id:
            out.append({"code": code, "name": name, "id": lib_id})
    return out


def _menu_markup() -> dict[str, Any]:
    return {
        "inline_keyboard": [
            [
                {"text": "新增用户", "callback_data": "add_user"},
                {"text": "禁用用户", "callback_data": "disable_user"},
            ],
            [
                {"text": "删除用户", "callback_data": "delete_user"},
                {"text": "续期用户", "callback_data": "extend_user"},
            ],
            [
                {"text": "改套餐", "callback_data": "change_plan"},
                {"text": "刷新全部库", "callback_data": "scan_all"},
            ],
            [
                {"text": "刷新指定库", "callback_data": "scan_one"},
                {"text": "手动备份", "callback_data": "backup"},
            ],
            [
                {"text": "查询流量", "callback_data": "stream"},
                {"text": "列出用户", "callback_data": "list_users"},
            ],
        ]
    }


def _library_keyboard(items: list[dict[str, str]]) -> dict[str, Any]:
    rows: list[list[dict[str, str]]] = []
    for it in items[:12]:
        code = str(it.get("code") or "").strip()
        name = str(it.get("name") or "").strip()
        if not code:
            continue
        rows.append([{"text": f"{name or code}", "callback_data": f"scan_pick:{code}"}])
    return {"inline_keyboard": rows}


def _set_pending(chat_id: str, action: str) -> None:
    PENDING[chat_id] = {"action": action}


def _clear_pending(chat_id: str) -> None:
    if chat_id in PENDING:
        del PENDING[chat_id]


def _resolve_user_id(rt: RuntimeSettings, identifier: str) -> tuple[str, str]:
    identifier = str(identifier or "").strip()
    if not identifier:
        return "", ""
    data = _manager_request(rt, "GET", "/api/users")
    users = data.get("data") if isinstance(data, dict) else None
    if isinstance(data, dict) and data.get("ok") is False:
        return "", str(data.get("error") or "")
    if not isinstance(users, list):
        users = data if isinstance(data, list) else []
    for raw in users:
        if not isinstance(raw, dict):
            continue
        uid = str(raw.get("jellyfin_id") or "").strip()
        uname = str(raw.get("username") or "").strip()
        if identifier == uid or identifier == uname:
            return uid, ""
        if identifier.lower() == uname.lower():
            return uid, ""
    return "", "未找到用户"


def _handle_command(token: str, rt: RuntimeSettings, chat_id: str, text: str) -> None:
    cmd = text.strip().split()[0]
    if cmd in {"/start", "/menu"}:
        _clear_pending(chat_id)
        _send_message(token, chat_id, "管理菜单：", _menu_markup())
        return
    if cmd == "/help":
        _clear_pending(chat_id)
        _send_message(
            token,
            chat_id,
            "可用指令：\n"
            "/menu - 打开菜单\n"
            "/cancel - 取消当前操作\n"
            f"套餐说明：{PLAN_HINT}",
        )
        return
    if cmd == "/cancel":
        _clear_pending(chat_id)
        _send_message(token, chat_id, "已取消当前操作。")
        return
    _send_message(token, chat_id, "未知指令，发送 /menu 打开菜单。")


def _handle_pending(token: str, rt: RuntimeSettings, chat_id: str, text: str) -> None:
    pending = PENDING.get(chat_id)
    if not pending:
        _send_message(token, chat_id, "请输入 /menu 打开菜单。")
        return

    action = pending.get("action")
    parts = text.strip().split()
    if action == "add_user":
        if len(parts) < 3:
            _send_message(token, chat_id, f"格式：用户名 密码 套餐ID(1-8)\n{PLAN_HINT}")
            return
        username, password, plan_id = parts[0], parts[1], parts[2]
        res = _manager_request(rt, "POST", "/api/users", {"username": username, "password": password, "plan_id": plan_id})
        _send_message(token, chat_id, f"新增用户结果：{res}")
        _clear_pending(chat_id)
        return
    if action in {"disable_user", "delete_user"}:
        if not parts:
            _send_message(token, chat_id, "格式：用户名 或 Jellyfin ID")
            return
        user_id, err = _resolve_user_id(rt, parts[0])
        if not user_id:
            _send_message(token, chat_id, f"未找到用户：{err}")
            return
        if action == "disable_user":
            res = _manager_request(rt, "POST", f"/api/users/{user_id}/disable")
        else:
            res = _manager_request(rt, "DELETE", f"/api/users/{user_id}")
        _send_message(token, chat_id, f"操作结果：{res}")
        _clear_pending(chat_id)
        return
    if action == "extend_user":
        if len(parts) < 2:
            _send_message(token, chat_id, "格式：用户名/ID 延长天数")
            return
        user_id, err = _resolve_user_id(rt, parts[0])
        if not user_id:
            _send_message(token, chat_id, f"未找到用户：{err}")
            return
        try:
            days = int(parts[1])
        except Exception:
            days = 0
        if days <= 0:
            _send_message(token, chat_id, "延长天数必须为正整数")
            return
        res = _manager_request(rt, "POST", f"/api/users/{user_id}/extend", {"days": days})
        _send_message(token, chat_id, f"续期结果：{res}")
        _clear_pending(chat_id)
        return
    if action == "change_plan":
        if len(parts) < 2:
            _send_message(token, chat_id, f"格式：用户名/ID 套餐ID(1-8)\n{PLAN_HINT}")
            return
        user_id, err = _resolve_user_id(rt, parts[0])
        if not user_id:
            _send_message(token, chat_id, f"未找到用户：{err}")
            return
        plan_id = parts[1]
        res = _manager_request(rt, "POST", f"/api/users/{user_id}/plan", {"plan_id": plan_id})
        _send_message(token, chat_id, f"改套餐结果：{res}")
        _clear_pending(chat_id)
        return

    _send_message(token, chat_id, "操作已失效，请重新打开菜单。")
    _clear_pending(chat_id)


def _handle_callback(token: str, rt: RuntimeSettings, chat_id: str, data: str) -> None:
    if data == "list_users":
        try:
            users = list_users(Db(DB_PATH))
        except Exception as e:
            _send_message(token, chat_id, f"查询失败：{e}")
            return
        entries = []
        for raw in users:
            if not isinstance(raw, dict):
                continue
            name = str(raw.get("username") or "-")
            plan = str(raw.get("plan_name") or "-")
            status = str(raw.get("status") or "-")
            reg_raw = str(raw.get("registration_date") or "")
            reg_dt = None
            if reg_raw:
                try:
                    reg_dt = datetime.fromisoformat(reg_raw.replace("Z", "+00:00"))
                    if reg_dt.tzinfo is None:
                        reg_dt = reg_dt.replace(tzinfo=TZ_UTC8)
                except Exception:
                    reg_dt = None
            entries.append((reg_dt, name, plan, status, reg_raw))
        entries.sort(key=lambda x: x[0] or datetime.max.replace(tzinfo=TZ_UTC8))
        lines = []
        for reg_dt, name, plan, status, reg_raw in entries:
            if reg_dt:
                try:
                    reg_text = reg_dt.astimezone(TZ_UTC8).strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    reg_text = reg_raw or "-"
            else:
                reg_text = reg_raw or "-"
            lines.append(f"{name} | {plan} | {status} | {reg_text}")
        if not lines:
            _send_message(token, chat_id, "暂无用户")
            return
        page_size = 100
        total_pages = (len(lines) + page_size - 1) // page_size
        for idx in range(total_pages):
            start = idx * page_size
            chunk = lines[start:start + page_size]
            header = f"用户列表 第{idx + 1}/{total_pages}页："
            text = header + "\n" + "\n".join(chunk)
            _send_message(token, chat_id, text)
        return
    if data == "scan_all":
        res = _manager_request(rt, "POST", "/api/tasks/library-scan")
        _send_message(token, chat_id, f"刷新全部库结果：{res}")
        return
    if data == "backup":
        res = _manager_request(rt, "POST", "/api/tasks/backup")
        _send_message(token, chat_id, f"手动备份触发结果：{res}")
        return
    if data == "stream":
        res = _manager_request(rt, "GET", "/api/server-stream")
        if res.get("ok") is False:
            _send_message(token, chat_id, f"流量查询失败：{res.get('error')}")
            return
        items = res.get("items") or res.get("data", {}).get("items")
        if not isinstance(items, list) or not items:
            _send_message(token, chat_id, "暂无流量数据")
            return
        lines = []
        for it in items:
            mark = it.get("mark") or "-"
            traffic = it.get("traffic") or "-"
            percent = it.get("percent") or "-"
            reset_time = it.get("reset_time") or "-"
            lines.append(f"{mark} | {traffic} | {percent} | {reset_time}")
        _send_message(token, chat_id, "流量概览：\n" + "\n".join(lines[:20]))
        return

    if data == "add_user":
        _set_pending(chat_id, "add_user")
        _send_message(token, chat_id, f"请输入：用户名 密码 套餐ID(1-8)\n{PLAN_HINT}")
        return
    if data == "disable_user":
        _set_pending(chat_id, "disable_user")
        _send_message(token, chat_id, "请输入：用户名 或 Jellyfin ID")
        return
    if data == "delete_user":
        _set_pending(chat_id, "delete_user")
        _send_message(token, chat_id, "请输入：用户名 或 Jellyfin ID")
        return
    if data == "extend_user":
        _set_pending(chat_id, "extend_user")
        _send_message(token, chat_id, "请输入：用户名/ID 延长天数")
        return
    if data == "change_plan":
        _set_pending(chat_id, "change_plan")
        _send_message(token, chat_id, f"请输入：用户名/ID 套餐ID(1-8)\n{PLAN_HINT}")
        return
    if data == "scan_one":
        items = _load_library_items()
        if not items:
            _send_message(token, chat_id, "未配置媒体库，请先在 /settings 添加。")
            return
        _send_message(token, chat_id, "请选择要刷新的库：", _library_keyboard(items))
        return
    if data.startswith("scan_pick:"):
        code = data.split(":", 1)[-1]
        res = _manager_request(rt, "POST", f"/api/tasks/library-scan/{code}")
        _send_message(token, chat_id, f"刷新库结果：{res}")
        return

    _send_message(token, chat_id, "未知操作，请发送 /menu。")


def main() -> None:
    state: dict[str, Any] = {}
    offset = 0
    while True:
        rt = _load_rt_cached(state)
        if not rt or not rt.telegram_bot_token:
            logger.warning("敏感通知 Bot 未配置，等待...")
            time.sleep(10)
            continue
        allowed = _authorized_chat_ids(rt)
        if not allowed:
            logger.warning("未配置 Telegram 管理员 ID，等待...")
            time.sleep(10)
            continue
        token = rt.telegram_bot_token

        try:
            updates = _tg_get_updates(token, offset)
        except Exception as e:
            logger.warning(f"拉取更新失败: {e}")
            time.sleep(3)
            continue

        for upd in updates:
            offset = max(offset, int(upd.get("update_id", 0)) + 1)
            if "message" in upd:
                msg = upd.get("message") or {}
                chat = msg.get("chat") or {}
                chat_id = str(chat.get("id") or "")
                text = str(msg.get("text") or "").strip()
                if not chat_id or not text:
                    continue
                if chat_id not in allowed:
                    continue
                if text.startswith("/"):
                    _handle_command(token, rt, chat_id, text)
                else:
                    _handle_pending(token, rt, chat_id, text)
            if "callback_query" in upd:
                cq = upd.get("callback_query") or {}
                data = str(cq.get("data") or "")
                chat_id = str((cq.get("message") or {}).get("chat", {}).get("id") or "")
                if not chat_id or chat_id not in allowed:
                    continue
                _answer_callback(token, str(cq.get("id") or ""))
                _handle_callback(token, rt, chat_id, data)

        time.sleep(0.5)


if __name__ == "__main__":
    main()
