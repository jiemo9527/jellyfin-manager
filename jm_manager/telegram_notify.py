"""
Telegram 通知模块

在用户相关操作时发送通知到 Telegram。
"""
from __future__ import annotations

import logging
import threading
from typing import Any

import requests

from jm_manager.runtime_settings import RuntimeSettings
from jm_manager.utils import parse_iso

logger = logging.getLogger(__name__)

# 通知类型
NOTIFY_USER_CREATED = "user_created"
NOTIFY_USER_DISABLED = "user_disabled"
NOTIFY_USER_ENABLED = "user_enabled"
NOTIFY_USER_EXTENDED = "user_extended"
NOTIFY_USER_PLAN_CHANGED = "user_plan_changed"
NOTIFY_USER_DELETED = "user_deleted"
NOTIFY_USER_IMPORTED = "user_imported"
NOTIFY_USER_AUTO_DISABLED = "user_auto_disabled"
NOTIFY_USER_AUTO_DELETED = "user_auto_deleted"
NOTIFY_USER_BAN_KICK = "user_ban_kick"
NOTIFY_STREAM_USAGE_HIGH = "stream_usage_high"


def _format_datetime(value: str) -> str:
    if not value:
        return ""
    try:
        dt = parse_iso(str(value))
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(value)


def _format_bytes(size_bytes: int) -> str:
    if size_bytes <= 0:
        return "0 B"
    power = 1024
    n = 0
    power_labels = {0: "B", 1: "KiB", 2: "MiB", 3: "GiB", 4: "TiB"}
    size = float(size_bytes)
    while size >= power and n < 4:
        size /= power
        n += 1
    return f"{size:.2f} {power_labels[n]}"


def _to_float(value: str | int | float | None) -> float:
    try:
        return float(str(value))
    except Exception:
        return 0.0


def _to_int(value: str | int | float | None) -> int:
    try:
        return int(str(value))
    except Exception:
        return 0


def _format_message(notify_type: str, data: dict[str, Any]) -> str:
    """根据通知类型格式化消息内容"""
    username = data.get("username", "未知")
    plan_name = data.get("plan_name", "")
    days = data.get("days", 0)
    expiration_date = data.get("expiration_date", "")
    registration_date = data.get("registration_date", "")
    server_address = data.get("server_address", "")
    password = data.get("password", "")
    old_plan = data.get("old_plan_name", "")
    new_plan = data.get("new_plan_name", "")
    count = data.get("count", 0)

    if notify_type == NOTIFY_USER_CREATED:
        exp_fmt = _format_datetime(str(expiration_date))
        return "\n".join(
            [
                "🎉 用户创建成功！",
                f"📦 套餐类型: {plan_name or '未知'}",
                f"🌐 服务器地址: {server_address or '未配置'}",
                f"👤 用户名: {username}",
                f"🔑 密码: {password or '未提供'}",
                f"⏰ 到期时间: {exp_fmt}",
            ]
        )
    elif notify_type == NOTIFY_USER_DISABLED:
        reg_fmt = _format_datetime(str(registration_date))
        exp_fmt = _format_datetime(str(expiration_date))
        return (
            f"⛔ 用户禁用\n"
            f"👤 用户: {username}\n"
            f"🧾 注册时间: {reg_fmt or '未知'}\n"
            f"⏰ 到期时间: {exp_fmt or '未知'}"
        )
    elif notify_type == NOTIFY_USER_ENABLED:
        return f"✅ 用户启用\n👤 用户: {username}"
    elif notify_type == NOTIFY_USER_EXTENDED:
        exp_fmt = _format_datetime(str(expiration_date))
        reg_fmt = _format_datetime(str(registration_date))
        return (
            f"✅ 续期成功\n"
            f"👤 用户: {username}\n"
            f"🧾 注册时间: {reg_fmt or '未知'}\n"
            f"⏰ 新到期时间: {exp_fmt}"
        )
    elif notify_type == NOTIFY_USER_PLAN_CHANGED:
        return (
            f"🔁 套餐变更\n"
            f"👤 用户: {username}\n"
            f"📦 原套餐: {old_plan}\n"
            f"📦 新套餐: {new_plan}"
        )
    elif notify_type == NOTIFY_USER_DELETED:
        reg_fmt = _format_datetime(str(registration_date))
        exp_fmt = _format_datetime(str(expiration_date))
        return (
            f"🗑️ 用户删除\n"
            f"👤 用户: {username}\n"
            f"🧾 注册时间: {reg_fmt or '未知'}\n"
            f"⏰ 到期时间: {exp_fmt or '未知'}"
        )
    elif notify_type == NOTIFY_USER_IMPORTED:
        return f"📥 批量导入\n导入用户: {count} 个"
    elif notify_type == NOTIFY_USER_AUTO_DISABLED:
        reg_fmt = _format_datetime(str(registration_date))
        exp_fmt = _format_datetime(str(expiration_date))
        return (
            f"⚠️ 到期禁用\n"
            f"👤 用户: {username}\n"
            f"🧾 注册时间: {reg_fmt or '未知'}\n"
            f"⏰ 到期时间: {exp_fmt or '未知'}"
        )
    elif notify_type == NOTIFY_USER_AUTO_DELETED:
        reg_fmt = _format_datetime(str(registration_date))
        exp_fmt = _format_datetime(str(expiration_date))
        return (
            f"🧹 到期删除\n"
            f"👤 用户: {username}\n"
            f"🧾 注册时间: {reg_fmt or '未知'}\n"
            f"⏰ 到期时间: {exp_fmt or '未知'}"
        )
    elif notify_type == NOTIFY_USER_BAN_KICK:
        ip = data.get("ip", "")
        event_type = data.get("event_type", "")
        strategy = data.get("strategy", "")
        device = data.get("device_name", "")
        parts = [
            "🚫 分流剔除",
            f"👤 用户: {username}",
            f"🌐 IP: {ip or '未知'}",
            f"🧭 事件: {event_type or '未知'}",
        ]
        if strategy:
            parts.append(f"🎯 匹配: {strategy}")
        if device:
            parts.append(f"📱 设备: {device}")
        return "\n".join(parts)
    elif notify_type == NOTIFY_STREAM_USAGE_HIGH:
        mark = data.get("mark", "-")
        traffic = data.get("traffic", "-")
        percent = data.get("percent", "-")
        reset_time = data.get("reset_time", "-")
        ip = data.get("ip", "-")
        dc = data.get("data_center", "-")
        return (
            "📊 流量告警\n"
            f"🧾 标记: {mark}\n"
            f"🌐 IP: {ip}\n"
            f"📈 使用: {traffic} ({percent})\n"
            f"⏰ 重置: {reset_time}\n"
            f"🏷️ 机房: {dc}"
        )
    else:
        return f"[通知] {notify_type}: {data}"


def _parse_user_ids(user_id_str: str) -> list[str]:
    """解析用户 ID 字符串，支持逗号分隔的多个 ID"""
    if not user_id_str:
        return []
    ids = []
    for part in user_id_str.split(","):
        uid = part.strip()
        if uid:
            ids.append(uid)
    return ids


def send_telegram_notification(
    rt: RuntimeSettings,
    notify_type: str,
    data: dict[str, Any],
    *,
    sync: bool = False,
) -> bool:
    """
    发送 Telegram 通知

    参数:
        rt: 运行时设置（包含 telegram_enabled, telegram_bot_token, telegram_user_id）
        notify_type: 通知类型（如 NOTIFY_USER_CREATED）
        data: 通知数据（如 {"username": "xxx", "plan_name": "普通月卡"}）
        sync: 是否同步发送（默认异步）

    返回:
        同步模式下返回是否全部成功；异步模式下始终返回 True（发送结果通过日志记录）

    注意:
        telegram_user_id 支持逗号分隔的多个 ID，会依次发送给每个用户/群组
    """
    if not rt.telegram_enabled:
        return False

    if not rt.telegram_bot_token or not rt.telegram_user_id:
        logger.warning("Telegram 通知已启用但缺少配置（Bot Token 或 User ID）")
        return False

    user_ids = _parse_user_ids(rt.telegram_user_id)
    if not user_ids:
        logger.warning("Telegram 通知已启用但 User ID 列表为空")
        return False

    message = _format_message(notify_type, data)

    def _send_to_one(chat_id: str) -> bool:
        try:
            url = f"https://api.telegram.org/bot{rt.telegram_bot_token}/sendMessage"
            payload = {
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "HTML",
            }
            resp = requests.post(url, json=payload, timeout=10)
            if resp.status_code == 200:
                result = resp.json()
                if result.get("ok"):
                    logger.info(f"Telegram 通知发送成功: {notify_type} -> {chat_id}")
                    return True
                logger.warning(f"Telegram API 返回错误 (chat_id={chat_id}): {result}")
                return False
            logger.warning(f"Telegram 通知发送失败 (chat_id={chat_id}): HTTP {resp.status_code}")
            return False
        except requests.exceptions.Timeout:
            logger.warning(f"Telegram 通知发送超时 (chat_id={chat_id})")
            return False
        except requests.exceptions.RequestException as e:
            logger.warning(f"Telegram 通知发送异常 (chat_id={chat_id}): {e}")
            return False
        except Exception as e:
            logger.warning(f"Telegram 通知发送未知错误 (chat_id={chat_id}): {e}")
            return False

    def _do_send_all() -> bool:
        success_count = 0
        for uid in user_ids:
            if _send_to_one(uid):
                success_count += 1
        return success_count == len(user_ids)

    if sync:
        return _do_send_all()
    t = threading.Thread(target=_do_send_all, daemon=True)
    t.start()
    return True


def notify_user_ban_kick(
    rt: RuntimeSettings,
    *,
    username: str,
    ip: str,
    event_type: str,
    strategy: str,
    device_name: str = "",
) -> None:
    data = {
        "username": username,
        "ip": ip,
        "event_type": event_type,
        "strategy": strategy,
        "device_name": device_name,
    }
    send_telegram_notification(rt, NOTIFY_USER_BAN_KICK, data)


def notify_stream_usage_high(
    rt: RuntimeSettings,
    *,
    mark: str,
    ip: str,
    traffic: str,
    percent: str,
    reset_time: str,
    data_center: str,
) -> None:
    data = {
        "mark": mark,
        "ip": ip,
        "traffic": traffic,
        "percent": percent,
        "reset_time": reset_time,
        "data_center": data_center,
    }
    send_telegram_notification(rt, NOTIFY_STREAM_USAGE_HIGH, data)


def send_telegram_public_notification(
    rt: RuntimeSettings,
    message: str,
    *,
    sync: bool = False,
) -> bool:
    if not rt.telegram_public_enabled:
        return False
    if not rt.telegram_public_bot_token or not rt.telegram_public_user_id:
        logger.warning("Telegram 公共通知已启用但缺少配置（Bot Token 或 User ID）")
        return False

    user_ids = _parse_user_ids(rt.telegram_public_user_id)
    if not user_ids:
        logger.warning("Telegram 公共通知已启用但 User ID 列表为空")
        return False

    def _send_to_one(chat_id: str) -> bool:
        try:
            url = f"https://api.telegram.org/bot{rt.telegram_public_bot_token}/sendMessage"
            payload = {
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "HTML",
            }
            resp = requests.post(url, json=payload, timeout=10)
            if resp.status_code == 200:
                result = resp.json()
                if result.get("ok"):
                    logger.info(f"Telegram 公共通知发送成功 -> {chat_id}")
                    return True
                logger.warning(f"Telegram 公共通知 API 返回错误 (chat_id={chat_id}): {result}")
                return False
            logger.warning(f"Telegram 公共通知发送失败 (chat_id={chat_id}): HTTP {resp.status_code}")
            return False
        except requests.exceptions.Timeout:
            logger.warning(f"Telegram 公共通知发送超时 (chat_id={chat_id})")
            return False
        except requests.exceptions.RequestException as e:
            logger.warning(f"Telegram 公共通知发送异常 (chat_id={chat_id}): {e}")
            return False
        except Exception as e:
            logger.warning(f"Telegram 公共通知发送未知错误 (chat_id={chat_id}): {e}")
            return False

    def _do_send_all() -> bool:
        success_count = 0
        for uid in user_ids:
            if _send_to_one(uid):
                success_count += 1
        return success_count == len(user_ids)

    if sync:
        return _do_send_all()
    t = threading.Thread(target=_do_send_all, daemon=True)
    t.start()
    return True


def notify_public_backup_result(
    rt: RuntimeSettings,
    *,
    ok: bool,
    repo: str,
    source_dir: str,
    reason: str,
    duration_seconds: str | int | float = 0,
    total_bytes_processed: str | int | float = 0,
    data_added: str | int | float = 0,
    files_new: str | int | float = 0,
    files_changed: str | int | float = 0,
    total_files_processed: str | int | float = 0,
    error: str = "",
) -> None:
    status = "成功" if ok else "失败"
    icon = "✅" if ok else "❌"
    duration = _to_float(duration_seconds)
    duration_text = f"{duration:.2f}s" if duration > 0 else "未知"
    size_processed = _format_bytes(_to_int(total_bytes_processed))
    size_added = _format_bytes(_to_int(data_added))
    msg = (
        f"{icon} 备份{status}\n"
        f"📦 仓库: {repo or '未配置'}\n"
        f"📁 目录: {source_dir or '未配置'}\n"
        f"🧭 触发: {reason}\n"
        f"⏱️ 用时: {duration_text}\n"
        f"📦 处理大小: {size_processed}\n"
        f"➕ 新增数据: {size_added}\n"
        f"🧾 文件: 新增 {_to_int(files_new)} / 修改 {_to_int(files_changed)} / 总处理 {_to_int(total_files_processed)}"
    )
    if not ok and error:
        msg += f"\n❗ 错误: {error}"
    send_telegram_public_notification(rt, msg)


def notify_public_user_expiring(rt: RuntimeSettings, *, username: str, expiration_date: str, days_left: int) -> None:
    exp_fmt = _format_datetime(str(expiration_date))
    msg = (
        f"⏳ 即将到期\n"
        f"👤 用户: {username}\n"
        f"⏰ 到期时间: {exp_fmt}\n"
        f"📆 剩余天数: {days_left}"
    )
    send_telegram_public_notification(rt, msg)


def notify_public_user_auto_disabled(rt: RuntimeSettings, *, username: str, expiration_date: str) -> None:
    exp_fmt = _format_datetime(str(expiration_date))
    msg = (
        f"⚠️ 自动禁用\n"
        f"👤 用户: {username}\n"
        f"⏰ 到期时间: {exp_fmt}"
    )
    send_telegram_public_notification(rt, msg)


def notify_user_created(
    rt: RuntimeSettings,
    username: str,
    plan_name: str,
    expiration_date: str,
    server_address: str,
    password: str,
) -> None:
    """通知：用户创建"""
    send_telegram_notification(
        rt,
        NOTIFY_USER_CREATED,
        {
            "username": username,
            "plan_name": plan_name,
            "expiration_date": expiration_date,
            "server_address": server_address,
            "password": password,
        },
    )


def notify_user_disabled(rt: RuntimeSettings, username: str, registration_date: str, expiration_date: str) -> None:
    """通知：用户禁用（手动）"""
    send_telegram_notification(
        rt,
        NOTIFY_USER_DISABLED,
        {
            "username": username,
            "registration_date": registration_date,
            "expiration_date": expiration_date,
        },
    )


def notify_user_enabled(rt: RuntimeSettings, username: str) -> None:
    """通知：用户启用"""
    send_telegram_notification(
        rt,
        NOTIFY_USER_ENABLED,
        {"username": username},
    )


def notify_user_extended(
    rt: RuntimeSettings,
    username: str,
    days: int,
    expiration_date: str,
    registration_date: str,
) -> None:
    """通知：用户续期"""
    send_telegram_notification(
        rt,
        NOTIFY_USER_EXTENDED,
        {
            "username": username,
            "days": days,
            "expiration_date": expiration_date,
            "registration_date": registration_date,
        },
    )


def notify_user_plan_changed(
    rt: RuntimeSettings,
    username: str,
    old_plan_name: str,
    new_plan_name: str,
) -> None:
    """通知：套餐变更"""
    send_telegram_notification(
        rt,
        NOTIFY_USER_PLAN_CHANGED,
        {
            "username": username,
            "old_plan_name": old_plan_name,
            "new_plan_name": new_plan_name,
        },
    )


def notify_user_deleted(rt: RuntimeSettings, username: str, registration_date: str, expiration_date: str) -> None:
    """通知：用户删除"""
    send_telegram_notification(
        rt,
        NOTIFY_USER_DELETED,
        {
            "username": username,
            "registration_date": registration_date,
            "expiration_date": expiration_date,
        },
    )


def notify_user_imported(rt: RuntimeSettings, count: int) -> None:
    """通知：批量导入用户"""
    if count > 0:
        send_telegram_notification(
            rt,
            NOTIFY_USER_IMPORTED,
            {"count": count},
        )


def notify_user_auto_disabled(rt: RuntimeSettings, username: str, registration_date: str, expiration_date: str) -> None:
    """通知：用户自动禁用（到期）"""
    send_telegram_notification(
        rt,
        NOTIFY_USER_AUTO_DISABLED,
        {
            "username": username,
            "registration_date": registration_date,
            "expiration_date": expiration_date,
        },
    )


def notify_user_auto_deleted(rt: RuntimeSettings, username: str, registration_date: str, expiration_date: str) -> None:
    """通知：用户自动清理（过期超过4天）"""
    send_telegram_notification(
        rt,
        NOTIFY_USER_AUTO_DELETED,
        {
            "username": username,
            "registration_date": registration_date,
            "expiration_date": expiration_date,
        },
    )
