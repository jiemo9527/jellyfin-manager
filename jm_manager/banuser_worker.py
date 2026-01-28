from __future__ import annotations

import json
import logging
import logging.handlers
import os
import re
import socket
import sqlite3
import sys
import threading
import time
from datetime import datetime, timedelta, timezone, date
from pathlib import Path
from urllib.parse import urlparse

import requests

from jm_manager.db import Db
from jm_manager.runtime_settings import RuntimeSettings, load_runtime_settings
from jm_manager.telegram_notify import notify_user_ban_kick

# --- 1. 全局配置 ---
# banuser 不再读取本地配置文件；改为从 jm_manager 拉取配置。
POLL_INTERVAL_SECONDS = 5
YOUR_TIMEZONE_OFFSET_HOURS = 8
DNS_REFRESH_INTERVAL_MINUTES = 4
BAN_RULES_ENABLED = True
RETRY_ATTEMPTS = 5
RETRY_DELAY_SECONDS = 2
ACTIVITY_POLL_LIMIT = 200

# === 关键：管理员白名单 (双重保险) ===
# 即使配置文件中有误，脚本层也会强制忽略这些用户，防止误封
PROTECTED_USERS = ["JIEMO", "00000"]

# 匹配策略
TIME_MATCH_WINDOW_SECONDS = 5
SESSION_STARTED_WINDOW_HOURS = 1.0
SESSION_STARTED_WINDOW_SECONDS = SESSION_STARTED_WINDOW_HOURS * 3600

# --- 日志初始化 ---
LOG_FILE = str(Path("data") / "banuser.log")
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
console_handler.setLevel(logging.INFO)

file_handler = logging.handlers.TimedRotatingFileHandler(
    LOG_FILE, when="midnight", interval=1, backupCount=30, encoding="utf-8"
)
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.INFO)

root_logger = logging.getLogger()
if not root_logger.handlers:
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

# --- 配置源：SQLite（jm_manager 的数据文件） ---
# banuser 直接读取 SQLite 中的 app_settings + ban_user_blacklists。
# Web 面板保存后，banuser 下次轮询会自动生效（无需 JM_MANAGER_URL/JM_API_KEY）。
DB_PATH = os.getenv("JM_DB_PATH", str(Path("data") / "jellyfin_manager.db")).strip()
CONFIG_FINGERPRINT = ""

# --- 全局变量 ---
API_KEY = ""
JELLYFIN_HOST = ""
API_HEADERS: dict[str, str] = {}
TARGET_USER_MAP: dict[str, str] = {}  # { UserID: Username }

# 核心数据结构: 用户 -> 黑名单IP集合
# { "UserA": {"1.1.1.1", "2.2.2.2"}, "UserB": set() }
USER_IP_BLACKLISTS: dict[str, set[str]] = {}
# 原始域名配置: { "UserA": ["domain1", "domain2"] }
USER_DOMAIN_CONFIG: dict[str, list[str]] = {}

list_lock = threading.Lock()
# 对齐 legacy：禁用环境代理（requests 默认会读取 HTTP(S)_PROXY）
SESSION = requests.Session()
SESSION.trust_env = False
# ActivityLog 里 IP 字段在不同版本/语言可能不同；先匹配带标签的格式，再做兜底匹配
IP_LABELLED_REGEXES = [
    re.compile(r"IP\s*地址\s*[：:]\s*([0-9]{1,3}(?:\.[0-9]{1,3}){3})"),
    re.compile(r"IP\s*Address\s*[：:]\s*([0-9]{1,3}(?:\.[0-9]{1,3}){3})", re.IGNORECASE),
    re.compile(r"Remote\s*IP\s*[：:]\s*([0-9]{1,3}(?:\.[0-9]{1,3}){3})", re.IGNORECASE),
]
IPV4_FALLBACK_REGEX = re.compile(r"([0-9]{1,3}(?:\.[0-9]{1,3}){3})")
TZ_UTC_PLUS_8 = timezone(timedelta(hours=YOUR_TIMEZONE_OFFSET_HOURS))
PROCESSED_LOG_IDS_TODAY: set[str] = set()
CURRENT_PROCESSING_DAY: date | None = None

LAST_HEARTBEAT_AT = 0.0
HEARTBEAT_INTERVAL_SECONDS = 60
SEEN_IP_PARSE_FAIL_TODAY: set[str] = set()
SEEN_IP_OK_TODAY: set[str] = set()

_STARTED = False
_LAST_NOTIFY_SETTINGS_AT = 0.0
_NOTIFY_SETTINGS_TTL = 30
_CACHED_NOTIFY_RT: RuntimeSettings | None = None


def _clear_runtime_state() -> None:
    global API_KEY, JELLYFIN_HOST, API_HEADERS, USER_DOMAIN_CONFIG, USER_IP_BLACKLISTS, TARGET_USER_MAP, CONFIG_FINGERPRINT
    API_KEY = ""
    JELLYFIN_HOST = ""
    API_HEADERS = {}
    USER_DOMAIN_CONFIG = {}
    USER_IP_BLACKLISTS = {}
    TARGET_USER_MAP = {}
    CONFIG_FINGERPRINT = ""


def _load_notify_settings() -> RuntimeSettings | None:
    global _LAST_NOTIFY_SETTINGS_AT, _CACHED_NOTIFY_RT
    now_ts = time.time()
    if _CACHED_NOTIFY_RT and now_ts - _LAST_NOTIFY_SETTINGS_AT < _NOTIFY_SETTINGS_TTL:
        return _CACHED_NOTIFY_RT
    try:
        db = Db(DB_PATH)
        rt = load_runtime_settings(db)
        if not rt.telegram_enabled:
            _CACHED_NOTIFY_RT = None
            _LAST_NOTIFY_SETTINGS_AT = now_ts
            return None
        _CACHED_NOTIFY_RT = rt
        _LAST_NOTIFY_SETTINGS_AT = now_ts
        return rt
    except Exception as e:
        logging.warning(f"[通知] 读取 Telegram 设置失败: {e}")
        _CACHED_NOTIFY_RT = None
        _LAST_NOTIFY_SETTINGS_AT = now_ts
        return None


def _notify_kick(username: str, ip: str, event_type: str, strategy: str, device_name: str = "") -> None:
    rt = _load_notify_settings()
    if not rt:
        return
    notify_user_ban_kick(
        rt,
        username=username,
        ip=ip,
        event_type=event_type,
        strategy=strategy,
        device_name=device_name,
    )


def load_config(is_reload: bool = False) -> bool:
    global API_KEY, JELLYFIN_HOST, USER_DOMAIN_CONFIG, API_HEADERS, CONFIG_FINGERPRINT, DNS_REFRESH_INTERVAL_MINUTES
    global BAN_RULES_ENABLED

    try:
        if not DB_PATH:
            raise ValueError("缺少 JM_DB_PATH")

        p = Path(DB_PATH)
        # 注意：sqlite3.connect 会在文件不存在时创建空库，这里先检查避免误连空库
        if not p.exists() and not is_reload:
            raise ValueError(f"SQLite 不存在: {DB_PATH}")

        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            try:
                rows = conn.execute("SELECT key, value FROM app_settings").fetchall()
            except Exception:
                rows = []
            settings = {str(r["key"]): str(r["value"]) for r in rows}

            new_host = str(settings.get("jellyfin_base_url", "") or "").strip().rstrip("/")
            new_key = str(settings.get("jellyfin_admin_api_key", "") or "").strip()
            ban_enabled_raw = str(settings.get("ban_rules_enabled", "1") or "1").strip().lower()
            BAN_RULES_ENABLED = ban_enabled_raw in {"1", "true", "yes", "on"}
            minutes_raw = str(settings.get("dns_refresh_interval_minutes") or "").strip()
            hours_raw = str(settings.get("dns_refresh_interval_hours") or "").strip()
            if minutes_raw:
                try:
                    DNS_REFRESH_INTERVAL_MINUTES = float(minutes_raw)
                except Exception:
                    DNS_REFRESH_INTERVAL_MINUTES = 4
            elif hours_raw:
                try:
                    DNS_REFRESH_INTERVAL_MINUTES = float(hours_raw) * 60
                except Exception:
                    DNS_REFRESH_INTERVAL_MINUTES = 4
            else:
                DNS_REFRESH_INTERVAL_MINUTES = 4

            # { username: [url, ...] }
            rules: dict[str, list[str]] = {}
            try:
                rrows = conn.execute("SELECT username, url FROM ban_user_blacklists").fetchall()
            except Exception:
                rrows = []
            for r in rrows:
                u = str(r["username"] or "").strip()
                url = str(r["url"] or "").strip().rstrip("/")
                if not u or not url:
                    continue
                rules.setdefault(u, []).append(url)
        finally:
            conn.close()

        new_user_config = {k: sorted(set(v)) for k, v in (rules or {}).items()}

        # 移除受保护的用户
        for p_user in PROTECTED_USERS:
            if p_user in new_user_config:
                if not is_reload:
                    logging.warning(f"检测到规则包含管理员 {p_user}，已强制忽略其规则。")
                del new_user_config[p_user]

        if not new_host or not new_key:
            _clear_runtime_state()
            if is_reload:
                logging.warning("配置不完整：已暂停分流规则（缺少 Jellyfin Base URL 或 Admin API Key）")
            else:
                logging.error("配置不完整：请先在 Web /settings 设置 Jellyfin Base URL + Admin API Key")
            return False

        try:
            fingerprint = json.dumps(
                {"jellyfin_host": new_host, "api_key": new_key, "user_blacklists": new_user_config},
                sort_keys=True,
                ensure_ascii=True,
            )
        except Exception:
            fingerprint = str(time.time())

        if is_reload and fingerprint == CONFIG_FINGERPRINT:
            return False

        JELLYFIN_HOST = new_host
        API_KEY = new_key
        USER_DOMAIN_CONFIG = new_user_config
        API_HEADERS = {
            "Authorization": f'MediaBrowser Token="{API_KEY}"',
            "Content-Type": "application/json",
        }
        CONFIG_FINGERPRINT = fingerprint

        action_str = "重新加载" if is_reload else "加载"
        logging.info(
            f"配置{action_str}成功（SQLite: {DB_PATH}）。监控 {len(USER_DOMAIN_CONFIG)} 个用户的访问规则。"
        )
        if not BAN_RULES_ENABLED:
            logging.warning("【分流】监控已关闭（ban_rules_enabled=0）")
        logging.info(f"DNS 刷新间隔: {DNS_REFRESH_INTERVAL_MINUTES} 分钟")

        # 规则快照（便于确认哪些用户在监控、每个用户多少条规则）
        try:
            items = []
            for uname in sorted(USER_DOMAIN_CONFIG.keys(), key=lambda x: str(x).lower()):
                urls = USER_DOMAIN_CONFIG.get(uname) or []
                items.append(f"{uname}({len(urls)})")
            if items:
                logging.info("规则用户清单: " + ", ".join(items[:30]) + (" ..." if len(items) > 30 else ""))
        except Exception:
            pass
        return True

    except Exception as e:
        msg = f"获取/解析配置失败: {e}"
        if is_reload:
            logging.error(msg + " 保持旧配置运行。")
            return False
        logging.error(msg)
        return False


def check_config_update() -> None:
    """检测 jm_manager 配置是否有更新"""
    try:
        if load_config(is_reload=True):
            logging.info(">>> 检测到配置变更，正在热重载...")
            resolve_all_domains()
            get_target_user_ids()
            logging.info(">>> 热重载完成，新规则已生效。")
    except Exception as e:
        logging.error(f"检查配置更新出错: {e}")


def parse_iso_datetime(date_str: str | None) -> datetime | None:
    try:
        if not date_str:
            return None
        return datetime.fromisoformat(str(date_str).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def extract_ip_from_overview(overview: str) -> str | None:
    s = str(overview or "")
    for rx in IP_LABELLED_REGEXES:
        m = rx.search(s)
        if m:
            return str(m.group(1))
    m = IPV4_FALLBACK_REGEX.search(s)
    if m:
        return str(m.group(1))
    return None


def resolve_all_domains() -> None:
    """
    解析所有用户配置中涉及的域名，构建 USER_IP_BLACKLISTS。
    """
    global USER_IP_BLACKLISTS

    if not BAN_RULES_ENABLED:
        with list_lock:
            USER_IP_BLACKLISTS = {}
        logging.info("【DNS】监控已关闭，跳过解析。")
        return

    # 1. 收集所有唯一的域名以减少重复解析
    all_domains: set[str] = set()
    for domains in USER_DOMAIN_CONFIG.values():
        all_domains.update(domains)

    if not all_domains:
        logging.info("【DNS】没有需要封禁的域名规则。")
        # 清空现有黑名单
        with list_lock:
            USER_IP_BLACKLISTS = {}
        return

    logging.info(f"【DNS】开始更新解析 {len(all_domains)} 个域名...")

    # 2. 解析域名 -> IP
    domain_to_ip: dict[str, set[str]] = {}
    for url_raw in all_domains:
        # [修复逻辑] 从 URL (如 https://a.com:8096) 提取纯主机名 (a.com)
        hostname = url_raw
        try:
            # 补全 scheme 以防 urlparse 解析出错
            parse_target = url_raw
            if not parse_target.startswith(("http://", "https://")):
                parse_target = "http://" + parse_target

            parsed = urlparse(parse_target)
            if parsed.hostname:
                hostname = parsed.hostname  # 提取纯域名，去掉了端口和协议

            socket.setdefaulttimeout(3)
            # 改进：获取所有 A 记录，以应对负载均衡或CDN
            infos = socket.getaddrinfo(hostname, None)
            ips = set()
            for info in infos:
                # info[4] is (ip, port)
                ip_addr = info[4][0]
                # 简单过滤 IPv4
                ips.add(ip_addr)

            # 注意：这里 key 依然存原始 url_raw，以便后续匹配 config
            domain_to_ip[url_raw] = ips

            # DNS 明细日志已隐藏

        except Exception as e:
            # 解析失败忽略，但记录日志以便知晓
            logging.warning(f"【DNS跳过】无法解析域名 {url_raw} (识别为主机名: {hostname}): {e}")
            pass
    socket.setdefaulttimeout(None)

    dns_records = sum(len(ips) for ips in domain_to_ip.values())
    logging.info(f"【DNS】解析记录数: {dns_records} (域名数: {len(domain_to_ip)})")

    # 3. 构建用户 -> IP 映射
    new_user_ip_map: dict[str, set[str]] = {}
    for username, domains in USER_DOMAIN_CONFIG.items():
        user_bad_ips = set()
        for d in domains:
            if d in domain_to_ip:
                user_bad_ips.update(domain_to_ip[d])
        new_user_ip_map[username] = user_bad_ips

    with list_lock:
        USER_IP_BLACKLISTS = new_user_ip_map
        total_ips = sum(len(ips) for ips in USER_IP_BLACKLISTS.values())
        logging.info(f"【DNS】黑名单更新完毕。IP 规则总数: {total_ips}")

        try:
            items = []
            for uname in sorted(USER_IP_BLACKLISTS.keys(), key=lambda x: str(x).lower()):
                items.append(f"{uname}({len(USER_IP_BLACKLISTS.get(uname) or set())})")
            if items:
                logging.info("【DNS】每用户 IP 数: " + ", ".join(items[:30]) + (" ..." if len(items) > 30 else ""))
        except Exception:
            pass


def start_periodic_dns_resolver() -> None:
    if not BAN_RULES_ENABLED:
        interval_seconds = DNS_REFRESH_INTERVAL_MINUTES * 60
        logging.info("【DNS】监控已关闭，跳过刷新。")
        if interval_seconds > 0:
            t = threading.Timer(interval_seconds, start_periodic_dns_resolver)
            t.daemon = True
            t.start()
        return
    resolve_all_domains()
    interval_seconds = DNS_REFRESH_INTERVAL_MINUTES * 60
    logging.info(f"【DNS】下一次刷新间隔: {DNS_REFRESH_INTERVAL_MINUTES} 分钟")
    if interval_seconds > 0:
        t = threading.Timer(interval_seconds, start_periodic_dns_resolver)
        t.daemon = True
        t.start()


def get_target_user_ids() -> bool:
    global TARGET_USER_MAP
    try:
        response = SESSION.get(f"{JELLYFIN_HOST}/Users", headers=API_HEADERS, timeout=10)
        response.raise_for_status()

        all_users = response.json()
        users_found_map = {user["Name"]: user["Id"] for user in all_users}

        temp_map: dict[str, str] = {}
        # 只监控在配置中存在的用户
        target_usernames = USER_DOMAIN_CONFIG.keys()

        for username in target_usernames:
            # 再次检查管理员保护
            if username in PROTECTED_USERS:
                continue

            if username in users_found_map:
                user_id = users_found_map[username]
                temp_map[user_id] = username
            else:
                logging.warning(f"  [警告] 规则中的用户 '{username}' 未在 Jellyfin 找到。")

        TARGET_USER_MAP = temp_map
        logging.info(f"用户 ID 匹配完成，共监控 {len(TARGET_USER_MAP)} 个用户。")
        return True
    except Exception as e:
        logging.error(f"连接 Jellyfin API 失败: {e}")
        return False


def prime_processed_logs() -> None:
    global PROCESSED_LOG_IDS_TODAY
    try:
        response_act = SESSION.get(
            f"{JELLYFIN_HOST}/System/ActivityLog/Entries?startIndex=0&limit=200",
            headers=API_HEADERS,
            timeout=10,
        )
        if response_act.status_code == 200:
            for item in response_act.json().get("Items", []):
                log_id = str(item.get("Id") or "").strip()
                if log_id:
                    PROCESSED_LOG_IDS_TODAY.add(log_id)
    except Exception:
        pass


def get_all_devices() -> list[dict[str, str]] | None:
    try:
        response = SESSION.get(f"{JELLYFIN_HOST}/Devices", headers=API_HEADERS, timeout=5)
        if response.status_code == 200:
            return response.json().get("Items", [])
    except Exception:
        pass
    return None


def delete_device(device_id: str, device_name: str, username: str) -> bool:
    # === 最终防线 ===
    if username in PROTECTED_USERS:
        logging.error(f"【严重警告】试图踢出管理员 {username} 的设备，操作已拦截！")
        return False

    try:
        logging.warning(f"  >>> [KILL] 正在踢出设备: 用户={username}, 设备={device_name}")
        resp = SESSION.delete(
            f"{JELLYFIN_HOST}/Devices?Id={device_id}",
            headers=API_HEADERS,
            timeout=5,
        )
        if resp is not None and getattr(resp, "status_code", 0) not in (200, 204):
            logging.warning(f"  >>> [KILL] 返回状态异常: status={getattr(resp, 'status_code', None)}")
        return True
    except Exception:
        return False


def find_and_delete_device_with_retry(
    user_id: str,
    event_time: datetime | None,
    username: str,
    match_strategy: str,
) -> tuple[bool, str]:
    if not event_time:
        logging.warning(f"  >>> [MATCH] 跳过：事件时间为空 user={username} strategy={match_strategy}")
        return False, ""
    for attempt in range(RETRY_ATTEMPTS):
        logging.info(f"  >>> [MATCH] 尝试 {attempt + 1}/{RETRY_ATTEMPTS} user={username} strategy={match_strategy}")
        devices = get_all_devices()
        if not devices:
            time.sleep(RETRY_DELAY_SECONDS)
            continue

        candidates: list[tuple[float, dict[str, str]]] = []
        for d in devices:
            if d.get("LastUserId") != user_id:
                continue
            d_time = parse_iso_datetime(str(d.get("DateLastActivity") or ""))
            if not d_time:
                continue
            diff = (event_time - d_time).total_seconds()

            if match_strategy == "precise" and abs(diff) <= TIME_MATCH_WINDOW_SECONDS:
                candidates.append((abs(diff), d))
            elif match_strategy == "recent" and 0 <= diff < SESSION_STARTED_WINDOW_SECONDS:
                candidates.append((diff, d))

        if candidates:
            candidates.sort(key=lambda x: x[0])
            target = candidates[0][1]
            logging.info(
                f"  >>> [MATCH] 命中设备: user={username} device={target.get('Name')} id={target.get('Id')} last={target.get('DateLastActivity')}"
            )
            device_id = str(target.get("Id") or "").strip()
            if not device_id:
                return False, ""
            device_name = str(target.get("Name") or "")
            ok = delete_device(device_id, device_name, username)
            return ok, device_name
        time.sleep(RETRY_DELAY_SECONDS)
    return False, ""


def check_activity_and_devices() -> None:
    global CURRENT_PROCESSING_DAY, PROCESSED_LOG_IDS_TODAY

    # [新增] 每一轮循环都检查配置文件是否变更
    check_config_update()

    if not JELLYFIN_HOST or not API_KEY:
        return
    if not BAN_RULES_ENABLED:
        return

    try:
        now_utc8 = datetime.now(TZ_UTC_PLUS_8)
        if CURRENT_PROCESSING_DAY is None or now_utc8.date() != CURRENT_PROCESSING_DAY:
            PROCESSED_LOG_IDS_TODAY = set()
            CURRENT_PROCESSING_DAY = now_utc8.date()
            SEEN_IP_PARSE_FAIL_TODAY.clear()
            SEEN_IP_OK_TODAY.clear()

        resp = SESSION.get(
            f"{JELLYFIN_HOST}/System/ActivityLog/Entries?startIndex=0&limit={ACTIVITY_POLL_LIMIT}",
            headers=API_HEADERS,
            timeout=5,
        )
        resp.raise_for_status()

        items = resp.json().get("Items", [])

        # 心跳：每分钟输出一次，确认脚本在跑 & 能拿到 ActivityLog
        global LAST_HEARTBEAT_AT
        now_ts = time.time()
        if now_ts - LAST_HEARTBEAT_AT >= HEARTBEAT_INTERVAL_SECONDS:
            LAST_HEARTBEAT_AT = now_ts
            with list_lock:
                watched_users = len(USER_IP_BLACKLISTS)
                watched_ips = sum(len(v or set()) for v in USER_IP_BLACKLISTS.values())
            logging.info(
                f"[HB] poll_ok items={len(items)} processed_today={len(PROCESSED_LOG_IDS_TODAY)} watched_users={watched_users} watched_ips={watched_ips}"
            )

        new_items = 0
        relevant_items = 0
        bad_hits = 0

        for item in items:
            log_id = str(item.get("Id") or "").strip()
            if not log_id:
                continue
            if log_id in PROCESSED_LOG_IDS_TODAY:
                continue
            PROCESSED_LOG_IDS_TODAY.add(log_id)
            new_items += 1

            user_id = str(item.get("UserId") or "").strip()
            if not user_id:
                continue
            if user_id not in TARGET_USER_MAP:
                continue

            event_type = item.get("Type")
            if event_type not in ["AuthenticationSucceeded", "SessionStarted"]:
                continue

            relevant_items += 1

            overview = str(item.get("ShortOverview", "") or "")
            ip = extract_ip_from_overview(overview)
            if not ip:
                uname = TARGET_USER_MAP.get(user_id, "")
                key = f"{uname}|{event_type}"
                if key not in SEEN_IP_PARSE_FAIL_TODAY:
                    SEEN_IP_PARSE_FAIL_TODAY.add(key)
                    logging.warning(f"[IP] 无法解析 IP: user={uname} type={event_type} overview={overview[:200]}")
                continue

            username = TARGET_USER_MAP[user_id]

            with list_lock:
                user_blacklist = USER_IP_BLACKLISTS.get(username, set())
                is_bad = ip in user_blacklist

            if is_bad:
                bad_hits += 1
                logging.warning(f"!!! 发现违规 !!! 用户: {username} 使用了禁止线路 IP: {ip}")
                event_time = parse_iso_datetime(str(item.get("Date") or ""))
                strategy = "precise" if event_type == "AuthenticationSucceeded" else "recent"
                ok, device_name = find_and_delete_device_with_retry(user_id, event_time, username, strategy)
                if ok:
                    _notify_kick(username, ip, str(event_type or ""), strategy, device_name)
                else:
                    logging.warning(f"  >>> [KILL] 未找到可匹配设备或踢出失败: user={username} type={event_type} ip={ip}")
            else:
                if username not in SEEN_IP_OK_TODAY:
                    SEEN_IP_OK_TODAY.add(username)
                    with list_lock:
                        cnt = len(USER_IP_BLACKLISTS.get(username, set()) or set())
                    logging.info(f"[OK] user={username} ip={ip} (blacklist_ips={cnt})")

        if new_items and (relevant_items or bad_hits):
            logging.info(f"[POLL] new_items={new_items} relevant={relevant_items} bad_hits={bad_hits}")

    except Exception as e:
        if "Timeout" not in str(e):
            logging.error(f"轮询错误: {e}")


def _run() -> None:
    paused_logged = False
    while True:
        ok = load_config()
        if ok:
            paused_logged = False
            break
        if not paused_logged:
            logging.warning("[分流] 缺少 Jellyfin 配置，已暂停分流规则")
            paused_logged = True
        time.sleep(10)
    global CURRENT_PROCESSING_DAY
    CURRENT_PROCESSING_DAY = datetime.now(TZ_UTC_PLUS_8).date()

    start_periodic_dns_resolver()
    time.sleep(2)

    get_target_user_ids()
    prime_processed_logs()

    logging.info("--- 智能分流监控服务已启动 (支持热重载) ---")
    while True:
        check_activity_and_devices()
        time.sleep(POLL_INTERVAL_SECONDS)


def start_banuser_worker(db_path: str) -> None:
    global DB_PATH, _STARTED
    if _STARTED:
        return
    DB_PATH = str(db_path or DB_PATH).strip()
    _STARTED = True
    t = threading.Thread(target=_run, daemon=True)
    t.start()
