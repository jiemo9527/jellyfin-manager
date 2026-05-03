from __future__ import annotations

import json
import re
import urllib3
from datetime import datetime, timedelta
from typing import Any

import requests

from jm_manager.db import Db, connect
from jm_manager.runtime_settings import load_runtime_settings
from jm_manager.utils import now_shanghai, to_iso


def _load_startj_url(db: Db) -> str:
    return str(load_runtime_settings(db).startj_url or "").strip()


def _get_cached(db: Db) -> tuple[dict[str, list[str]] | None, datetime | None]:
    conn = connect(db)
    try:
        row = conn.execute(
            "SELECT value, updated_at FROM app_settings WHERE key = ?", ("startj_pools_json",)
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return None, None
    try:
        val = json.loads(str(row["value"]))
        if not isinstance(val, dict):
            return None, None
        pools: dict[str, list[str]] = {}
        for k, v in val.items():
            if not isinstance(k, str) or not isinstance(v, list):
                continue
            pools[k] = [str(x) for x in v if str(x).strip()]
    except Exception:
        return None, None
    try:
        ts = datetime.fromisoformat(str(row["updated_at"]))
    except Exception:
        ts = None
    return pools, ts


def _set_cached(db: Db, pools: dict[str, list[str]]) -> None:
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
            (
                "startj_pools_json",
                json.dumps(pools, ensure_ascii=False, separators=(",", ":")),
                to_iso(now_shanghai()),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def fetch_startj_pools(db: Db, *, timeout: int = 10) -> dict[str, list[str]]:
    # startj 站点证书可能不标准：这里采用 verify=False。
    # 注意：这不影响 Jellyfin 访问（Jellyfin 仍按你填写的 http/https 访问）。
    startj_url = _load_startj_url(db)
    if not startj_url:
        raise ValueError("startj_url 未配置")
    try:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    except Exception:
        pass
    r = requests.get(startj_url, timeout=timeout, verify=False)
    r.raise_for_status()
    html = r.text

    # 兼容常见写法：const servers = {...};
    m = re.search(r"const\s+servers\s*=\s*({[\s\S]*?});", html)
    blob = m.group(1) if m else ""
    if not blob:
        # 兜底：只提取 url 字段并放入一个分组
        urls = sorted(set(re.findall(r"url\s*:\s*['\"](https?://[^'\"]+)['\"]", html)))
        return {"其他类型": urls} if urls else {}

    pools: dict[str, list[str]] = {}
    # 尽量从对象字面量中提取：<groupName>: [ ... ]
    # groupName 允许中文/英文/数字/下划线/连字符
    group_re = re.compile(
        r"([\u4e00-\u9fffA-Za-z0-9_\-]+)\s*:\s*\[([\s\S]*?)\](?=\s*,\s*[\u4e00-\u9fffA-Za-z0-9_\-]+\s*:\s*\[|\s*\}\s*$)",
        re.M,
    )
    for gm in group_re.finditer(blob):
        name = gm.group(1)
        body = gm.group(2)
        urls = re.findall(r"url\s*:\s*['\"](https?://[^'\"\s]+)['\"]", body)
        unique = sorted(set(u.rstrip("/") for u in urls if u.strip()))
        if unique:
            pools[name] = unique
    if pools:
        return pools

    # 兜底：blob 解析失败时
    urls = sorted(set(re.findall(r"url\s*:\s*['\"](https?://[^'\"]+)['\"]", blob)))
    return {"其他类型": urls} if urls else {}


def get_startj_pools(db: Db, *, ttl_seconds: int = 600) -> dict[str, list[str]]:
    pools, _ = refresh_startj_pools(db, ttl_seconds=ttl_seconds)
    return pools


def get_cached_startj_pools(db: Db) -> dict[str, list[str]]:
    cached, _ = _get_cached(db)
    return cached or {}


def refresh_startj_pools(db: Db, *, ttl_seconds: int = 600) -> tuple[dict[str, list[str]], bool]:
    cached, ts = _get_cached(db)
    if cached and ts:
        try:
            if now_shanghai() - ts <= timedelta(seconds=ttl_seconds):
                return cached, False
        except Exception:
            return cached, False

    try:
        pools = fetch_startj_pools(db, timeout=12)
    except Exception:
        return cached or {}, False
    changed = pools != (cached or {})
    if pools and changed:
        _set_cached(db, pools)
    return pools, changed
