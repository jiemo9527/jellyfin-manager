from __future__ import annotations

from datetime import datetime, timedelta, timezone


SHANGHAI_TZ = timezone(timedelta(hours=8))


def now_shanghai() -> datetime:
    return datetime.now(SHANGHAI_TZ)


def parse_iso(iso_str: str) -> datetime:
    if not iso_str:
        return datetime.fromtimestamp(0, tz=SHANGHAI_TZ)
    if iso_str.endswith("Z"):
        iso_str = iso_str[:-1] + "+00:00"
    dt_obj = datetime.fromisoformat(iso_str)
    if dt_obj.tzinfo is None:
        return dt_obj.replace(tzinfo=SHANGHAI_TZ)
    return dt_obj.astimezone(SHANGHAI_TZ)


def to_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=SHANGHAI_TZ)
    return dt.astimezone(SHANGHAI_TZ).isoformat()
