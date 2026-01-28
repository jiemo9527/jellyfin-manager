from __future__ import annotations

import json
import os
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime

from jm_manager.utils import SHANGHAI_TZ


@dataclass(frozen=True)
class BackupConfig:
    enabled: bool
    time_hhmm: str
    repo: str
    source_dir: str
    tag: str
    keep_daily: int
    keep_weekly: int
    keep_monthly: int
    restic_password: str = ""  # 优先使用，为空则从环境变量读取


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


def _restic_password(cfg: BackupConfig | None = None) -> str:
    # 优先使用配置中的密码，其次从环境变量读取
    if cfg and cfg.restic_password:
        return cfg.restic_password.strip()
    return str(os.getenv("JM_BACKUP_RESTIC_PASSWORD") or os.getenv("RESTIC_PASSWORD") or "").strip()


def _is_restic_lock_error(text: str) -> bool:
    t = str(text or "")
    if not t:
        return False
    return "repository is already locked" in t or "locked exclusively" in t


def _run_restic_unlock(cfg: BackupConfig, log_fn) -> None:
    password = _restic_password(cfg)
    if not password:
        log_fn("[备份] 解锁跳过：缺少 RESTIC_PASSWORD")
        return
    if not cfg.repo:
        log_fn("[备份] 解锁跳过：未设置仓库")
        return

    command = ["restic", "--repo", cfg.repo, "unlock"]
    env = os.environ.copy()
    env["RESTIC_PASSWORD"] = password
    try:
        subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            env=env,
            timeout=60,
        )
        log_fn("[备份] 已执行 unlock")
    except subprocess.CalledProcessError as e:
        log_fn(f"[备份] 解锁失败 code={e.returncode}")
        if e.stderr:
            log_fn(f"[备份] 解锁stderr: {e.stderr.strip()}")


def _run_restic_backup(
    cfg: BackupConfig,
    log_fn,
    *,
    allow_retry: bool = True,
) -> tuple[bool, dict[str, int | float | str]]:
    password = _restic_password(cfg)
    if not password:
        log_fn("[备份] 缺少 RESTIC_PASSWORD（建议设置 JM_BACKUP_RESTIC_PASSWORD）")
        return False, {"error": "missing_password"}

    if not cfg.repo or not cfg.source_dir:
        log_fn("[备份] 配置不完整：需要设置 备份仓库 与 备份目录")
        return False, {"error": "missing_config"}

    log_fn(f"[备份] 开始：{cfg.source_dir} -> {cfg.repo} (Tag: {cfg.tag})")
    start_time = time.time()

    command = [
        "restic",
        "--repo",
        cfg.repo,
        "backup",
        cfg.source_dir,
        "--tag",
        cfg.tag,
        "--json",
    ]

    env = os.environ.copy()
    env["RESTIC_PASSWORD"] = password

    result: subprocess.CompletedProcess[str] | None = None
    try:
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            env=env,
            timeout=60 * 60 * 4,
        )

        summary_line = result.stdout.strip().split("\n")[-1] if result.stdout else ""
        summary = json.loads(summary_line) if summary_line else {}
        duration = time.time() - start_time

        msg = (
            "[备份] 成功 | "
            f"新增文件={summary.get('files_new', 'N/A')} "
            f"修改文件={summary.get('files_changed', 'N/A')} "
            f"处理文件={summary.get('total_files_processed', 'N/A')} "
            f"新增数据={_format_bytes(int(summary.get('data_added', 0) or 0))} "
            f"处理大小={_format_bytes(int(summary.get('total_bytes_processed', 0) or 0))} "
            f"耗时={duration:.2f}s"
        )
        log_fn(msg)
        info: dict[str, object] = {
            "duration_seconds": duration,
            "files_new": summary.get("files_new"),
            "files_changed": summary.get("files_changed"),
            "total_files_processed": summary.get("total_files_processed"),
            "data_added": int(summary.get("data_added", 0) or 0),
            "total_bytes_processed": int(summary.get("total_bytes_processed", 0) or 0),
        }
        return True, info
    except subprocess.CalledProcessError as e:
        duration = time.time() - start_time
        log_fn(f"[备份] 失败 | 耗时={duration:.2f}s code={e.returncode}")
        if e.stderr:
            log_fn(f"[备份] stderr: {e.stderr.strip()}")
        if e.stdout:
            log_fn(f"[备份] stdout: {e.stdout.strip()}")
        if allow_retry and _is_restic_lock_error((e.stderr or "") + "\n" + (e.stdout or "")):
            log_fn("[备份] 检测到仓库锁，尝试解锁并重试一次")
            _run_restic_unlock(cfg, log_fn)
            time.sleep(5)
            return _run_restic_backup(cfg, log_fn, allow_retry=False)
        err_raw = (e.stderr or "").strip() or (e.stdout or "").strip()
        err_short = err_raw[:500] if err_raw else "command_failed"
        return False, {"duration_seconds": duration, "error": err_short, "returncode": e.returncode}
    except json.JSONDecodeError as e:
        log_fn(f"[备份] 报告解析失败: {e}")
        if result and result.stdout:
            log_fn(f"[备份] 原始输出: {result.stdout.strip()}")
        return False, {"error": f"json_decode: {e}"}
    except Exception as e:
        log_fn(f"[备份] 执行异常: {e}")
        return False, {"error": f"exception: {e}"}


def _run_restic_prune(cfg: BackupConfig, log_fn) -> None:
    password = _restic_password(cfg)
    if not password:
        log_fn("[备份] 跳过清理：缺少 RESTIC_PASSWORD")
        return

    if not cfg.repo:
        log_fn("[备份] 跳过清理：未设置仓库")
        return

    log_fn(f"[备份] 开始清理：{cfg.repo} (Tag: {cfg.tag})")
    command = [
        "restic",
        "--repo",
        cfg.repo,
        "forget",
        "--tag",
        cfg.tag,
        "--keep-daily",
        str(cfg.keep_daily),
        "--keep-weekly",
        str(cfg.keep_weekly),
        "--keep-monthly",
        str(cfg.keep_monthly),
        "--prune",
    ]
    env = os.environ.copy()
    env["RESTIC_PASSWORD"] = password

    try:
        subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            env=env,
            timeout=60 * 60,
        )
        log_fn("[备份] 清理完成")
    except subprocess.CalledProcessError as e:
        log_fn(f"[备份] 清理失败 code={e.returncode}")
        if e.stderr:
            log_fn(f"[备份] 清理stderr: {e.stderr.strip()}")


def run_backup_once(cfg: BackupConfig, log_fn) -> tuple[bool, dict[str, int | float | str]]:
    if not cfg.enabled:
        log_fn("[备份] 已禁用，跳过执行")
        return False, {"error": "disabled"}

    ok, info = _run_restic_backup(cfg, log_fn)
    if ok:
        _run_restic_prune(cfg, log_fn)
    return ok, info


def list_snapshots(cfg: BackupConfig) -> tuple[bool, list[dict[str, str]], str]:
    password = _restic_password(cfg)
    if not password:
        return False, [], "缺少 RESTIC_PASSWORD"
    if not cfg.repo:
        return False, [], "未配置备份仓库"

    command = [
        "restic",
        "--repo",
        cfg.repo,
        "snapshots",
        "--tag",
        cfg.tag,
        "--json",
    ]
    env = os.environ.copy()
    env["RESTIC_PASSWORD"] = password

    try:
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            env=env,
            timeout=60,
        )
        raw = result.stdout.strip()
        data = json.loads(raw) if raw else []
        out: list[dict[str, str]] = []
        for item in data or []:
            if not isinstance(item, dict):
                continue
            snapshot_id = str(item.get("short_id") or item.get("id") or "")
            time_raw = str(item.get("time") or "")
            try:
                dt = datetime.fromisoformat(time_raw.replace("Z", "+00:00"))
            except Exception:
                dt = None
            out.append(
                {
                    "id": snapshot_id,
                    "time": format_shanghai(dt),
                    "host": str(item.get("hostname") or ""),
                    "paths": ", ".join([str(p) for p in (item.get("paths") or [])]),
                    "tags": ", ".join([str(t) for t in (item.get("tags") or [])]),
                }
            )
        return True, out, ""
    except subprocess.CalledProcessError as e:
        return False, [], f"命令失败 code={e.returncode}"
    except Exception as e:
        return False, [], f"解析失败: {e}"


def parse_backup_time(time_hhmm: str) -> tuple[int, int] | None:
    raw = str(time_hhmm or "").strip()
    if not raw:
        return None
    try:
        parts = raw.split(":")
        if len(parts) != 2:
            return None
        hh = int(parts[0])
        mm = int(parts[1])
        if not (0 <= hh <= 23 and 0 <= mm <= 59):
            return None
        return hh, mm
    except Exception:
        return None


def format_shanghai(dt: datetime | None) -> str:
    if not dt:
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=SHANGHAI_TZ)
    return dt.astimezone(SHANGHAI_TZ).strftime("%Y-%m-%d %H:%M:%S")
