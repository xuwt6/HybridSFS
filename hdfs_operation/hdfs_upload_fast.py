#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# hdfs_upload_fast.py
# HDFS fast upload (multi-threaded concurrency + streaming chunks), with real-time statistics throughout:
#   - Upload rate (recent-window instantaneous + overall average)
#   - Remaining file count, remaining data volume
#   - Estimated time remaining (ETA), elapsed time
#
# [Anti-overwrite design — core constraint of this script]
#   1. The remote path includes the "local source folder name" as a prefix layer:
#          <hdfs-path>/<local-folder-name>/<path-relative-to-local-directory>
#      This way, identically named files from different sources (different folders / different machines) land in different
#      HDFS directories and will never overwrite each other.
#      Example: local E:\...\data-update\p\q\f1.jpg
#          -> /merged_small_files/data-update/p/q/f1.jpg
#   2. All writes use overwrite=False; if a same-named file already exists remotely it is "skipped" and never overwritten,
#      and counted separately under the "skipped" category in statistics.
#
# Unlike hdfs_upload_system.py (single-threaded sequential upload with overwrite=True as the baseline),
# this version pursues throughput while guaranteeing that no existing data is overwritten.

import argparse
import os
import sys
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor

import requests
from requests.adapters import HTTPAdapter

# Reuse configuration from the parent directory smallfiles_heuristic to ensure "all configurations are identical"
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from hdfs import InsecureClient
from hdfs.util import HdfsError
from config import Config

MIB = 1024 * 1024

# Global stop signal: set on Ctrl+C or when the main flow ends; worker threads exit promptly
STOP = threading.Event()


# ────────────────────────────── Utility Functions ──────────────────────────────

def fmt_duration(seconds):
    """Seconds -> HH:MM:SS; returns a placeholder when unavailable."""
    if seconds is None:
        return "--:--:--"
    try:
        if seconds != seconds or seconds in (float("inf"), float("-inf")) or seconds < 0:
            return "--:--:--"
    except TypeError:
        return "--:--:--"
    total = int(seconds)
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def fmt_bytes(nbytes):
    """Automatically selects unit by size."""
    if nbytes >= 1024 ** 3:
        return f"{nbytes / (1024 ** 3):.2f} GiB"
    if nbytes >= MIB:
        return f"{nbytes / MIB:.2f} MiB"
    if nbytes >= 1024:
        return f"{nbytes / 1024:.2f} KiB"
    return f"{nbytes} B"


def build_bar(ratio, width=24):
    ratio = max(0.0, min(1.0, ratio))
    filled = int(round(ratio * width))
    return "[" + "#" * filled + "-" * (width - filled) + "]"


def is_already_exists(exc):
    """Determines whether an exception indicates \"remote file already exists\".

    WebHDFS returns FileAlreadyExistsException when overwrite=False and the target already exists;
    the hdfs library wraps it as HdfsError, where exc.exception is the class name and the message contains 'already exists'.
    """
    if isinstance(exc, HdfsError):
        if getattr(exc, "exception", None) == "FileAlreadyExistsException":
            return True
    msg = str(getattr(exc, "message", "") or exc).lower()
    return "already exists" in msg


# ────────────────────────────── Statistics Tracker ──────────────────────────────

class UploadStats:
    """Thread-safe upload statistics. All external readings are obtained via a single snapshot() call to ensure consistency.

    Each file ends up in one of three outcomes:
      - done    : successfully written to remote
      - skipped : a same-named file already exists remotely; skipped as requested, not overwritten
      - failed  : genuine failure (network/permissions, etc.), retryable
    """

    def __init__(self, total_files, total_bytes, window=15.0):
        self.total_files = total_files
        self.total_bytes = total_bytes
        self._window = window
        self._lock = threading.Lock()
        self._samples = deque()          # (timestamp, cumulative bytes) — used for recent rate
        self.done_files = 0
        self.skipped_files = 0
        self.skipped_bytes = 0
        self.failed_files = 0
        self.transferred = 0             # Bytes successfully written (failures/skips are rolled back)
        self.start_ts = None
        self.end_ts = None

    # ── Lifecycle ──
    def start(self):
        self.start_ts = time.time()
        with self._lock:
            self._samples.append((self.start_ts, 0))

    def stop(self):
        self.end_ts = time.time()

    # ── Worker thread calls ──
    def add_bytes(self, nbytes):
        with self._lock:
            self.transferred += nbytes

    def file_done(self):
        with self._lock:
            self.done_files += 1

    def file_skipped(self, sent, file_size):
        """Remote already exists: roll back previously counted bytes and add the full file size to \"processed (skipped)\"."""
        with self._lock:
            self.skipped_files += 1
            self.transferred = max(0, self.transferred - sent)
            self.skipped_bytes += file_size

    def file_failed(self, nbytes_sent):
        """On failure, roll back bytes previously counted for that file to avoid polluting remaining/rate figures."""
        with self._lock:
            self.failed_files += 1
            self.transferred = max(0, self.transferred - nbytes_sent)

    def begin_retry(self):
        """Before retry, offset the previous failure count."""
        with self._lock:
            self.failed_files = max(0, self.failed_files - 1)

    # ── Readings ──
    def elapsed(self):
        end = self.end_ts if self.end_ts is not None else time.time()
        return (end - self.start_ts) if self.start_ts is not None else 0.0

    def snapshot(self):
        """Take one complete snapshot; also advance the sliding-window sampling for the recent rate."""
        now = time.time()
        with self._lock:
            transferred = self.transferred
            done = self.done_files
            skipped = self.skipped_files
            skipped_bytes = self.skipped_bytes
            failed = self.failed_files
            self._samples.append((now, transferred))
            cutoff = now - self._window
            while len(self._samples) > 2 and self._samples[0][0] < cutoff:
                self._samples.popleft()
            t0, b0 = self._samples[0]

        end = self.end_ts if self.end_ts is not None else now
        elapsed = (end - self.start_ts) if self.start_ts is not None else 0.0

        avg_speed = transferred / elapsed if elapsed > 0 else 0.0
        dt = now - t0
        recent_speed = (transferred - b0) / dt if dt > 1e-6 else 0.0

        finished = done + skipped + failed
        remain_files = max(0, self.total_files - finished)
        # Skipped files require no transfer; their size is subtracted from remaining bytes
        remain_bytes = max(0, self.total_bytes - transferred - skipped_bytes)

        # ETA prefers the recent rate (closer to current network conditions); falls back to overall average when recent is 0
        rate = recent_speed if recent_speed > 0 else avg_speed
        eta = (remain_bytes / rate) if rate > 0 else None

        # Progress is measured by "bytes processed": actual transfers + sizes of skipped (already-existing) files
        processed_bytes = transferred + skipped_bytes
        if self.total_bytes > 0:
            ratio = processed_bytes / self.total_bytes
        elif self.total_files > 0:
            ratio = 1.0 if finished >= self.total_files else 0.0
        else:
            ratio = 0.0

        return {
            "elapsed": elapsed,
            "transferred": transferred,
            "done": done,
            "skipped": skipped,
            "skipped_bytes": skipped_bytes,
            "failed": failed,
            "remain_files": remain_files,
            "remain_bytes": remain_bytes,
            "avg_speed": avg_speed,
            "recent_speed": recent_speed,
            "eta": eta,
            "ratio": min(1.0, max(0.0, ratio)),
        }

    def render(self, snap):
        bar = build_bar(snap["ratio"])
        pct = snap["ratio"] * 100
        speed = snap["recent_speed"] if snap["recent_speed"] > 0 else snap["avg_speed"]
        return (
            f"{bar} {pct:5.1f}%  "
            f"速率 {speed / MIB:7.2f} MiB/s (均 {snap['avg_speed'] / MIB:6.2f})  "
            f"已传 {snap['done']:>7}/{self.total_files} "
            f"跳过 {snap['skipped']:>5} 失败 {snap['failed']:>4}  "
            f"剩余 {snap['remain_files']:>7} 个/{fmt_bytes(snap['remain_bytes']):>10}  "
            f"已用 {fmt_duration(snap['elapsed'])}  预计剩余 {fmt_duration(snap['eta'])}"
        )


# ────────────────────────────── Upload Core ──────────────────────────────

def make_client(workers, timeout):
    """Construct an HDFS client with an enlarged connection pool.

    requests.Session defaults to pool_maxsize=10; once concurrency rises it triggers
    "Connection pool is full, discarding connection", frequently rebuilding TCP connections and slowing uploads.
    Scaling the pool to match concurrency is a key step for speedup.
    """
    session = requests.Session()
    pool = max(workers * 2, 16)
    adapter = HTTPAdapter(pool_connections=pool, pool_maxsize=pool, max_retries=0)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.params = {"user.name": Config.HDFS_USER}
    return InsecureClient(
        Config.HDFS_URL,
        user=Config.HDFS_USER,
        session=session,
        timeout=(min(30.0, timeout), timeout) if timeout and timeout > 0 else None,
    )


def scan_local_dir(local_dir, limit=0):
    """Scan the local directory and return (file list, total bytes). Each file item is (relative path, absolute path, size)."""
    files = []
    total = 0
    for root, _, names in os.walk(local_dir):
        for name in names:
            abs_path = os.path.join(root, name)
            if not os.path.isfile(abs_path):
                continue
            try:
                size = os.path.getsize(abs_path)
            except OSError:
                continue
            rel = os.path.relpath(abs_path, local_dir).replace(os.sep, "/")
            files.append((rel, abs_path, size))
            total += size
            if limit and len(files) >= limit:
                return files, total
    return files, total


def leaf_dirs(rel_paths):
    """Collect all \"deepest\" directories that need creation to avoid issuing mkdirs for every intermediate level."""
    dirs = {os.path.dirname(r) for r in rel_paths}
    dirs.discard("")
    parents = set()
    for d in dirs:
        parts = d.split("/")
        for i in range(1, len(parts)):
            parents.add("/".join(parts[:i]))
    return sorted(d for d in dirs if d not in parents)


def ensure_remote_dirs(client, base_path, rel_paths, workers):
    """Concurrently pre-create remote directories. write() itself also creates directories, but multiple threads creating the same parent concurrently causes races;
    pre-creating significantly reduces failures. Returns the list of failed directories."""
    targets = leaf_dirs(rel_paths)
    client.makedirs(base_path)
    if not targets:
        return []

    errors = []
    lock = threading.Lock()

    def mkdir(d):
        try:
            client.makedirs(f"{base_path}/{d}")
        except Exception as exc:                     # noqa: BLE001 - log and continue
            with lock:
                errors.append((d, repr(exc)))

    with ThreadPoolExecutor(max_workers=min(workers, 16), thread_name_prefix="mkdir") as ex:
        list(ex.map(mkdir, targets))
    return errors


def upload_one(client, base_path, item, chunk_size, stats, stop_event):
    """Upload a single file (streaming, without loading the entire file into memory); never overwrite.

    :return: (status, message), where status is "ok" / "skipped" / "failed" / "cancelled"
    """
    rel_path, local_path, file_size = item
    if stop_event.is_set():
        return "cancelled", "已取消"

    hdfs_path = f"{base_path}/{rel_path}"
    sent = 0

    def chunk_iter():
        nonlocal sent
        with open(local_path, "rb") as fh:
            while True:
                if stop_event.is_set():
                    raise RuntimeError("已取消")
                chunk = fh.read(chunk_size)
                if not chunk:
                    break
                sent += len(chunk)
                stats.add_bytes(len(chunk))
                yield chunk

    try:
        # Key: overwrite=False; if the remote file already exists, FileAlreadyExistsException is raised and nothing is overwritten
        client.write(hdfs_path, data=chunk_iter(), overwrite=False)
    except HdfsError as exc:
        if is_already_exists(exc):
            stats.file_skipped(sent, file_size)
            return "skipped", "远端已存在同名文件，跳过（不覆盖）"
        stats.file_failed(sent)
        return "failed", repr(exc)
    except Exception as exc:                          # noqa: BLE001 - single-file failure does not abort the whole run
        stats.file_failed(sent)
        return "failed", repr(exc)

    stats.file_done()
    return "ok", ""


def run_uploads(client, base_path, items, chunk_size, stats, stop_event, workers, failures):
    """Upload a batch of files concurrently; genuinely failed items are appended to failures (skipped items are not counted as failures and are not retried)."""
    lock = threading.Lock()

    def task(item):
        status, err = upload_one(client, base_path, item, chunk_size, stats, stop_event)
        if status == "failed" and not stop_event.is_set():
            with lock:
                failures.append((item[0], item[1], err))

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="upload") as ex:
        list(ex.map(task, items))


# ────────────────────────────── Live Refresh Thread ──────────────────────────────

class ProgressReporter(threading.Thread):
    """Background thread refreshes the statistics line at a fixed interval.

    Uses \\r for single-line in-place refresh during terminal interaction; switches to line-by-line printing when output is redirected (non-tty),
    so the log file retains a complete statistics trace.
    """

    def __init__(self, stats, interval, tty):
        super().__init__(daemon=True, name="reporter")
        self.stats = stats
        self.interval = interval
        self.tty = tty
        self._last_len = 0
        self.lock = threading.Lock()   # Mutually exclusive with main-thread message output to avoid splitting progress lines

    def run(self):
        while not STOP.wait(self.interval):
            self.emit()

    def emit(self):
        line = self.stats.render(self.stats.snapshot())
        with self.lock:
            self._write(line)
        return line

    def say(self, text):
        """When the main thread needs to insert a regular message, go through here: clear the progress line first, then print."""
        with self.lock:
            if self.tty and self._last_len:
                sys.stdout.write("\r" + " " * self._last_len + "\r")
                self._last_len = 0
            sys.stdout.write(text + "\n")
            sys.stdout.flush()

    def _write(self, line):
        if self.tty:
            # Single-line in-place refresh: return to line start to overwrite the previous line, pad trailing spaces to erase residual characters.
            # Does not rely on ANSI escapes, avoiding garbled output on Windows consoles that do not support \033[K.
            pad = max(0, self._last_len - len(line))
            sys.stdout.write("\r" + line + " " * pad + "\r")
            self._last_len = len(line)
        else:
            sys.stdout.write(line + "\n")
        sys.stdout.flush()

    def clear_line(self):
        """Before printing the final report after all uploads finish, clear any residual progress line."""
        with self.lock:
            if self.tty and self._last_len:
                sys.stdout.write("\r" + " " * self._last_len + "\r")
                sys.stdout.flush()
                self._last_len = 0


# ────────────────────────────── Main Flow ──────────────────────────────

def resolve_remote_root(base_path, local_dir, remote_subdir, no_folder_prefix):
    """Compute remote root directory = base_path / prefix.

    The prefix defaults to the local folder name (ensuring same-named files from different sources land in different directories and do not overwrite each other);
    can be customized with --remote-subdir, or disabled with --no-folder-prefix.
    """
    folder_name = os.path.basename(local_dir.rstrip("\\/")) or "root"
    if remote_subdir:
        prefix = remote_subdir.strip("/")
    elif no_folder_prefix:
        prefix = ""
    else:
        prefix = folder_name
    return base_path + ("/" + prefix if prefix else ""), prefix, folder_name


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="HDFS 快速并发上传（不覆盖同名文件；远端保留本地文件夹名；实时统计速率/剩余文件/剩余时间/已用时间）",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--local-dir", default=Config.LOCAL_DIR, help="本地待上传目录")
    p.add_argument("--hdfs-path", default=Config.HDFS_BASE_PATH, help="HDFS 目标根目录")
    p.add_argument("--remote-subdir", default="",
                   help="自定义远端子目录名（默认用本地文件夹名，作为防覆盖前缀）")
    p.add_argument("--no-folder-prefix", action="store_true",
                   help="不加文件夹名前缀，直接传到 --hdfs-path 下（仍不覆盖同名）")
    p.add_argument("--workers", type=int, default=16, help="并发上传线程数")
    p.add_argument("--chunk-size", type=int, default=8, help="流式分块大小（MiB）")
    p.add_argument("--interval", type=float, default=1.0, help="统计刷新间隔（秒）")
    p.add_argument("--window", type=float, default=15.0, help="近期速率滑动窗口（秒）")
    p.add_argument("--timeout", type=float, default=300.0,
                   help="单次 HTTP 读超时（秒），0 表示不限时")
    p.add_argument("--retries", type=int, default=2, help="失败文件重试轮数")
    p.add_argument("--limit", type=int, default=0, help="只上传前 N 个文件（0=全部），用于试跑")
    p.add_argument("--skip-mkdir", action="store_true",
                   help="跳过远端目录预建（目录层级很浅时可省这一步）")
    p.add_argument("--failure-log", default="", help="把失败清单写入指定文件（默认不写）")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    local_dir = os.path.abspath(args.local_dir)
    base_path = "/" + args.hdfs_path.strip("/")
    workers = max(1, args.workers)
    chunk_size = max(1, args.chunk_size) * MIB

    if not os.path.isdir(local_dir):
        print(f"[错误] 本地目录不存在或不是目录: {local_dir}")
        return 2

    remote_root, prefix, folder_name = resolve_remote_root(
        base_path, local_dir, args.remote_subdir, args.no_folder_prefix)

    print("=" * 100)
    print("HDFS 快速上传（不覆盖模式）")
    print(f"  本地目录  : {local_dir}")
    print(f"  本地文件夹名: {folder_name}")
    print(f"  HDFS 目标 : {Config.HDFS_URL}{remote_root}  (user={Config.HDFS_USER})")
    print(f"  远端结构  : {remote_root}/<本地相对路径>   —— 已包含文件夹名前缀，防止同名覆盖")
    print(f"  覆盖策略  : 不覆盖（远端已存在同名文件则跳过）")
    print(f"  并发线程  : {workers}    分块大小: {args.chunk_size} MiB")
    print("=" * 100)

    print("正在扫描本地目录...")
    scan_start = time.time()
    items, total_bytes = scan_local_dir(local_dir, limit=max(0, args.limit))
    if not items:
        print("[结束] 没有发现可上传的文件。")
        return 0
    print(f"扫描完成: {len(items)} 个文件, {fmt_bytes(total_bytes)} "
          f"(耗时 {time.time() - scan_start:.2f} 秒)")

    client = make_client(workers, args.timeout)

    # Connectivity pre-check: exit immediately if unreachable, instead of waiting until the upload phase to flood errors
    try:
        client.status(remote_root, strict=False)
    except Exception as exc:                          # noqa: BLE001
        print(f"[错误] 无法连接 HDFS ({Config.HDFS_URL}): {exc}")
        return 2

    if not args.skip_mkdir:
        rel_paths = [rel for rel, _, _ in items]
        print("正在预建 HDFS 目录...")
        dir_errors = ensure_remote_dirs(client, remote_root, rel_paths, workers)
        if dir_errors:
            print(f"[警告] {len(dir_errors)} 个目录预建失败（上传时会再尝试自动创建）:")
            for d, err in dir_errors[:10]:
                print(f"   - {d}: {err}")

    stats = UploadStats(len(items), total_bytes, window=max(1.0, args.window))
    failures = []

    tty = sys.stdout.isatty()
    reporter = ProgressReporter(stats, max(0.2, args.interval), tty)

    stats.start()
    reporter.start()

    exit_code = 0
    try:
        run_uploads(client, remote_root, items, chunk_size, stats, STOP, workers, failures)

        # ── Failure retry (only truly failed files; already-existing skipped ones are not retried) ──
        round_no = 0
        while failures and round_no < args.retries and not STOP.is_set():
            round_no += 1
            retry_items = []
            for rel, abs_path, _err in failures:
                stats.begin_retry()
                try:
                    size = os.path.getsize(abs_path)
                except OSError:
                    size = 0
                retry_items.append((rel, abs_path, size))
            failures = []
            reporter.say(f"\n[重试] 第 {round_no}/{args.retries} 轮，重试 {len(retry_items)} 个文件...")
            run_uploads(client, remote_root, retry_items, chunk_size, stats, STOP, workers, failures)
    except KeyboardInterrupt:
        STOP.set()
        reporter.clear_line()
        print("\n[中断] 收到 Ctrl+C，正在停止上传（已写入的文件不会回滚）...")
        exit_code = 130
    finally:
        STOP.set()
        stats.stop()
        reporter.join(timeout=args.interval + 2)

    reporter.clear_line()
    final = stats.snapshot()
    # Print one final complete statistics line at the end so redirection to a log file also captures the final state
    print(stats.render(final), flush=True)

    elapsed = final["elapsed"]
    hours = int(elapsed // 3600)
    minutes = int((elapsed % 3600) // 60)
    seconds = elapsed % 60
    time_str = f"{hours}时{minutes}分{seconds:.2f}秒"
    ok_files = final["done"]
    avg_mib = (final["transferred"] / MIB) / elapsed if elapsed > 0 else 0.0
    files_per_sec = ok_files / elapsed if elapsed > 0 else 0.0
    avg_file_mib = (final["transferred"] / MIB) / ok_files if ok_files > 0 else 0.0

    print("\n" + "=" * 100)
    print("上传任务结束")
    print(f"   - 成功上传    : {ok_files} / {len(items)}")
    print(f"   - 跳过(已存在): {final['skipped']} 个 / {fmt_bytes(final['skipped_bytes'])}  —— 未覆盖任何远端文件")
    print(f"   - 失败文件数  : {final['failed']}")
    print(f"   - 实传数据量  : {fmt_bytes(final['transferred'])} ({final['transferred']} Bytes)")
    print(f"   - 平均文件大小: {avg_file_mib:.4f} MiB")
    print(f"   - 上传耗时    : {elapsed:.2f} 秒 ({time_str})")
    print(f"   - 平均速率    : {avg_mib:.2f} MiB/s  ({files_per_sec:.2f} 文件/秒)")
    print("=" * 100)

    if failures:
        print(f"\n[失败清单] 共 {len(failures)} 个（最多显示 20 条）:")
        for rel, _abs_path, err in failures[:20]:
            print(f"   - {rel}: {err}")
        if args.failure_log:
            log_path = os.path.abspath(args.failure_log)
            with open(log_path, "w", encoding="utf-8") as fh:
                for rel, abs_path, err in failures:
                    fh.write(f"{rel}\t{abs_path}\t{err}\n")
            print(f"[失败清单] 完整清单已写入: {log_path}")
        exit_code = 1

    return exit_code


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
