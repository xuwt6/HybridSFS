#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import time
import shutil
import statistics
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import List

from hdfs import InsecureClient

from file_search import FileSearcher
from config import Config

@dataclass
class UserResult:
    """Task result for a single user"""
    user_id: int
    search_mode: str
    query: str
    matched_files: int
    downloaded_files: int
    search_time_sec: float
    download_time_sec: float
    total_bytes: int
    success: bool
    error: str = ""
    search_end_abs: float = 0.0    # Absolute timestamp when search ended
    task_end_abs: float = 0.0      # Absolute timestamp when the entire task ended


def single_user_task(
    user_id: int,
    query: str,
    search_mode: str,
    searcher: FileSearcher,
    hdfs_client: InsecureClient,
    base_download_dir: str,
    global_start: float,
    max_downloads_per_user: int = 1000,
    download_timeout: int = 300,
    user_download_time_limit: int = 1200,
) -> UserResult:
    """
    A complete operation for one user: search → download.
    All users share the same searcher and hdfs_client instances.
    Each user writes to an independent subdirectory to avoid file-write conflicts.

    :param global_start: global start time (moment when the first query begins)
    :param max_downloads_per_user: maximum number of files downloaded per user (prevents hanging when ngram/wildcard matches too many)
    :param download_timeout: per-file download timeout in seconds
    :param user_download_time_limit: per-user total download time limit (seconds); on timeout, stop downloading further files while keeping already-downloaded ones
    """
    user_dir = os.path.join(base_download_dir, search_mode, f"user_{user_id:03d}")
    os.makedirs(user_dir, exist_ok=True)

    t_start = time.time()

    # ── Phase 1: Search ──
    t_search_start = time.time()
    try:
        search_results = searcher.search_by_filename(query, search_mode)
    except Exception as e:
        t_end = time.time()
        return UserResult(
            user_id=user_id, search_mode=search_mode, query=query,
            matched_files=0, downloaded_files=0,
            search_time_sec=round(time.time() - t_search_start, 4),
            download_time_sec=round(t_end - global_start, 4),
            total_bytes=0, success=False, error=f"搜索异常: {e}",
            search_end_abs=time.time(), task_end_abs=t_end
        )
    t_search_end = time.time()
    search_time = t_search_end - t_search_start

    matched_files = len(search_results) if search_results else 0
    if not search_results:
        t_end = time.time()
        return UserResult(
            user_id=user_id, search_mode=search_mode, query=query,
            matched_files=0, downloaded_files=0,
            search_time_sec=round(search_time, 4),
            download_time_sec=round(t_end - global_start, 4),
            total_bytes=0, success=True, error="未找到匹配文件",
            search_end_abs=t_search_end, task_end_abs=t_end
        )

    # Limit per-user download count to prevent ngram/wildcard from matching too many files and exhausting the HDFS connection pool
    download_candidates = search_results[:max_downloads_per_user]
    if matched_files > max_downloads_per_user:
        print(f"   ⚠️ 用户 {user_id}: ngram 命中 {matched_files} 个文件，"
              f"截断为前 {max_downloads_per_user} 个进行下载")

    # ── Phase 2: Download one by one (using shared hdfs_client) ──
    t_download_start = time.time()
    downloaded_count = 0
    total_bytes = 0
    download_errors = []

    print(f"   📥 用户 {user_id} 开始下载 {len(download_candidates)} 个文件...")

    for idx, result in enumerate(download_candidates, 1):
        # Check whether the user's total download time exceeds the limit
        if time.time() - t_download_start > user_download_time_limit:
            print(f"   ⏰ 用户 {user_id} 下载总时间超过 {user_download_time_limit}s，"
                  f"停止下载（已完成 {downloaded_count}/{len(download_candidates)}）")
            break

        block_path = result['block_info']['hdfs_path']
        file_info = result['file_info']
        original_name = file_info['original_name']
        offset = file_info['offset']
        length = file_info['length']

        # Generate a non-conflicting local path
        base_name, ext = os.path.splitext(original_name)
        local_path = os.path.join(user_dir, original_name)
        counter = 1
        while os.path.exists(local_path):
            local_path = os.path.join(user_dir, f"{base_name}_{counter}{ext}")
            counter += 1

        try:
            with hdfs_client.read(block_path, offset=offset, length=length) as reader:
                file_data = reader.read()
                if file_data:
                    with open(local_path, 'wb') as f:
                        f.write(file_data)
                    total_bytes += len(file_data)
                    downloaded_count += 1
        except Exception as e:
            download_errors.append(f"{original_name}: {e}")

    print(f"   ✅ 用户 {user_id} 下载完成: {downloaded_count}/{len(download_candidates)} 成功")
    if download_errors:
        print(f"   ⚠️  用户 {user_id} 下载失败 {len(download_errors)} 个:")
        for err in download_errors[:5]:
            print(f"      {err}")

    t_download_end = time.time()
    t_end = time.time()

    return UserResult(
        user_id=user_id,
        search_mode=search_mode,
        query=query,
        matched_files=matched_files,
        downloaded_files=downloaded_count,
        search_time_sec=round(search_time, 4),
        download_time_sec=round(t_download_end - global_start, 4),
        total_bytes=total_bytes,
        success=True,
        search_end_abs=t_search_end,
        task_end_abs=t_end,
    )


def percentile(data: List[float], p: float) -> float:
    if not data:
        return 0.0
    s = sorted(data)
    k = (len(s) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(s) - 1)
    return s[f] + (k - f) * (s[c] - s[f])


def print_summary(results: List[UserResult], num_users: int,
                  search_mode: str,
                  search_duration: float,
                  download_duration: float):
    """Print benchmark summary report"""
    search_times = [r.search_time_sec for r in results if r.search_time_sec > 0]
    download_times = [r.download_time_sec for r in results if r.download_time_sec > 0]
    total_downloaded = sum(r.downloaded_files for r in results)
    total_bytes = sum(r.total_bytes for r in results)
    success_count = sum(1 for r in results if r.success)
    fail_count = len(results) - success_count
    total_matched = sum(r.matched_files for r in results)

    print("\n" + "=" * 90)
    print(f"  并发基准测试报告 — {num_users} 用户共享同一客户端 | 搜索模式: {search_mode}")
    print("=" * 90)

    print(f"\n【整体指标】")
    print(f"  并发用户数:          {num_users}")
    print(f"  搜索阶段持续时间:    {search_duration:.2f} 秒 (首查询开始 → 末查询结束)")
    print(f"  下载阶段持续时间:    {download_duration:.2f} 秒 (首查询开始 → 末下载完成)")
    print(f"  成功用户数:          {success_count}")
    print(f"  失败用户数:          {fail_count}")
    print(f"  搜索命中文件总数:    {total_matched}")
    print(f"  成功下载文件总数:    {total_downloaded}")
    print(f"  下载数据总量:        {total_bytes / (1024 * 1024):.2f} MiB")

    print(f"\n【吞吐量】")
    if search_duration > 0 and download_duration > 0:
        search_throughput = num_users / search_duration
        download_throughput = num_users / download_duration
        byte_throughput = (total_bytes / (1024 * 1024)) / download_duration
        avg_files_per_user = total_downloaded / success_count if success_count > 0 else 0
        print(f"  搜索吞吐量:          {search_throughput:.2f} 用户/秒  ({num_users} 用户 / {search_duration:.2f}s)")
        print(f"  下载吞吐量:          {download_throughput:.2f} 用户/秒  ({num_users} 用户 / {download_duration:.2f}s)")
        print(f"  数据传输速率:        {byte_throughput:.2f} MiB/s")
        print(f"  每用户平均下载文件数: {avg_files_per_user:.2f} 个")
    else:
        print(f"  (耗时为 0，无法计算)")

    for label, times in [("搜索阶段", search_times),
                         ("下载阶段", download_times)]:
        print(f"\n【响应时间 — {label} (秒)】")
        if times:
            print(f"  最小: {min(times):.4f}  最大: {max(times):.4f}  "
                  f"平均: {statistics.mean(times):.4f}")
            print(f"  P50:  {percentile(times, 50):.4f}  "
                  f"P90: {percentile(times, 90):.4f}  "
                  f"P99: {percentile(times, 99):.4f}")
        else:
            print(f"  (无数据)")

    # Per-user details
    print(f"\n【用户明细（按下载响应时间排序）】")
    sorted_results = sorted(results, key=lambda r: r.download_time_sec)
    print(f"  {'用户':>6} {'查询':<25} {'命中':>5} {'下载':>5} "
          f"{'搜索(s)':>9} {'下载(s)':>9} {'大小(KiB)':>10}")
    print(f"  {'-' * 75}")
    for r in sorted_results:
        print(f"  {r.user_id:>6} {r.query:<25} {r.matched_files:>5} "
              f"{r.downloaded_files:>5} {r.search_time_sec:>9.4f} "
              f"{r.download_time_sec:>9.4f} "
              f"{r.total_bytes / 1024:>10.1f}")

    print("=" * 90)

    error_results = [r for r in results if not r.success]
    if error_results:
        print(f"\n⚠️ {len(error_results)} 个用户任务失败:")
        for r in error_results:
            print(f"  用户 {r.user_id}: {r.error}")



def run_benchmark(
    queries: List[str],
    search_mode: str,
    searcher: FileSearcher,
    hdfs_client: InsecureClient,
    num_users: int = 3,
    download_dir: str = "./benchmark_downloads",
):
    """
    Start num_users concurrent threads; all threads share the same searcher and hdfs_client,
    simulating multiple users issuing search+download from the same client.
    """
    mode_dir = os.path.join(download_dir, search_mode)
    if os.path.exists(mode_dir):
        shutil.rmtree(mode_dir)
    os.makedirs(mode_dir, exist_ok=True)

    # Assign query terms to each user (cycled)
    tasks = [(i, queries[i % len(queries)]) for i in range(num_users)]

    print(f"\n{'=' * 60}")
    print(f"  启动测试: {num_users} 并发用户 | 搜索模式: {search_mode}")
    print(f"  查询词池: {queries}")
    print(f"{'=' * 60}\n")

    results: List[UserResult] = []
    completed = 0
    lock = threading.Lock()

    # ── Start all users simultaneously ──
    global_start = time.time()

    with ThreadPoolExecutor(max_workers=num_users) as executor:
        futures = {}
        for user_id, query in tasks:
            future = executor.submit(
                single_user_task,
                user_id=user_id,
                query=query,
                search_mode=search_mode,
                searcher=searcher,
                hdfs_client=hdfs_client,
                base_download_dir=download_dir,
                global_start=global_start,
            )
            futures[future] = user_id

        for future in as_completed(futures):
            user_id = futures[future]
            try:
                result = future.result()
                results.append(result)
                with lock:
                    completed += 1
                    status = "✅" if result.success else "❌"
                    print(f"  [{completed:>3}/{num_users}] {status} 用户 {user_id:>3} | "
                          f"命中={result.matched_files:>3} 下载={result.downloaded_files:>3} | "
                          f"下载耗时={result.download_time_sec:.2f}s")
            except Exception as e:
                with lock:
                    completed += 1
                    print(f"  [{completed:>3}/{num_users}] ❌ 用户 {user_id:>3} | 异常: {e}")
                results.append(UserResult(
                    user_id=user_id, search_mode=search_mode, query="",
                    matched_files=0, downloaded_files=0,
                    search_time_sec=0, download_time_sec=0,
                    total_bytes=0, success=False, error=str(e)
                ))

    wall_end = time.time()
    wall_clock = wall_end - global_start

    # ── Compute two independent durations ──
    # Search duration: from the first user starting the query → the last user finishing the query
    search_ends = [r.search_end_abs for r in results if r.search_end_abs > 0]
    search_duration = (max(search_ends) - global_start) if search_ends else wall_clock

    # Download duration: from the first user starting the query → the last user finishing downloads (all tasks completed)
    task_ends = [r.task_end_abs for r in results if r.task_end_abs > 0]
    download_duration = (max(task_ends) - global_start) if task_ends else wall_clock

    results.sort(key=lambda r: r.user_id)
    print_summary(results, num_users, search_mode,
                  search_duration=search_duration,
                  download_duration=download_duration)

    csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            f"benchmark_{search_mode}_{num_users}users.csv")
    # export_csv(results, search_duration, download_duration, csv_path)

    return results, search_duration, download_duration


def main():
    # ── Configuration ──
    NUM_USERS = 100
    DOWNLOAD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "benchmark_downloads")

    # ── Create [shared] client instances (shared by all users) ──
    print("正在初始化共享客户端...")
    shared_hdfs_client = InsecureClient(Config.HDFS_URL, user=Config.HDFS_USER)
    shared_searcher = FileSearcher()
    print("✅ 共享客户端初始化完成\n")

    # Query terms for each search mode (modify according to actual file names)
    query_sets = {
        "exact": ["great_pyrenees_66.jpg"],
        "ngram": ["enees_67"],
        "wildcard": ["great_pyr*_78.jpg"],
    }

    all_modes_results = {}

    for mode in ["exact", "ngram", "wildcard"]:
        queries = query_sets[mode]
        results, search_dur, download_dur = run_benchmark(
            queries=queries,
            search_mode=mode,
            searcher=shared_searcher,
            hdfs_client=shared_hdfs_client,
            num_users=NUM_USERS,
            download_dir=DOWNLOAD_DIR,
        )
        all_modes_results[mode] = (results, search_dur, download_dur)

if __name__ == "__main__":
    main()
