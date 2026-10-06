# count_files_hdfs.py
# Count how many files have already been uploaded to HDFS: a single traversal of the given directory aggregates file count, directory count,
# total bytes, block count, and average file size, and also reports the distribution across the first-level subdirectories under the root.
# The timing method is identical to the other scripts in the project: start_time -> traversal -> end_time -> elapsed.
#
# The default directory to count is Config.HDFS_BASE_PATH (i.e. the upload target of hdfs_upload_system.py /
# hdfs_upload_fast.py); use --hdfs-path to specify another directory.
#
# About the "- 副本" (Chinese for "copy") directories: dataset directories often contain two nearly duplicate copies, "X" and "X - 副本",
# so summing them directly overestimates the actual uploaded file volume. Adding --check-duplicates compares them exactly by relative path
# and reports the deduplicated file count; without that flag the report only notes the presence of copy directories and makes no estimate.
#
# Read-only script: performs no write, modification, or deletion.
import os
import sys
import time
import argparse
import threading
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor

# Reuse configuration from the parent directory smallfiles_heuristic to ensure "all configurations are identical"
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from hdfs import InsecureClient
from config import Config

COPY_SUFFIXES = (" - 副本", "-副本", "_副本", " 副本", " copy", " Copy", " COPY")


class BucketStat:
    """Statistics for a single first-level subdirectory."""

    def __init__(self, name):
        self.name = name
        self.file_count = 0
        self.total_bytes = 0
        self.dir_count = 0
        self.rel_paths = set()          # Populated only with --check-duplicates

    @property
    def avg_bytes(self):
        return self.total_bytes / self.file_count if self.file_count else 0


class HDFSFileCounter:
    """Recursively count the files and used space under an HDFS directory (breadth-first + multi-threaded listing)."""

    def __init__(self, workers=8, heartbeat=10000, track_duplicates=False):
        self.client = InsecureClient(Config.HDFS_URL, user=Config.HDFS_USER)
        self.workers = max(1, workers)
        self.heartbeat = heartbeat
        self.track_duplicates = track_duplicates
        self._lock = threading.Lock()

        # Global statistics
        self.file_count = 0
        self.dir_count = 0
        self.total_bytes = 0
        self.total_blocks = 0
        self.empty_file_count = 0
        self.largest_file = (0, "")      # (byte count, path)
        self.failed_dirs = []            # Directories that failed to list
        self.elapsed = 0.0

        # First-level subdirectory distribution: bucket name -> BucketStat
        self.buckets = OrderedDict()
        self._root_bucket_name = "(根目录直属文件)"
        self._dup_buckets = set()        # Buckets whose relative paths must be recorded (used for copy comparison)

        # Verify the connection
        try:
            self.client.status("/")
        except Exception as e:
            raise ValueError(f"[错误] 无法连接到 HDFS {Config.HDFS_URL}，请检查服务是否启动: {e}")

    # ---------- Internal Utilities ----------

    @staticmethod
    def _block_count(length, block_size):
        """Estimate the number of blocks a file occupies by HDFS rules (empty files occupy no block)."""
        if length <= 0 or block_size <= 0:
            return 0
        return (length + block_size - 1) // block_size

    @staticmethod
    def _strip_copy_suffix(name):
        """Strip the copy suffix and return the original directory name; return None if it is not a copy directory."""
        for suffix in COPY_SUFFIXES:
            if name.endswith(suffix):
                base = name[: -len(suffix)].strip()
                if base:
                    return base
        return None

    def _list_dir(self, item):
        """List a single directory.

        :param item: (directory path, name of the first-level subdirectory it belongs to)
        :return: (subdirectory list [(path, bucket)], file entry list [(path, status, bucket)])
        """
        path, bucket = item
        try:
            entries = self.client.list(path, status=True) or []
        except Exception as e:
            with self._lock:
                self.failed_dirs.append((path, repr(e)))
            return [], []

        sub_dirs = []
        files = []
        for name, status in entries:
            if not isinstance(status, dict):
                continue
            child = f"{path.rstrip('/')}/{name}"
            if status.get("type") == "DIRECTORY":
                sub_dirs.append((child, bucket))
                with self._lock:
                    if bucket not in self.buckets:
                        self.buckets[bucket] = BucketStat(bucket)
                    self.buckets[bucket].dir_count += 1
            else:
                files.append((child, status, bucket))

        with self._lock:
            self.dir_count += 1

        return sub_dirs, files

    def _account_file(self, path, status, bucket):
        length = status.get("length", 0) or 0
        block_size = status.get("blockSize", 0) or 0
        blocks = self._block_count(length, block_size)
        rel = None
        if bucket in self._dup_buckets:
            # The relative path must have the "root directory/bucket name" prefix stripped before comparison,
            # otherwise "3-pet/x.jpg" and "3-pet - 副本/x.jpg" would never be equal
            prefix_len = len(self._root_path) + 1 + len(bucket)
            rel = path[prefix_len:].lstrip("/")

        with self._lock:
            self.file_count += 1
            self.total_bytes += length
            self.total_blocks += blocks
            if length == 0:
                self.empty_file_count += 1
            if length > self.largest_file[0]:
                self.largest_file = (length, path)

            stat = self.buckets.get(bucket)
            if stat is None:
                stat = self.buckets[bucket] = BucketStat(bucket)
            stat.file_count += 1
            stat.total_bytes += length
            if rel is not None:
                stat.rel_paths.add(rel)

            if self.heartbeat and self.file_count % self.heartbeat == 0:
                print(f"  ...已统计 {self.file_count} 个文件（{self.total_bytes / 1073741824:.2f} GiB）")

    # ---------- Main Flow ----------

    def count(self, hdfs_path):
        """Recursively count breadth-first starting from hdfs_path."""
        hdfs_path = hdfs_path.rstrip("/") or "/"
        self._root_path = hdfs_path

        root_status = self.client.status(hdfs_path, strict=False)
        if root_status is None:
            raise ValueError(f"[错误] HDFS 路径不存在: {hdfs_path}")

        # The root path itself is a single file
        if root_status.get("type") == "FILE":
            start = time.time()
            self.buckets[self._root_bucket_name] = BucketStat(self._root_bucket_name)
            self._account_file(hdfs_path, root_status, self._root_bucket_name)
            self.elapsed = time.time() - start
            return

        # List the root directory first to determine the first-level subdirectories (buckets) and identify which ones form copy pairs
        root_entries = self.client.list(hdfs_path, status=True) or []
        top_dirs = [name for name, st in root_entries
                    if isinstance(st, dict) and st.get("type") == "DIRECTORY"]
        top_dir_set = set(top_dirs)

        if self.track_duplicates:
            for name in top_dirs:
                base = self._strip_copy_suffix(name)
                if base and base in top_dir_set:
                    self._dup_buckets.add(name)
                    self._dup_buckets.add(base)

        pending = []
        for name, st in root_entries:
            if not isinstance(st, dict):
                continue
            child = f"{hdfs_path}/{name}"
            if st.get("type") == "DIRECTORY":
                self.buckets[name] = BucketStat(name)
                pending.append((child, name))
            else:
                # Files sitting directly under the root directory form a separate category
                if self._root_bucket_name not in self.buckets:
                    self.buckets[self._root_bucket_name] = BucketStat(self._root_bucket_name)
                self._account_file(child, st, self._root_bucket_name)

        start_time = time.time()
        with ThreadPoolExecutor(max_workers=self.workers, thread_name_prefix="list") as ex:
            while pending:
                batch = pending
                pending = []
                for sub_dirs, files in ex.map(self._list_dir, batch):
                    pending.extend(sub_dirs)
                    for child_path, status, bucket in files:
                        self._account_file(child_path, status, bucket)

        self.elapsed = time.time() - start_time

    # ---------- Copy Comparison ----------

    def duplicate_report(self):
        """Compare "X" and "X - 副本" exactly by relative path and return (duplicate file count, detail list).

        Has data only when track_duplicates is enabled during count().
        Each detail item is (original directory, copy directory, present in both, only in the original, only in the copy).
        """
        details = []
        total_overlap = 0
        handled = set()

        for name in list(self.buckets):
            if name in handled:
                continue
            base = self._strip_copy_suffix(name)
            if not base or base not in self.buckets:
                continue
            # name is the copy directory, base is the original directory
            handled.add(name)
            handled.add(base)
            copy_paths = self.buckets[name].rel_paths
            base_paths = self.buckets[base].rel_paths
            if not copy_paths and not base_paths:
                continue
            both = len(base_paths & copy_paths)
            only_base = len(base_paths - copy_paths)
            only_copy = len(copy_paths - base_paths)
            total_overlap += both
            details.append((base, name, both, only_base, only_copy))

        return total_overlap, details


def fmt_size(num_bytes):
    """Format a byte count as a human-readable string."""
    if num_bytes >= 1099511627776:      # TiB
        return f"{num_bytes / 1099511627776:.2f} TiB"
    if num_bytes >= 1073741824:         # GiB
        return f"{num_bytes / 1073741824:.2f} GiB"
    if num_bytes >= 1048576:            # MiB
        return f"{num_bytes / 1048576:.2f} MiB"
    if num_bytes >= 1024:               # KiB
        return f"{num_bytes / 1024:.2f} KiB"
    return f"{num_bytes} B"


def print_breakdown(counter):
    """Print the first-level subdirectory distribution table."""
    buckets = list(counter.buckets.values())
    if not buckets:
        return
    print(f"\n各一级子目录分布（共 {len(buckets)} 个）:")
    print(f"  {'子目录':<34}{'文件数':>10}{'大小':>14}{'平均大小':>12}")
    print("  " + "-" * 70)
    for stat in sorted(buckets, key=lambda s: -s.file_count):
        name = stat.name
        display = name if len(name) <= 32 else name[:29] + "..."
        print(f"  {display:<34}{stat.file_count:>10}{fmt_size(stat.total_bytes):>14}"
              f"{fmt_size(int(stat.avg_bytes)):>12}")
    print("  " + "-" * 70)
    print(f"  {'合计':<34}{counter.file_count:>10}{fmt_size(counter.total_bytes):>14}")


def run(hdfs_path=None, workers=8, show_breakdown=True, check_duplicates=False, heartbeat=10000):
    hdfs_path = hdfs_path or Config.HDFS_BASE_PATH

    print("开始统计 HDFS 文件数量...")
    print(f"  HDFS 地址: {Config.HDFS_URL}（用户 {Config.HDFS_USER}）")
    print(f"  统计目录: {hdfs_path}")
    print(f"  并发线程: {workers}")
    if check_duplicates:
        print("  副本比对: 已开启（按相对路径精确比对）")
    print()

    counter = HDFSFileCounter(workers=workers, heartbeat=heartbeat,
                              track_duplicates=check_duplicates)
    try:
        counter.count(hdfs_path)
    except Exception as e:
        print(f"[错误] 统计失败: {e}")
        return None

    avg_bytes = (counter.total_bytes / counter.file_count) if counter.file_count else 0

    print("\n" + "=" * 60)
    print(f"HDFS 目录 {hdfs_path} 统计结果")
    print("=" * 60)
    print(f"  文件总数:     {counter.file_count}")
    print(f"  目录总数:     {counter.dir_count}")
    print(f"  总大小:       {fmt_size(counter.total_bytes)}（{counter.total_bytes} 字节）")
    print(f"  占用 block:   {counter.total_blocks}")
    print(f"  平均文件大小: {fmt_size(int(avg_bytes))}")
    if counter.empty_file_count:
        print(f"  空文件数:     {counter.empty_file_count}")
    if counter.largest_file[1]:
        size, path = counter.largest_file
        print(f"  最大文件:     {fmt_size(size)}  {path}")
    print(f"  遍历耗时:     {counter.elapsed:.2f} 秒")
    if counter.elapsed > 0:
        print(f"  遍历速率:     {counter.file_count / counter.elapsed:.1f} 文件/秒")
    if counter.failed_dirs:
        print(f"  [警告] 有 {len(counter.failed_dirs)} 个目录列举失败，结果可能偏少：")
        for path, err in counter.failed_dirs[:10]:
            print(f"    - {path}: {err}")

    # Copy situation
    copy_names = [name for name in counter.buckets
                  if counter._strip_copy_suffix(name) in counter.buckets]
    if copy_names:
        if check_duplicates:
            overlap, details = counter.duplicate_report()
            unique = counter.file_count - overlap
            print(f"\n副本目录去重（按相对路径精确比对）:")
            for base, copy, both, only_base, only_copy in details:
                print(f"  {base}  <->  {copy}")
                print(f"    两边都有: {both}    仅原始目录有: {only_base}    仅副本目录有: {only_copy}")
            print(f"  重复文件合计: {overlap}")
            print(f"  去重后文件数: {unique}（= {counter.file_count} - {overlap}）")
        else:
            copy_files = sum(counter.buckets[n].file_count for n in copy_names)
            print(f"\n  [提示] 检测到 {len(copy_names)} 个疑似副本目录，共 {copy_files} 个文件：")
            for name in sorted(copy_names):
                print(f"    - {name}（{counter.buckets[name].file_count} 个文件）")
            print("    这些文件与对应原始目录高度重复，上面的「文件总数」是包含副本的物理文件数。")
            print("    需要精确去重后的数字请加 --check-duplicates 重跑。")

    print("=" * 60)

    if show_breakdown:
        print_breakdown(counter)

    return counter


def parse_args():
    p = argparse.ArgumentParser(description="统计 HDFS 上已上传的文件数量与占用空间")
    p.add_argument("--hdfs-path", default=Config.HDFS_BASE_PATH,
                   help=f"要统计的 HDFS 目录，默认 {Config.HDFS_BASE_PATH}")
    p.add_argument("--workers", type=int, default=8, help="并发列举目录的线程数，默认 8")
    p.add_argument("--no-breakdown", action="store_true", help="不输出一级子目录分布")
    p.add_argument("--check-duplicates", action="store_true",
                   help="对 'X' 与 'X - 副本' 目录按相对路径精确比对，给出去重后文件数")
    p.add_argument("--heartbeat", type=int, default=10000,
                   help="每统计多少个文件打印一次进度，0 表示不打印，默认 10000")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(hdfs_path=args.hdfs_path,
        workers=args.workers,
        show_breakdown=not args.no_breakdown,
        check_duplicates=args.check_duplicates,
        heartbeat=args.heartbeat)
