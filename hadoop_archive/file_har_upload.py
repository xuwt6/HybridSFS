# file_har_upload.py
# Hadoop Archive (HAR) baseline upload system (for comparison experiments):
# Scan Config.LOCAL_DIR, group and pack the small files in scan order into standard HAR archives, then upload them to HDFS.
#
# The internal HAR structure is fully identical to the output of the official Hadoop HadoopArchives (the hadoop archive command, version 3):
#   <name>.har/_masterindex  master index: the first line is the version number, followed by one "startHash endHash startPos endPos" per line
#   <name>.har/_index        archive index: sorted by path hash, one line per file/directory
#   <name>.har/part-N        data files: small file contents appended sequentially
# Index format details (from the Hadoop 3.3.1 source code HarFileSystem / HadoopArchives):
#   File line: URLEncode(path within the archive) file part-N start offset length encoded attributes
#   Directory line: URLEncode(path within the archive) dir encoded attributes 0 0 children (each URLEncoded)
#   Encoded attributes = URLEncode("modification_time_ms permissions_short URLEncode(owner) URLEncode(group)")
#   hash = Java String.hashCode() & 0x7fffffff
# After the archive is complete, it can be accessed via hadoop fs as "har://<underlying file system>/<archive path>/inner path".
#
# Similarities with hdfs_upload_system.py / upload_system.py:
#   - The data source and all configuration (LOCAL_DIR, HDFS_URL, HDFS_USER, etc.) reuse the same Config
#   - Files are grouped by target size (TARGET_SIZE_BYTES) before upload, corresponding to the block-based merging approach
#   - The counting basis is identical: per-archive success logs, cumulative upload volume/total elapsed time/average rate, and final overall statistics
#   - The timing method aligns with hdfs_upload_system.py: the total elapsed time is measured only once for the entire code section,
#     the start time is recorded before the os.walk traversal, so the total elapsed time covers file traversal + index building + upload,
#     and the average rate uses the whole-flow total elapsed time as the denominator (a fair comparison with the pure HDFS baseline)
# Differences:
#   - Does not depend on Elasticsearch;
#   - The hadoop archive command must submit a MapReduce job to the cluster, whereas this script uses pure Python to generate
#     a HAR with exactly the same structure and uploads it via WebHDFS, so the comparison can run directly in this experiment environment.
import os
import sys
import time
import urllib.parse
import uuid

# Reuse configuration from the parent directory smallfiles_heuristic to ensure "all configurations are identical"
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from hdfs import InsecureClient
from config import Config

# ================= HAR Baseline Configuration =================
# HDFS root directory for the HAR baseline: isolated from the other baselines (/merged_small_files) so the comparison experiments do not interfere with each other
HAR_HDFS_BASE_PATH = Config.HDFS_BASE_PATH + "_har"
# HAR format version (identical to the official HadoopArchives.VERSION in Hadoop 3.3.1)
HAR_VERSION = 3
# The master index splits one hash range per 1000 index entries (identical to the official numIndexes)
HAR_INDEX_CHUNK = 1000
# Target size of a single part data file (official default 2 GiB; here aligned with this experiment's 128 MiB block scheme for easier comparison)
HAR_PART_SIZE_BYTES = Config.TARGET_SIZE_BYTES
# Upper limit on the total file size a single archive can hold; a new archive is started once exceeded
HAR_ARCHIVE_MAX_BYTES = Config.TARGET_SIZE_BYTES
# The short value corresponding to the default HDFS file permission 644 (octal), identical to the result of FsPermission.toShort()
HAR_DEFAULT_PERM_SHORT = 420


def java_string_hash(s):
    """
    Equivalent to Java String.hashCode() followed by & 0x7fffffff,
    i.e. the implementation of HarFileSystem.getHarHash(Path p).
    Computed over UTF-16 code units, consistent with Java string semantics.
    """
    data = s.encode('utf-16-be')
    h = 0
    for i in range(0, len(data), 2):
        unit = (data[i] << 8) | data[i + 1]
        h = (31 * h + unit) & 0xFFFFFFFF
    if h >= 0x80000000:
        h -= 0x100000000
    return h & 0x7fffffff


def java_url_encode(s):
    """
    Equivalent to Java URLEncoder.encode(s, "UTF-8"):
    Letters, digits and . - * _ stay unchanged, spaces become '+', and the remaining characters are percent-encoded as UTF-8.
    """
    encoded = urllib.parse.quote(s, safe='-*_.')
    encoded = encoded.replace('%20', '+')
    # Python's quote does not encode '~' while Java URLEncoder does; this aligns them
    encoded = encoded.replace('~', '%7E')
    return encoded


class HarIndexWriter:
    """Collect archive entries and generate the _index and _masterindex contents in the official format"""

    def __init__(self):
        self.entries = []  # Elements are (hash, index line text)

    def _encode_properties(self, mtime_ms):
        """Encode the attribute string: URLEncode("modification_time permission_short URLEncode(owner) URLEncode(group)")"""
        inner = (f"{mtime_ms} {HAR_DEFAULT_PERM_SHORT} "
                 f"{java_url_encode(Config.HDFS_USER)} {java_url_encode(Config.HDFS_USER)}")
        return java_url_encode(inner)

    def add_file_entry(self, har_path, part_name, start_pos, length, mtime_ms):
        line = (f"{java_url_encode(har_path)} file {part_name} {start_pos} "
                f"{length} {self._encode_properties(mtime_ms)} ")
        self.entries.append((java_string_hash(har_path), line))

    def add_dir_entry(self, har_path, children, mtime_ms):
        child_str = "".join(java_url_encode(c) + " " for c in children)
        line = (f"{java_url_encode(har_path)} dir {self._encode_properties(mtime_ms)} "
                f"0 0 {child_str}")
        self.entries.append((java_string_hash(har_path), line))

    def build(self):
        """
        Generate (_masterindex bytes, _index bytes).
        Entries are sorted by hash; for every HAR_INDEX_CHUNK entries written, the master index records one
        "startHash endHash startPos endPos" range (identical to the official Reducer logic).
        """
        self.entries.sort(key=lambda x: x[0])

        index_bytes = bytearray()
        master_lines = [f"{HAR_VERSION} \n"]

        written = 0
        carry_hash = 0          # Identical to the official implementation: the startHash of the first range is initialized to 0
        range_start_pos = 0
        last_hash = 0
        for h, line in self.entries:
            index_bytes += line.encode('utf-8')
            index_bytes += b"\n"
            last_hash = h
            written += 1
            if written > HAR_INDEX_CHUNK - 1:
                master_lines.append(
                    f"{carry_hash} {last_hash} {range_start_pos} {len(index_bytes)} \n")
                range_start_pos = len(index_bytes)
                carry_hash = last_hash
                written = 0
        if written > 0:
            master_lines.append(
                f"{carry_hash} {last_hash} {range_start_pos} {len(index_bytes)} \n")

        master_bytes = "".join(master_lines).encode('utf-8')
        return master_bytes, bytes(index_bytes)


class HarArchiveBuilder:
    """Build a HAR archive in memory: append file data to parts sequentially and record index entries"""

    def __init__(self, archive_name):
        self.archive_name = archive_name
        self.index_writer = HarIndexWriter()
        self.parts = [bytearray()]   # part-0, part-1, ...
        self.file_count = 0
        self.total_bytes = 0

    @property
    def part_count(self):
        return len(self.parts)

    def add_file(self, local_path, har_path, mtime_ms):
        """Read the local file and append it to the current part; start a new part when the part exceeds the target size"""
        with open(local_path, 'rb') as fh:
            data = fh.read()
        size = len(data)

        # A single file is not split: a new part is started only when the current part is non-empty and would exceed the limit
        if len(self.parts[-1]) > 0 and len(self.parts[-1]) + size > HAR_PART_SIZE_BYTES:
            self.parts.append(bytearray())

        part_name = f"part-{len(self.parts) - 1}"
        start_pos = len(self.parts[-1])
        self.parts[-1].extend(data)

        self.file_count += 1
        self.total_bytes += size
        self.index_writer.add_file_entry(har_path, part_name, start_pos, size, mtime_ms)

    def add_dir(self, har_path, children, mtime_ms):
        self.index_writer.add_dir_entry(har_path, children, mtime_ms)

    def finish(self):
        """Generate the index file contents and return (_masterindex bytes, _index bytes)"""
        return self.index_writer.build()


class FileHarUploader:
    def __init__(self):
        self.hdfs = InsecureClient(Config.HDFS_URL, user=Config.HDFS_USER)
        # Verify the connection
        try:
            self.hdfs.status("/")
        except Exception as e:
            raise ValueError(f"❌ 无法连接到 HDFS，请检查服务是否启动: {e}")
        print("✅ HDFS 客户端连接验证通过。")

        # Statistics variables
        self.total_uploaded_bytes = 0
        self.total_upload_time = 0.0
        self.archive_count = 0
        self.total_part_count = 0
        self.total_file_count = 0
        self.total_index_bytes = 0
        self.read_failed = 0

    # ================= Utility Methods =================

    @staticmethod
    def _format_elapsed(elapsed):
        hours = int(elapsed // 3600)
        minutes = int((elapsed % 3600) // 60)
        seconds = elapsed % 60
        return f"{hours}时{minutes}分{seconds:.2f}秒"

    @staticmethod
    def _build_dir_entries(dir_entries, file_har_paths):
        """
        Prune the directory tree according to the actual set of files contained in this archive:
        Keep only the non-empty directories within the archive and their children inside the archive, ensuring each archive's directory index is self-consistent.
        :param dir_entries: global directory entry list [(directory path within the archive, [child names], modification time in ms)]
        :param file_har_paths: set of file paths contained in this archive
        :return: {directory path within the archive: ([child names], modification time in ms)}
        """
        file_set = set(file_har_paths)

        def _depth(har_dir):
            # "/" has depth 0, "/a" has 1, and "/a/b" has 2 (count('/') cannot distinguish the root from a first-level directory)
            if har_dir == "/":
                return 0
            return len(har_dir.strip('/').split('/'))

        # Deeper directories are processed first so that a parent directory knows whether its subdirectories were kept
        sorted_dirs = sorted(dir_entries, key=lambda d: _depth(d[0]), reverse=True)
        kept = {}
        kept_dir_paths = set()
        for har_dir, children, mtime_ms in sorted_dirs:
            prefix = har_dir if har_dir == "/" else har_dir + "/"
            kept_children = []
            for child in children:
                child_path = prefix + child if har_dir != "/" else "/" + child
                if child_path in file_set or child_path in kept_dir_paths:
                    kept_children.append(child)
            if har_dir == "/" or kept_children:
                kept[har_dir] = (kept_children, mtime_ms)
                kept_dir_paths.add(har_dir)
        return kept

    def _upload_bytes(self, data, hdfs_path):
        with self.hdfs.write(hdfs_path, overwrite=True) as writer:
            writer.write(data)

    # ================= Scan and Group =================

    def scan_local_dir(self):
        """
        Scan Config.LOCAL_DIR and return:
        file_entries: [(local path, path within the archive, size, modification time in ms)]
        dir_entries:  [(directory path within the archive, [child names], modification time in ms)]
        """
        local_dir_abs = os.path.abspath(Config.LOCAL_DIR)
        file_entries = []
        dir_entries = []

        for root, dirs, files in os.walk(local_dir_abs):
            rel_root = os.path.relpath(root, local_dir_abs)
            har_dir = "/" if rel_root == "." else "/" + rel_root.replace(os.sep, "/")

            # Directory entries: children = immediate subdirectory names + immediate file names
            mtime_ms = int(os.path.getmtime(root) * 1000)
            dir_entries.append((har_dir, sorted(dirs + files), mtime_ms))

            for name in files:
                local_path = os.path.join(root, name)
                try:
                    size = os.path.getsize(local_path)
                    file_mtime_ms = int(os.path.getmtime(local_path) * 1000)
                except OSError as e:
                    print(f"[错误] 无法读取文件: {local_path}, {e}")
                    self.read_failed += 1
                    continue
                har_path = f"/{name}" if har_dir == "/" else f"{har_dir}/{name}"
                file_entries.append((local_path, har_path, size, file_mtime_ms))

        return file_entries, dir_entries

    @staticmethod
    def group_files(file_entries):
        """Group files into archives in scan order: start a new archive once the cumulative size exceeds HAR_ARCHIVE_MAX_BYTES"""
        groups = []
        current_group = []
        current_bytes = 0
        for entry in file_entries:
            size = entry[2]
            if current_group and current_bytes + size > HAR_ARCHIVE_MAX_BYTES:
                groups.append(current_group)
                current_group = []
                current_bytes = 0
            current_group.append(entry)
            current_bytes += size
        if current_group:
            groups.append(current_group)
        return groups

    # ================= Upload =================

    def upload_archive(self, builder, master_bytes, index_bytes, group_idx, group_total):
        """Upload a built HAR archive (_masterindex/_index/part-*) to HDFS"""
        har_dir = f"{HAR_HDFS_BASE_PATH}/{builder.archive_name}"
        upload_start = time.time()

        self._upload_bytes(master_bytes, f"{har_dir}/_masterindex")
        self._upload_bytes(index_bytes, f"{har_dir}/_index")
        for i, part in enumerate(builder.parts):
            self._upload_bytes(bytes(part), f"{har_dir}/part-{i}")

        upload_elapsed = time.time() - upload_start
        time_str = self._format_elapsed(upload_elapsed)

        # Update statistics (counting basis identical to upload_system.py)
        self.total_uploaded_bytes += builder.total_bytes
        self.total_upload_time += upload_elapsed
        self.archive_count += 1
        self.total_part_count += builder.part_count
        self.total_file_count += builder.file_count
        self.total_index_bytes += len(master_bytes) + len(index_bytes)

        rate = (self.total_uploaded_bytes / 1048576) / self.total_upload_time if self.total_upload_time > 0 else 0.0
        print(f"[成功] 归档 {group_idx}/{group_total} {builder.archive_name} 已上传，"
              f"包含 {builder.file_count} 个文件，大小 {builder.total_bytes / 1048576:.2f} MiB，"
              f"part 文件 {builder.part_count} 个，耗时 {time_str}。")
        print(f"[统计] 累计上传 {self.total_uploaded_bytes / 1048576:.2f} MiB，"
              f"纯上传合计 {self.total_upload_time:.2f} 秒，纯上传速率 {rate:.2f} MiB/秒。")

    # ================= Main Flow =================

    def run(self):
        print("=" * 60)
        print("🚀 Hadoop Archive (HAR) 基线上传系统")
        print(f"📂 本地目录: {Config.LOCAL_DIR}")
        print(f"📦 归档目标大小: {HAR_ARCHIVE_MAX_BYTES / 1048576:.0f} MiB | "
              f"part 目标大小: {HAR_PART_SIZE_BYTES / 1048576:.0f} MiB")
        print(f"🗂️ HDFS 目标目录: {HAR_HDFS_BASE_PATH}")
        print("=" * 60)

        if not os.path.isdir(Config.LOCAL_DIR):
            print(f"❌ 本地目录不存在: {Config.LOCAL_DIR}")
            return

        # The timing basis aligns with hdfs_upload_system.py: the start time is recorded before the os.walk traversal,
        # and the total elapsed time is measured only once for the whole flow (file traversal + index building + upload),
        # with the average rate using the whole-flow total elapsed time as the denominator for a fair comparison with the pure HDFS baseline
        upload_start = time.time()

        # 1. Scan the local directory (os.walk traverses the files, counted in the whole-flow elapsed time)
        scan_start = time.time()
        file_entries, dir_entries = self.scan_local_dir()
        scan_elapsed = time.time() - scan_start
        total_scan_bytes = sum(e[2] for e in file_entries)
        print(f"[扫描] 共 {len(file_entries)} 个文件、{len(dir_entries)} 个目录，"
              f"合计 {total_scan_bytes / 1048576:.2f} MiB，扫描耗时 {scan_elapsed:.2f} 秒。")

        if not file_entries:
            print("⚠️ 没有可上传的文件，退出。")
            return

        # 2. Group sequentially by target size
        groups = self.group_files(file_entries)
        print(f"[分组] 按 {HAR_ARCHIVE_MAX_BYTES / 1048576:.0f} MiB 目标大小，"
              f"共分为 {len(groups)} 个归档。")

        # 3. Ensure the HDFS baseline directory exists
        try:
            self.hdfs.makedirs(HAR_HDFS_BASE_PATH)
        except Exception as e:
            print(f"❌ 创建 HDFS 目录失败 {HAR_HDFS_BASE_PATH}: {e}")
            return

        # 4. Build and upload archive by archive (both index building and upload count toward the whole-flow total elapsed time)
        for idx, group in enumerate(groups, 1):
            archive_name = f"HAR_{int(time.time())}_{uuid.uuid4().hex[:8]}.har"
            builder = HarArchiveBuilder(archive_name)

            # Directory entries: prune the directory tree according to the files contained in this archive
            kept_dirs = self._build_dir_entries(dir_entries, [e[1] for e in group])
            for har_dir in sorted(kept_dirs):
                children, mtime_ms = kept_dirs[har_dir]
                builder.add_dir(har_dir, children, mtime_ms)

            # File entries: appended sequentially to the part data files
            for local_path, har_path, _, mtime_ms in group:
                try:
                    builder.add_file(local_path, har_path, mtime_ms)
                except Exception as e:
                    print(f"[错误] 文件写入归档失败 {archive_name}: {local_path}, {e}")
                    self.read_failed += 1

            master_bytes, index_bytes = builder.finish()
            try:
                self.upload_archive(builder, master_bytes, index_bytes, idx, len(groups))
            except Exception as e:
                print(f"[严重错误] 归档 {archive_name} 上传失败: {e}")

        total_elapsed = time.time() - upload_start

        # 5. Final statistics (counting basis identical to hdfs_upload_system.py: the average rate uses the whole-flow total elapsed time as the denominator,
        #    plus supplementary HAR comparison metrics)
        rate = (self.total_uploaded_bytes / 1048576) / total_elapsed if total_elapsed > 0 else 0.0
        objects_after = self.total_part_count + 2 * self.archive_count
        reduction = (1 - objects_after / self.total_file_count) * 100 if self.total_file_count > 0 else 0.0

        print("=" * 60)
        print(f"[总体统计] 共上传 {self.total_file_count} 个文件、"
              f"{self.total_uploaded_bytes / 1048576:.2f} MiB，"
              f"生成 {self.archive_count} 个 HAR 归档（part 数据文件 {self.total_part_count} 个、"
              f"索引文件 {2 * self.archive_count} 个），"
              f"合计 {total_elapsed:.2f} 秒，上传耗时 {self._format_elapsed(total_elapsed)}，"
              f"平均速率 {rate:.2f} MiB/秒（含遍历与索引构建；"
              f"其中遍历文件 {scan_elapsed:.2f} 秒，纯上传合计 {self.total_upload_time:.2f} 秒）。")
        print("=" * 60)


if __name__ == "__main__":
    uploader = FileHarUploader()
    uploader.run()
