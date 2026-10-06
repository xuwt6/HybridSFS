# file_har_search.py
# Hadoop Archive (HAR) baseline search system (for comparison experiment):
# Directly search for small files by filename in HDFS HAR archives (HAR_HDFS_BASE_PATH uploaded by file_har_upload.py)
# without relying on Elasticsearch.
#
# Supports three search modes (fully consistent with file_search.py / file_hdfs_search.py):
#   exact    exact match (filename equals the keyword exactly)
#   ngram    substring fuzzy match (keyword appears in the filename, case-insensitive)
#   wildcard wildcard match (supports * and ?, case-insensitive)
#
# Timing and measurement basis notes (outputs three durations, each corresponding to a different comparison baseline):
#   - file_search.py      builds the ES index before querying; query duration counts only the ES query itself;
#   - file_hdfs_search.py recursively traverses HDFS directories directly; query duration counts the full traversal (cold start);
#   - This script (HAR baseline) outputs three durations:
#       1) Index load duration: download and parse the archive's _index/_masterindex (preparation phase,
#          equivalent to HarFileSystem initializing its index);
#       2) Query duration: match by filename in the in-memory index (corresponds to ES "index ready" query semantics,
#          comparable with file_search.py's query duration);
#       3) Total duration (cold start) = 1 + 2 (corresponds to the full cost of answering a query from scratch,
#          comparable with file_hdfs_search.py's full traversal duration).
#     For comparison with pure HDFS, use "total duration (cold start)": both sides measure the full cost of
#     "finding a file from scratch", so the measurement basis aligns; for comparison with ES, use "query duration": both sides have resident indexes.
#
# Time statistics:
#   - File time: the local file modification time written into the HAR index at archiving time
#     (file_har_upload.py writes os.path.getmtime into the encoded attribute string);
#   - Archive creation time: the creation time of the HAR archive (block); prefer the HDFS archive directory's
#     modificationTime; if unavailable, parse it from the archive name HAR_<epoch_seconds>_<hex>.har.
#
# Index parsing format (consistent with Hadoop 3.3.1 HarFileSystem / HadoopArchives,
# see the header comments in file_har_upload.py for details):
#   File line: URLEncode(path within the archive) file part-N start offset length encoded attributes
#   Directory line: URLEncode(path within the archive) dir encoded attributes 0 0 children (each URLEncoded)
#   Encoded attributes = URLEncode("modification_time_ms permissions_short URLEncode(owner) URLEncode(group)")
#   Note: encoding uses Java URLEncoder semantics (space encoded as '+'); therefore decoding must use
#   URLDecoder semantics (only Python's unquote_plus can restore '+'); otherwise the
#   modification time field in the attribute string cannot be split out.
import os
import re
import sys
import time
import fnmatch
import urllib.parse
from datetime import datetime

# Reuse configuration from the parent directory smallfiles_heuristic to ensure "all configurations are identical"
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from hdfs import InsecureClient
from config import Config

# ================= HAR Baseline Configuration =================
# HAR baseline HDFS root directory: consistent with file_har_upload.py, isolated from other baselines
HAR_HDFS_BASE_PATH = Config.HDFS_BASE_PATH + "_har"


def parse_har_index(index_bytes):
    """
    Parse HAR _index contents; return (file entry list, total bytes of part data).
    Each file entry is a dict: {har_path, name, part, start_pos, length, mtime_ms}
      - har_path:  path within the archive after URLEncode decoding (e.g. /cat/xxx.jpg)
      - name:      filename (used for filename-based matching)
      - part:      part filename where the data resides (e.g. part-0)
      - start_pos: start offset within the part file (bytes)
      - length:    file length (bytes)
      - mtime_ms:  modification time (milliseconds, from the encoded attribute string)
    Directory lines (dir) do not produce file entries and are skipped.
    """
    entries = []
    parts_total_bytes = 0
    for raw_line in index_bytes.decode('utf-8').splitlines():
        line = raw_line.strip()
        if not line:
            continue
        fields = line.split(' ')
        # A file line contains at least: path file part-N offset length encoded attributes
        if len(fields) < 6 or fields[1] != 'file':
            continue
        # Path within the archive: Java URLEncoder encoded (space → '+'); must be decoded with unquote_plus
        har_path = urllib.parse.unquote_plus(fields[0])
        part_name = fields[2]
        try:
            start_pos = int(fields[3])
            length = int(fields[4])
        except ValueError:
            continue

        # Encoded attributes → decode once to get "modification_time_ms permissions URLEncode(owner) URLEncode(group)"
        # Key fix: the uploader uses Java URLEncoder which encodes space as '+'; must use unquote_plus
        # (unquote does not restore '+' to space; otherwise the time field cannot be split and parsing fails).
        mtime_ms = 0
        try:
            props = urllib.parse.unquote_plus(fields[5])
            mtime_ms = int(props.split(' ')[0])
        except Exception:
            pass

        name = har_path.rsplit('/', 1)[-1]
        entries.append({
            "har_path": har_path,
            "name": name,
            "part": part_name,
            "start_pos": start_pos,
            "length": length,
            "mtime_ms": mtime_ms,
        })
        parts_total_bytes += length
    return entries, parts_total_bytes


class FileHarSearcher:
    def __init__(self):
        """Initialize the HDFS client"""
        self.client = InsecureClient(Config.HDFS_URL, user=Config.HDFS_USER)
        # Verify the connection
        try:
            self.client.status("/")
        except Exception as e:
            raise ValueError(f"❌ 无法连接到 HDFS，请检查服务是否启动: {e}")

        # Archive index cache: {archive path: parsed result}, to avoid repeatedly downloading _index within the same session
        self._index_cache = {}

    def _match_filename(self, filename, target_name, search_mode):
        """
        Determine whether the filename matches based on the search mode (logic consistent with file_hdfs_search.py)
        :param filename: filename within the HAR archive
        :param target_name: search keyword
        :param search_mode: exact / ngram / wildcard
        :return: bool
        """
        if search_mode == "exact":
            return filename == target_name
        elif search_mode == "ngram":
            return target_name.lower() in filename.lower()
        elif search_mode == "wildcard":
            return fnmatch.fnmatch(filename.lower(), target_name.lower())
        else:
            raise ValueError(f"❌ 不支持的搜索模式: {search_mode}")

    def _find_har_archives(self, hdfs_path):
        """List all HAR archive directories (ending with .har) under the search directory"""
        archives = []
        try:
            for root, dirs, files in self.client.walk(hdfs_path):
                for d in dirs:
                    if d.endswith('.har'):
                        archives.append(f"{root}/{d}")
        except Exception as e:
            print(f"❌ 遍历 HDFS 目录失败: {e}")
        return archives

    def _load_archive_index(self, archive_path):
        """
        Download and parse a HAR archive's _index/_masterindex (with caching).
        Returns: {
            "version":          HAR version number (from the first line of _masterindex; 0 on parse failure),
            "file_entries":     list of file entries (see parse_har_index),
            "part_names":       deduplicated list of part filenames,
            "total_data_bytes": sum of lengths of all file entries (i.e. the data volume carried by the archive),
            "file_count":       number of file entries in the archive,
        }
        """
        cached = self._index_cache.get(archive_path)
        if cached is not None:
            return cached

        info = {
            "version": 0,
            "file_entries": [],
            "part_names": [],
            "total_data_bytes": 0,
            "file_count": 0,
        }
        try:
            with self.client.read(f"{archive_path}/_index") as reader:
                index_bytes = reader.read()
        except Exception as e:
            print(f"  ⚠️ 读取归档索引失败: {archive_path}/_index, {e}")
            self._index_cache[archive_path] = info
            return info

        # Parse the version number from the first line of _masterindex (failure does not affect file retrieval)
        try:
            with self.client.read(f"{archive_path}/_masterindex") as reader:
                master_bytes = reader.read()
            version_line = master_bytes.decode('utf-8').splitlines()[0].strip()
            info["version"] = int(version_line.split(' ')[0])
        except Exception:
            info["version"] = 0

        file_entries, total_data_bytes = parse_har_index(index_bytes)
        info["file_entries"] = file_entries
        info["part_names"] = sorted({e["part"] for e in file_entries})
        info["total_data_bytes"] = total_data_bytes
        info["file_count"] = len(file_entries)

        self._index_cache[archive_path] = info
        return info

    def _parse_archive_created_time(self, archive_path):
        """
        Get the creation time (millisecond timestamp) of a HAR archive (block), i.e. "the time the block was created".
        Prefer parsing from the archive name HAR_<epoch_seconds>_<hex>.har:
          file_har_upload.py names archives as f"HAR_{int(time.time())}_{uuid...}.har",
          the epoch seconds in the name represent the upload/creation time of the archive (block), precise with no extra RPC.
        Fall back to the HDFS archive directory's modificationTime on parse failure.
        :return: (created_time_ms, source)  source ∈ {"name", "hdfs", None}
        """
        base = archive_path.rstrip('/').rsplit('/', 1)[-1]
        m = re.match(r"HAR_(\d+)_", base)
        if m:
            try:
                return int(m.group(1)) * 1000, "name"
            except ValueError:
                pass
        try:
            st = self.client.status(archive_path)
            return st.get('modificationTime', 0), "hdfs"
        except Exception:
            return 0, None

    def search_by_filename(self, target_name, search_mode="exact", hdfs_path=None):
        """
        Search for small files by filename in HDFS HAR archives
        :param target_name: file name or keyword to search for
        :param search_mode: search mode
            - "exact":    exact match (filename equals target_name exactly)
            - "ngram":    substring fuzzy match (target_name appears in the filename)
            - "wildcard": wildcard match (supports * and ?)
        :param hdfs_path: HDFS root directory to search; defaults to HAR_HDFS_BASE_PATH
        :return: list of matched results
        """
        if hdfs_path is None:
            hdfs_path = HAR_HDFS_BASE_PATH

        results = []

        try:
            # ---- Total duration (cold start) start point: corresponds to file_hdfs_search.py's full traversal duration,
            #      i.e. the full cost of "answering a query from scratch", used for comparison with the pure HDFS baseline ----
            total_start = time.time()

            # ---- Preparation phase: list archives and load indexes (analogous to ES index being ready, timed separately) ----
            prep_start = time.time()
            archives = self._find_har_archives(hdfs_path)
            archive_infos = []
            for archive_path in archives:
                archive_infos.append((archive_path, self._load_archive_index(archive_path)))
            prep_elapsed_ms = round((time.time() - prep_start) * 1000, 2)

            total_indexed_files = sum(info["file_count"] for _, info in archive_infos)
            print(f"\n📦 已扫描 {len(archive_infos)} 个 HAR 归档，"
                  f"索引文件条目共 {total_indexed_files} 个，"
                  f"索引加载耗时: {prep_elapsed_ms} ms")

            # ---- Query phase: match in the in-memory index (timing approach consistent with file_search.py / file_hdfs_search.py) ----
            start_time = time.time()

            for archive_path, info in archive_infos:
                for entry in info["file_entries"]:
                    if self._match_filename(entry["name"], target_name, search_mode):
                        mtime_ms = entry["mtime_ms"]
                        created_ms, created_src = self._parse_archive_created_time(archive_path)
                        results.append({
                            "file_info": {
                                "original_name": entry["name"],
                                "har_path": archive_path,
                                "path_in_archive": entry["har_path"],
                                "file_size_bytes": entry["length"],
                                "file_size_mb": round(entry["length"] / 1024 / 1024, 4),
                                # File time: the local file modification time written into the index at archiving time
                                "timestamp": mtime_ms,
                            },
                            "block_info": {
                                "har_version": info["version"],
                                "part_name": entry["part"],
                                "offset": entry["start_pos"],
                                "total_size_mb": round(info["total_data_bytes"] / 1024 / 1024, 2),
                                "total_files": info["file_count"],
                                "part_count": len(info["part_names"]),
                                # Block creation time: the creation time of the HAR archive (block) in milliseconds
                                "created_time": created_ms,
                                "created_source": created_src,
                            }
                        })

            end_time = time.time()
            elapsed_ms = round((end_time - start_time) * 1000, 2)
            total_elapsed_ms = round((end_time - total_start) * 1000, 2)
            print(f"\n⏱️ 查询耗时: {elapsed_ms} ms")
            print(f"⏱️ 总耗时（冷启动，含索引加载）: {total_elapsed_ms} ms "
                  f"  ← 与 file_hdfs_search.py 的查询耗时同口径")

            return results

        except Exception as e:
            print(f"❌ 查询发生错误: {e}")
            return []


if __name__ == "__main__":
    searcher = FileHarSearcher()

    # 1. Get user input
    file_to_search = input("请输入要查询的文件名: ").strip()
    if not file_to_search:
        print("⚠️ 文件名不能为空。")
    else:
        # 2. Select search mode
        print("请选择搜索模式:")
        print("  1. exact    (精确匹配)")
        print("  2. ngram    (模糊搜索)")
        print("  3. wildcard (通配符搜索，如 2007_*.jpg)")
        mode_choice = input("请输入选项 (1/2/3，默认为1): ").strip()

        mode_map = {"1": "exact", "2": "ngram", "3": "wildcard"}
        search_mode = mode_map.get(mode_choice, "exact")

        print(f"\n🔍 正在以 [{search_mode}] 模式在 HAR 归档中搜索文件: {file_to_search}")
        print(f"   搜索目录: {HAR_HDFS_BASE_PATH}")

        # 3. Execute search and format output
        results = searcher.search_by_filename(file_to_search, search_mode)

        if results:
            print(f"\n✅ 找到 {len(results)} 个匹配的文件:\n")
            for i, result in enumerate(results, 1):
                file_info = result['file_info']
                block_info = result['block_info']
                print(f"--- 匹配文件 {i} ---")
                print(f"  文件名:   {file_info['original_name']}")
                print(f"  所在归档: {file_info['har_path']}")
                print(f"  归档内路径: {file_info['path_in_archive']}")
                print(f"  文件大小: {file_info['file_size_mb']} MiB ({file_info['file_size_bytes']} 字节)")
                print(f"  数据分片: {block_info['part_name']} (偏移 {block_info['offset']} 字节)")
                print(f"  归档统计: 共 {block_info['total_files']} 个文件、"
                      f"{block_info['total_size_mb']} MiB、part 文件 {block_info['part_count']} 个")

                # Timestamp handling consistent with file_hdfs_search.py: millisecond timestamps in HDFS/index
                timestamp_ms = file_info.get('timestamp', 0)
                if timestamp_ms:
                    upload_time_str = datetime.fromtimestamp(timestamp_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')
                else:
                    upload_time_str = '未获取到时间'
                print(f"  文件时间: {upload_time_str} (归档时记录的源文件修改时间)")

                # Block (HAR archive) creation time
                created_ms = block_info.get('created_time', 0)
                if created_ms:
                    created_str = datetime.fromtimestamp(created_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')
                else:
                    created_str = '未获取到时间'
                src = block_info.get('created_source')
                src_desc = {'name': '归档名时间戳', 'hdfs': 'HDFS目录时间'}.get(src, '未知')
                print(f"  块创建时间: {created_str} (来源: {src_desc})")

                print("-" * 40)
        else:
            print(f"❌ 未找到匹配 '{file_to_search}' 的文件。")
