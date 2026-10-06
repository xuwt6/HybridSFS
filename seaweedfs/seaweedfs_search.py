# seaweedfs_search.py
# Search the dataset by file name directly in the SeaweedFS Filer, without relying on Elasticsearch
# Functionality and counting basis aligned with file_hdfs_search.py:
#   - Supports three search modes: exact (exact match), ngram (substring fuzzy), wildcard
#   - Timing method consistent: start_time → query → end_time → elapsed_ms
#   - Return structure consistent: file_info (with original_name/path/size/timestamp) + block_info (chunk corresponds to HDFS block)
# SeaweedFS chunk corresponds to HDFS block: chunk_size comes from Config.SEAWEDFS_CHUNK_SIZE_MB,
# chunk_count prefers the real chunk count from metadata, falling back to ceil(file_size / chunk_size) (consistent with the HDFS algorithm).
import os
import sys
import time
import fnmatch
from datetime import datetime

# Reuse the configuration from the parent directory smallfiles_heuristic
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from config import Config
from seaweedfs_client import SeaweedFSFilerClient


class FileSeaweedFSSearcher:
    def __init__(self):
        """Initialize SeaweedFS Filer client"""
        self.client = SeaweedFSFilerClient()
        # Verify connection (raise ValueError when unreachable, consistent with HDFS scripts)
        self.client.health_check()
        self.chunk_size_bytes = Config.SEAWEDFS_CHUNK_SIZE_MB * 1024 * 1024

    def _match_filename(self, filename, target_name, search_mode):
        """
        Determine whether the file name matches according to the search mode (fully consistent with file_hdfs_search.py)
        :param filename: file name in SeaweedFS
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

    def search_by_filename(self, target_name, search_mode="exact", filer_path=None):
        """
        Recursively search files directly in the SeaweedFS directory (consistent with file_hdfs_search.py)
        :param target_name: file name or keyword to search for
        :param search_mode: search mode
            - "exact":    exact match (filename equals target_name exactly)
            - "ngram":     substring fuzzy match (target_name appears in the file name)
            - "wildcard":  wildcard match (supports * and ?)
        :param filer_path: SeaweedFS root directory to search, defaults to Config.SEAWEDFS_BASE_PATH
        :return: list of matched results
        """
        if filer_path is None:
            filer_path = Config.SEAWEDFS_BASE_PATH

        results = []

        try:
            # Timing approach consistent with file_hdfs_search.py
            start_time = time.time()

            # Recursively traverse the SeaweedFS directory
            for root, dirs, files in self.client.walk(filer_path):
                for file_name in files:
                    if self._match_filename(file_name, target_name, search_mode):
                        file_path = f"{root.rstrip('/')}/{file_name}"
                        try:
                            meta = self.client.stat(file_path)
                            if meta is None:
                                print(f"  ⚠️ 读取文件状态失败: {file_path}")
                                continue
                            file_size = meta["file_size"]
                            block_size = self.chunk_size_bytes
                            modification_time = meta["mtime_ms"]
                            # chunk_count: prefer the real chunk count, fall back to the same ceil algorithm as HDFS
                            block_count = meta["chunk_count"]
                            if block_count <= 0 and block_size > 0:
                                block_count = (file_size + block_size - 1) // block_size

                            results.append({
                                "file_info": {
                                    "original_name": file_name,
                                    "hdfs_path": file_path,   # Key names kept consistent with the HDFS version for downstream reuse
                                    "seaweedfs_path": file_path,
                                    "file_size_bytes": file_size,
                                    "file_size_mb": round(file_size / 1024 / 1024, 4),
                                    "timestamp": modification_time,
                                },
                                "block_info": {
                                    "block_size_bytes": block_size,
                                    "block_size_mb": round(block_size / 1024 / 1024, 2),
                                    "block_count": block_count,
                                }
                            })
                        except Exception as e:
                            print(f"  ⚠️ 读取文件状态失败: {file_path}, {e}")

            end_time = time.time()
            elapsed_ms = round((end_time - start_time) * 1000, 2)
            print(f"\n⏱️ 查询耗时: {elapsed_ms} ms")

            return results

        except Exception as e:
            print(f"❌ 查询发生错误: {e}")
            return []


if __name__ == "__main__":
    searcher = FileSeaweedFSSearcher()

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

        print(f"\n🔍 正在以 [{search_mode}] 模式在 SeaweedFS 中搜索文件: {file_to_search}")
        print(f"   搜索目录: {Config.SEAWEDFS_BASE_PATH}")

        # 3. Perform the search
        results = searcher.search_by_filename(file_to_search, search_mode)

        if results:
            print(f"\n✅ 找到 {len(results)} 个匹配的文件:\n")
            for i, result in enumerate(results, 1):
                file_info = result['file_info']
                block_info = result['block_info']
                print(f"--- 匹配文件 {i} ---")
                print(f"  文件名:        {file_info['original_name']}")
                print(f"  SeaweedFS路径: {file_info['seaweedfs_path']}")
                print(f"  文件大小:      {file_info['file_size_mb']} MiB ({file_info['file_size_bytes']} 字节)")
                print(f"  块大小:        {block_info['block_size_mb']} MiB")
                print(f"  块数量:        {block_info['block_count']}")

                # Safely get the timestamp field (milliseconds, consistent with HDFS) from file_info
                timestamp_ms = file_info.get('timestamp', 0)
                if timestamp_ms:
                    upload_time_str = datetime.fromtimestamp(timestamp_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')
                else:
                    upload_time_str = '未获取到时间'

                print(f"  上传时间:      {upload_time_str}")
                print("-" * 40)
        else:
            print(f"❌ 未找到匹配 '{file_to_search}' 的文件。")
