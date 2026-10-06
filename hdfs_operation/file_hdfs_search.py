# file_hdfs_search.py
# Search the pet dataset in HDFS directly by file name, without depending on Elasticsearch
# Supports three search modes: exact (exact match), ngram (substring fuzzy match), and wildcard (wildcard match)
# The timing method is identical to file_search.py: start_time -> query -> end_time -> elapsed_ms
import os
import sys
import time
import fnmatch
from datetime import datetime

# Reuse the configuration from the parent directory smallfiles_heuristic
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from hdfs import InsecureClient
from config import Config


class FileHDFSSearcher:
    def __init__(self):
        """Initialize the HDFS client"""
        self.client = InsecureClient(Config.HDFS_URL, user=Config.HDFS_USER)
        # Verify the connection
        try:
            self.client.status("/")
        except Exception as e:
            raise ValueError(f"❌ 无法连接到 HDFS，请检查服务是否启动: {e}")

    def _match_filename(self, filename, target_name, search_mode):
        """
        Determine whether the file name matches according to the search mode
        :param filename: the file name in HDFS
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

    def search_by_filename(self, target_name, search_mode="exact", hdfs_path=None):
        """
        Search files recursively directly in the HDFS directory
        :param target_name: file name or keyword to search for
        :param search_mode: search mode
            - "exact":    exact match (filename equals target_name exactly)
            - "ngram":     substring fuzzy match (target_name appears in the file name)
            - "wildcard":  wildcard match (supports * and ?)
        :param hdfs_path: the HDFS root directory to search, defaults to Config.HDFS_BASE_PATH
        :return: list of matched results
        """
        if hdfs_path is None:
            hdfs_path = Config.HDFS_BASE_PATH

        results = []

        try:
            # The timing method is identical to file_search.py
            start_time = time.time()

            # Recursively traverse the HDFS directory
            for root, dirs, files in self.client.walk(hdfs_path):
                for file_name in files:
                    if self._match_filename(file_name, target_name, search_mode):
                        file_path = f"{root}/{file_name}"
                        try:
                            status = self.client.status(file_path)
                            file_size = status['length']
                            block_size = status.get('blockSize', 0)
                            modification_time = status.get('modificationTime', 0)

                            results.append({
                                "file_info": {
                                    "original_name": file_name,
                                    "hdfs_path": file_path,
                                    "file_size_bytes": file_size,
                                    "file_size_mb": round(file_size / 1024 / 1024, 4),
                                    "timestamp": modification_time,
                                },
                                "block_info": {
                                    "block_size_bytes": block_size,
                                    "block_size_mb": round(block_size / 1024 / 1024, 2),
                                    "block_count": (file_size + block_size - 1) // block_size if block_size > 0 else 0,
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
    searcher = FileHDFSSearcher()

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

        print(f"\n🔍 正在以 [{search_mode}] 模式在 HDFS 中搜索文件: {file_to_search}")
        print(f"   搜索目录: {Config.HDFS_BASE_PATH}")

        # 3. Perform the search
        results = searcher.search_by_filename(file_to_search, search_mode)

        if results:
            print(f"\n✅ 找到 {len(results)} 个匹配的文件:\n")
            for i, result in enumerate(results, 1):
                file_info = result['file_info']
                block_info = result['block_info']
                print(f"--- 匹配文件 {i} ---")
                print(f"  文件名:   {file_info['original_name']}")
                print(f"  HDFS路径: {file_info['hdfs_path']}")
                print(f"  文件大小: {file_info['file_size_mb']} MiB ({file_info['file_size_bytes']} 字节)")
                print(f"  块大小:   {block_info['block_size_mb']} MiB")
                print(f"  块数量:   {block_info['block_count']}")

                # 1. Safely get the timestamp field from file_info
                timestamp_ms = file_info.get('timestamp', 0)

                # 2. Check whether the timestamp exists and is valid
                if timestamp_ms:
                    # HDFS usually returns a millisecond timestamp, which must be divided by 1000 to convert it to seconds
                    # then use strftime to format it as 'year-month-day hour:minute:second'
                    upload_time_str = datetime.fromtimestamp(timestamp_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')
                else:
                    upload_time_str = '未获取到时间'

                print(f"  上传时间: {upload_time_str}")

                print("-" * 40)
        else:
            print(f"❌ 未找到匹配 '{file_to_search}' 的文件。")
