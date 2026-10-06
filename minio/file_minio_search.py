# file_minio_search.py
# Search files in MinIO directly by file name, without relying on Elasticsearch
# Supports three search modes: exact (exact match), ngram (substring fuzzy match), and wildcard (wildcard match)
# Timing approach consistent with file_hdfs_search.py: start_time → query → end_time → elapsed_ms

import os
import sys
import time
import fnmatch
from datetime import datetime, timezone

# Reuse the configuration from the parent directory smallfiles_heuristic
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from minio import Minio

# ============ MinIO connection configuration (consistent with file_minio_upload.py) ============
MINIO_ENDPOINT = "192.168.31.111:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_SECURE = False
MINIO_BUCKET = "small-files-test"
MINIO_BASE_PREFIX = "raw_small_files"
# Trash prefix: soft-deleted files are moved here; search results must exclude them (consistent with file_minio_delete.py / file_minio_update.py)
MINIO_TRASH_PREFIX = "raw_small_files_trash"
# =====================================================================


class FileMinioSearcher:
    def __init__(self):
        """Initialize MinIO client"""
        self.client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE,
        )
        # Verify connection: check whether the bucket exists
        try:
            if not self.client.bucket_exists(MINIO_BUCKET):
                raise ValueError(f"存储桶 {MINIO_BUCKET} 不存在，请先创建。")
        except Exception as e:
            raise ValueError(f"无法连接到 MinIO，请检查服务是否启动: {e}")

    def _match_filename(self, filename, target_name, search_mode):
        """
        Determine whether the file name matches according to the search mode
        :param filename: file name within the MinIO object
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
            raise ValueError(f"不支持的搜索模式: {search_mode}")

    def search_by_filename(self, target_name, search_mode="exact", prefix=None):
        """
        Recursively search files in the MinIO bucket
        :param target_name: file name or keyword to search for
        :param search_mode: search mode
            - "exact":    exact match (filename equals target_name exactly)
            - "ngram":     substring fuzzy match (target_name appears in the file name)
            - "wildcard":  wildcard match (supports * and ?)
        :param prefix: object prefix to search, defaults to MINIO_BASE_PREFIX
        :return: list of matched results
        """
        if prefix is None:
            prefix = MINIO_BASE_PREFIX

        # Critical fix: MinIO's list_objects(prefix=...) performs pure string prefix matching, not directory-boundary matching.
        # The trash prefix raw_small_files_trash starts with the search prefix raw_small_files and would be listed together,
        # causing old files soft-deleted into trash after update to be mistakenly matched as search results (i.e. the root cause of "found 2 files with the same name").
        # Append a trailing slash to the prefix to enforce directory-boundary matching: raw_small_files/ no longer matches raw_small_files_trash/...
        prefix = prefix.rstrip("/") + "/"

        results = []

        try:
            # Timing approach consistent with file_hdfs_search.py
            start_time = time.time()

            # Recursively list all objects in the MinIO bucket
            objects = self.client.list_objects(
                MINIO_BUCKET,
                prefix=prefix,
                recursive=True,
            )

            for obj in objects:
                # Skip directory marker objects
                if obj.object_name.endswith("/"):
                    continue

                # Double safety: even if prefix matching misses some, explicitly exclude objects in the trash,
                # ensuring old files soft-deleted into trash never appear in search results
                if obj.object_name.startswith(MINIO_TRASH_PREFIX + "/"):
                    continue

                # Extract file name (last segment of the object path)
                file_name = obj.object_name.rsplit("/", 1)[-1]

                if self._match_filename(file_name, target_name, search_mode):
                    try:
                        stat = self.client.stat_object(
                            MINIO_BUCKET,
                            obj.object_name,
                        )
                        file_size = stat.size
                        last_modified = stat.last_modified

                        # Calculate upload timestamp (millisecond precision, aligned with HDFS format)
                        if last_modified:
                            timestamp_ms = int(last_modified.timestamp() * 1000)
                            upload_time_str = last_modified.astimezone().strftime("%Y-%m-%d %H:%M:%S")
                        else:
                            timestamp_ms = 0
                            upload_time_str = "未获取到时间"

                        results.append({
                            "file_info": {
                                "original_name": file_name,
                                "object_path": obj.object_name,
                                "file_size_bytes": file_size,
                                "file_size_mb": round(file_size / 1024 / 1024, 4),
                                "timestamp": timestamp_ms,
                                "upload_time": upload_time_str,
                                "etag": stat.etag,
                                "content_type": stat.content_type,
                            }
                        })
                    except Exception as e:
                        print(f"  读取对象状态失败: {obj.object_name}, {e}")

            end_time = time.time()
            elapsed_ms = round((end_time - start_time) * 1000, 2)
            print(f"\n查询耗时: {elapsed_ms} ms")

            return results

        except Exception as e:
            print(f"查询发生错误: {e}")
            return []


if __name__ == "__main__":
    searcher = FileMinioSearcher()

    # 1. Get user input
    file_to_search = input("请输入要查询的文件名: ").strip()
    if not file_to_search:
        print("文件名不能为空。")
    else:
        # 2. Select search mode
        print("请选择搜索模式:")
        print("  1. exact    (精确匹配)")
        print("  2. ngram    (模糊搜索)")
        print("  3. wildcard (通配符搜索，如 2007_*.jpg)")
        mode_choice = input("请输入选项 (1/2/3，默认为1): ").strip()

        mode_map = {"1": "exact", "2": "ngram", "3": "wildcard"}
        search_mode = mode_map.get(mode_choice, "exact")

        print(f"\n正在以 [{search_mode}] 模式在 MinIO 中搜索文件: {file_to_search}")
        print(f"   存储桶: {MINIO_BUCKET}")
        print(f"   搜索前缀: {MINIO_BASE_PREFIX}/  (按目录边界匹配，已排除回收站 {MINIO_TRASH_PREFIX}/)")

        # 3. Perform the search
        results = searcher.search_by_filename(file_to_search, search_mode)

        if results:
            print(f"\n找到 {len(results)} 个匹配的文件:\n")
            for i, result in enumerate(results, 1):
                file_info = result["file_info"]
                print(f"--- 匹配文件 {i} ---")
                print(f"  文件名:       {file_info['original_name']}")
                print(f"  对象路径:     {file_info['object_path']}")
                print(f"  文件大小:     {file_info['file_size_mb']} MiB ({file_info['file_size_bytes']} 字节)")
                print(f"  上传时间:     {file_info['upload_time']}")
                print(f"  ETag:         {file_info['etag']}")
                print(f"  Content-Type: {file_info['content_type']}")
                print("-" * 40)
        else:
            print(f"未找到匹配 '{file_to_search}' 的文件。")
