# file_minio_delete.py
# Search and soft-delete files directly in MinIO, without relying on Elasticsearch
# Soft-delete method: copy the object to the trash prefix (trash), then delete the original object; recoverable
# Reuse FileMinioSearcher from file_minio_search.py for searching
# Timing approach consistent with file_hdfs_delete.py

import os
import sys
import time
import copy

# Reuse the configuration from the parent directory smallfiles_heuristic
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from minio import Minio
from minio.commonconfig import CopySource
from file_minio_search import FileMinioSearcher

# MinIO connection configuration (consistent with file_minio_upload.py / file_minio_search.py)
MINIO_ENDPOINT = "192.168.31.111:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_SECURE = False
MINIO_BUCKET = "small-files-test"
MINIO_BASE_PREFIX = "raw_small_files"

# MinIO trash prefix: placed outside the search root prefix,
# soft-deleted files will no longer be matched by search, equivalent to is_deleted=true in the ES version
MINIO_TRASH_PREFIX = "raw_small_files_trash"


class FileMinioDeleter:
    def __init__(self):
        """Initialize MinIO client and file searcher"""
        self.client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE,
        )
        # Verify the connection
        try:
            if not self.client.bucket_exists(MINIO_BUCKET):
                raise ValueError(f"存储桶 {MINIO_BUCKET} 不存在，请先创建。")
        except Exception as e:
            raise ValueError(f"无法连接到 MinIO，请检查服务是否启动: {e}")
        self.searcher = FileMinioSearcher()

    def delete_by_filename(self, target_name, delete_mode="exact"):
        """
        Soft-delete files in MinIO by file name (copy to trash prefix then delete original object; recoverable).
        Supports 3 delete modes, consistent with the search functionality:
        - exact:    exact match, delete files with exactly the same name
        - ngram:    fuzzy delete, delete files containing the keyword
        - wildcard: wildcard delete, e.g. *.log

        :param target_name:  file name or keyword to delete
        :param delete_mode:  delete mode ("exact", "ngram", "wildcard")
        """
        if delete_mode not in ("exact", "ngram", "wildcard"):
            raise ValueError(f"不支持的删除模式: {delete_mode}")

        # 1. Search using FileMinioSearcher
        print(f"\n正在以 [{delete_mode}] 模式搜索文件: {target_name}")
        results = self.searcher.search_by_filename(target_name, delete_mode)

        if not results:
            print(f"未找到匹配 '{target_name}' 的文件，无需删除。")
            return

        # 2. Display matched results for user confirmation
        print(f"\n找到 {len(results)} 个匹配的文件，准备软删除:\n")
        for i, result in enumerate(results, 1):
            fi = result["file_info"]
            print(f"   {i}. 文件: {fi['original_name']}")
            print(f"      对象路径: {fi['object_path']} (大小: {fi['file_size_mb']} MiB)")

        # 3. Confirm deletion (soft delete only)
        confirm = input(
            f"\n共 {len(results)} 个文件将被软删除"
            f"（复制到回收站前缀 {MINIO_TRASH_PREFIX}/，可恢复），确认？(y/n): "
        ).strip().lower()
        if confirm != "y":
            print("已取消删除操作。")
            return

        # 4. Perform soft deletion: copy each object to the trash prefix, then delete the original object
        start_time = time.time()
        deleted_count = 0

        for result in results:
            file_info = result["file_info"]
            object_name = file_info["object_path"]
            original_name = file_info["original_name"]

            try:
                # Generate a non-conflicting trash object path
                trash_name = self._get_unique_trash_name(original_name)
                trash_object = f"{MINIO_TRASH_PREFIX}/{trash_name}"

                # Copy object to trash prefix
                self.client.copy_object(
                    MINIO_BUCKET,
                    trash_object,
                    CopySource(MINIO_BUCKET, object_name),
                )

                # Delete original object
                self.client.remove_object(MINIO_BUCKET, object_name)

                print(f"   已软删除: {original_name} -> {trash_object}")
                deleted_count += 1
            except Exception as e:
                print(f"   删除文件 {original_name} 失败: {e}")

        elapsed_ms = round((time.time() - start_time) * 1000, 2)

        print(f"\n成功软删除 {deleted_count} 个文件，耗时 {elapsed_ms} ms。")
        if deleted_count > 0:
            print(f"文件已移入回收站前缀 {MINIO_TRASH_PREFIX}/，如需恢复可将其复制回原前缀。")

    def _get_unique_trash_name(self, filename):
        """
        Generate a non-conflicting trash file name.
        If a file with the same name already exists in the trash, append a numeric suffix to the file name.
        """
        base_name, ext = os.path.splitext(filename)
        candidate = filename
        counter = 1

        # Check whether an object with the same name already exists under the trash prefix
        while True:
            trash_object = f"{MINIO_TRASH_PREFIX}/{candidate}"
            try:
                self.client.stat_object(MINIO_BUCKET, trash_object)
                # Object exists; generate the next candidate name
                candidate = f"{base_name}_{counter}{ext}"
                counter += 1
            except Exception:
                # Object does not exist (exception raised); this name can be used
                break

        return candidate


if __name__ == "__main__":
    deleter = FileMinioDeleter()

    # 1. Get user input
    file_to_delete = input("请输入要删除的文件名: ").strip()
    if not file_to_delete:
        print("文件名不能为空。")
    else:
        # 2. Choose delete mode
        print("请选择删除模式:")
        print("  1. exact    (精确匹配，删除完全相同的文件名)")
        print("  2. ngram    (模糊删除，删除包含关键字的文件)")
        print("  3. wildcard (通配符删除，如 *.log)")

        mode_choice = input("请输入选项 (1/2/3，默认为1): ").strip()
        mode_map = {"1": "exact", "2": "ngram", "3": "wildcard"}
        delete_mode = mode_map.get(mode_choice, "exact")

        # 3. Execute soft delete
        deleter.delete_by_filename(file_to_delete, delete_mode)
