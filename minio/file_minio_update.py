# file_minio_update.py
# MinIO baseline batch file update tool (for comparison experiments), without relying on Elasticsearch
# Functionality and flow consistent with the HDFS version file_hdfs_update.py (two phases: soft-delete old files + re-upload new files):
#   - Phase 1: Soft-delete same-named old files in MinIO (copy to trash prefix then delete original object; recoverable)
#   - Phase 2: Upload new files to MinIO one by one, without packing or merging (consistent with file_minio_upload.py)
# Metrics fully consistent with file_hdfs_update.py for cross-comparison

import glob
import os
import sys
import time

# Reuse the configuration from the parent directory smallfiles_heuristic
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from minio import Minio
from minio.commonconfig import CopySource
from file_minio_search import FileMinioSearcher

# ============ MinIO connection configuration (consistent with other minio scripts) ============
MINIO_ENDPOINT = "192.168.31.111:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_SECURE = False
MINIO_BUCKET = "small-files-test"
MINIO_BASE_PREFIX = "raw_small_files"
MINIO_TRASH_PREFIX = "raw_small_files_trash"

# 8 MiB multipart upload (consistent with file_minio_upload.py)
PART_SIZE = 8 * 1024 * 1024

# ================= Configuration =================
CONFIG = {
    # Path of the folder containing the files to update
    "folder_path": r"E:\merge_small_files_for_memory\datatest\3-pet",
    "dry_run": False,  # True: preview only, do not execute; False: actually execute
}


class BatchFileMinioUpdater:
    def __init__(self, config):
        self.config = config
        self.folder_path = config["folder_path"]
        self.dry_run = config["dry_run"]

        # Initialize MinIO client
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
            raise ConnectionError(f"无法连接到 MinIO，请检查服务是否启动: {e}")
        print("MinIO 客户端连接验证通过。")

        # Initialize the file searcher (used in phase one to locate old files with the same name)
        self.searcher = FileMinioSearcher()

    # ================= Utility Methods =================

    def get_files_to_update(self):
        """Get all files to update under the folder"""
        if not os.path.exists(self.folder_path):
            raise FileNotFoundError(f"文件夹不存在: {self.folder_path}")
        files = [f for f in glob.glob(os.path.join(self.folder_path, "*")) if os.path.isfile(f)]
        if not files:
            print("[警告] 文件夹中没有找到任何文件。")
        return files

    def _get_unique_trash_name(self, filename):
        """
        Generate a non-conflicting trash file name (consistent with file_minio_delete.py).
        If a file with the same name already exists in the trash, append a numeric suffix to the file name.
        """
        base_name, ext = os.path.splitext(filename)
        candidate = filename
        counter = 1

        while True:
            trash_object = f"{MINIO_TRASH_PREFIX}/{candidate}"
            try:
                self.client.stat_object(MINIO_BUCKET, trash_object)
                # Object exists; generate the next candidate name
                candidate = f"{base_name}_{counter}{ext}"
                counter += 1
            except Exception:
                # Object does not exist; this name can be used
                break

        return candidate

    # ================= Phase One: Soft-Delete Old Files =================

    def soft_delete_old_files(self, filenames):
        """
        Phase 1: Soft-delete same-named old files in MinIO (copy to trash prefix then delete original object; recoverable).
        Corresponds to the rename-to-trash-directory logic in the HDFS version.
        """
        print(f"\n=== 阶段一：软删除旧文件 (共 {len(filenames)} 个文件) ===")
        deleted_count, failed_count, not_found_count = 0, 0, 0

        for filename in filenames:
            if self.dry_run:
                print(f"[预览] 将软删除旧文件: {filename}")
                deleted_count += 1
                continue

            try:
                # Use FileMinioSearcher to perform exact search for same-named old files
                results = self.searcher.search_by_filename(filename, "exact")

                if not results:
                    print(f"[跳过] 未找到旧文件: {filename}（阶段二将直接上传新文件）")
                    not_found_count += 1
                    continue

                for result in results:
                    object_path = result["file_info"]["object_path"]

                    # Generate a non-conflicting trash object path
                    trash_name = self._get_unique_trash_name(filename)
                    trash_object = f"{MINIO_TRASH_PREFIX}/{trash_name}"

                    # Copy object to trash prefix
                    self.client.copy_object(
                        MINIO_BUCKET,
                        trash_object,
                        CopySource(MINIO_BUCKET, object_path),
                    )

                    # Delete original object
                    self.client.remove_object(MINIO_BUCKET, object_path)

                    print(f"[执行] 软删除 {filename} ({object_path}) -> {trash_object}")
                    deleted_count += 1

            except Exception as e:
                print(f"[错误] 软删除 {filename} 失败: {e}")
                failed_count += 1

        if not self.dry_run and not_found_count > 0:
            print(f"共 {not_found_count} 个文件在 MinIO 中无旧版本，跳过软删除。")

        return deleted_count, failed_count

    # ================= Phase Two: Re-upload New Files =================

    def upload_new_files(self, file_paths):
        """
        Phase 2: Upload new files to MinIO one by one.
        Baseline approach: no packaging, no merging (consistent with file_minio_upload.py).
        """
        print(f"\n=== 阶段二：重新上传新文件 (共 {len(file_paths)} 个) ===")

        if self.dry_run:
            for file_path in file_paths:
                filename = os.path.basename(file_path)
                print(f"[预览] 将上传新文件: {filename}")
            return len(file_paths), 0

        upload_success = 0
        upload_failed = 0
        total_bytes = 0

        # Measure total duration for the entire upload code section only once (consistent with file_minio_upload.py)
        upload_start = time.time()
        for file_path in file_paths:
            filename = os.path.basename(file_path)
            object_name = f"{MINIO_BASE_PREFIX}/{filename}"
            try:
                self.client.fput_object(
                    MINIO_BUCKET,
                    object_name,
                    file_path,
                    part_size=PART_SIZE,
                )
            except Exception as e:
                print(f"[错误] 上传失败 {filename}: {e}")
                upload_failed += 1
                continue

            file_size = os.path.getsize(file_path)
            total_bytes += file_size
            upload_success += 1
            print(f"[执行] 已上传 {filename}，大小 {file_size / 1048576:.4f} MiB")
        upload_elapsed = time.time() - upload_start

        rate = (total_bytes / 1048576) / upload_elapsed if upload_elapsed > 0 else 0.0
        print(f"\n上传统计: 成功 {upload_success} 个, 失败 {upload_failed} 个, "
              f"合计 {total_bytes / 1048576:.2f} MiB, 耗时 {upload_elapsed:.2f} 秒, "
              f"平均速率 {rate:.2f} MiB/秒")

        return upload_success, upload_failed

    # ================= Main Flow =================

    def run(self):
        """Run the batch update main flow"""
        print("=" * 60)
        print("批量文件更新工具 - MinIO 基线版 (软删除 + 重新上传)")
        print(f"当前模式: {'预览模式 (DRY RUN)' if self.dry_run else '正式执行模式'}")
        print(f"文件夹路径: {self.folder_path}")
        print("=" * 60)

        total_start = time.time()

        # Get the list of files to update
        file_paths = self.get_files_to_update()
        filenames = [os.path.basename(f) for f in file_paths]

        # Phase one: soft-delete the old files
        del_start = time.time()
        del_success, del_failed = self.soft_delete_old_files(filenames)
        del_elapsed = time.time() - del_start

        if del_failed > 0 and not self.dry_run:
            print("\n[终止] 阶段一存在失败项，为防止数据不一致，已终止阶段二！")
            return

        # Phase two: re-upload the new files
        up_success, up_failed = self.upload_new_files(file_paths)

        total_elapsed = time.time() - total_start

        # Print final statistics (format consistent with file_hdfs_update.py)
        print("\n" + "=" * 60)
        print("执行统计摘要:")
        print(f"  文件总数: {len(file_paths)}")
        print(f"  软删除成功: {del_success} | 软删除失败: {del_failed} | 阶段一耗时: {del_elapsed:.2f} 秒")
        print(f"  上传成功: {up_success} | 上传失败: {up_failed}")
        print(f"  总耗时: {total_elapsed:.2f} 秒")
        print("=" * 60)


if __name__ == "__main__":
    updater = BatchFileMinioUpdater(CONFIG)
    updater.run()
