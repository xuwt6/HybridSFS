# file_hdfs_update.py
# HDFS baseline version of the batch file update tool (for comparison experiments), without depending on Elasticsearch
# The functionality and flow are identical to the local version file_update.py (two phases: soft-delete the old files + re-upload the new files):
#   - ES version phase one achieves soft delete by marking is_deleted=true;
#     the HDFS version implements the equivalent by moving the old files into a trash directory (consistent with file_hdfs_delete.py, recoverable)
#   - ES version phase two uses FFDPacker to merge and pack into large blocks before uploading;
#     The HDFS version uses the baseline approach: upload one by one, with 1 small file exclusively occupying 1 block, no packing and no merging
#     (consistent with hdfs_upload_system.py)
import glob
import os
import sys
import time

# Reuse configuration from the parent directory smallfiles_heuristic to ensure "all configurations are identical"
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from hdfs import InsecureClient
from config import Config
from file_hdfs_search import FileHDFSSearcher

# ================= Configuration =================
CONFIG = {
    # Path of the folder containing the files to update
    "folder_path": r"E:\merge_small_files_for_memory\datatest\1-police_data_eng",
    "dry_run": False,                   # True: preview only, do not execute; False: actually execute
}

# HDFS trash path: consistent with file_hdfs_delete.py, placed outside the search root directory,
# so soft-deleted files are no longer matched by searches, equivalent to the ES version where is_deleted=true is filtered out at query time
HDFS_TRASH_PATH = Config.HDFS_BASE_PATH + "_trash"

CHUNK_SIZE = 8 * 1024 * 1024  # Stream-write in 8 MiB chunks (consistent with hdfs_upload_system.py)


class BatchFileHDFSUpdater:
    def __init__(self, config):
        self.config = config
        self.folder_path = config["folder_path"]
        self.dry_run = config["dry_run"]

        # Initialize the HDFS client
        self.hdfs = InsecureClient(Config.HDFS_URL, user=Config.HDFS_USER)
        # Verify the connection
        try:
            self.hdfs.status("/")
        except Exception as e:
            raise ConnectionError(f"❌ 无法连接到 HDFS，请检查服务是否启动: {e}")
        print("✅ HDFS 客户端连接验证通过。")

        # Initialize the file searcher (used in phase one to locate old files with the same name)
        self.searcher = FileHDFSSearcher()

    # ================= Utility Methods =================

    def get_files_to_update(self):
        """Get all files to update under the folder"""
        if not os.path.exists(self.folder_path):
            raise FileNotFoundError(f"❌ 文件夹不存在: {self.folder_path}")
        files = [f for f in glob.glob(os.path.join(self.folder_path, "*")) if os.path.isfile(f)]
        if not files:
            print("⚠️ [警告] 文件夹中没有找到任何文件。")
        return files

    def _get_unique_trash_path(self, trash_dir, filename):
        """
        Generate a non-conflicting trash path (consistent with file_hdfs_delete.py).
        If a file with the same name already exists in the trash directory, append a numeric suffix to the file name, e.g. file.txt -> file_1.txt.
        """
        base_name, ext = os.path.splitext(filename)
        target_path = f"{trash_dir}/{filename}"
        counter = 1

        # Loop to check whether the file already exists in the trash directory, and modify the file name if it does
        while self.hdfs.status(target_path, strict=False) is not None:
            target_path = f"{trash_dir}/{base_name}_{counter}{ext}"
            counter += 1

        return target_path

    # ================= Phase One: Soft-Delete Old Files =================

    def soft_delete_old_files(self, filenames):
        """
        Phase one: soft-delete the same-named old files in HDFS (moved into the trash directory, recoverable)
        Corresponds to the ES version's logic of exact matching on original_name and marking is_deleted=true
        """
        print(f"\n=== 阶段一：软删除旧文件 (共 {len(filenames)} 个文件) ===")
        deleted_count, failed_count, not_found_count = 0, 0, 0

        if not self.dry_run:
            # Ensure the trash directory exists (placed outside the search root directory so it will not be matched by searches)
            try:
                if self.hdfs.status(HDFS_TRASH_PATH, strict=False) is None:
                    self.hdfs.makedirs(HDFS_TRASH_PATH)
            except Exception as e:
                print(f"❌ [错误] 创建回收站目录失败: {e}")
                return 0, len(filenames)

        for filename in filenames:
            if self.dry_run:
                print(f"🔍 [预览] 将软删除旧文件: {filename}")
                deleted_count += 1
                continue

            try:
                # Use FileHDFSSearcher to search exactly for the same-named old files (consistent with file_hdfs_delete.py)
                results = self.searcher.search_by_filename(filename, "exact")

                if not results:
                    print(f"⚠️ [跳过] 未找到旧文件: {filename}（阶段二将直接上传新文件）")
                    not_found_count += 1
                    continue

                for result in results:
                    hdfs_path = result['file_info']['hdfs_path']
                    trash_path = self._get_unique_trash_path(HDFS_TRASH_PATH, filename)
                    self.hdfs.rename(hdfs_path, trash_path)
                    print(f"✅ [执行] 软删除 {filename} ({hdfs_path}) -> {trash_path}")
                    deleted_count += 1

            except Exception as e:
                print(f"❌ [错误] 软删除 {filename} 失败: {e}")
                failed_count += 1

        if not self.dry_run and not_found_count > 0:
            print(f"ℹ️ 共 {not_found_count} 个文件在 HDFS 中无旧版本，跳过软删除。")

        return deleted_count, failed_count

    # ================= Phase Two: Re-upload New Files =================

    def upload_new_files(self, file_paths):
        """
        Phase 2: upload new files one by one to the HDFS baseline directory
        Baseline approach: 1 small file exclusively occupies 1 block, no packing, no merging (consistent with hdfs_upload_system.py)
        """
        print(f"\n=== 阶段二：重新上传新文件 (共 {len(file_paths)} 个) ===")

        if self.dry_run:
            for file_path in file_paths:
                filename = os.path.basename(file_path)
                print(f"🔍 [预览] 将上传新文件: {filename}")
            return len(file_paths), 0

        upload_success = 0
        upload_failed = 0
        total_bytes = 0

        # Measure the total elapsed time only once for the entire upload code section (consistent with hdfs_upload_system.py)
        upload_start = time.time()
        for file_path in file_paths:
            filename = os.path.basename(file_path)
            hdfs_path = f"{Config.HDFS_BASE_PATH}/{filename}"
            try:
                # Streaming read/write: read locally while writing to HDFS, with 1 file exclusively occupying 1 block
                with open(file_path, 'rb') as fh, self.hdfs.write(hdfs_path, overwrite=True) as writer:
                    while True:
                        chunk = fh.read(CHUNK_SIZE)
                        if not chunk:
                            break
                        writer.write(chunk)
            except Exception as e:
                print(f"❌ [错误] 上传失败 {filename}: {e}")
                upload_failed += 1
                continue

            file_size = os.path.getsize(file_path)
            total_bytes += file_size
            upload_success += 1
            print(f"✅ [执行] 已上传 {filename}（独占 1 block），大小 {file_size / 1048576:.4f} MiB")
        upload_elapsed = time.time() - upload_start

        rate = (total_bytes / 1048576) / upload_elapsed if upload_elapsed > 0 else 0.0
        print(f"\n📊 上传统计: 成功 {upload_success} 个, 失败 {upload_failed} 个, "
              f"合计 {total_bytes / 1048576:.2f} MiB, 耗时 {upload_elapsed:.2f} 秒, 平均速率 {rate:.2f} MiB/秒")

        return upload_success, upload_failed

    # ================= Main Flow =================

    def run(self):
        """Run the batch update main flow"""
        print("=" * 60)
        print("🚀 批量文件更新工具 - HDFS 基线版 (软删除 + 重新上传)")
        print(f"📌 当前模式: {'🔍 预览模式 (DRY RUN)' if self.dry_run else '🚀 正式执行模式'}")
        print(f"📂 文件夹路径: {self.folder_path}")
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
            print("\n🛑 [终止] 阶段一存在失败项，为防止数据不一致，已终止阶段二！")
            return

        # Phase two: re-upload the new files
        up_success, up_failed = self.upload_new_files(file_paths)

        total_elapsed = time.time() - total_start

        # Print final statistics (a summary consistent with file_update.py, plus elapsed time for comparison experiments)
        print("\n" + "=" * 60)
        print("📊 执行统计摘要:")
        print(f"  文件总数: {len(file_paths)}")
        print(f"  软删除成功: {del_success} | 软删除失败: {del_failed} | 阶段一耗时: {del_elapsed:.2f} 秒")
        print(f"  上传成功: {up_success} | 上传失败: {up_failed}")
        print(f"  总耗时: {total_elapsed:.2f} 秒")
        print("=" * 60)


if __name__ == "__main__":
    updater = BatchFileHDFSUpdater(CONFIG)
    updater.run()
