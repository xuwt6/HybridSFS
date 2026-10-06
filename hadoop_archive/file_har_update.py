# file_har_update.py
# HAR baseline batch file update tool (for comparison experiment), without relying on Elasticsearch
# Functionality and flow consistent with file_update.py (ES version) / file_hdfs_update.py (HDFS version)
# (two phases: soft-delete old files + re-upload new files):
#   - ES version phase one achieves soft delete by marking is_deleted=true;
#     HDFS version equivalently moves old files into the trash directory;
#     HAR version: files have been merged into part data files and cannot be moved individually; therefore reuse
#     the equivalent soft-delete approach from file_har_delete.py — back up the original archive index to the trash directory +
#     remove corresponding entries from the archive index (recoverable; after deletion file_har_search.py no longer matches them)
#   - ES version phase two uses FFDPacker to merge and pack into large blocks before uploading;
#     HDFS version uses the baseline approach: upload one by one, each small file exclusively occupies 1 block;
#     HAR version uses the same HAR baseline approach as file_har_upload.py: group files by target size
#     (HAR_ARCHIVE_MAX_BYTES), build standard HAR archives
#     (_masterindex/_index/part-N) and upload to HAR_HDFS_BASE_PATH
import glob
import os
import sys
import time
import uuid

# Reuse configuration from the parent directory smallfiles_heuristic to ensure "all configurations are identical"
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from hdfs import InsecureClient
from config import Config
from file_har_search import FileHarSearcher
from file_har_upload import (
    HarArchiveBuilder,
    FileHarUploader,
    HAR_HDFS_BASE_PATH,
    HAR_ARCHIVE_MAX_BYTES,
)
from file_har_delete import FileHarDeleter

# ================= Configuration =================
CONFIG = {
    # Folder path of files to be updated (consistent with other baselines' update directories; modify if different)
    "folder_path": r"E:\merge_small_files_for_memory\datatest\data-update",
    "dry_run": False,                   # True: preview only, do not execute; False: actually execute
}


class BatchFileHarUpdater:
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
        self.searcher = FileHarSearcher()

        # Reuse FileHarDeleter's archive index rewriting logic (phase one soft delete);
        # share the searcher to avoid reloading archive indexes repeatedly
        self.deleter = FileHarDeleter()
        self.deleter.searcher = self.searcher

    # ================= Utility Methods =================

    def get_files_to_update(self):
        """Get all files to update under the folder"""
        if not os.path.exists(self.folder_path):
            raise FileNotFoundError(f"❌ 文件夹不存在: {self.folder_path}")
        files = [f for f in glob.glob(os.path.join(self.folder_path, "*")) if os.path.isfile(f)]
        if not files:
            print("⚠️ [警告] 文件夹中没有找到任何文件。")
        return files

    def _upload_bytes(self, data, hdfs_path):
        with self.hdfs.write(hdfs_path, overwrite=True) as writer:
            writer.write(data)

    # ================= Phase One: Soft-Delete Old Files =================

    def soft_delete_old_files(self, filenames):
        """
        Phase one: soft-delete the same-named old files in the HAR archives (remove the entries from the archive index,
        the original index is backed up to the trash directory and is recoverable), corresponding to the logic based on original_name
        in the ES version that matches exactly and marks is_deleted=true
        """
        print(f"\n=== 阶段一：软删除旧文件 (共 {len(filenames)} 个文件) ===")
        deleted_count, failed_count, not_found_count = 0, 0, 0

        if self.dry_run:
            for filename in filenames:
                print(f"🔍 [预览] 将软删除旧文件: {filename}")
            return len(filenames), 0

        # 1. Search for each old file by exact name one by one (consistent with file_hdfs_update.py)
        #    The same searcher instance carries an index cache, avoiding reloading the archive index for every file
        archive_targets = {}  # {archive path: [matching results, ...]}
        for filename in filenames:
            try:
                results = self.searcher.search_by_filename(filename, "exact")
            except Exception as e:
                print(f"❌ [错误] 软删除 {filename} 失败: {e}")
                failed_count += 1
                continue

            if not results:
                print(f"⚠️ [跳过] 未找到旧文件: {filename}（阶段二将直接上传新文件）")
                not_found_count += 1
                continue

            for result in results:
                archive_targets.setdefault(result['file_info']['har_path'], []).append(result)

        # 2. Rewrite indexes in batches grouped by archive (back up the original index + remove entries, consistent with file_har_delete.py)
        for archive_path, targets in archive_targets.items():
            removed = self.deleter._soft_delete_in_archive(archive_path, targets)
            deleted_count += removed
            # Archive-level rewrite failures or partial failures are also counted as failures,
            # Consistent with the ES version: when phase one has failed items, the main flow aborts phase two to prevent old and new versions from coexisting
            if removed < len(targets):
                failed_count += len(targets) - removed

        # The archive index has been rewritten; clear the searcher's index cache to prevent later misuse of a stale index
        self.searcher._index_cache.clear()

        if not_found_count > 0:
            print(f"ℹ️ 共 {not_found_count} 个文件在 HAR 归档中无旧版本，跳过软删除。")

        return deleted_count, failed_count

    # ================= Phase Two: Re-upload New Files =================

    def upload_new_files(self, file_paths):
        """
        Phase two: pack and upload the new files using the HAR baseline approach
        The packing strategy is consistent with file_har_upload.py: group by the HAR_ARCHIVE_MAX_BYTES target size,
        build standard HAR archives (_masterindex/_index/part-N), and upload them
        """
        print(f"\n=== 阶段二：重新上传新文件 (共 {len(file_paths)} 个) ===")

        if self.dry_run:
            for file_path in file_paths:
                filename = os.path.basename(file_path)
                print(f"🔍 [预览] 将上传新文件: {filename}")
            return len(file_paths), 0

        # 1. Read file metadata (size, modification time; time semantics consistent with file_har_upload.py)
        file_entries = []
        read_failed = 0
        for file_path in file_paths:
            filename = os.path.basename(file_path)
            try:
                size = os.path.getsize(file_path)
                mtime_ms = int(os.path.getmtime(file_path) * 1000)
            except OSError as e:
                print(f"❌ [错误] 读取文件失败 {filename}: {e}")
                read_failed += 1
                continue
            file_entries.append((file_path, f"/{filename}", size, mtime_ms))

        if not file_entries:
            print("⚠️ [警告] 没有有效文件可上传。")
            return 0, read_failed

        total_size = sum(e[2] for e in file_entries)
        print(f"📋 共读取 {len(file_entries)} 个有效文件，合计 {total_size / 1048576:.2f} MiB")

        # 2. Group by target size (consistent with group_files in file_har_upload.py)
        groups = FileHarUploader.group_files(file_entries)
        print(f"📦 按 HAR 基线分组策略（{HAR_ARCHIVE_MAX_BYTES / 1048576:.0f} MiB 目标大小），"
              f"分为 {len(groups)} 个归档")

        # 3. Build and upload archive by archive
        upload_success = 0
        upload_failed = 0
        total_bytes = 0
        uploaded_archives = 0
        dir_mtime_ms = int(os.path.getmtime(self.folder_path) * 1000)

        # Measure the total elapsed time only once for the entire upload code section (consistent with file_hdfs_update.py)
        upload_start = time.time()
        for idx, group in enumerate(groups, 1):
            archive_name = f"HAR_{int(time.time())}_{uuid.uuid4().hex[:8]}.har"
            builder = HarArchiveBuilder(archive_name)
            archive_uploaded = False
            try:
                # Directory entries: the update folder has a flat structure, so only the root directory entry is generated
                # (consistent with the directory index semantics of file_har_upload.py)
                builder.add_dir("/", sorted(os.path.basename(e[0]) for e in group), dir_mtime_ms)

                # File entries: appended sequentially to the part data files
                for local_path, har_path, _, mtime_ms in group:
                    try:
                        builder.add_file(local_path, har_path, mtime_ms)
                    except Exception as e:
                        print(f"❌ [错误] 文件写入归档失败 {archive_name}: {local_path}, {e}")
                        upload_failed += 1

                master_bytes, index_bytes = builder.finish()
                har_dir = f"{HAR_HDFS_BASE_PATH}/{archive_name}"
                self._upload_bytes(master_bytes, f"{har_dir}/_masterindex")
                self._upload_bytes(index_bytes, f"{har_dir}/_index")
                for i, part in enumerate(builder.parts):
                    self._upload_bytes(bytes(part), f"{har_dir}/part-{i}")

                total_bytes += builder.total_bytes
                upload_success += builder.file_count
                uploaded_archives += 1
                archive_uploaded = True
                file_names = [os.path.basename(e[0]) for e in group]
                print(f"✅ [执行] 归档 {idx}/{len(groups)} 上传成功: {archive_name} "
                      f"(包含 {builder.file_count} 个文件, {builder.total_bytes / 1048576:.2f} MiB)")
                print(f"   文件: {', '.join(file_names[:3])}" + (f" 等 {len(file_names)} 个" if len(file_names) > 3 else ""))

            except Exception as e:
                block_names = [os.path.basename(e_[0]) for e_ in group]
                print(f"❌ [错误] 归档 {idx}/{len(groups)} 上传失败 ({block_names}): {e}")
                # If the success count has already been incremented (the exception occurred after the statistics), roll back first and then count it as a failure
                if archive_uploaded:
                    upload_success -= builder.file_count
                # Files already written to the builder are lost along with the failed archive upload and are counted as failures;
                # the remaining files were already counted by the earlier inner exception
                upload_failed += builder.file_count

        upload_elapsed = time.time() - upload_start

        rate = (total_bytes / 1048576) / upload_elapsed if upload_elapsed > 0 else 0.0
        print(f"\n📊 上传统计: 成功 {upload_success} 个, 失败 {upload_failed + read_failed} 个 "
              f"(含读取失败 {read_failed} 个), 合计 {total_bytes / 1048576:.2f} MiB, "
              f"耗时 {upload_elapsed:.2f} 秒, 平均速率 {rate:.2f} MiB/秒, "
              f"生成 HAR 归档 {uploaded_archives} 个")

        return upload_success, upload_failed + read_failed

    # ================= Main Flow =================

    def run(self):
        """Run the batch update main flow"""
        print("=" * 60)
        print("🚀 批量文件更新工具 - HAR 基线版 (软删除 + 重新上传)")
        print(f"📌 当前模式: {'🔍 预览模式 (DRY RUN)' if self.dry_run else '🚀 正式执行模式'}")
        print(f"📂 文件夹路径: {self.folder_path}")
        print(f"🗂️ HAR 上传目录: {HAR_HDFS_BASE_PATH}")
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
        up_start = time.time()
        up_success, up_failed = self.upload_new_files(file_paths)
        up_elapsed = time.time() - up_start

        total_elapsed = time.time() - total_start

        # Print final statistics (a summary consistent with file_update.py / file_hdfs_update.py, plus elapsed time for comparison experiments)
        print("\n" + "=" * 60)
        print("📊 执行统计摘要:")
        print(f"  文件总数: {len(file_paths)}")
        print(f"  软删除成功: {del_success} | 软删除失败: {del_failed} | 阶段一耗时: {del_elapsed:.2f} 秒")
        print(f"  上传成功: {up_success} | 上传失败: {up_failed} | 阶段二耗时: {up_elapsed:.2f} 秒")
        print(f"  总耗时: {total_elapsed:.2f} 秒")
        print("=" * 60)


if __name__ == "__main__":
    updater = BatchFileHarUpdater(CONFIG)
    updater.run()
