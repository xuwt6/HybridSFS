# seaweedfs_remove.py
# SeaweedFS directory checker + one-click delete tool, functionality and counting basis aligned with hdfs_block_checker.py:
#   - check_directory_blocks: recursively traverse the directory, list size and chunk info per file, and print a summary
#       (SeaweedFS chunk corresponds to HDFS block; chunk size comes from Config.SEAWEDFS_CHUNK_SIZE_MB,
#         each file is uploaded with maxMB forcing exclusive occupation of 1 chunk, so block count = ceil(file_size / chunk_size))
#   - delete_directory: delete the specified directory or file; requires typing yes for secondary confirmation before deletion (consistent with HDFS version, irreversible)
# Traversal prefers client.walk_entries (directory listings already include FileSize, avoiding per-file metadata requests and improving efficiency).
import os
import sys
from collections import defaultdict

# Reuse the configuration from the parent directory smallfiles_heuristic
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from config import Config
from seaweedfs_client import SeaweedFSFilerClient


class SeaweedFSBlockChecker:
    def __init__(self, filer_url=None):
        """
        :param filer_url: SeaweedFS Filer address, defaults to Config.SEAWEDFS_FILER_URL
        """
        self.client = SeaweedFSFilerClient(base_url=filer_url) if filer_url else SeaweedFSFilerClient()
        # Verify connection (raise ValueError when unreachable, consistent with other seaweedfs scripts)
        self.client.health_check()
        # Single block size (bytes): corresponds to HDFS block_size, used to compute the number of blocks per file
        self.chunk_size = Config.SEAWEDFS_CHUNK_SIZE_MB * 1024 * 1024

    def check_directory_blocks(self, filer_path):
        """
        Check block (chunk) information of all files under the specified directory (aligned with hdfs_block_checker.py)
        """
        print(f"\n🔍 正在检查 SeaweedFS 目录: {filer_path}")
        print("=" * 70)

        total_blocks = 0
        total_size = 0
        total_files = 0
        block_size_distribution = defaultdict(int)

        try:
            # Recursively traverse all files under the directory (entries returned by walk_entries already contain FileSize, avoiding per-file stat)
            for root, file_entries in self.client.walk_entries(filer_path):
                for entry in file_entries:
                    full_path = entry.get("FullPath", "")
                    file_name = full_path.rsplit("/", 1)[-1]
                    file_size = entry.get("FileSize", 0) or 0
                    block_size = self.chunk_size

                    # Compute the number of blocks for this file (same ceil algorithm as the HDFS version)
                    file_blocks = (file_size + block_size - 1) // block_size if block_size > 0 else 0
                    total_blocks += file_blocks
                    total_size += file_size
                    total_files += 1

                    # Record block size distribution
                    block_size_distribution[block_size] += file_blocks

                    print(f"📄 文件: {file_name}")
                    print(f"   大小: {file_size / (1024*1024):.2f} MiB | 块数: {file_blocks} | 单块大小: {block_size / (1024*1024):.2f} MiB")

            # Print summary report (consistent with hdfs_block_checker.py, with file count appended)
            print("\n" + "=" * 70)
            print("📊 SeaweedFS 块信息汇总:")
            print(f"   总文件数: {total_files}")
            print(f"   总文件块数: {total_blocks}")
            print(f"   总数据大小: {total_size / (1024*1024*1024):.2f} GiB")
            print(f"   块大小分布(字节->块数): {dict(block_size_distribution)}")
            print("=" * 70)

        except Exception as e:
            print(f"❌ 连接或读取 SeaweedFS 目录失败: {e}")

    def delete_directory(self, filer_path, recursive=True):
        """
        Delete the specified SeaweedFS directory or file (one-click delete all).
        :param filer_path: SeaweedFS path to delete
        :param recursive: whether to delete recursively, default True. When True, even non-empty directories are deleted entirely.
        Note: this is a real delete (irreversible), different from soft delete (move to trash); requires typing yes for secondary confirmation before deletion.
        """
        print(f"\n⚠️ 警告：即将删除 SeaweedFS 路径: {filer_path}")
        if recursive:
            print("   递归删除：该路径下的所有子目录与文件都会被一并删除！")
        confirm = input("确定要继续吗？此操作不可逆！(yes/NO): ")
        if confirm.lower() == 'yes':
            try:
                ok = self.client.delete(filer_path, recursive=recursive)
                if ok:
                    print(f"✅ 成功删除: {filer_path}")
                else:
                    print(f"❌ 删除失败: {filer_path}")
            except Exception as e:
                print(f"❌ 删除失败: {e}")
        else:
            print("操作已取消。")


if __name__ == "__main__":
    # Target directory defaults to Config.SEAWEDFS_BASE_PATH (i.e., the root uploaded by seaweedfs_upload.py); adjust as needed
    TARGET_PATH = Config.SEAWEDFS_BASE_PATH

    checker = SeaweedFSBlockChecker()

    # 1. List directory and block information first
    checker.check_directory_blocks(TARGET_PATH)

    # 2. One-click delete the entire directory (including all subdirectories and files); comment out the line below to inspect only without deleting
    checker.delete_directory(TARGET_PATH)
