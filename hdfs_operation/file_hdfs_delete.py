# file_hdfs_delete.py
# Search and soft-delete files directly in HDFS, without depending on Elasticsearch
# Corresponds to the local version file_delete.py: the ES version achieves recoverable soft deletion by marking is_deleted=true;
# the HDFS version achieves the equivalent recoverable soft deletion by moving files to a trash directory
# Searching reuses FileHDFSSearcher from file_hdfs_search.py
import os
import sys

# Reuse the configuration from the parent directory smallfiles_heuristic
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from hdfs import InsecureClient
from config import Config
from file_hdfs_search import FileHDFSSearcher

# HDFS trash path: placed outside the search root directory (HDFS_BASE_PATH),
# so soft-deleted files are no longer matched by searches, equivalent to the ES version where is_deleted=true is filtered out at query time
HDFS_TRASH_PATH = Config.HDFS_BASE_PATH + "_trash"


class FileHDFSDeleter:
    def __init__(self):
        """Initialize the HDFS client and file searcher"""
        self.hdfs_client = InsecureClient(Config.HDFS_URL, user=Config.HDFS_USER)
        # Verify the connection
        try:
            self.hdfs_client.status("/")
        except Exception as e:
            raise ValueError(f"❌ 无法连接到 HDFS，请检查服务是否启动: {e}")
        self.searcher = FileHDFSSearcher()

    def delete_by_filename(self, target_name, delete_mode="exact"):
        """
        Soft-delete files in HDFS by file name (moved to the trash directory, recoverable).
        Supports 3 delete modes, consistent with search and download:
        - exact:  exact match, delete files with exactly the same filename
        - ngram:  fuzzy delete, delete files containing the keyword
        - wildcard: wildcard delete, e.g. *.log

        :param target_name:  file name or keyword to delete
        :param delete_mode:  delete mode ("exact", "ngram", "wildcard")
        """
        if delete_mode not in ("exact", "ngram", "wildcard"):
            raise ValueError(f"❌ 不支持的删除模式: {delete_mode}")

        # 1. Search with FileHDFSSearcher (the searcher already matches according to the delete mode internally)
        print(f"\n🔍 正在以 [{delete_mode}] 模式搜索文件: {target_name}")
        results = self.searcher.search_by_filename(target_name, delete_mode)

        if not results:
            print(f"❌ 未找到匹配 '{target_name}' 的文件，无需删除。")
            return

        # 2. Display matched results for user confirmation
        print(f"✅ 找到 {len(results)} 个匹配的文件，准备软删除:\n")
        for i, result in enumerate(results, 1):
            fi = result['file_info']
            bi = result['block_info']
            print(f"   {i}. 文件: {fi['original_name']}")
            print(f"      HDFS路径: {fi['hdfs_path']} (大小: {fi['file_size_mb']} MB)")
            print(f"      块大小: {bi['block_size_mb']} MB, 块数量: {bi['block_count']}")

        # 3. Confirm deletion (soft delete only)
        confirm = input(f"\n⚠️ 共 {len(results)} 个文件将被软删除（移入回收站 {HDFS_TRASH_PATH}，可恢复），确认？(y/n): ").strip().lower()
        if confirm != 'y':
            print("⚠️ 已取消删除操作。")
            return

        # 4. Ensure the trash directory exists
        try:
            self.hdfs_client.makedirs(HDFS_TRASH_PATH)
        except Exception as e:
            print(f"❌ 创建回收站目录失败: {e}")
            return

        # 5. Perform the soft delete: move the files to the trash directory one by one
        deleted_count = 0
        for result in results:
            file_info = result['file_info']
            hdfs_path = file_info['hdfs_path']
            original_name = file_info['original_name']
            try:
                trash_path = self._get_unique_trash_path(HDFS_TRASH_PATH, original_name)
                self.hdfs_client.rename(hdfs_path, trash_path)
                print(f"   ✅ 已软删除: {original_name} -> {trash_path}")
                deleted_count += 1
            except Exception as e:
                print(f"   ❌ 删除文件 {original_name} 失败: {e}")

        print(f"\n✅ 成功删除 {deleted_count} 条记录。")
        if deleted_count > 0:
            print(f"💡 文件已移入回收站 {HDFS_TRASH_PATH}，如需恢复可将其移回原目录。")

    def _get_unique_trash_path(self, trash_dir, filename):
        """
        Generate a non-conflicting trash path.
        If a file with the same name already exists in the trash directory, append a numeric suffix to the file name, e.g. file.txt -> file_1.txt.
        """
        base_name, ext = os.path.splitext(filename)
        target_path = f"{trash_dir}/{filename}"
        counter = 1

        # Loop to check whether the file already exists in the trash directory, and modify the file name if it does
        while self.hdfs_client.status(target_path, strict=False) is not None:
            target_path = f"{trash_dir}/{base_name}_{counter}{ext}"
            counter += 1

        return target_path


if __name__ == "__main__":
    deleter = FileHDFSDeleter()

    # 1. Get user input
    file_to_delete = input("请输入要删除的文件名: ").strip()
    if not file_to_delete:
        print("⚠️ 文件名不能为空。")
    else:
        # 2. Choose the delete mode (consistent with file_hdfs_search.py / file_hdfs_download.py)
        print("请选择删除模式:")
        print(" 1. exact   (精确匹配，删除完全相同的文件名)")
        print(" 2. ngram   (模糊删除，删除包含关键字的文件)")
        print(" 3. wildcard (通配符删除，如 *.log)")

        mode_choice = input("请输入选项 (1/2/3，默认为1): ").strip()
        mode_map = {"1": "exact", "2": "ngram", "3": "wildcard"}
        delete_mode = mode_map.get(mode_choice, "exact")

        # 3. Execute soft delete
        deleter.delete_by_filename(file_to_delete, delete_mode)
