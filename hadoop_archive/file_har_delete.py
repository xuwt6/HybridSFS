# file_har_delete.py
# Hadoop Archive (HAR) baseline delete system (for comparison experiment):
# First search for files in the HAR archive (reusing FileHarSearcher from file_har_search.py),
# then perform soft delete on matched files: remove corresponding file entries from the archive's _index and rebuild _masterindex.
#
# Correspondence with file_delete.py (ES version) / file_hdfs_delete.py (pure HDFS version):
#   - ES version:   mark is_deleted=true (the record remains, filtered out at query time, recoverable)
#   - HDFS version: move files into the trash directory (physical move, still recoverable)
#   - HAR version:  files have been merged into part-N data files; individual file objects cannot be moved separately;
#              therefore equivalent soft delete is achieved by "removing entries from the archive index" — back up before deletion
#              the original _index/_masterindex to the trash directory (recoverable); after deletion
#              file_har_search.py no longer matches these files, which is consistent with the ES version where
#              is_deleted=true filters results at query time, and the HDFS version where files moved to trash are no longer searchable.
#              The file data itself remains in the part data files (corresponding to the ES version where data blocks are untouched).
# Flow and format aligned with file_hdfs_delete.py:
#   search → display matched results → confirm (y/n) → execute soft delete → print "successfully deleted X records" + recovery hint
import os
import sys
import urllib.parse

# Reuse configuration from the parent directory smallfiles_heuristic to ensure "all configurations are identical"
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from hdfs import InsecureClient
from config import Config
from file_har_search import FileHarSearcher, HAR_HDFS_BASE_PATH
from file_har_upload import HarIndexWriter, java_string_hash

# HAR trash path: placed outside the search root directory (HAR_HDFS_BASE_PATH),
# naming convention consistent with the HDFS version's HDFS_TRASH_PATH;
# after soft delete, the backed-up original index is stored under this directory; searches will no longer match deleted files
HAR_TRASH_PATH = HAR_HDFS_BASE_PATH + "_trash"


class FileHarDeleter:
    def __init__(self):
        """Initialize the HDFS client and file searcher"""
        self.hdfs_client = InsecureClient(Config.HDFS_URL, user=Config.HDFS_USER)
        # Verify the connection
        try:
            self.hdfs_client.status("/")
        except Exception as e:
            raise ValueError(f"❌ 无法连接到 HDFS，请检查服务是否启动: {e}")
        self.searcher = FileHarSearcher()

    def delete_by_filename(self, target_name, delete_mode="exact"):
        """
        Soft-delete files by filename in the HAR archive (remove entries from the archive index; original index backed up to the trash directory; recoverable).
        Supports 3 delete modes, consistent with search and download:
        - exact:  exact match, delete files with exactly the same filename
        - ngram:  fuzzy delete, delete files containing the keyword
        - wildcard: wildcard delete, e.g. *.log

        :param target_name:  file name or keyword to delete
        :param delete_mode:  delete mode ("exact", "ngram", "wildcard")
        """
        if delete_mode not in ("exact", "ngram", "wildcard"):
            raise ValueError(f"❌ 不支持的删除模式: {delete_mode}")

        # 1. Use FileHarSearcher to search (the searcher internally matches by delete mode)
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
            print(f"      所在归档: {fi['har_path']} (归档内路径: {fi['path_in_archive']})")
            print(f"      数据分片: {bi['part_name']} (偏移: {bi['offset']}, 长度: {fi['file_size_bytes']})")

        # 3. Confirm deletion (soft delete only)
        confirm = input(f"\n⚠️ 共 {len(results)} 个文件将被软删除（从归档索引移除条目，"
                        f"原索引备份至回收站 {HAR_TRASH_PATH}，可恢复），确认？(y/n): ").strip().lower()
        if confirm != 'y':
            print("⚠️ 已取消删除操作。")
            return

        # 4. Ensure the trash directory exists
        try:
            self.hdfs_client.makedirs(HAR_TRASH_PATH)
        except Exception as e:
            print(f"❌ 创建回收站目录失败: {e}")
            return

        # 5. Execute soft delete: group by affected archives and rewrite the index per archive
        deleted_count = 0
        archive_targets = {}
        for result in results:
            archive_path = result['file_info']['har_path']
            archive_targets.setdefault(archive_path, []).append(result)

        for archive_path, targets in archive_targets.items():
            deleted_count += self._soft_delete_in_archive(archive_path, targets)

        print(f"\n✅ 成功删除 {deleted_count} 条记录。")
        if deleted_count > 0:
            print(f"💡 原归档索引已备份至回收站 {HAR_TRASH_PATH}，"
                  f"如需恢复，将备份的 _index/_masterindex 移回对应归档目录即可。")

    def _soft_delete_in_archive(self, archive_path, targets):
        """
        Execute soft delete within a single archive:
          1) Read and back up the original _index/_masterindex to the trash directory (abort on backup failure; do not modify the archive)
          2) Remove matched file entries from _index (retain all directory entries)
          3) Rebuild _index/_masterindex using the same HarIndexWriter as file_har_upload.py and overwrite
        :return: number of successfully soft-deleted files
        """
        delete_paths = [t['file_info']['path_in_archive'] for t in targets]
        name_by_path = {t['file_info']['path_in_archive']: t['file_info']['original_name']
                        for t in targets}
        try:
            # 5.1 Read the original index
            with self.hdfs_client.read(f"{archive_path}/_index") as reader:
                index_bytes = reader.read()
            with self.hdfs_client.read(f"{archive_path}/_masterindex") as reader:
                master_bytes = reader.read()

            # 5.2 Back up to the trash directory first (corresponds to the HDFS version's recoverable "move to trash" semantics)
            archive_name = archive_path.rstrip('/').rsplit('/', 1)[-1]
            trash_dir = f"{HAR_TRASH_PATH}/{archive_name}"
            self.hdfs_client.makedirs(trash_dir)
            self._upload_bytes(index_bytes, f"{trash_dir}/_index")
            self._upload_bytes(master_bytes, f"{trash_dir}/_masterindex")

            # 5.3 Rebuild the index: retain all directory entries and unmatched file entries
            #      Preserve encoded attribute strings as-is (do not re-encode to avoid double URLEncode escaping)
            writer = HarIndexWriter()
            removed = []
            for raw_line in index_bytes.decode('utf-8').splitlines():
                if not raw_line.strip():
                    continue
                fields = raw_line.strip().split(' ')
                is_file = len(fields) >= 6 and fields[1] == 'file'
                decoded_path = urllib.parse.unquote_plus(fields[0])
                if is_file and decoded_path in delete_paths:
                    removed.append(decoded_path)
                    continue
                writer.entries.append((java_string_hash(decoded_path), raw_line))

            master_new, index_new = writer.build()

            # 5.4 Overwrite the archive index (data files part-* remain untouched)
            self._upload_bytes(master_new, f"{archive_path}/_masterindex")
            self._upload_bytes(index_new, f"{archive_path}/_index")

            for p in removed:
                print(f"   ✅ 已软删除: {name_by_path.get(p, p)}")
            # Exceptional cases such as index changes between search and delete
            for p in delete_paths:
                if p not in removed:
                    print(f"   ❌ 删除文件 {name_by_path.get(p, p)} 失败: 归档索引中未找到对应条目")
            return len(removed)
        except Exception as e:
            for t in targets:
                print(f"   ❌ 删除文件 {t['file_info']['original_name']} 失败: {e}")
            return 0

    def _upload_bytes(self, data, hdfs_path):
        with self.hdfs_client.write(hdfs_path, overwrite=True) as writer:
            writer.write(data)


if __name__ == "__main__":
    deleter = FileHarDeleter()

    # 1. Get user input
    file_to_delete = input("请输入要删除的文件名: ").strip()
    if not file_to_delete:
        print("⚠️ 文件名不能为空。")
    else:
        # 2. Select delete mode (consistent with file_har_search.py / file_har_download.py)
        print("请选择删除模式:")
        print(" 1. exact   (精确匹配，删除完全相同的文件名)")
        print(" 2. ngram   (模糊删除，删除包含关键字的文件)")
        print(" 3. wildcard (通配符删除，如 *.log)")

        mode_choice = input("请输入选项 (1/2/3，默认为1): ").strip()
        mode_map = {"1": "exact", "2": "ngram", "3": "wildcard"}
        delete_mode = mode_map.get(mode_choice, "exact")

        # 3. Execute soft delete
        deleter.delete_by_filename(file_to_delete, delete_mode)
