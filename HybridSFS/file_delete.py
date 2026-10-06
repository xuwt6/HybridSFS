# file_delete.py
from elasticsearch import Elasticsearch
from config import Config


class FileDeleter:
    def __init__(self):
        """Initialize Elasticsearch client"""
        self.es = Elasticsearch([Config.ES_HOST])
        if not self.es.ping():
            raise ValueError("❌ 无法连接到 Elasticsearch，请检查服务是否启动。")

    def delete_by_filename(self, target_name, delete_mode="exact"):
        """
        Soft-delete file records in the Elasticsearch index by file name (mark is_deleted=true, recoverable).
        Supports 3 delete modes, consistent with search and download:
        - exact:  exact match, delete files with exactly the same filename
        - ngram:  fuzzy delete, deleting records containing the keyword
        - wildcard: wildcard delete, e.g. *.log

        :param target_name:  file name or keyword to delete
        :param delete_mode:  delete mode ("exact", "ngram", "wildcard")
        """
        # 1. Dynamically build the query body based on delete mode (consistent with file_search.py)
        if delete_mode == "exact":
            inner_query = {
                "term": {
                    "small_files_mappings.original_name.keyword": target_name
                }
            }
        elif delete_mode == "ngram":
            inner_query = {
                "match_phrase": {
                    "small_files_mappings.original_name": target_name
                }
            }
        elif delete_mode == "wildcard":
            inner_query = {
                "wildcard": {
                    "small_files_mappings.original_name.wildcard": {
                        "value": target_name,
                        "case_insensitive": True
                    }
                }
            }
        else:
            raise ValueError(f"❌ 不支持的删除模式: {delete_mode}")

        # 2. Build the overall query body containing a nested query (with inner_hits to retrieve detailed information)
        #    Filter out deleted records (is_deleted=False); operate only on non-deleted files
        query_body = {
            "size": 10000,
            "query": {
                "nested": {
                    "path": "small_files_mappings",
                    "query": {
                        "bool": {
                            "must": [inner_query],
                            "filter": [{"term": {"small_files_mappings.is_deleted": False}}]
                        }
                    },
                    "inner_hits": {
                        "size": 10000
                    }
                }
            }
        }

        # 3. Execute the search and obtain the list of matched files
        print(f"\n🔍 正在以 [{delete_mode}] 模式搜索文件: {target_name}")
        try:
            res = self.es.search(index=Config.ES_INDEX, body=query_body)
        except Exception as e:
            print(f"❌ 查询发生错误: {e}")
            return

        results = []
        for hit in res['hits']['hits']:
            source = hit['_source']
            block_path = source.get('hdfs_path', 'N/A')
            block_size = source.get('block_size', 0)
            file_count = source.get('file_count', 0)
            parent_id = hit['_id']
            parent_index = hit['_index']

            inner_hits = hit.get("inner_hits", {}).get("small_files_mappings", {}).get("hits", {}).get("hits", [])
            for mapping_hit in inner_hits:
                mapping_source = mapping_hit.get("_source", {})
                results.append({
                    "file_info": {
                        "file_id": mapping_source.get('file_id'),
                        "original_name": mapping_source.get('original_name'),
                        "offset": mapping_source.get('offset'),
                        "length": mapping_source.get('length'),
                    },
                    "block_info": {
                        "hdfs_path": block_path,
                        "total_size_mb": round(block_size / 1024 / 1024, 2),
                        "total_files": file_count
                    },
                    "parent_id": parent_id,
                    "parent_index": parent_index
                })

        if not results:
            print(f"❌ 未找到匹配 '{target_name}' 的记录，无需删除。")
            return

        # 4. Display matched results for user confirmation
        print(f"✅ 找到 {len(results)} 个匹配的文件，准备软删除:\n")
        for i, result in enumerate(results, 1):
            fi = result['file_info']
            bi = result['block_info']
            print(f"   {i}. 文件: {fi['original_name']} (ID: {fi['file_id']})")
            print(f"      所在数据块: {bi['hdfs_path']} (总大小: {bi['total_size_mb']} MB, 总文件数: {bi['total_files']})")
            print(f"      偏移量: {fi['offset']}, 长度: {fi['length']}")

        # 5. Confirm deletion (soft delete only)
        confirm = input(f"\n⚠️ 共 {len(results)} 个文件将被软删除（标记 is_deleted=true，可恢复），确认？(y/n): ").strip().lower()
        if confirm != 'y':
            print("⚠️ 已取消删除操作。")
            return

        # 6. Perform soft delete
        deleted_count = 0
        processed_parents = set()  # Deduplication: delete each parent document only once
        for result in results:
            file_info = result['file_info']
            parent_id = result['parent_id']
            parent_index = result['parent_index']
            try:
                # Soft delete: update the nested document's is_deleted field via a painless script
                script = {
                    "script": {
                        "lang": "painless",
                        "source": """
                            for (def i = 0; i < ctx._source.small_files_mappings.size(); i++) {
                                if (ctx._source.small_files_mappings[i].file_id == params.file_id) {
                                    ctx._source.small_files_mappings[i].is_deleted = true;
                                    break;
                                }
                            }
                        """,
                        "params": {"file_id": file_info['file_id']}
                    }
                }
                self.es.update(index=parent_index, id=parent_id, body=script)
                print(f"   ✅ 已软删除: {file_info['original_name']}")
                deleted_count += 1
            except Exception as e:
                print(f"   ❌ 删除文件 {file_info['original_name']} 失败: {e}")

        print(f"\n✅ 成功删除 {deleted_count} 条记录。")


if __name__ == "__main__":
    deleter = FileDeleter()

    # 1. Get user input
    file_to_delete = input("请输入要删除的文件名: ").strip()
    if not file_to_delete:
        print("⚠️ 文件名不能为空。")
    else:
        # 2. Select delete mode (consistent with file_search.py / file_download.py)
        print("请选择删除模式:")
        print(" 1. exact   (精确匹配，删除完全相同的文件名)")
        print(" 2. ngram   (模糊删除，删除包含关键字的记录)")
        print(" 3. wildcard (通配符删除，如 *.log)")

        mode_choice = input("请输入选项 (1/2/3，默认为1): ").strip()
        mode_map = {"1": "exact", "2": "ngram", "3": "wildcard"}
        delete_mode = mode_map.get(mode_choice, "exact")

        # 3. Execute soft delete
        deleter.delete_by_filename(file_to_delete, delete_mode)
