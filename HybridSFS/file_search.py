# file_search.py
from elasticsearch import Elasticsearch
from config import Config
import json
import time

class FileSearcher:
    def __init__(self):
        """Initialize Elasticsearch client"""
        self.es = Elasticsearch([Config.ES_HOST])
        if not self.es.ping():
            raise ValueError("❌ 无法连接到 Elasticsearch，请检查服务是否启动。")

    def search_by_filename(self, target_name, search_mode="exact"):
        """
        Search in the Elasticsearch index by file name
        :param target_name: file name or keyword to search for
        :param search_mode: search mode
            - "exact": exact match (using .keyword)
            - "ngram": fuzzy search (using ngram tokenizer)
            - "wildcard": wildcard search (using .wildcard)
        """
        # 1. Dynamically build the query body based on search mode
        if search_mode == "exact":
            inner_query = {
                "term": {
                    "small_files_mappings.original_name.keyword": target_name
                }
            }
        elif search_mode == "ngram":
            inner_query = {
                "match_phrase": {
                    "small_files_mappings.original_name": target_name
                }
            }
        elif search_mode == "wildcard":
            inner_query = {
                "wildcard": {
                    "small_files_mappings.original_name.wildcard": {
                        "value": target_name,
                        "case_insensitive": True
                    }
                }
            }
        else:
            raise ValueError(f"❌ 不支持的搜索模式: {search_mode}")

        # 2. Build the overall query body containing a nested query
        # 2. Build the overall query body containing a nested query
        query_body = {
            "size": 10000,
            "query": {
                "nested": {
                    "path": "small_files_mappings",
                    "query": {
                        "bool": {
                            "must": [inner_query],  # Place your previously built exact/ngram/wildcard query here
                            "filter": [             # Filter out deleted files
                                {"term": {"small_files_mappings.is_deleted": False}}
                            ]
                        }
                    },
                    "inner_hits": {
                        "size": 10000  # ⚠️ Key change: force each parent document to return at most 100 matched small files, overriding the default limit of 3
                    }
                }
            }
        }

        try:
            start_time = time.time()
            res = self.es.search(index=Config.ES_INDEX, body=query_body)
            results = []
            end_time = time.time()
            elapsed_ms = round((end_time - start_time) * 1000, 2) # Convert to milliseconds and keep two decimal places
            print(f"\n⏱️ 查询耗时: {elapsed_ms} ms")

            for hit in res['hits']['hits']:
                source = hit['_source']
                block_path = source.get('hdfs_path', 'N/A')
                block_size = source.get('block_size', 0)
                file_count = source.get('file_count', 0)

                # Extract the actually matched nested objects from inner_hits
                inner_hits = hit.get("inner_hits", {})
                # Fix: the key of inner_hits should be the nested path name
                mappings_hits = inner_hits.get("small_files_mappings", {}).get("hits", {}).get("hits", [])

                for mapping_hit in mappings_hits:
                    mapping_source = mapping_hit.get("_source", {})
                    # Combine each matched file's info with its containing data block info
                    results.append({
                        "file_info": {
                            "file_id": mapping_source.get('file_id'),
                            "original_name": mapping_source.get('original_name'),
                            "offset": mapping_source.get('offset'),
                            "length": mapping_source.get('length'),
                            "timestamp": mapping_source.get('timestamp_smallfiles') # Extract the timestamp of the small file
                        },
                        "block_info": {
                            "hdfs_path": block_path,
                            "total_size_mb": round(block_size / 1024 / 1024, 2),
                            "total_files": file_count
                        }
                    })

            return results

        except Exception as e:
            print(f"❌ 查询发生错误: {e}")
            return []

if __name__ == "__main__":
    searcher = FileSearcher()

    # 1. Get user input
    file_to_search = input("请输入要查询的文件名: ").strip()
    if not file_to_search:
        print("⚠️ 文件名不能为空。")
    else:
        # 2. Let the user choose the search mode
        print("请选择搜索模式:")
        print(" 1. exact (精确匹配)")
        print(" 2. ngram (模糊搜索)")
        print(" 3. wildcard (通配符搜索，如 2007_*.jpg)")
        mode_choice = input("请输入选项 (1/2/3，默认为1): ").strip()

        mode_map = {"1": "exact", "2": "ngram", "3": "wildcard"}
        search_mode = mode_map.get(mode_choice, "exact")

        print(f"\n🔍 正在以 [{search_mode}] 模式搜索文件: {file_to_search}")

        # 3. Execute search and format output
        results = searcher.search_by_filename(file_to_search, search_mode)

        if results:
            print(f"\n✅ 找到 {len(results)} 个匹配的文件:\n")
            for i, result in enumerate(results, 1):
                file_info = result['file_info']
                block_info = result['block_info']
                print(f"--- 匹配文件 {i} ---")
                print(f" 文件名: {file_info['original_name']}")
                print(f" 文件ID: {file_info['file_id']}")
                print(f" 所在数据块: {block_info['hdfs_path']}")
                print(f" 文件时间: {file_info['timestamp']}")
                print(f" 偏移量: {file_info['offset']} 字节")
                print(f" 长  度: {file_info['length']} 字节")
                print("-" * 30)
        else:
            print(f"❌ 未找到包含 '{file_to_search}' 的记录。")