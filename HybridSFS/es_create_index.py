# es_create_index.py
from elasticsearch import Elasticsearch
import json

def create_file_merge_index():
    # 1. Connect to Elasticsearch
    es = Elasticsearch(["http://192.168.31.113:9200"])

    index_name = "small_files_mapping_large_files"

    # 2. If the index already exists, delete it first (⚠️ Warning: this clears all data; use caution in production)
    if es.indices.exists(index=index_name):
        print(f"⚠️ 索引 {index_name} 已存在，正在删除...")
        es.indices.delete(index=index_name)
        print("✅ 旧索引删除成功。")

    # 3. Define the index Settings and Mappings
    index_body = {
        "settings": {
            # --- Set shard and replica counts here ---
            "number_of_shards": 2,   # Number of primary shards
            "number_of_replicas": 1, # Number of replica shards (0 means no replicas)
            # --------------------------------
            "index": {
                # Allow Ngram max_gram and min_gram difference up to 10
                "max_ngram_diff": 10,
                "max_inner_result_window": 10000,
                # Maximum nested sub-documents allowed per document (default 10000)
                "mapping": {
                    "nested_objects": {
                        "limit": 50000
                    }
                }
            },
            "analysis": {
                "tokenizer": {
                    "my_ngram_tokenizer": {
                        "type": "ngram",
                        "min_gram": 2,
                        "max_gram": 8,
                        "token_chars": ["letter", "digit", "punctuation", "symbol"]
                    }
                },
                "analyzer": {
                    # Ngram analyzer used at index time
                    "ngram_index_analyzer": {
                        "type": "custom",
                        "tokenizer": "my_ngram_tokenizer"
                    },
                    # Analyzer used at search time (prevents search terms from being tokenized)
                    "standard_search_analyzer": {
                        "type": "custom",
                        "tokenizer": "whitespace"
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                # --- Large file (parent document) base fields ---
                "block_size": {
                    "type": "integer"
                },
                "file_count": {
                    "type": "integer"
                },
                "timestamp_largefile": {
                    "type": "date",
                    "format": "yyyy-MM-dd HH:mm:ss"
                },
                "hdfs_path": {
                    "type": "keyword"
                },

                # --- Small file mapping (nested document) fields ---
                "small_files_mappings": {
                    "type": "nested",
                    "properties": {
                        "file_id": {
                            "type": "keyword"
                        },
                        "length": {
                            "type": "integer"
                        },
                        "offset": {
                            "type": "integer"
                        },
                        "timestamp_smallfiles": {
                            "type": "date",
                            "format": "yyyy-MM-dd HH:mm:ss"
                        },

                        # --- Core field: file name ---
                        "original_name": {
                            "type": "text",
                            "analyzer": "ngram_index_analyzer",
                            "search_analyzer": "standard_search_analyzer",
                            "fields": {
                                "keyword": {
                                    "type": "keyword"
                                },
                                "wildcard": {
                                    "type": "wildcard"
                                }
                            }
                        },

                        # --- New field: deletion flag ---
                        "is_deleted": {
                            "type": "boolean"
                        }
                    }
                }
            }
        }
    }

    # 4. Create the index
    try:
        response = es.indices.create(index=index_name, body=index_body)
        print(f"🚀 索引 {index_name} 创建成功！")
        print(json.dumps(response.body, indent=2, ensure_ascii=False))
    except Exception as e:
        print(f"❌ 创建索引失败: {e}")

if __name__ == "__main__":
    create_file_merge_index()
