# es_index_manager.py
from elasticsearch import Elasticsearch
import json


class ESIndexManager:
    def __init__(self, es_url, user=None):
        """
        Initialize Elasticsearch client
        :param es_url: ES cluster address, e.g. http://192.168.31.113:9200
        :param user: ES operation user (reserved)
        """
        self.es = Elasticsearch([es_url])

    def show_index_info(self):
        """
        Retrieve size information and mappings (including fields and analyzers) for all indices
        """
        print("\n=== 索引大小 ===")
        indices_stats = self.es.cat.indices(
            format="json", h="index,pri.store.size,store.size", s="pri.store.size:desc"
        )
        for index in indices_stats:
            print(
                f"索引: {index['index']}, "
                f"主分片大小: {index['pri.store.size']}, "
                f"总大小: {index['store.size']}"
            )

        print("\n=== 所有索引的映射信息 ===")
        for index_info in indices_stats:
            index_name = index_info["index"]
            print(f"\n--- 索引: {index_name} ---")
            try:
                mapping_response = self.es.indices.get_mapping(index=index_name)
                mapping_dict = mapping_response.body
                print(json.dumps(mapping_dict, indent=2))
            except Exception as e:
                print(f"获取索引 {index_name} 的映射信息失败: {e}")

    def delete_index(self, index_name):
        """
        Delete the specified index
        :param index_name: name of the index to delete
        """
        if self.es.indices.exists(index=index_name):
            print(f"索引 {index_name} 已存在，正在删除...")
            self.es.indices.delete(index=index_name)
            print("删除成功。")
        else:
            print(f"索引 {index_name} 不存在")


if __name__ == "__main__":
    ES_URL = "http://192.168.31.113:9200"

    manager = ESIndexManager(ES_URL)

    # Select operation: 1-view index info  2-delete index
    choice = input("请选择操作 [1-查看索引信息, 2-删除索引]: ").strip()

    if choice == "1":
        manager.show_index_info()
    elif choice == "2":
        index_name = input("请输入要删除的索引名称: ")
        manager.delete_index(index_name)
    else:
        print("无效选择，操作已取消。")
