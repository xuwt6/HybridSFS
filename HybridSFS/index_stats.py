# index_stats.py
# Collect statistics on the ES index: block count, space utilization, total size, and undeleted file volume

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from config import Config


def get_index_stats():
    es = Elasticsearch(Config.ES_HOST)

    if not es.ping():
        print(f"❌ 无法连接 Elasticsearch: {Config.ES_HOST}")
        return

    index = Config.ES_INDEX
    target_block = Config.TARGET_SIZE_BYTES  # 128 MiB

    # Check whether the index exists
    if not es.indices.exists(index=index):
        print(f"❌ 索引 {index} 不存在")
        return

    print(f"📊 开始统计索引: {index}")
    print(f"   ES 地址: {Config.ES_HOST}")
    print(f"   目标块大小: {target_block / 1048576:.0f} MiB")
    print()

    # Traverse all documents using scroll
    block_count = 0
    total_size_bytes = 0          # Total size of all blocks (including deleted data)
    undeleted_size_bytes = 0      # Total size of files where is_deleted=False

    query = {
        "query": {"match_all": {}},
        "_source": ["block_size", "small_files_mappings"]
    }

    docs = scan(es, query=query, index=index, scroll='5m', size=100)

    for doc in docs:
        source = doc['_source']
        block_count += 1

        # Accumulate total block size
        block_size = source.get('block_size', 0)
        total_size_bytes += block_size

        # Iterate over small_files_mappings and accumulate file sizes where is_deleted=False
        mappings = source.get('small_files_mappings', [])
        for m in mappings:
            if not m.get('is_deleted', False):
                undeleted_size_bytes += m.get('length', 0)

        # Output progress every 100 blocks
        if block_count % 100 == 0:
            print(f"   已扫描 {block_count} 个块...")

    # Calculate space utilization
    capacity_bytes = block_count * target_block
    utilization = (undeleted_size_bytes / capacity_bytes * 100) if capacity_bytes > 0 else 0

    # Output results
    print()
    print("=" * 60)
    print(f"📦 块数量:          {block_count} 个")
    print(f"💾 总大小:          {total_size_bytes} 字节 "
          f"({total_size_bytes / 1073741824:.2f} GiB)")
    print(f"✅ 未删除文件规模:  {undeleted_size_bytes} 字节 "
          f"({undeleted_size_bytes / 1073741824:.2f} GiB)")
    print(f"📐 理论容量:        {capacity_bytes} 字节 "
          f"({capacity_bytes / 1073741824:.2f} GiB)")
    print(f"📊 空间使用率:      {utilization:.2f}%")
    print(f"   (未删除文件总和 / (块数 × 128MiB))")
    print("=" * 60)


if __name__ == "__main__":
    get_index_stats()
