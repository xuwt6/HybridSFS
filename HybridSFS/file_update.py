import os
import time
import glob
import uuid
from elasticsearch import Elasticsearch
from hdfs import InsecureClient
from config import Config

# ================= Configuration =================
CONFIG = {
    # Path of the folder containing the files to update
    "folder_path": r"E:\merge_small_files_for_memory\datatest\pet-update",
    "dry_run": False,                   # True: preview only, do not execute; False: actually execute
    "es_refresh_wait": 2,               # Seconds to wait for refresh after soft delete
}

# ES index name (consistent with upload_system.py, using the configuration from Config)
ES_INDEX = Config.ES_INDEX


class BatchFileUpdater:
    def __init__(self, config):
        self.config = config
        self.folder_path = config["folder_path"]
        self.dry_run = config["dry_run"]

        # 1. Initialize the ES client (for soft deletion and writing new documents)
        self.es = Elasticsearch([Config.ES_HOST])
        if not self.es.ping():
            raise ConnectionError(f"❌ 无法连接到 Elasticsearch ({Config.ES_HOST})")
        print("✅ Elasticsearch 连接验证通过。")

        # 2. Initialize the HDFS client (consistent with __init__ in upload_system.py)
        self.hdfs = InsecureClient(Config.HDFS_URL, user=Config.HDFS_USER)
        print("✅ HDFS 客户端连接验证通过。")

    # ================= Utility Methods =================

    def get_files_to_update(self):
        """Get all files to update under the folder"""
        if not os.path.exists(self.folder_path):
            raise FileNotFoundError(f"❌ 文件夹不存在: {self.folder_path}")
        files = [f for f in glob.glob(os.path.join(self.folder_path, "*")) if os.path.isfile(f)]
        if not files:
            print("⚠️ [警告] 文件夹中没有找到任何文件。")
        return files

    # ================= Phase 1: Soft-delete old records =================

    def soft_delete_old_records(self, filenames):
        """
        Phase 1: Mark same-named old records as is_deleted = true in ES
        Use the same file_id-based exact soft deletion as in file_delete.py
        """
        print(f"\n=== 阶段一：软删除旧记录 (共 {len(filenames)} 个文件) ===")
        deleted_count, failed_count = 0, 0

        for filename in filenames:
            # 1. First search for matching records (consistent with the search logic in file_delete.py)
            query = {
                "size": 10000,
                "query": {
                    "nested": {
                        "path": "small_files_mappings",
                        "query": {
                            "bool": {
                                "must": [{
                                    "term": {"small_files_mappings.original_name.keyword": filename}
                                }],
                                "filter": [{
                                    "term": {"small_files_mappings.is_deleted": False}
                                }]
                            }
                        },
                        "inner_hits": {
                            "size": 10000
                        }
                    }
                }
            }

            if self.dry_run:
                print(f"🔍 [预览] 将软删除旧记录: {filename}")
                deleted_count += 1
                continue

            try:
                res = self.es.search(index=ES_INDEX, body=query)

                for hit in res['hits']['hits']:
                    parent_id = hit['_id']
                    parent_index = hit['_index']

                    # Get the matched file info from inner_hits
                    inner_hits = hit.get("inner_hits", {}).get("small_files_mappings", {}).get("hits", {}).get("hits", [])

                    for mapping_hit in inner_hits:
                        mapping_source = mapping_hit.get("_source", {})
                        file_id = mapping_source.get('file_id')

                        # Soft-delete via exact match on file_id (identical to file_delete.py)
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
                                "params": {"file_id": file_id}
                            }
                        }
                        self.es.update(index=parent_index, id=parent_id, body=script)
                        print(f"✅ [执行] 软删除 {filename} (file_id: {file_id}) -> 父文档: {parent_id[:8]}...")
                        deleted_count += 1

            except Exception as e:
                print(f"❌ [错误] 软删除 {filename} 失败: {e}")
                failed_count += 1

        if not self.dry_run and deleted_count > 0:
            print(f"⏳ 等待 {self.config['es_refresh_wait']} 秒让 ES 刷新生效...")
            time.sleep(self.config['es_refresh_wait'])

        return deleted_count, failed_count

    # ================= Phase Two: Re-upload New Files =================

    def upload_new_files(self, file_paths):
        """
        Phase 2: Pack files using FFDPacker optimization, write to HDFS, and build ES documents
        Following upload_system.py, use a greedy-algorithm-optimized bin-packing strategy
        """
        print(f"\n=== 阶段二：重新上传新文件 (共 {len(file_paths)} 个) ===")

        if self.dry_run:
            for file_path in file_paths:
                filename = os.path.basename(file_path)
                print(f"🔍 [预览] 将上传新文件: {filename}")
            return len(file_paths), 0

        # 1. Read all files into memory
        file_entries = []
        read_failed = 0
        for file_path in file_paths:
            filename = os.path.basename(file_path)
            try:
                with open(file_path, 'rb') as fh:
                    data = fh.read()
                file_entries.append({
                    "id": str(uuid.uuid4()),
                    "name": filename,
                    "data": data,
                    "size": len(data),
                    "path": file_path
                })
            except Exception as e:
                print(f"❌ [错误] 读取文件失败 {filename}: {e}")
                read_failed += 1

        if not file_entries:
            print("⚠️ [警告] 没有有效文件可上传。")
            return 0, read_failed

        total_size = sum(f['size'] for f in file_entries)
        print(f"📋 共读取 {len(file_entries)} 个有效文件，合计 {total_size / 1048576:.2f} MiB")

        # 2. Use FFDPacker for optimized grouping
        from ffd_engine import FFDPacker
        
        blocks = []
        remaining_files = file_entries.copy()
        
        while remaining_files:
            packer = FFDPacker(remaining_files)
            optimal_files = packer.evolve()
            
            if not optimal_files:
                # If no optimization result is returned, the remaining files are too small; force merge
                if remaining_files:
                    blocks.append(remaining_files)
                break
            
            blocks.append(optimal_files)
            
            # Remove already-assigned files from the remaining files
            optimal_ids = {f['id'] for f in optimal_files}
            remaining_files = [f for f in remaining_files if f['id'] not in optimal_ids]

        # 3. Merge and upload each block
        blocks_count = len(blocks)
        print(f"📦 使用 FFDPacker 优化后分为 {blocks_count} 个块")

        upload_success = 0
        upload_failed = 0

        for block_idx, block_files in enumerate(blocks, 1):
            try:
                # Merge block data
                merged_data = bytearray()
                current_offset = 0
                small_files_mappings = []

                for file_info in block_files:
                    data = file_info['data']
                    merged_data.extend(data)
                    
                    small_files_mappings.append({
                        "file_id": file_info['id'],
                        "original_name": file_info['name'],
                        "offset": current_offset,
                        "length": file_info['size']
                    })
                    
                    current_offset += file_info['size']

                # Upload to HDFS
                block_filename = f"BLOCK_{int(time.time())}_{uuid.uuid4().hex[:8]}.bin"
                block_hdfs_path = f"{Config.HDFS_BASE_PATH}/{block_filename}"
                
                with self.hdfs.write(block_hdfs_path, overwrite=True) as writer:
                    writer.write(bytes(merged_data))

                # Write ES document
                doc = {
                    "hdfs_path": block_hdfs_path,
                    "block_size": len(merged_data),
                    "file_count": len(block_files),
                    "small_files_mappings": small_files_mappings
                }

                self.es.index(index=Config.ES_INDEX, body=doc)

                file_names = [f['name'] for f in block_files]
                print(f"✅ [执行] 块 {block_idx}/{blocks_count} 上传成功: {block_filename} "
                      f"(包含 {len(block_files)} 个文件, {len(merged_data) / 1048576:.2f} MiB)")
                print(f"   文件: {', '.join(file_names[:3])}" + (f" 等 {len(file_names)} 个" if len(file_names) > 3 else ""))
                
                upload_success += len(block_files)

            except Exception as e:
                block_names = [f['name'] for f in block_files]
                print(f"❌ [错误] 块 {block_idx}/{blocks_count} 上传失败 ({block_names}): {e}")
                upload_failed += len(block_files)

        print(f"\n📊 上传统计: 成功 {upload_success} 个, 失败 {upload_failed} 个, 读取失败 {read_failed} 个")
        
        return upload_success, upload_failed + read_failed

    # ================= Main Flow =================

    def run(self):
        """Run the batch update main flow"""
        print("=" * 60)
        print("🚀 批量文件更新工具 (软删除 + 重新上传)")
        print(f"📌 当前模式: {'🔍 预览模式 (DRY RUN)' if self.dry_run else '🚀 正式执行模式'}")
        print(f"📂 文件夹路径: {self.folder_path}")
        print("=" * 60)

        # Get the list of files to update
        file_paths = self.get_files_to_update()
        filenames = [os.path.basename(f) for f in file_paths]

        # Phase 1: Soft-delete old records
        del_success, del_failed = self.soft_delete_old_records(filenames)
        if del_failed > 0 and not self.dry_run:
            print("\n🛑 [终止] 阶段一存在失败项，为防止数据不一致，已终止阶段二！")
            return

        # Phase two: re-upload the new files
        up_success, up_failed = self.upload_new_files(file_paths)

        # Print final statistics
        print("\n" + "=" * 60)
        print("📊 执行统计摘要:")
        print(f"  文件总数: {len(file_paths)}")
        print(f"  软删除成功: {del_success} | 软删除失败: {del_failed}")
        print(f"  上传成功: {up_success} | 上传失败: {up_failed}")
        print("=" * 60)


if __name__ == "__main__":
    updater = BatchFileUpdater(CONFIG)
    updater.run()