# hdfs_compaction.py
import os
import time
import uuid
import shutil
import threading
from hdfs import InsecureClient
from elasticsearch import Elasticsearch
from config import Config
from ffd_engine import FFDPacker
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def _validate_compaction_config():
    """Validate Compaction configuration parameters; auto-correct and warn when out of safe range"""
    threshold = Config.COMPACTION_UTILIZATION_THRESHOLD
    min_count = Config.COMPACTION_MIN_BLOCK_COUNT

    if threshold > 0.5:
        logger.warning(f"⚠️ COMPACTION_UTILIZATION_THRESHOLD={threshold} 超过上限 0.5，已自动修正为 0.5")
        Config.COMPACTION_UTILIZATION_THRESHOLD = 0.5
    elif threshold <= 0:
        logger.warning(f"⚠️ COMPACTION_UTILIZATION_THRESHOLD={threshold} 无效，已自动修正为 0.4")
        Config.COMPACTION_UTILIZATION_THRESHOLD = 0.4

    if min_count < 2:
        logger.warning(f"⚠️ COMPACTION_MIN_BLOCK_COUNT={min_count} 低于下限 2，已自动修正为 2")
        Config.COMPACTION_MIN_BLOCK_COUNT = 2


class HDFSCompactor:
    def __init__(self, upload_system=None):
        """
        :param upload_system: UploadSystem instance for re-injecting downloaded files into the candidate pool
                              If None, only download to local without automatic re-upload
        """
        _validate_compaction_config()
        self.hdfs_client = InsecureClient(Config.HDFS_URL, user=Config.HDFS_USER)
        self.es = Elasticsearch([Config.ES_HOST])
        if not self.es.ping():
            raise ValueError("❌ 无法连接到 Elasticsearch")
        self.upload_system = upload_system
        self.temp_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "compaction_temp")

    def _calculate_utilization(self, doc_source):
        """Calculate the utilization of a single block: valid file size / 128 MiB"""
        mappings = doc_source.get('small_files_mappings', [])
        active_size = sum(
            m.get('length', 0)
            for m in mappings
            if not m.get('is_deleted', False)
        )
        return active_size / Config.TARGET_SIZE_BYTES

    def _get_compaction_candidates(self):
        """Filter low-utilization blocks from ES and return the candidate list"""
        threshold = Config.COMPACTION_UTILIZATION_THRESHOLD
        min_count = Config.COMPACTION_MIN_BLOCK_COUNT

        logger.info(f"📊 扫描 ES 索引，查找利用率 < {threshold:.0%} 的块...")

        query = {
            "query": {"match_all": {}},
            "size": 10000
        }
        result = self.es.search(index=Config.ES_INDEX, body=query)
        hits = result['hits']['hits']

        candidates = []
        for doc in hits:
            utilization = self._calculate_utilization(doc['_source'])
            if utilization < threshold:
                candidates.append({
                    '_id': doc['_id'],
                    '_source': doc['_source'],
                    'utilization': utilization
                })

        # Sort by utilization from low to high, prioritizing merging the emptiest blocks
        candidates.sort(key=lambda x: x['utilization'])

        logger.info(f"📊 发现 {len(candidates)} 个低利用率块（阈值 {threshold:.0%}）")
        for c in candidates:
            logger.info(f"   块 {c['_source']['hdfs_path']}: 利用率 {c['utilization']:.1%}")

        if len(candidates) < min_count:
            logger.info(f"ℹ️ 低利用率块数量 {len(candidates)} 不足触发阈值 {min_count}，本次不执行合并。")
            return []

        return candidates

    def start_compaction(self):
        """Start defragmentation: download valid files → delete old blocks → re-upload"""
        logger.info("🚀 开始执行 HDFS 碎片整理 (下载→清理→重新上传模式)...")

        # 1. Get candidate blocks
        candidates = self._get_compaction_candidates()
        if not candidates:
            logger.info("ℹ️ 未发现需要合并的块，系统当前状态健康。")
            return

        # 2. Prepare temporary directory
        os.makedirs(self.temp_dir, exist_ok=True)

        # 3. Download valid files from each block to local, recording success/failure status per block
        download_results = []  # [{'doc': doc, 'success': True/False, 'count': N, 'bytes': N}]
        for doc in candidates:
            source = doc['_source']
            old_hdfs_path = source['hdfs_path']
            success, downloaded_count, downloaded_bytes = self._download_valid_files(old_hdfs_path, source['small_files_mappings'])
            download_results.append({
                'doc': doc,
                'success': success,
                'count': downloaded_count,
                'bytes': downloaded_bytes
            })
            if success:
                logger.info(f"✅ 块 {old_hdfs_path} 下载成功: {downloaded_count} 个文件, {downloaded_bytes / 1048576:.2f} MiB")
            else:
                logger.error(f"❌ 块 {old_hdfs_path} 下载失败，将跳过删除以保护数据")

        # 4. Compile statistics
        successful_downloads = [r for r in download_results if r['success']]
        failed_downloads = [r for r in download_results if not r['success']]
        total_valid_files = sum(r['count'] for r in successful_downloads)
        total_valid_bytes = sum(r['bytes'] for r in successful_downloads)

        logger.info(f"📥 下载结果: 成功 {len(successful_downloads)}/{len(candidates)} 个块, "
                    f"共 {total_valid_files} 个有效文件, {total_valid_bytes / 1048576:.2f} MiB")

        # 5. Safety check: abort if all downloads failed
        if not successful_downloads:
            logger.error("🛑 所有块下载均失败，中止合并流程以保护数据安全。")
            logger.error("   请检查 HDFS 连接和网络状态后重试。")
            return

        if failed_downloads:
            logger.warning(f"⚠️ {len(failed_downloads)} 个块下载失败，仅处理成功下载的 {len(successful_downloads)} 个块。")

        # 6. Only delete old blocks that were successfully downloaded
        self._cleanup_old_blocks([r['doc'] for r in successful_downloads])

        # 7. Re-upload valid files
        upload_success, upload_failed = self._reinject_files()
        
        logger.info(f"✅ 碎片整理完成: 重新上传 {upload_success} 个文件, 失败 {upload_failed} 个")

    def _download_valid_files(self, hdfs_path, mappings):
        """Extract valid files from HDFS blocks and save to local temporary directory; returns (success, file count, byte count)"""
        downloaded_count = 0
        downloaded_bytes = 0
        success = False

        # Sort by offset and read sequentially
        sorted_mappings = sorted(mappings, key=lambda m: m['offset'])

        try:
            # Download the entire block to a temporary file
            temp_block_path = os.path.join(self.temp_dir, f"temp_block_{uuid.uuid4().hex[:8]}.bin")
            with self.hdfs_client.read(hdfs_path) as reader:
                with open(temp_block_path, 'wb') as temp_file:
                    for chunk in reader:
                        temp_file.write(chunk)

            # Extract valid files from the temporary file by offset
            with open(temp_block_path, 'rb') as block_file:
                for m in sorted_mappings:
                    # Skip deleted files
                    if m.get('is_deleted', False):
                        continue

                    file_length = m['length']
                    original_name = m.get('original_name', m['file_id'])

                    # Seek to the start position of this file
                    block_file.seek(m['offset'])
                    file_data = block_file.read(file_length)

                    # Write to local temporary directory
                    local_path = os.path.join(self.temp_dir, original_name)
                    with open(local_path, 'wb') as f:
                        f.write(file_data)

                    downloaded_count += 1
                    downloaded_bytes += file_length

            # Delete temporary block file
            os.remove(temp_block_path)
            success = True

        except Exception as e:
            logger.error(f"❌ 下载块 {hdfs_path} 失败: {e}")

        return success, downloaded_count, downloaded_bytes

    def _cleanup_old_blocks(self, candidates):
        """Delete ES documents and HDFS files of old blocks"""
        # Delete ES document
        for doc in candidates:
            try:
                self.es.delete(index=Config.ES_INDEX, id=doc['_id'])
            except Exception as e:
                logger.warning(f"⚠️ 删除 ES 文档失败 {doc['_id']}: {e}")

        logger.info("✅ ES 旧文档删除完成。")

        # Delete HDFS file
        for doc in candidates:
            try:
                self.hdfs_client.delete(doc['_source']['hdfs_path'])
            except Exception as e:
                logger.warning(f"⚠️ 删除 HDFS 旧块失败 {doc['_source']['hdfs_path']}: {e}")

        logger.info("🧹 HDFS 旧文件清理完成。")

    def _reinject_files(self):
        """
        Scan the temporary directory and, following upload_system.py, pack multiple small files into
        blocks close to TARGET_SIZE_BYTES (128 MiB) before uploading, instead of uploading individually.
        Returns (number of successfully uploaded blocks, failure count)
        """
        logger.info("📤 开始重新打包并上传有效文件...")

        # 1. Scan the temporary directory and read all valid files
        file_entries = []
        for filename in os.listdir(self.temp_dir):
            local_path = os.path.join(self.temp_dir, filename)
            if not os.path.isfile(local_path):
                continue
            try:
                with open(local_path, 'rb') as fh:
                    data = fh.read()
                file_entries.append({
                    "id": str(uuid.uuid4()),
                    "name": filename,
                    "data": data,
                    "size": len(data)
                })
            except Exception as e:
                logger.error(f"❌ 读取临时文件失败 {filename}: {e}")

        if not file_entries:
            logger.warning("⚠️ 临时目录中没有有效文件可上传。")
            return 0, 0

        logger.info(f"📋 共读取 {len(file_entries)} 个有效文件，"
                    f"合计 {sum(f['size'] for f in file_entries) / 1048576:.2f} MiB")

        # 2. Use FFDPacker for optimized grouping
        blocks = []
        remaining_files = file_entries.copy()

        while remaining_files:
            packer = FFDPacker(remaining_files)
            optimal_files = packer.evolve()

            if not optimal_files:
                # If FFDPacker returns empty, the remaining files are too small; force merge
                if remaining_files:
                    blocks.append(remaining_files)
                break

            blocks.append(optimal_files)

            # Remove already-assigned files from the remaining files
            optimal_ids = {f['id'] for f in optimal_files}
            remaining_files = [f for f in remaining_files if f['id'] not in optimal_ids]

        logger.info(f"📦 使用 FFDPacker 优化后分为 {len(blocks)} 个块")

        # 3. For each BLOCK: merge data, upload to HDFS, and write to ES
        block_success = 0
        block_failed = 0

        for i, block_files in enumerate(blocks):
            try:
                merged_data = bytearray()
                mappings = []
                offset = 0
                current_time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))

                for entry in block_files:
                    merged_data.extend(entry['data'])
                    mappings.append({
                        "file_id": entry['id'],
                        "length": entry['size'],
                        "offset": offset,
                        "original_name": entry['name'],
                        "timestamp_smallfiles": current_time_str,
                        "is_deleted": False
                    })
                    offset += entry['size']

                # Upload to HDFS
                hdfs_filename = f"COMPACTED_{int(time.time())}_{uuid.uuid4().hex[:8]}.bin"
                hdfs_path = f"{Config.HDFS_BASE_PATH}/{hdfs_filename}"

                upload_start = time.time()
                with self.hdfs_client.write(hdfs_path, overwrite=True) as writer:
                    writer.write(bytes(merged_data))

                # Write ES document
                doc = {
                    "hdfs_path": hdfs_path,
                    "block_size": len(merged_data),
                    "file_count": len(block_files),
                    "timestamp_largefile": current_time_str,
                    "small_files_mappings": mappings
                }
                self.es.index(index=Config.ES_INDEX, document=doc, refresh='wait_for')

                upload_elapsed = time.time() - upload_start
                hours = int(upload_elapsed // 3600)
                minutes = int((upload_elapsed % 3600) // 60)
                seconds = upload_elapsed % 60
                time_str = f"{hours}时{minutes}分{seconds:.2f}秒"

                logger.info(
                    f"✅ 块 {i+1}/{len(blocks)} {hdfs_filename} 已上传，"
                    f"包含 {len(block_files)} 个文件，"
                    f"大小 {len(merged_data) / 1048576:.2f} MiB，"
                    f"耗时 {time_str}"
                )
                block_success += 1

            except Exception as e:
                logger.error(f"❌ 块 {i+1}/{len(blocks)} 上传失败: {e}")
                block_failed += 1

        logger.info(f"📊 重新打包上传完成: 成功 {block_success} 个块, 失败 {block_failed} 个块")

        # 4. Clean up temporary directory
        try:
            shutil.rmtree(self.temp_dir)
            logger.info("🧹 临时目录清理完成。")
        except Exception as e:
            logger.warning(f"⚠️ 清理临时目录失败: {e}")

        return block_success, block_failed


# Standalone entry point
if __name__ == "__main__":
    compactor = HDFSCompactor()
    compactor.start_compaction()
