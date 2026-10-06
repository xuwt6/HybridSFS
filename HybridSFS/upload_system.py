# upload_system.py
import os
import time
import uuid
from hdfs import InsecureClient
from elasticsearch import Elasticsearch
from ffd_engine import FFDPacker
from config import Config
import threading

# [Core checkpoint] Ensure the class name is UploadSystem and is written at the top level (no indentation)
class UploadSystem:
    def __init__(self):
        self.candidate_pool = []
        self.pool_lock = threading.Lock()
        self.stream_finished = False
        self.hdfs = InsecureClient(Config.HDFS_URL, user=Config.HDFS_USER)
        self.es = Elasticsearch([Config.ES_HOST])
        if not self.es.indices.exists(index=Config.ES_INDEX):
            self.es.indices.create(index=Config.ES_INDEX)
        self.total_uploaded_bytes = 0
        self.total_upload_time = 0.0

    def stream_injector(self):
        print("[注入器] 开始扫描本地目录...")
        for root, _, files in os.walk(Config.LOCAL_DIR):
            batch = []
            for f in files:
                path = os.path.join(root, f)
                batch.append({
                    "id": str(uuid.uuid4()),
                    "path": path,
                    "name": f,
                    "size": os.path.getsize(path)
                })
                if len(batch) >= Config.STREAM_BATCH_SIZE:
                    with self.pool_lock:
                        self.candidate_pool.extend(batch)
                    batch = []

            if batch:
                with self.pool_lock:
                    self.candidate_pool.extend(batch)

        self.stream_finished = True
        print("[注入器] 所有文件扫描完毕。")

    def process_cycle(self):
        avg_size_history = []
        HISTORY_SIZE = 1000

        while not self.stream_finished or self.candidate_pool:
            current_batch = None
            with self.pool_lock:
                current_pool_size = len(self.candidate_pool)
                current_pool_bytes = sum(f['size'] for f in self.candidate_pool)

                should_trigger = False
                if self.stream_finished:
                    if current_pool_bytes > 0:
                        should_trigger = True
                else:
                    # Trigger watermark aligned to target block size: the pool must accumulate at least one full block (TARGET_SIZE_BYTES)
                    # of data before packing starts. Otherwise FFD can only assemble from candidates under 128 MiB, resulting in undersized blocks
                    # (e.g. the first block being only 66 MiB). The fallback trigger after stream end is in the stream_finished branch above.
                    if current_pool_bytes >= Config.TARGET_SIZE_BYTES:
                        should_trigger = True

                if should_trigger:
                    if current_pool_size == 0:
                        time.sleep(0.5)
                        continue

                    sample_batch = self.candidate_pool[:min(500, current_pool_size)]
                    for f in sample_batch:
                        if len(avg_size_history) >= HISTORY_SIZE:
                            avg_size_history.pop(0)
                        avg_size_history.append(f['size'])

                    take_count = current_pool_size
                    current_batch = self.candidate_pool[:take_count]
                    self.candidate_pool = self.candidate_pool[take_count:]

                    print(f"[调度器] 获取到 {len(current_batch)} 个候选文件 (总大小: {current_pool_bytes/1024/1024:.2f} MiB)，启动快速打包...")
                else:
                    time.sleep(0.1)
                    continue

            if current_batch:
                packer = FFDPacker(current_batch)
                optimal_files = packer.evolve()

                if not optimal_files:
                    remaining_files = current_batch
                else:
                    selected_ids = {f['id'] for f in optimal_files}
                    remaining_files = [f for f in current_batch if f['id'] not in selected_ids]

                if self.stream_finished and remaining_files:
                    current_block_size = sum(f['size'] for f in optimal_files)
                    remaining_size = sum(f['size'] for f in remaining_files)
                    if current_block_size + remaining_size <= Config.TARGET_SIZE_BYTES + Config.TOLERANCE_BYTES:
                        print(f"[强制合并] 流已结束，合并剩余 {len(remaining_files)} 个文件到当前 Block")
                        optimal_files.extend(remaining_files)
                        remaining_files = []

                with self.pool_lock:
                    self.candidate_pool.extend(remaining_files)

                self._merge_and_upload(optimal_files)
                time.sleep(0.01)

    def _merge_and_upload(self, files):
        if not files:
            return

        merged_data = bytearray()
        mappings = []
        offset = 0
        current_time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))

        for f in files:
            try:
                with open(f['path'], 'rb') as fh:
                    data = fh.read()
                    merged_data.extend(data)

                    mappings.append({
                        "file_id": f['id'],
                        "length": len(data),
                        "offset": offset,
                        "original_name": f['name'],
                        "timestamp_smallfiles": current_time_str,
                        "is_deleted": False
                    })
                    offset += len(data)

            except Exception as e:
                print(f"[错误] 读取文件失败: {f['path']}, {e}")

        if not merged_data:
            return

        hdfs_filename = f"BLOCK_{int(time.time())}_{uuid.uuid4().hex[:8]}.bin"
        hdfs_path = f"{Config.HDFS_BASE_PATH}/{hdfs_filename}"
        upload_start = time.time()
        try:
            with self.hdfs.write(hdfs_path, overwrite=True) as writer:
                writer.write(bytes(merged_data))

            doc = {
                "hdfs_path": hdfs_path,
                "block_size": len(merged_data),
                "file_count": len(files),
                "timestamp_largefile": current_time_str,
                "small_files_mappings": mappings
            }

            self.es.index(index=Config.ES_INDEX, document=doc, refresh='wait_for')

            upload_elapsed = time.time() - upload_start
            hours = int(upload_elapsed // 3600)
            minutes = int((upload_elapsed % 3600) // 60)
            seconds = upload_elapsed % 60
            time_str = f"{hours}时{minutes}分{seconds:.2f}秒"

            self.total_uploaded_bytes += len(merged_data)
            self.total_upload_time += upload_elapsed
            if self.total_upload_time > 0:
                rate = (self.total_uploaded_bytes / 1048576) / self.total_upload_time
            else:
                rate = 0.0

            print(f"[成功] 块 {hdfs_filename} 已上传，包含 {len(files)} 个文件，大小 {len(merged_data) / 1048576:.2f} MiB，耗时 {time_str}。")
            print(f"[统计] 累计上传 {self.total_uploaded_bytes / 1048576:.2f} MiB，总耗时 {self.total_upload_time:.2f} 秒，平均速率 {rate:.2f} MiB/秒。")
        except Exception as e:
            print(f"[严重错误] 上传或索引失败: {e}")