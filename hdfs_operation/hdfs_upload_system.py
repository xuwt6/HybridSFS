# hdfs_upload_system.py
# HDFS-only baseline addition system (most primitive approach):
# Upload local small files one by one; 1 small file occupies 1 block, no packing, no merging, no Elasticsearch.
# HDFS directory mirrors the local relative directory; file names are preserved; streaming scan-and-upload;
# Timing: measure total elapsed time once for the entire upload code section, not accumulated per file.
import os
import sys
import time

# Reuse configuration from the parent directory smallfiles_heuristic to ensure "all configurations are identical"
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from hdfs import InsecureClient
from config import Config

CHUNK_SIZE = 8 * 1024 * 1024  # 8 MiB chunk streaming write


def run():
    hdfs = InsecureClient(Config.HDFS_URL, user=Config.HDFS_USER)

    total_bytes = 0
    file_count = 0

    print("开始扫描本地目录并逐个上传...")

    # Measure time only once for the entire upload code section
    upload_start = time.time()
    for root, _, files in os.walk(Config.LOCAL_DIR):
        for name in files:
            local_path = os.path.join(root, name)

            # HDFS directory mirrors the local relative directory; file names are preserved
            rel_path = os.path.relpath(local_path, Config.LOCAL_DIR).replace(os.sep, "/")
            hdfs_path = f"{Config.HDFS_BASE_PATH}/{rel_path}"

            try:
                # Streaming read/write: read locally while writing to HDFS, with 1 file exclusively occupying 1 block
                with open(local_path, 'rb') as fh, hdfs.write(hdfs_path, overwrite=True) as writer:
                    while True:
                        chunk = fh.read(CHUNK_SIZE)
                        if not chunk:
                            break
                        writer.write(chunk)
            except Exception as e:
                print(f"[严重错误] 上传失败: {local_path}, {e}")
                continue

            file_size = os.path.getsize(local_path)
            total_bytes += file_size
            file_count += 1
            if file_count % 30 ==0 or file_count == 1:
                print(f"[成功上传]第 {file_count} 个文件 {name} 已上传（独占 1 block），大小 {file_size / 1048576:.4f} MiB。")
    upload_elapsed = time.time() - upload_start

    hours = int(upload_elapsed // 3600)
    minutes = int((upload_elapsed % 3600) // 60)
    seconds = upload_elapsed % 60
    time_str = f"{hours}时{minutes}分{seconds:.2f}秒"
    rate = (total_bytes / 1048576) / upload_elapsed if upload_elapsed > 0 else 0.0
    print(f"[总体统计] 共上传 {file_count} 个文件、{total_bytes / 1048576:.2f} MiB，合计{upload_elapsed}秒，上传耗时 {time_str}，平均速率 {rate:.2f} MiB/秒。")
#     19113.44 seconds  0.04 pet


if __name__ == "__main__":
    run()
