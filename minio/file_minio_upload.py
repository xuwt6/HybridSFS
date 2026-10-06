# file_minio_upload.py
# MinIO baseline upload system (comparison algorithm):
# Upload local small files to MinIO one by one, without packaging, merging, or Elasticsearch.
# MinIO object paths mirror the local relative directory structure; original file names are preserved;
# Timing: measure total elapsed time once for the entire upload code section, not accumulated per file.
# Statistics are fully consistent with hdfs_upload_system.py for cross-comparison.

import os
import sys
import time

# Reuse the configuration from the parent directory smallfiles_heuristic
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from minio import Minio
from config import Config

# ============ MinIO Connection Configuration ============
MINIO_ENDPOINT = "192.168.31.111:9000"  # MinIO service address (IP:port, without http://)
MINIO_ACCESS_KEY = "minioadmin"          # Access key
MINIO_SECRET_KEY = "minioadmin"       # Secret key
MINIO_SECURE = False                     # Whether to use HTTPS
MINIO_BUCKET = "small-files-test"        # Target bucket name
MINIO_BASE_PREFIX = "raw_small_files"    # Object prefix (similar to HDFS BASE_PATH)
# =========================================

# 8 MiB multipart part size
PART_SIZE = 8 * 1024 * 1024


def ensure_bucket(client, bucket_name):
    """Ensure the bucket exists; create it if not"""
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"[初始化] 存储桶 {bucket_name} 已创建。")
    else:
        print(f"[初始化] 存储桶 {bucket_name} 已存在。")


def run():
    # Create MinIO client
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )

    ensure_bucket(client, MINIO_BUCKET)

    total_bytes = 0
    file_count = 0
    fail_count = 0

    print(f"开始扫描本地目录并逐个上传到 MinIO...")
    print(f"  源目录: {Config.LOCAL_DIR}")
    print(f"  目标: {MINIO_ENDPOINT} / {MINIO_BUCKET} / {MINIO_BASE_PREFIX}/")

    # Measure time only once for the entire upload code section (consistent with hdfs_upload_system.py)
    upload_start = time.time()

    for root, _, files in os.walk(Config.LOCAL_DIR):
        for name in files:
            local_path = os.path.join(root, name)

            # Object paths mirror the local relative directory structure; original file names are preserved
            rel_path = os.path.relpath(local_path, Config.LOCAL_DIR).replace(os.sep, "/")
            object_name = f"{MINIO_BASE_PREFIX}/{rel_path}"

            try:
                file_size = os.path.getsize(local_path)

                # Use put_object to upload; large files automatically use multipart
                client.fput_object(
                    bucket_name=MINIO_BUCKET,
                    object_name=object_name,
                    file_path=local_path,
                    part_size=PART_SIZE,
                )

                total_bytes += file_size
                file_count += 1

                if file_count % 30 == 0 or file_count == 1:
                    print(
                        f"[成功上传] 第 {file_count} 个文件 {name}，"
                        f"大小 {file_size / 1048576:.4f} MiB"
                    )

            except Exception as e:
                fail_count += 1
                print(f"[严重错误] 上传失败: {local_path}, {e}")
                continue

    upload_elapsed = time.time() - upload_start

    # ============ Statistics Output (format consistent with hdfs_upload_system.py) ============
    hours = int(upload_elapsed // 3600)
    minutes = int((upload_elapsed % 3600) // 60)
    seconds = upload_elapsed % 60
    time_str = f"{hours}时{minutes}分{seconds:.2f}秒"

    rate = (total_bytes / 1048576) / upload_elapsed if upload_elapsed > 0 else 0.0

    print(
        f"[总体统计] 共上传 {file_count} 个文件、"
        f"{total_bytes / 1048576:.2f} MiB，"
        f"失败 {fail_count} 个，"
        f"合计 {upload_elapsed:.2f} 秒，"
        f"上传耗时 {time_str}，"
        f"平均速率 {rate:.2f} MiB/秒。"
    )
    # =====================================================================


if __name__ == "__main__":
    run()
