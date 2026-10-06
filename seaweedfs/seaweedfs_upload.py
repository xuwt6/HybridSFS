# seaweedfs_upload.py
# SeaweedFS baseline upload system (most primitive approach, functionality and counting basis aligned with hdfs_upload_system.py):
# Upload local small files one by one, 1 small file exclusively occupies 1 chunk (forced via maxMB, equivalent to HDFS 1 block),
# no packaging, no merging, no Elasticsearch.
# SeaweedFS directories mirror the local relative directory structure; original file names are preserved; streaming scan uploads files one by one;
# Timing: measure total elapsed time only once for the entire upload code section, not accumulated per file (fully consistent with the HDFS baseline).
import os
import sys
import time

# Reuse configuration from the parent directory smallfiles_heuristic to ensure "all configurations are identical"
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from config import Config
from seaweedfs_client import SeaweedFSFilerClient


def run():
    client = SeaweedFSFilerClient()
    client.health_check()  # Raise an error immediately when unreachable

    total_bytes = 0
    file_count = 0

    print("开始扫描本地目录并逐个上传到 SeaweedFS...")
    print(f"  源目录: {Config.LOCAL_DIR}")
    print(f"  目标: {Config.SEAWEDFS_FILER_URL}{Config.SEAWEDFS_BASE_PATH}/")

    # Measure time only once for the entire upload code section (consistent with hdfs_upload_system.py)
    upload_start = time.time()
    for root, _, files in os.walk(Config.LOCAL_DIR):
        for name in files:
            local_path = os.path.join(root, name)

            # SeaweedFS directories mirror the local relative directory structure; original file names are preserved (consistent with HDFS)
            rel_path = os.path.relpath(local_path, Config.LOCAL_DIR).replace(os.sep, "/")
            filer_path = f"{Config.SEAWEDFS_BASE_PATH}/{rel_path}"

            try:
                # Upload: 1 file exclusively occupies 1 chunk (maxMB forces alignment with HDFS 1 block semantics)
                ok = client.upload_file(local_path, filer_path)
                if not ok:
                    print(f"[严重错误] 上传失败: {local_path}")
                    continue
            except Exception as e:
                print(f"[严重错误] 上传失败: {local_path}, {e}")
                continue

            file_size = os.path.getsize(local_path)
            total_bytes += file_size
            file_count += 1
            if file_count % 30 == 0 or file_count == 1:
                print(f"[成功上传]第 {file_count} 个文件 {name} 已上传（独占 1 chunk），大小 {file_size / 1048576:.4f} MiB。")
    upload_elapsed = time.time() - upload_start

    hours = int(upload_elapsed // 3600)
    minutes = int((upload_elapsed % 3600) // 60)
    seconds = upload_elapsed % 60
    time_str = f"{hours}时{minutes}分{seconds:.2f}秒"
    rate = (total_bytes / 1048576) / upload_elapsed if upload_elapsed > 0 else 0.0
    print(f"[总体统计] 共上传 {file_count} 个文件、{total_bytes / 1048576:.2f} MiB，合计{upload_elapsed}秒，上传耗时 {time_str}，平均速率 {rate:.2f} MiB/秒。")


if __name__ == "__main__":
    run()
