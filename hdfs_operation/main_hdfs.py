# main_hdfs.py
# HDFS-only baseline addition entry point: upload small files one by one; 1 file occupies 1 block, no packing or merging.
from hdfs_upload_system import run

if __name__ == "__main__":
    run()
