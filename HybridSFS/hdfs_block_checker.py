# hdfs_block_checker.py
from hdfs import InsecureClient
from collections import defaultdict

class HDFSBlockChecker:
    def __init__(self, webhdfs_url, user=None):
        """
        :param webhdfs_url: HDFS WebUI address, e.g. http://192.168.31.111:9870
        :param user: HDFS operation user
        """
        self.client = InsecureClient(webhdfs_url, user=user)

    def check_directory_blocks(self, hdfs_path):
        """
        Check block information for all files under the specified directory
        """
        print(f"\n🔍 正在检查 HDFS 目录: {hdfs_path}")
        print("=" * 70)

        total_blocks = 0
        total_size = 0
        block_size_distribution = defaultdict(int)

        try:
            # Recursively traverse all files under the directory
            for root, dirs, files in self.client.walk(hdfs_path):
                for file_name in files:
                    file_path = f"{root}/{file_name}"
                    try:
                        # Get the full status of the file (including block info)
                        status = self.client.status(file_path)
                        file_size = status['length']
                        block_size = status['blockSize']

                        # Calculate the number of blocks for this file
                        file_blocks = (file_size + block_size - 1) // block_size if block_size > 0 else 0
                        total_blocks += file_blocks
                        total_size += file_size

                        # Record block size distribution
                        block_size_distribution[block_size] += file_blocks

                        print(f"📄 文件: {file_name}")
                        print(f"   大小: {file_size / (1024*1024):.2f} MiB | 块数: {file_blocks} | 单块大小: {block_size / (1024*1024):.2f} MiB")

                    except Exception as e:
                        print(f"   ⚠️ 读取文件 {file_path} 失败: {e}")

            # Print summary report
            print("\n" + "=" * 70)
            print("📊 HDFS 块信息汇总:")
            print(f"   总文件块数: {total_blocks}")
            print(f"   总数据大小: {total_size / (1024*1024*1024):.2f} GiB")
            print(f"   块大小分布: {dict(block_size_distribution)}")
            print("=" * 70)

        except Exception as e:
            print(f"❌ 连接或读取 HDFS 目录失败: {e}")

    def delete_directory(self, hdfs_path, recursive=True):
        """
        Delete the specified HDFS directory or file
        :param hdfs_path: HDFS path to delete
        :param recursive: whether to delete recursively, defaults to True. If True, the directory will be deleted even if it is not empty.
        """
        print(f"\n⚠️ 警告：即将删除 HDFS 路径: {hdfs_path}")
        confirm = input("确定要继续吗？此操作不可逆！(yes/NO): ")
        if confirm.lower() == 'yes':
            try:
                self.client.delete(hdfs_path, recursive=recursive)
                print(f"✅ 成功删除: {hdfs_path}")
            except Exception as e:
                print(f"❌ 删除失败: {e}")
        else:
            print("操作已取消。")

if __name__ == "__main__":
    # Replace with your actual HDFS WebUI address and directory
    HDFS_WEB_URL = "http://192.168.31.111:9870"
    HDFS_USER = "smallfiles"
    TARGET_HDFS_PATH = "/merged_small_files"  # Replace with the actual HDFS directory you uploaded to

    checker = HDFSBlockChecker(HDFS_WEB_URL, user=HDFS_USER)
    checker.check_directory_blocks(TARGET_HDFS_PATH)

    # Call the newly added delete method
    # For example, delete the entire /merged_small_files directory
    checker.delete_directory(TARGET_HDFS_PATH)
