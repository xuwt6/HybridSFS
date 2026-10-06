# minio_checker.py
# MinIO object checker, similar in functionality and statistics to hdfs_block_checker.py
# Used to inspect object information (size, count, distribution) in a MinIO bucket
# Statistics: total object count, total data size, object size distribution

import os
import sys
from collections import defaultdict

# Reuse the configuration from the parent directory smallfiles_heuristic
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from minio import Minio

# MinIO connection configuration (consistent with other minio scripts)
MINIO_ENDPOINT = "192.168.31.111:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_SECURE = False


class MinioObjectChecker:
    def __init__(self, endpoint=MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY,
                 secret_key=MINIO_SECRET_KEY, secure=MINIO_SECURE):
        """
        :param endpoint: MinIO service address, e.g. 192.168.31.111:9000
        :param access_key: access key
        :param secret_key: secret key
        :param secure: whether to use HTTPS
        """
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

    def check_bucket_objects(self, bucket_name, prefix=""):
        """
        Inspect information of all objects in the specified bucket
        :param bucket_name: bucket name
        :param prefix: object prefix (similar to an HDFS directory path)
        """
        print(f"\n正在检查 MinIO 存储桶: {bucket_name}")
        if prefix:
            print(f"   前缀过滤: {prefix}")
        print("=" * 70)

        # Verify that the bucket exists
        if not self.client.bucket_exists(bucket_name):
            print(f"存储桶 {bucket_name} 不存在！")
            return

        total_objects = 0
        total_size = 0
        size_distribution = defaultdict(int)  # Count objects by size range

        try:
            # Recursively list all objects
            objects = self.client.list_objects(bucket_name, prefix=prefix, recursive=True)

            for obj in objects:
                # Skip directory marker objects
                if obj.object_name.endswith("/"):
                    continue

                try:
                    # Retrieve detailed object information
                    stat = self.client.stat_object(bucket_name, obj.object_name)
                    obj_size = stat.size
                    last_modified = stat.last_modified

                    total_objects += 1
                    total_size += obj_size

                    # Classify and count by size range
                    size_range = self._get_size_range(obj_size)
                    size_distribution[size_range] += 1

                    # Extract file name (from the object path)
                    obj_name = obj.object_name.split("/")[-1]
                    print(f"对象: {obj_name}")
                    print(f"   完整路径: {obj.object_name}")
                    print(f"   大小: {obj_size / (1024*1024):.2f} MiB ({obj_size} Bytes)")
                    if last_modified:
                        print(f"   修改时间: {last_modified.strftime('%Y-%m-%d %H:%M:%S')}")
                    print()

                except Exception as e:
                    print(f"   读取对象 {obj.object_name} 失败: {e}")

            # Print summary report
            print("\n" + "=" * 70)
            print("MinIO 对象信息汇总:")
            print(f"   总对象数: {total_objects}")
            print(f"   总数据大小: {total_size / (1024*1024*1024):.2f} GiB ({total_size / (1024*1024):.2f} MiB)")
            print(f"   平均对象大小: {total_size / total_objects / (1024*1024):.2f} MiB" if total_objects > 0 else "   平均对象大小: 0.00 MiB")
            print(f"\n   对象大小分布:")
            for size_range in sorted(size_distribution.keys()):
                count = size_distribution[size_range]
                percentage = (count / total_objects * 100) if total_objects > 0 else 0
                print(f"     {size_range}: {count} 个对象 ({percentage:.1f}%)")
            print("=" * 70)

        except Exception as e:
            print(f"连接或读取 MinIO 存储桶失败: {e}")

    def _get_size_range(self, size_bytes):
        """
        Return the size-range label based on object size
        """
        size_mib = size_bytes / (1024 * 1024)
        if size_mib < 1:
            return "< 1 MiB"
        elif size_mib < 10:
            return "1-10 MiB"
        elif size_mib < 50:
            return "10-50 MiB"
        elif size_mib < 100:
            return "50-100 MiB"
        elif size_mib < 500:
            return "100-500 MiB"
        elif size_mib < 1024:
            return "500 MiB - 1 GiB"
        else:
            return ">= 1 GiB"

    def delete_bucket_objects(self, bucket_name, prefix="", confirm=True):
        """
        Delete objects in the specified bucket
        :param bucket_name: bucket name
        :param prefix: object prefix (only delete objects under this prefix)
        :param confirm: whether user confirmation is required, defaults to True
        """
        print(f"\n警告：即将删除 MinIO 存储桶 {bucket_name} 中的对象")
        if prefix:
            print(f"   前缀过滤: {prefix}")

        if confirm:
            user_confirm = input("确定要继续吗？此操作不可逆！(yes/NO): ")
            if user_confirm.lower() != 'yes':
                print("操作已取消。")
                return

        try:
            # List all objects to be deleted
            objects = self.client.list_objects(bucket_name, prefix=prefix, recursive=True)
            obj_list = [obj.object_name for obj in objects if not obj.object_name.endswith("/")]

            if not obj_list:
                print(f"存储桶 {bucket_name} 中没有找到要删除的对象。")
                return

            print(f"找到 {len(obj_list)} 个对象，开始删除...")

            # Batch delete objects
            deleted_count = 0
            errors = list(self.client.remove_objects(bucket_name, obj_list))

            if errors:
                print(f"\n部分对象删除失败:")
                for error in errors:
                    print(f"   {error}")
                deleted_count = len(obj_list) - len(errors)
            else:
                deleted_count = len(obj_list)

            print(f"成功删除 {deleted_count} 个对象。")

        except Exception as e:
            print(f"删除失败: {e}")

    def delete_bucket(self, bucket_name, force=False):
        """
        Delete the entire bucket (objects inside must be cleared first)
        :param bucket_name: bucket name
        :param force: whether to force deletion (clear first then delete the bucket), defaults to False
        """
        print(f"\n警告：即将删除 MinIO 存储桶: {bucket_name}")

        if not self.client.bucket_exists(bucket_name):
            print(f"存储桶 {bucket_name} 不存在。")
            return

        if force:
            print("强制删除模式：将先清空存储桶内所有对象...")
            self.delete_bucket_objects(bucket_name, prefix="", confirm=False)

        user_confirm = input("确定要删除存储桶吗？(yes/NO): ")
        if user_confirm.lower() == 'yes':
            try:
                self.client.remove_bucket(bucket_name)
                print(f"成功删除存储桶: {bucket_name}")
            except Exception as e:
                print(f"删除存储桶失败: {e}")
                print("提示：存储桶可能不为空，请先清空对象或使用 force=True")
        else:
            print("操作已取消。")


if __name__ == "__main__":
    # MinIO bucket and prefix configuration
    TARGET_BUCKET = "small-files-test"
    TARGET_PREFIX = "raw_small_files"  # Check objects under a specific prefix; leave empty to check the entire bucket

    checker = MinioObjectChecker()
    checker.check_bucket_objects(TARGET_BUCKET, prefix=TARGET_PREFIX)

    # Optional: delete objects (uncomment to enable)
    # checker.delete_bucket_objects(TARGET_BUCKET, prefix=TARGET_PREFIX)

    # Optional: delete the entire bucket (uncomment to enable)
    # checker.delete_bucket(TARGET_BUCKET, force=True)
