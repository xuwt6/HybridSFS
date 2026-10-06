import os
import time
from hdfs import InsecureClient
from config import Config
from file_search import FileSearcher


class FileDownloader:
    def __init__(self):
        """Initialize the HDFS client and file searcher"""
        self.hdfs_client = InsecureClient(Config.HDFS_URL, user=Config.HDFS_USER)
        self.searcher = FileSearcher()

    def download_files(self, filename, search_mode="exact", download_dir="./downloads"):
        """
        Search and download all matched files by file name, and report elapsed time and speed.

        :param filename: file name to search for
        :param search_mode: search mode ("exact", "ngram", "wildcard")
        :param download_dir: local download directory
        """
        # 1. Ensure the download directory exists
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)
            print(f"✅ 创建下载目录: {download_dir}")

        # 2. Use FileSearcher to search
        print(f"\n🔍 正在以 [{search_mode}] 模式搜索文件: {filename}")
        search_results = self.searcher.search_by_filename(filename, search_mode)
        if not search_results:
            print(f"❌ 未找到名为 '{filename}' 的文件。")
            return

        print(f"✅ 找到 {len(search_results)} 个匹配的文件，准备开始下载...\n")

        # --- Added: initialize statistics variables ---
        start_time = time.time()
        total_bytes_downloaded = 0
        downloaded_count = 0
        # -------------------------

        # 3. Iterate over all search results and download files
        for result in search_results:
            block_path = result['block_info']['hdfs_path']
            file_info = result['file_info']
            original_name = file_info['original_name']
            offset = file_info['offset']
            length = file_info['length']

            # Handle duplicate filenames
            local_path = self._get_unique_filepath(download_dir, original_name)

            try:
                # Open the HDFS file stream using a with statement
                with self.hdfs_client.read(block_path, offset=offset, length=length) as reader:
                    # Call the .read() method to obtain binary data
                    file_data = reader.read()

                    # Check whether the data is empty
                    if not file_data:
                        print(f"   ⚠️ 警告: 读取到的数据为空 (Offset: {offset}, Length: {length})")
                        continue

                    # Write the read binary data to a local file
                    with open(local_path, 'wb') as f:
                        f.write(file_data)

                    # --- Added: update statistics ---
                    file_size = len(file_data)
                    total_bytes_downloaded += file_size
                    downloaded_count += 1
                    # -------------------------

                    print(f"   📥 已下载: {original_name} (大小: {file_size} bytes)")

            except Exception as e:
                print(f"   ❌ 下载文件 {original_name} 失败: {e}")

        # --- Added: compute and print statistics ---
        end_time = time.time()
        elapsed_time = end_time - start_time

        # Compute speed (MiB/s)
        # 1 MiB = 1024 * 1024 bytes
        if elapsed_time > 0:
            speed_mib_per_sec = (total_bytes_downloaded / (1024 * 1024)) / elapsed_time
        else:
            speed_mib_per_sec = 0.0

        print("\n" + "="*50)
        print(f"🎉 下载任务完成！")
        print(f"📊 统计报告:")
        print(f"   - 成功文件数: {downloaded_count}")
        print(f"   - 总数据量:   {total_bytes_downloaded / (1024 * 1024):.2f} MiB ({total_bytes_downloaded} Bytes)")
        print(f"   - 下载的平均文件大小:   {total_bytes_downloaded / (1024 * 1024 * downloaded_count):.2f} MiB")
        print(f"   - 总耗时:     {elapsed_time:.2f} 秒")
        print(f"   - 平均速度:   {speed_mib_per_sec:.2f} MiB/s")
        print(f"📂 保存路径:    {os.path.abspath(download_dir)}")
        print("="*50)
        # -----------------------------

    def _get_unique_filepath(self, directory, filename):
        """
        Generate a non-conflicting local file path.
        If the file already exists, append a numeric suffix to the filename, e.g. file.txt -> file_1.txt.
        """
        base_name, ext = os.path.splitext(filename)
        target_path = os.path.join(directory, filename)
        counter = 1

        # Loop to check whether the file exists; if so, modify the filename
        while os.path.exists(target_path):
            new_filename = f"{base_name}_{counter}{ext}"
            target_path = os.path.join(directory, new_filename)
            counter += 1

        return target_path


if __name__ == "__main__":
    downloader = FileDownloader()

    # 1. Get user input
    file_to_download = input("请输入要下载的文件名: ").strip()
    if not file_to_download:
        print("⚠️ 文件名不能为空。")
    else:
        # 2. Select search mode
        print("请选择搜索模式:")
        print(" 1. exact  (精确匹配，如: test.jpg)")
        print(" 2. ngram  (模糊搜索，如: test 可匹配 test_1.jpg, my_test_file.jpg)")
        print(" 3. wildcard (通配符搜索，如: 2007_*.jpg)")
        mode_choice = input("请输入选项 (1/2/3，默认为1): ").strip()
        mode_map = {"1": "exact", "2": "ngram", "3": "wildcard"}
        search_mode = mode_map.get(mode_choice, "exact")

        # 3. Execute download
        downloader.download_files(file_to_download, search_mode)