# main.py
import threading
import time
from upload_system import UploadSystem

if __name__ == "__main__":
    system = UploadSystem()

    start_time = time.time()
    # Start the streaming injection thread
    injector_thread = threading.Thread(target=system.stream_injector, daemon=True)
    injector_thread.start()

    # Start the main scheduling loop, measuring only the execution time of process_cycle itself
    system.process_cycle()

    end_time = time.time()

    # Calculate total process_cycle duration and upload rate
    total_time = end_time - start_time
    total_bytes = system.total_uploaded_bytes
    rate = (total_bytes / 1048576) / total_time if total_time > 0 else 0.0

    print(f"[总结] process_cycle 耗时: {total_time:.2f} 秒，"
          f"累计上传: {total_bytes / 1048576:.2f} MiB，"
          f"平均速率: {rate:.2f} MiB/s。")
