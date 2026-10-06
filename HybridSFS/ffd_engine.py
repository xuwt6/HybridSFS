# ffd_engine.py (greedy algorithm version)
from config import Config

class FFDPacker:
    def __init__(self, files):
        self.files = files
        self.target = Config.TARGET_SIZE_BYTES

    def evolve(self):
        if not self.files:
            return []

        # 1. Sort files by size in descending order (prioritize larger files to reduce fragmentation)
        sorted_files = sorted(self.files, key=lambda x: x['size'], reverse=True)

        result = []
        current_size = 0

        # 2. Greedy packing
        for file in sorted_files:
            file_size = file['size']

            # If the current block can still fit this file, or the current block is empty (to prevent oversized files from being dropped)
            if current_size + file_size <= self.target or current_size == 0:
                result.append(file)
                current_size += file_size
            # Otherwise skip and leave for the next round
            else:
                continue

        # 3. If the result is too small (<64 MB) while the original data volume is large, files are too fragmented
        # Here we could return all files directly (force-fill), but upper-level logic controls whether to merge
        # Currently kept as-is, with the upper-level end-of-stream forced merge logic serving as fallback

        return result