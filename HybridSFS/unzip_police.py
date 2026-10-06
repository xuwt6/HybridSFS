#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Batch-unzip files from the police_data_eng directory to D:\police_en
- Unzipped folder name = zip file name without .zip suffix (e.g. 2025-01.zip → 2025-01)
- Skip if the target folder already exists
- Show progress bar and current file being unzipped
"""

import os
import sys
import zipfile
import shutil


# ── Configuration ──
SRC_DIR = r"E:\merge_small_files_for_memory\datatest\police_data_eng"
DST_DIR = r"E:\police_en"


def print_progress(current, total, width=40):
    """Print progress bar in terminal"""
    pct = current / total if total > 0 else 1
    filled = int(width * pct)
    bar = "█" * filled + "░" * (width - filled)
    sys.stdout.write(f"\r  [{bar}] {pct*100:5.1f}%  ({current}/{total})")
    sys.stdout.flush()
    if pct >= 1.0:
        sys.stdout.write("\n")


def extract_zip_with_progress(zip_path, extract_to):
    """Unzip a single zip file, showing progress bar by file count"""
    with zipfile.ZipFile(zip_path, 'r') as zf:
        members = zf.namelist()
        total = len(members)

        for i, member in enumerate(members):
            zf.extract(member, extract_to)
            print_progress(i + 1, total)


def main():
    # Ensure the target root directory exists
    os.makedirs(DST_DIR, exist_ok=True)

    # Collect all .zip files and sort them
    zip_files = sorted([
        f for f in os.listdir(SRC_DIR)
        if f.lower().endswith('.zip')
    ])

    if not zip_files:
        print(f"在 {SRC_DIR} 中未找到任何 .zip 文件。")
        return

    print(f"源目录: {SRC_DIR}")
    print(f"目标目录: {DST_DIR}")
    print(f"发现 {len(zip_files)} 个 zip 文件\n")
    print("=" * 60)

    skipped = 0
    extracted = 0
    failed = 0

    for idx, zip_name in enumerate(zip_files, 1):
        # Folder name = zip file name without .zip
        folder_name = os.path.splitext(zip_name)[0]
        target_dir = os.path.join(DST_DIR, folder_name)

        zip_path = os.path.join(SRC_DIR, zip_name)

        # Skip if the target folder already exists
        if os.path.exists(target_dir):
            print(f"[{idx:>2}/{len(zip_files)}] ⏭  跳过（已存在）: {folder_name}")
            skipped += 1
            continue

        # Unzip
        print(f"[{idx:>2}/{len(zip_files)}] 📦 正在解压: {zip_name} → {folder_name}")
        try:
            extract_zip_with_progress(zip_path, target_dir)
            extracted += 1
            print(f"         ✅ 完成: {folder_name}")
        except Exception as e:
            failed += 1
            # Unzip failed; clean up incomplete directory
            if os.path.exists(target_dir):
                shutil.rmtree(target_dir)
            print(f"\n         ❌ 失败: {e}")

    # ── Summary ──
    print("\n" + "=" * 60)
    print(f"解压完成汇总:")
    print(f"  成功解压: {extracted} 个")
    print(f"  跳过(已存在): {skipped} 个")
    print(f"  失败: {failed} 个")
    print(f"  总计: {len(zip_files)} 个")
    print("=" * 60)


if __name__ == "__main__":
    main()
