"""
Traverse all folders and subfolders under police_data,
find leaf folders (bottom-level directories with no subfolders),
deduplicate by name, and copy unique leaf folders to police_data_simple.
"""

import os
import shutil
from pathlib import Path
from collections import defaultdict


def find_leaf_folders(root: Path) -> list[Path]:
    """Find all leaf folders (directories that contain no subfolders)."""
    leafs = []
    for dirpath, dirnames, filenames in os.walk(root):
        dp = Path(dirpath)
        # Exclude the root directory itself
        if dp == root:
            continue
        if not dirnames:  # No subfolders → leaf node
            leafs.append(dp)
    return leafs


def deduplicate_leafs(leafs: list[Path]) -> list[Path]:
    """Deduplicate by folder name, keeping only the first occurrence of each name."""
    seen = set()
    unique = []
    for leaf in leafs:
        name = leaf.name
        if name not in seen:
            seen.add(name)
            unique.append(leaf)
    return unique


def copy_folders(unique_leafs: list[Path], src_root: Path, dst_root: Path):
    """Copy unique leaf folders to the target directory."""
    dst_root.mkdir(parents=True, exist_ok=True)

    for leaf in unique_leafs:
        dst_path = dst_root / leaf.name
        if dst_path.exists():
            print(f"  跳过（目标已存在）: {dst_path}")
            continue
        shutil.copytree(leaf, dst_path)
        print(f"  已复制: {leaf.name}")


def main():
    src_root = Path("E:\merge_small_files_for_memory\datatest\police_data_eng")
    dst_root = Path("E:\merge_small_files_for_memory\datatest\police_data_eng\police_data_simple")

    if not src_root.exists():
        print(f"错误：源目录 {src_root.resolve()} 不存在，请确认路径。")
        return

    print(f"源目录: {src_root.resolve()}")
    print(f"目标目录: {dst_root.resolve()}\n")

    # 1. Find all leaf folders
    leafs = find_leaf_folders(src_root)
    print(f"找到 {len(leafs)} 个叶子文件夹")

    # 2. Deduplicate by name
    unique = deduplicate_leafs(leafs)
    dup_count = len(leafs) - len(unique)
    print(f"去重后保留 {len(unique)} 个，过滤掉 {dup_count} 个重复\n")

    # 3. Copy to target directory
    print("开始复制:")
    copy_folders(unique, src_root, dst_root)

    print(f"\n完成！不重复的叶子文件夹已保存到: {dst_root.resolve()}")


if __name__ == "__main__":
    main()
