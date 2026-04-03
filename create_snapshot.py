#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
项目快照生成器
将整个项目压缩到一个文件中，运行恢复脚本即可还原
"""

import os
import json
import zlib
import base64
from datetime import datetime


def compress_content(content):
    """
    压缩内容
    
    使用zlib压缩并base64编码
    
    Args:
        content (str): 原始内容
    
    Returns:
        str: 压缩后的字符串
    """
    return base64.b64encode(zlib.compress(content.encode('utf-8'), 9)).decode('ascii')


def read_file(file_path):
    """
    读取文件内容
    
    Args:
        file_path (str): 文件路径
    
    Returns:
        str: 文件内容，失败返回None
    """
    if not os.path.exists(file_path):
        return None
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        print(f"[错误] 读取失败 {file_path}: {str(e)}")
        return None


def main():
    """
    主函数 - 生成项目快照
    
    读取所有项目文件，压缩并生成恢复脚本
    """
    print("=" * 70)
    print("AKShare数据采集器 - 项目快照生成器")
    print("=" * 70)
    
    files = [
        "main.py",
        "run_collector.py",
        "db_core.py",
        "collector_static.py",
        "collector_daily.py",
        "collector_realtime.py",
        "collector_config.json",
        "buildozer.spec",
        ".github/workflows/build.yml",
        "README.md",
        "README_KIVY.md",
        "BUILD_GUIDE.md",
        "create_snapshot.py",
    ]
    
    snapshot = {
        "version": "3.1",
        "created": datetime.now().isoformat(),
        "description": "AKShare A股数据采集器 (DuckDB版 + Kivy安卓版)",
        "directories": [
            "akshare_data",
            "akshare_data/logs",
            ".github",
            ".github/workflows",
        ],
        "files": {}
    }
    
    total_size = 0
    compressed_size = 0
    
    for file_path in files:
        content = read_file(file_path)
        if content is not None:
            compressed = compress_content(content)
            snapshot["files"][file_path] = compressed
            total_size += len(content)
            compressed_size += len(compressed)
            print(f"[压缩] {file_path} - {len(content)} -> {len(compressed)} 字节")
        else:
            print(f"[跳过] {file_path} - 文件不存在")
    
    output_content = '''#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
AKShare数据采集器 - 项目恢复脚本
运行: python restore_project.py
"""

import os
import json
import zlib
import base64

# ============================================
# 项目快照数据
# ============================================
SNAPSHOT = ''' + json.dumps(snapshot, ensure_ascii=False, indent=2) + '''

def decompress_content(compressed):
    """
    解压内容
    
    Args:
        compressed (str): 压缩后的字符串
    
    Returns:
        str: 原始内容
    """
    return zlib.decompress(base64.b64decode(compressed.encode('ascii'))).decode('utf-8')


def main():
    print("=" * 70)
    print("AKShare数据采集器 - 项目恢复")
    print("=" * 70)
    
    base_dir = os.path.dirname(os.path.abspath(__file__))
    print(f"项目根目录: {base_dir}")
    
    print("\\n创建目录...")
    for d in SNAPSHOT["directories"]:
        dir_path = os.path.join(base_dir, d)
        os.makedirs(dir_path, exist_ok=True)
        print(f"  [创建] {d}")
    
    print("\\n恢复文件...")
    for file_path, compressed in SNAPSHOT["files"].items():
        full_path = os.path.join(base_dir, file_path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, 'w', encoding='utf-8') as f:
            f.write(decompress_content(compressed))
        print(f"  [恢复] {file_path}")
    
    print("\\n" + "=" * 70)
    print("恢复完成！")
    print("=" * 70)
    print("\\n使用说明:")
    print("  1. 运行主菜单:   python run_collector.py")
    print("  2. 静态数据:     python collector_static.py stock_list")
    print("  3. 每日数据:     python collector_daily.py stock")
    print("  4. 实时数据:     python collector_realtime.py all")
    print("  5. 详细文档:     查看 README.md")
    print("=" * 70)


if __name__ == "__main__":
    main()
'''
    
    output_file = "restore_project.py"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(output_content)
    
    print(f"\n{'=' * 70}")
    print(f"完成！")
    print(f"  原始大小: {total_size / 1024:.1f} KB")
    print(f"  压缩后: {compressed_size / 1024:.1f} KB")
    print(f"  压缩率: {(1 - compressed_size/total_size)*100:.1f}%")
    print(f"  恢复脚本: {output_file}")
    print(f"{'=' * 70}")
    print("\\n使用方法:")
    print("  1. 将 restore_project.py 复制到目标目录")
    print("  2. 运行: python restore_project.py")
    print("  3. 项目将自动恢复到当前目录")
    print("=" * 70)


if __name__ == "__main__":
    main()
