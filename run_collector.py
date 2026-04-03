#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
AKShare 数据采集器 - 主菜单
整合所有数据采集功能的交互式菜单
使用 DuckDB 作为主存储，SQLite 用于实时行情
"""

import sys
import os
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR))

from db_core import DuckDBManager, SQLiteManager, check_disk_space, check_network, DATA_DIR
from collector_static import (
    collect_stock_list, collect_stock_info, collect_board_industry,
    collect_board_concept, collect_index_cons, collect_etf_list
)
from collector_daily import (
    collect_stock_daily, collect_index_daily, collect_etf_daily,
    collect_north_flow, collect_macro
)
from collector_realtime import RealtimeCollector


def print_header():
    """打印标题"""
    print("\n" + "=" * 60)
    print("AKShare A股数据采集器 (DuckDB版)")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)


def check_system():
    """检查系统状态"""
    print("\n🔍 系统状态检查:")
    ok, free_gb = check_disk_space()
    print(f"   磁盘空间: {'✓ 正常' if ok else '✗ 不足'} ({free_gb:.2f}GB)")
    print(f"   网络连接: {'✓ 正常' if check_network() else '✗ 失败'}")
    
    with DuckDBManager() as db:
        stocks = db.get_table_count('stocks')
        quotes = db.get_table_count('daily_quotes')
        print(f"   数据库状态: ✓ 正常")
        print(f"   股票数量: {stocks}")
        print(f"   日线数据: {quotes} 条")


def print_menu():
    """打印主菜单"""
    print("\n" + "-" * 60)
    print("【单项数据采集】")
    print("-" * 60)
    
    print("\n  静态数据（建议每周/每月更新一次）:")
    print("    1. 股票列表")
    print("    2. 个股基本信息")
    print("    3. 行业板块")
    print("    4. 概念板块")
    print("    5. 指数成分股")
    print("    6. ETF列表")
    
    print("\n  每日数据（建议每个交易日收盘后更新）:")
    print("    7. 股票日线+资金流")
    print("    8. 指数历史")
    print("    9. ETF历史")
    print("    10. 北向资金")
    print("    11. 宏观数据")
    
    print("\n  实时数据（盘中实时运行）:")
    print("    12. 全市场快照")
    
    print("\n" + "-" * 60)
    print("【组合数据采集】")
    print("-" * 60)
    
    print("\n  静态数据组合:")
    print("    13. 静态数据全量更新 (1-6)")
    
    print("\n  每日数据组合:")
    print("    14. 每日数据全量更新 (7-11)")
    
    print("\n  一键更新:")
    print("    15. 一键更新全部 (静态+每日)")
    
    print("\n" + "-" * 60)
    print("【数据查询】")
    print("-" * 60)
    
    print("\n    16. 查看数据库统计")
    print("    17. 查询股票数据")
    
    print("\n" + "-" * 60)
    print("  0. 检查系统状态")
    print("  q. 退出")
    print("-" * 60)


def run_static_all():
    """运行静态数据全量更新"""
    print("\n" + "=" * 60)
    print("静态数据全量更新")
    print("=" * 60)
    
    collect_stock_list()
    collect_stock_info()
    collect_board_industry()
    collect_board_concept()
    collect_index_cons()
    collect_etf_list()
    
    print("\n" + "=" * 60)
    print("静态数据全量更新完成！")
    print("=" * 60)


def run_daily_all():
    """运行每日数据全量更新"""
    print("\n" + "=" * 60)
    print("每日数据全量更新")
    print("=" * 60)
    
    collect_stock_daily()
    collect_index_daily()
    collect_etf_daily()
    collect_north_flow()
    collect_macro()
    
    print("\n" + "=" * 60)
    print("每日数据全量更新完成！")
    print("=" * 60)


def run_all():
    """运行全部数据更新"""
    print("\n" + "=" * 60)
    print("一键更新全部数据")
    print("=" * 60)
    
    run_static_all()
    run_daily_all()
    
    print("\n" + "=" * 60)
    print("全部数据更新完成！")
    print("=" * 60)


def show_db_stats():
    """显示数据库统计信息"""
    print("\n" + "=" * 60)
    print("数据库统计信息")
    print("=" * 60)
    
    with DuckDBManager() as db:
        print("\n【DuckDB 主数据库】")
        tables = [
            ('stocks', '股票列表'),
            ('stock_info', '个股信息'),
            ('board_industry', '行业板块'),
            ('board_concept', '概念板块'),
            ('board_industry_stocks', '行业成分股'),
            ('board_concept_stocks', '概念成分股'),
            ('index_cons', '指数成分股'),
            ('etf_list', 'ETF列表'),
            ('daily_quotes', '日线行情'),
            ('index_daily', '指数历史'),
            ('etf_daily', 'ETF历史'),
            ('north_flow', '北向资金'),
            ('macro_data', '宏观数据'),
        ]
        
        total_rows = 0
        for table, name in tables:
            count = db.get_table_count(table)
            total_rows += count
            print(f"   {name}: {count:,} 条")
        
        date_range = db.get_date_range('daily_quotes')
        if date_range['min_date']:
            print(f"\n   日线数据日期范围: {date_range['min_date']} ~ {date_range['max_date']}")
        
        print(f"\n   总记录数: {total_rows:,} 条")
    
    with SQLiteManager() as db:
        print("\n【SQLite 实时数据库】")
        result = db.fetch_one("SELECT COUNT(*) FROM realtime_quotes")
        count = result[0] if result else 0
        print(f"   实时行情快照: {count:,} 条")


def query_stock_data():
    """查询股票数据"""
    print("\n" + "=" * 60)
    print("股票数据查询")
    print("=" * 60)
    
    try:
        code = input("\n请输入股票代码 (如 600519): ").strip()
        if not code:
            print("已取消")
            return
        
        with DuckDBManager() as db:
            info = db.fetch_one(
                "SELECT name, industry, list_date FROM stock_info WHERE code = ?",
                (code,)
            )
            if info:
                print(f"\n【基本信息】")
                print(f"   股票名称: {info[0]}")
                print(f"   所属行业: {info[1]}")
                print(f"   上市日期: {info[2]}")
            else:
                print(f"\n   未找到 {code} 的基本信息")
            
            quotes = db.fetch_all("""
                SELECT trade_date, open, high, low, close, volume, 
                       change_pct, main_net_inflow
                FROM daily_quotes 
                WHERE code = ?
                ORDER BY trade_date DESC
                LIMIT 10
            """, (code,))
            
            if quotes:
                print(f"\n【最近10日行情】")
                print(f"   {'日期':<12} {'开盘':>8} {'最高':>8} {'最低':>8} {'收盘':>8} {'涨跌幅':>8} {'主力净流入':>12}")
                print("   " + "-" * 70)
                for q in quotes:
                    date_str = str(q[0])
                    main_flow = q[7] if q[7] else 0
                    print(f"   {date_str:<12} {q[1]:>8.2f} {q[2]:>8.2f} {q[3]:>8.2f} {q[4]:>8.2f} {q[6]:>7.2f}% {main_flow:>12.0f}")
            else:
                print(f"\n   未找到 {code} 的行情数据")
    
    except KeyboardInterrupt:
        print("\n已取消")


def main():
    """主函数"""
    print_header()
    
    while True:
        print_menu()
        
        try:
            choice = input("\n请选择功能 [0-17, q]: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print("\n\n已退出")
            break
        
        if choice == 'q' or choice == 'exit':
            print("\n已退出")
            break
        
        if choice == '0':
            check_system()
            continue
        
        try:
            choice = int(choice)
        except ValueError:
            print("\n⚠ 无效输入，请输入数字 0-17 或 q")
            continue
        
        print()
        
        if choice == 1:
            collect_stock_list()
        elif choice == 2:
            collect_stock_info()
        elif choice == 3:
            collect_board_industry()
        elif choice == 4:
            collect_board_concept()
        elif choice == 5:
            collect_index_cons()
        elif choice == 6:
            collect_etf_list()
        elif choice == 7:
            collect_stock_daily()
        elif choice == 8:
            collect_index_daily()
        elif choice == 9:
            collect_etf_daily()
        elif choice == 10:
            collect_north_flow()
        elif choice == 11:
            collect_macro()
        elif choice == 12:
            collector = RealtimeCollector()
            collector.snapshot_all()
        elif choice == 13:
            run_static_all()
        elif choice == 14:
            run_daily_all()
        elif choice == 15:
            run_all()
        elif choice == 16:
            show_db_stats()
        elif choice == 17:
            query_stock_data()
        else:
            print("⚠ 无效选项，请输入 0-17 或 q")
            continue
        
        print("\n" + "=" * 60)
        input("按回车键继续...")
        print_header()


if __name__ == "__main__":
    main()
