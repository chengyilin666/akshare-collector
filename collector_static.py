#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
AKShare 数据采集器 - 静态数据采集模块
采集不经常变化的数据：股票列表、个股信息、板块、指数成分股、ETF列表
使用 DuckDB 存储
"""

import sys
import time
import logging
from datetime import datetime
from typing import Optional, List

try:
    import akshare as ak
except ImportError:
    print("请先安装 akshare: pip install akshare")
    sys.exit(1)

from db_core import DuckDBManager, load_config, check_disk_space, DATA_DIR

LOG_DIR = DATA_DIR / 'logs'
LOG_DIR.mkdir(parents=True, exist_ok=True)

config = load_config()


def setup_logger(task_name: str) -> logging.Logger:
    """
    设置日志记录器
    
    Args:
        task_name: 任务名称
        
    Returns:
        logging.Logger: 日志记录器
    """
    logger = logging.getLogger(task_name)
    logger.setLevel(logging.INFO)
    
    if not logger.handlers:
        log_file = LOG_DIR / f'{task_name}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        fh = logging.FileHandler(log_file, encoding='utf-8')
        fh.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    
    return logger


def smart_sleep():
    """智能等待，防止请求过快"""
    interval = config.get('request_interval', 1)
    time.sleep(interval)


def safe_call_with_retry(func, max_retries: int = None, delay: int = None, **kwargs):
    """
    带重试的安全调用
    
    Args:
        func: 要调用的函数
        max_retries: 最大重试次数
        delay: 重试延迟
        **kwargs: 函数参数
        
    Returns:
        调用结果，失败返回None
    """
    max_retries = max_retries or config.get('max_retries', 3)
    delay = delay or config.get('retry_delay', 2)
    
    for attempt in range(max_retries):
        try:
            result = func(**kwargs)
            return result
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(delay)
            else:
                print(f"  ✗ 调用失败: {e}")
                return None


def collect_stock_list():
    """
    采集全部上市A股股票列表
    
    获取所有在上交所和深交所上市的A股股票代码和名称
    存储到 DuckDB 的 stocks 表
    """
    print("\n【1. 股票列表】")
    logger = setup_logger('stock_list')
    logger.info("开始采集股票列表")
    
    df = safe_call_with_retry(ak.stock_zh_a_spot_em)
    if df is not None and not df.empty:
        data = []
        count = 0
        for _, row in df.iterrows():
            code = str(row.get('代码', ''))
            name = str(row.get('名称', ''))
            if code and name:
                if code.startswith('6'):
                    market = 'sh'
                elif code.startswith('0') or code.startswith('3'):
                    market = 'sz'
                else:
                    continue
                data.append((code, name, market, datetime.now()))
                count += 1
        
        with DuckDBManager() as db:
            db.execute("DELETE FROM stocks")
            db.insert_many('stocks', data, ['code', 'name', 'market', 'update_time'])
        
        print(f"  ✓ 全部上市A股(上证+深证): {count}只")
        logger.info(f"采集完成，共 {count} 只股票")
    else:
        print("  ✗ 股票列表数据获取失败")
        logger.error("股票列表数据获取失败")


def collect_stock_info(codes: List[str] = None, max_count: int = None):
    """
    采集个股基本信息
    
    包括股票简称、行业、上市时间、总股本、流通股本等信息
    
    Args:
        codes: 股票代码列表，None则使用全部A股
        max_count: 最大采集数量，用于测试
    """
    print("\n【2. 个股基本信息】")
    logger = setup_logger('stock_info')
    logger.info("开始采集个股基本信息")
    
    ok, free_gb = check_disk_space()
    if not ok:
        print(f"  ✗ 磁盘空间不足: {free_gb:.2f}GB")
        logger.error(f"磁盘空间不足: {free_gb:.2f}GB")
        return
    
    with DuckDBManager() as db:
        if codes is None:
            stocks = db.get_stock_list()
            codes = [s['code'] for s in stocks]
        
        if not codes:
            print("  ✗ 没有股票代码，请先运行 stock_list")
            logger.error("没有股票代码")
            return
        
        if max_count:
            codes = codes[:max_count]
        
        total = len(codes)
        success = 0
        failed = 0
        
        print(f"  共 {total} 只股票需要采集")
        
        for i, code in enumerate(codes, 1):
            if i % 50 == 0 or i == total:
                print(f"\r  进度: {i}/{total} (成功:{success}, 失败:{failed})", end='', flush=True)
            
            df = safe_call_with_retry(ak.stock_individual_info_em, symbol=code)
            
            if df is not None and not df.empty:
                info = dict(zip(df['item'], df['value']))
                
                data = (
                    code,
                    str(info.get('股票简称', '')),
                    str(info.get('行业', '')),
                    str(info.get('板块', '')),
                    info.get('上市时间'),
                    float(info.get('总股本', 0) or 0),
                    float(info.get('流通股', 0) or 0),
                    float(info.get('总市值', 0) or 0),
                    float(info.get('流通市值', 0) or 0),
                    datetime.now()
                )
                
                db.insert_many('stock_info', [data], 
                    ['code', 'name', 'industry', 'sector', 'list_date', 
                     'total_share', 'float_share', 'total_market_value', 
                     'float_market_value', 'update_time'])
                success += 1
                logger.info(f"{code} 采集成功")
            else:
                failed += 1
                logger.warning(f"{code} 采集失败")
            
            smart_sleep()
        
        print(f"\n  ✓ 完成: 成功 {success}, 失败 {failed}")
        logger.info(f"采集完成: 成功 {success}, 失败 {failed}")


def collect_board_industry():
    """
    采集行业板块信息
    
    包括行业板块名称和成分股
    """
    print("\n【3. 行业板块】")
    logger = setup_logger('board_industry')
    logger.info("开始采集行业板块")
    
    df = safe_call_with_retry(ak.stock_board_industry_name_em)
    if df is not None and not df.empty:
        with DuckDBManager() as db:
            db.execute("DELETE FROM board_industry")
            db.execute("DELETE FROM board_industry_stocks")
            
            board_data = []
            stock_data = []
            count = 0
            
            for _, row in df.iterrows():
                board_code = str(row.get('板块代码', ''))
                board_name = str(row.get('板块名称', ''))
                
                if not board_code:
                    continue
                
                board_data.append((board_code, board_name, 0, datetime.now()))
                
                stocks_df = safe_call_with_retry(ak.stock_board_industry_cons_em, symbol=board_name)
                if stocks_df is not None and not stocks_df.empty:
                    for _, sr in stocks_df.iterrows():
                        stock_code = str(sr.get('代码', ''))
                        stock_name = str(sr.get('名称', ''))
                        if stock_code:
                            stock_data.append((board_code, stock_code, stock_name))
                    board_data[-1] = (board_code, board_name, len(stocks_df), datetime.now())
                
                count += 1
                if count % 10 == 0:
                    print(f"\r  进度: {count}/{len(df)}", end='', flush=True)
                
                smart_sleep()
            
            db.insert_many('board_industry', board_data, 
                ['board_code', 'board_name', 'stock_count', 'update_time'])
            db.insert_many('board_industry_stocks', stock_data,
                ['board_code', 'stock_code', 'stock_name'])
            
            print(f"\n  ✓ 行业板块: {len(board_data)}个, 成分股: {len(stock_data)}条")
            logger.info(f"采集完成: {len(board_data)}个板块, {len(stock_data)}条成分股")
    else:
        print("  ✗ 行业板块数据获取失败")
        logger.error("行业板块数据获取失败")


def collect_board_concept(max_count: int = 100):
    """
    采集概念板块信息
    
    Args:
        max_count: 最大采集数量
    """
    print("\n【4. 概念板块】")
    logger = setup_logger('board_concept')
    logger.info("开始采集概念板块")
    
    df = safe_call_with_retry(ak.stock_board_concept_name_em)
    if df is not None and not df.empty:
        with DuckDBManager() as db:
            db.execute("DELETE FROM board_concept")
            db.execute("DELETE FROM board_concept_stocks")
            
            board_data = []
            stock_data = []
            count = 0
            
            for _, row in df.head(max_count).iterrows():
                board_code = str(row.get('板块代码', ''))
                board_name = str(row.get('板块名称', ''))
                
                if not board_code:
                    continue
                
                board_data.append((board_code, board_name, 0, datetime.now()))
                
                stocks_df = safe_call_with_retry(ak.stock_board_concept_cons_em, symbol=board_name)
                if stocks_df is not None and not stocks_df.empty:
                    for _, sr in stocks_df.iterrows():
                        stock_code = str(sr.get('代码', ''))
                        stock_name = str(sr.get('名称', ''))
                        if stock_code:
                            stock_data.append((board_code, stock_code, stock_name))
                    board_data[-1] = (board_code, board_name, len(stocks_df), datetime.now())
                
                count += 1
                if count % 10 == 0:
                    print(f"\r  进度: {count}/{min(len(df), max_count)}", end='', flush=True)
                
                smart_sleep()
            
            db.insert_many('board_concept', board_data,
                ['board_code', 'board_name', 'stock_count', 'update_time'])
            db.insert_many('board_concept_stocks', stock_data,
                ['board_code', 'stock_code', 'stock_name'])
            
            print(f"\n  ✓ 概念板块: {len(board_data)}个, 成分股: {len(stock_data)}条")
            logger.info(f"采集完成: {len(board_data)}个板块, {len(stock_data)}条成分股")
    else:
        print("  ✗ 概念板块数据获取失败")
        logger.error("概念板块数据获取失败")


def collect_index_cons():
    """
    采集主要指数的成分股
    
    包括上证指数、深证成指、沪深300、上证50、中证500、创业板指等
    """
    print("\n【5. 指数成分股】")
    logger = setup_logger('index_cons')
    logger.info("开始采集指数成分股")
    
    indices = [
        ('000001', '上证指数'), ('399001', '深证成指'),
        ('000300', '沪深300'), ('000016', '上证50'),
        ('000905', '中证500'), ('399006', '创业板指'),
        ('000852', '中证1000'), ('399673', '创业板50'),
    ]
    
    with DuckDBManager() as db:
        success = 0
        total = len(indices)
        
        for idx_code, idx_name in indices:
            df = safe_call_with_retry(ak.index_stock_cons, symbol=idx_code)
            if df is not None and not df.empty:
                db.execute("DELETE FROM index_cons WHERE index_code = ?", (idx_code,))
                
                data = []
                for _, r in df.iterrows():
                    data.append((
                        idx_code, idx_name,
                        str(r.get('成分股代码', '')),
                        str(r.get('成分股名称', '')),
                        datetime.now()
                    ))
                
                db.insert_many('index_cons', data,
                    ['index_code', 'index_name', 'stock_code', 'stock_name', 'update_time'])
                
                print(f"  ✓ {idx_name}: {len(data)}只")
                logger.info(f"{idx_name}: {len(data)}只")
                success += 1
            else:
                print(f"  ✗ {idx_name}: 获取失败")
                logger.warning(f"{idx_name}: 获取失败")
            
            smart_sleep()
        
        print(f"  完成: {success}/{total}")
        logger.info(f"采集完成: {success}/{total}")


def collect_etf_list():
    """
    采集ETF列表
    
    获取所有上市ETF的代码和名称
    """
    print("\n【6. ETF列表】")
    logger = setup_logger('etf_list')
    logger.info("开始采集ETF列表")
    
    df = safe_call_with_retry(ak.fund_etf_spot_em)
    if df is not None and not df.empty:
        data = []
        for _, r in df.iterrows():
            data.append((
                str(r.get('代码', '')),
                str(r.get('名称', '')),
                'ETF',
                datetime.now()
            ))
        
        with DuckDBManager() as db:
            db.execute("DELETE FROM etf_list")
            db.insert_many('etf_list', data, ['code', 'name', 'etf_type', 'update_time'])
        
        print(f"  ✓ ETF列表: {len(data)}只")
        logger.info(f"采集完成: {len(data)}只")
    else:
        print("  ✗ ETF列表数据获取失败")
        logger.error("ETF列表数据获取失败")


def show_stats():
    """显示数据库统计信息"""
    print("\n📊 数据库统计:")
    with DuckDBManager() as db:
        tables = [
            ('stocks', '股票列表'),
            ('stock_info', '个股信息'),
            ('board_industry', '行业板块'),
            ('board_concept', '概念板块'),
            ('index_cons', '指数成分股'),
            ('etf_list', 'ETF列表'),
        ]
        for table, name in tables:
            count = db.get_table_count(table)
            print(f"   {name}: {count} 条")


def main():
    """主函数"""
    print("=" * 60)
    print("静态数据采集（建议每周/每月运行一次）")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    def print_help():
        print("\n可用命令:")
        print("  stock_list           - 股票列表")
        print("  stock_info [count]   - 个股基本信息 (可选数量)")
        print("  board                - 行业+概念板块")
        print("  index                - 指数成分股")
        print("  etf                  - ETF列表")
        print("  stats                - 查看统计")
        print("  check                - 检查系统状态")
        print("  config               - 查看当前配置")
    
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        
        if cmd == 'stock_list':
            collect_stock_list()
        elif cmd == 'stock_info':
            count = int(sys.argv[2]) if len(sys.argv) > 2 else None
            collect_stock_info(max_count=count)
        elif cmd == 'board':
            collect_board_industry()
            collect_board_concept()
        elif cmd == 'index':
            collect_index_cons()
        elif cmd == 'etf':
            collect_etf_list()
        elif cmd == 'stats':
            show_stats()
        elif cmd == 'check':
            ok, free_gb = check_disk_space()
            print(f"\n🔍 系统状态:")
            print(f"   磁盘空间: {'✓ 正常' if ok else '✗ 不足'} ({free_gb:.2f}GB)")
        elif cmd == 'config':
            print("\n⚙️ 当前配置:")
            for k, v in config.items():
                print(f"   {k}: {v}")
        else:
            print(f"未知命令: {cmd}")
            print_help()
    else:
        print_help()
        print("\n提示: 使用 run_collector.py 进入交互式菜单")
        return
    
    print("\n" + "=" * 60)
    print("静态数据采集完成！")
    print("=" * 60)


if __name__ == '__main__':
    main()
