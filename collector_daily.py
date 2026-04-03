#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
AKShare 数据采集器 - 每日数据采集模块
采集每日更新的数据：日线行情、资金流向、指数历史、ETF历史、北向资金、宏观数据
使用 DuckDB 存储
"""

import sys
import time
import logging
from datetime import datetime, timedelta
from typing import Optional, List

try:
    import akshare as ak
    import pandas as pd
except ImportError:
    print("请先安装依赖: pip install akshare pandas")
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
                return None


def collect_stock_daily(codes: List[str] = None, mode: str = 'auto', days: int = 30):
    """
    采集个股日线行情和资金流向数据
    
    Args:
        codes: 股票代码列表，None则使用全部A股
        mode: 模式 - auto(自动), full(全量), increment(增量)
        days: 增量模式下回溯天数
    """
    print("\n【1. 股票日线+资金流】")
    logger = setup_logger('stock_daily')
    logger.info(f"开始采集股票日线数据，模式: {mode}")
    
    ok, free_gb = check_disk_space()
    if not ok:
        print(f"  ✗ 磁盘空间不足: {free_gb:.2f}GB")
        return
    
    with DuckDBManager() as db:
        if codes is None:
            stocks = db.get_stock_list()
            codes = [s['code'] for s in stocks]
        
        if not codes:
            print("  ✗ 没有股票代码，请先运行 stock_list")
            return
        
        total = len(codes)
        success = 0
        failed = 0
        total_rows = 0
        
        print(f"  共 {total} 只股票需要采集")
        
        today = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
        
        for i, code in enumerate(codes, 1):
            if i % 10 == 0 or i == total:
                print(f"\r  进度: {i}/{total} (成功:{success}, 失败:{failed}, 数据:{total_rows}行)", 
                      end='', flush=True)
            
            try:
                if mode == 'full':
                    df_quotes = safe_call_with_retry(
                        ak.stock_zh_a_hist, symbol=code, period='daily',
                        start_date='19900101', end_date=today, adjust=''
                    )
                else:
                    latest = db.get_latest_trade_date(code)
                    
                    if mode == 'increment' and latest:
                        start = (datetime.strptime(str(latest), '%Y-%m-%d') + timedelta(days=1)).strftime('%Y%m%d')
                    elif mode == 'auto' and latest:
                        start = (datetime.strptime(str(latest), '%Y-%m-%d') + timedelta(days=1)).strftime('%Y%m%d')
                    else:
                        start = start_date
                    
                    if start > today:
                        success += 1
                        smart_sleep()
                        continue
                    
                    df_quotes = safe_call_with_retry(
                        ak.stock_zh_a_hist, symbol=code, period='daily',
                        start_date=start, end_date=today, adjust=''
                    )
                
                df_flow = safe_call_with_retry(
                    ak.stock_individual_fund_flow, stock=code, market='sh' if code.startswith('6') else 'sz'
                )
                
                if df_quotes is not None and not df_quotes.empty:
                    col_map = {
                        '日期': 'trade_date', '开盘': 'open', '收盘': 'close',
                        '最高': 'high', '最低': 'low', '成交量': 'volume',
                        '成交额': 'amount', '换手率': 'turnover_rate'
                    }
                    df_quotes = df_quotes.rename(columns=col_map)
                    
                    if 'trade_date' in df_quotes.columns:
                        df_quotes['trade_date'] = pd.to_datetime(df_quotes['trade_date']).dt.date
                    
                    flow_dict = {}
                    if df_flow is not None and not df_flow.empty:
                        for _, row in df_flow.iterrows():
                            date_str = str(row.get('日期', ''))
                            if date_str:
                                date_key = date_str.replace('-', '')
                                flow_dict[date_key] = {
                                    'main_net_inflow': float(row.get('主力净流入-净额', 0) or 0),
                                    'main_net_inflow_pct': float(row.get('主力净流入-净占比', 0) or 0),
                                    'super_large_net_inflow': float(row.get('超大单净流入-净额', 0) or 0),
                                    'large_net_inflow': float(row.get('大单净流入-净额', 0) or 0),
                                    'medium_net_inflow': float(row.get('中单净流入-净额', 0) or 0),
                                    'small_net_inflow': float(row.get('小单净流入-净额', 0) or 0),
                                }
                    
                    data = []
                    for _, row in df_quotes.iterrows():
                        trade_date = row.get('trade_date')
                        if trade_date is None:
                            continue
                        
                        date_str = str(trade_date).replace('-', '')
                        flow = flow_dict.get(date_str, {})
                        
                        data.append((
                            code,
                            trade_date,
                            float(row.get('open', 0) or 0),
                            float(row.get('high', 0) or 0),
                            float(row.get('low', 0) or 0),
                            float(row.get('close', 0) or 0),
                            float(row.get('volume', 0) or 0),
                            float(row.get('amount', 0) or 0),
                            float(row.get('涨跌幅', 0) or 0),
                            float(row.get('turnover_rate', 0) or 0),
                            flow.get('main_net_inflow', 0),
                            flow.get('main_net_inflow_pct', 0),
                            flow.get('super_large_net_inflow', 0),
                            flow.get('large_net_inflow', 0),
                            flow.get('medium_net_inflow', 0),
                            flow.get('small_net_inflow', 0),
                            datetime.now()
                        ))
                    
                    if data:
                        db.insert_many('daily_quotes', data, [
                            'code', 'trade_date', 'open', 'high', 'low', 'close',
                            'volume', 'amount', 'change_pct', 'turnover_rate',
                            'main_net_inflow', 'main_net_inflow_pct',
                            'super_large_net_inflow', 'large_net_inflow',
                            'medium_net_inflow', 'small_net_inflow', 'update_time'
                        ])
                        total_rows += len(data)
                    
                    success += 1
                    logger.info(f"{code}: {len(data)}行")
                else:
                    failed += 1
                    logger.warning(f"{code}: 无数据")
                
            except Exception as e:
                failed += 1
                logger.error(f"{code}: {e}")
            
            smart_sleep()
        
        print(f"\n  ✓ 完成: 成功 {success}, 失败 {failed}, 共 {total_rows} 行数据")
        logger.info(f"采集完成: 成功 {success}, 失败 {failed}, 共 {total_rows} 行")


def collect_index_daily():
    """
    采集主要指数的历史行情数据
    
    采集上证指数、深证成指、沪深300、上证50、中证500、创业板指等
    """
    print("\n【2. 指数历史行情】")
    logger = setup_logger('index_daily')
    logger.info("开始采集指数历史行情")
    
    indices = [
        ('000001', '上证指数', 'sh'), ('399001', '深证成指', 'sz'),
        ('000300', '沪深300', 'sh'), ('000016', '上证50', 'sh'),
        ('000905', '中证500', 'sh'), ('399006', '创业板指', 'sz'),
    ]
    
    with DuckDBManager() as db:
        success = 0
        
        for idx_code, idx_name, market in indices:
            df = safe_call_with_retry(ak.stock_zh_index_daily, symbol=f'{market}{idx_code}')
            if df is not None and not df.empty:
                df = df.rename(columns={'date': 'trade_date'})
                
                data = []
                for _, row in df.tail(500).iterrows():
                    trade_date = row.get('trade_date')
                    if trade_date:
                        if hasattr(trade_date, 'strftime'):
                            trade_date = trade_date.date()
                        data.append((
                            idx_code,
                            trade_date,
                            float(row.get('open', 0) or 0),
                            float(row.get('high', 0) or 0),
                            float(row.get('low', 0) or 0),
                            float(row.get('close', 0) or 0),
                            float(row.get('volume', 0) or 0),
                            float(row.get('amount', 0) or 0),
                            datetime.now()
                        ))
                
                if data:
                    db.insert_many('index_daily', data, [
                        'code', 'trade_date', 'open', 'high', 'low', 'close',
                        'volume', 'amount', 'update_time'
                    ])
                
                print(f"  ✓ {idx_name}: {len(data)}条")
                logger.info(f"{idx_name}: {len(data)}条")
                success += 1
            else:
                print(f"  ✗ {idx_name}: 获取失败")
                logger.warning(f"{idx_name}: 获取失败")
            
            smart_sleep()
        
        print(f"  完成: {success}/{len(indices)}")


def collect_etf_daily(codes: List[str] = None, days: int = 30):
    """
    采集ETF历史行情数据
    
    Args:
        codes: ETF代码列表，None则使用默认列表
        days: 回溯天数
    """
    print("\n【3. ETF历史行情】")
    logger = setup_logger('etf_daily')
    logger.info("开始采集ETF历史行情")
    
    if codes is None:
        codes = ['510300', '510050', '159915', '510500', '159919', '512880']
    
    today = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
    
    with DuckDBManager() as db:
        success = 0
        
        for etf_code in codes:
            df = safe_call_with_retry(
                ak.fund_etf_hist_em, symbol=etf_code, period='daily',
                start_date=start_date, end_date=today, adjust=''
            )
            if df is not None and not df.empty:
                col_map = {
                    '日期': 'trade_date', '开盘': 'open', '收盘': 'close',
                    '最高': 'high', '最低': 'low', '成交量': 'volume',
                    '成交额': 'amount', '涨跌幅': 'change_pct'
                }
                df = df.rename(columns=col_map)
                
                data = []
                for _, row in df.iterrows():
                    date_val = row.get('trade_date')
                    if date_val:
                        if hasattr(date_val, 'strftime'):
                            trade_date = date_val.date()
                        else:
                            trade_date = datetime.strptime(str(date_val), '%Y-%m-%d').date()
                        
                        data.append((
                            etf_code,
                            trade_date,
                            float(row.get('open', 0) or 0),
                            float(row.get('high', 0) or 0),
                            float(row.get('low', 0) or 0),
                            float(row.get('close', 0) or 0),
                            float(row.get('volume', 0) or 0),
                            float(row.get('amount', 0) or 0),
                            float(row.get('change_pct', 0) or 0),
                            datetime.now()
                        ))
                
                if data:
                    db.insert_many('etf_daily', data, [
                        'code', 'trade_date', 'open', 'high', 'low', 'close',
                        'volume', 'amount', 'change_pct', 'update_time'
                    ])
                
                print(f"  ✓ {etf_code}: {len(data)}条")
                logger.info(f"{etf_code}: {len(data)}条")
                success += 1
            else:
                print(f"  ✗ {etf_code}: 获取失败")
                logger.warning(f"{etf_code}: 获取失败")
            
            smart_sleep()
        
        print(f"  完成: {success}/{len(codes)}")


def collect_north_flow():
    """
    采集北向资金数据
    
    记录外资通过沪港通、深港通流入A股的资金情况
    """
    print("\n【4. 北向资金】")
    logger = setup_logger('north_flow')
    logger.info("开始采集北向资金")
    
    df = safe_call_with_retry(ak.stock_hsgt_north_net_flow_in_em)
    if df is not None and not df.empty:
        with DuckDBManager() as db:
            data = []
            for _, row in df.iterrows():
                date_val = row.get('日期') or row.get(df.columns[0])
                if date_val:
                    if hasattr(date_val, 'strftime'):
                        trade_date = date_val.date()
                    else:
                        trade_date = datetime.strptime(str(date_val).split()[0], '%Y-%m-%d').date()
                    
                    data.append((
                        trade_date,
                        float(row.get('当日净流入', 0) or row.get(df.columns[1], 0) or 0),
                        float(row.get('当日资金余额', 0) or row.get(df.columns[2], 0) or 0),
                        float(row.get('累计净流入', 0) or row.get(df.columns[3], 0) or 0),
                        datetime.now()
                    ))
            
            if data:
                db.insert_many('north_flow', data, [
                    'trade_date', 'net_inflow', 'balance', 'cumulative', 'update_time'
                ])
            
            print(f"  ✓ 北向资金: {len(data)}条")
            logger.info(f"采集完成: {len(data)}条")
    else:
        print("  ✗ 北向资金数据获取失败")
        logger.error("北向资金数据获取失败")


def collect_macro():
    """
    采集宏观经济数据
    
    包括GDP、CPI、PPI、PMI等重要经济指标
    """
    print("\n【5. 宏观数据】")
    logger = setup_logger('macro_data')
    logger.info("开始采集宏观数据")
    
    macros = [
        ('gdp', ak.macro_china_gdp, '中国GDP'),
        ('cpi', ak.macro_china_cpi, '中国CPI'),
        ('ppi', ak.macro_china_ppi, '中国PPI'),
        ('pmi', ak.macro_china_pmi, '中国PMI'),
    ]
    
    with DuckDBManager() as db:
        success = 0
        
        for indicator, func, desc in macros:
            df = safe_call_with_retry(func)
            if df is not None and not df.empty:
                data = []
                for _, row in df.iterrows():
                    date_val = row.get('日期') or row.get(df.columns[0])
                    value = row.get('今值') or row.get(df.columns[1])
                    
                    if date_val:
                        if hasattr(date_val, 'strftime'):
                            report_date = date_val.date()
                        else:
                            try:
                                report_date = datetime.strptime(str(date_val).split()[0], '%Y-%m-%d').date()
                            except:
                                continue
                        
                        data.append((
                            indicator,
                            report_date,
                            float(value or 0),
                            '',
                            datetime.now()
                        ))
                
                if data:
                    db.insert_many('macro_data', data, [
                        'indicator', 'report_date', 'value', 'unit', 'update_time'
                    ])
                
                print(f"  ✓ {desc}: {len(data)}条")
                logger.info(f"{desc}: {len(data)}条")
                success += 1
            else:
                print(f"  ✗ {desc}: 获取失败")
                logger.warning(f"{desc}: 获取失败")
            
            smart_sleep()
        
        print(f"  完成: {success}/{len(macros)}")


def show_stats():
    """显示数据库统计信息"""
    print("\n📊 数据库统计:")
    with DuckDBManager() as db:
        tables = [
            ('daily_quotes', '日线行情'),
            ('index_daily', '指数历史'),
            ('etf_daily', 'ETF历史'),
            ('north_flow', '北向资金'),
            ('macro_data', '宏观数据'),
        ]
        for table, name in tables:
            count = db.get_table_count(table)
            print(f"   {name}: {count} 条")
        
        date_range = db.get_date_range('daily_quotes')
        if date_range['min_date']:
            print(f"\n   日线数据日期范围: {date_range['min_date']} ~ {date_range['max_date']}")


def main():
    """主函数"""
    print("=" * 60)
    print("每日数据采集（建议每个交易日收盘后运行）")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    def print_help():
        print("\n可用命令:")
        print("  stock [mode]  - 股票日线+资金流 (mode: auto/full/increment)")
        print("  full          - 全量下载(从上市日)")
        print("  increment     - 增量更新")
        print("  index         - 指数历史")
        print("  etf           - ETF历史")
        print("  north         - 北向资金")
        print("  macro         - 宏观数据")
        print("  stats         - 查看统计")
        print("  check         - 检查系统状态")
        print("  config        - 查看当前配置")
    
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        
        if cmd == 'stock':
            mode = sys.argv[2] if len(sys.argv) > 2 else 'auto'
            collect_stock_daily(mode=mode)
        elif cmd == 'full':
            collect_stock_daily(mode='full')
        elif cmd == 'increment':
            collect_stock_daily(mode='increment')
        elif cmd == 'index':
            collect_index_daily()
        elif cmd == 'etf':
            collect_etf_daily()
        elif cmd == 'north':
            collect_north_flow()
        elif cmd == 'macro':
            collect_macro()
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
    print("每日数据采集完成！")
    print("=" * 60)


if __name__ == '__main__':
    main()
