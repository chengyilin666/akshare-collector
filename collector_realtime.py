#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
AKShare 数据采集器 - 实时行情采集模块
采集实时行情数据，使用 SQLite 存储（支持高频写入）
"""

import sys
import time
import logging
from datetime import datetime
from typing import Optional, List

try:
    import akshare as ak
    import pandas as pd
except ImportError:
    print("请先安装依赖: pip install akshare pandas")
    sys.exit(1)

from db_core import SQLiteManager, DuckDBManager, load_config, check_disk_space, DATA_DIR

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


def smart_sleep(interval: float = None):
    """智能等待"""
    interval = interval or config.get('request_interval', 1)
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


class RealtimeCollector:
    """
    实时行情采集器
    
    使用 SQLite 存储实时数据，支持高频写入
    """
    
    def __init__(self, interval: float = 5):
        """
        初始化采集器
        
        Args:
            interval: 监控间隔（秒）
        """
        self.interval = interval
        self.logger = setup_logger('realtime')
    
    def snapshot_all(self):
        """
        全市场快照
        
        采集所有A股实时行情并存储到SQLite
        """
        print("\n【全市场快照】")
        self.logger.info("开始全市场快照")
        
        df = safe_call_with_retry(ak.stock_zh_a_spot_em)
        if df is not None and not df.empty:
            with SQLiteManager() as db:
                count = 0
                for _, row in df.iterrows():
                    code = str(row.get('代码', ''))
                    if not code:
                        continue
                    
                    # 只采集上证和深证A股
                    if not (code.startswith('6') or code.startswith('0') or code.startswith('3')):
                        continue
                    
                    data = {
                        'code': code,
                        'name': str(row.get('名称', '')),
                        'price': float(row.get('最新价', 0) or 0),
                        'change_pct': float(row.get('涨跌幅', 0) or 0),
                        'volume': float(row.get('成交量', 0) or 0),
                        'amount': float(row.get('成交额', 0) or 0),
                        'high': float(row.get('最高', 0) or 0),
                        'low': float(row.get('最低', 0) or 0),
                        'open': float(row.get('今开', 0) or 0),
                        'pre_close': float(row.get('昨收', 0) or 0),
                        'bid_price1': float(row.get('买一', 0) or 0),
                        'bid_volume1': float(row.get('买一量', 0) or 0),
                        'ask_price1': float(row.get('卖一', 0) or 0),
                        'ask_volume1': float(row.get('卖一量', 0) or 0),
                        'update_time': datetime.now()
                    }
                    db.insert_one('realtime_quotes', data)
                    count += 1
                
                print(f"  ✓ 快照完成: {count}只股票")
                self.logger.info(f"快照完成: {count}只股票")
        else:
            print("  ✗ 快照数据获取失败")
            self.logger.error("快照数据获取失败")
    
    def get_realtime_quote(self, code: str) -> Optional[dict]:
        """
        获取单只股票实时行情
        
        Args:
            code: 股票代码
            
        Returns:
            dict: 行情数据
        """
        df = safe_call_with_retry(ak.stock_zh_a_spot_em)
        if df is not None and not df.empty:
            row = df[df['代码'] == code]
            if not row.empty:
                r = row.iloc[0]
                return {
                    'code': code,
                    'name': str(r.get('名称', '')),
                    'price': float(r.get('最新价', 0) or 0),
                    'change_pct': float(r.get('涨跌幅', 0) or 0),
                    'volume': float(r.get('成交量', 0) or 0),
                    'amount': float(r.get('成交额', 0) or 0),
                    'high': float(r.get('最高', 0) or 0),
                    'low': float(r.get('最低', 0) or 0),
                    'open': float(r.get('今开', 0) or 0),
                    'pre_close': float(r.get('昨收', 0) or 0),
                }
        return None
    
    def get_tick_data(self, code: str) -> Optional[pd.DataFrame]:
        """
        获取分时成交数据
        
        Args:
            code: 股票代码
            
        Returns:
            pandas.DataFrame: 分时成交数据
        """
        df = safe_call_with_retry(ak.stock_intraday_em, symbol=code)
        return df
    
    def get_minute_data(self, code: str, period: int = 1) -> Optional[pd.DataFrame]:
        """
        获取分钟K线数据
        
        Args:
            code: 股票代码
            period: 周期 (1/5/15/30/60)
            
        Returns:
            pandas.DataFrame: 分钟K线数据
        """
        period_map = {1: '1', 5: '5', 15: '15', 30: '30', 60: '60'}
        df = safe_call_with_retry(
            ak.stock_zh_a_minute, symbol=code, period=period_map.get(period, '1')
        )
        return df
    
    def get_bid_data(self, code: str) -> Optional[dict]:
        """
        获取五档盘口数据
        
        Args:
            code: 股票代码
            
        Returns:
            dict: 五档数据
        """
        df = safe_call_with_retry(ak.stock_bid_ask_em, symbol=code)
        if df is not None and not df.empty:
            result = {'code': code}
            for _, row in df.iterrows():
                item = str(row.get('item', ''))
                value = row.get('value', 0)
                result[item] = value
            return result
        return None
    
    def monitor_stocks(self, codes: List[str], duration: int = 0):
        """
        持续监控股票行情
        
        Args:
            codes: 股票代码列表
            duration: 监控时长（秒），0表示持续监控
        """
        print(f"\n【实时监控】监控股票: {', '.join(codes)}")
        print(f"  刷新间隔: {self.interval}秒")
        print("  按 Ctrl+C 停止监控\n")
        
        start_time = time.time()
        
        try:
            while True:
                if duration > 0 and (time.time() - start_time) >= duration:
                    break
                
                print(f"\r[{datetime.now().strftime('%H:%M:%S')}] ", end='')
                
                for code in codes:
                    quote = self.get_realtime_quote(code)
                    if quote:
                        change = quote['change_pct']
                        sign = '+' if change >= 0 else ''
                        print(f"{quote['name']}({code}): {quote['price']:.2f} {sign}{change:.2f}%  ", end='')
                    else:
                        print(f"{code}: --  ", end='')
                
                print('', end='', flush=True)
                time.sleep(self.interval)
                
        except KeyboardInterrupt:
            print("\n\n监控已停止")
    
    def save_tick_to_db(self, code: str):
        """
        保存分时成交数据到数据库
        
        Args:
            code: 股票代码
        """
        df = self.get_tick_data(code)
        if df is not None and not df.empty:
            with SQLiteManager() as db:
                data = []
                for _, row in df.iterrows():
                    time_val = row.get('时间') or row.get(df.columns[0])
                    data.append((
                        code,
                        time_val,
                        float(row.get('成交价', 0) or 0),
                        float(row.get('成交量', 0) or 0),
                        float(row.get('成交额', 0) or 0),
                        str(row.get('性质', '')),
                        datetime.now()
                    ))
                
                db.insert_many('tick_data', data, [
                    'code', 'trade_time', 'price', 'volume', 'amount', 'buy_sell', 'update_time'
                ])
                
                print(f"  ✓ {code} 分时成交: {len(data)}条")
                self.logger.info(f"{code} 分时成交: {len(data)}条")
        else:
            print(f"  ✗ {code} 分时成交数据获取失败")


def show_realtime_stats():
    """显示实时数据库统计"""
    print("\n📊 实时数据库统计:")
    with SQLiteManager() as db:
        tables = ['realtime_quotes', 'tick_data', 'minute_quotes']
        for table in tables:
            result = db.fetch_one(f"SELECT COUNT(*) FROM {table}")
            count = result[0] if result else 0
            print(f"   {table}: {count} 条")


def main():
    """主函数"""
    print("=" * 60)
    print("实时数据采集")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    def print_help():
        print("\n可用命令:")
        print("  all              - 全市场快照")
        print("  monitor [codes]  - 实时监控 (默认: 600519 000001 300750)")
        print("  stock <code>     - 单股实时行情")
        print("  tick <code>      - 分时成交")
        print("  minute <code> [period] - 分钟K线 (period: 1/5/15/30/60)")
        print("  bid <code>       - 五档盘口")
        print("  stats            - 查看统计")
        print("  check            - 检查系统状态")
    
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        collector = RealtimeCollector()
        
        if cmd == 'all':
            collector.snapshot_all()
        elif cmd == 'monitor':
            codes = sys.argv[2:] if len(sys.argv) > 2 else ['600519', '000001', '300750']
            collector.monitor_stocks(codes)
        elif cmd == 'stock':
            if len(sys.argv) > 2:
                code = sys.argv[2]
                quote = collector.get_realtime_quote(code)
                if quote:
                    print(f"\n{quote['name']} ({code})")
                    print(f"  最新价: {quote['price']:.2f}")
                    print(f"  涨跌幅: {quote['change_pct']:.2f}%")
                    print(f"  成交量: {quote['volume']:.0f}")
                    print(f"  成交额: {quote['amount']:.0f}")
                    print(f"  最高: {quote['high']:.2f}")
                    print(f"  最低: {quote['low']:.2f}")
                    print(f"  今开: {quote['open']:.2f}")
                    print(f"  昨收: {quote['pre_close']:.2f}")
                else:
                    print(f"  ✗ {code} 行情获取失败")
            else:
                print("  请指定股票代码")
        elif cmd == 'tick':
            if len(sys.argv) > 2:
                code = sys.argv[2]
                df = collector.get_tick_data(code)
                if df is not None and not df.empty:
                    print(f"\n{code} 分时成交 (最近10条):")
                    print(df.tail(10).to_string(index=False))
                else:
                    print(f"  ✗ {code} 分时成交获取失败")
            else:
                print("  请指定股票代码")
        elif cmd == 'minute':
            if len(sys.argv) > 2:
                code = sys.argv[2]
                period = int(sys.argv[3]) if len(sys.argv) > 3 else 1
                df = collector.get_minute_data(code, period)
                if df is not None and not df.empty:
                    print(f"\n{code} {period}分钟K线 (最近10条):")
                    print(df.tail(10).to_string(index=False))
                else:
                    print(f"  ✗ {code} 分钟K线获取失败")
            else:
                print("  请指定股票代码")
        elif cmd == 'bid':
            if len(sys.argv) > 2:
                code = sys.argv[2]
                bid = collector.get_bid_data(code)
                if bid:
                    print(f"\n{code} 五档盘口:")
                    print(f"  买一: {bid.get('买一', 0)} ({bid.get('买一量', 0)})")
                    print(f"  买二: {bid.get('买二', 0)} ({bid.get('买二量', 0)})")
                    print(f"  买三: {bid.get('买三', 0)} ({bid.get('买三量', 0)})")
                    print(f"  买四: {bid.get('买四', 0)} ({bid.get('买四量', 0)})")
                    print(f"  买五: {bid.get('买五', 0)} ({bid.get('买五量', 0)})")
                    print(f"  卖一: {bid.get('卖一', 0)} ({bid.get('卖一量', 0)})")
                    print(f"  卖二: {bid.get('卖二', 0)} ({bid.get('卖二量', 0)})")
                    print(f"  卖三: {bid.get('卖三', 0)} ({bid.get('卖三量', 0)})")
                    print(f"  卖四: {bid.get('卖四', 0)} ({bid.get('卖四量', 0)})")
                    print(f"  卖五: {bid.get('卖五', 0)} ({bid.get('卖五量', 0)})")
                else:
                    print(f"  ✗ {code} 五档盘口获取失败")
            else:
                print("  请指定股票代码")
        elif cmd == 'stats':
            show_realtime_stats()
        elif cmd == 'check':
            ok, free_gb = check_disk_space()
            print(f"\n🔍 系统状态:")
            print(f"   磁盘空间: {'✓ 正常' if ok else '✗ 不足'} ({free_gb:.2f}GB)")
        else:
            print(f"未知命令: {cmd}")
            print_help()
    else:
        print_help()
        print("\n提示: 使用 run_collector.py 进入交互式菜单")
        return
    
    print("\n" + "=" * 60)
    print("实时数据采集完成！")
    print("=" * 60)


if __name__ == '__main__':
    main()
