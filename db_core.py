#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
AKShare 数据采集器 - 数据库核心模块
使用 DuckDB 作为主存储，SQLite 用于实时行情
"""

import duckdb
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any
import json

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / 'akshare_data'
DB_FILE = DATA_DIR / 'stock_data.duckdb'
REALTIME_DB = DATA_DIR / 'realtime.db'
CONFIG_FILE = BASE_DIR / 'collector_config.json'

DATA_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_CONFIG = {
    'request_interval': 1,
    'max_retries': 3,
    'retry_delay': 2,
    'min_disk_space_gb': 10,
}


def load_config() -> dict:
    """
    加载配置文件
    
    Returns:
        dict: 配置字典
    """
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, encoding='utf-8') as f:
            config = json.load(f)
            return {k: v for k, v in config.items() if not k.startswith('_')}
    return DEFAULT_CONFIG.copy()


class DuckDBManager:
    """
    DuckDB 数据库管理器
    
    用于管理股票数据的存储和查询
    支持大数据量（千万级行）的高效分析
    """
    
    def __init__(self, db_path: Path = None):
        """
        初始化数据库连接
        
        Args:
            db_path: 数据库文件路径，默认使用 stock_data.duckdb
        """
        self.db_path = db_path or DB_FILE
        self.conn = None
        self._connect()
        self._init_tables()
    
    def _connect(self):
        """建立数据库连接"""
        self.conn = duckdb.connect(str(self.db_path))
    
    def _init_tables(self):
        """初始化所有数据表"""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS stocks (
                code VARCHAR PRIMARY KEY,
                name VARCHAR,
                market VARCHAR,
                update_time TIMESTAMP
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_info (
                code VARCHAR PRIMARY KEY,
                name VARCHAR,
                industry VARCHAR,
                sector VARCHAR,
                list_date DATE,
                total_share DOUBLE,
                float_share DOUBLE,
                total_market_value DOUBLE,
                float_market_value DOUBLE,
                update_time TIMESTAMP
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS board_industry (
                board_code VARCHAR PRIMARY KEY,
                board_name VARCHAR,
                stock_count INTEGER,
                update_time TIMESTAMP
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS board_industry_stocks (
                board_code VARCHAR,
                stock_code VARCHAR,
                stock_name VARCHAR,
                PRIMARY KEY (board_code, stock_code)
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS board_concept (
                board_code VARCHAR PRIMARY KEY,
                board_name VARCHAR,
                stock_count INTEGER,
                update_time TIMESTAMP
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS board_concept_stocks (
                board_code VARCHAR,
                stock_code VARCHAR,
                stock_name VARCHAR,
                PRIMARY KEY (board_code, stock_code)
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS index_cons (
                index_code VARCHAR,
                index_name VARCHAR,
                stock_code VARCHAR,
                stock_name VARCHAR,
                update_time TIMESTAMP,
                PRIMARY KEY (index_code, stock_code)
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS etf_list (
                code VARCHAR PRIMARY KEY,
                name VARCHAR,
                etf_type VARCHAR,
                update_time TIMESTAMP
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_quotes (
                code VARCHAR,
                trade_date DATE,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                amount DOUBLE,
                change_pct DOUBLE,
                turnover_rate DOUBLE,
                main_net_inflow DOUBLE,
                main_net_inflow_pct DOUBLE,
                super_large_net_inflow DOUBLE,
                large_net_inflow DOUBLE,
                medium_net_inflow DOUBLE,
                small_net_inflow DOUBLE,
                update_time TIMESTAMP,
                PRIMARY KEY (code, trade_date)
            )
        """)
        
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_quotes_code ON daily_quotes(code)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_quotes_date ON daily_quotes(trade_date)")
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS index_daily (
                code VARCHAR,
                trade_date DATE,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                amount DOUBLE,
                update_time TIMESTAMP,
                PRIMARY KEY (code, trade_date)
            )
        """)
        
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_index_code ON index_daily(code)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_index_date ON index_daily(trade_date)")
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS etf_daily (
                code VARCHAR,
                trade_date DATE,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                amount DOUBLE,
                change_pct DOUBLE,
                update_time TIMESTAMP,
                PRIMARY KEY (code, trade_date)
            )
        """)
        
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_etf_code ON etf_daily(code)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_etf_date ON etf_daily(trade_date)")
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS north_flow (
                trade_date DATE PRIMARY KEY,
                net_inflow DOUBLE,
                balance DOUBLE,
                cumulative DOUBLE,
                update_time TIMESTAMP
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS macro_data (
                indicator VARCHAR,
                report_date DATE,
                value DOUBLE,
                unit VARCHAR,
                update_time TIMESTAMP,
                PRIMARY KEY (indicator, report_date)
            )
        """)
    
    def execute(self, sql: str, params: tuple = None):
        """
        执行SQL语句
        
        Args:
            sql: SQL语句
            params: 参数元组
        """
        if params:
            return self.conn.execute(sql, params)
        return self.conn.execute(sql)
    
    def fetch_df(self, sql: str, params: tuple = None):
        """
        执行查询并返回DataFrame
        
        Args:
            sql: SQL语句
            params: 参数元组
            
        Returns:
            pandas.DataFrame: 查询结果
        """
        if params:
            return self.conn.execute(sql, params).fetchdf()
        return self.conn.execute(sql).fetchdf()
    
    def fetch_all(self, sql: str, params: tuple = None) -> List[tuple]:
        """
        执行查询并返回所有结果
        
        Args:
            sql: SQL语句
            params: 参数元组
            
        Returns:
            List[tuple]: 查询结果列表
        """
        if params:
            return self.conn.execute(sql, params).fetchall()
        return self.conn.execute(sql).fetchall()
    
    def fetch_one(self, sql: str, params: tuple = None) -> Optional[tuple]:
        """
        执行查询并返回单条结果
        
        Args:
            sql: SQL语句
            params: 参数元组
            
        Returns:
            tuple: 查询结果，无结果返回None
        """
        if params:
            return self.conn.execute(sql, params).fetchone()
        return self.conn.execute(sql).fetchone()
    
    def insert_many(self, table: str, data: List[tuple], columns: List[str] = None):
        """
        批量插入数据
        
        Args:
            table: 表名
            data: 数据列表
            columns: 列名列表
        """
        if not data:
            return
        
        if columns:
            cols = ', '.join(columns)
            placeholders = ', '.join(['?' for _ in columns])
            sql = f"INSERT OR REPLACE INTO {table} ({cols}) VALUES ({placeholders})"
        else:
            placeholders = ', '.join(['?' for _ in data[0]])
            sql = f"INSERT OR REPLACE INTO {table} VALUES ({placeholders})"
        
        self.conn.executemany(sql, data)
    
    def get_table_count(self, table: str) -> int:
        """
        获取表的记录数
        
        Args:
            table: 表名
            
        Returns:
            int: 记录数
        """
        result = self.fetch_one(f"SELECT COUNT(*) FROM {table}")
        return result[0] if result else 0
    
    def get_stock_list(self) -> List[Dict]:
        """
        获取股票列表
        
        Returns:
            List[Dict]: 股票列表
        """
        rows = self.fetch_all("SELECT code, name, market FROM stocks ORDER BY code")
        return [{'code': r[0], 'name': r[1], 'market': r[2]} for r in rows]
    
    def get_latest_trade_date(self, code: str) -> Optional[str]:
        """
        获取指定股票的最新交易日期
        
        Args:
            code: 股票代码
            
        Returns:
            str: 最新交易日期，无数据返回None
        """
        result = self.fetch_one(
            "SELECT MAX(trade_date) FROM daily_quotes WHERE code = ?",
            (code,)
        )
        return str(result[0]) if result and result[0] else None
    
    def get_date_range(self, table: str = 'daily_quotes') -> Dict[str, str]:
        """
        获取数据的日期范围
        
        Args:
            table: 表名
            
        Returns:
            Dict: {'min_date': '...', 'max_date': '...'}
        """
        result = self.fetch_one(f"SELECT MIN(trade_date), MAX(trade_date) FROM {table}")
        if result and result[0]:
            return {'min_date': str(result[0]), 'max_date': str(result[1])}
        return {'min_date': None, 'max_date': None}
    
    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class SQLiteManager:
    """
    SQLite 数据库管理器
    
    用于实时行情数据存储
    支持高频写入场景
    """
    
    def __init__(self, db_path: Path = None):
        """
        初始化数据库连接
        
        Args:
            db_path: 数据库文件路径，默认使用 realtime.db
        """
        self.db_path = db_path or REALTIME_DB
        self.conn = None
        self._connect()
        self._init_tables()
    
    def _connect(self):
        """建立数据库连接"""
        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
    
    def _init_tables(self):
        """初始化实时行情表"""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS realtime_quotes (
                code VARCHAR PRIMARY KEY,
                name VARCHAR,
                price DOUBLE,
                change_pct DOUBLE,
                volume DOUBLE,
                amount DOUBLE,
                high DOUBLE,
                low DOUBLE,
                open DOUBLE,
                pre_close DOUBLE,
                bid_price1 DOUBLE,
                bid_volume1 DOUBLE,
                ask_price1 DOUBLE,
                ask_volume1 DOUBLE,
                update_time TIMESTAMP
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS tick_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code VARCHAR,
                trade_time TIMESTAMP,
                price DOUBLE,
                volume DOUBLE,
                amount DOUBLE,
                buy_sell VARCHAR,
                update_time TIMESTAMP
            )
        """)
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_tick_code ON tick_data(code)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_tick_time ON tick_data(trade_time)")
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS minute_quotes (
                code VARCHAR,
                trade_time TIMESTAMP,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                amount DOUBLE,
                update_time TIMESTAMP,
                PRIMARY KEY (code, trade_time)
            )
        """)
        
        self.conn.commit()
    
    def execute(self, sql: str, params: tuple = None):
        """
        执行SQL语句
        
        Args:
            sql: SQL语句
            params: 参数元组
        """
        if params:
            return self.conn.execute(sql, params)
        return self.conn.execute(sql)
    
    def fetch_all(self, sql: str, params: tuple = None) -> List[sqlite3.Row]:
        """
        执行查询并返回所有结果
        
        Args:
            sql: SQL语句
            params: 参数元组
            
        Returns:
            List[sqlite3.Row]: 查询结果列表
        """
        if params:
            return self.conn.execute(sql, params).fetchall()
        return self.conn.execute(sql).fetchall()
    
    def fetch_one(self, sql: str, params: tuple = None) -> Optional[sqlite3.Row]:
        """
        执行查询并返回单条结果
        
        Args:
            sql: SQL语句
            params: 参数元组
            
        Returns:
            sqlite3.Row: 查询结果，无结果返回None
        """
        if params:
            return self.conn.execute(sql, params).fetchone()
        return self.conn.execute(sql).fetchone()
    
    def insert_one(self, table: str, data: dict):
        """
        插入单条数据
        
        Args:
            table: 表名
            data: 数据字典
        """
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?' for _ in data])
        sql = f"INSERT OR REPLACE INTO {table} ({columns}) VALUES ({placeholders})"
        self.conn.execute(sql, tuple(data.values()))
        self.conn.commit()
    
    def insert_many(self, table: str, data: List[tuple], columns: List[str]):
        """
        批量插入数据
        
        Args:
            table: 表名
            data: 数据列表
            columns: 列名列表
        """
        if not data:
            return
        
        cols = ', '.join(columns)
        placeholders = ', '.join(['?' for _ in columns])
        sql = f"INSERT OR REPLACE INTO {table} ({cols}) VALUES ({placeholders})"
        
        self.conn.executemany(sql, data)
        self.conn.commit()
    
    def commit(self):
        """提交事务"""
        self.conn.commit()
    
    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def check_disk_space(path: Path = DATA_DIR) -> tuple:
    """
    检查磁盘空间
    
    Args:
        path: 检查路径
        
    Returns:
        tuple: (是否充足, 可用空间GB)
    """
    import shutil
    check_path = path if path.exists() else path.parent
    total, used, free = shutil.disk_usage(check_path)
    free_gb = free / (1024 ** 3)
    config = load_config()
    min_space = config.get('min_disk_space_gb', 10)
    return free_gb >= min_space, free_gb


def check_network() -> bool:
    """
    检查网络连接
    
    Returns:
        bool: 网络是否可用
    """
    import socket
    try:
        socket.create_connection(("www.baidu.com", 80), timeout=5)
        return True
    except OSError:
        return False


if __name__ == '__main__':
    print("DuckDB 数据库核心模块")
    print(f"数据库路径: {DB_FILE}")
    print(f"实时数据库路径: {REALTIME_DB}")
    
    with DuckDBManager() as db:
        print("\n数据表统计:")
        tables = ['stocks', 'stock_info', 'board_industry', 'board_concept', 
                  'daily_quotes', 'index_daily', 'etf_daily', 'north_flow', 'macro_data']
        for t in tables:
            count = db.get_table_count(t)
            print(f"  {t}: {count} 条记录")
