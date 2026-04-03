# AKShare 数据采集器 (DuckDB版)

A股市场数据采集工具，使用 DuckDB 作为主存储，SQLite 用于实时行情。

## 架构说明

```
数据存储架构:
├── stock_data.duckdb    # DuckDB 主数据库 (静态数据 + 历史数据)
│   ├── stocks           # 股票列表
│   ├── stock_info       # 个股基本信息
│   ├── board_industry   # 行业板块
│   ├── board_concept    # 概念板块
│   ├── index_cons       # 指数成分股
│   ├── etf_list         # ETF列表
│   ├── daily_quotes     # 日线行情+资金流 (核心大表)
│   ├── index_daily      # 指数历史
│   ├── etf_daily        # ETF历史
│   ├── north_flow       # 北向资金
│   └── macro_data       # 宏观数据
│
└── realtime.db          # SQLite 实时数据库 (高频写入)
    ├── realtime_quotes  # 实时行情快照
    ├── tick_data        # 分时成交
    └── minute_quotes    # 分钟K线
```

## 为什么选择 DuckDB？

| 特性 | CSV | DuckDB |
|------|-----|--------|
| 查询速度 | 慢 (全量加载) | 快 (索引查询) |
| 4千万行查询 | 15-30秒 | 0.5-3秒 |
| 内存占用 | 高 (2-4GB) | 低 (100-500MB) |
| 聚合分析 | 慢 | 极快 (列式存储) |
| 并发读取 | 差 | 好 |

## 快速开始

### 交互式菜单（推荐）

```bash
python run_collector.py
```

菜单选项：
- **1-6**: 静态数据采集（股票列表、个股信息、板块等）
- **7-11**: 每日数据采集（日线、指数、ETF、北向资金、宏观）
- **12**: 实时行情快照
- **13-15**: 组合数据采集
- **16-17**: 数据查询
- **0**: 检查系统状态
- **q**: 退出

### 命令行方式

```bash
# 静态数据
python collector_static.py stock_list      # 股票列表
python collector_static.py stock_info 10   # 个股信息 (测试10只)
python collector_static.py board           # 板块
python collector_static.py index           # 指数成分股
python collector_static.py etf             # ETF列表

# 每日数据
python collector_daily.py stock auto       # 日线+资金流 (自动模式)
python collector_daily.py full             # 全量下载
python collector_daily.py increment        # 增量更新
python collector_daily.py index            # 指数历史
python collector_daily.py etf              # ETF历史
python collector_daily.py north            # 北向资金
python collector_daily.py macro            # 宏观数据

# 实时数据
python collector_realtime.py all           # 全市场快照
python collector_realtime.py monitor       # 实时监控
python collector_realtime.py stock 600519  # 单股行情
```

## 配置文件

配置文件: `collector_config.json`

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `request_interval` | API请求间隔(秒) | 1 |
| `max_retries` | 最大重试次数 | 3 |
| `retry_delay` | 重试延迟(秒) | 2 |
| `min_disk_space_gb` | 最小磁盘空间(GB) | 10 |

## 数据表说明

### daily_quotes (日线行情+资金流)

| 字段 | 类型 | 说明 |
|------|------|------|
| code | VARCHAR | 股票代码 |
| trade_date | DATE | 交易日期 |
| open | DOUBLE | 开盘价 |
| high | DOUBLE | 最高价 |
| low | DOUBLE | 最低价 |
| close | DOUBLE | 收盘价 |
| volume | DOUBLE | 成交量 |
| amount | DOUBLE | 成交额 |
| change_pct | DOUBLE | 涨跌幅(%) |
| turnover_rate | DOUBLE | 换手率(%) |
| main_net_inflow | DOUBLE | 主力净流入 |
| main_net_inflow_pct | DOUBLE | 主力净流入占比(%) |
| super_large_net_inflow | DOUBLE | 超大单净流入 |
| large_net_inflow | DOUBLE | 大单净流入 |
| medium_net_inflow | DOUBLE | 中单净流入 |
| small_net_inflow | DOUBLE | 小单净流入 |

## 量化分析示例

```python
from db_core import DuckDBManager

with DuckDBManager() as db:
    # 查询某股票最近30天数据
    df = db.fetch_df("""
        SELECT * FROM daily_quotes 
        WHERE code = '600519' 
        ORDER BY trade_date DESC 
        LIMIT 30
    """)
    
    # 计算月度涨跌幅统计
    df = db.fetch_df("""
        SELECT 
            strftime(trade_date, '%Y-%m') as month,
            AVG(change_pct) as avg_change,
            COUNT(*) as trading_days
        FROM daily_quotes
        WHERE trade_date >= '2024-01-01'
        GROUP BY month
        ORDER BY month
    """)
    
    # 计算主力资金流向
    df = db.fetch_df("""
        SELECT 
            trade_date,
            SUM(main_net_inflow) as total_main_flow
        FROM daily_quotes
        WHERE trade_date >= '2024-01-01'
        GROUP BY trade_date
        ORDER BY trade_date
    """)
```

## 依赖安装

```bash
pip install akshare duckdb pandas
```

## 文件说明

| 文件 | 说明 |
|------|------|
| `run_collector.py` | 主菜单入口 |
| `db_core.py` | 数据库核心模块 |
| `collector_static.py` | 静态数据采集 |
| `collector_daily.py` | 每日数据采集 |
| `collector_realtime.py` | 实时行情采集 |
| `collector_config.json` | 配置文件 |

## 注意事项

1. **首次使用**: 先运行"股票列表"采集，再运行其他功能
2. **数据量**: 4千万行数据约占用 2-3GB 磁盘空间
3. **更新频率**: 静态数据每周/月更新，每日数据交易日收盘后更新
4. **网络**: 采集过程中需要稳定网络连接

## 后续计划

- [ ] Kivy 安卓App
- [ ] 量化分析模块
- [ ] 数据可视化
