# AKShare A股数据采集器 - Kivy安卓版

## 项目结构

```
├── main.py                 # Kivy主应用
├── db_core.py              # 数据库核心模块
├── collector_static.py     # 静态数据采集
├── collector_daily.py      # 每日数据采集
├── collector_realtime.py   # 实时数据采集
├── collector_config.json   # 配置文件
├── buildozer.spec          # 安卓打包配置
└── README.md               # 说明文档
```

## Windows开发测试

```bash
# 安装依赖
pip install kivy duckdb akshare pandas requests

# 运行应用
python main.py
```

## 安卓打包 (需要Linux环境)

### 方法1: 使用WSL2 + Buildozer

```bash
# 在WSL2 Ubuntu中
sudo apt update
sudo apt install -y git zip unzip openjdk-17-jdk autoconf libtool pkg-config zlib1g-dev libncurses5-dev libncursesw5-dev libtinfo5 cmake libffi-dev libssl-dev

pip install buildozer cython

# 克隆项目
cd /mnt/c/code/test

# 首次打包（会自动下载SDK/NDK）
buildozer android debug

# 后续打包
buildozer android debug deploy run
```

### 方法2: 使用GitHub Actions自动打包

创建 `.github/workflows/build.yml`:

```yaml
name: Build Android APK

on:
  push:
    tags:
      - 'v*'

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      
      - name: Install dependencies
        run: |
          pip install buildozer cython
          sudo apt-get update
          sudo apt-get install -y git zip unzip openjdk-17-jdk autoconf libtool pkg-config zlib1g-dev libncurses5-dev libncursesw5-dev libtinfo5 cmake libffi-dev libssl-dev
      
      - name: Build APK
        run: buildozer android debug
      
      - name: Upload APK
        uses: actions/upload-artifact@v3
        with:
          name: akshare-collector
          path: bin/*.apk
```

### 方法3: 使用Docker

```bash
docker pull kivy/buildozer

docker run --rm -v "$PWD":/home/user/hostcwd kivy/buildozer android debug
```

## vivo X300 适配说明

- 目标API: 33 (Android 13)
- 最低API: 24 (Android 7.0)
- 架构: arm64-v8a
- 权限: 网络访问、存储读写

## 功能列表

### 静态数据
1. 股票列表 - 全部A股代码和名称
2. 个股信息 - 行业、市值、股本等
3. 行业板块 - 行业分类和成分
4. 概念板块 - 概念主题分类
5. 指数成分 - 主要指数成分股
6. ETF列表 - ETF基金列表

### 每日数据
7. 股票日线 - 日线行情+资金流
8. 指数日线 - 主要指数历史
9. ETF日线 - ETF历史行情
10. 北向资金 - 外资流向数据
11. 宏观数据 - 经济指标数据

### 实时数据
12. 全市场快照 - 实时行情快照

### 批量操作
13. 静态数据全量更新
14. 每日数据全量更新
15. 一键更新全部

## 数据存储

- DuckDB: 主数据存储 (`stock_data.duckdb`)
- SQLite: 实时数据存储 (`realtime.db`)

## 注意事项

1. 首次运行需要先采集股票列表
2. 每日数据建议在收盘后采集
3. 实时数据仅在交易时间有效
4. 数据目录在应用启动时自动创建
