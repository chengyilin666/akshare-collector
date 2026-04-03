# APK打包指南

## 方法一：GitHub Actions自动打包（推荐，免费）

### 步骤：

1. **创建GitHub仓库**
   - 登录 https://github.com
   - 点击 "New repository"
   - 仓库名如：`akshare-collector`

2. **上传代码**
   ```bash
   cd C:\code\test
   git init
   git add .
   git commit -m "Initial commit"
   git branch -M main
   git remote add origin https://github.com/你的用户名/akshare-collector.git
   git push -u origin main
   ```

3. **自动打包**
   - 推送后，GitHub Actions自动开始打包
   - 点击仓库的 "Actions" 标签查看进度
   - 约20-30分钟完成

4. **下载APK**
   - Actions完成后，点击对应的工作流
   - 在 "Artifacts" 中下载 `akshare-collector-apk`
   - 解压得到APK文件

---

## 方法二：手动下载WSL2离线包安装

1. 下载WSL2离线包：
   https://aka.ms/wslubuntu2204

2. 双击安装

3. 然后执行打包命令

---

## 方法三：使用云服务器

如果你有阿里云/腾讯云等Linux服务器：

```bash
# 连接服务器后
git clone 你的仓库地址
cd akshare-collector
pip install buildozer cython
sudo apt-get update
sudo apt-get install -y git zip unzip openjdk-17-jdk autoconf libtool pkg-config zlib1g-dev libncurses5-dev libncursesw5-dev libtinfo5 cmake libffi-dev libssl-dev
buildozer android debug
```

---

## 当前项目文件

| 文件 | 说明 |
|------|------|
| `main.py` | Kivy安卓应用 |
| `run_collector.py` | PC端命令行工具 |
| `buildozer.spec` | 安卓打包配置 |
| `.github/workflows/build.yml` | GitHub Actions配置 |

---

## 注意

PC端功能完全可用：
```bash
python run_collector.py
```

安卓APK需要打包后才能使用。
