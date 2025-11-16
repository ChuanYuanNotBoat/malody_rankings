# Malody 数据工具集

> **项目状态更新**：本项目已重构到新仓库

## 项目架构

### 🚀 Malody API 服务 (主要维护版本)
**仓库地址**: [malody_api](https://github.com/ChuanYuanNotBoat/malody_api)
基于 FastAPI 构建的现代化 Malody 数据服务，提供：
- RESTful API 接口
- 实时数据爬取和缓存
- 结构化数据返回
- 完整的玩家数据分析和统计功能

**推荐使用此版本**，它包含了本项目的所有功能并进行了全面重构和增强。

### 📊 数据分析模块 (已整合到 API)
原近5000行的 CMD 数据分析工具已重构并整合到 Malody API 服务中，提供更强大的数据分析能力。

### 🗃️ 本项目 (参考实现)
当前仓库保留作为参考实现，包含原始的爬虫脚本和数据分析工具。未重构的原始源代码也在 API 仓库的根目录中保留，供参考使用。

## 原始功能说明

这是一个用于自动爬取 Malody 游戏各个模式前 50 名玩家排行榜数据的 Python 脚本。它会定期抓取数据并保存到 Excel 文件中，便于分析玩家排名变化趋势。

### 功能特点

- 爬取 Malody 所有 10 个游戏模式的前 50 名玩家数据
- 将数据保存到结构化的 Excel 文件中
- 自动定时运行（默认每 30 分钟一次）
- 支持自定义 Cookie 认证
- 智能重试机制，处理网络异常
- 详细的日志记录，便于调试

### 安装与使用

#### 前置要求

- Python 3.7+
- 有效的 Malody 账号 Cookie

#### 安装步骤

1. 克隆仓库：
   ```bash
   git clone https://github.com/yourusername/malody-rankings.git
   cd malody-rankings
   ```

2. 安装依赖：
   ```bash
   pip install -r requirements.txt
   ```

3. 配置 Cookie：
   打开 `malody_rankings.py` 文件，修改以下部分：
   ```python
   COOKIES = {
     "sessionid": "你的 sessionid",
     "csrftoken": "你的 csrftoken"
   }
   ```
   你可以在浏览器登录 Malody 后，通过开发者工具获取这些 Cookie 值。

#### 运行脚本

- 以守护进程模式运行（默认每 30 分钟爬取一次）：
  ```bash
  python malody_rankings.py
  ```

- 单次运行模式：
  ```bash
  python malody_rankings.py --once
  ```

## 快速开始 (推荐)

建议直接使用新的 API 服务：

```bash
# 克隆 API 仓库
git clone https://github.com/ChuanYuanNotBoat/malody_api.git
cd malody_api

# 安装依赖和运行
pip install -r requirements.txt
uvicorn main:app --reload
```

访问 `http://localhost:8000/docs` 查看完整的 API 文档。

## 数据字段

Malody 玩家数据包含以下字段：

| 字段名 | 类型   | 描述         |
|--------|--------|--------------|
| rank   | int    | 玩家排名     |
| name   | string | 玩家名称     |
| lv     | int    | 玩家等级     |
| exp    | int    | 玩家经验值   |
| acc    | float  | 平均准确率   |
| combo  | int    | 最大连击数   |
| pc     | int    | 游玩次数     |

## 注意事项

- **Cookie 有效期**：Malody 的 sessionid 会过期，需要定期更新
- **请求频率**：避免设置过短的请求间隔，以免被服务器限制
- **数据使用**：请遵守 Malody 服务条款，合理使用数据

## 贡献

欢迎提交 Pull Request 或 Issue 到新的 [malody_api](https://github.com/ChuanYuanNotBoat/malody_api) 仓库：
- 添加新功能
- 修复解析问题
- 优化性能
- 改进文档