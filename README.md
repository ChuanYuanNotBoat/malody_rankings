Malody 排行榜爬虫
这是一个用于自动爬取 Malody 游戏各个模式前 50 名玩家排行榜数据的 Python 脚本。它会定期抓取数据并保存到 Excel 文件中，便于分析玩家排名变化趋势。

功能特点
爬取 Malody 所有 10 个游戏模式的前 50 名玩家数据

将数据保存到结构化的 Excel 文件中

自动定时运行（默认每 30 分钟一次）

支持自定义 Cookie 认证

重试机制，处理网络异常

详细的日志记录，便于调试

安装与使用
前置要求
Python 3.7+

有效的 Malody 账号 Cookie

安装步骤
克隆仓库：

bash
git clone https://github.com/yourusername/malody-rankings.git
cd malody-rankings
安装依赖：

bash
pip install -r requirements.txt
配置 Cookie：
打开 malody_rankings.py 文件，修改以下部分：

python
COOKIES = {
  "sessionid": "你的 sessionid",
  "csrftoken": "你的 csrftoken"
}
你可以在浏览器登录 Malody 后，通过开发者工具获取这些 Cookie 值。

运行脚本
以守护进程模式运行（默认每 30 分钟爬取一次）：

bash
python malody_rankings.py
单次运行模式：

bash
python malody_rankings.py --once
输出文件
脚本会根据游戏模式生成不同的 Excel 文件：

模式	文件名	描述
0	key.xlsx	按键模式
3	catch.xlsx	接水果模式
其他	modeX.xlsx	X 为模式编号
每个 Excel 文件包含多个工作表，每个工作表对应一次爬取的时间戳，格式为：mode_X_YYYY-MM-DD_HH-MM

数据字段
每个玩家记录包含以下字段：

字段名	类型	描述
rank	int	玩家排名
name	string	玩家名称
lv	int	玩家等级
exp	int	玩家经验值
acc	float	平均准确率
combo	int	最大连击数
pc	int	游玩次数
配置选项
你可以在 malody_rankings.py 中修改以下配置：

python
# 爬取模式列表 (0-9)
MODES = list(range(10))

# 请求头设置
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)...",
    "Referer": "https://m.mugzone.net/",
    # 可添加其他头信息
}

# 请求间隔 (秒)
REQUEST_DELAY = 5  # 模式间延迟
CYCLE_DELAY = 1800  # 爬取周期延迟 (30分钟)
日志文件
脚本运行时会生成 crawler.log 日志文件，包含以下信息：

爬取开始/结束时间

每个模式的处理状态

数据保存位置

错误和异常信息

注意事项
Cookie 有效期：Malody 的 sessionid 会过期，需要定期更新

请求频率：避免设置过短的爬取间隔，以免被服务器封禁

数据存储：长时间运行会产生多个 Excel 工作表，定期备份或清理旧数据

网络环境：确保运行脚本的设备可以稳定访问 Malody 服务器

贡献
欢迎提交 Pull Request 或 Issue 来改进项目：

添加新功能

修复解析问题

优化性能

改进文档

许可
本项目采用 MIT 许可证。
