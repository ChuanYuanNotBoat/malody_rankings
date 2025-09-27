import sqlite3
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import cmd
import os
import sys
import textwrap
from typing import Dict, List, Tuple, Optional
import matplotlib.dates as mdates
from matplotlib.ticker import MaxNLocator, LogLocator, FuncFormatter
import subprocess
import atexit
import signal
from functools import wraps
import shutil
import re
import math

# 修复Python 3.12中SQLite datetime适配器的弃用警告
def adapt_datetime(dt):
    return dt.isoformat()

def convert_datetime(s):
    return datetime.fromisoformat(s.decode())

sqlite3.register_adapter(datetime, adapt_datetime)
sqlite3.register_converter("timestamp", convert_datetime)

# 设置matplotlib使用Agg后端（无GUI）
plt.switch_backend('Agg')

# 设置matplotlib支持中文显示
try:
    plt.rcParams['font.sans-serif'] = ['SimHei', 'DejaVu Sans', 'Arial']
    plt.rcParams['axes.unicode_minus'] = False
except:
    print("警告: 无法设置中文字体，图表中的中文可能显示为方块")

# 设置图表字体颜色为深色，避免与背景冲突
plt.rcParams['text.color'] = 'black'
plt.rcParams['axes.labelcolor'] = 'black'
plt.rcParams['xtick.color'] = 'black'
plt.rcParams['ytick.color'] = 'black'
plt.rcParams['axes.titlecolor'] = 'black'

# 颜色定义 (ANSI转义码)
class Colors:
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'

def color_enabled():
    """检查当前环境是否支持颜色输出"""
    # 检查是否在终端中运行（不是重定向到文件）
    if not sys.stdout.isatty():
        return False
    
    # 检查环境变量
    if os.environ.get('NO_COLOR'):
        return False
    
    # 检查平台和终端类型
    platform = sys.platform
    
    # Windows系统检测
    if platform == "win32":
        # 检查是否在支持ANSI的终端中（如Windows Terminal、PowerShell 5.1+、Git Bash等）
        term = os.environ.get('TERM', '')
        # 检查是否是Windows Terminal
        if 'WT_SESSION' in os.environ:
            return True
        # 检查是否是PowerShell 5.1或更高版本
        try:
            # 通过检查PSVersionTable来判断PowerShell版本
            if 'PSVersionTable' in os.environ:
                return True
        except:
            pass
        # 检查是否在Git Bash、Cygwin等兼容环境中
        if 'TERM' in os.environ and ('xterm' in term or 'cygwin' in term):
            return True
        # 对于传统CMD，只有Windows 10周年更新(1607)及以上版本支持ANSI
        try:
            import ctypes
            kernel32 = ctypes.windll.kernel32
            # 检查是否支持虚拟终端序列
            ENABLE_VIRTUAL_TERMINAL_PROCESSING = 0x0004
            stdout_handle = kernel32.GetStdHandle(-11)  # STD_OUTPUT_HANDLE
            mode = ctypes.c_uint32()
            if kernel32.GetConsoleMode(stdout_handle, ctypes.byref(mode)):
                if mode.value & ENABLE_VIRTUAL_TERMINAL_PROCESSING:
                    return True
                # 尝试启用虚拟终端支持
                kernel32.SetConsoleMode(stdout_handle, mode.value | ENABLE_VIRTUAL_TERMINAL_PROCESSING)
                if kernel32.GetConsoleMode(stdout_handle, ctypes.byref(mode)):
                    if mode.value & ENABLE_VIRTUAL_TERMINAL_PROCESSING:
                        return True
        except:
            pass
        return False
    else:
        # Linux、macOS、Termux等Unix-like系统通常支持颜色
        term = os.environ.get('TERM', '')
        # 检查是否是dumb终端或不支持颜色的终端
        if term == 'dumb':
            return False
        return True

def colorize(text, color):
    """有条件地添加颜色到文本"""
    if color_enabled():
        return f"{color}{text}{Colors.END}"
    return text

def db_safe_operation(func):
    """装饰器用于确保数据库操作安全"""
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except sqlite3.Error as e:
            print(colorize(f"数据库错误: {e}", Colors.RED))
            return False
        except Exception as e:
            print(colorize(f"操作错误: {e}", Colors.RED))
            return False
    return wrapper

class MalodyViz(cmd.Cmd):
    """Malody排行榜数据可视化工具"""
    
    intro = colorize("\n欢迎使用Malody排行榜数据可视化工具!\n\n", Colors.CYAN) + \
            "输入 " + colorize("help", Colors.GREEN) + " 或 " + colorize("?", Colors.GREEN) + " 查看命令列表。\n" + \
            "输入 " + colorize("help <命令名>", Colors.GREEN) + " 查看具体命令的详细说明。\n\n" + \
            "所有生成的图表将保存在 " + colorize("viz_output", Colors.YELLOW) + " 目录中。\n" + \
            "提示: 可以使用 " + colorize("ls", Colors.GREEN) + " 命令查看当前目录文件。\n"
    prompt = colorize("(malody-viz) ", Colors.BLUE)
    
    def __init__(self):
        super().__init__()
        self.db_path = "malody_rankings.db"
        self.conn = None
        self.current_mode = 0
        self.output_dir = "viz_output"
        
        atexit.register(self.cleanup)
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        
        self.connect_db()
        
        self.mode_names = {
            0: "Key",
            1: "Step",
            2: "DJ",
            3: "Catch",
            4: "Pad",
            5: "Taiko",
            6: "Ring",
            7: "Slide",
            8: "Live",
            9: "Cube"
        }
    
    def connect_db(self):
        """连接到SQLite数据库"""
        try:
            self.conn = sqlite3.connect(
                self.db_path, 
                detect_types=sqlite3.PARSE_DECLTYPES,
                check_same_thread=False
            )
            self.conn.execute("PRAGMA foreign_keys = ON")
            self.conn.execute("PRAGMA busy_timeout = 3000")
            print(colorize(f"成功连接到数据库: {self.db_path}", Colors.GREEN))
        except sqlite3.Error as e:
            print(colorize(f"数据库连接错误: {e}", Colors.RED))
            sys.exit(1)
    
    def cleanup(self):
        """清理资源"""
        if self.conn:
            try:
                self.conn.close()
                print(colorize("\n数据库连接已安全关闭", Colors.GREEN))
            except:
                pass
    
    def signal_handler(self, signum, frame):
        """处理中断信号"""
        print(colorize("\n正在安全退出...", Colors.YELLOW))
        self.cleanup()
        sys.exit(0)
    
    def emptyline(self):
        """空行时不执行任何操作"""
        pass
    
    def get_unique_filename(self, base_name, extension):
        """生成不重复的文件名，添加时间戳"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        name_only = os.path.splitext(base_name)[0]
        return f"{name_only}_{timestamp}.{extension}"
    
    def do_help(self, arg):
        """显示帮助信息"""
        if arg:
            cmd_name = arg.strip().lower()
            if hasattr(self, 'do_' + cmd_name):
                func = getattr(self, 'do_' + cmd_name)
                if func.__doc__:
                    print(colorize(f"\n{cmd_name} 命令帮助:", Colors.CYAN))
                    print(colorize("=" * 50, Colors.CYAN))
                    print(textwrap.dedent(func.__doc__))
                else:
                    print(colorize(f"没有找到命令 '{cmd_name}' 的帮助文档", Colors.YELLOW))
            else:
                print(colorize(f"未知命令: {cmd_name}", Colors.RED))
        else:
            print(colorize("\nMalody排行榜数据可视化工具 - 命令列表", Colors.CYAN))
            print(colorize("=" * 60, Colors.CYAN))
            
            commands = [
                ("ls [路径]", "列出目录内容"),
                ("mode [模式]", "设置或查看当前模式"),
                ("top [数量]", "显示顶级玩家排名"),
                ("player <玩家名> [模式]", "查看玩家信息"),
                ("history <玩家名> [模式] [天数]", "查看玩家历史排名并生成图表"),
                ("compare <玩家1> <玩家2> [...] [模式] [天数]", "比较多个玩家的排名变化"),
                ("top_chart [数量] [模式]", "生成顶级玩家分布图表"),
                ("alias <原名> <新名>", "设置玩家别名"),
                ("export <类型> [模式] [天数]", "导出数据为CSV文件"),
                ("update", "更新数据（调用爬虫脚本）"),
                ("help [命令]", "显示帮助信息"),
                ("exit/quit", "退出程序")
            ]
            
            for cmd, desc in commands:
                print(f"  {colorize(cmd, Colors.GREEN):<30} {desc}")
            
            print(colorize("\n模式编号对应表:", Colors.CYAN))
            print(colorize("-" * 30, Colors.CYAN))
            for mode_id, mode_name in self.mode_names.items():
                print(f"  {mode_id}: {mode_name}")
            
            print(colorize("\n输入 'help <命令名>' 查看具体命令的详细说明", Colors.YELLOW))
    
    @db_safe_operation
    def do_ls(self, arg):
        """
        列出当前目录的文件和文件夹
        
        用法: ls [路径]
        参数:
          路径 - 可选，要列出的目录路径，默认为当前目录
        
        示例:
          ls        # 列出当前目录
          ls viz_output  # 列出viz_output目录
        """
        path = arg if arg else "."
        try:
            if os.path.exists(path):
                items = os.listdir(path)
                print(colorize(f"\n{path} 目录内容:", Colors.CYAN))
                print(colorize("-" * 40, Colors.CYAN))
                for item in items:
                    full_path = os.path.join(path, item)
                    if os.path.isdir(full_path):
                        print(colorize(f"[目录] {item}/", Colors.BLUE))
                    else:
                        size = os.path.getsize(full_path)
                        print(f"[文件] {item} ({size} 字节)")
            else:
                print(colorize(f"路径不存在: {path}", Colors.RED))
        except Exception as e:
            print(colorize(f"列出目录时出错: {e}", Colors.RED))
    
    def do_mode(self, arg):
        """
        设置或查看当前模式
        
        用法: mode [模式编号]
        参数:
          模式编号 - 0到9之间的数字，表示不同的游戏模式
        
        示例:
          mode     # 查看当前模式
          mode 0   # 切换到Key模式
          mode 3   # 切换到Catch模式
        """
        if not arg:
            mode_name = self.mode_names.get(self.current_mode, "未知")
            print(colorize(f"\n当前模式: {self.current_mode} ({mode_name})", Colors.CYAN))
            return
        
        try:
            mode = int(arg)
            if mode not in self.mode_names:
                print(colorize("错误: 模式必须在0-9之间", Colors.RED))
                return
            self.current_mode = mode
            mode_name = self.mode_names.get(mode, "未知")
            print(colorize(f"\n已切换到模式: {mode} ({mode_name})", Colors.GREEN))
        except ValueError:
            print(colorize("错误: 请输入有效的模式数字(0-9)", Colors.RED))
    
    @db_safe_operation
    def do_top(self, arg):
        """
        查看当前模式的顶级玩家
        
        用法: top [数量]
        参数:
          数量 - 可选，要显示的玩家数量，默认为10
        
        示例:
          top      # 显示前10名玩家
          top 20   # 显示前20名玩家
        """
        try:
            limit = int(arg) if arg else 10
            if limit <= 0:
                print(colorize("错误: 数量必须大于0", Colors.RED))
                return
        except ValueError:
            print(colorize("错误: 请输入有效的数字", Colors.RED))
            return
        
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT MAX(crawl_time) FROM player_rankings WHERE mode = ?",
            (self.current_mode,)
        )
        latest_time = cursor.fetchone()[0]
        
        if not latest_time:
            print(colorize(f"\n模式 {self.current_mode} 没有数据", Colors.YELLOW))
            return
        
        cursor.execute(
            """
            SELECT pr.rank, pr.name, pr.lv, pr.exp, pr.acc, pr.combo, pr.pc
            FROM player_rankings pr
            WHERE pr.mode = ? AND pr.crawl_time = ?
            ORDER BY pr.rank
            LIMIT ?
            """,
            (self.current_mode, latest_time, limit)
        )
        
        players = cursor.fetchall()
        
        if not players:
            print(colorize(f"\n模式 {self.current_mode} 没有找到玩家数据", Colors.YELLOW))
            return
        
        try:
            terminal_width = shutil.get_terminal_size().columns
        except:
            terminal_width = 80
        
        if terminal_width < 100:
            col_widths = [6, 15, 6, 8, 8, 6, 8]
            header_format = "{:<6} {:<15} {:<6} {:<8} {:<8} {:<6} {:<8}"
            row_format = "{:<6} {:<15} {:<6} {:<8} {:<8.2f} {:<6} {:<8}"
        else:
            col_widths = [6, 20, 6, 10, 8, 6, 8]
            header_format = "{:<6} {:<20} {:<6} {:<10} {:<8} {:<6} {:<8}"
            row_format = "{:<6} {:<20} {:<6} {:<10} {:<8.2f} {:<6} {:<8}"
        
        mode_name = self.mode_names.get(self.current_mode, "未知")
        print(colorize(f"\n模式 {self.current_mode} ({mode_name}) 顶级玩家排名", Colors.CYAN))
        print(colorize(f"数据时间: {latest_time}", Colors.YELLOW))
        print(colorize("-" * sum(col_widths), Colors.CYAN))
        print(colorize(header_format.format("排名", "玩家名", "等级", "经验", "准确率", "连击", "游玩次数"), Colors.BOLD))
        print(colorize("-" * sum(col_widths), Colors.CYAN))
        
        for rank, name, lv, exp, acc, combo, pc in players:
            if rank == 1:
                rank_str = colorize(f"{rank}", Colors.YELLOW)
            elif rank == 2:
                rank_str = colorize(f"{rank}", Colors.WHITE)
            elif rank == 3:
                rank_str = colorize(f"{rank}", Colors.MAGENTA)
            else:
                rank_str = str(rank)
            
            if len(name) > col_widths[1]:
                name = name[:col_widths[1]-3] + "..."
                
            print(row_format.format(
                rank_str, name, lv, exp, acc, combo, pc
            ))
    
    @db_safe_operation
    def do_player(self, arg):
        """
        查看玩家信息
        
        用法: player <玩家名> [模式]
        参数:
          玩家名 - 要查询的玩家名称
          模式   - 可选，模式编号，默认为当前模式
        
        示例:
          player Zani      # 查看Zani在当前模式的信息
          player Zani 0    # 查看Zani在模式0的信息
        """
        args = arg.split()
        if not args:
            print(colorize("错误: 请输入玩家名", Colors.RED))
            return
        
        player_name = args[0]
        mode = self.current_mode
        if len(args) > 1:
            try:
                mode = int(args[1])
                if mode not in self.mode_names:
                    print(colorize("错误: 模式必须在0-9之间", Colors.RED))
                    return
            except ValueError:
                print(colorize("错误: 请输入有效的模式数字(0-9)", Colors.RED))
                return
        
        cursor = self.conn.cursor()
        
        cursor.execute(
            "SELECT player_id FROM player_aliases WHERE alias = ?",
            (player_name,)
        )
        result = cursor.fetchone()
        
        if not result:
            print(colorize(f"\n未找到玩家: {player_name}", Colors.YELLOW))
            return
        
        player_id = result[0]
        
        cursor.execute(
            """
            SELECT pr.rank, pr.lv, pr.exp, pr.acc, pr.combo, pr.pc, pr.crawl_time
            FROM player_rankings pr
            WHERE pr.player_id = ? AND pr.mode = ?
            ORDER BY pr.crawl_time DESC
            LIMIT 1
            """,
            (player_id, mode)
        )
        
        player_data = cursor.fetchone()
        
        if not player_data:
            mode_name = self.mode_names.get(mode, "未知")
            print(colorize(f"\n玩家 {player_name} 在模式 {mode} ({mode_name}) 中没有数据", Colors.YELLOW))
            return
            
        if len(player_data) < 7:
            print(colorize(f"\n玩家 {player_name} 的数据不完整", Colors.YELLOW))
            return
            
        rank, lv, exp, acc, combo, pc, crawl_time = player_data
        
        mode_name = self.mode_names.get(mode, "未知")
        print(colorize(f"\n玩家: {player_name} (模式 {mode} - {mode_name})", Colors.CYAN))
        print(colorize(f"数据时间: {crawl_time}", Colors.YELLOW))
        print(colorize("-" * 50, Colors.CYAN))
        print(f"排名: {colorize(rank, Colors.GREEN)}")
        print(f"等级: {lv}")
        print(f"经验: {exp}")
        print(f"准确率: {colorize(f'{acc:.2f}%', Colors.GREEN)}")
        print(f"最大连击: {combo}")
        print(f"游玩次数: {pc}")
        
        cursor.execute(
            "SELECT alias FROM player_aliases WHERE player_id = ?",
            (player_id,)
        )
        aliases = [row[0] for row in cursor.fetchall()]
        
        if len(aliases) > 1:
            print(f"曾用名: {', '.join(aliases)}")
    
    @db_safe_operation
    def do_history(self, arg):
        """
        查看玩家历史排名并生成图表
        
        用法: history <玩家名> [模式] [天数]
        参数:
          玩家名 - 要查询的玩家名称
          模式   - 可选，模式编号，默认为当前模式
          天数   - 可选，要查询的历史天数，默认为30天
        
        示例:
          history Zani        # 查看Zani在当前模式最近30天的历史
          history Zani 0 60   # 查看Zani在模式0最近60天的历史
        """
        args = arg.split()
        if not args:
            print(colorize("错误: 请输入玩家名", Colors.RED))
            return
        
        player_name = args[0]
        mode = self.current_mode
        days = 30
        
        if len(args) > 1:
            try:
                mode = int(args[1])
                if mode not in self.mode_names:
                    print(colorize("错误: 模式必须在0-9之间", Colors.RED))
                    return
            except ValueError:
                print(colorize("错误: 请输入有效的模式数字(0-9)", Colors.RED))
                return
        
        if len(args) > 2:
            try:
                days = int(args[2])
                if days <= 0:
                    print(colorize("错误: 天数必须大于0", Colors.RED))
                    return
            except ValueError:
                print(colorize("错误: 请输入有效的天数", Colors.RED))
                return
        
        cursor = self.conn.cursor()
        
        cursor.execute(
            "SELECT player_id FROM player_aliases WHERE alias = ?",
            (player_name,)
        )
        result = cursor.fetchone()
        
        if not result:
            print(colorize(f"\n未找到玩家: {player_name}", Colors.YELLOW))
            return
        
        player_id = result[0]
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        cursor.execute(
            """
            SELECT pr.rank, pr.crawl_time
            FROM player_rankings pr
            WHERE pr.player_id = ? AND pr.mode = ? AND pr.crawl_time >= ?
            ORDER BY pr.crawl_time
            """,
            (player_id, mode, start_date)
        )
        
        history_data = cursor.fetchall()
        
        if not history_data:
            mode_name = self.mode_names.get(mode, "未知")
            print(colorize(f"\n玩家 {player_name} 在模式 {mode} ({mode_name}) 中最近 {days} 天没有数据", Colors.YELLOW))
            return
        
        dates = [row[1] for row in history_data]
        ranks = [row[0] for row in history_data]
        
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(dates, ranks, 'o-', linewidth=2, markersize=4)
        ax.invert_yaxis()
        mode_name = self.mode_names.get(mode, "未知")
        
        # 修复字体颜色为黑色
        ax.set_title(f"Player {player_name} Ranking History (Mode {mode} - {mode_name})", color='black')
        ax.set_xlabel("Date", color='black')
        ax.set_ylabel("Rank", color='black')
        ax.grid(True, alpha=0.3)
        
        # 设置刻度颜色
        ax.tick_params(colors='black')
        
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
        fig.autofmt_xdate()
        
        # 使用唯一文件名避免覆盖
        base_filename = f"player_history_{player_name}_mode{mode}.png"
        filename = self.get_unique_filename(base_filename, "png")
        filepath = os.path.join(self.output_dir, filename)
        plt.tight_layout()
        plt.savefig(filepath, dpi=150, facecolor='white')  # 设置背景为白色
        plt.close()
        
        print(colorize(f"\n已生成历史排名图表: {filepath}", Colors.GREEN))
        
        print(colorize(f"\n{player_name} 最近排名变化:", Colors.CYAN))
        print(colorize("-" * 40, Colors.CYAN))
        for i, (rank, date) in enumerate(history_data[-10:] if len(history_data) > 10 else history_data):
            print(f"{date.strftime('%Y-%m-%d')}: 第{rank}名")
    
    @db_safe_operation
    def do_compare(self, arg):
        """
        比较多个玩家的排名变化
        
        用法: compare <玩家1> <玩家2> [更多玩家...] [模式] [天数]
        参数:
          玩家1, 玩家2... - 要比较的玩家名称
          模式            - 可选，模式编号，默认为当前模式
          天数            - 可选，要查询的历史天数，默认为30天
        
        示例:
          compare Zani N0tYour1dol           # 比较两名玩家在当前模式最近30天的排名
          compare Zani N0tYour1dol -KIRITAN- 0 60  # 比较三名玩家在模式0最近60天的排名
        """
        args = arg.split()
        if len(args) < 2:
            print(colorize("错误: 请至少输入两个玩家名", Colors.RED))
            return
        
        players = []
        mode = self.current_mode
        days = 30
        i = 0
        
        while i < len(args):
            if args[i].isdigit() and int(args[i]) in self.mode_names:
                mode = int(args[i])
                i += 1
                break
            elif i > 0 and args[i].isdigit():
                days = int(args[i])
                i += 1
                break
            else:
                players.append(args[i])
                i += 1
        
        if i < len(args) and args[i].isdigit():
            days = int(args[i])
        
        if len(players) < 2:
            print(colorize("错误: 请至少输入两个玩家名", Colors.RED))
            return
        
        cursor = self.conn.cursor()
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        fig, ax = plt.subplots(figsize=(12, 8))
        colors = plt.cm.Set3(np.linspace(0, 1, len(players)))
        
        for idx, player_name in enumerate(players):
            cursor.execute(
                "SELECT player_id FROM player_aliases WHERE alias = ?",
                (player_name,)
            )
            result = cursor.fetchone()
            
            if not result:
                print(colorize(f"\n未找到玩家: {player_name}", Colors.YELLOW))
                continue
            
            player_id = result[0]
            
            cursor.execute(
                """
                SELECT pr.rank, pr.crawl_time
                FROM player_rankings pr
                WHERE pr.player_id = ? AND pr.mode = ? AND pr.crawl_time >= ?
                ORDER BY pr.crawl_time
                """,
                (player_id, mode, start_date)
            )
            
            history_data = cursor.fetchall()
            
            if not history_data:
                mode_name = self.mode_names.get(mode, "未知")
                print(colorize(f"\n玩家 {player_name} 在模式 {mode} ({mode_name}) 中最近 {days} 天没有数据", Colors.YELLOW))
                continue
            
            dates = [row[1] for row in history_data]
            ranks = [row[0] for row in history_data]
            
            ax.plot(dates, ranks, 'o-', linewidth=2, markersize=4, 
                   color=colors[idx], label=player_name)
        
        ax.invert_yaxis()
        mode_name = self.mode_names.get(mode, "未知")
        
        # 修复字体颜色为黑色
        ax.set_title(f"Player Ranking Comparison (Mode {mode} - {mode_name})", color='black')
        ax.set_xlabel("Date", color='black')
        ax.set_ylabel("Rank", color='black')
        ax.grid(True, alpha=0.3)
        ax.legend()
        ax.tick_params(colors='black')
        
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
        fig.autofmt_xdate()
        
        # 使用唯一文件名避免覆盖
        base_filename = f"player_comparison_mode{mode}.png"
        filename = self.get_unique_filename(base_filename, "png")
        filepath = os.path.join(self.output_dir, filename)
        plt.tight_layout()
        plt.savefig(filepath, dpi=150, facecolor='white')
        plt.close()
        
        print(colorize(f"\n已生成玩家比较图表: {filepath}", Colors.GREEN))
    
    @db_safe_operation
    def do_top_chart(self, arg):
        """
        生成顶级玩家分布图表
        
        用法: top_chart [数量] [模式]
        参数:
          数量 - 可选，要显示的玩家数量，默认为20
          模式 - 可选，模式编号，默认为当前模式
        
        示例:
          top_chart        # 生成当前模式前20名玩家的图表
          top_chart 10 0   # 生成模式0前10名玩家的图表
        """
        args = arg.split()
        limit = 20
        mode = self.current_mode
        
        if args:
            try:
                if int(args[0]) in self.mode_names:
                    mode = int(args[0])
                    if len(args) > 1:
                        limit = int(args[1])
                else:
                    limit = int(args[0])
                    if len(args) > 1 and int(args[1]) in self.mode_names:
                        mode = int(args[1])
            except ValueError:
                print(colorize("错误: 请输入有效的数字", Colors.RED))
                return
        
        if limit <= 0:
            print(colorize("错误: 数量必须大于0", Colors.RED))
            return
        
        cursor = self.conn.cursor()
        
        cursor.execute(
            "SELECT MAX(crawl_time) FROM player_rankings WHERE mode = ?",
            (mode,)
        )
        latest_time = cursor.fetchone()[0]
        
        if not latest_time:
            mode_name = self.mode_names.get(mode, "未知")
            print(colorize(f"\n模式 {mode} ({mode_name}) 没有数据", Colors.YELLOW))
            return
        
        cursor.execute(
            """
            SELECT pr.rank, pr.name, pr.acc, pr.exp
            FROM player_rankings pr
            WHERE pr.mode = ? AND pr.crawl_time = ?
            ORDER BY pr.rank
            LIMIT ?
            """,
            (mode, latest_time, limit)
        )
        
        players = cursor.fetchall()
        
        if not players:
            mode_name = self.mode_names.get(mode, "未知")
            print(colorize(f"\n模式 {mode} ({mode_name}) 没有找到玩家数据", Colors.YELLOW))
            return
        
        ranks = [p[0] for p in players]
        names = [p[1] for p in players]
        accuracies = [p[2] for p in players]
        exps = [p[3] for p in players]
        
        # 创建更大的图表以适应更多玩家名
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 12))
        
        # 设置图表背景和字体颜色
        fig.patch.set_facecolor('white')
        
        # 准确率图表
        max_acc = max(accuracies)
        acc_diffs = [max_acc - acc for acc in accuracies]
        
        bars = ax1.bar(range(len(players)), acc_diffs, color=plt.cm.viridis(np.linspace(0, 1, len(players))))
        mode_name = self.mode_names.get(mode, "未知")
        
        # 修复字体颜色为黑色
        ax1.set_title(f"Mode {mode} ({mode_name}) Top {limit} Players Accuracy Difference", color='black')
        ax1.set_xlabel("Rank", color='black')
        ax1.set_ylabel("Accuracy Difference from Max (%)", color='black')
        ax1.set_xticks(range(len(players)))
        ax1.set_xticklabels(ranks, rotation=45)
        ax1.tick_params(colors='black')
        ax1.invert_yaxis()
        
        # 智能数据标签布局 - 避免重叠
        def smart_label_placement(ax, bars, values, y_offset_factor=0.01, rotation=45):
            """智能放置数据标签以避免重叠"""
            max_val = max(values) if values else 1
            y_offset = max_val * y_offset_factor
            
            # 收集所有标签位置
            label_positions = []
            for i, (bar, value) in enumerate(zip(bars, values)):
                x = bar.get_x() + bar.get_width() / 2
                y = bar.get_height() + y_offset
                label_positions.append((x, y, value, i))
            
            # 按y值排序，从高到低处理
            label_positions.sort(key=lambda pos: pos[1], reverse=True)
            
            # 调整重叠的标签
            adjusted_positions = []
            min_vertical_spacing = max_val * 0.05  # 最小垂直间距
            
            for x, y, value, idx in label_positions:
                # 检查是否与已放置的标签重叠
                overlap = False
                for adj_x, adj_y, _, _ in adjusted_positions:
                    if abs(x - adj_x) < 0.3 and abs(y - adj_y) < min_vertical_spacing:
                        overlap = True
                        break
                
                if overlap:
                    # 如果有重叠，稍微调整y位置
                    y_adjust = min_vertical_spacing
                    while any(abs(x - adj_x) < 0.3 and abs(y + y_adjust - adj_y) < min_vertical_spacing 
                             for adj_x, adj_y, _, _ in adjusted_positions):
                        y_adjust += min_vertical_spacing
                    y += y_adjust
                
                adjusted_positions.append((x, y, value, idx))
            
            # 按原始索引排序并添加标签
            adjusted_positions.sort(key=lambda pos: pos[3])
            
            for x, y, value, idx in adjusted_positions:
                ax.text(x, y, f'{value:.2f}%', 
                       ha='center', va='bottom', fontsize=8,
                       color='black', rotation=rotation,
                       bbox=dict(boxstyle='round,pad=0.2', facecolor='white', alpha=0.7, edgecolor='none'))
        
        # 使用智能标签布局
        smart_label_placement(ax1, bars, accuracies, y_offset_factor=0.02, rotation=45)
        
        # 在x轴下方添加玩家名字，使用更智能的布局
        name_y_pos = -0.08 * max(acc_diffs) if max(acc_diffs) > 0 else -0.1
        
        for i, (bar, name) in enumerate(zip(bars, names)):
            # 截断过长的名字
            display_name = name if len(name) <= 12 else name[:10] + '...'
            ax1.text(bar.get_x() + bar.get_width()/2., name_y_pos,
                    display_name, ha='right', va='top', fontsize=7, 
                    rotation=60, color='black')
        
        # 经验值图表
        exp_bars = ax2.bar(range(len(players)), exps, color=plt.cm.plasma(np.linspace(0, 1, len(players))))
        ax2.set_title(f"Mode {mode} ({mode_name}) Top {limit} Players Experience", color='black')
        ax2.set_xlabel("Rank", color='black')
        ax2.set_ylabel("Experience", color='black')
        ax2.set_xticks(range(len(players)))
        ax2.set_xticklabels(ranks, rotation=45)
        ax2.set_yscale('log')
        ax2.tick_params(colors='black')
        
        # 改进对数坐标轴的刻度标注
        def log_format_func(value, pos=None):
            if value >= 1000000:
                return f'{value/1000000:.1f}M'
            elif value >= 1000:
                return f'{value/1000:.0f}k'
            elif value >= 100:
                return f'{value:.0f}'
            else:
                return f'{value:.0f}'
        
        # 设置y轴格式
        ax2.yaxis.set_major_formatter(FuncFormatter(log_format_func))
        
        # 计算合适的刻度范围
        min_exp = min(exps) if exps else 1
        max_exp = max(exps) if exps else 1000
        
        # 生成更细分的刻度值
        min_power = math.floor(math.log10(min_exp))
        max_power = math.ceil(math.log10(max_exp))
        
        tick_values = []
        for power in range(int(min_power), int(max_power) + 1):
            base = 10 ** power
            tick_values.extend([base * mult for mult in [1, 2, 5] 
                              if min_exp <= base * mult <= max_exp * 1.1])
        
        # 设置刻度
        ax2.set_yticks(tick_values)
        
        # 添加网格线
        ax2.grid(True, which='both', alpha=0.3)
        
        # 智能放置经验值标签
        def smart_exp_label_placement(ax, bars, values, rotation=45):
            """智能放置经验值标签"""
            if not values:
                return
                
            # 使用对数空间计算偏移
            log_values = [math.log10(v) for v in values]
            max_log = max(log_values)
            log_offset = 0.05 * (max_log - min(log_values)) if len(log_values) > 1 else 0.1
            
            label_positions = []
            for i, (bar, value, log_val) in enumerate(zip(bars, values, log_values)):
                x = bar.get_x() + bar.get_width() / 2
                y_log = log_val + log_offset
                y = 10 ** y_log
                label_positions.append((x, y, value, i))
            
            # 按y值排序，从高到低处理
            label_positions.sort(key=lambda pos: pos[1], reverse=True)
            
            # 调整重叠的标签（在对数空间中）
            adjusted_positions = []
            min_log_spacing = 0.08  # 对数空间中的最小间距
            
            for x, y, value, idx in label_positions:
                log_y = math.log10(y)
                overlap = False
                for adj_x, adj_y, _, _ in adjusted_positions:
                    adj_log_y = math.log10(adj_y)
                    if abs(x - adj_x) < 0.3 and abs(log_y - adj_log_y) < min_log_spacing:
                        overlap = True
                        break
                
                if overlap:
                    # 在对数空间中调整
                    log_adjust = min_log_spacing
                    while any(abs(x - adj_x) < 0.3 and 
                             abs(log_y + log_adjust - math.log10(adj_y)) < min_log_spacing
                             for adj_x, adj_y, _, _ in adjusted_positions):
                        log_adjust += min_log_spacing
                    y = 10 ** (log_y + log_adjust)
                
                adjusted_positions.append((x, y, value, idx))
            
            # 按原始索引排序并添加标签
            adjusted_positions.sort(key=lambda pos: pos[3])
            
            for x, y, value, idx in adjusted_positions:
                ax.text(x, y, f'{value:.0f}', 
                       ha='center', va='bottom', fontsize=8,
                       color='black', rotation=rotation,
                       bbox=dict(boxstyle='round,pad=0.2', facecolor='white', alpha=0.7, edgecolor='none'))
        
        # 使用智能经验值标签布局
        smart_exp_label_placement(ax2, exp_bars, exps, rotation=45)
        
        # 在x轴下方添加玩家名字
        name_log_pos = math.log10(min_exp) - 0.3 if min_exp > 0 else 0
        name_y_pos = 10 ** name_log_pos
        
        for i, (bar, name) in enumerate(zip(exp_bars, names)):
            # 截断过长的名字
            display_name = name if len(name) <= 12 else name[:10] + '...'
            ax2.text(bar.get_x() + bar.get_width()/2., name_y_pos,
                    display_name, ha='right', va='top', fontsize=7, 
                    rotation=60, color='black')
        
        # 根据玩家数量调整布局
        bottom_margin = 0.25 + 0.01 * limit  # 动态调整底部边距
        plt.subplots_adjust(bottom=min(bottom_margin, 0.4), top=0.9, wspace=0.3)
        
        plt.tight_layout()
        
        # 使用唯一文件名避免覆盖
        base_filename = f"top_players_mode{mode}.png"
        filename = self.get_unique_filename(base_filename, "png")
        filepath = os.path.join(self.output_dir, filename)
        plt.savefig(filepath, dpi=150, facecolor='white', bbox_inches='tight')
        plt.close()
        
        print(colorize(f"\n已生成顶级玩家分布图表: {filepath}", Colors.GREEN))
        print(colorize(f"图表尺寸已调整为适应 {limit} 名玩家", Colors.YELLOW))
    
    @db_safe_operation
    def do_alias(self, arg):
        """
        设置玩家别名
        
        用法: alias <原名> <新名>
        参数:
          原名 - 玩家的原始名称
          新名 - 要为玩家设置的新名称
        
        示例:
          alias "旧名字" "新名字"   # 将玩家的名称从"旧名字"改为"新名字"
        """
        args = arg.split()
        if len(args) < 2:
            print(colorize("错误: 请输入原名和新名", Colors.RED))
            return
        
        original_name = args[0]
        new_name = ' '.join(args[1:])
        
        cursor = self.conn.cursor()
        
        try:
            cursor.execute(
                "SELECT player_id FROM player_aliases WHERE alias = ?",
                (original_name,)
            )
            result = cursor.fetchone()
            
            if not result:
                print(colorize(f"\n未找到玩家: {original_name}", Colors.YELLOW))
                return
            
            player_id = result[0]
            
            cursor.execute(
                "SELECT player_id FROM player_aliases WHERE alias = ?",
                (new_name,)
            )
            result = cursor.fetchone()
            
            if result:
                print(colorize(f"\n名称 '{new_name}' 已被其他玩家使用", Colors.RED))
                return
            
            current_time = datetime.now()
            cursor.execute(
                """
                INSERT INTO player_aliases (player_id, alias, first_seen, last_seen)
                VALUES (?, ?, ?, ?)
                """,
                (player_id, new_name, current_time, current_time)
            )
            
            cursor.execute(
                "UPDATE player_identity SET current_name = ? WHERE player_id = ?",
                (new_name, player_id)
            )
            
            self.conn.commit()
            print(colorize(f"\n成功将 '{original_name}' 的别名设置为 '{new_name}'", Colors.GREEN))
            
        except sqlite3.Error as e:
            self.conn.rollback()
            print(colorize(f"\n数据库错误: {e}", Colors.RED))
    
    @db_safe_operation
    def do_export(self, arg):
        """
        导出数据为CSV文件
        
        用法: export <类型> [模式] [天数]
        参数:
          类型 - 导出类型: top(顶级玩家) 或 history(历史数据)
          模式 - 可选，模式编号，默认为当前模式
          天数 - 可选，要导出的历史天数，仅对history类型有效，默认为30天
        
        示例:
          export top        # 导出当前模式的顶级玩家数据
          export top 0      # 导出模式0的顶级玩家数据
          export history    # 导出最近30天的历史数据
          export history 0 60  # 导出模式0最近60天的历史数据
        """
        args = arg.split()
        if not args:
            print(colorize("错误: 请指定导出类型: top 或 history", Colors.RED))
            return
        
        export_type = args[0].lower()
        mode = self.current_mode
        days = 30
        
        if len(args) > 1:
            try:
                if int(args[1]) in self.mode_names:
                    mode = int(args[1])
                    if len(args) > 2:
                        days = int(args[2])
                else:
                    days = int(args[1])
            except ValueError:
                print(colorize("错误: 请输入有效的数字", Colors.RED))
                return
        
        cursor = self.conn.cursor()
        
        if export_type == "top":
            cursor.execute(
                "SELECT MAX(crawl_time) FROM player_rankings WHERE mode = ?",
                (mode,)
            )
            latest_time = cursor.fetchone()[0]
            
            if not latest_time:
                mode_name = self.mode_names.get(mode, "未知")
                print(colorize(f"\n模式 {mode} ({mode_name}) 没有数据", Colors.YELLOW))
                return
            
            cursor.execute(
                """
                SELECT pr.rank, pr.name, pr.lv, pr.exp, pr.acc, pr.combo, pr.pc
                FROM player_rankings pr
                WHERE pr.mode = ? AND pr.crawl_time = ?
                ORDER BY pr.rank
                """,
                (mode, latest_time)
            )
            
            players = cursor.fetchall()
            
            if not players:
                mode_name = self.mode_names.get(mode, "未知")
                print(colorize(f"\n模式 {mode} ({mode_name}) 没有找到玩家数据", Colors.YELLOW))
                return
            
            df = pd.DataFrame(players, columns=['排名', '玩家名', '等级', '经验', '准确率', '连击', '游玩次数'])
            
            # 使用唯一文件名避免覆盖
            base_filename = f"top_players_mode{mode}.csv"
            filename = self.get_unique_filename(base_filename, "csv")
            filepath = os.path.join(self.output_dir, filename)
            df.to_csv(filepath, index=False, encoding='utf-8-sig')
            
            print(colorize(f"\n已导出顶级玩家数据: {filepath}", Colors.GREEN))
            
        elif export_type == "history":
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            cursor.execute(
                """
                SELECT pr.name, pr.rank, pr.crawl_time, pr.mode
                FROM player_rankings pr
                WHERE pr.crawl_time >= ?
                ORDER BY pr.crawl_time, pr.mode, pr.rank
                """,
                (start_date,)
            )
            
            history_data = cursor.fetchall()
            
            if not history_data:
                print(colorize(f"\n最近 {days} 天没有数据", Colors.YELLOW))
                return
            
            df = pd.DataFrame(history_data, columns=['玩家名', '排名', '时间', '模式'])
            
            # 使用唯一文件名避免覆盖
            base_filename = f"history_last{days}days.csv"
            filename = self.get_unique_filename(base_filename, "csv")
            filepath = os.path.join(self.output_dir, filename)
            df.to_csv(filepath, index=False, encoding='utf-8-sig')
            
            print(colorize(f"\n已导出历史数据: {filepath}", Colors.GREEN))
            
        else:
            print(colorize("错误: 请指定有效的导出类型: top 或 history", Colors.RED))
    
    def do_update(self, arg):
        """
        更新数据（调用爬虫脚本）
        
        用法: update
        说明: 此命令会调用malody_rankings.py脚本更新数据
        
        注意: 更新过程中会暂时断开数据库连接，更新完成后会自动重连
        """
        print(colorize("\n开始更新数据...", Colors.CYAN))
        
        if self.conn:
            self.conn.close()
            self.conn = None
            print(colorize("已断开数据库连接", Colors.YELLOW))
        
        try:
            script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "malody_rankings.py")
            
            cmd = [sys.executable, script_path, "--once"]
            
            print(colorize(f"执行命令: {' '.join(cmd)}", Colors.YELLOW))
            print(colorize("=" * 50, Colors.CYAN))
            
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1
            )
            
            for line in process.stdout:
                clean_line = re.sub(r'\x1b\[[0-9;]*[mK]', '', line.strip())
                if clean_line:
                    print(clean_line)
            
            process.wait()
            
            if process.returncode == 0:
                print(colorize("\n数据更新成功!", Colors.GREEN))
            else:
                print(colorize(f"\n数据更新失败，退出码: {process.returncode}", Colors.RED))
                
        except Exception as e:
            print(colorize(f"\n更新过程中发生错误: {e}", Colors.RED))
        finally:
            print(colorize("重新连接数据库...", Colors.YELLOW))
            self.connect_db()
    
    def do_exit(self, arg):
        """退出程序"""
        print(colorize("\n感谢使用Malody排行榜数据可视化工具!", Colors.CYAN))
        self.cleanup()
        return True
    
    def do_quit(self, arg):
        """退出程序"""
        return self.do_exit(arg)
    
    do_q = do_quit
    do_e = do_exit

    def print_topics(self, header, cmds, cmdlen, maxcol):
        if cmds:
            self.stdout.write(colorize("%s\n" % str(header), Colors.CYAN))
            if self.ruler:
                self.stdout.write(colorize("%s\n" % str(self.ruler * len(header)), Colors.CYAN))
            self.columnize(cmds, maxcol-1)
            self.stdout.write("\n")

if __name__ == "__main__":
    if not os.path.exists("malody_rankings.db"):
        print(colorize("错误: 未找到数据库文件 'malody_rankings.db'", Colors.RED))
        print(colorize("请确保数据库文件与脚本在同一目录下", Colors.YELLOW))
        sys.exit(1)
    
    try:
        MalodyViz().cmdloop()
    except KeyboardInterrupt:
        print(colorize("\n程序被用户中断", Colors.YELLOW))
    except Exception as e:
        print(colorize(f"\n程序发生错误: {e}", Colors.RED))