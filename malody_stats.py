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

# 修复matplotlib中文字体问题
def setup_chinese_font():
    """设置中文字体支持"""
    try:
        import matplotlib.font_manager as fm
        import platform
        
        # 获取系统中所有字体
        fonts = fm.findSystemFonts()
        chinese_fonts = []
        
        # 常见中文字体列表
        common_chinese_fonts = [
            'SimHei', 'Microsoft YaHei', 'SimSun', 'KaiTi', 'FangSong',
            'STSong', 'STKaiti', 'STFangsong', 'STHeiti', 'PingFang SC',
            'Hiragino Sans GB', 'WenQuanYi Micro Hei', 'Noto Sans CJK SC'
        ]
        
        # 查找可用的中文字体
        for font_path in fonts:
            try:
                font = fm.FontProperties(fname=font_path)
                font_name = font.get_name()
                if any(ch_font in font_name for ch_font in common_chinese_fonts):
                    chinese_fonts.append(font_path)
            except:
                continue
        
        if chinese_fonts:
            # 使用找到的第一个中文字体
            plt.rcParams['font.sans-serif'] = [fm.FontProperties(fname=chinese_fonts[0]).get_name()] + plt.rcParams['font.sans-serif']
            plt.rcParams['axes.unicode_minus'] = False
            print(f"已设置中文字体: {fm.FontProperties(fname=chinese_fonts[0]).get_name()}")
            return True
        else:
            print("警告: 未找到中文字体，图表中的中文可能显示为方块")
            return False
            
    except Exception as e:
        print(f"字体设置错误: {e}")
        return False

# 在程序初始化时调用字体设置
setup_chinese_font()


# PowerShell 颜色支持修复
def enable_powershell_colors():
    """在 PowerShell 中启用 ANSI 颜色支持"""
    if sys.platform == "win32":
        try:
            # 方法1: 通过设置环境变量启用虚拟终端
            os.environ["TERM"] = "xterm-256color"
            
            # 方法2: 使用 ctypes 启用虚拟终端处理
            if hasattr(sys, 'getwindowsversion'):
                import ctypes
                from ctypes import wintypes
                
                kernel32 = ctypes.windll.kernel32
                STD_OUTPUT_HANDLE = -11
                
                # 获取标准输出句柄
                hstdout = kernel32.GetStdHandle(STD_OUTPUT_HANDLE)
                
                # 获取当前控制台模式
                mode = wintypes.DWORD()
                if kernel32.GetConsoleMode(hstdout, ctypes.byref(mode)):
                    # 启用虚拟终端处理
                    ENABLE_VIRTUAL_TERMINAL_PROCESSING = 0x0004
                    new_mode = mode.value | ENABLE_VIRTUAL_TERMINAL_PROCESSING
                    kernel32.SetConsoleMode(hstdout, new_mode)
                    return True
        except:
            pass
    return False

# 在程序启动时启用 PowerShell 颜色
enable_powershell_colors()

# 修复Python 3.12中SQLite datetime适配器的弃用警告
def adapt_datetime(dt):
    return dt.isoformat()

def convert_datetime(s):
    return datetime.fromisoformat(s.decode())

sqlite3.register_adapter(datetime, adapt_datetime)
sqlite3.register_converter("timestamp", convert_datetime)

# 设置matplotlib使用Agg后端（无GUI）
plt.switch_backend('Agg')

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
    if sys.platform == "win32":
        # 在 Windows 上检查是否在支持颜色的终端中运行
        try:
            # 检查是否在 Windows Terminal、PowerShell 5.1+ 或支持 ANSI 的 CMD 中运行
            term_program = os.environ.get('TERM_PROGRAM', '')
            term = os.environ.get('TERM', '')
            
            # Windows Terminal 或现代 PowerShell
            if 'WindowsTerminal' in term_program or 'TERM' in os.environ:
                return True
            
            # 检查 PowerShell 版本（5.1+ 支持 ANSI）
            import subprocess
            result = subprocess.run(['powershell', '$PSVersionTable.PSVersion.Major'], 
                                  capture_output=True, text=True, timeout=2)
            if result.returncode == 0 and result.stdout.strip().isdigit():
                ps_version = int(result.stdout.strip())
                if ps_version >= 5:
                    return True
                    
            # 最后的手段：尝试检测控制台能力
            import ctypes
            from ctypes import wintypes
            
            kernel32 = ctypes.windll.kernel32
            STD_OUTPUT_HANDLE = -11
            hstdout = kernel32.GetStdHandle(STD_OUTPUT_HANDLE)
            
            mode = wintypes.DWORD()
            if kernel32.GetConsoleMode(hstdout, ctypes.byref(mode)):
                ENABLE_VIRTUAL_TERMINAL_PROCESSING = 0x0004
                return (mode.value & ENABLE_VIRTUAL_TERMINAL_PROCESSING) != 0
                
        except:
            pass
        return False
    else:
        # 非 Windows 系统通常支持颜色
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

def format_change(change_value, reverse=False, is_percent=False):
    """格式化变化值，添加颜色"""
    if change_value is None:
        return "N/A"
    
    if change_value == 0:
        return "0"
    
    if is_percent:
        change_str = f"{change_value:+.2f}%"
    else:
        change_str = f"{change_value:+d}"
    
    # 对于排名变化，负数表示进步（排名上升）
    if reverse:
        if change_value < 0:
            return colorize(change_str, Colors.GREEN)  # 进步
        elif change_value > 0:
            return colorize(change_str, Colors.RED)    # 退步
    else:
        if change_value > 0:
            return colorize(change_str, Colors.GREEN)  # 增加
        elif change_value < 0:
            return colorize(change_str, Colors.RED)    # 减少
    
    return change_str

def format_number(number):
    """格式化大数字，添加千位分隔符"""
    if number is None:
        return "N/A"
    return f"{number:,}"

def get_terminal_width():
    """获取终端宽度"""
    try:
        return shutil.get_terminal_size().columns
    except:
        return 80

def get_separator(width=None):
    """获取自适应分隔线"""
    if width is None:
        width = get_terminal_width()
    return colorize("=" * min(width, 120), Colors.CYAN)

def get_subseparator(width=None):
    """获取自适应子分隔线"""
    if width is None:
        width = get_terminal_width()
    return colorize("-" * min(width, 100), Colors.CYAN)

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
                    print(get_separator())
                    print(textwrap.dedent(func.__doc__))
                else:
                    print(colorize(f"没有找到命令 '{cmd_name}' 的帮助文档", Colors.YELLOW))
            else:
                print(colorize(f"未知命令: {cmd_name}", Colors.RED))
        else:
            print(colorize("\nMalody排行榜数据可视化工具 - 命令列表", Colors.CYAN))
            print(get_separator())
            
            commands = [
                ("ls [路径]", "列出目录内容"),
                ("mode [模式]", "设置或查看当前模式"),
                ("top [数量]", "显示顶级玩家排名"),
                ("player <玩家名> [模式]", "查看玩家信息"),
                ("history <玩家名> [模式] [天数]", "查看玩家历史排名并生成图表"),
                ("compare <玩家1> <玩家2> [...] [模式] [天数]", "比较多个玩家的排名变化"),
                ("top_chart [数量] [模式]", "生成顶级玩家分布图表"),
                ("trend <起始日期> [模式] [显示项]", "统计玩家数据变化趋势"),
                ("search <关键词> [类型] [模式]", "搜索玩家/谱面/创作者"),
                ("stb_stats [模式]", "谱面基础统计"),
                ("stb_summary [模式] [级别]", "谱面综合统计报告"),
                ("stb_hot [模式] [排序] [数量]", "热门谱面排行榜"),
                ("stb_pie [模式] [类型]", "生成谱面分布饼状图"),
                ("stb_recent [天数] [模式] [数量]", "查询最近更新的谱面"),
                ("stb_quality [模式]", "检查数据质量"),
                ("stb_trends [模式] [周期]", "分析谱面数据趋势"),
                ("stb_compare [模式列表]", "比较不同模式的谱面数据"),
                ("alias <原名> <新名>", "设置玩家别名"),
                ("export <类型> [模式] [天数]", "导出数据为CSV文件"),
                ("update", "更新数据（调用爬虫脚本）"),
                ("help [命令]", "显示帮助信息"),
                ("exit/quit", "退出程序")
            ]
            
            for cmd, desc in commands:
                print(f"  {colorize(cmd, Colors.GREEN):<35} {desc}")
            
            print(colorize("\n模式编号对应表:", Colors.CYAN))
            print(get_subseparator())
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
                print(get_separator())
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
        
        terminal_width = get_terminal_width()
        
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
        print(get_separator())
        print(colorize(header_format.format("排名", "玩家名", "等级", "经验", "准确率", "连击", "游玩次数"), Colors.BOLD))
        print(get_separator())
        
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
        查看玩家信息（支持玩家名或UID）
        
        用法: player <玩家名或UID> [模式]
        参数:
          玩家名或UID - 要查询的玩家名称或UID
          模式       - 可选，模式编号，默认为当前模式
        
        示例:
          player Zani      # 查看玩家Zani在当前模式的信息
          player 123456    # 查看UID为123456的玩家
          player Zani 0    # 查看Zani在模式0的信息
        """
        args = arg.split()
        if not args:
            print(colorize("错误: 请输入玩家名或UID", Colors.RED))
            return
        
        identifier = args[0]
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
        
        # 判断是UID还是名称
        if identifier.isdigit():
            # UID查询
            cursor.execute(
                "SELECT player_id FROM player_identity WHERE uid = ?", 
                (identifier,)
            )
        else:
            # 名称查询
            cursor.execute(
                "SELECT player_id FROM player_aliases WHERE alias = ?",
                (identifier,)
            )
        
        result = cursor.fetchone()
        
        if not result:
            print(colorize(f"\n未找到玩家: {identifier}", Colors.YELLOW))
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
            print(colorize(f"\n玩家 {identifier} 在模式 {mode} ({mode_name}) 中没有数据", Colors.YELLOW))
            return
            
        if len(player_data) < 7:
            print(colorize(f"\n玩家 {identifier} 的数据不完整", Colors.YELLOW))
            return
            
        rank, lv, exp, acc, combo, pc, crawl_time = player_data
        
        mode_name = self.mode_names.get(mode, "未知")
        print(colorize(f"\n玩家: {identifier} (模式 {mode} - {mode_name})", Colors.CYAN))
        print(colorize(f"数据时间: {crawl_time}", Colors.YELLOW))
        print(get_separator())
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
        print(get_separator())
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
    def do_trend(self, arg):
        """
        统计玩家数据变化趋势
        
        用法: trend <起始日期> [模式] [显示项]
        参数:
          起始日期 - 格式为YYYY-MM-DD，统计从该日期开始的变化
          模式     - 可选，模式编号，默认为当前模式
          显示项   - 可选，要显示的统计项，用逗号分隔，如: rank,lv,exp,acc,combo,pc
        
        示例:
          trend 2024-01-01                    # 统计从2024年1月1日开始的玩家数据变化
          trend 2024-01-01 0                  # 统计模式0从2024年1月1日开始的玩家数据变化
          trend 2024-01-01 0 rank,exp,acc     # 只显示排名、经验和准确率的变化
        """
        args = arg.split()
        if not args:
            print(colorize("错误: 请输入起始日期", Colors.RED))
            return
        
        # 解析起始日期
        try:
            start_date = datetime.strptime(args[0], "%Y-%m-%d")
        except ValueError:
            print(colorize("错误: 日期格式应为 YYYY-MM-DD", Colors.RED))
            return
        
        mode = self.current_mode
        display_fields = ["rank", "lv", "exp", "acc", "combo", "pc"]  # 默认显示所有项
        
        if len(args) > 1:
            # 检查第二个参数是否为模式
            if args[1].isdigit() and int(args[1]) in self.mode_names:
                mode = int(args[1])
                # 检查是否有显示项参数
                if len(args) > 2:
                    display_fields = [field.strip() for field in args[2].split(",")]
            else:
                # 第二个参数是显示项
                display_fields = [field.strip() for field in args[1].split(",")]
        
        # 验证显示项
        valid_fields = ["rank", "lv", "exp", "acc", "combo", "pc"]
        invalid_fields = [field for field in display_fields if field not in valid_fields]
        if invalid_fields:
            print(colorize(f"错误: 无效的显示项: {', '.join(invalid_fields)}", Colors.RED))
            print(colorize(f"有效的显示项: {', '.join(valid_fields)}", Colors.YELLOW))
            return
        
        cursor = self.conn.cursor()
        
        # 获取起始日期的数据（如果一天内有多个数据，取第一个）
        cursor.execute(
            """
            SELECT crawl_time 
            FROM player_rankings 
            WHERE mode = ? AND DATE(crawl_time) >= DATE(?)
            ORDER BY crawl_time
            LIMIT 1
            """,
            (mode, start_date)
        )
        
        start_result = cursor.fetchone()
        
        if not start_result:
            print(colorize(f"错误: 在 {start_date.date()} 及之后没有找到模式 {mode} 的数据", Colors.RED))
            return
        
        start_crawl_time = start_result[0]
        
        # 获取最新数据
        cursor.execute(
            "SELECT MAX(crawl_time) FROM player_rankings WHERE mode = ?",
            (mode,)
        )
        end_crawl_time = cursor.fetchone()[0]
        
        if not end_crawl_time:
            print(colorize(f"错误: 模式 {mode} 没有最新数据", Colors.RED))
            return
        
        # 获取起始日期的玩家数据
        cursor.execute(
            """
            SELECT player_id, name, rank, lv, exp, acc, combo, pc
            FROM player_rankings
            WHERE mode = ? AND crawl_time = ?
            ORDER BY rank
            """,
            (mode, start_crawl_time)
        )
        
        start_players = {row[0]: (row[1], row[2], row[3], row[4], row[5], row[6], row[7]) for row in cursor.fetchall()}
        
        # 获取最新日期的玩家数据
        cursor.execute(
            """
            SELECT player_id, name, rank, lv, exp, acc, combo, pc
            FROM player_rankings
            WHERE mode = ? AND crawl_time = ?
            ORDER BY rank
            """,
            (mode, end_crawl_time)
        )
        
        end_players = {row[0]: (row[1], row[2], row[3], row[4], row[5], row[6], row[7]) for row in cursor.fetchall()}
        
        # 分析变化
        all_player_ids = set(start_players.keys()) | set(end_players.keys())
        
        trend_data = []
        
        for player_id in all_player_ids:
            in_start = player_id in start_players
            in_end = player_id in end_players
            
            if in_start and in_end:
                # 一直在榜的玩家
                start_name, start_rank, start_lv, start_exp, start_acc, start_combo, start_pc = start_players[player_id]
                end_name, end_rank, end_lv, end_exp, end_acc, end_combo, end_pc = end_players[player_id]
                
                # 检查是否改名
                current_name = end_name if end_name != start_name else start_name
                
                # 检查是否有任何变化
                has_changes = (
                    start_rank != end_rank or
                    start_lv != end_lv or
                    start_exp != end_exp or
                    start_acc != end_acc or
                    start_combo != end_combo or
                    start_pc != end_pc
                )
                
                # 如果用户指定了显示字段，检查这些字段是否有变化
                if display_fields:
                    field_has_changes = False
                    for field in display_fields:
                        if field == "rank" and start_rank != end_rank:
                            field_has_changes = True
                            break
                        elif field == "lv" and start_lv != end_lv:
                            field_has_changes = True
                            break
                        elif field == "exp" and start_exp != end_exp:
                            field_has_changes = True
                            break
                        elif field == "acc" and start_acc != end_acc:
                            field_has_changes = True
                            break
                        elif field == "combo" and start_combo != end_combo:
                            field_has_changes = True
                            break
                        elif field == "pc" and start_pc != end_pc:
                            field_has_changes = True
                            break
                    
                    # 如果没有变化且用户指定了显示字段，跳过这个玩家
                    if not field_has_changes:
                        continue
                
                trend_data.append({
                    'player_id': player_id,
                    'name': current_name,
                    'status': '=',  # 一直在榜
                    'start_rank': start_rank,
                    'end_rank': end_rank,
                    'rank_change': end_rank - start_rank,  # 负数表示进步
                    'start_lv': start_lv,
                    'end_lv': end_lv,
                    'lv_change': end_lv - start_lv,
                    'start_exp': start_exp,
                    'end_exp': end_exp,
                    'exp_change': end_exp - start_exp,
                    'start_acc': start_acc,
                    'end_acc': end_acc,
                    'acc_change': end_acc - start_acc,
                    'start_combo': start_combo,
                    'end_combo': end_combo,
                    'combo_change': end_combo - start_combo,
                    'start_pc': start_pc,
                    'end_pc': end_pc,
                    'pc_change': end_pc - start_pc,
                    'has_changes': has_changes
                })
            elif in_start and not in_end:
                # 掉出榜的玩家
                start_name, start_rank, start_lv, start_exp, start_acc, start_combo, start_pc = start_players[player_id]
                
                trend_data.append({
                    'player_id': player_id,
                    'name': start_name,
                    'status': '-',  # 掉出榜
                    'start_rank': start_rank,
                    'end_rank': None,
                    'rank_change': None,
                    'start_lv': start_lv,
                    'end_lv': None,
                    'lv_change': None,
                    'start_exp': start_exp,
                    'end_exp': None,
                    'exp_change': None,
                    'start_acc': start_acc,
                    'end_acc': None,
                    'acc_change': None,
                    'start_combo': start_combo,
                    'end_combo': None,
                    'combo_change': None,
                    'start_pc': start_pc,
                    'end_pc': None,
                    'pc_change': None,
                    'has_changes': True  # 掉出榜本身就是变化
                })
            else:
                # 新上榜的玩家
                end_name, end_rank, end_lv, end_exp, end_acc, end_combo, end_pc = end_players[player_id]
                
                trend_data.append({
                    'player_id': player_id,
                    'name': end_name,
                    'status': '+',  # 新上榜
                    'start_rank': None,
                    'end_rank': end_rank,
                    'rank_change': None,
                    'start_lv': None,
                    'end_lv': end_lv,
                    'lv_change': None,
                    'start_exp': None,
                    'end_exp': end_exp,
                    'exp_change': None,
                    'start_acc': None,
                    'end_acc': end_acc,
                    'acc_change': None,
                    'start_combo': None,
                    'end_combo': end_combo,
                    'combo_change': None,
                    'start_pc': None,
                    'end_pc': end_pc,
                    'pc_change': None,
                    'has_changes': True  # 新上榜本身就是变化
                })
        
        # 如果没有数据，显示提示
        if not trend_data:
            print(colorize(f"\n在指定的时间范围内，模式 {mode} 没有发现数据变化", Colors.YELLOW))
            return
        
        # 按结束排名排序（掉出榜的玩家排最后）
        trend_data.sort(key=lambda x: (x['end_rank'] is None, x['end_rank'] or 9999))
        
        # 显示结果
        mode_name = self.mode_names.get(mode, "未知")
        print(colorize(f"\n玩家数据变化趋势 (模式 {mode} - {mode_name})", Colors.CYAN))
        print(colorize(f"时间范围: {start_crawl_time} 到 {end_crawl_time}", Colors.YELLOW))
        
        separator_width = get_terminal_width()
        print(get_separator(separator_width))
        
        # 构建表头
        header_parts = []
        format_specs = []
        
        # 状态和玩家名总是显示
        header_parts.extend(["状态", "玩家名"])
        format_specs.extend([8, 20])  # 状态和玩家名的宽度
        
        # 根据选择的字段添加表头
        field_configs = {
            "rank": ("排名", 10, 10, 10),
            "lv": ("等级", 10, 10, 10),
            "exp": ("经验", 12, 12, 10),
            "acc": ("准确率", 12, 12, 12),
            "combo": ("连击", 10, 10, 10),
            "pc": ("游玩", 10, 10, 10)
        }
        
        for field in display_fields:
            if field in field_configs:
                name, start_width, end_width, change_width = field_configs[field]
                header_parts.extend([f"起始{name}", f"结束{name}", f"{name}变化"])
                format_specs.extend([start_width, end_width, change_width])
        
        # 计算总宽度
        total_width = sum(format_specs) + (len(format_specs) - 1) * 2  # 每列之间2个空格
        
        # 如果总宽度超过终端宽度，调整玩家名宽度
        if total_width > separator_width:
            excess_width = total_width - separator_width
            player_name_width = max(10, 20 - excess_width)  # 玩家名最小宽度为10
            format_specs[1] = player_name_width
        
        # 构建格式字符串
        format_parts = []
        for width in format_specs:
            format_parts.append(f"{{:<{width}}}")
        header_format = "  ".join(format_parts)
        
        # 打印表头
        print(colorize(header_format.format(*header_parts), Colors.BOLD))
        print(get_separator(separator_width))
        
        # 打印数据行
        for player in trend_data:
            row_parts = []
            
            # 状态和玩家名
            status_symbol = player['status']
            if status_symbol == '+':
                status_display = colorize("[+]", Colors.GREEN)
            elif status_symbol == '-':
                status_display = colorize("[-]", Colors.RED)
            else:
                status_display = colorize("[=]", Colors.BLUE)
            
            # 处理玩家名长度
            player_name = player['name']
            max_name_width = format_specs[1]
            if len(player_name) > max_name_width:
                player_name = player_name[:max_name_width-3] + "..."
            
            row_parts.extend([status_display, player_name])
            
            # 根据选择的字段添加数据
            for field in display_fields:
                if field == "rank":
                    row_parts.extend([
                        str(player['start_rank']) if player['start_rank'] is not None else "N/A",
                        str(player['end_rank']) if player['end_rank'] is not None else "掉出",
                        format_change(player['rank_change'], reverse=True)  # 排名变化：负数表示进步
                    ])
                elif field == "lv":
                    row_parts.extend([
                        str(player['start_lv']) if player['start_lv'] is not None else "N/A",
                        str(player['end_lv']) if player['end_lv'] is not None else "N/A",
                        format_change(player['lv_change'])
                    ])
                elif field == "exp":
                    row_parts.extend([
                        format_number(player['start_exp']) if player['start_exp'] is not None else "N/A",
                        format_number(player['end_exp']) if player['end_exp'] is not None else "N/A",
                        format_change(player['exp_change'])
                    ])
                elif field == "acc":
                    row_parts.extend([
                        f"{player['start_acc']:.2f}%" if player['start_acc'] is not None else "N/A",
                        f"{player['end_acc']:.2f}%" if player['end_acc'] is not None else "N/A",
                        format_change(player['acc_change'], is_percent=True)
                    ])
                elif field == "combo":
                    row_parts.extend([
                        format_number(player['start_combo']) if player['start_combo'] is not None else "N/A",
                        format_number(player['end_combo']) if player['end_combo'] is not None else "N/A",
                        format_change(player['combo_change'])
                    ])
                elif field == "pc":
                    row_parts.extend([
                        format_number(player['start_pc']) if player['start_pc'] is not None else "N/A",
                        format_number(player['end_pc']) if player['end_pc'] is not None else "N/A",
                        format_change(player['pc_change'])
                    ])
            
            print(header_format.format(*row_parts))
        
        print(get_separator(separator_width))
        
        # 统计信息
        total_players = len(trend_data)
        stayed_players = len([p for p in trend_data if p['status'] == '='])
        dropped_players = len([p for p in trend_data if p['status'] == '-'])
        new_players = len([p for p in trend_data if p['status'] == '+'])
        
        print(colorize(f"统计: 总计 {total_players} 名玩家 | 一直在榜: {stayed_players} | 掉出榜: {dropped_players} | 新上榜: {new_players}", Colors.YELLOW))
        
        # 导出选项
        export_choice = input(colorize("\n是否导出为CSV文件? (y/N): ", Colors.CYAN)).lower()
        if export_choice == 'y':
            self.export_trend_data(trend_data, display_fields, mode, start_crawl_time, end_crawl_time)
    
    def export_trend_data(self, trend_data, display_fields, mode, start_time, end_time):
        """导出趋势数据为CSV文件"""
        # 构建数据框
        data_dict = {}
        
        # 基本字段
        data_dict['状态'] = [player['status'] for player in trend_data]
        data_dict['玩家名'] = [player['name'] for player in trend_data]
        
        # 根据选择的字段添加数据
        if "rank" in display_fields:
            data_dict['起始排名'] = [player['start_rank'] for player in trend_data]
            data_dict['结束排名'] = [player['end_rank'] for player in trend_data]
            data_dict['排名变化'] = [player['rank_change'] for player in trend_data]
        
        if "lv" in display_fields:
            data_dict['起始等级'] = [player['start_lv'] for player in trend_data]
            data_dict['结束等级'] = [player['end_lv'] for player in trend_data]
            data_dict['等级变化'] = [player['lv_change'] for player in trend_data]
        
        if "exp" in display_fields:
            data_dict['起始经验'] = [player['start_exp'] for player in trend_data]
            data_dict['结束经验'] = [player['end_exp'] for player in trend_data]
            data_dict['经验变化'] = [player['exp_change'] for player in trend_data]
        
        if "acc" in display_fields:
            data_dict['起始准确率'] = [player['start_acc'] for player in trend_data]
            data_dict['结束准确率'] = [player['end_acc'] for player in trend_data]
            data_dict['准确率变化'] = [player['acc_change'] for player in trend_data]
        
        if "combo" in display_fields:
            data_dict['起始连击'] = [player['start_combo'] for player in trend_data]
            data_dict['结束连击'] = [player['end_combo'] for player in trend_data]
            data_dict['连击变化'] = [player['combo_change'] for player in trend_data]
        
        if "pc" in display_fields:
            data_dict['起始游玩次数'] = [player['start_pc'] for player in trend_data]
            data_dict['结束游玩次数'] = [player['end_pc'] for player in trend_data]
            data_dict['游玩次数变化'] = [player['pc_change'] for player in trend_data]
        
        df = pd.DataFrame(data_dict)
        
        # 生成文件名
        mode_name = self.mode_names.get(mode, "未知")
        base_filename = f"trend_mode{mode}_{start_time.strftime('%Y%m%d')}_{end_time.strftime('%Y%m%d')}.csv"
        filename = self.get_unique_filename(base_filename, "csv")
        filepath = os.path.join(self.output_dir, filename)
        
        # 保存文件
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        print(colorize(f"\n已导出趋势数据: {filepath}", Colors.GREEN))
    
    @db_safe_operation
    def do_search(self, arg):
        """
        通用搜索功能
        
        用法: search <关键词> [类型] [模式]
        参数:
          关键词 - 要搜索的内容
          类型   - player(玩家), chart(谱面), creator(创作者), 默认为player
          模式   - 可选，模式编号
        
        示例:
          search Zani                    # 搜索玩家Zani
          search "song title" chart      # 搜索谱面
          search creator_name creator    # 搜索创作者
          search 123456 player           # 搜索UID为123456的玩家
        """
        args = arg.split()
        if not args:
            print(colorize("错误: 请输入搜索关键词", Colors.RED))
            return
        
        keyword = args[0]
        search_type = "player"
        mode = self.current_mode
        
        if len(args) > 1:
            search_type = args[1].lower()
        
        if len(args) > 2:
            try:
                mode = int(args[2])
            except ValueError:
                print(colorize("错误: 模式必须是数字", Colors.RED))
                return
        
        cursor = self.conn.cursor()
        
        if search_type == "player":
            self._search_players(cursor, keyword, mode)
        elif search_type == "chart":
            self._search_charts(cursor, keyword, mode)
        elif search_type == "creator":
            self._search_creators(cursor, keyword, mode)
        else:
            print(colorize(f"错误: 不支持的搜索类型 '{search_type}'", Colors.RED))
    
    def _search_players(self, cursor, keyword, mode):
        """搜索玩家（支持名称和UID）"""
        if keyword.isdigit():
            # UID搜索
            cursor.execute(
                "SELECT pi.player_id, pi.current_name, pi.uid "
                "FROM player_identity pi WHERE pi.uid = ?", 
                (keyword,)
            )
            result = cursor.fetchone()
            if result:
                player_id, name, uid = result
                cursor.execute(
                    """
                    SELECT pr.rank, pr.lv, pr.acc, pr.combo, pr.pc, pr.crawl_time
                    FROM player_rankings pr
                    WHERE pr.player_id = ? AND pr.mode = ?
                    ORDER BY pr.crawl_time DESC LIMIT 1
                    """,
                    (player_id, mode)
                )
                player_data = cursor.fetchone()
                if player_data:
                    rank, lv, acc, combo, pc, crawl_time = player_data
                    print(colorize(f"\n玩家: {name} (UID: {uid})", Colors.CYAN))
                    print(get_separator())
                    print(f"排名: {rank}, 等级: {lv}, 准确率: {acc:.2f}%")
                    print(f"连击: {combo}, 游玩次数: {pc}")
                    return
            
            print(colorize(f"未找到UID为 {keyword} 的玩家", Colors.YELLOW))
        else:
            # 名称搜索
            cursor.execute(
                """
                SELECT DISTINCT pr.name, pr.rank, pr.lv, pr.acc, pr.crawl_time
                FROM player_rankings pr
                WHERE pr.name LIKE ? AND pr.mode = ?
                ORDER BY pr.rank LIMIT 10
                """,
                (f'%{keyword}%', mode)
            )
            results = cursor.fetchall()
            if results:
                print(colorize(f"\n找到 {len(results)} 个匹配玩家:", Colors.CYAN))
                print(get_separator())
                for name, rank, lv, acc, crawl_time in results:
                    print(f"{name}: 排名 {rank}, 等级 {lv}, 准确率 {acc:.2f}%")
            else:
                print(colorize(f"未找到包含 '{keyword}' 的玩家", Colors.YELLOW))
    
    def _search_charts(self, cursor, keyword, mode):
        """搜索谱面"""
        cursor.execute(
            """
            SELECT c.cid, c.version, c.level, c.status, s.title, s.artist,
                   c.creator_name, c.heat, c.donate_count, c.last_updated
            FROM charts c
            JOIN songs s ON c.sid = s.sid
            WHERE (s.title LIKE ? OR s.artist LIKE ?) AND c.mode = ?
            ORDER BY c.heat DESC LIMIT 10
            """,
            (f'%{keyword}%', f'%{keyword}%', mode)
        )
        results = cursor.fetchall()
        if results:
            print(colorize(f"\n找到 {len(results)} 个匹配谱面:", Colors.CYAN))
            print(get_separator())
            for cid, version, level, status, title, artist, creator, heat, donate, updated in results:
                status_name = {0: "Alpha", 1: "Beta", 2: "Stable"}.get(status, "Unknown")
                print(f"  {title} - {artist} (Lv.{level})")
                print(f"    版本: {version}, 状态: {status_name}, 热度: {heat}")
                print(f"    创作者: {creator}, CID: {cid}")
        else:
            print(colorize(f"未找到包含 '{keyword}' 的谱面", Colors.YELLOW))
    
    def _search_creators(self, cursor, keyword, mode):
        """搜索创作者"""
        cursor.execute(
            """
            SELECT creator_name, COUNT(*) as chart_count, 
                   AVG(heat) as avg_heat, MAX(heat) as max_heat
            FROM charts 
            WHERE creator_name LIKE ? AND mode = ?
            GROUP BY creator_name
            ORDER BY chart_count DESC LIMIT 10
            """,
            (f'%{keyword}%', mode)
        )
        results = cursor.fetchall()
        if results:
            print(colorize(f"\n找到 {len(results)} 个匹配创作者:", Colors.CYAN))
            print(get_separator())
            for creator, count, avg_heat, max_heat in results:
                print(f"  {creator}: {count} 个谱面")
                print(f"    平均热度: {avg_heat:.1f}, 最高热度: {max_heat}")
        else:
            print(colorize(f"未找到包含 '{keyword}' 的创作者", Colors.YELLOW))
    
    @db_safe_operation
    def do_stb_stats(self, arg):
        """
        谱面基础统计
        
        用法: stb_stats [模式]
        参数:
          模式 - 可选，模式编号，默认为当前模式
        
        示例:
          stb_stats      # 当前模式统计
          stb_stats 0    # 模式0统计
        """
        args = arg.split()
        mode = self.current_mode
        if args:
            try:
                mode = int(args[0])
            except ValueError:
                print(colorize("错误: 模式必须是数字", Colors.RED))
                return
        
        cursor = self.conn.cursor()
        stats = self._get_chart_stats(cursor, mode)
        self._display_chart_stats(stats, mode)
    
    def _get_chart_stats(self, cursor, mode):
        """获取谱面统计信息"""
        stats = {}
        
        # 总谱面数
        cursor.execute("SELECT COUNT(*) FROM charts WHERE mode = ?", (mode,))
        stats['total_charts'] = cursor.fetchone()[0]
        
        # 按状态统计
        cursor.execute(
            "SELECT status, COUNT(*) FROM charts WHERE mode = ? GROUP BY status",
            (mode,)
        )
        stats['status_dist'] = dict(cursor.fetchall())
        
        # 难度统计
        cursor.execute(
            "SELECT level, COUNT(*) FROM charts WHERE mode = ? AND level IS NOT NULL GROUP BY level ORDER BY CAST(level AS REAL)",
            (mode,)
        )
        stats['level_dist'] = dict(cursor.fetchall())
        
        # 创作者统计
        cursor.execute(
            "SELECT creator_name, COUNT(*) FROM charts WHERE mode = ? AND creator_name IS NOT NULL GROUP BY creator_name ORDER BY COUNT(*) DESC LIMIT 10",
            (mode,)
        )
        stats['top_creators'] = cursor.fetchall()
        
        # 热度统计
        cursor.execute(
            "SELECT AVG(heat), MAX(heat), AVG(donate_count), MAX(donate_count) FROM charts WHERE mode = ?",
            (mode,)
        )
        heat_stats = cursor.fetchone()
        stats['heat_avg'], stats['heat_max'], stats['donate_avg'], stats['donate_max'] = heat_stats
        
        return stats
    
    def _display_chart_stats(self, stats, mode):
        """显示谱面统计信息"""
        mode_name = self.mode_names.get(mode, "未知")
        
        print(colorize(f"\n谱面统计 - 模式 {mode} ({mode_name})", Colors.CYAN))
        print(get_separator())
        
        print(f"总谱面数: {colorize(stats['total_charts'], Colors.GREEN)}")
        
        # 状态分布
        print(f"\n{colorize('状态分布:', Colors.BOLD)}")
        status_names = {0: "Alpha", 1: "Beta", 2: "Stable"}
        for status, count in stats['status_dist'].items():
            status_name = status_names.get(status, f"未知({status})")
            print(f"  {status_name}: {count}")
        
        # 难度分布
        if stats['level_dist']:
            print(f"\n{colorize('难度分布:', Colors.BOLD)}")
            for level, count in sorted(stats['level_dist'].items(), key=lambda x: float(x[0])):
                print(f"  Lv.{level}: {count}")
        
        # 热门创作者
        if stats['top_creators']:
            print(f"\n{colorize('热门创作者:', Colors.BOLD)}")
            for creator, count in stats['top_creators']:
                print(f"  {creator}: {count} 个谱面")
        
        # 热度统计（忽略爱心数）
        print(f"\n{colorize('热度统计:', Colors.BOLD)}")
        print(f"  平均热度: {stats['heat_avg']:.1f}")
        print(f"  最高热度: {stats['heat_max']}")
        print(f"  平均打赏: {stats['donate_avg']:.1f}")
        print(f"  最多打赏: {stats['donate_max']}")
    
    @db_safe_operation
    def do_stb_pie(self, arg):
        """
        生成谱面分布饼状图
        
        用法: stb_pie [模式] [类型]
        参数:
          模式 - 可选，模式编号，默认为当前模式
          类型 - 可选，status(状态分布), level(难度分布), 默认为status
        
        示例:
          stb_pie        # 当前模式状态分布饼图
          stb_pie 0 level # 模式0难度分布饼图
        """
        args = arg.split()
        mode = self.current_mode
        chart_type = "status"
        
        if args:
            try:
                if args[0].isdigit():
                    mode = int(args[0])
                    if len(args) > 1:
                        chart_type = args[1].lower()
                else:
                    chart_type = args[0].lower()
                    if len(args) > 1 and args[1].isdigit():
                        mode = int(args[1])
            except ValueError:
                print(colorize("错误: 模式必须是数字", Colors.RED))
                return
        
        cursor = self.conn.cursor()
        
        if chart_type == "status":
            self._generate_status_pie(cursor, mode)
        elif chart_type == "level":
            self._generate_level_pie(cursor, mode)
        else:
            print(colorize(f"错误: 不支持的图表类型 '{chart_type}'", Colors.RED))
    
    def _generate_status_pie(self, cursor, mode):
        """生成状态分布饼图"""
        cursor.execute(
            "SELECT status, COUNT(*) FROM charts WHERE mode = ? GROUP BY status",
            (mode,)
        )
        status_data = cursor.fetchall()
        
        if not status_data:
            print(colorize(f"模式 {mode} 没有谱面数据", Colors.YELLOW))
            return
        
        status_names = {0: "Alpha", 1: "Beta", 2: "Stable"}
        labels = []
        sizes = []
        
        for status, count in status_data:
            labels.append(status_names.get(status, f"未知({status})"))
            sizes.append(count)
        
        # 生成饼图
        fig, ax = plt.subplots(figsize=(10, 8))
        colors = plt.cm.Set3(np.linspace(0, 1, len(labels)))
        wedges, texts, autotexts = ax.pie(sizes, labels=labels, autopct='%1.1f%%', 
                                         colors=colors, startangle=90)
        
        # 美化文本
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
        
        mode_name = self.mode_names.get(mode, "未知")
        ax.set_title(f'谱面状态分布 - 模式 {mode} ({mode_name})', fontsize=14, fontweight='bold')
        
        # 保存图表
        base_filename = f"stb_status_pie_mode{mode}.png"
        filename = self.get_unique_filename(base_filename, "png")
        filepath = os.path.join(self.output_dir, filename)
        plt.tight_layout()
        plt.savefig(filepath, dpi=150, facecolor='white')
        plt.close()
        
        print(colorize(f"\n已生成状态分布饼图: {filepath}", Colors.GREEN))
    
    def _generate_level_pie(self, cursor, mode):
        """生成难度分布饼图"""
        cursor.execute(
            "SELECT level, COUNT(*) FROM charts WHERE mode = ? AND level IS NOT NULL GROUP BY level ORDER BY CAST(level AS REAL)",
            (mode,)
        )
        level_data = cursor.fetchall()
        
        if not level_data:
            print(colorize(f"模式 {mode} 没有难度数据", Colors.YELLOW))
            return
        
        # 分组处理：将难度分组以避免饼图过于碎片化
        level_groups = {}
        for level, count in level_data:
            level_float = float(level)
            if level_float < 5:
                group = "1-4"
            elif level_float < 10:
                group = "5-9" 
            elif level_float < 15:
                group = "10-14"
            else:
                group = "15+"
            
            level_groups[group] = level_groups.get(group, 0) + count
        
        labels = list(level_groups.keys())
        sizes = list(level_groups.values())
        
        # 生成饼图
        fig, ax = plt.subplots(figsize=(10, 8))
        colors = plt.cm.viridis(np.linspace(0, 1, len(labels)))
        wedges, texts, autotexts = ax.pie(sizes, labels=labels, autopct='%1.1f%%', 
                                         colors=colors, startangle=90)
        
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
        
        mode_name = self.mode_names.get(mode, "未知")
        ax.set_title(f'谱面难度分布 - 模式 {mode} ({mode_name})', fontsize=14, fontweight='bold')
        
        # 保存图表
        base_filename = f"stb_level_pie_mode{mode}.png"
        filename = self.get_unique_filename(base_filename, "png")
        filepath = os.path.join(self.output_dir, filename)
        plt.tight_layout()
        plt.savefig(filepath, dpi=150, facecolor='white')
        plt.close()
        
        print(colorize(f"\n已生成难度分布饼图: {filepath}", Colors.GREEN))
    
    @db_safe_operation
    def do_stb_recent(self, arg):
        """
        查询最近更新的谱面
        
        用法: stb_recent [天数] [模式] [数量]
        参数:
          天数 - 可选，最近多少天内更新的谱面，默认为7天
          模式 - 可选，模式编号，默认为当前模式  
          数量 - 可选，要显示的谱面数量，默认为10
        
        示例:
          stb_recent        # 最近7天更新的谱面
          stb_recent 30 0 20 # 模式0最近30天前20个更新谱面
        """
        args = arg.split()
        days = 7
        mode = self.current_mode
        limit = 10
        
        if args:
            try:
                days = int(args[0])
                if len(args) > 1:
                    mode = int(args[1])
                    if len(args) > 2:
                        limit = int(args[2])
            except ValueError:
                print(colorize("错误: 参数必须是数字", Colors.RED))
                return
        
        cursor = self.conn.cursor()
        
        # 计算时间范围
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        cursor.execute(
            """
            SELECT c.cid, c.version, c.level, c.status, s.title, s.artist,
                   c.creator_name, c.heat, c.last_updated, c.crawl_time
            FROM charts c
            JOIN songs s ON c.sid = s.sid
            WHERE c.mode = ? AND c.last_updated >= ?
            ORDER BY c.last_updated DESC
            LIMIT ?
            """,
            (mode, start_date, limit)
        )
        
        results = cursor.fetchall()
        
        if not results:
            print(colorize(f"\n模式 {mode} 最近 {days} 天没有更新的谱面", Colors.YELLOW))
            return
        
        mode_name = self.mode_names.get(mode, "未知")
        print(colorize(f"\n最近 {days} 天更新的谱面 - 模式 {mode} ({mode_name})", Colors.CYAN))
        print(get_separator())
        
        for cid, version, level, status, title, artist, creator, heat, last_updated, crawl_time in results:
            status_name = {0: "Alpha", 1: "Beta", 2: "Stable"}.get(status, "Unknown")
            days_ago = (datetime.now() - last_updated).days if last_updated else "未知"
            
            print(f"{colorize(title, Colors.BOLD)} - {artist}")
            print(f"  版本: {version}, 难度: Lv.{level}, 状态: {status_name}")
            print(f"  创作者: {creator}, 热度: {heat}")
            print(f"  最后更新: {last_updated} ({days_ago}天前), CID: {cid}")
            print()
    
    @db_safe_operation  
    def do_stb_hot(self, arg):
        """
        显示热门谱面排行榜
        
        用法: stb_hot [模式] [排序字段] [数量]
        参数:
          模式     - 可选，模式编号，默认为当前模式
          排序字段 - 可选，heat(热度), donate_count(打赏数), 默认为heat
          数量     - 可选，要显示的谱面数量，默认为10
        
        示例:
          stb_hot           # 当前模式按热度前10
          stb_hot 0         # 模式0按热度前10  
          stb_hot 0 donate_count 5   # 模式0按打赏数前5
        """
        args = arg.split()
        mode = self.current_mode
        sort_field = "heat"
        limit = 10
        
        if args:
            try:
                if args[0].isdigit():
                    mode = int(args[0])
                    if len(args) > 1:
                        sort_field = args[1].lower()
                        if len(args) > 2:
                            limit = int(args[2])
                else:
                    sort_field = args[0].lower()
                    if len(args) > 1 and args[1].isdigit():
                        mode = int(args[1])
                        if len(args) > 2:
                            limit = int(args[2])
            except ValueError:
                print(colorize("错误: 参数必须是数字", Colors.RED))
                return
        
        # 验证排序字段
        valid_fields = ["heat", "donate_count"]
        if sort_field not in valid_fields:
            print(colorize(f"错误: 排序字段必须是 {valid_fields} 之一", Colors.RED))
            return
        
        cursor = self.conn.cursor()
        
        cursor.execute(
            f"""
            SELECT c.cid, c.version, c.level, c.status, s.title, s.artist,
                   c.creator_name, c.heat, c.donate_count, c.last_updated
            FROM charts c
            JOIN songs s ON c.sid = s.sid
            WHERE c.mode = ?
            ORDER BY c.{sort_field} DESC
            LIMIT ?
            """,
            (mode, limit)
        )
        
        results = cursor.fetchall()
        
        if not results:
            print(colorize(f"\n模式 {mode} 没有谱面数据", Colors.YELLOW))
            return
        
        mode_name = self.mode_names.get(mode, "未知")
        field_name = "热度" if sort_field == "heat" else "打赏数"
        print(colorize(f"\n热门谱面排行榜 ({field_name}) - 模式 {mode} ({mode_name})", Colors.CYAN))
        print(get_separator())
        
        for i, (cid, version, level, status, title, artist, creator, heat, donate, updated) in enumerate(results, 1):
            status_name = {0: "Alpha", 1: "Beta", 2: "Stable"}.get(status, "Unknown")
            value = heat if sort_field == "heat" else donate
            
            print(f"{colorize(f'#{i}', Colors.YELLOW)} {colorize(title, Colors.BOLD)} - {artist}")
            print(f"  难度: Lv.{level}, 状态: {status_name}, 版本: {version}")
            print(f"  创作者: {creator}, {field_name}: {value}, CID: {cid}")
            print()
    
    @db_safe_operation
    def do_stb_summary(self, arg):
        """
        生成谱面综合统计报告
        
        用法: stb_summary [模式] [详细级别]
        参数:
          模式 - 可选，模式编号，默认为当前模式
          详细级别 - 可选，basic(基础), detailed(详细), 默认为basic
        
        示例:
          stb_summary           # 当前模式基础统计
          stb_summary 0 detailed # 模式0详细统计
        """
        args = arg.split()
        mode = self.current_mode
        detail_level = "basic"
        
        if args:
            try:
                if args[0].isdigit():
                    mode = int(args[0])
                    if len(args) > 1:
                        detail_level = args[1].lower()
                else:
                    detail_level = args[0].lower()
                    if len(args) > 1 and args[1].isdigit():
                        mode = int(args[1])
            except ValueError:
                print(colorize("错误: 模式必须是数字", Colors.RED))
                return
        
        cursor = self.conn.cursor()
        
        # 获取综合统计
        stats = self._get_comprehensive_stats(cursor, mode, detail_level)
        self._display_summary_report(stats, mode, detail_level)
        
        # 询问是否生成图表
        if detail_level == "detailed":
            chart_choice = input(colorize("\n是否生成统计图表? (y/N): ", Colors.CYAN)).lower()
            if chart_choice == 'y':
                self._generate_summary_charts(stats, mode)
    
    def _get_comprehensive_stats(self, cursor, mode, detail_level):
        """获取综合统计数据"""
        stats = {}
        
        # 基础统计
        cursor.execute("SELECT COUNT(*) FROM charts WHERE mode = ?", (mode,))
        stats['total_charts'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT sid) FROM charts WHERE mode = ?", (mode,))
        stats['unique_songs'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT creator_name) FROM charts WHERE mode = ? AND creator_name IS NOT NULL", (mode,))
        stats['unique_creators'] = cursor.fetchone()[0]
        
        # 时间统计
        cursor.execute("SELECT MIN(last_updated), MAX(last_updated) FROM charts WHERE mode = ? AND last_updated IS NOT NULL", (mode,))
        min_max_dates = cursor.fetchone()
        stats['first_update'] = min_max_dates[0]
        stats['last_update'] = min_max_dates[1]
        
        # 热度统计
        cursor.execute(
            "SELECT AVG(heat), MAX(heat), MIN(heat), STDDEV(heat) FROM charts WHERE mode = ? AND heat > 0",
            (mode,)
        )
        heat_stats = cursor.fetchone()
        stats['heat_stats'] = {
            'avg': heat_stats[0] or 0,
            'max': heat_stats[1] or 0,
            'min': heat_stats[2] or 0,
            'std': heat_stats[3] or 0
        }
        
        # 难度统计
        cursor.execute(
            "SELECT AVG(CAST(level AS REAL)), MAX(CAST(level AS REAL)), MIN(CAST(level AS REAL)) FROM charts WHERE mode = ? AND level IS NOT NULL",
            (mode,)
        )
        level_stats = cursor.fetchone()
        stats['level_stats'] = {
            'avg': level_stats[0] or 0,
            'max': level_stats[1] or 0,
            'min': level_stats[2] or 0
        }
        
        # 状态分布
        cursor.execute(
            "SELECT status, COUNT(*) FROM charts WHERE mode = ? GROUP BY status",
            (mode,)
        )
        stats['status_dist'] = dict(cursor.fetchall())
        
        if detail_level == "detailed":
            # 详细统计
            cursor.execute(
                "SELECT creator_name, COUNT(*) as count FROM charts WHERE mode = ? AND creator_name IS NOT NULL GROUP BY creator_name ORDER BY count DESC LIMIT 20",
                (mode,)
            )
            stats['top_creators'] = cursor.fetchall()
            
            cursor.execute(
                "SELECT level, COUNT(*) as count FROM charts WHERE mode = ? AND level IS NOT NULL GROUP BY level ORDER BY CAST(level AS REAL)",
                (mode,)
            )
            stats['level_breakdown'] = cursor.fetchall()
            
            # 热度分布
            cursor.execute(
                "SELECT COUNT(*) FROM charts WHERE mode = ? AND heat = 0",
                (mode,)
            )
            stats['zero_heat'] = cursor.fetchone()[0]
            
            cursor.execute(
                "SELECT COUNT(*) FROM charts WHERE mode = ? AND heat BETWEEN 1 AND 10",
                (mode,)
            )
            stats['low_heat'] = cursor.fetchone()[0]
            
            cursor.execute(
                "SELECT COUNT(*) FROM charts WHERE mode = ? AND heat BETWEEN 11 AND 50",
                (mode,)
            )
            stats['medium_heat'] = cursor.fetchone()[0]
            
            cursor.execute(
                "SELECT COUNT(*) FROM charts WHERE mode = ? AND heat > 50",
                (mode,)
            )
            stats['high_heat'] = cursor.fetchone()[0]
            
            # 更新频率统计
            cursor.execute(
                "SELECT strftime('%Y-%m', last_updated) as month, COUNT(*) FROM charts WHERE mode = ? AND last_updated IS NOT NULL GROUP BY month ORDER BY month DESC LIMIT 12",
                (mode,)
            )
            stats['monthly_updates'] = cursor.fetchall()
        
        return stats
    
    def _display_summary_report(self, stats, mode, detail_level):
        """显示综合统计报告"""
        mode_name = self.mode_names.get(mode, "未知")
        
        print(colorize(f"\n谱面综合统计报告 - 模式 {mode} ({mode_name})", Colors.CYAN))
        print(get_separator())
        
        # 基础概览
        print(colorize("\n📊 基础概览", Colors.BOLD))
        print(f"  总谱面数: {colorize(stats['total_charts'], Colors.GREEN)}")
        print(f"  唯一歌曲数: {stats['unique_songs']}")
        print(f"  创作者数: {stats['unique_creators']}")
        
        if stats['first_update'] and stats['last_update']:
            first_date = stats['first_update'].strftime('%Y-%m-%d') if hasattr(stats['first_update'], 'strftime') else stats['first_update']
            last_date = stats['last_update'].strftime('%Y-%m-%d') if hasattr(stats['last_update'], 'strftime') else stats['last_update']
            print(f"  数据时间范围: {first_date} 至 {last_date}")
        
        # 热度统计
        print(colorize("\n🔥 热度统计", Colors.BOLD))
        heat = stats['heat_stats']
        print(f"  平均热度: {heat['avg']:.1f}")
        print(f"  最高热度: {heat['max']}")
        print(f"  最低热度: {heat['min']}")
        if heat['std'] > 0:
            print(f"  热度标准差: {heat['std']:.1f}")
        
        # 难度统计
        if stats['level_stats']['avg'] > 0:
            print(colorize("\n🎯 难度统计", Colors.BOLD))
            level = stats['level_stats']
            print(f"  平均难度: Lv.{level['avg']:.1f}")
            print(f"  最高难度: Lv.{level['max']}")
            print(f"  最低难度: Lv.{level['min']}")
        
        # 状态分布
        print(colorize("\n📝 状态分布", Colors.BOLD))
        status_names = {0: "Alpha", 1: "Beta", 2: "Stable"}
        for status, count in stats['status_dist'].items():
            status_name = status_names.get(status, f"未知({status})")
            percentage = (count / stats['total_charts']) * 100
            print(f"  {status_name}: {count} ({percentage:.1f}%)")
        
        if detail_level == "detailed":
            # 详细统计
            print(colorize("\n👑 顶级创作者 (前20)", Colors.BOLD))
            for i, (creator, count) in enumerate(stats['top_creators'][:10], 1):
                percentage = (count / stats['total_charts']) * 100
                print(f"  {i:2d}. {creator}: {count} 谱面 ({percentage:.1f}%)")
            
            # 热度分布
            print(colorize("\n📈 热度分布", Colors.BOLD))
            total_with_heat = stats['total_charts'] - stats['zero_heat']
            print(f"  无热度: {stats['zero_heat']} ({stats['zero_heat']/stats['total_charts']*100:.1f}%)")
            print(f"  低热度 (1-10): {stats['low_heat']} ({stats['low_heat']/total_with_heat*100:.1f}%)")
            print(f"  中热度 (11-50): {stats['medium_heat']} ({stats['medium_heat']/total_with_heat*100:.1f}%)")
            print(f"  高热度 (50+): {stats['high_heat']} ({stats['high_heat']/total_with_heat*100:.1f}%)")
            
            # 月度更新
            if stats['monthly_updates']:
                print(colorize("\n📅 月度更新趋势 (最近12个月)", Colors.BOLD))
                for month, count in stats['monthly_updates']:
                    print(f"  {month}: {count} 个谱面")
    
    def _generate_summary_charts(self, stats, mode):
        """生成综合统计图表"""
        mode_name = self.mode_names.get(mode, "未知")
        
        # 创建多个子图
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle(f'谱面综合统计 - 模式 {mode} ({mode_name})', fontsize=16, fontweight='bold')
        
        # 1. 状态分布饼图
        status_names = {0: "Alpha", 1: "Beta", 2: "Stable"}
        status_labels = [status_names.get(s, f"未知({s})") for s in stats['status_dist'].keys()]
        status_sizes = list(stats['status_dist'].values())
        
        colors1 = ['#ff9999', '#66b3ff', '#99ff99']
        ax1.pie(status_sizes, labels=status_labels, autopct='%1.1f%%', colors=colors1, startangle=90)
        ax1.set_title('状态分布')
        
        # 2. 热度分布柱状图
        heat_categories = ['无热度', '低热度', '中热度', '高热度']
        heat_values = [stats['zero_heat'], stats['low_heat'], stats['medium_heat'], stats['high_heat']]
        colors2 = ['#cccccc', '#ffeb3b', '#ff9800', '#f44336']
        
        bars = ax2.bar(heat_categories, heat_values, color=colors2)
        ax2.set_title('热度分布')
        ax2.set_ylabel('谱面数量')
        
        # 添加数值标签
        for bar, value in zip(bars, heat_values):
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                    f'{value}', ha='center', va='bottom')
        
        # 3. 难度分布柱状图（如果数据存在）
        if hasattr(stats, 'level_breakdown') and stats['level_breakdown']:
            levels = [str(item[0]) for item in stats['level_breakdown']]
            counts = [item[1] for item in stats['level_breakdown']]
            
            ax3.bar(levels, counts, color='skyblue')
            ax3.set_title('难度分布')
            ax3.set_ylabel('谱面数量')
            ax3.tick_params(axis='x', rotation=45)
        else:
            ax3.text(0.5, 0.5, '无难度数据', ha='center', va='center', transform=ax3.transAxes)
            ax3.set_title('难度分布')
        
        # 4. 创作者排行榜（前10）
        if hasattr(stats, 'top_creators') and stats['top_creators']:
            creators = [item[0][:15] + '...' if len(item[0]) > 15 else item[0] for item in stats['top_creators'][:10]]
            creator_counts = [item[1] for item in stats['top_creators'][:10]]
            
            y_pos = range(len(creators))
            ax4.barh(y_pos, creator_counts, color='lightgreen')
            ax4.set_yticks(y_pos)
            ax4.set_yticklabels(creators)
            ax4.set_title('创作者排行榜 (前10)')
            ax4.set_xlabel('谱面数量')
        else:
            ax4.text(0.5, 0.5, '无创作者数据', ha='center', va='center', transform=ax4.transAxes)
            ax4.set_title('创作者排行榜')
        
        plt.tight_layout()
        
        # 保存图表
        base_filename = f"stb_summary_mode{mode}.png"
        filename = self.get_unique_filename(base_filename, "png")
        filepath = os.path.join(self.output_dir, filename)
        plt.savefig(filepath, dpi=150, facecolor='white', bbox_inches='tight')
        plt.close()
        
        print(colorize(f"\n已生成综合统计图表: {filepath}", Colors.GREEN))
    
    @db_safe_operation
    def do_stb_quality(self, arg):
        """
        检查数据质量
        
        用法: stb_quality [模式]
        参数:
          模式 - 可选，模式编号，默认为当前模式
        
        示例:
          stb_quality      # 当前模式数据质量检查
          stb_quality 0    # 模式0数据质量检查
        """
        args = arg.split()
        mode = self.current_mode
        if args:
            try:
                mode = int(args[0])
            except ValueError:
                print(colorize("错误: 模式必须是数字", Colors.RED))
                return
        
        cursor = self.conn.cursor()
        
        print(colorize(f"\n数据质量检查 - 模式 {mode}", Colors.CYAN))
        print(get_separator())
        
        # 检查数据完整性
        issues = []
        
        # 1. 检查缺失字段
        cursor.execute("SELECT COUNT(*) FROM charts WHERE mode = ? AND creator_name IS NULL", (mode,))
        missing_creator = cursor.fetchone()[0]
        if missing_creator > 0:
            issues.append(f"缺失创作者: {missing_creator} 个谱面")
        
        cursor.execute("SELECT COUNT(*) FROM charts WHERE mode = ? AND level IS NULL", (mode,))
        missing_level = cursor.fetchone()[0]
        if missing_level > 0:
            issues.append(f"缺失难度: {missing_level} 个谱面")
        
        cursor.execute("SELECT COUNT(*) FROM charts WHERE mode = ? AND last_updated IS NULL", (mode,))
        missing_update = cursor.fetchone()[0]
        if missing_update > 0:
            issues.append(f"缺失更新时间: {missing_update} 个谱面")
        
        # 2. 检查数据一致性
        cursor.execute("SELECT COUNT(*) FROM charts c LEFT JOIN songs s ON c.sid = s.sid WHERE c.mode = ? AND s.sid IS NULL", (mode,))
        orphan_charts = cursor.fetchone()[0]
        if orphan_charts > 0:
            issues.append(f"孤立的谱面记录: {orphan_charts} 个")
        
        # 3. 检查异常值
        cursor.execute("SELECT COUNT(*) FROM charts WHERE mode = ? AND heat < 0", (mode,))
        negative_heat = cursor.fetchone()[0]
        if negative_heat > 0:
            issues.append(f"负热度值: {negative_heat} 个谱面")
        
        cursor.execute("SELECT COUNT(*) FROM charts WHERE mode = ? AND donate_count < 0", (mode,))
        negative_donate = cursor.fetchone()[0]
        if negative_donate > 0:
            issues.append(f"负打赏数: {negative_donate} 个谱面")
        
        # 显示结果
        if issues:
            print(colorize("❌ 发现数据质量问题:", Colors.RED))
            for issue in issues:
                print(f"  • {issue}")
        else:
            print(colorize("✅ 数据质量良好，未发现问题", Colors.GREEN))
        
        # 显示数据完整性统计
        cursor.execute("SELECT COUNT(*) FROM charts WHERE mode = ?", (mode,))
        total_charts = cursor.fetchone()[0]
        
        completeness_stats = []
        if total_charts > 0:
            completeness_stats.append(f"总谱面数: {total_charts}")
            
            creator_completeness = ((total_charts - missing_creator) / total_charts) * 100
            completeness_stats.append(f"创作者完整性: {creator_completeness:.1f}%")
            
            level_completeness = ((total_charts - missing_level) / total_charts) * 100
            completeness_stats.append(f"难度完整性: {level_completeness:.1f}%")
            
            update_completeness = ((total_charts - missing_update) / total_charts) * 100
            completeness_stats.append(f"更新时间完整性: {update_completeness:.1f}%")
        
        print(colorize("\n📊 数据完整性统计:", Colors.BOLD))
        for stat in completeness_stats:
            print(f"  {stat}")
    
    @db_safe_operation
    def do_stb_trends(self, arg):
        """
        分析谱面数据趋势
        
        用法: stb_trends [模式] [时间段]
        参数:
          模式 - 可选，模式编号，默认为当前模式
          时间段 - 可选，days(天), months(月), 默认为months
        
        示例:
          stb_trends           # 当前模式月度趋势
          stb_trends 0 days    # 模式0每日趋势
        """
        args = arg.split()
        mode = self.current_mode
        period = "months"
        
        if args:
            try:
                if args[0].isdigit():
                    mode = int(args[0])
                    if len(args) > 1:
                        period = args[1].lower()
                else:
                    period = args[0].lower()
                    if len(args) > 1 and args[1].isdigit():
                        mode = int(args[1])
            except ValueError:
                print(colorize("错误: 模式必须是数字", Colors.RED))
                return
        
        cursor = self.conn.cursor()
        
        if period == "days":
            # 每日趋势（最近30天）
            cursor.execute(
                "SELECT DATE(last_updated), COUNT(*) FROM charts WHERE mode = ? AND last_updated >= date('now', '-30 days') GROUP BY DATE(last_updated) ORDER BY DATE(last_updated)",
                (mode,)
            )
            trend_data = cursor.fetchall()
            period_name = "每日"
            x_label = "日期"
        else:
            # 月度趋势（最近12个月）
            cursor.execute(
                "SELECT strftime('%Y-%m', last_updated), COUNT(*) FROM charts WHERE mode = ? AND last_updated >= date('now', '-1 year') GROUP BY strftime('%Y-%m', last_updated) ORDER BY strftime('%Y-%m', last_updated)",
                (mode,)
            )
            trend_data = cursor.fetchall()
            period_name = "月度"
            x_label = "月份"
        
        if not trend_data:
            print(colorize(f"模式 {mode} 没有找到趋势数据", Colors.YELLOW))
            return
        
        dates = [item[0] for item in trend_data]
        counts = [item[1] for item in trend_data]
        
        # 显示趋势统计
        mode_name = self.mode_names.get(mode, "未知")
        print(colorize(f"\n谱面{period_name}趋势 - 模式 {mode} ({mode_name})", Colors.CYAN))
        print(get_separator())
        
        total_updates = sum(counts)
        avg_updates = total_updates / len(counts) if counts else 0
        max_updates = max(counts) if counts else 0
        min_updates = min(counts) if counts else 0
        
        print(f"总更新谱面: {total_updates}")
        print(f"平均{period_name}更新: {avg_updates:.1f}")
        print(f"最高{period_name}更新: {max_updates}")
        print(f"最低{period_name}更新: {min_updates}")
        
        # 显示趋势数据
        print(colorize(f"\n{period_name}详细数据:", Colors.BOLD))
        for date, count in trend_data[-10:]:  # 显示最近10个周期
            print(f"  {date}: {count} 个谱面")
        
        # 生成趋势图表
        fig, ax = plt.subplots(figsize=(12, 6))
        
        # 创建趋势线
        ax.plot(dates, counts, 'o-', linewidth=2, markersize=6, color='#2196F3')
        ax.fill_between(dates, counts, alpha=0.3, color='#2196F3')
        
        # 添加平均线
        ax.axhline(y=avg_updates, color='red', linestyle='--', alpha=0.7, label=f'平均值: {avg_updates:.1f}')
        
        ax.set_title(f'谱面{period_name}更新趋势 - 模式 {mode} ({mode_name})')
        ax.set_xlabel(x_label)
        ax.set_ylabel('更新谱面数量')
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        # 旋转x轴标签
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        # 保存图表
        base_filename = f"stb_trends_{period}_mode{mode}.png"
        filename = self.get_unique_filename(base_filename, "png")
        filepath = os.path.join(self.output_dir, filename)
        plt.savefig(filepath, dpi=150, facecolor='white')
        plt.close()
        
        print(colorize(f"\n已生成趋势图表: {filepath}", Colors.GREEN))
    
    @db_safe_operation
    def do_stb_compare(self, arg):
        """
        比较不同模式的谱面数据
        
        用法: stb_compare [模式列表]
        参数:
          模式列表 - 可选，要比较的模式编号，用逗号分隔，默认为所有模式
        
        示例:
          stb_compare           # 比较所有模式
          stb_compare 0,3,5     # 比较模式0,3,5
        """
        if arg:
            try:
                modes = [int(m.strip()) for m in arg.split(',')]
                # 验证模式有效性
                for mode in modes:
                    if mode not in self.mode_names:
                        print(colorize(f"错误: 模式 {mode} 不存在", Colors.RED))
                        return
            except ValueError:
                print(colorize("错误: 模式必须是数字", Colors.RED))
                return
        else:
            modes = list(self.mode_names.keys())
        
        cursor = self.conn.cursor()
        
        print(colorize(f"\n模式比较分析", Colors.CYAN))
        print(get_separator())
        
        comparison_data = []
        
        for mode in modes:
            cursor.execute("SELECT COUNT(*) FROM charts WHERE mode = ?", (mode,))
            total_charts = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(DISTINCT creator_name) FROM charts WHERE mode = ? AND creator_name IS NOT NULL", (mode,))
            unique_creators = cursor.fetchone()[0]
            
            cursor.execute("SELECT AVG(heat) FROM charts WHERE mode = ? AND heat > 0", (mode,))
            avg_heat = cursor.fetchone()[0] or 0
            
            cursor.execute("SELECT AVG(CAST(level AS REAL)) FROM charts WHERE mode = ? AND level IS NOT NULL", (mode,))
            avg_level = cursor.fetchone()[0] or 0
            
            cursor.execute("SELECT COUNT(*) FROM charts WHERE mode = ? AND status = 2", (mode,))
            stable_charts = cursor.fetchone()[0]
            
            mode_name = self.mode_names.get(mode, "未知")
            comparison_data.append({
                'mode': mode,
                'name': mode_name,
                'total_charts': total_charts,
                'unique_creators': unique_creators,
                'avg_heat': avg_heat,
                'avg_level': avg_level,
                'stable_charts': stable_charts,
                'stability_rate': (stable_charts / total_charts * 100) if total_charts > 0 else 0
            })
        
        # 按总谱面数排序
        comparison_data.sort(key=lambda x: x['total_charts'], reverse=True)
        
        # 显示比较表格
        header = f"{'模式':<10} {'模式名':<12} {'总谱面':<8} {'创作者':<8} {'平均热度':<10} {'平均难度':<10} {'稳定率':<8}"
        print(header)
        print(get_separator())
        
        for data in comparison_data:
            mode_str = f"{data['mode']} ({data['name']})"
            print(f"{mode_str:<10} {data['name']:<12} {data['total_charts']:<8} {data['unique_creators']:<8} "
                  f"{data['avg_heat']:<10.1f} {data['avg_level']:<10.1f} {data['stability_rate']:<8.1f}%")
        
        # 生成比较图表
        if len(modes) > 1:
            self._generate_comparison_chart(comparison_data)
    
    def _generate_comparison_chart(self, comparison_data):
        """生成模式比较图表"""
        modes = [f"{d['mode']}\n({d['name']})" for d in comparison_data]
        total_charts = [d['total_charts'] for d in comparison_data]
        unique_creators = [d['unique_creators'] for d in comparison_data]
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
        
        # 左侧：总谱面数比较
        bars1 = ax1.bar(modes, total_charts, color='lightblue', alpha=0.7)
        ax1.set_title('各模式总谱面数比较')
        ax1.set_ylabel('谱面数量')
        ax1.tick_params(axis='x', rotation=45)
        
        # 添加数值标签
        for bar in bars1:
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                    f'{int(height)}', ha='center', va='bottom')
        
        # 右侧：创作者数比较
        bars2 = ax2.bar(modes, unique_creators, color='lightgreen', alpha=0.7)
        ax2.set_title('各模式创作者数比较')
        ax2.set_ylabel('创作者数量')
        ax2.tick_params(axis='x', rotation=45)
        
        # 添加数值标签
        for bar in bars2:
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                    f'{int(height)}', ha='center', va='bottom')
        
        plt.tight_layout()
        
        # 保存图表
        base_filename = "stb_mode_comparison.png"
        filename = self.get_unique_filename(base_filename, "png")
        filepath = os.path.join(self.output_dir, filename)
        plt.savefig(filepath, dpi=150, facecolor='white')
        plt.close()
        
        print(colorize(f"\n已生成模式比较图表: {filepath}", Colors.GREEN))
    
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
            print(get_separator())
            
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