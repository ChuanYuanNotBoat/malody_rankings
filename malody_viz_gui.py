# malody_viz_gui.py
import sys
import os
import tkinter as tk
from tkinter import ttk, messagebox, filedialog, simpledialog
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
from matplotlib.figure import Figure
import matplotlib.dates as mdates
from datetime import datetime, timedelta
import sqlite3
import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Tuple
import threading
import queue
import textwrap
import webbrowser
import re
import math
from functools import wraps
import subprocess

# 导入原脚本的功能
from malody_stats import MalodyViz, Colors, colorize, db_safe_operation

class MalodyGUI:
    """Malody数据可视化GUI界面"""
    
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Malody排行榜数据可视化工具")
        self.root.geometry("1200x800")
        self.root.minsize(1000, 700)
        
        # 检测GUI支持
        self.gui_supported = self._check_gui_support()
        
        if not self.gui_supported:
            self._show_fallback_message()
            return
        
        # 初始化核心组件
        self.viz = MalodyViz()
        self.current_figure = None
        self.canvas = None
        self.toolbar = None
        
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
        
        # 创建GUI组件
        self._setup_gui()
        
        # 状态变量
        self.processing = False
        self.message_queue = queue.Queue()
        self._process_messages()
    
    def _check_gui_support(self) -> bool:
        """检查当前环境是否支持GUI"""
        try:
            # 测试Tkinter是否正常工作
            test = tk.Tk()
            test.withdraw()  # 隐藏测试窗口
            test.destroy()
            return True
        except:
            return False
    
    def _show_fallback_message(self):
        """显示回退消息并启动命令行版本"""
        messagebox.showwarning(
            "GUI不支持", 
            "当前环境不支持图形界面，将启动命令行版本。"
        )
        # 启动命令行版本
        self.viz = MalodyViz()
        self.viz.cmdloop()
        sys.exit(0)
    
    def _setup_gui(self):
        """设置GUI界面"""
        # 创建主框架
        main_frame = ttk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # 创建顶部控制面板
        self._create_control_panel(main_frame)
        
        # 创建图表显示区域
        self._create_chart_area(main_frame)
        
        # 创建状态栏
        self._create_status_bar(main_frame)
        
        # 绑定窗口关闭事件
        self.root.protocol("WM_DELETE_WINDOW", self._on_closing)
    
    def _create_control_panel(self, parent):
        """创建控制面板"""
        control_frame = ttk.LabelFrame(parent, text="控制面板", padding=10)
        control_frame.pack(fill=tk.X, pady=(0, 10))
        
        # 第一行：模式选择和基本功能
        row1_frame = ttk.Frame(control_frame)
        row1_frame.pack(fill=tk.X, pady=5)
        
        # 模式选择
        ttk.Label(row1_frame, text="游戏模式:").pack(side=tk.LEFT)
        self.mode_var = tk.StringVar(value="0 - Key")
        mode_combo = ttk.Combobox(row1_frame, textvariable=self.mode_var, 
                                 values=[(f"{k} - {v}") for k, v in self.viz.mode_names.items()],
                                 state="readonly", width=15)
        mode_combo.pack(side=tk.LEFT, padx=5)
        mode_combo.bind('<<ComboboxSelected>>', self._on_mode_change)
        
        # 功能按钮
        ttk.Button(row1_frame, text="顶级玩家排名", 
                  command=self._show_top_players).pack(side=tk.LEFT, padx=5)
        ttk.Button(row1_frame, text="顶级玩家图表", 
                  command=self._show_top_chart).pack(side=tk.LEFT, padx=5)
        ttk.Button(row1_frame, text="更新数据", 
                  command=self._update_data).pack(side=tk.LEFT, padx=5)
        
        # 第二行：玩家查询
        row2_frame = ttk.Frame(control_frame)
        row2_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(row2_frame, text="玩家查询:").pack(side=tk.LEFT)
        self.player_var = tk.StringVar()
        player_entry = ttk.Entry(row2_frame, textvariable=self.player_var, width=20)
        player_entry.pack(side=tk.LEFT, padx=5)
        player_entry.bind('<Return>', lambda e: self._show_player_info())
        
        ttk.Button(row2_frame, text="查询信息", 
                  command=self._show_player_info).pack(side=tk.LEFT, padx=2)
        ttk.Button(row2_frame, text="历史图表", 
                  command=self._show_player_history).pack(side=tk.LEFT, padx=2)
        
        # 第三行：多玩家比较
        row3_frame = ttk.Frame(control_frame)
        row3_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(row3_frame, text="多玩家比较:").pack(side=tk.LEFT)
        self.compare_var = tk.StringVar()
        compare_entry = ttk.Entry(row3_frame, textvariable=self.compare_var, width=40)
        compare_entry.pack(side=tk.LEFT, padx=5)
        compare_entry.bind('<Return>', lambda e: self._compare_players())
        
        ttk.Button(row3_frame, text="比较图表", 
                  command=self._compare_players).pack(side=tk.LEFT, padx=2)
        
        # 第四行：参数设置和其他功能
        row4_frame = ttk.Frame(control_frame)
        row4_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(row4_frame, text="历史天数:").pack(side=tk.LEFT)
        self.days_var = tk.StringVar(value="30")
        days_spinbox = ttk.Spinbox(row4_frame, from_=1, to=365, textvariable=self.days_var, width=8)
        days_spinbox.pack(side=tk.LEFT, padx=5)
        
        ttk.Label(row4_frame, text="显示数量:").pack(side=tk.LEFT, padx=(20, 0))
        self.limit_var = tk.StringVar(value="20")
        limit_spinbox = ttk.Spinbox(row4_frame, from_=1, to=100, textvariable=self.limit_var, width=8)
        limit_spinbox.pack(side=tk.LEFT, padx=5)
        
        ttk.Button(row4_frame, text="导出数据", 
                  command=self._export_data).pack(side=tk.LEFT, padx=10)
        ttk.Button(row4_frame, text="设置别名", 
                  command=self._set_alias).pack(side=tk.LEFT, padx=2)
        ttk.Button(row4_frame, text="查看输出目录", 
                  command=self._open_output_dir).pack(side=tk.LEFT, padx=2)
    
    def _create_chart_area(self, parent):
        """创建图表显示区域"""
        chart_frame = ttk.LabelFrame(parent, text="图表显示", padding=5)
        chart_frame.pack(fill=tk.BOTH, expand=True)
        
        # 创建matplotlib图形
        self.figure = Figure(figsize=(10, 6), dpi=100)
        self.canvas = FigureCanvasTkAgg(self.figure, chart_frame)
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        
        # 添加工具栏
        self.toolbar = NavigationToolbar2Tk(self.canvas, chart_frame)
        self.toolbar.update()
        
        # 初始显示说明文本
        self._show_welcome_text()
    
    def _create_status_bar(self, parent):
        """创建状态栏"""
        self.status_var = tk.StringVar(value="就绪")
        status_bar = ttk.Label(parent, textvariable=self.status_var, relief=tk.SUNKEN, anchor=tk.W)
        status_bar.pack(fill=tk.X, pady=(10, 0))
    
    def _show_welcome_text(self):
        """显示欢迎文本"""
        self.figure.clear()
        ax = self.figure.add_subplot(111)
        
        # 使用安全的文本显示，避免字体问题
        welcome_text = (
            "Malody排行榜数据可视化工具\n\n"
            "使用说明:\n"
            "1. 选择游戏模式\n"
            "2. 输入玩家名称进行查询\n"
            "3. 点击相应按钮生成图表\n"
            "4. 使用工具栏进行图表操作\n\n"
            "支持的功能:\n"
            "- 玩家信息查询\n"
            "- 历史排名图表\n"
            "- 多玩家比较\n"
            "- 顶级玩家分布\n"
            "- 数据导出"
        )
        
        ax.text(0.5, 0.5, welcome_text,
               ha='center', va='center', fontsize=12, 
               transform=ax.transAxes, wrap=True)
        ax.set_axis_off()
        self.canvas.draw()
    
    def _update_status(self, message: str):
        """更新状态栏"""
        self.status_var.set(message)
        self.root.update_idletasks()
    
    def _show_message(self, title: str, message: str, is_error: bool = False):
        """显示消息对话框"""
        if is_error:
            messagebox.showerror(title, message)
        else:
            messagebox.showinfo(title, message)
    
    def _process_messages(self):
        """处理消息队列"""
        try:
            while True:
                message = self.message_queue.get_nowait()
                if message.startswith("STATUS:"):
                    self._update_status(message[7:])
                elif message.startswith("MESSAGE:"):
                    parts = message[8:].split("|", 1)
                    if len(parts) == 2:
                        self._show_message(parts[0], parts[1])
                elif message.startswith("ERROR:"):
                    parts = message[6:].split("|", 1)
                    if len(parts) == 2:
                        self._show_message(parts[0], parts[1], True)
        except queue.Empty:
            pass
        finally:
            self.root.after(100, self._process_messages)
    
    def _thread_safe_draw_figure(self, fig):
        """线程安全地绘制图表"""
        def wrapper():
            try:
                self.figure.clear()
                
                # 复制原图的所有子图
                for i, ax_src in enumerate(fig.axes):
                    if i == 0:
                        ax_dest = self.figure.add_subplot(111)
                    else:
                        # 对于多子图的情况，需要更复杂的处理
                        # 这里简化处理，只复制第一个子图
                        break
                    
                    # 复制线条
                    for line in ax_src.get_lines():
                        xdata = line.get_xdata()
                        ydata = line.get_ydata()
                        ax_dest.plot(xdata, ydata, 
                                   color=line.get_color(), 
                                   linestyle=line.get_linestyle(),
                                   linewidth=line.get_linewidth(),
                                   marker=line.get_marker(),
                                   markersize=line.get_markersize(),
                                   label=line.get_label())
                    
                    # 复制标题和标签
                    ax_dest.set_title(ax_src.get_title(), color='black')
                    ax_dest.set_xlabel(ax_src.get_xlabel(), color='black')
                    ax_dest.set_ylabel(ax_src.get_ylabel(), color='black')
                    
                    # 修复：使用正确的方法检查网格状态
                    # 检查网格是否启用（通过检查是否有网格线或使用grid()方法）
                    grid_lines = ax_src.get_xgridlines() + ax_src.get_ygridlines()
                    if grid_lines and any(line.get_visible() for line in grid_lines):
                        ax_dest.grid(True, alpha=0.3)
                    else:
                        ax_dest.grid(False)
                    
                    # 复制y轴反转
                    if ax_src.yaxis_inverted():
                        ax_dest.invert_yaxis()
                    
                    # 复制图例
                    if ax_src.get_legend() is not None:
                        ax_dest.legend()
                    
                    # 设置刻度颜色
                    ax_dest.tick_params(colors='black')
                    
                    # 处理日期格式
                    if len(xdata) > 0 and isinstance(xdata[0], (datetime, np.datetime64)):
                        ax_dest.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
                        ax_dest.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
                        self.figure.autofmt_xdate()
                
                self.canvas.draw()
                self._update_status("图表生成完成")
                
            except Exception as e:
                self.message_queue.put(f"ERROR:错误|绘制图表时出错: {str(e)}")
            finally:
                self.processing = False
                plt.close(fig)  # 关闭原图释放内存
        
        return wrapper
    
    def _on_mode_change(self, event=None):
        """模式改变事件"""
        try:
            mode_str = self.mode_var.get().split(" - ")[0]
            mode = int(mode_str)
            self.viz.current_mode = mode
            mode_name = self.viz.mode_names.get(mode, "未知")
            self._update_status(f"已切换到模式: {mode} - {mode_name}")
        except Exception as e:
            self.message_queue.put(f"ERROR:错误|模式切换失败: {str(e)}")
    
    def _get_player_id(self, player_name):
        """获取玩家ID"""
        cursor = self.viz.conn.cursor()
        cursor.execute(
            "SELECT player_id FROM player_aliases WHERE alias = ?",
            (player_name,)
        )
        result = cursor.fetchone()
        return result[0] if result else None
    
    def _show_player_info(self):
        """显示玩家信息"""
        player_name = self.player_var.get().strip()
        if not player_name:
            self._show_message("错误", "请输入玩家名称")
            return
        
        if self.processing:
            return
        
        self.processing = True
        self._update_status(f"查询玩家信息: {player_name}")
        
        def get_player_info():
            try:
                # 复用命令行版本的代码
                result = self.viz.do_player(f"{player_name} {self.viz.current_mode}")
                if result is False:  # 表示出错
                    return f"ERROR:错误|查询玩家信息失败"
                return f"MESSAGE:玩家信息|查询完成，请查看控制台输出"
                
            except Exception as e:
                return f"ERROR:错误|查询玩家信息时出错: {str(e)}"
        
        def on_complete():
            result = get_player_info()
            self.message_queue.put(result)
            self.processing = False
        
        threading.Thread(target=on_complete, daemon=True).start()
    
    def _plot_player_history(self, player_name, mode, days):
        """绘制玩家历史图表 - 复用命令行版本代码"""
        cursor = self.viz.conn.cursor()
        
        player_id = self._get_player_id(player_name)
        if not player_id:
            return None, f"未找到玩家: {player_name}"
        
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
            mode_name = self.viz.mode_names.get(mode, "未知")
            return None, f"玩家 {player_name} 在模式 {mode} ({mode_name}) 中最近 {days} 天没有数据"
        
        dates = [row[1] for row in history_data]
        ranks = [row[0] for row in history_data]
        
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(dates, ranks, 'o-', linewidth=2, markersize=4)
        ax.invert_yaxis()
        mode_name = self.viz.mode_names.get(mode, "未知")
        
        # 使用命令行版本的样式
        ax.set_title(f"Player {player_name} Ranking History (Mode {mode} - {mode_name})", color='black')
        ax.set_xlabel("Date", color='black')
        ax.set_ylabel("Rank", color='black')
        ax.grid(True, alpha=0.3)
        ax.tick_params(colors='black')
        
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
        fig.autofmt_xdate()
        plt.tight_layout()
        
        return fig, None
    
    def _show_player_history(self):
        """显示玩家历史图表"""
        player_name = self.player_var.get().strip()
        if not player_name:
            self._show_message("错误", "请输入玩家名称")
            return
        
        if self.processing:
            return
        
        self.processing = True
        mode = self.viz.current_mode
        
        try:
            days = int(self.days_var.get())
        except:
            days = 30
        
        self._update_status(f"生成 {player_name} 的历史排名图表...")
        
        def generate_history():
            try:
                fig, error = self._plot_player_history(player_name, mode, days)
                if error:
                    return f"ERROR:错误|{error}"
                
                self.root.after(0, self._thread_safe_draw_figure(fig))
                return None
                
            except Exception as e:
                return f"ERROR:错误|生成历史图表时出错: {str(e)}"
        
        def on_complete():
            result = generate_history()
            if result:
                self.message_queue.put(result)
        
        threading.Thread(target=on_complete, daemon=True).start()
    
    def _plot_players_comparison(self, players, mode, days):
        """绘制多玩家比较图表 - 复用命令行版本代码"""
        cursor = self.viz.conn.cursor()
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        fig, ax = plt.subplots(figsize=(12, 8))
        colors = plt.cm.Set3(np.linspace(0, 1, len(players)))
        found_any = False
        
        for idx, player_name in enumerate(players):
            player_id = self._get_player_id(player_name)
            if not player_id:
                continue
            
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
                continue
            
            dates = [row[1] for row in history_data]
            ranks = [row[0] for row in history_data]
            
            ax.plot(dates, ranks, 'o-', linewidth=2, markersize=4, 
                   color=colors[idx], label=player_name)
            found_any = True
        
        if not found_any:
            return None, "没有找到任何玩家的数据"
        
        ax.invert_yaxis()
        mode_name = self.viz.mode_names.get(mode, "未知")
        
        # 使用命令行版本的样式
        ax.set_title(f"Player Ranking Comparison (Mode {mode} - {mode_name})", color='black')
        ax.set_xlabel("Date", color='black')
        ax.set_ylabel("Rank", color='black')
        ax.grid(True, alpha=0.3)
        ax.legend()
        ax.tick_params(colors='black')
        
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
        fig.autofmt_xdate()
        plt.tight_layout()
        
        return fig, None
    
    def _compare_players(self):
        """比较多个玩家"""
        players_text = self.compare_var.get().strip()
        if not players_text:
            self._show_message("错误", "请输入要比较的玩家名称（用空格分隔）")
            return
        
        players = players_text.split()
        if len(players) < 2:
            self._show_message("错误", "请至少输入两个玩家名称")
            return
        
        if self.processing:
            return
        
        self.processing = True
        mode = self.viz.current_mode
        
        try:
            days = int(self.days_var.get())
        except:
            days = 30
        
        self._update_status(f"比较玩家: {', '.join(players)}")
        
        def generate_comparison():
            try:
                fig, error = self._plot_players_comparison(players, mode, days)
                if error:
                    return f"ERROR:错误|{error}"
                
                self.root.after(0, self._thread_safe_draw_figure(fig))
                return None
                
            except Exception as e:
                return f"ERROR:错误|生成比较图表时出错: {str(e)}"
        
        def on_complete():
            result = generate_comparison()
            if result:
                self.message_queue.put(result)
        
        threading.Thread(target=on_complete, daemon=True).start()
    
    def _plot_top_players(self, mode, limit):
        """绘制顶级玩家分布图表 - 复用命令行版本代码"""
        cursor = self.viz.conn.cursor()
        
        cursor.execute(
            "SELECT MAX(crawl_time) FROM player_rankings WHERE mode = ?",
            (mode,)
        )
        latest_time = cursor.fetchone()[0]
        
        if not latest_time:
            mode_name = self.viz.mode_names.get(mode, "未知")
            return None, f"模式 {mode} ({mode_name}) 没有数据"
        
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
            mode_name = self.viz.mode_names.get(mode, "未知")
            return None, f"模式 {mode} ({mode_name}) 没有找到玩家数据"
        
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
        mode_name = self.viz.mode_names.get(mode, "未知")
        
        # 使用命令行版本的样式
        ax1.set_title(f"Mode {mode} ({mode_name}) Top {limit} Players Accuracy Difference", color='black')
        ax1.set_xlabel("Rank", color='black')
        ax1.set_ylabel("Accuracy Difference from Max (%)", color='black')
        ax1.set_xticks(range(len(players)))
        ax1.set_xticklabels(ranks, rotation=45)
        ax1.tick_params(colors='black')
        ax1.invert_yaxis()
        
        # 添加准确率标签
        for i, (bar, acc) in enumerate(zip(bars, accuracies)):
            ax1.text(bar.get_x() + bar.get_width()/2., bar.get_height() + 0.02,
                    f'{acc:.2f}%', ha='center', va='bottom', fontsize=8,
                    bbox=dict(boxstyle='round,pad=0.2', facecolor='white', alpha=0.7))
        
        # 经验值图表
        exp_bars = ax2.bar(range(len(players)), exps, color=plt.cm.plasma(np.linspace(0, 1, len(players))))
        ax2.set_title(f"Mode {mode} ({mode_name}) Top {limit} Players Experience", color='black')
        ax2.set_xlabel("Rank", color='black')
        ax2.set_ylabel("Experience", color='black')
        ax2.set_xticks(range(len(players)))
        ax2.set_xticklabels(ranks, rotation=45)
        ax2.set_yscale('log')
        ax2.tick_params(colors='black')
        
        # 添加经验值标签
        for i, (bar, exp) in enumerate(zip(exp_bars, exps)):
            ax2.text(bar.get_x() + bar.get_width()/2., bar.get_height(),
                    f'{exp:.0f}', ha='center', va='bottom', fontsize=8,
                    bbox=dict(boxstyle='round,pad=0.2', facecolor='white', alpha=0.7))
        
        # 添加玩家名字
        for i, name in enumerate(names):
            display_name = name if len(name) <= 12 else name[:10] + '...'
            ax1.text(i, -0.08 * max(acc_diffs), display_name, 
                    ha='right', va='top', rotation=60, fontsize=7, color='black')
            ax2.text(i, 0.1 * min(exps) if min(exps) > 0 else 1, display_name,
                    ha='right', va='bottom', rotation=60, fontsize=7, color='black')
        
        plt.tight_layout()
        return fig, None
    
    def _show_top_players(self):
        """显示顶级玩家排名"""
        if self.processing:
            return
        
        self.processing = True
        mode = self.viz.current_mode
        
        try:
            limit = int(self.limit_var.get())
        except:
            limit = 20
        
        self._update_status(f"查询模式 {mode} 的前 {limit} 名玩家")
        
        def get_top_players():
            try:
                # 复用命令行版本的代码
                result = self.viz.do_top(str(limit))
                if result is False:  # 表示出错
                    return f"ERROR:错误|查询顶级玩家失败"
                return f"MESSAGE:顶级玩家排名|查询完成，请查看控制台输出"
                
            except Exception as e:
                return f"ERROR:错误|查询顶级玩家时出错: {str(e)}"
        
        def on_complete():
            result = get_top_players()
            self.message_queue.put(result)
            self.processing = False
        
        threading.Thread(target=on_complete, daemon=True).start()
    
    def _show_top_chart(self):
        """显示顶级玩家分布图表"""
        if self.processing:
            return
        
        self.processing = True
        mode = self.viz.current_mode
        
        try:
            limit = int(self.limit_var.get())
        except:
            limit = 20
        
        self._update_status(f"生成模式 {mode} 的前 {limit} 名玩家分布图表")
        
        def generate_top_chart():
            try:
                fig, error = self._plot_top_players(mode, limit)
                if error:
                    return f"ERROR:错误|{error}"
                
                self.root.after(0, self._thread_safe_draw_figure(fig))
                return None
                
            except Exception as e:
                return f"ERROR:错误|生成顶级玩家图表时出错: {str(e)}"
        
        def on_complete():
            result = generate_top_chart()
            if result:
                self.message_queue.put(result)
        
        threading.Thread(target=on_complete, daemon=True).start()
    
    def _update_data(self):
        """更新数据"""
        if self.processing:
            return
        
        response = messagebox.askyesno("确认", "更新数据可能需要几分钟时间，是否继续？")
        if not response:
            return
        
        self.processing = True
        self._update_status("正在更新数据...")
        
        def update_thread():
            try:
                # 复用命令行版本的更新功能
                result = self.viz.do_update("")
                if result is False:  # 表示出错
                    self.message_queue.put("ERROR:错误|数据更新失败")
                else:
                    self.message_queue.put("MESSAGE:数据更新|数据更新完成")
            except Exception as e:
                self.message_queue.put(f"ERROR:错误|更新数据时出错: {str(e)}")
            finally:
                self.processing = False
        
        threading.Thread(target=update_thread, daemon=True).start()
    
    def _export_data(self):
        """导出数据"""
        file_path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV文件", "*.csv"), ("所有文件", "*.*")],
            title="导出数据"
        )
        
        if not file_path:
            return
        
        self._update_status("正在导出数据...")
        
        def export_thread():
            try:
                # 复用命令行版本的导出功能
                result = self.viz.do_export(f"top {self.viz.current_mode}")
                if result is False:  # 表示出错
                    self.message_queue.put("ERROR:错误|数据导出失败")
                else:
                    # 重命名文件到用户选择的位置
                    import shutil
                    base_name = os.path.basename(file_path)
                    viz_file = os.path.join(self.viz.output_dir, base_name)
                    if os.path.exists(viz_file):
                        shutil.move(viz_file, file_path)
                    self.message_queue.put(f"MESSAGE:导出成功|数据已成功导出到: {file_path}")
            except Exception as e:
                self.message_queue.put(f"ERROR:错误|导出数据时出错: {str(e)}")
        
        threading.Thread(target=export_thread, daemon=True).start()
    
    def _set_alias(self):
        """设置玩家别名"""
        original = simpledialog.askstring("设置别名", "请输入玩家原名:")
        if not original:
            return
        
        new_alias = simpledialog.askstring("设置别名", "请输入新别名:")
        if not new_alias:
            return
        
        def set_alias_thread():
            try:
                # 复用命令行版本的别名设置功能
                result = self.viz.do_alias(f'"{original}" "{new_alias}"')
                if result is False:  # 表示出错
                    self.message_queue.put("ERROR:错误|别名设置失败")
                else:
                    self.message_queue.put("MESSAGE:别名设置|别名设置成功")
            except Exception as e:
                self.message_queue.put(f"ERROR:错误|设置别名时出错: {str(e)}")
        
        threading.Thread(target=set_alias_thread, daemon=True).start()
    
    def _open_output_dir(self):
        """打开输出目录"""
        try:
            output_dir = os.path.abspath(self.viz.output_dir)
            if os.path.exists(output_dir):
                if sys.platform == "win32":
                    os.startfile(output_dir)
                elif sys.platform == "darwin":  # macOS
                    subprocess.run(["open", output_dir])
                else:  # Linux
                    subprocess.run(["xdg-open", output_dir])
                self.message_queue.put("MESSAGE:打开目录|已打开输出目录")
            else:
                self.message_queue.put("ERROR:错误|输出目录不存在")
        except Exception as e:
            self.message_queue.put(f"ERROR:错误|打开目录时出错: {str(e)}")
    
    def _on_closing(self):
        """关闭窗口事件"""
        if messagebox.askokcancel("退出", "确定要退出程序吗？"):
            self.viz.cleanup()
            self.root.destroy()
    
    def run(self):
        """运行GUI程序"""
        if not self.gui_supported:
            return
        
        # 居中显示窗口
        self.root.eval('tk::PlaceWindow . center')
        self.root.mainloop()

def main():
    """主函数"""
    # 尝试启动GUI版本
    try:
        app = MalodyGUI()
        app.run()
    except Exception as e:
        print(f"GUI启动失败: {e}")
        print("启动命令行版本...")
        # 回退到命令行版本
        viz = MalodyViz()
        viz.cmdloop()

if __name__ == "__main__":
    main()