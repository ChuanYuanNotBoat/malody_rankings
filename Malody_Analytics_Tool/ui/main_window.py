# ui/main_window.py
import os
import logging
import pandas as pd
from openpyxl import load_workbook
from PyQt5.QtWidgets import (
    QMainWindow, QAction, QActionGroup, QMessageBox,
    QFileDialog, QStatusBar, QLabel, QVBoxLayout, QWidget, QApplication,
    QTabWidget, QDateEdit, QHBoxLayout, QPushButton, QTableWidget,
    QTableWidgetItem, QHeaderView, QComboBox, QProgressBar
)
from PyQt5.QtCore import QSettings, QEvent, QTranslator, Qt, QDateTime, QThread, pyqtSignal
from PyQt5.QtGui import QIcon
from core.analytics import analyze_mode_data, MODE_FILES, get_latest_sheet_data
from core.history_analyzer import get_player_history, get_all_players_growth
from widgets.chart_widget import ChartWidget
from widgets.history_chart import HistoryChartWidget

logger = logging.getLogger(__name__)


# ================= 后台线程类 =================
class ModeAnalysisThread(QThread):
  finished = pyqtSignal(int, dict)  # 模式, 分析结果
  error = pyqtSignal(int, str)      # 模式, 错误信息
  progress = pyqtSignal(int, int)   # 当前模式, 总模式数

  def __init__(self, mode, file_path):
    super().__init__()
    self.mode = mode
    self.file_path = file_path

  def run(self):
    try:
      if not os.path.exists(self.file_path):
        self.error.emit(self.mode, f"File not found: {self.file_path}")
        return

      df = get_latest_sheet_data(self.file_path)
      if df.empty:
        self.error.emit(self.mode, f"No valid data found in {self.file_path}")
        return

      mode_results = analyze_mode_data(df, self.mode)
      self.finished.emit(self.mode, mode_results)
    except Exception as e:
      self.error.emit(self.mode, str(e))


class HistoryThread(QThread):
  finished = pyqtSignal(str, list)  # 玩家名称, 历史数据
  error = pyqtSignal(str)

  def __init__(self, folder_path, player_name, start_date, end_date, max_points=500):
    super().__init__()
    self.folder_path = folder_path
    self.player_name = player_name
    self.start_date = start_date
    self.end_date = end_date
    self.max_points = max_points

  def run(self):
    try:
      history = get_player_history(self.folder_path, self.player_name)

      # 过滤日期范围
      filtered_history = [
        h for h in history
        if h['date'].date() >= self.start_date and h['date'].date() <= self.end_date
      ]

      # 如果数据点太多，进行采样
      if len(filtered_history) > self.max_points:
        step = max(1, len(filtered_history) // self.max_points)
        filtered_history = filtered_history[::step]

      self.finished.emit(self.player_name, filtered_history)
    except Exception as e:
      self.error.emit(str(e))


class GrowthThread(QThread):
  finished = pyqtSignal(dict)  # 成长数据
  error = pyqtSignal(str)

  def __init__(self, folder_path, start_date, end_date):
    super().__init__()
    self.folder_path = folder_path
    self.start_date = start_date
    self.end_date = end_date

  def run(self):
    try:
      growth_data = get_all_players_growth(self.folder_path, self.start_date, self.end_date)
      self.finished.emit(growth_data)
    except Exception as e:
      self.error.emit(str(e))


# ================= 主窗口类 =================
class MainWindow(QMainWindow):
  def __init__(self, translator):
    super().__init__()
    self.translator = translator
    self.settings = QSettings("MalodyAnalytics", "MalodyAnalyticsTool")
    self.folder_path = ""
    self.player_history = []
    self.player_growth_data = {}
    self.analysis_threads = {}  # 存储分析线程
    self.init_ui()
    self.load_language()

  def init_ui(self):
    # 设置窗口标题和图标
    self.setWindowTitle(self.tr("Malody Analytics Tool"))
    self.setWindowIcon(QIcon(":/icons/app_icon.png"))
    self.setGeometry(100, 100, 1200, 800)
    self.chart_widget.mode_combo.currentIndexChanged.connect(self.on_mode_selected)
    # 创建菜单栏
    self.create_menus()

    # 创建状态栏
    self.status_bar = QStatusBar()
    self.setStatusBar(self.status_bar)
    self.status_label = QLabel(self.tr("Ready"))
    self.status_bar.addWidget(self.status_label)

    # 添加进度条
    self.progress_bar = QProgressBar()
    self.progress_bar.setMaximum(100)
    self.progress_bar.setVisible(False)
    self.status_bar.addPermanentWidget(self.progress_bar)

    # 创建主内容区域
    self.main_widget = QTabWidget()
    self.setCentralWidget(self.main_widget)

  def on_mode_selected(self):
    """当用户选择模式时触发"""
    mode = self.chart_widget.mode_combo.currentData()

    # "All Modes" 不需要单独分析
    if mode is None:
      self.chart_widget.on_mode_changed()
      return

    # 如果数据已存在，直接显示
    if mode in self.chart_widget.mode_data:
      self.chart_widget.on_mode_changed()
      return

    # 启动分析线程
    self.analyze_mode(mode)

  def analyze_mode(self, mode):
    """分析指定模式的数据"""
    if not self.folder_path:
      return

    file_path = os.path.join(self.folder_path, MODE_FILES.get(mode, f"mode{mode}.xlsx"))

    # 如果已有分析线程在运行，跳过
    if mode in self.analysis_threads:
      return

    self.status_label.setText(self.tr("Analyzing mode {}...").format(mode))

    # 创建并启动分析线程
    thread = ModeAnalysisThread(mode, file_path)
    thread.finished.connect(self.on_mode_analysis_finished)
    thread.error.connect(self.on_mode_analysis_error)
    thread.start()

    self.analysis_threads[mode] = thread

  def on_mode_analysis_finished(self, mode, data):
    """模式分析完成"""
    # 从线程字典中移除
    if mode in self.analysis_threads:
      del self.analysis_threads[mode]

    # 更新图表数据
    self.chart_widget.update_mode_data(mode, data)
    self.status_label.setText(self.tr("Mode {} analysis completed").format(mode))

  def on_mode_analysis_error(self, mode, error_msg):
    """模式分析出错"""
    # 从线程字典中移除
    if mode in self.analysis_threads:
      del self.analysis_threads[mode]

    QMessageBox.critical(self, self.tr("Error"),
                         self.tr("Failed to analyze mode {}: {}").format(mode, error_msg))
    self.status_label.setText(self.tr("Ready"))

    # 添加成长表格标题翻译
    self.growth_table.setHorizontalHeaderLabels([
      self.tr("Player"),
      self.tr("Rank Change"),
      self.tr("Level Change"),
      self.tr("EXP Change"),
      self.tr("Play Count Change"),
      self.tr("Daily EXP Growth"),
      self.tr("Period"),
      self.tr("Days")
    ])
    self.refresh_btn.setText(self.tr("Refresh History"))
    self.refresh_growth_btn.setText(self.tr("Refresh Growth Data"))
    self.sort_combo.setItemText(0, self.tr("Rank Change (Best First)"))
    self.sort_combo.setItemText(1, self.tr("Level Change (Best First)"))
    self.sort_combo.setItemText(2, self.tr("Experience Change (Best First)"))
    self.sort_combo.setItemText(3, self.tr("Play Count Change (Best First)"))
    self.sort_combo.setItemText(4, self.tr("Daily EXP Growth (Best First)"))
    self.sort_combo.setItemText(5, self.tr("Player Name (A-Z)"))

    # 更新菜单标题
    file_menu = self.menuBar().actions()[0].menu()
    file_menu.setTitle(self.tr("&File"))

    lang_menu = self.menuBar().actions()[1].menu()
    lang_menu.setTitle(self.tr("&Language"))

    help_menu = self.menuBar().actions()[2].menu()
    help_menu.setTitle(self.tr("&Help"))

  def open_folder(self):
    """打开文件夹对话框"""
    folder_path = QFileDialog.getExistingDirectory(
      self,
      self.tr("Select Malody Data Folder"),
      ""
    )

    if folder_path:
      self.folder_path = folder_path
      self.status_label.setText(self.tr("Folder selected"))

      # 清除之前的分析数据
      self.chart_widget.mode_data.clear()
      self.chart_widget.show_empty_chart()

      # 加载玩家列表
      self.load_player_list()

      # 加载成长数据
      self.refresh_growth_data()

  def on_analysis_error(self, error_msg):
    """分析出错的处理"""
    self.open_action.setEnabled(True)
    self.progress_bar.setVisible(False)
    QMessageBox.critical(self, self.tr("Error"), self.tr("Failed to process folder: {}").format(error_msg))
    self.status_label.setText(self.tr("Ready"))

  def save_report(self):
    """保存分析报告"""
    QMessageBox.information(self, self.tr("Save"), self.tr("Report saving functionality will be implemented soon."))

  def show_about(self):
    """显示关于对话框"""
    about_text = self.tr(
      "<h2>Malody Analytics Tool</h2>"
      "<p>Version 1.3.0</p>"
      "<p>A tool for analyzing Malody rhythm game ranking data.</p>"
      "<p>© 2023 Malody Analytics Team</p>"
    )
    QMessageBox.about(self, self.tr("About"), about_text)

  def changeEvent(self, event):
    """处理语言更改事件"""
    if event.type() == QEvent.LanguageChange:
      logger.debug("LanguageChange event received")
      self.retranslate_ui()
    super().changeEvent(event)

  def load_player_list(self):
    """加载玩家列表"""
    if not self.folder_path:
      return

    # 获取所有玩家
    all_players = set()
    for mode in MODE_FILES.keys():
      file_path = os.path.join(self.folder_path, MODE_FILES[mode])
      if not os.path.exists(file_path):
        continue

      try:
        wb = load_workbook(file_path)
        for sheet_name in wb.sheetnames:
          if not sheet_name.startswith(f"mode_{mode}_"):
            continue

          df = pd.read_excel(file_path, sheet_name=sheet_name)
          all_players.update(df['name'].unique())
      except Exception as e:
        logger.error(f"Error loading players from {file_path}: {str(e)}")
        continue

    # 更新玩家下拉框
    self.player_combo.clear()
    self.player_combo.addItems(sorted(all_players))
    self.history_chart.set_player_list(sorted(all_players))

    if all_players:
      self.player_combo.setCurrentIndex(0)

  def on_player_changed(self, index):
    """玩家选择改变时触发"""
    if index >= 0 and self.folder_path:
      player_name = self.player_combo.currentText()
      self.refresh_player_history()

  def refresh_player_history(self):
    """刷新玩家历史数据"""
    if not self.folder_path or self.player_combo.count() == 0:
      return

    player_name = self.player_combo.currentText()
    start_date = self.start_date_edit.date().toPyDate()
    end_date = self.end_date_edit.date().toPyDate()

    self.status_label.setText(self.tr("Loading history for {}...").format(player_name))
    self.refresh_btn.setEnabled(False)

    # 创建并启动历史数据线程
    self.history_thread = HistoryThread(
      self.folder_path,
      player_name,
      start_date,
      end_date,
      max_points=500  # 限制最多500个数据点
    )
    self.history_thread.finished.connect(self.on_history_finished)
    self.history_thread.error.connect(self.on_history_error)
    self.history_thread.start()

  def on_history_finished(self, player_name, history):
    self.refresh_btn.setEnabled(True)
    self.history_chart.set_history_data(player_name, history)
    self.status_label.setText(self.tr("History loaded for {}").format(player_name))

  def on_history_error(self, error_msg):
    self.refresh_btn.setEnabled(True)
    QMessageBox.critical(self, self.tr("Error"), self.tr("Failed to load history: {}").format(error_msg))
    self.status_label.setText(self.tr("Ready"))

  def refresh_growth_data(self):
    """刷新玩家成长数据"""
    if not self.folder_path:
      return

    start_date = self.start_date_edit.date().toPyDate()
    end_date = self.end_date_edit.date().toPyDate()

    self.status_label.setText(self.tr("Calculating player growth..."))
    self.refresh_growth_btn.setEnabled(False)

    # 创建并启动成长数据线程
    self.growth_thread = GrowthThread(self.folder_path, start_date, end_date)
    self.growth_thread.finished.connect(self.on_growth_finished)
    self.growth_thread.error.connect(self.on_growth_error)
    self.growth_thread.start()

  def on_growth_finished(self, growth_data):
    self.refresh_growth_btn.setEnabled(True)
    self.player_growth_data = growth_data
    self.update_growth_table()
    self.status_label.setText(self.tr("Growth data updated"))

  def on_growth_error(self, error_msg):
    self.refresh_growth_btn.setEnabled(True)
    QMessageBox.critical(self, self.tr("Error"), self.tr("Failed to calculate growth: {}").format(error_msg))
    self.status_label.setText(self.tr("Ready"))

  def update_growth_table(self):
    """更新成长数据表格"""
    if not self.player_growth_data:
      self.growth_table.setRowCount(0)
      return

    # 获取排序方式
    sort_field = self.sort_combo.currentData()

    # 准备数据
    data = []
    for player, growth in self.player_growth_data.items():
      # 跳过没有有效数据的变化
      if growth.get('rank_change') is None and growth.get('lv_change') is None:
        continue

      data.append({
        'player': player,
        'rank_change': growth.get('rank_change', 0),
        'lv_change': growth.get('lv_change', 0),
        'exp_change': growth.get('exp_change', 0),
        'pc_change': growth.get('pc_change', 0),
        'daily_exp_growth': growth.get('daily_exp_growth', 0),
        'period': f"{growth['start_date'].strftime('%Y-%m-%d')} to {growth['end_date'].strftime('%Y-%m-%d')}",
        'days': growth.get('days', 0)
      })

    # 排序数据
    if sort_field == "name":
      data.sort(key=lambda x: x['player'])
    else:
      # 对于数值字段，降序排列（最佳变化在前）
      data.sort(key=lambda x: x.get(sort_field, 0) or 0, reverse=True)

    # 更新表格
    self.growth_table.setRowCount(len(data))

    for row, item in enumerate(data):
      self.growth_table.setItem(row, 0, QTableWidgetItem(item['player']))
      self.growth_table.setItem(row, 1, QTableWidgetItem(str(item['rank_change'])))
      self.growth_table.setItem(row, 2, QTableWidgetItem(str(item['lv_change'])))
      self.growth_table.setItem(row, 3, QTableWidgetItem(str(item['exp_change'])))
      self.growth_table.setItem(row, 4, QTableWidgetItem(str(item['pc_change'])))
      self.growth_table.setItem(row, 5, QTableWidgetItem(str(item['daily_exp_growth'])))
      self.growth_table.setItem(row, 6, QTableWidgetItem(item['period']))
      self.growth_table.setItem(row, 7, QTableWidgetItem(str(item['days'])))
