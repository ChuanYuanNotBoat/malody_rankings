# ui/main_window.py
import os
from PyQt5.QtWidgets import (
  QMainWindow, QAction, QMenu, QMessageBox,
  QFileDialog, QStatusBar, QLabel, QVBoxLayout, QWidget
)
from PyQt5.QtCore import Qt, QSettings
from PyQt5.QtGui import QIcon
from utils.i18n import get_system_language
from core.analytics import analyze_malody_data
from widgets.chart_widget import ChartWidget


class MainWindow(QMainWindow):
  def __init__(self, translator):
    super().__init__()
    self.translator = translator
    self.settings = QSettings("MalodyAnalytics", "MalodyAnalyticsTool")
    self.init_ui()
    self.load_language()

  def init_ui(self):
    # 设置窗口标题和图标
    self.setWindowTitle(self.tr("Malody Analytics Tool"))
    self.setWindowIcon(QIcon(":/icons/app_icon.png"))
    self.setGeometry(100, 100, 1000, 700)

    # 创建菜单栏
    self.create_menus()

    # 创建状态栏
    self.status_bar = QStatusBar()
    self.setStatusBar(self.status_bar)
    self.status_label = QLabel(self.tr("Ready"))
    self.status_bar.addWidget(self.status_label)

    # 创建主内容区域
    self.main_widget = QWidget()
    self.setCentralWidget(self.main_widget)

    # 主布局
    self.main_layout = QVBoxLayout()
    self.main_widget.setLayout(self.main_layout)

    # 创建图表控件
    self.chart_widget = ChartWidget()
    self.main_layout.addWidget(self.chart_widget)

    # 加载样式
    self.load_styles()

  def create_menus(self):
    # 文件菜单
    file_menu = self.menuBar().addMenu(self.tr("&File"))

    self.open_action = QAction(QIcon(":/icons/open.png"), self.tr("&Open"), self)
    self.open_action.setShortcut("Ctrl+O")
    self.open_action.triggered.connect(self.open_file)
    file_menu.addAction(self.open_action)

    self.save_action = QAction(QIcon(":/icons/save.png"), self.tr("&Save"), self)
    self.save_action.setShortcut("Ctrl+S")
    self.save_action.triggered.connect(self.save_results)
    file_menu.addAction(self.save_action)

    file_menu.addSeparator()

    self.exit_action = QAction(self.tr("E&xit"), self)
    self.exit_action.setShortcut("Ctrl+Q")
    self.exit_action.triggered.connect(self.close)
    file_menu.addAction(self.exit_action)

    # 语言菜单
    lang_menu = self.menuBar().addMenu(self.tr("&Language"))

    self.chinese_action = QAction(self.tr("中文"), self)
    self.chinese_action.triggered.connect(lambda: self.set_language('zh_CN'))
    lang_menu.addAction(self.chinese_action)

    self.english_action = QAction("English", self)
    self.english_action.triggered.connect(lambda: self.set_language('en'))
    lang_menu.addAction(self.english_action)

    # 帮助菜单
    help_menu = self.menuBar().addMenu(self.tr("&Help"))

    self.about_action = QAction(self.tr("&About"), self)
    self.about_action.triggered.connect(self.show_about)
    help_menu.addAction(self.about_action)

  def load_styles(self):
    # 设置样式
    self.setStyleSheet("""
            QMainWindow {
                background-color: #f0f0f0;
            }
            QMenuBar {
                background-color: #e0e0e0;
                padding: 2px;
            }
            QStatusBar {
                background-color: #e0e0e0;
                padding: 2px;
            }
        """)

  def load_language(self):
    # 从设置中获取语言或根据系统语言选择
    lang = self.settings.value("language", "")
    if not lang:
      lang = get_system_language()
    self.set_language(lang, initial_load=True)

  def set_language(self, lang_code, initial_load=False):
    # 保存语言设置
    self.settings.setValue("language", lang_code)

    # 重新加载界面
    self.retranslate_ui()

    # 提示用户重启应用
    if not initial_load:
      QMessageBox.information(
        self,
        self.tr("Language Changed"),
        self.tr("The application needs to restart for the language change to take full effect.")
      )

  def retranslate_ui(self):
    # 更新所有UI文本
    self.setWindowTitle(self.tr("Malody Analytics Tool"))
    self.open_action.setText(self.tr("&Open"))
    self.save_action.setText(self.tr("&Save"))
    self.exit_action.setText(self.tr("E&xit"))
    self.chinese_action.setText(self.tr("中文"))
    self.english_action.setText(self.tr("English"))
    self.about_action.setText(self.tr("&About"))
    self.status_label.setText(self.tr("Ready"))

  def open_file(self):
    # 打开文件对话框
    file_path, _ = QFileDialog.getOpenFileName(
      self,
      self.tr("Open Malody Data File"),
      "",
      self.tr("Malody Files (*.mld *.mldx);;All Files (*)")
    )

    if file_path:
      self.status_label.setText(self.tr("Processing file: {}").format(os.path.basename(file_path)))
      try:
        # 分析数据
        results = analyze_malody_data(file_path)
        # 更新图表
        self.chart_widget.update_chart(results)
        self.status_label.setText(self.tr("Analysis completed"))
      except Exception as e:
        QMessageBox.critical(self, self.tr("Error"), self.tr("Failed to process file: {}").format(str(e)))
        self.status_label.setText(self.tr("Ready"))

  def save_results(self):
    # 保存结果逻辑
    QMessageBox.information(self, self.tr("Save"), self.tr("Save functionality will be implemented soon."))

  def show_about(self):
    # 显示关于对话框
    about_text = self.tr(
      "<h2>Malody Analytics Tool</h2>"
      "<p>Version 1.2.0</p>"
      "<p>A tool for analyzing Malody rhythm game data.</p>"
      "<p>© 2023 Malody Analytics Team</p>"
    )
    QMessageBox.about(self, self.tr("About"), about_text)

  def changeEvent(self, event):
    # 处理语言更改事件
    if event.type() == event.LanguageChange:
      self.retranslate_ui()
    super().changeEvent(event)
