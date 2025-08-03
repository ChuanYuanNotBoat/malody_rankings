import logging
from PyQt5.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QLabel, QComboBox, QDateEdit, QPushButton
from PyQt5.QtChart import QChart, QChartView, QLineSeries, QValueAxis, QDateTimeAxis
from PyQt5.QtCore import Qt, QDateTime, QTimer
from PyQt5.QtGui import QPainter, QColor, QBrush, QPen

logger = logging.getLogger(__name__)


class HistoryChartWidget(QWidget):
  def __init__(self):
    super().__init__()
    self.player_history = []
    self.player_name = ""
    self.init_ui()
    self.chunk_size = 100  # 每次加载的数据点数

  def init_ui(self):
    # 主布局
    self.layout = QVBoxLayout()
    self.setLayout(self.layout)

    # 控制面板
    control_layout = QHBoxLayout()
    self.layout.addLayout(control_layout)

    # 玩家选择
    control_layout.addWidget(QLabel(self.tr("Select Player:")))
    self.player_combo = QComboBox()
    self.player_combo.currentIndexChanged.connect(self.on_player_changed)
    control_layout.addWidget(self.player_combo)

    # 日期范围选择
    control_layout.addWidget(QLabel(self.tr("Date Range:")))
    self.start_date_edit = QDateEdit()
    self.start_date_edit.setCalendarPopup(True)
    self.start_date_edit.setDisplayFormat("yyyy-MM-dd")
    control_layout.addWidget(self.start_date_edit)

    control_layout.addWidget(QLabel(self.tr("to")))
    self.end_date_edit = QDateEdit()
    self.end_date_edit.setCalendarPopup(True)
    self.end_date_edit.setDisplayFormat("yyyy-MM-dd")
    self.end_date_edit.setDate(QDateTime.currentDateTime().date())
    control_layout.addWidget(self.end_date_edit)

    # 刷新按钮
    self.refresh_btn = QPushButton(self.tr("Refresh"))
    self.refresh_btn.clicked.connect(self.refresh_data)
    control_layout.addWidget(self.refresh_btn)

    control_layout.addStretch()

    # 图表区域
    self.chart = QChart()
    self.chart.setAnimationOptions(QChart.SeriesAnimations)
    self.chart.legend().setVisible(True)
    self.chart.legend().setAlignment(Qt.AlignBottom)

    self.chart_view = QChartView(self.chart)
    self.chart_view.setRenderHint(QPainter.Antialiasing)
    self.chart_view.setRubberBand(QChartView.RectangleRubberBand)

    self.layout.addWidget(self.chart_view)

    # 初始设置
    self.setMinimumSize(800, 600)

  def set_player_list(self, players):
    """设置玩家列表"""
    self.player_combo.clear()
    self.player_combo.addItems(players)
    if players:
      self.player_combo.setCurrentIndex(0)

  def set_history_data(self, player_name, history):
    """设置历史数据"""
    self.player_name = player_name
    self.player_history = history
    self.update_chart()

  def on_player_changed(self, index):
    """玩家改变时触发"""
    if index >= 0:
      player_name = self.player_combo.currentText()
      # 实际应用中，这里应该从主窗口获取历史数据
      # 我们将在主窗口逻辑中处理

  def refresh_data(self):
    """刷新数据"""
    # 实际应用中，这里应该触发主窗口重新加载数据
    # 我们将在主窗口逻辑中处理
    pass

  def update_chart(self):
    """分块更新图表以避免UI冻结"""
    # 清除现有系列和定时器
    self.chart.removeAllSeries()

    if hasattr(self, 'timer') and self.timer.isActive():
      self.timer.stop()

    if not self.player_history:
      self.chart.setTitle(self.tr("No Data Available"))
      return

    # 设置图表标题
    self.chart.setTitle(self.tr("Player History: {}").format(self.player_name))

    # 创建数据系列
    self.rank_series = QLineSeries()
    self.rank_series.setName(self.tr("Rank"))
    self.rank_series.setColor(QColor(220, 53, 69))  # 红色

    self.lv_series = QLineSeries()
    self.lv_series.setName(self.tr("Level"))
    self.lv_series.setColor(QColor(40, 167, 69))  # 绿色

    self.exp_series = QLineSeries()
    self.exp_series.setName(self.tr("Experience"))
    self.exp_series.setColor(QColor(0, 123, 255))  # 蓝色

    self.pc_series = QLineSeries()
    self.pc_series.setName(self.tr("Play Count"))
    self.pc_series.setColor(QColor(255, 193, 7))  # 黄色

    # 添加系列到图表
    self.chart.addSeries(self.rank_series)
    self.chart.addSeries(self.lv_series)
    self.chart.addSeries(self.exp_series)
    self.chart.addSeries(self.pc_series)

    # 创建X轴（时间）
    self.axis_x = QDateTimeAxis()
    self.axis_x.setFormat("yyyy-MM-dd")
    self.axis_x.setTitleText(self.tr("Date"))
    self.chart.addAxis(self.axis_x, Qt.AlignBottom)

    # 创建Y轴（数值）
    self.axis_y = QValueAxis()
    self.axis_y.setTitleText(self.tr("Value"))
    self.chart.addAxis(self.axis_y, Qt.AlignLeft)

    # 初始化分块加载
    self.current_index = 0
    self.min_date = None
    self.max_date = None

    # 设置定时器分块加载数据
    self.timer = QTimer()
    self.timer.timeout.connect(self.add_data_chunk)
    self.timer.start(50)  # 每50ms加载一个数据块

  def add_data_chunk(self):
    """添加一块数据点"""
    if self.current_index >= len(self.player_history):
      # 所有数据加载完成
      self.timer.stop()

      # 设置坐标轴范围
      if self.min_date and self.max_date:
        self.axis_x.setRange(QDateTime(self.min_date), QDateTime(self.max_date))

      # 附加系列到坐标轴
      for series in [self.rank_series, self.lv_series, self.exp_series, self.pc_series]:
        series.attachAxis(self.axis_x)
        series.attachAxis(self.axis_y)

      return

    # 确定当前块的范围
    end_index = min(self.current_index + self.chunk_size, len(self.player_history))

    # 添加当前块的数据点
    for i in range(self.current_index, end_index):
      data = self.player_history[i]
      date = data['date']
      qt_date = QDateTime(date)

      # 记录最小和最大日期
      if self.min_date is None or date < self.min_date:
        self.min_date = date
      if self.max_date is None or date > self.max_date:
        self.max_date = date

      # 添加数据点
      if 'rank' in data and data['rank'] is not None:
        self.rank_series.append(qt_date.toMSecsSinceEpoch(), data['rank'])

      if 'lv' in data and data['lv'] is not None:
        self.lv_series.append(qt_date.toMSecsSinceEpoch(), data['lv'])

      if 'exp' in data and data['exp'] is not None:
        self.exp_series.append(qt_date.toMSecsSinceEpoch(), data['exp'])

      if 'pc' in data and data['pc'] is not None:
        self.pc_series.append(qt_date.toMSecsSinceEpoch(), data['pc'])

    # 更新索引
    self.current_index = end_index

  def on_series_hovered(self, point, state):
    """系列悬停事件"""
    if state:
      # 获取系列名称
      series = self.sender()
      series_name = series.name()

      # 转换时间为日期
      timestamp = point.x()
      date = QDateTime.fromMSecsSinceEpoch(timestamp).toString("yyyy-MM-dd")

      # 显示工具提示
      self.chart.setToolTip(f"{series_name}: {point.y():.0f}\n{date}")
    else:
      self.chart.setToolTip("")

  def retranslate_ui(self):
    """重新翻译UI文本"""
    self.refresh_btn.setText(self.tr("Refresh"))
    self.player_combo.setItemText(0, self.tr("Select Player"))

    # 更新图表标题
    if self.player_name:
      self.chart.setTitle(self.tr("Player History: {}").format(self.player_name))
