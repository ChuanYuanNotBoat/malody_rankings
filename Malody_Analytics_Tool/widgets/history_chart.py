# widgets/history_chart.py
import logging
from PyQt5.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QLabel, QComboBox, QDateEdit, QPushButton
from PyQt5.QtChart import QChart, QChartView, QLineSeries, QValueAxis, QDateTimeAxis
from PyQt5.QtCore import Qt, QDateTime
from PyQt5.QtGui import QPainter, QColor, QBrush, QPen
from PyQt5.QtCore import QPointF

logger = logging.getLogger(__name__)


class HistoryChartWidget(QWidget):
  def __init__(self):
    super().__init__()
    self.player_history = []
    self.player_name = ""
    self.init_ui()

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
    """更新图表"""
    self.chart.removeAllSeries()

    if not self.player_history:
      self.chart.setTitle(self.tr("No Data Available"))
      return

    # 设置图表标题
    self.chart.setTitle(self.tr("Player History: {}").format(self.player_name))

    # 创建数据系列
    rank_series = QLineSeries()
    rank_series.setName(self.tr("Rank"))
    rank_series.setColor(QColor(220, 53, 69))  # 红色

    lv_series = QLineSeries()
    lv_series.setName(self.tr("Level"))
    lv_series.setColor(QColor(40, 167, 69))  # 绿色

    exp_series = QLineSeries()
    exp_series.setName(self.tr("Experience"))
    exp_series.setColor(QColor(0, 123, 255))  # 蓝色

    pc_series = QLineSeries()
    pc_series.setName(self.tr("Play Count"))
    pc_series.setColor(QColor(255, 193, 7))  # 黄色

    # 添加数据点
    min_date = None
    max_date = None

    for data in self.player_history:
      date = data['date']
      qt_date = QDateTime(date)

      # 记录最小和最大日期
      if min_date is None or date < min_date:
        min_date = date
      if max_date is None or date > max_date:
        max_date = date

      # 添加数据点
      if 'rank' in data and data['rank'] is not None:
        rank_series.append(qt_date.toMSecsSinceEpoch(), data['rank'])

      if 'lv' in data and data['lv'] is not None:
        lv_series.append(qt_date.toMSecsSinceEpoch(), data['lv'])

      if 'exp' in data and data['exp'] is not None:
        exp_series.append(qt_date.toMSecsSinceEpoch(), data['exp'])

      if 'pc' in data and data['pc'] is not None:
        pc_series.append(qt_date.toMSecsSinceEpoch(), data['pc'])

    # 添加系列到图表
    self.chart.addSeries(rank_series)
    self.chart.addSeries(lv_series)
    self.chart.addSeries(exp_series)
    self.chart.addSeries(pc_series)

    # 创建X轴（时间）
    axis_x = QDateTimeAxis()
    axis_x.setFormat("yyyy-MM-dd")
    axis_x.setTitleText(self.tr("Date"))
    axis_x.setRange(QDateTime(min_date), QDateTime(max_date))
    self.chart.addAxis(axis_x, Qt.AlignBottom)

    # 创建Y轴（数值）
    axis_y = QValueAxis()
    axis_y.setTitleText(self.tr("Value"))
    self.chart.addAxis(axis_y, Qt.AlignLeft)

    # 将系列附加到轴
    for series in [rank_series, lv_series, exp_series, pc_series]:
      series.attachAxis(axis_x)
      series.attachAxis(axis_y)

    # 添加悬停效果
    for series in [rank_series, lv_series, exp_series, pc_series]:
      series.hovered.connect(self.on_series_hovered)

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
