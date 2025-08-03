import logging
from PyQt5.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QLabel, QComboBox, QGroupBox, QFormLayout
from PyQt5.QtChart import QChart, QChartView, QBarSet, QBarSeries, QBarCategoryAxis, QPieSeries, QPieSlice
from PyQt5.QtGui import QPainter, QColor
from PyQt5.QtCore import Qt, QTimer

logger = logging.getLogger(__name__)


class ChartWidget(QWidget):
  def __init__(self):
    super().__init__()
    self.current_data = None
    self.mode_data = {}
    self.init_ui()

  def init_ui(self):
    # 主布局
    self.layout = QVBoxLayout()
    self.setLayout(self.layout)

    # 控制面板
    control_layout = QHBoxLayout()
    self.layout.addLayout(control_layout)

    # 模式选择
    control_layout.addWidget(QLabel(self.tr("Select Mode:")))
    self.mode_combo = QComboBox()
    self.mode_combo.addItem(self.tr("All Modes"))
    self.mode_combo.addItem("0: Key", 0)
    self.mode_combo.addItem("1: DJ", 1)
    self.mode_combo.addItem("2: Pad", 2)
    self.mode_combo.addItem("3: Catch", 3)
    self.mode_combo.addItem("4: Ring", 4)
    self.mode_combo.addItem("5: Slide", 5)
    self.mode_combo.addItem("6: Live", 6)
    self.mode_combo.addItem("7: Key (7K)", 7)
    self.mode_combo.addItem("8: Key (9K)", 8)
    self.mode_combo.addItem("9: Key (10K)", 9)
    self.mode_combo.currentIndexChanged.connect(self.on_mode_changed)
    control_layout.addWidget(self.mode_combo)

    # 图表类型选择
    control_layout.addWidget(QLabel(self.tr("Chart Type:")))
    self.chart_type_combo = QComboBox()
    self.chart_type_combo.addItem(self.tr("Accuracy Distribution"))
    self.chart_type_combo.addItem(self.tr("Level Distribution"))
    self.chart_type_combo.addItem(self.tr("Play Count Comparison"))
    self.chart_type_combo.currentIndexChanged.connect(self.on_chart_type_changed)
    control_layout.addWidget(self.chart_type_combo)

    control_layout.addStretch()

    # 统计信息面板
    self.stats_group = QGroupBox(self.tr("Mode Statistics"))
    self.stats_layout = QFormLayout()
    self.stats_group.setLayout(self.stats_layout)
    self.layout.addWidget(self.stats_group)

    # 图表区域
    self.chart = QChart()
    self.chart.setAnimationOptions(QChart.SeriesAnimations)
    self.chart.setTitle(self.tr("Malody Ranking Analysis"))

    self.chart_view = QChartView(self.chart)
    self.chart_view.setRenderHint(QPainter.Antialiasing)

    self.layout.addWidget(self.chart_view)

    # 初始空图表
    self.update_chart({})

  def update_chart(self, data):
    """延迟更新图表以避免UI冻结"""
    self.mode_data = data

    # 清除现有定时器
    if hasattr(self, 'timer') and self.timer.isActive():
      self.timer.stop()

    # 使用定时器延迟更新
    self.timer = QTimer()
    self.timer.setSingleShot(True)
    self.timer.timeout.connect(self.delayed_update)
    self.timer.start(100)  # 延迟100ms更新

  def delayed_update(self):
    """延迟更新图表"""
    if self.mode_combo.currentIndex() == 0:  # "All Modes"
      self.show_summary_view()
    elif self.mode_combo.currentData() in self.mode_data:
      self.show_mode_view(self.mode_combo.currentData())
    else:
      self.show_empty_chart()

  # 其他方法保持不变...
  # [保留原有代码，包括show_summary_view, show_mode_view等方法]
