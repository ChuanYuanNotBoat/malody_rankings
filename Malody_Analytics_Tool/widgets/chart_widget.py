# widgets/chart_widget.py
from PyQt5.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QLabel, QComboBox
from PyQt5.QtChart import QChart, QChartView, QBarSet, QBarSeries, QBarCategoryAxis
from PyQt5.QtGui import QPainter
from PyQt5.QtCore import Qt


class ChartWidget(QWidget):
  def __init__(self):
    super().__init__()
    self.init_ui()

  def init_ui(self):
    # 主布局
    self.layout = QVBoxLayout()
    self.setLayout(self.layout)

    # 图表标题
    self.title_label = QLabel(self.tr("Performance Analysis"))
    self.title_label.setAlignment(Qt.AlignCenter)
    self.title_label.setStyleSheet("font-size: 16px; font-weight: bold;")
    self.layout.addWidget(self.title_label)

    # 图表类型选择
    self.chart_type_layout = QHBoxLayout()
    self.layout.addLayout(self.chart_type_layout)

    self.chart_type_label = QLabel(self.tr("Chart Type:"))
    self.chart_type_combo = QComboBox()
    self.chart_type_combo.addItem(self.tr("Accuracy Distribution"))
    self.chart_type_combo.addItem(self.tr("Difficulty Comparison"))
    self.chart_type_combo.addItem(self.tr("Genre Popularity"))

    self.chart_type_layout.addWidget(self.chart_type_label)
    self.chart_type_layout.addWidget(self.chart_type_combo)
    self.chart_type_layout.addStretch()

    # 图表视图
    self.chart = QChart()
    self.chart.setAnimationOptions(QChart.SeriesAnimations)

    self.chart_view = QChartView(self.chart)
    self.chart_view.setRenderHint(QPainter.Antialiasing)

    self.layout.addWidget(self.chart_view)

    # 初始空图表
    self.update_chart({})

  def update_chart(self, data):
    # 清除现有图表
    self.chart.removeAllSeries()

    if not data:
      # 创建空图表提示
      self.title_label.setText(self.tr("No Data Available"))
      return

    self.title_label.setText(self.tr("Performance Analysis"))

    # 获取当前选择的图表类型
    chart_type = self.chart_type_combo.currentText()

    if chart_type == self.tr("Accuracy Distribution"):
      self._create_accuracy_chart(data)
    elif chart_type == self.tr("Difficulty Comparison"):
      self._create_difficulty_chart(data)
    else:
      self._create_genre_chart(data)

  def _create_accuracy_chart(self, data):
    # 创建准确率分布图表
    if "accuracy_distribution" not in data:
      return

    dist = data["accuracy_distribution"]
    set_acc = QBarSet(self.tr("Accuracy"))

    # 添加数据
    categories = []
    for key in dist.keys():
      categories.append(key)
      set_acc.append(dist[key])

    series = QBarSeries()
    series.append(set_acc)

    self.chart.addSeries(series)
    self.chart.setTitle(self.tr("Accuracy Distribution"))

    # 创建X轴
    axis_x = QBarCategoryAxis()
    axis_x.append(categories)
    self.chart.createDefaultAxes()
    self.chart.setAxisX(axis_x, series)

    # 设置Y轴范围
    max_value = max(dist.values()) * 1.2
    self.chart.axisY().setRange(0, max_value)
    self.chart.axisY().setTitleText(self.tr("Count"))

  def _create_difficulty_chart(self, data):
    # 创建难度分布图表（模拟数据）
    set_diff = QBarSet(self.tr("Difficulty"))

    # 模拟难度分布
    difficulties = ["Easy", "Normal", "Hard", "Expert", "Master"]
    values = [12, 24, 32, 18, 14]

    for value in values:
      set_diff.append(value)

    series = QBarSeries()
    series.append(set_diff)

    self.chart.addSeries(series)
    self.chart.setTitle(self.tr("Difficulty Distribution"))

    # 创建X轴
    axis_x = QBarCategoryAxis()
    axis_x.append(difficulties)
    self.chart.createDefaultAxes()
    self.chart.setAxisX(axis_x, series)

    # 设置Y轴
    self.chart.axisY().setTitleText(self.tr("Count"))

  def _create_genre_chart(self, data):
    # 创建流派分布图表（模拟数据）
    set_genre = QBarSet(self.tr("Genre"))

    # 模拟流派分布
    genres = ["Pop", "Rock", "Electronic", "Classical", "Jazz"]
    values = [28, 22, 18, 15, 17]

    for value in values:
      set_genre.append(value)

    series = QBarSeries()
    series.append(set_genre)

    self.chart.addSeries(series)
    self.chart.setTitle(self.tr("Genre Popularity"))

    # 创建X轴
    axis_x = QBarCategoryAxis()
    axis_x.append(genres)
    self.chart.createDefaultAxes()
    self.chart.setAxisX(axis_x, series)

    # 设置Y轴
    self.chart.axisY().setTitleText(self.tr("Count"))
