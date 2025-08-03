# widgets/chart_widget.py
import logging
from PyQt5.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QLabel, QComboBox, QGroupBox, QFormLayout
from PyQt5.QtChart import QChart, QChartView, QBarSet, QBarSeries, QBarCategoryAxis, QPieSeries, QPieSlice
from PyQt5.QtGui import QPainter, QColor
from PyQt5.QtCore import Qt

logger = logging.getLogger(__name__)


class ChartWidget(QWidget):
  def __init__(self):
    super().__init__()
    self.mode_data = {}  # 存储所有模式的分析结果
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
    self.show_empty_chart()

  def update_mode_data(self, mode, data):
    """更新单个模式的数据"""
    self.mode_data[mode] = data
    # 如果当前选中的是该模式或所有模式，刷新图表
    current_mode = self.mode_combo.currentData()
    if current_mode is None or current_mode == mode:
      self.on_mode_changed()

  def on_mode_changed(self):
    """模式改变时更新图表"""
    mode = self.mode_combo.currentData()

    if mode is None:  # "All Modes" 选项
      self.show_summary_view()
    elif mode in self.mode_data:
      self.show_mode_view(mode)
    else:
      self.show_empty_chart()

  def on_chart_type_changed(self):
    """图表类型改变时更新图表"""
    self.on_mode_changed()

  def show_empty_chart(self):
    """显示空图表"""
    self.chart.removeAllSeries()
    self.chart.setTitle(self.tr("No Data Available"))
    self.update_stats({})

  def show_summary_view(self):
    """显示所有模式摘要"""
    if not self.mode_data:
      self.show_empty_chart()
      return

    self.chart.setTitle(self.tr("Summary of All Modes"))

    # 根据图表类型显示不同内容
    chart_type = self.chart_type_combo.currentIndex()

    if chart_type == 0:  # 准确率分布
      self.create_accuracy_summary()
    elif chart_type == 1:  # 等级分布
      self.create_level_summary()
    else:  # 游玩次数比较
      self.create_playcount_comparison()

    # 更新统计信息
    self.update_stats(self.calculate_summary_stats())

  def show_mode_view(self, mode):
    """显示单个模式详情"""
    mode_data = self.mode_data[mode]
    self.chart.setTitle(self.tr("Mode {} Analysis").format(mode))

    # 根据图表类型显示不同内容
    chart_type = self.chart_type_combo.currentIndex()

    if chart_type == 0:  # 准确率分布
      self.create_accuracy_chart(mode_data)
    elif chart_type == 1:  # 等级分布
      self.create_level_chart(mode_data)
    else:  # 游玩次数
      self.create_playcount_chart(mode_data)

    # 更新统计信息
    self.update_stats(mode_data)

  def create_accuracy_summary(self):
    """创建所有模式的准确率摘要图表"""
    self.chart.removeAllSeries()

    # 创建条形图系列
    series = QBarSeries()

    # 为每个准确率区间创建一个条形集
    accuracy_ranges = ['<70%', '70-79%', '80-89%', '90-100%']
    sets = {}

    for mode, data in self.mode_data.items():
      dist = data["accuracy_distribution"]

      for acc_range in accuracy_ranges:
        if acc_range not in sets:
          sets[acc_range] = QBarSet(acc_range)
          # 设置不同颜色
          if acc_range == '<70%':
            sets[acc_range].setColor(QColor(220, 53, 69))  # 红色
          elif acc_range == '70-79%':
            sets[acc_range].setColor(QColor(253, 126, 20))  # 橙色
          elif acc_range == '80-89%':
            sets[acc_range].setColor(QColor(255, 193, 7))  # 黄色
          else:
            sets[acc_range].setColor(QColor(40, 167, 69))  # 绿色

        value = dist.get(acc_range, 0)
        sets[acc_range].append(value)

    # 添加条形集到系列
    for bar_set in sets.values():
      series.append(bar_set)

    self.chart.addSeries(series)

    # 设置X轴（模式）
    categories = [f"Mode {mode}" for mode in self.mode_data.keys()]
    axis_x = QBarCategoryAxis()
    axis_x.append(categories)
    self.chart.createDefaultAxes()
    self.chart.setAxisX(axis_x, series)

    # 设置Y轴标题
    self.chart.axisY().setTitleText(self.tr("Player Count"))

  def create_level_summary(self):
    """创建所有模式的等级摘要图表"""
    self.chart.removeAllSeries()

    # 创建条形图系列
    series = QBarSeries()

    # 为每个模式创建一个条形集
    sets = {}
    colors = [
      QColor(65, 105, 225),  # 蓝色
      QColor(220, 20, 60),  # 红色
      QColor(50, 205, 50),  # 绿色
      QColor(255, 140, 0),  # 橙色
      QColor(138, 43, 226),  # 紫色
      QColor(255, 215, 0),  # 金色
      QColor(0, 206, 209),  # 青色
      QColor(199, 21, 133),  # 粉色
      QColor(0, 128, 128),  # 青色
      QColor(139, 0, 139)  # 深紫色
    ]

    for i, (mode, data) in enumerate(self.mode_data.items()):
      bar_set = QBarSet(f"Mode {mode}")
      if i < len(colors):
        bar_set.setColor(colors[i])

      # 获取前5个最常见的等级
      level_dist = data["level_distribution"]
      sorted_levels = sorted(level_dist.items(), key=lambda x: x[1], reverse=True)[:5]

      # 添加到条形集
      for level, count in sorted_levels:
        bar_set.append(count)

      sets[mode] = bar_set
      series.append(bar_set)

    self.chart.addSeries(series)

    # 设置X轴（等级）
    categories = [str(level) for level, _ in sorted_levels]
    axis_x = QBarCategoryAxis()
    axis_x.append(categories)
    self.chart.createDefaultAxes()
    self.chart.setAxisX(axis_x, series)

    # 设置Y轴标题
    self.chart.axisY().setTitleText(self.tr("Player Count"))

  def create_playcount_comparison(self):
    """创建所有模式的游玩次数比较图表"""
    self.chart.removeAllSeries()

    # 创建饼图系列
    series = QPieSeries()
    series.setPieSize(0.7)

    # 添加每个模式的数据
    total_playcount = 0
    mode_playcounts = []

    for mode, data in self.mode_data.items():
      playcount = data["avg_play_count"] * data["total_players"]
      total_playcount += playcount
      mode_playcounts.append((mode, playcount))

    # 按游玩次数排序
    mode_playcounts.sort(key=lambda x: x[1], reverse=True)

    # 添加切片到饼图
    for mode, playcount in mode_playcounts:
      percentage = (playcount / total_playcount) * 100 if total_playcount > 0 else 0
      slice = series.append(f"Mode {mode} ({playcount:,})", percentage)
      slice.setLabelVisible(True)

    self.chart.addSeries(series)

  def create_accuracy_chart(self, mode_data):
    """创建单个模式的准确率分布图表"""
    self.chart.removeAllSeries()

    # 创建饼图系列
    series = QPieSeries()
    series.setPieSize(0.7)
    series.setLabelsVisible(True)

    # 添加数据
    dist = mode_data["accuracy_distribution"]
    for acc_range, count in dist.items():
      if count > 0:
        slice = series.append(f"{acc_range} ({count})", count)
        # 设置不同颜色
        if acc_range == '<70%':
          slice.setColor(QColor(220, 53, 69))  # 红色
        elif acc_range == '70-79%':
          slice.setColor(QColor(253, 126, 20))  # 橙色
        elif acc_range == '80-89%':
          slice.setColor(QColor(255, 193, 7))  # 黄色
        else:
          slice.setColor(QColor(40, 167, 69))  # 绿色

    self.chart.addSeries(series)
    self.chart.setTitle(self.tr("Accuracy Distribution for Mode {}").format(mode_data["mode"]))

  def create_level_chart(self, mode_data):
    """创建单个模式的等级分布图表"""
    self.chart.removeAllSeries()

    # 创建条形图系列
    series = QBarSeries()
    bar_set = QBarSet(self.tr("Player Count"))
    bar_set.setColor(QColor(70, 130, 180))  # 钢蓝色

    # 获取前10个最常见的等级
    level_dist = mode_data["level_distribution"]
    sorted_levels = sorted(level_dist.items(), key=lambda x: x[1], reverse=True)[:10]

    # 添加到条形集
    categories = []
    for level, count in sorted_levels:
      bar_set.append(count)
      categories.append(str(level))

    series.append(bar_set)
    self.chart.addSeries(series)

    # 设置X轴（等级）
    axis_x = QBarCategoryAxis()
    axis_x.append(categories)
    self.chart.createDefaultAxes()
    self.chart.setAxisX(axis_x, series)

    # 设置标题和轴标签
    self.chart.setTitle(self.tr("Level Distribution for Mode {}").format(mode_data["mode"]))
    self.chart.axisY().setTitleText(self.tr("Player Count"))

  def create_playcount_chart(self, mode_data):
    """创建单个模式的游玩次数图表"""
    self.chart.removeAllSeries()

    # 创建条形图系列
    series = QBarSeries()
    bar_set = QBarSet(self.tr("Average Play Count"))
    bar_set.setColor(QColor(60, 179, 113))  # 海洋绿

    # 添加数据
    bar_set.append(mode_data["avg_play_count"])

    series.append(bar_set)
    self.chart.addSeries(series)

    # 设置X轴（模式）
    axis_x = QBarCategoryAxis()
    axis_x.append([f"Mode {mode_data['mode']}"])
    self.chart.createDefaultAxes()
    self.chart.setAxisX(axis_x, series)

    # 设置标题和轴标签
    self.chart.setTitle(self.tr("Average Play Count for Mode {}").format(mode_data["mode"]))
    self.chart.axisY().setTitleText(self.tr("Play Count"))

  def calculate_summary_stats(self):
    """计算所有模式的摘要统计"""
    if not self.mode_data:
      return {}

    total_players = sum(data["total_players"] for data in self.mode_data.values())
    avg_accuracy = sum(data["avg_accuracy"] * data["total_players"] for data in self.mode_data.values()) / total_players
    avg_playcount = sum(
      data["avg_play_count"] * data["total_players"] for data in self.mode_data.values()) / total_players

    # 查找玩家最多的模式
    popular_mode = max(self.mode_data.items(), key=lambda x: x[1]["total_players"])

    # 查找准确率最高的模式
    accurate_mode = max(self.mode_data.items(), key=lambda x: x[1]["avg_accuracy"])

    return {
      "total_modes": len(self.mode_data),
      "total_players": total_players,
      "avg_accuracy": round(avg_accuracy, 2),
      "avg_playcount": round(avg_playcount),
      "popular_mode": popular_mode[0],
      "popular_mode_players": popular_mode[1]["total_players"],
      "accurate_mode": accurate_mode[0],
      "accurate_mode_accuracy": accurate_mode[1]["avg_accuracy"]
    }

  def update_stats(self, data):
    """更新统计信息面板"""
    # 清除现有统计信息
    while self.stats_layout.count():
      item = self.stats_layout.takeAt(0)
      widget = item.widget()
      if widget is not None:
        widget.deleteLater()

    # 添加新统计信息
    if "mode" in data:  # 单个模式数据
      self.stats_layout.addRow(QLabel(self.tr("Mode:")), QLabel(str(data["mode"])))
      self.stats_layout.addRow(QLabel(self.tr("Total Players:")), QLabel(str(data["total_players"])))
      self.stats_layout.addRow(QLabel(self.tr("Average Accuracy:")), QLabel(f"{data['avg_accuracy']}%"))
      self.stats_layout.addRow(QLabel(self.tr("Average Play Count:")), QLabel(str(data["avg_play_count"])))

      if "top_player" in data and data["top_player"]:
        top_player = data["top_player"]
        self.stats_layout.addRow(QLabel(self.tr("Top Player:")), QLabel(top_player.get("name", "N/A")))
        self.stats_layout.addRow(QLabel(self.tr("Top Player Level:")), QLabel(str(top_player.get("lv", "N/A"))))
        self.stats_layout.addRow(QLabel(self.tr("Top Player Accuracy:")), QLabel(f"{top_player.get('acc', 'N/A')}%"))

    elif "total_modes" in data:  # 摘要数据
      self.stats_layout.addRow(QLabel(self.tr("Total Modes:")), QLabel(str(data["total_modes"])))
      self.stats_layout.addRow(QLabel(self.tr("Total Players:")), QLabel(str(data["total_players"])))
      self.stats_layout.addRow(QLabel(self.tr("Overall Accuracy:")), QLabel(f"{data['avg_accuracy']}%"))
      self.stats_layout.addRow(QLabel(self.tr("Average Play Count:")), QLabel(str(data["avg_playcount"])))
      self.stats_layout.addRow(QLabel(self.tr("Most Popular Mode:")),
                               QLabel(f"Mode {data['popular_mode']} ({data['popular_mode_players']} players)"))
      self.stats_layout.addRow(QLabel(self.tr("Most Accurate Mode:")),
                               QLabel(f"Mode {data['accurate_mode']} ({data['accurate_mode_accuracy']}%)"))

  def retranslate_ui(self):
    """重新翻译UI文本"""
    logger.debug("Retranslating chart widget")

    # 更新控件文本
    self.stats_group.setTitle(self.tr("Mode Statistics"))

    # 更新模式选择框
    current_mode_index = self.mode_combo.currentIndex()
    current_mode_data = self.mode_combo.currentData()

    self.mode_combo.clear()
    self.mode_combo.addItem(self.tr("All Modes"))
    self.mode_combo.addItem("0: " + self.tr("Key"), 0)
    self.mode_combo.addItem("1: " + self.tr("DJ"), 1)
    self.mode_combo.addItem("2: " + self.tr("Pad"), 2)
    self.mode_combo.addItem("3: " + self.tr("Catch"), 3)
    self.mode_combo.addItem("4: " + self.tr("Ring"), 4)
    self.mode_combo.addItem("5: " + self.tr("Slide"), 5)
    self.mode_combo.addItem("6: " + self.tr("Live"), 6)
    self.mode_combo.addItem("7: " + self.tr("Key (7K)"), 7)
    self.mode_combo.addItem("8: " + self.tr("Key (9K)"), 8)
    self.mode_combo.addItem("9: " + self.tr("Key (10K)"), 9)

    # 恢复之前的选择
    if current_mode_data is not None:
      for i in range(self.mode_combo.count()):
        if self.mode_combo.itemData(i) == current_mode_data:
          self.mode_combo.setCurrentIndex(i)
          break
    else:
      self.mode_combo.setCurrentIndex(current_mode_index)

    # 更新图表类型选择框
    current_chart_index = self.chart_type_combo.currentIndex()
    self.chart_type_combo.clear()
    self.chart_type_combo.addItem(self.tr("Accuracy Distribution"))
    self.chart_type_combo.addItem(self.tr("Level Distribution"))
    self.chart_type_combo.addItem(self.tr("Play Count Comparison"))
    self.chart_type_combo.setCurrentIndex(current_chart_index)

    # 如果当前有数据，重新绘制图表
    if self.mode_data:
      self.on_mode_changed()
