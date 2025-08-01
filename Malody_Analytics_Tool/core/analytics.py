# core/analytics.py
import json
import os
from collections import defaultdict
from PyQt5.QtCore import QObject, pyqtSignal


class MalodyAnalyzer(QObject):
  analysis_completed = pyqtSignal(dict)
  error_occurred = pyqtSignal(str)

  def __init__(self):
    super().__init__()
    self.results = {}

  def analyze_file(self, file_path):
    try:
      if not os.path.exists(file_path):
        raise FileNotFoundError("File not found")

      # 根据文件扩展名选择解析方法
      if file_path.endswith('.mld'):
        self.results = self._analyze_mld_file(file_path)
      elif file_path.endswith('.mldx'):
        self.results = self._analyze_mldx_file(file_path)
      else:
        raise ValueError("Unsupported file format")

      self.analysis_completed.emit(self.results)
    except Exception as e:
      self.error_occurred.emit(str(e))

  def _analyze_mld_file(self, file_path):
    # 模拟分析结果
    return {
      "total_songs": 42,
      "average_difficulty": 3.7,
      "most_played_genre": "Pop",
      "play_count": 128,
      "accuracy_distribution": {
        "90-100%": 25,
        "80-89%": 35,
        "70-79%": 28,
        "<70%": 12
      }
    }

  def _analyze_mldx_file(self, file_path):
    # 模拟分析结果
    return {
      "total_songs": 67,
      "average_difficulty": 4.2,
      "most_played_genre": "Rock",
      "play_count": 192,
      "accuracy_distribution": {
        "90-100%": 32,
        "80-89%": 42,
        "70-79%": 18,
        "<70%": 8
      }
    }


def analyze_malody_data(file_path):
  """分析Malody数据文件的快捷函数"""
  analyzer = MalodyAnalyzer()
  return analyzer._analyze_mld_file(file_path)  # 简化版直接调用
