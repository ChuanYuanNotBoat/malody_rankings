# core/analytics.py
import os
import pandas as pd
from datetime import datetime
from openpyxl import load_workbook
import logging
logger = logging.getLogger(__name__)


# 模式文件命名规则
MODE_FILES = {
  0: "key.xlsx",
  3: "catch.xlsx"
}
# 其他模式
for i in range(1, 10):
  if i != 3:  # 跳过3，因为已经定义了
    MODE_FILES[i] = f"mode{i}.xlsx"


def get_latest_sheet_data(file_path):
  """读取一个模式文件中最新时间的工作表，返回DataFrame"""
  if not os.path.exists(file_path):
    return pd.DataFrame()

  # 获取所有工作表名
  wb = load_workbook(file_path)
  sheet_names = wb.sheetnames

  # 筛选出以'mode_{mode}_'开头的sheet
  sheets_with_time = []
  for sheet in sheet_names:
    if not sheet.startswith("mode_"):
      continue

    try:
      # 新逻辑：直接提取模式和时间部分
      parts = sheet.split('_')
      if len(parts) < 3:
        continue

      # 提取时间字符串（最后两部分）
      time_str = f"{parts[-2]}_{parts[-1]}"
      dt = datetime.strptime(time_str, "%Y-%m-%d_%H-%M")
      sheets_with_time.append((dt, sheet))
    except Exception as e:
      logger.warning(f"Failed to parse sheet name '{sheet}': {str(e)}")
      continue

  if not sheets_with_time:
    logger.debug(f"No valid sheets found in {file_path}")
    return pd.DataFrame()

  # 按时间排序，获取最新的
  latest_sheet = max(sheets_with_time, key=lambda x: x[0])[1]
  logger.debug(f"Found {len(sheets_with_time)} valid sheets, using latest: {latest_sheet}")
  return pd.read_excel(file_path, sheet_name=latest_sheet)


def analyze_mode_data(df, mode):
  if df.empty:
    logger.warning(f"No data found for mode {mode}")
    return {}

  # 基本统计
  total_players = len(df)
  avg_accuracy = df['acc'].mean()
  avg_play_count = df['pc'].mean()
  top_player = df.iloc[0].to_dict() if total_players > 0 else {}

  # 准确率分布
  bins = [0, 70, 80, 90, 100]
  labels = ['<70%', '70-79%', '80-89%', '90-100%']
  accuracy_distribution = pd.cut(df['acc'], bins=bins, labels=labels, right=False).value_counts().to_dict()

  # 等级分布
  level_distribution = df['lv'].value_counts().sort_index().to_dict()

  return {
    "mode": mode,
    "total_players": total_players,
    "avg_accuracy": round(avg_accuracy, 2),
    "avg_play_count": round(avg_play_count),
    "top_player": top_player,
    "accuracy_distribution": accuracy_distribution,
    "level_distribution": level_distribution
  }


def analyze_malody_folder(folder_path):
  """分析文件夹中的所有模式文件"""
  results = {}

  for mode, filename in MODE_FILES.items():
    file_path = os.path.join(folder_path, filename)

    if not os.path.exists(file_path):
      logger.warning(f"File not found: {file_path}")
      continue

    try:
      # 读取最新数据
      df = get_latest_sheet_data(file_path)

      if df.empty:
        logger.warning(f"No valid data found in {file_path}")
        continue

      # 分析模式数据
      mode_results = analyze_mode_data(df, mode)
      results[mode] = mode_results
      logger.info(f"Analyzed mode {mode}: {mode_results['total_players']} players")

    except Exception as e:
      logger.error(f"Error analyzing mode {mode}: {str(e)}")

  return results
