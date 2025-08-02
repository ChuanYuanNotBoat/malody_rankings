# core/history_analyzer.py
import os
import pandas as pd
from datetime import datetime, timedelta
from openpyxl import load_workbook
import logging
import numpy as np
from core.analytics import MODE_FILES

logger = logging.getLogger(__name__)


def parse_sheet_date(sheet_name):
  """从工作表名解析日期"""
  try:
    # 格式: mode_0_2023-08-15_14-30
    parts = sheet_name.split('_')
    if len(parts) >= 3:
      date_str = parts[-2] + '_' + parts[-1]
      return datetime.strptime(date_str, "%Y-%m-%d_%H-%M")
  except:
    return None
  return None


def get_player_history(folder_path, player_name):
  """获取指定玩家的历史数据"""
  history = {}

  # 遍历所有模式文件
  for mode, filename in MODE_FILES.items():
    file_path = os.path.join(folder_path, filename)

    if not os.path.exists(file_path):
      continue

    try:
      wb = load_workbook(file_path)
      for sheet_name in wb.sheetnames:
        if not sheet_name.startswith(f"mode_{mode}_"):
          continue

        date = parse_sheet_date(sheet_name)
        if not date:
          continue

        df = pd.read_excel(file_path, sheet_name=sheet_name)
        player_data = df[df['name'] == player_name]

        if not player_data.empty:
          # 提取玩家数据
          player_row = player_data.iloc[0]
          history[date] = {
            'mode': mode,
            'rank': player_row.get('rank', None),
            'lv': player_row.get('lv', None),
            'exp': player_row.get('exp', None),
            'acc': player_row.get('acc', None),
            'combo': player_row.get('combo', None),
            'pc': player_row.get('pc', None),
            'date': date
          }
    except Exception as e:
      logger.error(f"Error processing {file_path} sheet {sheet_name}: {str(e)}")

  # 转换为按日期排序的列表
  sorted_history = sorted(history.values(), key=lambda x: x['date'])
  return sorted_history


def calculate_player_growth(history, start_date=None, end_date=None):
  """计算玩家在指定日期范围内的成长"""
  if not history:
    return {}

  # 过滤日期范围
  if start_date:
    history = [h for h in history if h['date'] >= start_date]
  if end_date:
    history = [h for h in history if h['date'] <= end_date]

  if not history:
    return {}

  # 计算增量
  first = history[0]
  last = history[-1]

  return {
    'player_name': last.get('name', 'Unknown'),
    'rank_change': last['rank'] - first['rank'] if 'rank' in last and 'rank' in first else None,
    'lv_change': last['lv'] - first['lv'] if 'lv' in last and 'lv' in first else None,
    'exp_change': last['exp'] - first['exp'] if 'exp' in last and 'exp' in first else None,
    'pc_change': last['pc'] - first['pc'] if 'pc' in last and 'pc' in first else None,
    'days': (last['date'] - first['date']).days,
    'daily_exp_growth': round((last['exp'] - first['exp']) / (last['date'] - first['date']).days, 2)
    if 'exp' in last and 'exp' in first and (last['date'] - first['date']).days > 0 else None,
    'start_date': first['date'],
    'end_date': last['date']
  }


def get_all_players_growth(folder_path, start_date=None, end_date=None):
  """获取所有玩家在指定日期范围内的成长数据"""
  player_growth = {}

  # 首先获取所有玩家列表
  all_players = set()
  for mode, filename in MODE_FILES.items():
    file_path = os.path.join(folder_path, filename)
    if not os.path.exists(file_path):
      continue

    try:
      wb = load_workbook(file_path)
      for sheet_name in wb.sheetnames:
        if not sheet_name.startswith(f"mode_{mode}_"):
          continue

        df = pd.read_excel(file_path, sheet_name=sheet_name)
        all_players.update(df['name'].unique())
    except:
      continue

  # 计算每个玩家的成长
  for player in all_players:
    history = get_player_history(folder_path, player)
    growth = calculate_player_growth(history, start_date, end_date)
    if growth:
      player_growth[player] = growth

  return player_growth

