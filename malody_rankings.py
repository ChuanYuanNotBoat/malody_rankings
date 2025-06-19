import requests
from bs4 import BeautifulSoup
import pandas as pd
from openpyxl import load_workbook
from datetime import datetime
import time
import os
import gc
import logging
import sys

# 配置日志
logging.basicConfig(
  filename='crawler.log',
  level=logging.INFO,
  format='%(asctime)s - %(levelname)s - %(message)s',
  filemode='a'  # 追加模式
)
logger = logging.getLogger()

# 同时输出到控制台
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
logger.addHandler(console_handler)

# 替换为你自己的 Cookie 信息
COOKIES = {
  "sessionid": "--",
  "csrftoken": "--"
}

HEADERS = {
  "User-Agent": "Mozilla/5.0 (Android 12; Mobile) Python Script",
  "Referer": "https://m.mugzone.net/"
}

BASE_URL = "https://m.mugzone.net/page/all/player?from=0&mode={mode}"
MODES = list(range(10))  # mode 0 ~ 9


def parse_player_list(html):
  soup = BeautifulSoup(html, "html.parser")
  players = []

  # 解析前三名特殊item-top
  top_items = soup.select("div.item-top")
  for item in top_items:
    # 提取排名数字
    label_tag = item.select_one("i.label")
    rank = None
    if label_tag and label_tag.has_attr("class"):
      for c in label_tag["class"]:
        if c.startswith("top-"):
          rank = c.replace("top-", "")
          break

    name_tag = item.select_one("span.name a")
    lv_tag = item.select_one("span.lv")
    acc_tag = item.select_one("span.acc")
    combo_tag = item.select_one("span.combo")

    # 修复：使用包含"pc"类的选择器
    pc_tag = item.select_one("span.pc, span[class*=pc]")

    # 等级和经验用 - 分隔开
    level = None
    exp = None
    if lv_tag:
      lv_text = lv_tag.text.strip()
      if '-' in lv_text:
        parts = lv_text.split('-')
        level = parts[0].replace("Lv.", "").strip()
        exp = parts[1].strip()
      else:
        level = lv_text.replace("Lv.", "").strip()

    # 处理acc字段
    acc_text = None
    if acc_tag:
      acc_text = acc_tag.text.replace("Acc:", "").replace("%", "").strip()

    # 处理combo字段
    combo_text = None
    if combo_tag:
      combo_text = combo_tag.text.replace("Combo:", "").strip()

    # 处理pc字段 - 修复选择器问题
    playcount = None
    if pc_tag:
      pc_text = pc_tag.text.replace("游玩次数:", "").strip()
      # 提取所有数字字符
      digits = ''.join(filter(str.isdigit, pc_text))
      if digits:
        playcount = int(digits)

    players.append({
      "rank": rank,
      "name": name_tag.text.strip() if name_tag else None,
      "lv": level,
      "exp": exp,
      "acc": acc_text,
      "combo": combo_text,
      "pc": playcount
    })

  # 解析4名及以后列表item
  list_items = soup.select("div.item")
  for item in list_items:
    rank_tag = item.select_one("span.rank")
    rank = rank_tag.text.strip() if rank_tag else None

    name_tag = item.select_one("span.name a")
    lv_tag = item.select_one("span.lv")
    exp_tag = item.select_one("span.exp")
    acc_tag = item.select_one("span.acc")

    # 修复：使用更灵活的选择器
    pc_tag = item.select_one("span.pc, span[class*=pc]")
    combo_tag = item.select_one("span.combo")

    # 处理acc字段
    acc_text = None
    if acc_tag:
      acc_text = acc_tag.text.replace("%", "").strip()

    # 处理pc字段 - 修复选择器问题
    playcount = None
    if pc_tag:
      pc_text = pc_tag.text.strip()
      # 提取所有数字字符
      digits = ''.join(filter(str.isdigit, pc_text))
      if digits:
        playcount = int(digits)

    players.append({
      "rank": rank,
      "name": name_tag.text.strip() if name_tag else None,
      "lv": lv_tag.text.strip() if lv_tag else None,
      "exp": exp_tag.text.strip() if exp_tag else None,
      "acc": acc_text,
      "combo": combo_tag.text.strip() if combo_tag else None,
      "pc": playcount
    })

  # 转换为统一的数据类型
  processed_players = []
  for p in players:
    # 转换rank为整数
    try:
      rank = int(p["rank"]) if p["rank"] else None
    except:
      rank = None

    # 转换lv为整数
    try:
      lv = int(p["lv"]) if p["lv"] else 0
    except:
      lv = 0

    # 转换exp为整数
    try:
      exp = int(p["exp"]) if p["exp"] else 0
    except:
      exp = 0

    # 转换acc为浮点数
    try:
      acc = float(p["acc"]) if p["acc"] else 0.0
    except:
      acc = 0.0

    # 转换combo为整数
    try:
      combo = int(p["combo"]) if p["combo"] else 0
    except:
      combo = 0

    # 确保pc是整数
    pc = p["pc"] if p["pc"] is not None else 0

    if rank is not None:
      processed_players.append({
        "rank": rank,
        "name": p["name"],
        "lv": lv,
        "exp": exp,
        "acc": acc,
        "combo": combo,
        "pc": pc
      })

  return processed_players


def get_excel_filename(mode):
  if mode == 0:
    return "key.xlsx"
  elif mode == 3:
    return "catch.xlsx"
  else:
    return f"mode{mode}.xlsx"


def crawl_mode_player(session, mode):
  url = BASE_URL.format(mode=mode)
  try:
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
  except requests.exceptions.RequestException as e:
    logger.error("模式 %d 请求失败: %s", mode, e)
    return pd.DataFrame()

  # 使用parse_player_list函数解析HTML
  players = parse_player_list(resp.text)

  # 创建DataFrame
  df = pd.DataFrame(players)

  # 清理和排序数据
  if not df.empty:
    df = df[df['rank'].notnull()]
    df['rank'] = df['rank'].astype(int)
    df = df.sort_values('rank').reset_index(drop=True)

  return df


def save_data_to_excel(mode, df, timestamp):
  if df.empty:
    logger.warning("模式 %d 无有效数据，跳过保存", mode)
    return

  filename = get_excel_filename(mode)
  sheet_name = f"mode_{mode}"

  try:
    if not os.path.exists(filename):
      with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        pd.DataFrame().to_excel(writer, sheet_name=sheet_name)

    wb = load_workbook(filename)
    if sheet_name not in wb.sheetnames:
      with pd.ExcelWriter(filename, engine='openpyxl', mode='a') as writer:
        pd.DataFrame().to_excel(writer, sheet_name=sheet_name)

    sub_sheets = [s for s in wb.sheetnames if s.startswith(f"{sheet_name}_")]
    latest_sheet = None
    latest_time = None
    for s in sub_sheets:
      try:
        dt_str = s.replace(f"{sheet_name}_", "")
        dt = datetime.strptime(dt_str, "%Y-%m-%d_%H-%M")
        if latest_time is None or dt > latest_time:
          latest_time = dt
          latest_sheet = s
      except:
        continue

    if latest_sheet:
      df_prev = pd.read_excel(filename, sheet_name=latest_sheet)
      if not df_prev.empty and df_prev.equals(df):
        logger.info("模式 %d 数据未变化，跳过保存", mode)
        return

    sub_sheet_name = f"{sheet_name}_{timestamp.strftime('%Y-%m-%d_%H-%M')}"
    with pd.ExcelWriter(filename, engine='openpyxl', mode='a') as writer:
      df.to_excel(writer, sheet_name=sub_sheet_name, index=False)

    logger.info("模式 %d 数据保存到 %s -> %s", mode, filename, sub_sheet_name)
  except Exception as e:
    logger.exception("保存模式 %d 数据到Excel失败", mode)


def run_crawler_cycle():
  session = requests.Session()
  session.cookies.update(COOKIES)
  session.headers.update(HEADERS)

  # 添加请求重试机制
  session.mount('https://', requests.adapters.HTTPAdapter(max_retries=3))

  start_time = datetime.now()
  logger.info("=" * 50)
  logger.info("开始爬取周期: %s", start_time)

  for mode in MODES:
    try:
      logger.info("处理模式: %d", mode)
      df = crawl_mode_player(session, mode)
      save_data_to_excel(mode, df, datetime.now())

      # 模式间短暂暂停，减少请求压力
      time.sleep(5)
    except Exception as e:
      logger.exception("处理模式 %d 时发生错误", mode)

  end_time = datetime.now()
  duration = (end_time - start_time).total_seconds()
  logger.info("爬取周期完成, 用时: %.2f秒", duration)
  logger.info("=" * 50)


def main():
  # 守护进程模式
  while True:
    try:
      run_crawler_cycle()
    except Exception as e:
      logger.exception("主循环发生未处理异常")

    # 添加定时睡眠，避免过于频繁重启
    logger.info("等待30分钟后重启...")

    # 分多次睡眠，便于中断
    for _ in range(30):
      time.sleep(60)  # 每次睡1分钟，共30分钟
      gc.collect()  # 手动触发垃圾回收


if __name__ == "__main__":
  # 简单参数处理
  if "--once" in sys.argv:
    run_crawler_cycle()
  else:
    main()
