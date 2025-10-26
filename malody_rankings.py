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
import sqlite3
import subprocess
import json
from threading import Lock
import signal
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# 修复Python 3.12中SQLite datetime适配器的弃用警告
def adapt_datetime(dt):
    return dt.isoformat()

def convert_datetime(s):
    return datetime.fromisoformat(s.decode())

sqlite3.register_adapter(datetime, adapt_datetime)
sqlite3.register_converter("timestamp", convert_datetime)

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False
    print("注意: 未安装tqdm库，将使用简单进度指示")

# 配置日志
logging.basicConfig(
    filename='crawler.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='a'
)
logger = logging.getLogger()

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
MODES = list(range(10))

DB_FILE = "malody_rankings.db"

GIT_REPO_PATH = os.path.dirname(os.path.abspath(__file__))

stop_requested = False
stop_lock = Lock()

def signal_handler(sig, frame):
    global stop_requested
    with stop_lock:
        stop_requested = True
    logger.info("收到终止信号，正在安全退出...")
    time.sleep(1)
    DatabaseManager().close_connection()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

class DatabaseManager:
    _instance = None
    _lock = Lock()
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
            cls._instance.connection = None
            cls._instance.connections = {}
        return cls._instance
    
    def get_connection(self, thread_id=None):
        if thread_id is None:
            thread_id = threading.get_ident()
            
        with self._lock:
            if thread_id not in self.connections:
                self.connections[thread_id] = sqlite3.connect(
                    DB_FILE, 
                    detect_types=sqlite3.PARSE_DECLTYPES,
                    timeout=30,
                    check_same_thread=False
                )
                self.connections[thread_id].execute("PRAGMA journal_mode=WAL")
                self.connections[thread_id].execute("PRAGMA busy_timeout = 30000")
            return self.connections[thread_id]
    
    def close_connection(self, thread_id=None):
        with self._lock:
            if thread_id is None:
                for conn in self.connections.values():
                    conn.close()
                self.connections = {}
            elif thread_id in self.connections:
                self.connections[thread_id].close()
                del self.connections[thread_id]
    
    def execute_query(self, query, params=None, thread_id=None):
        conn = self.get_connection(thread_id)
        cursor = conn.cursor()
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            conn.commit()
            return cursor
        except Exception as e:
            conn.rollback()
            raise e
    
    def executemany_query(self, query, params_list, thread_id=None):
        conn = self.get_connection(thread_id)
        cursor = conn.cursor()
        try:
            cursor.executemany(query, params_list)
            conn.commit()
            return cursor
        except Exception as e:
            conn.rollback()
            raise e

def init_database():
    """初始化数据库，创建表结构"""
    db_manager = DatabaseManager()
    cursor = db_manager.get_connection().cursor()
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS player_identity (
        player_id INTEGER PRIMARY KEY AUTOINCREMENT,
        current_name TEXT NOT NULL,
        first_seen TIMESTAMP NOT NULL,
        last_seen TIMESTAMP NOT NULL
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS player_aliases (
        alias_id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_id INTEGER NOT NULL,
        alias TEXT NOT NULL,
        first_seen TIMESTAMP NOT NULL,
        last_seen TIMESTAMP NOT NULL,
        FOREIGN KEY (player_id) REFERENCES player_identity (player_id)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS player_rankings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_id INTEGER,
        mode INTEGER NOT NULL,
        rank INTEGER NOT NULL,
        name TEXT NOT NULL,
        lv INTEGER,
        exp INTEGER,
        acc REAL,
        combo INTEGER,
        pc INTEGER,
        crawl_time TIMESTAMP NOT NULL,
        FOREIGN KEY (player_id) REFERENCES player_identity (player_id),
        UNIQUE(mode, rank, crawl_time)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS import_metadata (
        mode INTEGER PRIMARY KEY,
        last_import_time TIMESTAMP
    )
    ''')
    
    for mode in MODES:
        cursor.execute(
            "INSERT OR IGNORE INTO import_metadata (mode, last_import_time) VALUES (?, NULL)",
            (mode,)
        )
    
    db_manager.get_connection().commit()
    logger.info("数据库初始化完成")

def resolve_player_identity(name, crawl_time):
    """解析玩家身份，处理改名情况"""
    db_manager = DatabaseManager()
    cursor = db_manager.get_connection().cursor()
    
    try:
        cursor.execute(
            "SELECT player_id FROM player_aliases WHERE alias = ?",
            (name,)
        )
        result = cursor.fetchone()
        
        if result:
            player_id = result[0]
            
            cursor.execute(
                "UPDATE player_aliases SET last_seen = ? WHERE alias = ?",
                (crawl_time, name)
            )
            
            cursor.execute(
                "UPDATE player_identity SET last_seen = ? WHERE player_id = ?",
                (crawl_time, player_id)
            )
        else:
            cursor.execute(
                "INSERT INTO player_identity (current_name, first_seen, last_seen) VALUES (?, ?, ?)",
                (name, crawl_time, crawl_time)
            )
            player_id = cursor.lastrowid
            
            cursor.execute(
                "INSERT INTO player_aliases (player_id, alias, first_seen, last_seen) VALUES (?, ?, ?, ?)",
                (player_id, name, crawl_time, crawl_time)
            )
        
        db_manager.get_connection().commit()
        return player_id
    except Exception as e:
        logger.error("解析玩家身份失败: %s", e)
        db_manager.get_connection().rollback()
        return None

def link_player_aliases(original_name, new_name, change_time):
    """手动关联玩家的两个名字（处理改名）"""
    db_manager = DatabaseManager()
    cursor = db_manager.get_connection().cursor()
    
    try:
        cursor.execute(
            "SELECT player_id FROM player_aliases WHERE alias = ?",
            (original_name,)
        )
        result = cursor.fetchone()
        
        if not result:
            logger.error("找不到原始名字: %s", original_name)
            return False
        
        player_id = result[0]
        
        cursor.execute(
            "SELECT player_id FROM player_aliases WHERE alias = ?",
            (new_name,)
        )
        result = cursor.fetchone()
        
        if result:
            old_player_id = result[0]
            
            cursor.execute(
                "UPDATE player_rankings SET player_id = ? WHERE player_id = ?",
                (player_id, old_player_id)
            )
            
            cursor.execute(
                "UPDATE player_aliases SET player_id = ? WHERE player_id = ?",
                (player_id, old_player_id)
            )
            
            cursor.execute(
                "DELETE FROM player_identity WHERE player_id = ?",
                (old_player_id,)
            )
        else:
            cursor.execute(
                "INSERT INTO player_aliases (player_id, alias, first_seen, last_seen) VALUES (?, ?, ?, ?)",
                (player_id, new_name, change_time, change_time)
            )
        
        cursor.execute(
            "UPDATE player_identity SET current_name = ? WHERE player_id = ?",
            (new_name, player_id)
        )
        
        db_manager.get_connection().commit()
        logger.info("成功关联玩家改名: %s -> %s", original_name, new_name)
        return True
    except Exception as e:
        logger.error("处理玩家改名失败: %s", e)
        db_manager.get_connection().rollback()
        return False

def parse_player_list(html):
    soup = BeautifulSoup(html, "html.parser")
    players = []
    
    top_items = soup.select("div.item-top")
    for item in top_items:
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
        pc_tag = item.select_one("span.pc, span[class*=pc]")
        
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
        
        acc_text = None
        if acc_tag:
            acc_text = acc_tag.text.replace("Acc:", "").replace("%", "").strip()
        
        combo_text = None
        if combo_tag:
            combo_text = combo_tag.text.replace("Combo:", "").strip()
        
        playcount = None
        if pc_tag:
            pc_text = pc_tag.text.replace("游玩次数:", "").strip()
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

    list_items = soup.select("div.item")
    for item in list_items:
        rank_tag = item.select_one("span.rank")
        rank = rank_tag.text.strip() if rank_tag else None
        
        name_tag = item.select_one("span.name a")
        lv_tag = item.select_one("span.lv")
        exp_tag = item.select_one("span.exp")
        acc_tag = item.select_one("span.acc")
        pc_tag = item.select_one("span.pc, span[class*=pc]")
        combo_tag = item.select_one("span.combo")
        
        acc_text = None
        if acc_tag:
            acc_text = acc_tag.text.replace("%", "").strip()
        
        playcount = None
        if pc_tag:
            pc_text = pc_tag.text.strip()
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
    
    processed_players = []
    for p in players:
        try:
            rank = int(p["rank"]) if p["rank"] else None
        except:
            rank = None
            
        try:
            lv = int(p["lv"]) if p["lv"] else 0
        except:
            lv = 0
            
        try:
            exp = int(p["exp"]) if p["exp"] else 0
        except:
            exp = 0
            
        try:
            acc = float(p["acc"]) if p["acc"] else 0.0
        except:
            acc = 0.0
            
        try:
            combo = int(p["combo"]) if p["combo"] else 0
        except:
            combo = 0
            
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
    
    players = parse_player_list(resp.text)
    
    df = pd.DataFrame(players)
    
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

def save_to_database(mode, df, crawl_time):
    """将数据保存到数据库"""
    if df.empty:
        return
    
    db_manager = DatabaseManager()
    cursor = db_manager.get_connection().cursor()
    
    try:
        data_to_insert = []
        for _, row in df.iterrows():
            player_id = resolve_player_identity(row['name'], crawl_time)
            if player_id is not None:
                data_to_insert.append((
                    player_id, mode, row['rank'], row['name'], row['lv'], row['exp'],
                    row['acc'], row['combo'], row['pc'], crawl_time
                ))
        
        if data_to_insert:
            cursor.executemany('''
            INSERT OR IGNORE INTO player_rankings 
            (player_id, mode, rank, name, lv, exp, acc, combo, pc, crawl_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', data_to_insert)
            
            db_manager.get_connection().commit()
            logger.info("模式 %d 的 %d 条数据已保存到数据库", mode, len(data_to_insert))
    except Exception as e:
        logger.error("保存模式 %d 数据到数据库失败: %s", mode, e)
        db_manager.get_connection().rollback()

def check_excel_file_integrity(filename):
    """检查Excel文件是否完整可用"""
    try:
        if not os.path.exists(filename):
            return False
            
        with open(filename, 'rb') as f:
            header = f.read(8)
            if header[:4] != b'PK\x03\x04':
                return False
                
        wb = load_workbook(filename, read_only=True)
        sheetnames = wb.sheetnames
        wb.close()
        return bool(sheetnames)
    except Exception as e:
        logger.warning("Excel文件完整性检查失败: %s - %s", filename, e)
        return False

def repair_excel_file(filename):
    """尝试修复损坏的Excel文件"""
    try:
        backup_name = f"{filename}.backup.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        import shutil
        shutil.copy2(filename, backup_name)
        logger.info("已创建备份文件: %s", backup_name)
        
        try:
            xl = pd.ExcelFile(filename)
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                for sheet_name in xl.sheet_names:
                    df = pd.read_excel(filename, sheet_name=sheet_name)
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
            logger.info("成功修复Excel文件: %s", filename)
            return True
        except Exception as e:
            logger.error("使用pandas修复失败: %s", e)
            
            try:
                wb = load_workbook(filename)
                wb.save(filename)
                logger.info("使用openpyxl修复成功: %s", filename)
                return True
            except Exception as e2:
                logger.error("使用openpyxl修复也失败: %s", e2)
                return False
    except Exception as e:
        logger.error("修复Excel文件过程中发生错误: %s", e)
        return False

def import_mode_data(mode):
    """导入单个模式的数据"""
    global stop_requested
    
    thread_id = threading.get_ident()
    db_manager = DatabaseManager()
    conn = db_manager.get_connection(thread_id)
    
    conn.execute("PRAGMA synchronous = OFF")
    conn.execute("PRAGMA journal_mode = MEMORY")
    conn.execute("PRAGMA cache_size = 10000")
    conn.execute("PRAGMA temp_store = MEMORY")
    
    player_alias_cache = {}
    
    filename = get_excel_filename(mode)
    if not os.path.exists(filename):
        logger.warning("模式 %d 的Excel文件不存在: %s", mode, filename)
        return 0
    
    if not check_excel_file_integrity(filename):
        logger.warning("模式 %d 的Excel文件可能已损坏: %s", mode, filename)
        if repair_excel_file(filename):
            logger.info("文件修复成功，继续导入")
        else:
            logger.error("文件修复失败，跳过模式 %d", mode)
            return 0
    
    cursor = conn.cursor()
    cursor.execute(
        "SELECT last_import_time FROM import_metadata WHERE mode = ?",
        (mode,)
    )
    result = cursor.fetchone()
    last_import_time = result[0] if result else None
    
    try:
        xl = pd.ExcelFile(filename)
        sheet_names = xl.sheet_names
    except Exception as e:
        logger.error("打开Excel文件失败: %s", e)
        try:
            wb = load_workbook(filename)
            sheet_names = wb.sheetnames
        except Exception as e2:
            logger.error("两种方式都无法打开Excel文件: %s", e2)
            return 0
    
    sheet_names = [s for s in sheet_names if s.startswith(f"mode_{mode}_")]
    
    sheet_times = []
    for sheet_name in sheet_names:
        try:
            time_str = sheet_name.replace(f"mode_{mode}_", "")
            sheet_time = datetime.strptime(time_str, "%Y-%m-%d_%H-%M")
            sheet_times.append((sheet_name, sheet_time))
        except:
            continue
    
    sheet_times.sort(key=lambda x: x[1])
    
    if HAS_TQDM:
        mode_pbar = tqdm(
            sheet_times, 
            desc=f"模式 {mode}", 
            position=mode + 1, 
            leave=False,
            unit="表"
        )
    else:
        mode_pbar = sheet_times
        print(f"开始处理模式 {mode}，共 {len(sheet_times)} 个表...")
    
    imported_count = 0
    batch_size = 50
    batch_data = []
    
    for i, (sheet_name, sheet_time) in enumerate(mode_pbar):
        with stop_lock:
            if stop_requested:
                logger.info("模式 %d 导入被中断，已导入 %d 条数据", mode, imported_count)
                break
        
        if last_import_time and sheet_time <= datetime.strptime(last_import_time, "%Y-%m-%d %H:%M:%S"):
            if HAS_TQDM:
                mode_pbar.update(1)
            continue
            
        try:
            df = pd.read_excel(filename, sheet_name=sheet_name)
            if df.empty:
                if HAS_TQDM:
                    mode_pbar.update(1)
                continue
            
            for _, row in df.iterrows():
                name = row['name']
                
                if name in player_alias_cache:
                    player_id = player_alias_cache[name]
                else:
                    player_id = resolve_player_identity(name, sheet_time)
                    player_alias_cache[name] = player_id
                
                if player_id is not None:
                    batch_data.append((
                        player_id, mode, row['rank'], name, row['lv'], row['exp'],
                        row['acc'], row['combo'], row['pc'], sheet_time
                    ))
            
            if batch_data and (len(batch_data) >= 1000 or i == len(sheet_times) - 1):
                cursor.executemany('''
                INSERT OR IGNORE INTO player_rankings 
                (player_id, mode, rank, name, lv, exp, acc, combo, pc, crawl_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', batch_data)
                
                imported_count += len(batch_data)
                batch_data = []
            
            if (i + 1) % batch_size == 0 or i == len(sheet_times) - 1:
                cursor.execute(
                    "UPDATE import_metadata SET last_import_time = ? WHERE mode = ?",
                    (sheet_time.strftime("%Y-%m-%d %H:%M:%S"), mode)
                )
                conn.commit()
            
            if HAS_TQDM:
                mode_pbar.set_postfix_str(f"已导入: {imported_count}")
            
        except Exception as e:
            logger.error("导入模式 %d 表 %s 时出错: %s", mode, sheet_name, e)
            conn.rollback()
            continue
    
    if batch_data:
        cursor.executemany('''
        INSERT OR IGNORE INTO player_rankings 
        (player_id, mode, rank, name, lv, exp, acc, combo, pc, crawl_time)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', batch_data)
        imported_count += len(batch_data)
        conn.commit()
    
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.commit()
    
    db_manager.close_connection(thread_id)
    
    if HAS_TQDM:
        mode_pbar.close()
    
    return imported_count

def import_historical_data():
    """从Excel文件导入历史数据到数据库"""
    if HAS_TQDM:
        main_pbar = tqdm(total=len(MODES), desc="总体进度", position=0)
    else:
        print("开始导入历史数据...")
    
    for mode in MODES:
        try:
            result = import_mode_data(mode)
            if HAS_TQDM:
                main_pbar.update(1)
                main_pbar.set_postfix_str(f"模式 {mode} 完成: {result} 条记录")
            else:
                print(f"模式 {mode} 完成: {result} 条记录")
        except Exception as e:
            logger.error("模式 %d 导入失败: %s", mode, e)
            if HAS_TQDM:
                main_pbar.update(1)
                main_pbar.set_postfix_str(f"模式 {mode} 失败: {e}")
            else:
                print(f"模式 {mode} 失败: {e}")
    
    if HAS_TQDM:
        main_pbar.close()
    
    if not HAS_TQDM:
        print("历史数据导入完成")

def run_git_command_with_retry(command, max_retries=3, delay=5):
    """运行Git命令并带有重试机制"""
    for attempt in range(max_retries):
        try:
            result = subprocess.run(command, capture_output=True, text=True, timeout=60)
            if result.returncode == 0:
                return result
            elif attempt == max_retries - 1:
                return result
            else:
                logger.warning(f"Git命令失败 (尝试 {attempt + 1}/{max_retries}): {result.stderr}")
                time.sleep(delay)
        except subprocess.TimeoutExpired:
            if attempt == max_retries - 1:
                raise
            logger.warning(f"Git命令超时 (尝试 {attempt + 1}/{max_retries})")
            time.sleep(delay)
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            logger.warning(f"Git命令异常 (尝试 {attempt + 1}/{max_retries}): {e}")
            time.sleep(delay)
    return None

def git_check_updates():
    """检查远程Git仓库是否有更新"""
    try:
        if not os.path.exists(os.path.join(GIT_REPO_PATH, '.git')):
            logger.info("当前目录不是Git仓库，跳过Git更新检查")
            return False
            
        original_cwd = os.getcwd()
        os.chdir(GIT_REPO_PATH)
        
        result = run_git_command_with_retry(["git", "remote", "-v"])
        if not result or not result.stdout.strip():
            logger.info("未配置Git远程仓库，跳过更新检查")
            os.chdir(original_cwd)
            return False
        
        result = run_git_command_with_retry(["git", "fetch", "origin"])
        if not result or result.returncode != 0:
            logger.warning("Git fetch失败: %s", result.stderr if result else "未知错误")
            os.chdir(original_cwd)
            return False
        
        result = run_git_command_with_retry(
            ["git", "diff", "--name-only", "HEAD", "origin/main", "--", "*.db", "*.xlsx"]
        )
        
        os.chdir(original_cwd)
        
        if result and result.stdout.strip():
            updated_files = result.stdout.strip().split('\n')
            logger.info("发现远程更新文件: %s", updated_files)
            return True
        else:
            logger.info("远程仓库没有.db或.xlsx文件的更新")
            return False
            
    except Exception as e:
        logger.warning("Git检查更新发生意外错误: %s", e)
        return False

def git_merge_data_files():
    """合并远程数据文件而不是直接覆盖"""
    try:
        if not os.path.exists(os.path.join(GIT_REPO_PATH, '.git')):
            logger.info("当前目录不是Git仓库，跳过Git合并")
            return False
            
        original_cwd = os.getcwd()
        os.chdir(GIT_REPO_PATH)
        
        # 先保存当前工作状态
        result = run_git_command_with_retry(["git", "stash"])
        if result and result.returncode != 0:
            logger.warning("Git stash失败: %s", result.stderr)
        
        # 获取远程更新
        result = run_git_command_with_retry(["git", "fetch", "origin"])
        if not result or result.returncode != 0:
            logger.warning("Git fetch失败: %s", result.stderr if result else "未知错误")
            os.chdir(original_cwd)
            return False
        
        # 尝试合并，如果有冲突则采用ours策略（保留本地更改）
        result = run_git_command_with_retry(["git", "merge", "origin/main", "-Xours"])
        if result and result.returncode == 0:
            logger.info("Git合并成功")
            success = True
        else:
            logger.warning("Git合并失败，尝试中止合并: %s", result.stderr if result else "未知错误")
            run_git_command_with_retry(["git", "merge", "--abort"])
            success = False
        
        # 恢复之前的工作状态
        result = run_git_command_with_retry(["git", "stash", "pop"])
        if result and result.returncode != 0:
            logger.warning("Git stash pop失败: %s", result.stderr)
        
        os.chdir(original_cwd)
        return success
    except Exception as e:
        logger.warning("Git合并发生意外错误: %s", e)
        return False

def git_add_commit_push(has_changes=True):
    """自动添加、提交和推送Git更改"""
    if not has_changes:
        logger.info("所有模式均无数据变化，跳过Git推送")
        return True
        
    try:
        if not os.path.exists(os.path.join(GIT_REPO_PATH, '.git')):
            logger.info("当前目录不是Git仓库，跳过Git推送")
            return True
            
        original_cwd = os.getcwd()
        os.chdir(GIT_REPO_PATH)
        
        result = run_git_command_with_retry(["git", "remote", "-v"])
        if not result or not result.stdout.strip():
            logger.info("未配置Git远程仓库，跳过推送")
            os.chdir(original_cwd)
            return True
        
        excel_files = [get_excel_filename(mode) for mode in MODES]
        files_to_add = [f for f in excel_files if os.path.exists(f)]
        files_to_add.append(DB_FILE)
        
        for file in files_to_add:
            result = run_git_command_with_retry(["git", "add", file])
            if result and result.returncode != 0:
                logger.warning("添加文件 %s 失败: %s", file, result.stderr)
        
        result = run_git_command_with_retry(["git", "status", "--porcelain"])
        if not result or not result.stdout.strip():
            logger.info("没有文件更改，跳过Git提交")
            os.chdir(original_cwd)
            return True
        
        commit_message = get_git_commit_message()
        result = run_git_command_with_retry(["git", "commit", "-m", commit_message])
        if result and result.returncode == 0:
            logger.info("Git提交成功: %s", commit_message)
        else:
            logger.warning("Git提交失败: %s", result.stderr if result else "未知错误")
            os.chdir(original_cwd)
            return False
        
        result = run_git_command_with_retry(["git", "push"])
        if result and result.returncode == 0:
            logger.info("Git推送成功")
            success = True
        else:
            logger.warning("Git推送失败: %s", result.stderr if result else "未知错误")
            success = False
        
        os.chdir(original_cwd)
        return success
    except Exception as e:
        logger.warning("Git操作发生意外错误: %s", e)
        return False

def check_data_changed(mode, df):
    """检查数据是否发生变化"""
    filename = get_excel_filename(mode)
    sheet_name = f"mode_{mode}"
    
    if not os.path.exists(filename):
        return True
    
    try:
        wb = load_workbook(filename)
        if sheet_name not in wb.sheetnames:
            return True
        
        sub_sheets = [s for s in wb.sheetnames if s.startswith(f"{sheet_name}_")]
        if not sub_sheets:
            return True
            
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
                return False
                
    except Exception as e:
        logger.error("检查数据变化时出错: %s", e)
        return True
        
    return True

def run_crawler_cycle():
    try:
        if git_check_updates():
            logger.info("检测到远程仓库有更新，正在合并数据文件...")
            if git_merge_data_files():
                logger.info("数据文件合并完成，重新导入更新数据...")
                DatabaseManager().close_connection()
                
                # 只重新导入最近的数据，避免重复工作
                for mode in MODES:
                    try:
                        imported = import_mode_data(mode)
                        if imported > 0:
                            logger.info("模式 %d 重新导入 %d 条数据", mode, imported)
                    except Exception as e:
                        logger.error("模式 %d 重新导入失败: %s", mode, e)
            else:
                logger.warning("数据文件合并失败，继续使用本地数据")
        else:
            logger.info("未检测到远程更新或Git不可用，继续使用本地数据")
    except Exception as e:
        logger.warning("Git更新检查失败，继续使用本地数据: %s", e)
    
    session = requests.Session()
    session.cookies.update(COOKIES)
    session.headers.update(HEADERS)
    
    session.mount('https://', requests.adapters.HTTPAdapter(max_retries=3))

    start_time = datetime.now()
    logger.info("=" * 50)
    logger.info("开始爬取周期: %s", start_time)

    has_changes = False
    
    for mode in MODES:
        try:
            with stop_lock:
                if stop_requested:
                    logger.info("爬取被中断")
                    break
            
            logger.info("处理模式: %d", mode)
            df = crawl_mode_player(session, mode)
            
            if df.empty:
                logger.warning("模式 %d 获取数据为空，跳过", mode)
                continue
                
            if not check_data_changed(mode, df):
                logger.info("模式 %d 数据未变化，跳过保存", mode)
                continue
                
            crawl_time = datetime.now()
            save_data_to_excel(mode, df, crawl_time)
            save_to_database(mode, df, crawl_time)
            has_changes = True
            
            time.sleep(3)
        except Exception as e:
            logger.exception("处理模式 %d 时发生错误", mode)
    
    try:
        git_add_commit_push(has_changes)
    except Exception as e:
        logger.warning("Git推送失败，但数据已保存到本地: %s", e)
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logger.info("爬取周期完成, 用时: %.2f秒", duration)
    logger.info("=" * 50)

def main():
    init_database()
    
    db_manager = DatabaseManager()
    cursor = db_manager.get_connection().cursor()
    
    cursor.execute("SELECT COUNT(*) FROM player_rankings")
    count = cursor.fetchone()[0]
    
    if count == 0:
        logger.info("开始导入历史数据...")
        import_historical_data()
        logger.info("历史数据导入完成")
    else:
        logger.info("数据库中已有 %d 条记录，跳过历史数据导入", count)
    
    try:
        while True:
            with stop_lock:
                if stop_requested:
                    logger.info("程序被终止")
                    break
            
            try:
                run_crawler_cycle()
            except Exception as e:
                logger.exception("主循环发生未处理异常")
            
            logger.info("等待30分钟后重启...")
            
            for i in range(30):
                with stop_lock:
                    if stop_requested:
                        logger.info("程序被终止")
                        break
                
                time.sleep(60)
                gc.collect()
    finally:
        DatabaseManager().close_connection()

if __name__ == "__main__":
    if "--once" in sys.argv:
        init_database()
        
        db_manager = DatabaseManager()
        cursor = db_manager.get_connection().cursor()
        
        cursor.execute("SELECT COUNT(*) FROM player_rankings")
        count = cursor.fetchone()[0]
        
        if count == 0:
            logger.info("开始导入历史数据...")
            import_historical_data()
            logger.info("历史数据导入完成")
        else:
            logger.info("数据库中已有 %d 条记录，跳过历史数据导入", count)
        
        run_crawler_cycle()
        DatabaseManager().close_connection()
    elif "--import-only" in sys.argv:
        init_database()
        logger.info("开始导入历史数据...")
        import_historical_data()
        logger.info("历史数据导入完成")
        DatabaseManager().close_connection()
    else:
        main()