# stb_crawler.py
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import time
import os
import logging
import sys
import signal
import threading
from datetime import datetime, timedelta
import re
import json
import argparse
from threading import Lock

# 复用现有的数据库管理器和配置
from malody_rankings import DatabaseManager, init_database, stop_requested, stop_lock, COOKIES, HEADERS

# 配置日志
logger = logging.getLogger()

# Malody API配置
BASE_URL = "https://m.mugzone.net"
HOMEPAGE_URL = BASE_URL + "/index"  # 修正主页URL
SEARCH_API_URL = BASE_URL + "/page/chart/filter"
CHART_URL = BASE_URL + "/chart/{cid}"
SONG_URL = BASE_URL + "/song/{sid}"

# 谱面状态映射
STATUS_MAP = {
    "Stable": 2,
    "Beta": 1, 
    "Alpha": 0
}

# 模式映射（复用现有的）
MODE_MAP = {
    0: "Key",
    1: "Step", 
    2: "DJ",
    3: "Catch",
    4: "Pad", 
    5: "Taiko",
    6: "Ring",
    7: "Slide",
    8: "Live",
    9: "Cube"
}

class STBCrawler:
    def __init__(self, session=None):
        if session is None:
            # 创建新的session并复用认证配置
            self.session = requests.Session()
            self.session.cookies.update(COOKIES)
            
            # 设置完整的headers，包括CSRF token
            headers = HEADERS.copy()
            if 'csrftoken' in COOKIES:
                headers['X-CSRFToken'] = COOKIES['csrftoken']
                headers['X-CSRF-Token'] = COOKIES['csrftoken']
                headers['Referer'] = BASE_URL
            
            self.session.headers.update(headers)
            self.session.mount('https://', requests.adapters.HTTPAdapter(max_retries=3))
        else:
            self.session = session
            
        self.db_manager = DatabaseManager()
        self.init_database()
        
    def init_database(self):
        """初始化STB谱面相关的数据库表"""
        cursor = self.db_manager.get_connection().cursor()
        
        # 歌曲表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS songs (
            sid INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            artist TEXT,
            bpm REAL,
            length INTEGER,
            cover_url TEXT,
            last_updated TIMESTAMP,
            crawl_time TIMESTAMP NOT NULL
        )
        ''')
        
        # 谱面表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS charts (
            cid INTEGER PRIMARY KEY,
            sid INTEGER NOT NULL,
            version TEXT NOT NULL,
            creator_uid INTEGER,
            creator_name TEXT,
            stabled_by_uid INTEGER,
            stabled_by_name TEXT,
            level TEXT,
            mode INTEGER NOT NULL,
            chart_length INTEGER,
            status INTEGER NOT NULL,
            heat INTEGER DEFAULT 0,
            love_count INTEGER DEFAULT 0,
            donate_count INTEGER DEFAULT 0,
            play_count INTEGER DEFAULT 0,
            last_updated TIMESTAMP,
            crawl_time TIMESTAMP NOT NULL,
            FOREIGN KEY (sid) REFERENCES songs (sid)
        )
        ''')
        
        # 爬虫状态表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS stb_crawler_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            last_crawl_time TIMESTAMP,
            last_chart_cid INTEGER
        )
        ''')
        
        # 创建索引
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_charts_sid ON charts(sid)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_charts_mode ON charts(mode)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_charts_status ON charts(status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_charts_last_updated ON charts(last_updated)')
        
        self.db_manager.get_connection().commit()
        logger.info("STB谱面数据库表初始化完成")

    def search_charts(self, mode=None, status=2, count=18, page=0, key="", creator=""):
        """搜索谱面API"""
        params = {
            "status": status,
            "count": count,
            "page": page,
            "key": key,
            "creator": creator
        }
        
        if mode is not None:
            params["mode"] = mode
            
        try:
            logger.debug("搜索谱面参数: %s", params)
            
            # 添加CSRF token到表单数据
            data = params.copy()
            if 'csrftoken' in COOKIES:
                data['csrfmiddlewaretoken'] = COOKIES['csrftoken']
            
            response = self.session.post(SEARCH_API_URL, data=data, timeout=30)
            response.raise_for_status()
            
            # 检查响应内容类型
            content_type = response.headers.get('content-type', '')
            if 'application/json' in content_type:
                return response.json()
            else:
                # 如果不是JSON，可能是重定向或错误页面
                logger.warning("API返回非JSON响应: %s", content_type)
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error("搜索谱面失败: %s", e)
            if hasattr(e, 'response') and e.response is not None:
                logger.error("响应状态: %s, 响应内容: %s", e.response.status_code, e.response.text[:200])
            return None

    def parse_chart_page(self, html, cid):
        """解析谱面详情页面"""
        soup = BeautifulSoup(html, "html.parser")
        
        # 提取基础信息
        chart_data = {
            "cid": cid,
            "status": 0,  # 默认Alpha
            "version": "",
            "creator_uid": None,
            "creator_name": "",
            "stabled_by_uid": None, 
            "stabled_by_name": "",
            "level": "",
            "mode": 0,
            "chart_length": 0,
            "heat": 0,
            "love_count": 0,
            "donate_count": 0,
            "play_count": 0,
            "last_updated": None
        }
        
        # 提取歌曲信息
        song_data = {
            "sid": None,
            "title": "",
            "artist": "",
            "bpm": 0,
            "length": 0,
            "cover_url": ""
        }
        
        try:
            # 从JavaScript变量中提取sid和cid
            script_text = soup.find('script', string=re.compile('window.malody'))
            if script_text:
                match = re.search(r'sid:(\d+)', script_text.string)
                if match:
                    song_data["sid"] = int(match.group(1))
                # 同时提取cid确保一致
                cid_match = re.search(r'cid:(\d+)', script_text.string)
                if cid_match:
                    chart_data["cid"] = int(cid_match.group(1))
            
            # 提取状态
            status_tag = soup.select_one('.song_title .title em.t2')
            if status_tag:
                status_text = status_tag.get_text().strip()
                chart_data["status"] = STATUS_MAP.get(status_text, 0)
            
            # 提取标题和艺术家 - 修复解析逻辑
            title_tag = soup.select_one('.song_title .title')
            if title_tag:
                # 提取艺术家
                artist_span = title_tag.find('span', class_='artist')
                if artist_span:
                    song_data["artist"] = artist_span.get_text().strip()
                    artist_span.decompose()  # 移除艺术家span
                
                # 移除状态标签
                for em in title_tag.find_all('em'):
                    em.decompose()
                
                # 提取标题文本
                title_text = title_tag.get_text().strip()
                # 清理标题文本，移除可能的" - "前缀
                if title_text.startswith(' - '):
                    title_text = title_text[3:].strip()
                song_data["title"] = title_text
            
            # 提取版本和模式
            mode_tag = soup.select_one('.song_title .mode')
            if mode_tag:
                version_spans = mode_tag.find_all('span')
                if version_spans:
                    # 第一个span通常是版本信息
                    chart_data["version"] = version_spans[0].get_text().strip()
                
                # 提取模式
                img_tag = mode_tag.find('img')
                if img_tag and 'src' in img_tag.attrs:
                    src = img_tag['src']
                    mode_match = re.search(r'mode-(\d+)', src)
                    if mode_match:
                        chart_data["mode"] = int(mode_match.group(1))
                
                # 提取等级
                version_text = chart_data["version"]
                level_match = re.search(r'Lv\.(\d+(?:\.\d+)?)', version_text)
                if level_match:
                    chart_data["level"] = level_match.group(1)
            
            # 提取创作者信息 - 修复解析逻辑
            # 查找包含"Created by:"的文本
            created_by_spans = [span for span in soup.find_all('span') 
                              if span.get_text().strip().startswith('Created by:')]
            
            if created_by_spans:
                created_by_span = created_by_spans[0]
                # 查找紧随其后的创作者链接
                creator_link = None
                for sibling in created_by_span.next_siblings:
                    if hasattr(sibling, 'name') and sibling.name == 'a':
                        creator_link = sibling
                        break
                
                if creator_link and 'href' in creator_link.attrs:
                    href = creator_link['href']
                    uid_match = re.search(r'/accounts/user/(\d+)', href)
                    if uid_match:
                        chart_data["creator_uid"] = int(uid_match.group(1))
                        chart_data["creator_name"] = creator_link.get_text().strip()
            
            # 提取稳定者信息 - 修复解析逻辑
            # 查找包含"Stabled by:"的文本
            stabled_by_spans = [span for span in soup.find_all('span') 
                              if span.get_text().strip().startswith('Stabled by:')]
            
            if stabled_by_spans:
                stabled_by_span = stabled_by_spans[0]
                # 查找紧随其后的稳定者链接
                stabled_link = None
                for sibling in stabled_by_span.next_siblings:
                    if hasattr(sibling, 'name') and sibling.name == 'a':
                        stabled_link = sibling
                        break
                
                if stabled_link and 'href' in stabled_link.attrs:
                    href = stabled_link['href']
                    uid_match = re.search(r'/accounts/user/(\d+)', href)
                    if uid_match:
                        chart_data["stabled_by_uid"] = int(uid_match.group(1))
                        chart_data["stabled_by_name"] = stabled_link.get_text().strip()
            
            # 提取ID、长度、BPM、最后更新时间 - 修复解析逻辑
            sub_tag = soup.select_one('.song_title .sub')
            if sub_tag:
                sub_text = sub_tag.get_text()
                
                # 使用正则表达式提取所有信息
                # ID
                id_match = re.search(r'ID\s*:c?(\d+)', sub_text)
                if id_match:
                    chart_data["cid"] = int(id_match.group(1))
                
                # 长度 - 修复长度提取
                length_match = re.search(r'长度\s*:\s*(\d+)s', sub_text)
                if length_match:
                    length_value = int(length_match.group(1))
                    chart_data["chart_length"] = length_value
                    song_data["length"] = length_value
                
                # BPM - 修复BPM提取
                bpm_match = re.search(r'BPM\s*:\s*(\d+(?:\.\d+)?)', sub_text)
                if bpm_match:
                    try:
                        song_data["bpm"] = float(bpm_match.group(1))
                    except ValueError:
                        logger.warning("无法解析BPM值: %s", bpm_match.group(1))
                
                # 最后更新时间
                date_match = re.search(r'最后更新\s*:\s*(\d{4}-\d{2}-\d{2} \d{2}:\d{2})', sub_text)
                if date_match:
                    try:
                        chart_data["last_updated"] = datetime.strptime(date_match.group(1), "%Y-%m-%d %H:%M")
                    except ValueError:
                        logger.warning("无法解析日期: %s", date_match.group(1))
            
            # 提取热度信息 - 修复解析逻辑
            like_area = soup.select_one('.like_area')
            if like_area:
                # 热度
                heat_spans = like_area.find_all('span', class_='l')
                if len(heat_spans) >= 1:
                    try:
                        chart_data["heat"] = int(heat_spans[0].get_text().strip())
                    except ValueError:
                        pass
                
                # 打赏
                if len(heat_spans) >= 2:
                    try:
                        chart_data["donate_count"] = int(heat_spans[1].get_text().strip())
                    except ValueError:
                        pass
                
                # 播放次数（如果有）
                if len(heat_spans) >= 3:
                    try:
                        chart_data["play_count"] = int(heat_spans[2].get_text().strip())
                    except ValueError:
                        pass
            
            # 提取封面URL
            cover_div = soup.select_one('.song_title .cover')
            if cover_div and 'style' in cover_div.attrs:
                style = cover_div['style']
                url_match = re.search(r'url\((.*?)\)', style)
                if url_match:
                    song_data["cover_url"] = url_match.group(1)
            
            # 如果没有从JS获取到sid，尝试从封面URL提取
            if not song_data["sid"] and song_data["cover_url"]:
                sid_match = re.search(r'/(\d+)!', song_data["cover_url"])
                if sid_match:
                    song_data["sid"] = int(sid_match.group(1))
            
            # 如果还没有sid，记录警告
            if not song_data["sid"]:
                logger.warning("无法提取歌曲ID (cid=%s)", cid)
            
            return chart_data, song_data
            
        except Exception as e:
            logger.error("解析谱面页面失败 (cid=%s): %s", cid, e)
            return None, None

    def crawl_chart_detail(self, cid):
        """爬取单个谱面的详细信息"""
        url = CHART_URL.format(cid=cid)
        
        try:
            logger.debug("爬取谱面详情: %s", url)
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            chart_data, song_data = self.parse_chart_page(response.text, cid)
            if chart_data and song_data:
                return self.save_chart_data(chart_data, song_data)
            else:
                logger.warning("解析谱面页面返回空数据 (cid=%s)", cid)
                return False
                
        except requests.exceptions.RequestException as e:
            logger.error("爬取谱面详情失败 (cid=%s): %s", cid, e)
            return False

    def save_chart_data(self, chart_data, song_data):
        """保存谱面数据到数据库"""
        cursor = self.db_manager.get_connection().cursor()
        crawl_time = datetime.now()
        
        try:
            # 检查必要的数据是否存在
            if not song_data["sid"]:
                logger.error("缺少歌曲ID，无法保存数据 (cid=%s)", chart_data["cid"])
                return False
            
            # 保存歌曲信息
            cursor.execute('''
            INSERT OR REPLACE INTO songs 
            (sid, title, artist, bpm, length, cover_url, last_updated, crawl_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                song_data["sid"], song_data["title"], song_data["artist"], 
                song_data["bpm"], song_data["length"], song_data["cover_url"],
                chart_data["last_updated"], crawl_time
            ))
            
            # 保存谱面信息
            cursor.execute('''
            INSERT OR REPLACE INTO charts 
            (cid, sid, version, creator_uid, creator_name, stabled_by_uid, stabled_by_name,
             level, mode, chart_length, status, heat, love_count, donate_count, play_count,
             last_updated, crawl_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                chart_data["cid"], song_data["sid"], chart_data["version"],
                chart_data["creator_uid"], chart_data["creator_name"],
                chart_data["stabled_by_uid"], chart_data["stabled_by_name"],
                chart_data["level"], chart_data["mode"], chart_data["chart_length"],
                chart_data["status"], chart_data["heat"], chart_data["love_count"],
                chart_data["donate_count"], chart_data["play_count"],
                chart_data["last_updated"], crawl_time
            ))
            
            self.db_manager.get_connection().commit()
            logger.info("成功保存谱面数据: cid=%s, title=%s", chart_data["cid"], song_data["title"])
            return True
            
        except Exception as e:
            logger.error("保存谱面数据失败 (cid=%s): %s", chart_data["cid"], e)
            self.db_manager.get_connection().rollback()
            return False

    def get_last_crawl_state(self):
        """获取最后爬取状态"""
        cursor = self.db_manager.get_connection().cursor()
        cursor.execute("SELECT last_crawl_time, last_chart_cid FROM stb_crawler_state WHERE id = 1")
        result = cursor.fetchone()
        
        if result:
            return {
                "last_crawl_time": result[0],
                "last_chart_cid": result[1]
            }
        else:
            # 默认返回一周前
            one_week_ago = datetime.now() - timedelta(days=7)
            return {
                "last_crawl_time": one_week_ago,
                "last_chart_cid": None
            }

    def update_crawl_state(self, last_crawl_time=None, last_chart_cid=None):
        """更新爬取状态"""
        cursor = self.db_manager.get_connection().cursor()
        
        if last_crawl_time is None:
            last_crawl_time = datetime.now()
        
        cursor.execute('''
        INSERT OR REPLACE INTO stb_crawler_state 
        (id, last_crawl_time, last_chart_cid)
        VALUES (1, ?, ?)
        ''', (last_crawl_time, last_chart_cid))
        
        self.db_manager.get_connection().commit()

    def crawl_from_homepage(self, max_charts=50):
        """从主页爬取新上架谱面"""
        logger.info("从主页爬取新上架谱面，最多 %d 个", max_charts)
        
        try:
            response = self.session.get(HOMEPAGE_URL, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, "html.parser")
            
            # 查找新谱上架区域
            new_map_section = soup.find('div', id='newMap')
            if not new_map_section:
                logger.warning("未找到新谱上架区域")
                return 0
            
            # 查找所有谱面卡片
            chart_cards = new_map_section.find_all('div', class_='g_map')
            logger.info("在主页找到 %d 个新谱面卡片", len(chart_cards))
            
            success_count = 0
            crawled_songs = set()  # 记录已经爬取的歌曲，避免重复
            
            for card in chart_cards:
                if stop_requested:
                    break
                    
                # 提取歌曲链接
                song_link = card.find('a', class_='link', href=True)
                if not song_link:
                    continue
                    
                song_url = song_link['href']
                if not song_url.startswith('/song/'):
                    continue
                
                # 提取歌曲ID
                sid_match = re.search(r'/song/(\d+)', song_url)
                if not sid_match:
                    continue
                    
                sid = int(sid_match.group(1))
                
                # 避免重复爬取同一歌曲
                if sid in crawled_songs:
                    continue
                crawled_songs.add(sid)
                
                # 从歌曲页面获取所有谱面
                song_cids = self.get_charts_from_song_page(sid)
                if song_cids:
                    logger.info("歌曲 %d 有 %d 个谱面", sid, len(song_cids))
                    
                    for cid in song_cids:
                        if stop_requested:
                            break
                            
                        if self.crawl_chart_detail(cid):
                            success_count += 1
                        
                        # 避免请求过于频繁
                        time.sleep(1)
                        
                        # 检查是否达到最大数量
                        if success_count >= max_charts:
                            break
                
                # 检查是否达到最大数量
                if success_count >= max_charts:
                    break
            
            logger.info("主页爬取完成: 成功 %d 个谱面", success_count)
            return success_count
            
        except Exception as e:
            logger.error("从主页爬取失败: %s", e)
            return 0

    def get_charts_from_song_page(self, sid):
        """从歌曲页面获取所有谱面的CID和基本信息"""
        url = SONG_URL.format(sid=sid)
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, "html.parser")
            
            cids = set()
            
            # 方法1: 查找所有谱面表格行
            table_rows = soup.find_all('tr')
            for row in table_rows:
                # 查找包含谱面链接的单元格
                links = row.find_all('a', href=re.compile(r'/chart/\d+'))
                for link in links:
                    href = link['href']
                    cid_match = re.search(r'/chart/(\d+)', href)
                    if cid_match:
                        cid = int(cid_match.group(1))
                        cids.add(cid)
            
            # 方法2: 查找所有谱面卡片（备用方法）
            if not cids:
                chart_cards = soup.find_all('div', class_='g_map')
                for card in chart_cards:
                    link = card.find('a', href=re.compile(r'/chart/\d+'))
                    if link and link.has_attr('href'):
                        href = link['href']
                        cid_match = re.search(r'/chart/(\d+)', href)
                        if cid_match:
                            cid = int(cid_match.group(1))
                            cids.add(cid)
            
            logger.debug("从歌曲页面 %d 找到 %d 个谱面", sid, len(cids))
            return list(cids)
            
        except Exception as e:
            logger.error("访问歌曲页面 %d 失败: %s", sid, e)
            return []

    def crawl_from_latest_page(self, days=7, max_charts=100):
        """从最近变动页面爬取谱面"""
        logger.info("从最近变动页面爬取最近 %d 天的谱面，最多 %d 个", days, max_charts)
        
        latest_url = BASE_URL + "/page/latest"
        try:
            response = self.session.get(latest_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, "html.parser")
            
            # 查找所有包含谱面信息的元素
            # 方法1: 查找所有包含"chart"关键词的链接
            chart_links = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                if '/chart/' in href:
                    chart_links.append(href)
            
            # 从链接中提取CID
            cids = set()
            for link in chart_links:
                cid_match = re.search(r'/chart/(\d+)', link)
                if cid_match:
                    cid = int(cid_match.group(1))
                    cids.add(cid)
            
            logger.info("从最近变动页面找到 %d 个可能的谱面ID", len(cids))
            
            # 限制爬取数量
            cids_to_crawl = list(cids)[:max_charts]
            
            success_count = 0
            for cid in cids_to_crawl:
                if stop_requested:
                    break
                    
                if self.crawl_chart_detail(cid):
                    success_count += 1
                time.sleep(1)  # 避免请求过于频繁
            
            logger.info("从最近变动页面爬取完成: 成功 %d/%d", success_count, len(cids_to_crawl))
            return success_count
            
        except Exception as e:
            logger.error("从最近变动页面爬取失败: %s", e)
            return 0

    def crawl_from_all_sources(self, days=7, max_charts_per_source=50):
        """从所有可用数据源爬取谱面"""
        total_success = 0
        
        logger.info("开始多数据源爬取，时间范围: %d 天", days)
        
        # 1. 从主页爬取新上架谱面
        logger.info("=== 数据源1: 主页新上架谱面 ===")
        home_success = self.crawl_from_homepage(max_charts=max_charts_per_source)
        total_success += home_success
        
        if stop_requested:
            return total_success
        
        # 2. 从最近变动页面爬取
        logger.info("=== 数据源2: 最近变动页面 ===")
        latest_success = self.crawl_from_latest_page(days=days, max_charts=max_charts_per_source)
        total_success += latest_success
        
        if stop_requested:
            return total_success
        
        logger.info("多数据源爬取完成: 总计 %d 个谱面", total_success)
        return total_success

    def crawl_specific_songs(self, sid_list):
        """爬取指定歌曲的所有谱面"""
        logger.info("开始爬取 %d 个指定歌曲的所有谱面", len(sid_list))
        
        success_count = 0
        for sid in sid_list:
            if stop_requested:
                break
                
            song_cids = self.get_charts_from_song_page(sid)
            if song_cids:
                logger.info("歌曲 %d 有 %d 个谱面", sid, len(song_cids))
                
                for cid in song_cids:
                    if stop_requested:
                        break
                        
                    if self.crawl_chart_detail(cid):
                        success_count += 1
                    
                    time.sleep(1)
            else:
                logger.warning("歌曲 %d 没有找到谱面或访问失败", sid)
        
        logger.info("指定歌曲爬取完成: 成功 %d 个谱面", success_count)
        return success_count

    def crawl_recent_charts(self, days=7, modes=None, statuses=None, use_api=True):
        """爬取最近指定天数的谱面"""
        if modes is None:
            modes = list(MODE_MAP.keys())
        if statuses is None:
            statuses = [0, 1, 2]  # Alpha, Beta, Stable
        
        logger.info("开始爬取最近 %d 天的谱面数据", days)
        
        start_time = datetime.now()
        crawled_count = 0
        
        if use_api:
            # 使用API搜索
            for mode in modes:
                for status in statuses:
                    if stop_requested:
                        logger.info("爬取被中断")
                        break
                        
                    logger.info("尝试API搜索: 模式 %d, 状态 %d", mode, status)
                    page = 0
                    has_more = True
                    
                    while has_more and not stop_requested:
                        result = self.search_charts(mode=mode, status=status, page=page)
                        if not result or "list" not in result:
                            logger.warning("模式 %d 状态 %d 第 %d 页无数据或请求失败", mode, status, page)
                            break
                        
                        chart_list = result["list"]
                        if not chart_list:
                            logger.info("模式 %d 状态 %d 第 %d 页无数据，结束该模式", mode, status, page)
                            break
                        
                        logger.info("模式 %d 状态 %d 第 %d 页获取到 %d 个谱面", 
                                   mode, status, page, len(chart_list))
                        
                        for chart in chart_list:
                            if stop_requested:
                                break
                                
                            cid = chart.get("id")
                            if not cid:
                                continue
                                
                            # 爬取谱面详情
                            success = self.crawl_chart_detail(cid)
                            if success:
                                crawled_count += 1
                            
                            # 避免请求过于频繁
                            time.sleep(1)
                        
                        # 检查是否有更多页面
                        has_more = page + 1 < result.get("total", 0)
                        page += 1
                        
                        logger.info("模式 %d 状态 %d 第 %d 页完成, 已爬取 %d 个谱面", 
                                   mode, status, page, crawled_count)
                
                if stop_requested:
                    break
        else:
            # 使用最近变动页面
            crawled_count = self.crawl_from_latest_page(days=days)
        
        # 更新爬取状态
        self.update_crawl_state()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info("谱面爬取完成: 总计 %d 个谱面, 用时 %.2f秒", crawled_count, duration)
        
        return crawled_count

    def crawl_specific_charts(self, cid_list):
        """爬取指定的谱面列表"""
        logger.info("开始爬取 %d 个指定谱面", len(cid_list))
        
        success_count = 0
        for cid in cid_list:
            if stop_requested:
                break
                
            if self.crawl_chart_detail(cid):
                success_count += 1
            
            time.sleep(1)  # 避免请求过于频繁
        
        logger.info("指定谱面爬取完成: 成功 %d/%d", success_count, len(cid_list))
        return success_count

    def test_connection(self):
        """测试连接和认证"""
        try:
            # 测试访问主页
            response = self.session.get(HOMEPAGE_URL, timeout=100)
            response.raise_for_status()
            logger.info("连接测试成功 - 主页访问正常")
            
            # 测试访问一个已知谱面
            test_url = CHART_URL.format(cid=147719)
            response = self.session.get(test_url, timeout=100)
            response.raise_for_status()
            logger.info("认证测试成功 - 谱面页面访问正常")
            
            return True
        except Exception as e:
            logger.error("连接测试失败: %s", e)
            return False

    def test_api(self):
        """测试API访问"""
        try:
            result = self.search_charts(mode=0, status=2, page=0)
            if result and "list" in result:
                logger.info("API测试成功 - 获取到 %d 个谱面", len(result["list"]))
                return True
            else:
                logger.warning("API测试失败 - 无有效数据返回")
                return False
        except Exception as e:
            logger.error("API测试失败: %s", e)
            return False

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='STB谱面爬虫')
    parser.add_argument('--days', type=int, default=7, help='爬取最近多少天的数据（默认7天）')
    parser.add_argument('--modes', type=str, help='指定模式，逗号分隔（如0,1,2）')
    parser.add_argument('--statuses', type=str, help='指定状态，逗号分隔（0=Alpha,1=Beta,2=Stable）')
    parser.add_argument('--cid', type=str, help='爬取指定谱面ID，逗号分隔')
    parser.add_argument('--sid', type=str, help='爬取指定歌曲ID的所有谱面，逗号分隔')
    parser.add_argument('--once', action='store_true', help='运行一次后退出')
    parser.add_argument('--test', action='store_true', help='只测试连接')
    parser.add_argument('--test-api', action='store_true', help='测试API访问')
    parser.add_argument('--no-api', action='store_true', help='不使用API搜索，使用其他数据源')
    parser.add_argument('--source', choices=['all', 'home', 'latest'], default='all',
                       help='选择数据源: all=全部, home=主页, latest=最近变动')
    parser.add_argument('--max-charts', type=int, default=50, help='每个数据源最大爬取数量（默认50）')
    
    args = parser.parse_args()
    
    # 初始化数据库
    init_database()
    
    # 创建爬虫实例
    crawler = STBCrawler()
    
    # 测试连接
    if not crawler.test_connection():
        logger.error("连接测试失败，请检查网络或认证信息")
        return
    
    if args.test:
        logger.info("连接测试成功，退出")
        return
    
    if args.test_api:
        if crawler.test_api():
            logger.info("API测试成功")
        else:
            logger.error("API测试失败")
        return
    
    if args.cid:
        # 爬取指定谱面
        cid_list = [int(cid.strip()) for cid in args.cid.split(',')]
        crawler.crawl_specific_charts(cid_list)
    
    elif args.sid:
        # 爬取指定歌曲的所有谱面
        sid_list = [int(sid.strip()) for sid in args.sid.split(',')]
        crawler.crawl_specific_songs(sid_list)
    
    else:
        # 爬取最近谱面
        modes = None
        if args.modes:
            modes = [int(mode.strip()) for mode in args.modes.split(',')]
        
        statuses = None  
        if args.statuses:
            statuses = [int(status.strip()) for status in args.statuses.split(',')]
        
        use_api = not args.no_api
        
        if args.once:
            # 单次运行
            if use_api and args.source == 'all':
                # 尝试使用API
                crawler.crawl_recent_charts(days=args.days, modes=modes, statuses=statuses, use_api=True)
            else:
                # 使用其他数据源
                if args.source == 'all':
                    crawler.crawl_from_all_sources(days=args.days, max_charts_per_source=args.max_charts)
                elif args.source == 'home':
                    crawler.crawl_from_homepage(max_charts=args.max_charts)
                elif args.source == 'latest':
                    crawler.crawl_from_latest_page(days=args.days, max_charts=args.max_charts)
        else:
            # 持续运行
            try:
                while True:
                    with stop_lock:
                        if stop_requested:
                            break
                    
                    if use_api and args.source == 'all':
                        crawler.crawl_recent_charts(days=args.days, modes=modes, statuses=statuses, use_api=True)
                    else:
                        if args.source == 'all':
                            crawler.crawl_from_all_sources(days=args.days, max_charts_per_source=args.max_charts)
                        elif args.source == 'home':
                            crawler.crawl_from_homepage(max_charts=args.max_charts)
                        elif args.source == 'latest':
                            crawler.crawl_from_latest_page(days=args.days, max_charts=args.max_charts)
                    
                    logger.info("等待30分钟后继续...")
                    for i in range(30):
                        with stop_lock:
                            if stop_requested:
                                break
                        time.sleep(60)
                        
            except KeyboardInterrupt:
                logger.info("用户中断爬取")
            finally:
                DatabaseManager().close_connection()

if __name__ == "__main__":
    main()