import sqlite3
import pandas as pd
import logging
from datetime import datetime
import os

def export_all_key_stable_data():
    """导出Key模式下所有Stable谱面的完整数据"""
    
    # 配置参数
    db_path = 'malody_rankings.db'
    output_file = 'key_stable_complete_data.xlsx'
    
    # 设置日志
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    # 检查数据库文件
    if not os.path.exists(db_path):
        logger.error(f"数据库文件不存在: {db_path}")
        return False
    
    try:
        # 连接数据库
        conn = sqlite3.connect(db_path)
        logger.info("数据库连接成功")
        
        # 查询所有Key模式Stable谱面的完整数据
        query = """
        SELECT 
            s.sid,
            s.title,
            s.artist,
            s.bpm,
            s.length,
            s.cover_url,
            s.last_updated as song_last_updated,
            s.crawl_time as song_crawl_time,
            s.data_hash as song_data_hash,
            c.cid,
            c.version,
            c.creator_uid,
            c.creator_name,
            c.stabled_by_uid,
            c.stabled_by_name,
            c.level,
            c.mode,
            c.chart_length,
            c.status,
            c.heat,
            c.love_count,
            c.donate_count,
            c.play_count,
            c.last_updated as chart_last_updated,
            c.crawl_time as chart_crawl_time,
            c.data_hash as chart_data_hash
        FROM charts c
        JOIN songs s ON c.sid = s.sid
        WHERE c.mode = 0  -- Key模式
        AND c.status = 2  -- Stable状态
        ORDER BY s.sid, c.cid
        """
        
        # 执行查询
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if df.empty:
            logger.warning("没有找到Key模式的Stable谱面数据")
            return False
        
        logger.info(f"找到 {len(df)} 个Key模式Stable谱面")
        
        # 导出到Excel
        df.to_excel(output_file, index=False, sheet_name='Key模式Stable谱面')
        logger.info(f"数据已导出到: {output_file}")
        
        # 显示数据概览
        print(f"\n导出完成!")
        print(f"文件: {output_file}")
        print(f"谱面数量: {len(df)}")
        print(f"列数: {len(df.columns)}")
        print("\n包含的列:")
        for col in df.columns:
            print(f"  - {col}")
        
        return True
        
    except Exception as e:
        logger.error(f"导出过程中发生错误: {e}")
        return False

if __name__ == "__main__":
    export_all_key_stable_data()