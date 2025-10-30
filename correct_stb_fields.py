# correct_stb_fields.py
import sqlite3
import logging
from datetime import datetime

def setup_logging():
    """设置日志"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def correct_stb_fields(db_path='malody_rankings.db'):
    """更正STB谱面表中的字段错误"""
    logger = logging.getLogger(__name__)
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 检查表是否存在
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='charts'")
        if not cursor.fetchone():
            logger.error("charts表不存在")
            return False
        
        # 获取需要更正的记录数量
        cursor.execute("SELECT COUNT(*) FROM charts WHERE heat > 0 OR donate_count > 0")
        total_records = cursor.fetchone()[0]
        logger.info("发现 %d 条需要更正的记录", total_records)
        
        if total_records == 0:
            logger.info("没有需要更正的记录")
            return True
        
        # 交换heat和donate_count字段的值
        cursor.execute('''
        UPDATE charts 
        SET heat = donate_count, 
            donate_count = heat 
        WHERE heat > 0 OR donate_count > 0
        ''')
        
        affected_rows = cursor.rowcount
        conn.commit()
        
        logger.info("成功更正 %d 条记录的字段", affected_rows)
        
        # 验证更正结果
        cursor.execute("SELECT cid, heat, donate_count FROM charts WHERE heat > 0 OR donate_count > 0 LIMIT 5")
        sample_records = cursor.fetchall()
        
        logger.info("更正后的样本数据:")
        for cid, heat, donate in sample_records:
            logger.info("CID: %s, 热度: %s, 打赏: %s", cid, heat, donate)
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error("更正字段时出错: %s", e)
        return False

def main():
    """主函数"""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("开始更正STB谱面字段...")
    
    if correct_stb_fields():
        logger.info("字段更正完成")
    else:
        logger.error("字段更正失败")

if __name__ == "__main__":
    main()
