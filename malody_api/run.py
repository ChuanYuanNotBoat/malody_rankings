# malody_api/run.py
#!/usr/bin/env python3
"""
Malody API启动脚本
"""
import uvicorn
import os
import sys

def main():
    """启动API服务器"""
    # 获取当前目录和父目录
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    
    # 将父目录添加到Python路径，这样malody_api可以作为包导入
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)
    
    # 检查数据库文件是否存在
    db_path = os.path.join(current_dir, "malody_rankings.db")
    if not os.path.exists(db_path):
        print(f"错误: 数据库文件不存在: {db_path}")
        print("请确保malody_rankings.db文件在当前目录下")
        sys.exit(1)
    
    print("启动Malody数据API服务器...")
    print(f"数据库文件: {db_path}")
    print("文档地址: http://localhost:8000/docs")
    print("API地址: http://localhost:8000")
    print("按 Ctrl+C 停止服务器")
    
    # 启动服务器 - 使用正确的模块路径
    uvicorn.run(
        "malody_api.app:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_level="info",
        access_log=True
    )

if __name__ == "__main__":
    main()