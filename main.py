
import sys
import os

def main():
    # 添加当前目录到Python路径
    current_dir = os.path.dirname(os.path.abspath(__file__))
    if current_dir not in sys.path:
        sys.path.insert(0, current_dir)
    
    # 检查是否安装了必要的GUI库
    try:
        import tkinter
        import matplotlib
    except ImportError:
        # 没有GUI支持，直接启动命令行版本
        from malody_stats import MalodyViz
        viz = MalodyViz()
        viz.cmdloop()
        return
    
    # 尝试启动GUI版本
    try:
        from malody_viz_gui import MalodyGUI
        app = MalodyGUI()
        app.run()
    except Exception as e:
        print(f"GUI版本启动失败: {e}")
        print("切换到命令行版本...")
        from malody_stats import MalodyViz
        viz = MalodyViz()
        viz.cmdloop()

if __name__ == "__main__":
    main()