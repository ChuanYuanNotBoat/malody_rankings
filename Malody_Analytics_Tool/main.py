import sys
import os
import logging
from PyQt5.QtWidgets import QApplication

# 将项目根目录添加到 Python 路径
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

# 配置日志
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# 添加资源导入
try:
    import resources_rc
    logging.info("Successfully imported resources_rc")
except ImportError:
    logging.warning("Note: resources_rc module not found. UI icons may not display properly.")

try:
    from utils.i18n import set_global_font, install_translator
    from ui.main_window import MainWindow
except ImportError as e:
    logging.error(f"导入失败: {str(e)}")
    sys.exit(1)

def main():
    # 创建应用实例
    app = QApplication(sys.argv)

    # 设置全局字体（解决中文乱码）
    set_global_font(app)

    # 安装翻译器
    translator = install_translator(app)

    # 创建主窗口
    window = MainWindow(translator)
    window.show()

    # 启动应用
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()
