# main.py
import sys
from PyQt5.QtWidgets import QApplication
from ui.main_window import MainWindow
from utils.i18n import setup_i18n

# 添加资源导入
try:
  import resources_rc
except ImportError:
  print("Note: resources_rc module not found. UI icons may not display properly.")


def main():
  # 创建应用实例
  app = QApplication(sys.argv)

  # 设置国际化
  translator = setup_i18n(app)

  # 创建主窗口
  window = MainWindow(translator)
  window.show()

  # 启动应用
  sys.exit(app.exec_())


if __name__ == "__main__":
  main()
