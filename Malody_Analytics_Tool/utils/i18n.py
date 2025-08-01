# utils/i18n.py
import locale
import sys  # 添加这一行
from PyQt5.QtCore import QTranslator, QLocale
from PyQt5.QtWidgets import QApplication


def setup_i18n(app):
  """设置应用国际化支持"""
  translator = QTranslator()

  # 加载系统默认语言设置
  sys_lang = get_system_language()

  # 加载翻译文件
  if sys_lang == 'zh_CN':
    translator.load(":/translations/malody_zh_CN.qm")
    app.installTranslator(translator)

  # 设置全局字体以支持中文
  set_global_font(app)  # 修改函数调用

  return translator


def get_system_language():
  """获取系统语言代码"""
  # 尝试获取系统区域设置
  sys_lang = locale.getdefaultlocale()[0]

  # 检查是否为中文环境
  if sys_lang and (sys_lang.startswith('zh') or 'zh' in sys_lang.lower()):
    return 'zh_CN'
  return 'en'


def set_global_font(app):  # 添加app参数
  """设置全局字体以支持中文显示"""
  font = app.font()  # 使用应用当前字体

  # 根据操作系统选择合适的字体
  if sys.platform == 'win32':
    font_family = "Microsoft YaHei"
  elif sys.platform == 'darwin':
    font_family = "PingFang SC"
  else:  # Linux
    font_family = "Noto Sans CJK SC"

  # 尝试设置字体，如果失败则使用默认字体
  try:
    font.setFamily(font_family)
    app.setFont(font)
  except:
    # 如果字体设置失败，使用默认字体
    pass
