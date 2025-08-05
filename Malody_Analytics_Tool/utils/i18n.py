# utils/i18n.py
import locale
import sys
import os
import logging
from PyQt5.QtCore import QTranslator, QLocale, QSettings
from PyQt5.QtWidgets import QApplication
import psutil


logger = logging.getLogger(__name__)


def install_translator(app):
  """安装翻译器到应用程序"""
  # 获取用户设置
  settings = QSettings("MalodyAnalytics", "MalodyAnalyticsTool")

  logger.debug(f"User language setting: {settings.value('language', '')}")

  # 如果没有用户设置，使用系统语言
  user_lang = settings.value("language", "")
  if not user_lang:
    user_lang = get_system_language()
    logger.debug(f"Using system language: {user_lang}")

  translator = QTranslator(app)

  # 尝试加载中文翻译
  if user_lang == 'zh_CN':
    # 尝试从资源文件加载
    if translator.load(":/translations/malody_zh_CN.qm"):
      app.installTranslator(translator)
      logger.info("Loaded Chinese translation from resource")
      return translator

    # 尝试从文件系统加载
    script_dir = os.path.dirname(os.path.abspath(__file__))
    qm_path = os.path.join(script_dir, "..", "translations", "malody_zh_CN.qm")

    if os.path.exists(qm_path):
      if translator.load(qm_path):
        app.installTranslator(translator)
        logger.info(f"Loaded Chinese translation from file: {qm_path}")
        return translator
      else:
        logger.error(f"Failed to load translation file: {qm_path}")
    else:
      logger.error(f"Translation file not found: {qm_path}")

  # 如果中文加载失败或使用英文，加载空翻译器
  logger.info("Using English (no translation)")
  return translator


def get_system_language():
  """获取系统语言代码"""
  # 尝试获取系统区域设置
  sys_lang = locale.getdefaultlocale()[0]

  # 检查是否为中文环境
  if sys_lang and (sys_lang.startswith('zh') or 'zh' in sys_lang.lower()):
    return 'zh_CN'
  return 'en'


def set_global_font(app):
  """设置全局字体以支持中文显示"""
  font = app.font()

  # 回退字体列表
  fallback_fonts = [
    "Microsoft YaHei",  # Windows
    "Microsoft JhengHei",  # Windows
    "SimHei",  # Windows
    "NSimSun",  # Windows
    "PingFang SC",  # macOS
    "Hiragino Sans GB",  # macOS
    "STHeiti",  # macOS
    "Noto Sans CJK SC",  # Linux
    "WenQuanYi Micro Hei",  # Linux
    "WenQuanYi Zen Hei",  # Linux
    "Source Han Sans SC"  # Adobe
  ]

  # 尝试所有字体直到找到可用的
  for font_family in fallback_fonts:
    try:
      # 设置字体
      font.setFamily(font_family)
      app.setFont(font)

      # 验证字体是否设置成功
      if app.font().family() == font_family:
        logger.info(f"Using font: {font_family}")
        return
    except Exception as e:
      logger.warning(f"Font {font_family} not available: {str(e)}")

  logger.info("Using default system font")
