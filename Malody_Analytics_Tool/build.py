# build.py
import os
import subprocess
import sys


def build_resources():
  """编译资源文件"""
  print("Building resource file...")
  # 获取项目根目录
  project_root = os.path.dirname(os.path.abspath(__file__))
  qrc_path = os.path.join(project_root, "resources.qrc")
  output_path = os.path.join(project_root, "resources_rc.py")

  # 尝试直接调用 pyrcc5
  pyrcc5_cmd = [
    sys.executable,
    "-m", "PyQt5.pyrcc_main",
    qrc_path,
    "-o", output_path
  ]

  try:
    subprocess.run(pyrcc5_cmd, check=True)
    print(f"Resource file built successfully: {output_path}")
    return True
  except subprocess.CalledProcessError as e:
    print(f"Error building resource file: {e}")
    return False


def build_translations():
  """生成和编译翻译文件"""
  print("Building translation files...")
  translations_dir = os.path.join(os.path.dirname(__file__), "translations")
  os.makedirs(translations_dir, exist_ok=True)

  ts_path = os.path.join(translations_dir, "malody_zh_CN.ts")
  qm_path = os.path.join(translations_dir, "malody_zh_CN.qm")

  # 收集所有需要翻译的.py文件
  source_files = [
    "main.py",
    os.path.join("ui", "main_window.py"),
    os.path.join("widgets", "chart_widget.py")
  ]

  # 生成 TS 文件
  pylupdate5_cmd = [
    sys.executable,
    "-m", "PyQt5.pylupdate_main",
    *source_files,
    "-ts", ts_path
  ]

  try:
    subprocess.run(pylupdate5_cmd, check=True)
    print(f"Translation source (TS) file generated: {ts_path}")

    # 编译 QM 文件
    lrelease_cmd = [
      sys.executable,
      "-m", "PyQt5.uic.pyuic",
      ts_path,
      "-o", qm_path
    ]

    # 注意：PyQt5 没有直接提供 lrelease 的 Python 模块
    # 这里简化处理，实际需要 Qt 的 lrelease 工具
    print("Note: QM file compilation requires Qt's lrelease tool.")
    print("Please use Qt Linguist to compile the TS file to QM.")
    return True
  except subprocess.CalledProcessError as e:
    print(f"Error building translations: {e}")
    return False


if __name__ == "__main__":
  print("Starting build process...")
  build_resources()
  build_translations()
  print("Build process completed.")
