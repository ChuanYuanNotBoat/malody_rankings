import os
import re
import pandas as pd
from datetime import datetime
import argparse
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import sys

# 定义模式与文件名的映射
MODE_FILES = {
  0: "key.xlsx",
  3: "catch.xlsx"
}
# 为其他模式生成文件名
for i in range(1, 10):
  if i not in MODE_FILES and i != 3:  # 跳过已定义的3
    MODE_FILES[i] = f"mode{i}.xlsx"

# 时间戳格式和正则表达式
TIMESTAMP_FORMAT = "%Y-%m-%d_%H-%M"
TIMESTAMP_PATTERN = re.compile(r"mode_(\d+)_(\d{4}-\d{2}-\d{2}_\d{2}-\d{2})(?:_\d+)?$")


class MergeApp:
  def __init__(self, root):
    self.root = root
    self.root.title("Malody排行榜数据合并工具")
    self.root.geometry("800x600")
    self.root.resizable(True, True)

    # 创建主框架
    self.main_frame = ttk.Frame(root, padding="20")
    self.main_frame.pack(fill=tk.BOTH, expand=True)

    # 源目录部分
    ttk.Label(self.main_frame, text="源数据目录:").grid(row=0, column=0, sticky=tk.W, pady=5)
    self.source_listbox = tk.Listbox(self.main_frame, height=8, width=80)
    self.source_listbox.grid(row=1, column=0, columnspan=3, padx=5, pady=5, sticky=tk.W + tk.E)

    scrollbar = ttk.Scrollbar(self.main_frame, orient=tk.VERTICAL, command=self.source_listbox.yview)
    scrollbar.grid(row=1, column=3, sticky=tk.N + tk.S)
    self.source_listbox.config(yscrollcommand=scrollbar.set)

    btn_frame = ttk.Frame(self.main_frame)
    btn_frame.grid(row=2, column=0, columnspan=4, pady=10, sticky=tk.W)

    ttk.Button(btn_frame, text="添加目录", command=self.add_source).pack(side=tk.LEFT, padx=5)
    ttk.Button(btn_frame, text="移除所选", command=self.remove_selected).pack(side=tk.LEFT, padx=5)
    ttk.Button(btn_frame, text="清空列表", command=self.clear_sources).pack(side=tk.LEFT, padx=5)

    # 输出目录部分
    ttk.Label(self.main_frame, text="输出目录:").grid(row=3, column=0, sticky=tk.W, pady=5)
    self.output_var = tk.StringVar()
    ttk.Entry(self.main_frame, textvariable=self.output_var, width=70).grid(row=4, column=0, columnspan=2, padx=5,
                                                                            pady=5, sticky=tk.W + tk.E)
    ttk.Button(self.main_frame, text="浏览...", command=self.select_output).grid(row=4, column=2, padx=5)

    # 日志区域
    ttk.Label(self.main_frame, text="处理日志:").grid(row=5, column=0, sticky=tk.W, pady=10)
    self.log_text = tk.Text(self.main_frame, height=15, width=90)
    self.log_text.grid(row=6, column=0, columnspan=4, padx=5, pady=5, sticky=tk.W + tk.E + tk.N + tk.S)

    log_scrollbar = ttk.Scrollbar(self.main_frame, orient=tk.VERTICAL, command=self.log_text.yview)
    log_scrollbar.grid(row=6, column=4, sticky=tk.N + tk.S)
    self.log_text.config(yscrollcommand=log_scrollbar.set)

    # 按钮区域
    btn_frame2 = ttk.Frame(self.main_frame)
    btn_frame2.grid(row=7, column=0, columnspan=4, pady=20)

    ttk.Button(btn_frame2, text="开始合并", command=self.run_merge).pack(side=tk.LEFT, padx=20)
    ttk.Button(btn_frame2, text="退出", command=root.quit).pack(side=tk.RIGHT, padx=20)

    # 配置网格权重
    self.main_frame.columnconfigure(0, weight=1)
    self.main_frame.rowconfigure(6, weight=1)

    self.log("欢迎使用Malody排行榜数据合并工具")
    self.log("请添加源数据目录并选择输出位置")

  def log(self, message):
    """向日志区域添加消息"""
    self.log_text.insert(tk.END, message + "\n")
    self.log_text.see(tk.END)  # 滚动到底部
    self.root.update_idletasks()  # 更新UI

  def add_source(self):
    """添加源目录"""
    directory = filedialog.askdirectory(title="选择源数据目录")
    if directory:
      # 避免重复添加
      if directory not in self.source_listbox.get(0, tk.END):
        self.source_listbox.insert(tk.END, directory)
        self.log(f"已添加源目录: {directory}")

  def remove_selected(self):
    """移除选中的源目录"""
    selected = self.source_listbox.curselection()
    if selected:
      for index in selected[::-1]:  # 从后往前删除
        directory = self.source_listbox.get(index)
        self.source_listbox.delete(index)
        self.log(f"已移除源目录: {directory}")

  def clear_sources(self):
    """清空所有源目录"""
    self.source_listbox.delete(0, tk.END)
    self.log("已清空所有源目录")

  def select_output(self):
    """选择输出目录"""
    directory = filedialog.askdirectory(title="选择输出目录")
    if directory:
      self.output_var.set(directory)
      self.log(f"已设置输出目录: {directory}")

  def run_merge(self):
    """执行合并操作"""
    sources = self.source_listbox.get(0, tk.END)
    output_dir = self.output_var.get()

    if not sources:
      messagebox.showerror("错误", "请至少添加一个源目录")
      return

    if not output_dir:
      messagebox.showerror("错误", "请选择输出目录")
      return

    # 查找所有源文件
    self.log("\n开始查找源文件...")
    source_files_by_mode = self.find_source_files(sources)

    if not any(source_files_by_mode.values()):
      self.log("错误: 没有找到任何有效数据文件")
      messagebox.showerror("错误", "没有找到任何有效数据文件")
      return

    # 处理每种模式
    total_files = 0
    for mode, source_files in source_files_by_mode.items():
      total_files += len(source_files)
      if source_files:
        self.log(f"\n处理模式 {mode} ({MODE_FILES[mode]}), 找到 {len(source_files)} 个文件")
        self.merge_mode_data(source_files, output_dir, mode)

    self.log("\n所有模式处理完成!")
    messagebox.showinfo("完成", f"数据合并完成!\n共处理 {total_files} 个文件")

  def find_source_files(self, source_dirs):
    """在所有源目录中查找所有模式文件"""
    source_files = {mode: [] for mode in MODE_FILES}

    for source_dir in source_dirs:
      if not os.path.exists(source_dir) or not os.path.isdir(source_dir):
        self.log(f"警告: 源目录不存在或不是目录: {source_dir}")
        continue

      for file_name in os.listdir(source_dir):
        file_path = os.path.join(source_dir, file_name)
        if not os.path.isfile(file_path) or not file_name.endswith(".xlsx"):
          continue

        # 识别文件对应的模式
        for mode, expected_name in MODE_FILES.items():
          if file_name == expected_name:
            source_files[mode].append(file_path)
            self.log(f"找到模式 {mode} 文件: {file_path}")
            break

    return source_files

  def extract_sheet_info(self, sheet_name):
    """从工作表名称中提取模式和时间戳信息"""
    match = TIMESTAMP_PATTERN.match(sheet_name)
    if match:
      mode = int(match.group(1))
      timestamp_str = match.group(2)
      try:
        timestamp = datetime.strptime(timestamp_str, TIMESTAMP_FORMAT)
        return mode, timestamp
      except ValueError:
        return None, None
    return None, None

  def merge_mode_data(self, source_files, output_dir, mode):
    """合并指定模式的数据"""
    all_sheets = []  # 存储所有有效工作表数据

    # 收集所有源文件中的工作表数据
    for file_path in source_files:
      try:
        with pd.ExcelFile(file_path) as xls:
          for sheet_name in xls.sheet_names:
            mode_from_sheet, timestamp = self.extract_sheet_info(sheet_name)
            if mode_from_sheet is None or mode_from_sheet != mode:
              continue

            df = pd.read_excel(xls, sheet_name=sheet_name)
            # 添加唯一标识列用于去重
            df['__sheet_timestamp__'] = timestamp
            df['__sheet_name__'] = sheet_name
            df['__source_file__'] = os.path.basename(file_path)

            all_sheets.append((timestamp, sheet_name, df))
            self.log(f"  添加工作表: {sheet_name} (来自 {file_path})")
      except Exception as e:
        self.log(f"错误: 处理文件 {file_path} 时出错: {e}")

    if not all_sheets:
      self.log(f"模式 {mode} 没有找到有效数据")
      return

    # 按时间戳排序
    all_sheets.sort(key=lambda x: x[0])
    self.log(f"  按时间排序了 {len(all_sheets)} 个工作表")

    # 准备合并数据
    merged_data = []
    seen_hashes = set()

    for timestamp, sheet_name, df in all_sheets:
      # 创建数据哈希用于去重
      try:
        # 尝试创建哈希，忽略临时列
        temp_cols = ['__sheet_timestamp__', '__sheet_name__', '__source_file__']
        data_cols = [col for col in df.columns if col not in temp_cols]

        data_hash = hash(tuple(pd.util.hash_pandas_object(df[data_cols])))

        # 检查是否重复
        if data_hash in seen_hashes:
          self.log(f"  跳过重复数据: {sheet_name} (来自 {df['__source_file__'].iloc[0]})")
          continue

        seen_hashes.add(data_hash)
        merged_data.append((timestamp, sheet_name, df))
        self.log(f"  包含数据: {sheet_name} (来自 {df['__source_file__'].iloc[0]})")
      except Exception as e:
        self.log(f"  错误: 处理工作表 {sheet_name} 时出错: {e}")
        continue

    if not merged_data:
      self.log(f"模式 {mode} 没有唯一数据")
      return

    # 创建输出文件
    output_file = os.path.join(output_dir, MODE_FILES[mode])
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # 保存合并后的数据
    try:
      with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        for idx, (timestamp, sheet_name, df) in enumerate(merged_data):
          # 使用原始工作表名称或创建新名称
          if len(sheet_name) <= 31:
            new_sheet_name = sheet_name
          else:
            new_sheet_name = f"mode_{mode}_{timestamp.strftime(TIMESTAMP_FORMAT)}_{idx + 1}"

          # 清理临时列
          df_clean = df.drop(
            columns=['__sheet_timestamp__', '__sheet_name__', '__source_file__'],
            errors='ignore'
          )

          df_clean.to_excel(writer, sheet_name=new_sheet_name[:31], index=False)

      self.log(f"模式 {mode} 数据已保存到: {output_file}")
    except Exception as e:
      self.log(f"错误: 保存模式 {mode} 数据失败: {e}")


def merge_from_cli(sources, output):
  """命令行模式合并"""
  print(f"开始合并数据...")
  print(f"源目录: {sources}")
  print(f"输出目录: {output}")

  # 查找所有源文件
  source_files_by_mode = find_source_files(sources)

  if not any(source_files_by_mode.values()):
    print("错误: 没有找到任何有效数据文件")
    return

  # 处理每种模式
  total_files = 0
  for mode, source_files in source_files_by_mode.items():
    total_files += len(source_files)
    if source_files:
      print(f"\n处理模式 {mode} ({MODE_FILES[mode]}), 找到 {len(source_files)} 个文件")
      merge_mode_data(source_files, output, mode)

  print("\n所有模式处理完成!")
  print(f"共处理 {total_files} 个文件")


def find_source_files(source_dirs):
  """在所有源目录中查找所有模式文件"""
  source_files = {mode: [] for mode in MODE_FILES}

  for source_dir in source_dirs:
    if not os.path.exists(source_dir) or not os.path.isdir(source_dir):
      print(f"警告: 源目录不存在或不是目录: {source_dir}")
      continue

    for file_name in os.listdir(source_dir):
      file_path = os.path.join(source_dir, file_name)
      if not os.path.isfile(file_path) or not file_name.endswith(".xlsx"):
        continue

      # 识别文件对应的模式
      for mode, expected_name in MODE_FILES.items():
        if file_name == expected_name:
          source_files[mode].append(file_path)
          print(f"找到模式 {mode} 文件: {file_path}")
          break

  return source_files


def extract_sheet_info(sheet_name):
  """从工作表名称中提取模式和时间戳信息"""
  match = TIMESTAMP_PATTERN.match(sheet_name)
  if match:
    mode = int(match.group(1))
    timestamp_str = match.group(2)
    try:
      timestamp = datetime.strptime(timestamp_str, TIMESTAMP_FORMAT)
      return mode, timestamp
    except ValueError:
      return None, None
  return None, None


def merge_mode_data(source_files, output_dir, mode):
  """合并指定模式的数据"""
  all_sheets = []  # 存储所有有效工作表数据

  # 收集所有源文件中的工作表数据
  for file_path in source_files:
    try:
      with pd.ExcelFile(file_path) as xls:
        for sheet_name in xls.sheet_names:
          mode_from_sheet, timestamp = extract_sheet_info(sheet_name)
          if mode_from_sheet is None or mode_from_sheet != mode:
            continue

          df = pd.read_excel(xls, sheet_name=sheet_name)
          # 添加唯一标识列用于去重
          df['__sheet_timestamp__'] = timestamp
          df['__sheet_name__'] = sheet_name
          df['__source_file__'] = os.path.basename(file_path)

          all_sheets.append((timestamp, sheet_name, df))
          print(f"  添加工作表: {sheet_name} (来自 {file_path})")
    except Exception as e:
      print(f"错误: 处理文件 {file_path} 时出错: {e}")

  if not all_sheets:
    print(f"模式 {mode} 没有找到有效数据")
    return

  # 按时间戳排序
  all_sheets.sort(key=lambda x: x[0])
  print(f"  按时间排序了 {len(all_sheets)} 个工作表")

  # 准备合并数据
  merged_data = []
  seen_hashes = set()

  for timestamp, sheet_name, df in all_sheets:
    # 创建数据哈希用于去重
    try:
      # 尝试创建哈希，忽略临时列
      temp_cols = ['__sheet_timestamp__', '__sheet_name__', '__source_file__']
      data_cols = [col for col in df.columns if col not in temp_cols]

      data_hash = hash(tuple(pd.util.hash_pandas_object(df[data_cols])))

      # 检查是否重复
      if data_hash in seen_hashes:
        print(f"  跳过重复数据: {sheet_name} (来自 {df['__source_file__'].iloc[0]})")
        continue

      seen_hashes.add(data_hash)
      merged_data.append((timestamp, sheet_name, df))
      print(f"  包含数据: {sheet_name} (来自 {df['__source_file__'].iloc[0]})")
    except Exception as e:
      print(f"  错误: 处理工作表 {sheet_name} 时出错: {e}")
      continue

  if not merged_data:
    print(f"模式 {mode} 没有唯一数据")
    return

  # 创建输出文件
  output_file = os.path.join(output_dir, MODE_FILES[mode])
  os.makedirs(os.path.dirname(output_file), exist_ok=True)

  # 保存合并后的数据
  try:
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
      for idx, (timestamp, sheet_name, df) in enumerate(merged_data):
        # 使用原始工作表名称或创建新名称
        if len(sheet_name) <= 31:
          new_sheet_name = sheet_name
        else:
          new_sheet_name = f"mode_{mode}_{timestamp.strftime(TIMESTAMP_FORMAT)}_{idx + 1}"

        # 清理临时列
        df_clean = df.drop(
          columns=['__sheet_timestamp__', '__sheet_name__', '__source_file__'],
          errors='ignore'
        )

        df_clean.to_excel(writer, sheet_name=new_sheet_name[:31], index=False)

    print(f"模式 {mode} 数据已保存到: {output_file}")
  except Exception as e:
    print(f"错误: 保存模式 {mode} 数据失败: {e}")


def main():
  parser = argparse.ArgumentParser(description='合并Malody排行榜数据')
  parser.add_argument('--sources', nargs='+',
                      help='包含源数据的目录列表')
  parser.add_argument('--output',
                      help='合并数据的输出目录')
  parser.add_argument('--gui', action='store_true',
                      help='使用图形界面模式')

  args = parser.parse_args()

  if args.gui:
    root = tk.Tk()
    app = MergeApp(root)
    root.mainloop()
  elif args.sources and args.output:
    merge_from_cli(args.sources, args.output)
  else:
    print("请提供源目录和输出目录，或使用 --gui 启动图形界面")
    print("示例:")
    print("  命令行模式: python merge_rankings.py --sources dir1 dir2 --output merged_data")
    print("  图形界面模式: python merge_rankings.py --gui")


if __name__ == "__main__":
  main()
