# debug_settings.py
import sys
import os
from PyQt5.QtCore import QSettings


def check_settings():
  settings = QSettings("MalodyAnalytics", "MalodyAnalyticsTool")

  print(f"Settings file: {settings.fileName()}")
  print(f"Settings path: {os.path.abspath(settings.fileName())}")

  print("\nAll settings:")
  for key in settings.allKeys():
    value = settings.value(key)
    print(f"{key}: {value}")

  print(f"\nCurrent language setting: {settings.value('language', 'Not set')}")


if __name__ == "__main__":
  check_settings()
