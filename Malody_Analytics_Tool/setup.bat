@echo off
echo Installing dependencies...
pip install -r requirements.txt

echo Building resources...
python build.py

echo Setup completed. You can now run the application with:
echo python main.py
pause
