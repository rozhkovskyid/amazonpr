@echo off
cd /d %~dp0
echo Stopping any existing server on port 9001...
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":9001 " ^| findstr "LISTENING"') do (
    taskkill /F /PID %%a >nul 2>&1
)
timeout /t 1 /nobreak >nul
echo Starting Amazon PR server...
.venv\Scripts\python.exe -m uvicorn main:app --host 0.0.0.0 --port 9001 --reload
pause
