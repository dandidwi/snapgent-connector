@echo off
setlocal enabledelayedexpansion
chcp 65001 >nul
title Snapgent Bridge
cd /d "%~dp0"

if not exist "%~dp0logs" mkdir "%~dp0logs" >nul 2>nul
set "LOGFILE=%~dp0logs\start.log"

if not exist "%~dp0bridge.py" (
    echo.
    echo   ERROR: bridge.py not found next to start.bat.
    echo   Run start.bat from the project folder.
    echo.
    pause
    exit /b 1
)

echo.
echo   Configuring VS Code MCP and detecting Roblox Studio...
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0setup-config.ps1"
if errorlevel 1 (
    echo.
    echo   ERROR: Could not configure the VS Code MCP connection.
    echo   Check the message above, then run start.bat again.
    echo.
    pause
    exit /b 1
)

if exist "%~dp0start-effectcraft.ps1" (
    echo.
    echo   Checking EffectCraft control channel...
    powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0start-effectcraft.ps1"
)

set "ZS_BRIDGE_PORT=37613"
set "ZS_API_PORT=11434"

REM Free only our dedicated local proxy ports (never touch original 17613).
for %%p in (%ZS_BRIDGE_PORT%) do (
    set "OLDPID="
    for /f "tokens=5" %%a in ('netstat -aon ^| findstr :%%p ^| findstr LISTENING 2^>nul') do (
        set "OLDPID=%%a"
    )
    if defined OLDPID (
        echo   Replacing previous proxy bridge ^(pid !OLDPID!^) on port %%p...
        taskkill /F /T /PID !OLDPID! >nul 2>nul
        timeout /t 1 /nobreak >nul
    )
)

echo.
echo   +------------------------------------------------------+
echo   ^| Snapgent Local AI Bridge is now running.             ^|
echo   ^| WS Bridge  : ws://127.0.0.1:%ZS_BRIDGE_PORT%                  ^|
echo   ^| OpenAI API : http://127.0.0.1:%ZS_API_PORT%/v1               ^|
echo   ^| Minimise it freely - closing it stops the bridge.    ^|
echo   +------------------------------------------------------+
echo.

python "%~dp0bridge.py"
set "BRIDGE_EXIT=%errorlevel%"

echo.
if not "%BRIDGE_EXIT%"=="0" (
    echo   Bridge stopped with ERROR code %BRIDGE_EXIT%.
) else (
    echo   Bridge stopped normally.
)
echo   Press any key to close.
pause >nul
exit /b 0
