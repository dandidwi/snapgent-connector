@echo off
setlocal enabledelayedexpansion
chcp 65001 >nul
title Snapgent Local AI Bridge
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

REM Detect Python 3.9+ without requiring any external pip packages
set "PY_CMD="
for %%p in (py python python3) do (
    where %%p >nul 2>nul
    if !errorlevel! equ 0 (
        if "%%p"=="py" (
            py -3 -c "import sys; sys.exit(0 if sys.version_info >= (3, 9) else 1)" >nul 2>nul
            if !errorlevel! equ 0 if not defined PY_CMD set "PY_CMD=py -3"
        ) else (
            %%p -c "import sys; sys.exit(0 if sys.version_info >= (3, 9) else 1)" >nul 2>nul
            if !errorlevel! equ 0 if not defined PY_CMD set "PY_CMD=%%p"
        )
    )
)

if not defined PY_CMD (
    echo.
    echo   ERROR: Python 3.9 or newer was not found on PATH.
    echo   Install Python 3 from https://www.python.org/downloads/ and check "Add Python to PATH".
    echo.
    pause
    exit /b 1
)

if exist "%~dp0setup-config.ps1" (
    echo.
    echo   Configuring local MCP servers ^(if available^)...
    powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0setup-config.ps1" >nul 2>nul
)

if exist "%~dp0start-effectcraft.ps1" (
    powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0start-effectcraft.ps1" >nul 2>nul
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
echo   ^| Snapgent Local AI Bridge ^(Zero-Dependency^)          ^|
echo   ^| WS Bridge  : ws://127.0.0.1:%ZS_BRIDGE_PORT%                  ^|
echo   ^| OpenAI API : http://127.0.0.1:%ZS_API_PORT%/v1               ^|
echo   ^| Ollama API : http://127.0.0.1:%ZS_API_PORT%                  ^|
echo   ^| No .exe required - Pure Python Standard Library      ^|
echo   ^| Minimize freely - closing stops the bridge.          ^|
echo   +------------------------------------------------------+
echo.

%PY_CMD% "%~dp0bridge.py"
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
