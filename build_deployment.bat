@echo off
REM SuperNats 28 Points Tracker - Deployment Build Script
REM This creates a standalone executable for Windows

echo ========================================
echo SuperNats 28 Build Script v0.2.0
echo ========================================
echo.

REM Activate virtual environment
echo [1/5] Activating virtual environment...
call .venv\Scripts\activate.bat
if errorlevel 1 (
    echo ERROR: Failed to activate virtual environment
    echo Please run: python -m venv .venv
    pause
    exit /b 1
)

REM Install/upgrade PyInstaller
echo.
echo [2/5] Installing PyInstaller...
python -m pip install --upgrade pyinstaller
if errorlevel 1 (
    echo ERROR: Failed to install PyInstaller
    pause
    exit /b 1
)

REM Clean previous builds
echo.
echo [3/5] Cleaning previous builds...
if exist build rmdir /s /q build
if exist dist rmdir /s /q dist
if exist *.spec del /q *.spec

REM Build the executable
echo.
echo [4/5] Building executable...
echo This may take a few minutes...
pyinstaller --onefile ^
    --name "SuperNats28-UI" ^
    --add-data "service_account.json;." ^
    --hidden-import "sqlalchemy.sql.default_comparator" ^
    --hidden-import "gspread" ^
    --hidden-import "google.auth" ^
    --hidden-import "tkinter" ^
    --collect-all "gspread" ^
    --collect-all "google-auth" ^
    ui.py

if errorlevel 1 (
    echo ERROR: Build failed
    pause
    exit /b 1
)

REM Create deployment package
echo.
echo [5/5] Creating deployment package...
if not exist "deployment" mkdir deployment
copy dist\SuperNats28-UI.exe deployment\
copy .env.template deployment\.env
copy service_account.json deployment\ 2>nul
copy README.md deployment\
copy DEPLOYMENT.md deployment\

echo.
echo ========================================
echo BUILD COMPLETE!
echo ========================================
echo.
echo Executable created: dist\SuperNats28-UI.exe
echo Deployment package: deployment\
echo.
echo Next steps:
echo 1. Copy the 'deployment' folder to your target machine
echo 2. Edit .env file with your Google Sheets credentials
echo 3. Run SuperNats28-UI.exe
echo.
pause
