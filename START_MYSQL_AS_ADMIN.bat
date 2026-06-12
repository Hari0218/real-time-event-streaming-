@echo off
REM =============================================================================
REM START_MYSQL_AS_ADMIN.bat
REM Right-click this file and choose "Run as Administrator" to start MySQL Server
REM =============================================================================

title Starting MySQL Server (Admin Required)

net session >NUL 2>&1
if errorlevel 1 (
    echo.
    echo  *** YOU MUST RUN THIS AS ADMINISTRATOR! ***
    echo.
    echo  Right-click this .bat file and select "Run as administrator"
    echo.
    pause
    exit /b 1
)

echo.
echo ============================================================
echo   Starting MySQL Server 8.0
echo ============================================================
echo.

REM Try to install service first (if not already installed)
echo [1/3] Registering MySQL service...
"C:\Program Files\MySQL\MySQL Server 8.0\bin\mysqld.exe" --install MySQL80 --defaults-file="C:\ProgramData\MySQL\MySQL Server 8.0\my.ini" 2>NUL
echo   (Service registration attempted)

echo.
echo [2/3] Starting MySQL service...
net start MySQL80 2>&1
if errorlevel 1 (
    echo   Trying alternate service start...
    "C:\Program Files\MySQL\MySQL Server 8.0\bin\mysqld.exe" --defaults-file="C:\ProgramData\MySQL\MySQL Server 8.0\my.ini" --daemonize 2>NUL
    timeout /t 4 /nobreak >NUL
)

echo.
echo [3/3] Verifying MySQL is running...
"C:\Program Files\MySQL\MySQL Server 8.0\bin\mysqladmin.exe" -u root status 2>&1
if errorlevel 1 (
    echo.
    echo   MySQL may need a password. Try connecting with MySQL Workbench first
    echo   to verify the root password, then update config.py if needed.
    echo.
    echo   Default passwords to try:
    echo     root123  (set in config.py)
    echo     (blank)  - try pressing Enter with no password
) else (
    echo.
    echo   MySQL is RUNNING!
    echo.
    echo   Now create the database (run once):
    echo   "C:\Program Files\MySQL\MySQL Server 8.0\bin\mysql.exe" -u root -e "CREATE DATABASE IF NOT EXISTS ecommerce_streaming CHARACTER SET utf8mb4;"
)

echo.
echo ============================================================
echo   NEXT: Open a new terminal (regular, not admin) and run:
echo     python spark_streaming.py
echo ============================================================
echo.
pause
