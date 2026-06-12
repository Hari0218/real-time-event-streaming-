@echo off
REM =============================================================================
REM setup_and_run.bat
REM One-click setup for the Real-Time E-Commerce Streaming Pipeline
REM Run this from the project directory
REM =============================================================================

title E-Commerce Streaming Pipeline Setup

echo.
echo ============================================================
echo   Real-Time E-Commerce Event Streaming Pipeline  ^|  2026
echo   Python + Apache Kafka + PySpark + MySQL
echo ============================================================
echo.

REM ------------------------------------------------------------------
REM Step 1 — Check Python
REM ------------------------------------------------------------------
echo [Step 1/5] Checking Python installation...
python --version 2>NUL
if errorlevel 1 (
    echo   ERROR: Python not found. Install Python 3.9+ from https://python.org
    pause
    exit /b 1
)
echo   Python OK
echo.

REM ------------------------------------------------------------------
REM Step 2 — Install Python dependencies
REM ------------------------------------------------------------------
echo [Step 2/5] Installing Python dependencies...
pip install -r requirements.txt --quiet
if errorlevel 1 (
    echo   ERROR: pip install failed. Check requirements.txt
    pause
    exit /b 1
)
echo   Dependencies installed.
echo.

REM ------------------------------------------------------------------
REM Step 3 — Start Docker infrastructure
REM ------------------------------------------------------------------
echo [Step 3/5] Starting Docker containers (Kafka + MySQL)...
REM Try modern docker compose first, fall back to docker-compose
docker compose version >NUL 2>&1
if errorlevel 1 (
    docker-compose up -d
) else (
    docker compose up -d
)
if errorlevel 1 (
    echo   ERROR: docker compose failed. Is Docker Desktop running?
    echo   Install Docker Desktop from https://www.docker.com/products/docker-desktop
    pause
    exit /b 1
)
echo   Containers started. Waiting 45 seconds for services to be healthy...
timeout /t 45 /nobreak > NUL
echo   Infrastructure ready.
echo.

REM ------------------------------------------------------------------
REM Step 4 — Initialize MySQL schema
REM ------------------------------------------------------------------
echo [Step 4/5] Initializing MySQL schema...
python -c "from mysql_sink import initialize_schema; initialize_schema('schema.sql')"
if errorlevel 1 (
    echo   WARNING: Schema init may have partially failed. Retrying in 10s...
    timeout /t 10 /nobreak > NUL
    python -c "from mysql_sink import initialize_schema; initialize_schema('schema.sql')"
)
echo   Schema ready.
echo.

REM ------------------------------------------------------------------
REM Step 5 — Run preflight verification
REM ------------------------------------------------------------------
echo [Step 5/5] Running preflight verification...
python verify_setup.py
echo.

REM ------------------------------------------------------------------
REM Launch Instructions
REM ------------------------------------------------------------------
echo ============================================================
echo   SETUP COMPLETE! Follow these steps to run the pipeline:
echo.
echo   1. Open a NEW terminal and run:
echo         python spark_streaming.py
echo      (Wait for "Pipeline is LIVE" message)
echo.
echo   2. Open ANOTHER terminal and run:
echo         python kafka_producer.py
echo.
echo   3. Watch events flow in Kafka UI:
echo         http://localhost:8080
echo.
echo   4. Query results in MySQL:
echo         mysql -h 127.0.0.1 -u root -proot123 ecommerce_streaming
echo ============================================================
echo.
pause
