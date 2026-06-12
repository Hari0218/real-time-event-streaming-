@echo off
REM =============================================================================
REM start_kafka_wsl.bat
REM Installs Java 17 + Apache Kafka in Ubuntu WSL, then starts Zookeeper + Kafka
REM Run this from the project directory — keep the window OPEN while the pipeline runs
REM =============================================================================

title Starting Kafka in WSL Ubuntu

echo.
echo ============================================================
echo   Apache Kafka Setup in Ubuntu WSL
echo ============================================================
echo.

REM Check WSL Ubuntu is available
wsl bash -c "echo WSL_OK" 2>NUL
if errorlevel 1 (
    echo   ERROR: WSL Ubuntu not found or not running.
    echo   Enable WSL: Run in PowerShell as Admin: wsl --install
    pause
    exit /b 1
)
echo   [OK] Ubuntu WSL is available.
echo.

REM ---------------------------------------------------------------
REM Step 1 — Install Java 17 (required for Kafka)
REM ---------------------------------------------------------------
echo [1/4] Checking Java in WSL...
wsl bash -c "java -version 2>&1 | head -1"
wsl bash -c "java -version 2>&1 | grep -q 'version' && echo 'JAVA_OK' || echo 'JAVA_MISSING'" > %TEMP%\java_check.txt
findstr "JAVA_OK" %TEMP%\java_check.txt >NUL 2>&1
if errorlevel 1 (
    echo   Java not found. Installing OpenJDK 17 (may take 1-2 min)...
    wsl bash -c "sudo apt-get update -qq && sudo apt-get install -y openjdk-17-jdk-headless 2>&1 | tail -3"
    echo   Java installed.
) else (
    echo   Java already installed.
)
echo.

REM ---------------------------------------------------------------
REM Step 2 — Download Kafka (if not already present)
REM ---------------------------------------------------------------
echo [2/4] Checking Kafka installation in WSL...
wsl bash -c "[ -d ~/kafka ] && echo 'KAFKA_OK' || echo 'KAFKA_MISSING'" > %TEMP%\kafka_check.txt
findstr "KAFKA_OK" %TEMP%\kafka_check.txt >NUL 2>&1
if errorlevel 1 (
    echo   Kafka not found. Downloading Kafka 3.7.0 (~100MB)...
    wsl bash -c "cd ~ && wget -q --show-progress https://archive.apache.org/dist/kafka/3.7.0/kafka_2.12-3.7.0.tgz -O kafka.tgz 2>&1"
    if errorlevel 1 (
        echo   Primary mirror failed. Trying Apache CDN...
        wsl bash -c "cd ~ && curl -L -o kafka.tgz https://archive.apache.org/dist/kafka/3.7.0/kafka_2.12-3.7.0.tgz"
    )
    echo   Extracting Kafka...
    wsl bash -c "cd ~ && tar -xzf kafka.tgz && mv kafka_2.12-3.7.0 kafka && rm kafka.tgz && echo 'Kafka ready at ~/kafka'"
) else (
    echo   Kafka already installed.
)
echo.

REM ---------------------------------------------------------------
REM Step 3 — Kill any existing Kafka/ZK processes
REM ---------------------------------------------------------------
echo [3/4] Stopping any existing Kafka/Zookeeper processes...
wsl bash -c "pkill -f 'kafka.Kafka' 2>/dev/null; pkill -f 'zookeeper' 2>/dev/null; sleep 2; echo 'Clean.'"
echo.

REM ---------------------------------------------------------------
REM Step 4 — Start Zookeeper then Kafka
REM ---------------------------------------------------------------
echo [4/4] Starting Zookeeper...
wsl bash -c "~/kafka/bin/zookeeper-server-start.sh ~/kafka/config/zookeeper.properties > /tmp/zookeeper.log 2>&1 &"
echo   Waiting 8 seconds for Zookeeper to initialize...
timeout /t 8 /nobreak >NUL

echo   Starting Kafka Broker...
wsl bash -c "~/kafka/bin/kafka-server-start.sh ~/kafka/config/server.properties > /tmp/kafka.log 2>&1 &"
echo   Waiting 10 seconds for Kafka to initialize...
timeout /t 10 /nobreak >NUL

REM Create the topic
echo   Creating topic 'ecommerce_clickstream'...
wsl bash -c "~/kafka/bin/kafka-topics.sh --create --if-not-exists --topic ecommerce_clickstream --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 2>&1"

REM Verify
echo.
echo   Verifying Kafka topics:
wsl bash -c "~/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092 2>&1"

echo.
echo ============================================================
echo   Kafka is RUNNING on localhost:9092
echo   Zookeeper is on localhost:2181
echo   Topic: ecommerce_clickstream (3 partitions)
echo.
echo   Keep this window OPEN while the pipeline runs!
echo.
echo   NEXT STEPS:
echo     1. Start MySQL: RIGHT-CLICK START_MYSQL_AS_ADMIN.bat ^> Run as admin
echo     2. Terminal 1:  python spark_streaming.py
echo     3. Terminal 2:  python kafka_producer.py
echo ============================================================
echo.
echo   (Press Ctrl+C to stop Kafka when done)
echo.
pause
