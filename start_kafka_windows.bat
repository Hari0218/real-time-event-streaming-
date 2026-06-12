@echo off
REM =============================================================================
REM start_kafka_windows.bat
REM Downloads (if needed) and starts Apache Kafka + Zookeeper on Windows
REM Requires: Java 11+ (Java 25 is already installed on this machine)
REM Run this FIRST, keep the window open while the pipeline runs
REM =============================================================================

title Starting Apache Kafka on Windows

set KAFKA_DIR=C:\Users\Asus\kafka
set KAFKA_TGZ=C:\Users\Asus\kafka.tgz
set KAFKA_URL=https://archive.apache.org/dist/kafka/3.7.0/kafka_2.12-3.7.0.tgz
set JAVA_HOME=C:\Program Files\Java\jdk-25.0.3

echo.
echo ============================================================
echo   Apache Kafka Startup for Windows
echo   Java: %JAVA_HOME%
echo ============================================================
echo.

REM ------------------------------------------------------------------
REM Check Java
REM ------------------------------------------------------------------
echo [1/5] Checking Java...
"%JAVA_HOME%\bin\java.exe" -version 2>&1
if errorlevel 1 (
    echo   ERROR: Java not found at %JAVA_HOME%
    echo   Java 25 should be at: C:\Program Files\Java\jdk-25.0.3
    pause
    exit /b 1
)
echo   Java OK.
echo.

REM ------------------------------------------------------------------
REM Download Kafka if not present
REM ------------------------------------------------------------------
echo [2/5] Checking Kafka installation...
if exist "%KAFKA_DIR%\bin\windows\kafka-server-start.bat" (
    echo   Kafka already installed at %KAFKA_DIR%
) else (
    if not exist "%KAFKA_TGZ%" (
        echo   Downloading Kafka 3.7.0 (~100MB)...
        powershell -Command "Invoke-WebRequest -Uri '%KAFKA_URL%' -OutFile '%KAFKA_TGZ%' -UseBasicParsing"
        if errorlevel 1 (
            echo   Download failed. Check internet connection.
            pause
            exit /b 1
        )
    )
    echo   Extracting Kafka (this may take a moment)...
    powershell -Command "cd 'C:\Users\Asus'; tar -xzf kafka.tgz; Rename-Item 'kafka_2.12-3.7.0' 'kafka'"
    del "%KAFKA_TGZ%" 2>NUL
    echo   Kafka installed at %KAFKA_DIR%
)
echo.

REM ------------------------------------------------------------------
REM Kill any existing Kafka/ZK processes
REM ------------------------------------------------------------------
echo [3/5] Stopping any existing Kafka/Zookeeper...
taskkill /F /IM "java.exe" /FI "WINDOWTITLE eq Zookeeper*" 2>NUL
taskkill /F /IM "java.exe" /FI "WINDOWTITLE eq Kafka*" 2>NUL
timeout /t 2 /nobreak >NUL
echo   Done.
echo.

REM ------------------------------------------------------------------
REM Start Zookeeper in a new window
REM ------------------------------------------------------------------
echo [4/5] Starting Zookeeper...
set JAVA_HOME=C:\Program Files\Java\jdk-25.0.3
start "Zookeeper" cmd /k "set JAVA_HOME=C:\Program Files\Java\jdk-25.0.3 && C:\Users\Asus\kafka\bin\windows\zookeeper-server-start.bat C:\Users\Asus\kafka\config\zookeeper.properties"
echo   Waiting 10 seconds for Zookeeper to start...
timeout /t 10 /nobreak >NUL

REM ------------------------------------------------------------------
REM Start Kafka Broker in a new window
REM ------------------------------------------------------------------
echo [5/5] Starting Kafka Broker...
start "Kafka Broker" cmd /k "set JAVA_HOME=C:\Program Files\Java\jdk-25.0.3 && C:\Users\Asus\kafka\bin\windows\kafka-server-start.bat C:\Users\Asus\kafka\config\server.properties"
echo   Waiting 12 seconds for Kafka to start...
timeout /t 12 /nobreak >NUL

REM ------------------------------------------------------------------
REM Create topic
REM ------------------------------------------------------------------
echo   Creating Kafka topic 'ecommerce_clickstream'...
set JAVA_HOME=C:\Program Files\Java\jdk-25.0.3
C:\Users\Asus\kafka\bin\windows\kafka-topics.bat --create --if-not-exists --topic ecommerce_clickstream --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 2>&1

echo.
echo   Listing topics:
C:\Users\Asus\kafka\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092 2>&1

echo.
echo ============================================================
echo   Kafka is RUNNING!
echo   Zookeeper : localhost:2181
echo   Kafka     : localhost:9092
echo   Topic     : ecommerce_clickstream (3 partitions)
echo.
echo   Two new windows opened for Zookeeper + Kafka -- keep them open!
echo.
echo   NEXT STEPS:
echo     1. RIGHT-CLICK START_MYSQL_AS_ADMIN.bat ^> Run as administrator
echo     2. Terminal 1: python spark_streaming.py
echo     3. Terminal 2: python kafka_producer.py
echo ============================================================
echo.
pause
