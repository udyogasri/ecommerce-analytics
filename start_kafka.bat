@echo off
echo ==========================================
echo    Starting Apache Kafka (KRaft mode)
echo ==========================================

cd /d "%~dp0kafka"
set KAFKA_HEAP_OPTS=-Xmx1G -Xms1G

:: Check if storage is already formatted (meta.properties exists)
if exist "C:\tmp\kraft-combined-logs\meta.properties" goto :start_server

echo Formatting Kafka KRaft storage...
for /f "tokens=*" %%i in ('.\bin\windows\kafka-storage.bat random-uuid') do set KAFKA_CLUSTER_ID=%%i
call .\bin\windows\kafka-storage.bat format -t %KAFKA_CLUSTER_ID% -c .\config\kraft\server.properties

:start_server
echo.
echo Starting Kafka server...
call .\bin\windows\kafka-server-start.bat .\config\kraft\server.properties
