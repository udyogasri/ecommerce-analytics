@echo off
echo ===================================================
echo   E-Commerce Data Lakehouse - Start Consumer
echo ===================================================

set HADOOP_HOME=C:\hadoop
set SPARK_HOME=
set PYSPARK_PYTHON=%~dp0venv\Scripts\python.exe
set PYSPARK_DRIVER_PYTHON=%~dp0venv\Scripts\python.exe

call venv\Scripts\activate
python src\consumers\clickstream_consumer.py
pause
