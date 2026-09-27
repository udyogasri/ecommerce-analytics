@echo off
echo ===================================================
echo   E-Commerce Data Lakehouse - Local Pipeline Run
echo ===================================================

echo.
echo [1/4] Setting up Spark Environment Variables...
set HADOOP_HOME=C:\hadoop
set SPARK_HOME=
set PYSPARK_PYTHON=%~dp0venv\Scripts\python.exe
set PYSPARK_DRIVER_PYTHON=%~dp0venv\Scripts\python.exe

call venv\Scripts\activate

echo.
echo [2/4] Running Phase 2: Batch Ingestion (Raw -^> Bronze)...
python scripts\run_phase2.py
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Phase 2 failed!
    exit /b %ERRORLEVEL%
)

echo.
echo [3/4] Running Phase 4: Silver Transformations (Bronze -^> Silver)...
python src\etl\silver_transformations.py
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Phase 4 failed!
    exit /b %ERRORLEVEL%
)

echo.
echo [4/4] Running Phase 5: Gold Aggregations (Silver -^> Gold)...
python src\etl\gold_aggregations.py
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Phase 5 failed!
    exit /b %ERRORLEVEL%
)

echo.
echo ===================================================
echo   Pipeline Completed Successfully!
echo   Check the data/local_test/lakehouse directory
echo ===================================================
pause
