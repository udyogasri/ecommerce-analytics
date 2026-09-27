@echo off
echo ===================================================
echo   E-Commerce Data Lakehouse - Start Producer
echo ===================================================

call venv\Scripts\activate
python src\producers\clickstream_producer.py --eps 10
pause
