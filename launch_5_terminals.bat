@echo off
title Launch All 5 Activities in Separate Terminals
cls
echo ==============================================================================
echo Launching All 5 Big Data Activities in 5 Dedicated Terminals...
echo ==============================================================================

set WORKDIR=%~dp0

start "Terminal 1: Apache Hive (Sales DW)" powershell -NoExit -Command "cd '%WORKDIR%'; python activity_1_hive/run_hive_analysis.py --plot"
timeout /t 1 >nul

start "Terminal 2: Apache Kafka (Traffic Stream)" powershell -NoExit -Command "cd '%WORKDIR%'; python activity_2_kafka/traffic_kafka_prototype.py --plot"
timeout /t 1 >nul

start "Terminal 3: Apache Flink (Patient Vitals - Port 8081)" powershell -NoExit -Command "cd '%WORKDIR%'; python activity_3_flink/flink_patient_vitals_processor.py"
timeout /t 1 >nul

start "Terminal 4: MongoDB (Catalog & Index Benchmark)" powershell -NoExit -Command "cd '%WORKDIR%'; python activity_4_mongodb/mongodb_ecommerce_catalog.py --plot"
timeout /t 1 >nul

start "Terminal 5: Cloud Analytics (AWS Athena - Port 8050)" powershell -NoExit -Command "cd '%WORKDIR%'; python activity_5_cloud_logs/cloud_log_analytics_athena.py"

echo All 5 terminals launched successfully!
pause
