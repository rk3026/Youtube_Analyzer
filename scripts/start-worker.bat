@echo off
cd %SPARK_HOME%\bin
spark-class org.apache.spark.deploy.worker.Worker spark://localhost:7077 --host localhost
pause