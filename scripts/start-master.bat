@echo off
cd %SPARK_HOME%\bin
spark-class org.apache.spark.deploy.master.Master --host localhost
pause