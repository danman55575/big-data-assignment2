#!/bin/bash

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python) 
unset PYSPARK_PYTHON

echo "Clear old data in HDFS if exists"
hdfs dfs -rm -r -f /a.parquet
hdfs dfs -rm -r -f /data
hdfs dfs -rm -r -f /input/data

echo "Load z.parquet to the root of HDFS"
hdfs dfs -put -f z.parquet /

echo "Run PySpark job"
spark-submit prepare_data.py

echo "Copy generated files to HDFS /data"
hdfs dfs -mkdir -p /data
hdfs dfs -put -f data/*.txt /data/

echo "Check created directories in HDFS"
echo "Content of first 5 files in /data"
hdfs dfs -ls /data | head -n 6

echo "Content of /input/data:"
hdfs dfs -ls /input/data
hdfs dfs -cat /input/data/part-00000 | head -n 2

echo "Data preparation successfully done!"
