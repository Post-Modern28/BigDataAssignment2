#!/bin/bash

set -e  # exit in case of error

source .venv/bin/activate
export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON


mkdir -p index/data data


hdfs dfs -put a.parquet / && \
spark-submit \
--conf spark.sql.parquet.columnarReaderBatchSize=512 \
--conf spark.sql.parquet.enableVectorizedReader=true \
--driver-memory 4g \
--executor-memory 4g \
prepare_data.py


#echo "Check mapper locally:"
#
#hdfs dfs -cat /user/root/index/data/part-* | ./mapreduce/mapper1.py



echo "Putting data to hdfs"
hdfs dfs -put data /user/root/ || echo "Failed to put files in /user/root/"
hdfs dfs -put index/data /user/root/ || echo "Failed to put index files in /user/root/"
#hdfs dfs -ls /user/root || echo "Failed to list files"
#hdfs dfs -ls /user/root/index/data || echo "Failed to list files in index/data"
#hdfs dfs -cat /user/root/index/data/part-00000* | head -n 10


echo "Done data preparation!"
