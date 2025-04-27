#!/bin/bash

pip install cqlsh

# Ensure cassandra_lib.zip exists
if [[ ! -f cassandra_lib.zip ]]; then
    mkdir -p cassandra_lib
    pip install cassandra-driver -t cassandra_lib
    (cd cassandra_lib && zip -qr ../cassandra_lib.zip .)
    rm -rf cassandra_lib
fi




echo "Stage 1: Running MapReduce job for term frequency calculation"
hdfs dfs -rm -r -f /tmp/index || true

hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar \
    -D mapreduce.job.name="Stage1_TermFrequency" \
    -input /user/root/index/data \
    -output /tmp/index \
    -file ./mapreduce/mapper1.py \
    -file ./mapreduce/reducer1.py \
    -mapper "python3 mapper1.py" \
    -reducer "python3 reducer1.py" \
    -cmdenv PYTHONIOENCODING=utf8

if [ $? -eq 0 ]; then
    echo "✅  MapReduce job Stage 1 completed successfully!"
else
    echo "❌  MapReduce job Stage 1 FAILED."
    exit 1
fi

echo "Check output of stage1..."
hdfs dfs -ls /tmp/index
echo "Head of part 0:"
hdfs dfs -cat /tmp/index/part-00000 | head -n 10


echo "Stage 2: Calculating metrics"
hdfs dfs -rm -r -f /tmp/index_stage2 || true

hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar \
    -D mapreduce.job.name="Stage2_IDF" \
    -input /tmp/index \
    -output /tmp/index_stage2 \
    -file mapreduce/reducer2.py \
    -mapper cat \
    -reducer "python3 reducer2.py"


echo "Check output of stage2..."
hdfs dfs -ls /tmp/index_stage2
echo "Head of part 0:"
hdfs dfs -cat /tmp/index_stage2/part-00000 | head -n 10

hdfs dfs -getmerge /tmp/index_stage2 output_stage2.txt


echo "Inserting data into Cassandra..."
python3 app.py


echo "Verifying data in Cassandra..."

cqlsh cassandra-server -e "USE search_index; SELECT * FROM vocabulary LIMIT 10;"
cqlsh cassandra-server -e "USE search_index; SELECT * FROM document_stats LIMIT 10;"
cqlsh cassandra-server -e "USE search_index; SELECT * FROM inverted_index LIMIT 10;"



echo "Indexing completed successfully!"



