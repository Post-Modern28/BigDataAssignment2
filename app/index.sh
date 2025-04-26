#!/bin/bash
pip install cqlsh
# Ensure cassandra_lib.zip exists
if [[ ! -f cassandra_lib.zip ]]; then
    mkdir -p cassandra_lib
    pip install cassandra-driver -t cassandra_lib
    (cd cassandra_lib && zip -qr ../cassandra_lib.zip .)
    rm -rf cassandra_lib
fi

# Remove any existing HDFS output directory
hdfs dfs -rm -r -f /tmp/index || true

#echo "mapreduce:"
#ls ./mapreduce/
#ls -l ./mapreduce/mapper1.py
#ls -l ./mapreduce/reducer1.py


echo "Stage 1: Running MapReduce job for term frequency calculation"
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
    echo "✅ MapReduce job Stage 1 completed successfully!"
else
    echo "❌ MapReduce job Stage 1 FAILED."
    echo "Fetching job logs..."
    yarn logs -applicationId $(yarn application -list -appStates FINISHED,FAILED | grep Stage1_TermFrequency | awk '{print $1}') > stage1_yarn_logs.txt
    echo "Logs saved to stage1_yarn_logs.txt"
    exit 1
fi


# Check Stage 1 output
echo "Checking Stage 1 output..."
hdfs dfs -ls /tmp/index
hdfs dfs -cat /tmp/index/part-00000 | head -n 10

# Stage 2: Run the second MapReduce job for document frequency calculation
echo "Stage 2: Running MapReduce job for document frequency calculation"
hdfs dfs -rm -r -f /tmp/index_stage2 || true
hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar \
    -input /tmp/index \
    -output /tmp/index_stage2 \
    -file mapreduce/mapper2.py \
    -file mapreduce/reducer2.py \
    -mapper "python3 mapper2.py" \
    -reducer "python3 reducer2.py"


# Check Stage 2 output
echo "Checking Stage 2 output..."
hdfs dfs -ls /tmp/index_stage2
hdfs dfs -cat /tmp/index_stage2/part-00000 | head -n 10

# Final Step: Insert data into Cassandra
echo "Inserting data into Cassandra..."
python3 app.py

# Verify final output in Cassandra
echo "Verifying data in Cassandra..."
cqlsh -e "USE search_index; SELECT * FROM vocabulary LIMIT 10;"
cqlsh -e "USE search_index; SELECT * FROM document_index LIMIT 10;"

echo "Indexing completed successfully!"



##!/bin/bash
#echo "This script include commands to run mapreduce jobs using hadoop streaming to index documents"
#
#echo "Input file is :"
#echo $1
#
#
#hdfs dfs -ls /
#
#
##!/bin/bash
#
#INPUT=${1:-/index/data}
#TMP=/tmp/index
#OUTPUT1=$TMP/output1
#OUTPUT2=$TMP/output2
#
## Clean previous outputs
#hdfs dfs -rm -r -f $OUTPUT1 $OUTPUT2
#
#echo "Changing permissions..."
#chmod +x ./mapreduce/mapper1.py || echo "Error changing permissions1"
#chmod +x ./mapreduce/reducer1.py || echo "Error changing permissions2"
#
#ls -l ./mapreduce/
#
#echo "Done."
#
#echo "Running pipeline 1..."
## Run pipeline 1
#hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar \
#    -input /user/root/data \
#    -output $OUTPUT1 \
#    -file ./mapreduce/mapper1.py \
#    -file ./mapreduce/reducer1.py\
#    -mapper ./mapper1.py \
#    -reducer ./reducer1.py
#
#
#
#echo "Done."
#
#echo "Running pipeline 2..."
## Run pipeline 2
#hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar \
#    -input $INPUT \
#    -output $OUTPUT2 \
#    -file ./mapreduce/mapper2.py \
#    -file ./mapreduce/reducer2.py\
#    -mapper ./mapreduce/mapper2.py \
#    -reducer ./mapreduce/reducer2.py \
#
#
#
#echo "✅ Indexing complete."
