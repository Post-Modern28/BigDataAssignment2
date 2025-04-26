#!/bin/bash
# This will run only by the master node

#!/bin/bash
echo "Starting HDFS daemons..."
$HADOOP_HOME/sbin/start-dfs.sh

# Wait for Namenode to be available
echo "Waiting for HDFS to be ready..."
for i in {1..10}; do
  hdfs dfsadmin -report > /dev/null 2>&1
  if [ $? -eq 0 ]; then
    echo "HDFS is up!"
    break
  fi
  echo "Retrying in 3s..."
  sleep 3
done

# Continue only if HDFS is up
hdfs dfsadmin -safemode leave

echo "Starting YARN..."
$HADOOP_HOME/sbin/start-yarn.sh

echo "Starting MapReduce HistoryServer..."
mapred --daemon start historyserver

# track process IDs of services
jps -lm

# subtool to perform administrator functions on HDFS
# outputs a brief report on the overall HDFS filesystem
hdfs dfsadmin -report

# If namenode in safemode then leave it
hdfs dfsadmin -safemode leave

# create a directory for spark apps in HDFS
hdfs dfs -mkdir -p /apps/spark/jars
hdfs dfs -chmod 744 /apps/spark/jars


# Copy all jars to HDFS
hdfs dfs -put /usr/local/spark/jars/* /apps/spark/jars/
hdfs dfs -chmod +rx /apps/spark/jars/


# print version of Scala of Spark
scala -version

# track process IDs of services
jps -lm

# Create a directory for root user on HDFS
hdfs dfs -mkdir -p /user/root

