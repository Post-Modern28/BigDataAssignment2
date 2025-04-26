#!/bin/bash

service ssh restart
echo "Starting services..."
bash start-services.sh

echo "Creating venv..."
python3 -m venv .venv
source .venv/bin/activate


echo "Installing requirements..."
pip install -r requirements.txt

#echo "Python interpreter path:"
#which python3

echo "Packing venv"
venv-pack -o .venv.tar.gz

#echo "Actual path is: "
#find / -name "hadoop-streaming*.jar" 2>/dev/null

echo "Building Cassandra"
python app.py

echo "Collecting data"
bash prepare_data.sh

echo "Waiting for HDFS to become responsive..."
for i in {1..10}; do
  hdfs dfs -ls / > /dev/null 2>&1
  if [ $? -eq 0 ]; then
    echo "HDFS is responsive!"
    break
  fi
  echo "Retrying HDFS in 3s..."
  sleep 3
done

apt install dos2unix


dos2unix ./mapreduce/mapper1.py
dos2unix ./mapreduce/reducer1.py

head -n 1 ./mapreduce/mapper1.py | cat -v
chmod +x ./mapreduce/mapper1.py
chmod +x ./mapreduce/reducer1.py



echo "Running indexer"

bash index.sh


##
### Run the ranker
##bash search.sh "this is a query!"