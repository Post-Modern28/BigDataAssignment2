#!/bin/bash

service ssh restart
echo "Starting services..."
bash start-services.sh

echo "Creating virtual environment for Python..."
python3 -m venv .venv
source .venv/bin/activate


echo "Installing python libraries..."
pip install -r requirements.txt


echo "Packing venv"
venv-pack -o .venv.tar.gz


echo "Collecting data..."
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
dos2unix ./mapreduce/mapper2.py
dos2unix ./mapreduce/reducer2.py

chmod +x ./mapreduce/mapper1.py
chmod +x ./mapreduce/reducer1.py
chmod +x ./mapreduce/mapper2.py
chmod +x ./mapreduce/reducer2.py


echo "Running indexer"

bash index.sh

## Run the ranker
echo "Running search"
bash search.sh "Bangladeshi politician and former Mayor of Chittagong City Corporation"
echo "Finished."
tail -f /dev/null