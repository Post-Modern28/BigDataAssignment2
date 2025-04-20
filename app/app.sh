#!/bin/bash
# Start ssh server
service ssh restart 
echo "Starting services..."
# Starting the services
sed -i 's/\r$//' start-services.sh


echo "Creating venv..."
# Creating a virtual environment
python3 -m venv .venv
source .venv/bin/activate

echo "Installing requirements..."
# Install any packages
pip install -r requirements.txt  


echo "Packing venv"
# Package the virtual env.
venv-pack -o .venv.tar.gz

echo "Collecting data"
# Collect data
sed -i 's/\r$//' prepare_data.sh

echo "Done preparing"

# Run the indexer
#bash index.sh data/sample.txt
#
## Run the ranker
#bash search.sh "this is a query!"