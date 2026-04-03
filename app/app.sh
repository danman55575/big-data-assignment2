#!/bin/bash
# Start ssh server
service ssh restart 

# Starting the services
bash start-services.sh

echo "Creating and upgrading virtual environment"
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt  

echo "Packaging the virtual env for YARN"
venv-pack -o .venv.tar.gz

echo "Stage 1: Collect & Prepare Data"
bash prepare_data.sh

echo "Stage 2: Run the Indexer (MapReduce + ScyllaDB)"
bash index.sh

echo "Stage 3: Run the Ranker (BM25)"
bash search.sh "Sport in Zambia"
bash search.sh "music and art"

echo " All Pipeline Steps Completed Successfully!"
sleep infinity
