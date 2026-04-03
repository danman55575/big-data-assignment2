#!/bin/bash
echo "Stage 3: Storing Index in Cassandra/ScyllaDB"

# Activate virtual environment
source .venv/bin/activate

echo "Fetching index parts from HDFS to local file..."
# Remove old file if it exists
rm -f local_index.txt

# Merge all parts from MapReduce output into one local text file
hdfs dfs -getmerge /indexer/index local_index.txt

echo "Running Python loader..."
# Execute the cassandra-driver script
python3 app.py

echo "Index successfully stored in Database!"