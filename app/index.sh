#!/bin/bash
echo "Master Indexing Workflow"

# Default input path
INPUT_PATH=${1:-/input/data}

# Run MapReduce Pipeline
bash create_index.sh $INPUT_PATH

# Store results to Database
bash store_index.sh

echo "All Indexing Operations Completed Successfully!"
