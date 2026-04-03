#!/bin/bash

# Default input path if no argument is provided
INPUT_PATH=${1:-/input/data}
OUTPUT_PATH="/indexer/index"

echo "Starting MapReduce Indexing Pipeline"
echo "Input Path: $INPUT_PATH"
echo "Output Path: $OUTPUT_PATH"

# Remove existing output directory to prevent Hadoop errors
hdfs dfs -rm -r -f $OUTPUT_PATH

# Run Hadoop Streaming job
mapred streaming \
    -input $INPUT_PATH \
    -output $OUTPUT_PATH \
    -mapper "python3 mapper1.py" \
    -reducer "python3 reducer1.py" \
    -file mapreduce/mapper1.py \
    -file mapreduce/reducer1.py

echo "MapReduce Pipeline Completed"

# Display a preview of the generated index
echo "Preview of the output (/indexer/index/part-00000):"
hdfs dfs -cat $OUTPUT_PATH/part-00000 | head -n 15
