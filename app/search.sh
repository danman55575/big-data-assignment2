#!/bin/bash
# Run BM25 ranker on YARN
source .venv/bin/activate

# Pack env for YARN if missing
if [ ! -f ".venv.tar.gz" ]; then
    echo "Packing virtual environment..."
    venv-pack -o .venv.tar.gz
fi

export PYSPARK_DRIVER_PYTHON=$(which python)
export PYSPARK_PYTHON=./.venv/bin/python

# Execute
spark-submit \
    --master yarn \
    --archives .venv.tar.gz#.venv \
    query.py "$1"
