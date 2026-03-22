#!/bin/bash

echo "Starting container..."
echo "MODE=$MODE"

# if [ "$MODE" = "api" ]; then
#     echo "Running API..."
#     uvicorn src.api:app --host 0.0.0.0 --port 8000

if [ "$MODE" = "job" ]; then
    echo "Triggering Databricks SQL transformations..."
    python src/run_sql_pipeline.py

    # echo "Running bundle job..."
    # databricks bundle deploy
    # databricks bundle run medallion_job

else
    echo "Defaulting to API..."
    uvicorn src.api:app --host 0.0.0.0 --port 8000
fi