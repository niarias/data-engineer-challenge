import pandas as pd
from pyarrow import parquet as pq
from scripts.helpers.utils import load_json_file
from scripts.strategies.read import get_strategy
import os
import uuid


def procesar_archivo(params):
    # Load configuration
    config = load_json_file(params['path'])
    
    # Load data
    strategy = get_strategy(config['metadata']['type'])
    df = strategy.load_data(config)


    # Aplying transformations
    if 'additional_data' in config['metadata']:
        for col, valor in config['metadata']['additional_data'].items():
            df[col] = valor
    
    
    
    
    final_path = config['final_path']
    full_path = f"{final_path}/{uuid.uuid4()}.parquet"
    
    # Check if file already exists
    if os.path.exists(full_path):
        raise ValueError(f"File already exists: {full_path}")

    # Save data
    if 'partition_by' in config:
        df.to_parquet(full_path, partition_cols=[config['partition_by']])
    else:
        df.to_parquet(full_path)
    

