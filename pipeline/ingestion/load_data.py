"""Simple loader for raw dataset files used by the pipeline."""

import pandas as pd
import os


def load_data():
    """Load supported raw files from the data directory into pandas dataframes."""
    data = {}

    
    base_path = os.getcwd()
    data_path = os.path.join(base_path, "data")

    print(f"Reading from: {data_path}")

    for file in os.listdir(data_path):
        file_path = os.path.join(data_path, file)

        try:
            if file.endswith(".csv"):
                df = pd.read_csv(file_path)

            elif file.endswith(".json"):
                df = pd.read_json(file_path)

            elif file.endswith(".parquet"):
                df = pd.read_parquet(file_path)

            else:
                continue

            key = file.split(".")[0]
            data[key] = df

            print(f"Loaded {file} -> {df.shape}")

        except Exception as e:
            print(f"Error reading {file}: {e}")

    return data