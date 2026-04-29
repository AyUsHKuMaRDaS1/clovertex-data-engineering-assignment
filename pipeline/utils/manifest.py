import os
import json
import hashlib
from datetime import datetime, timezone

import pandas as pd


def calculate_sha256(file_path):
    sha256 = hashlib.sha256()

    with open(file_path, "rb") as file:
        for chunk in iter(lambda: file.read(4096), b""):
            sha256.update(chunk)

    return sha256.hexdigest()


def read_data_file(file_path):
    if file_path.endswith(".csv"):
        return pd.read_csv(file_path)

    if file_path.endswith(".json"):
        return pd.read_json(file_path)

    if file_path.endswith(".parquet"):
        return pd.read_parquet(file_path)

    return None


def generate_manifest(zone_path):
    manifest = []

    for root, _, files in os.walk(zone_path):
        for file_name in files:
            if file_name == "manifest.json":
                continue

            file_path = os.path.join(root, file_name)

            if not os.path.isfile(file_path):
                continue

            df = read_data_file(file_path)

            if df is None:
                continue

            manifest.append({
                "file_name": file_name,
                "file_path": file_path,
                "row_count": int(len(df)),
                "schema": {
                    column: str(dtype)
                    for column, dtype in df.dtypes.items()
                },
                "processing_timestamp": datetime.now(timezone.utc).isoformat(),
                "sha256_checksum": calculate_sha256(file_path)
            })

    manifest_path = os.path.join(zone_path, "manifest.json")

    with open(manifest_path, "w") as file:
        json.dump(manifest, file, indent=4)

    print(f"Manifest created: {manifest_path}")


def generate_all_manifests():
    base_path = os.path.join(os.getcwd(), "datalake")

    generate_manifest(os.path.join(base_path, "raw"))
    generate_manifest(os.path.join(base_path, "refined"))
    generate_manifest(os.path.join(base_path, "consumption"))