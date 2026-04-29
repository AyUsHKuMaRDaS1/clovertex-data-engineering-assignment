import os
import shutil


def copy_raw_data():
    source = os.path.join(os.getcwd(), "data")
    target = os.path.join(os.getcwd(), "datalake", "raw")

    os.makedirs(target, exist_ok=True)

    for item in os.listdir(source):
        source_path = os.path.join(source, item)
        target_path = os.path.join(target, item)

        if os.path.isfile(source_path):
            shutil.copy2(source_path, target_path)

    os.makedirs(os.path.join(os.getcwd(), "datalake", "consumption"), exist_ok=True)

    print("Raw data copied to datalake/raw")