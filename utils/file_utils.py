from utils.upload_file_to_s3 import upload_to_s3
import json
import os
from datetime import datetime
import pandas as pd

pd.set_option('future.no_silent_downcasting', True)

def save_to_file(data, layer, group, tableName, filename, upload=False):
    today = datetime.today().strftime('%Y%m%d')
    current_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.abspath(os.path.join(current_dir, "..", "data"))
    path = os.path.join(root_dir, layer, group, tableName, today)
    os.makedirs(path, exist_ok=True)
    file_path = os.path.join(path, filename)

    if isinstance(data, pd.DataFrame):
        data = data.to_dict(orient="records")

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"[SUCCESS] {len(data)} records exported → {file_path}")

    if upload:
        bucket = f"{layer}-data-music"
        s3_key = f"{group}/{tableName}/{today}/{filename}"
        upload_to_s3(file_path, bucket, s3_key)