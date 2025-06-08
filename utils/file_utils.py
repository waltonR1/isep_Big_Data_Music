import json
import os
from datetime import datetime

def save_to_file(data, layer, group, tableName, filename):
    today = datetime.today().strftime('%Y%m%d')
    current_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.abspath(os.path.join(current_dir, ".."))
    path = os.path.join(root_dir, layer, group, tableName, today)
    os.makedirs(path, exist_ok=True)
    file_path = os.path.join(path, filename)
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"[SUCCESS] {len(data)} records exported → {file_path}")