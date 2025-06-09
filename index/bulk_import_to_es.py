from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch, helpers
from datetime import datetime

# ========================
# 配置参数
# ========================
ROOT_DIR = "s3a://usage-data-music/combined"
ES_HOST = "http://localhost:9200"

# 初始化 SparkSession（确保 S3A 支持）
spark = (SparkSession.builder
    .appName("ImportParquetToES")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566")
    .config("spark.hadoop.fs.s3a.access.key", "dummy")
    .config("spark.hadoop.fs.s3a.secret.key", "dummy")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

es = Elasticsearch(hosts=[ES_HOST])

def import_all_parquet_files(root_dir):
    # 遍历所有逻辑表目录
    groups = ["artist_retention_metrics", "artist_track_count_metrics", "hot_tracks"]

    today = datetime.today().strftime("%Y%m%d")

    for index_name in groups:
        path = f"{root_dir}/{index_name}/{today}/{index_name}.parquet"
        print(f"\n[READ] {path}")

        try:
            df = spark.read.parquet(path)
            pandas_df = df.toPandas()
        except Exception as e:
            print(f"[ERROR] Failed to read {index_name}: {e}")
            continue

        # 添加导入时间戳
        pandas_df["import_timestamp"] = datetime.utcnow().isoformat()

        docs = [
            {"_index": index_name, "_source": row}
            for row in pandas_df.to_dict(orient="records")
        ]

        print(f"[INFO] Ready to index {len(docs)} docs → {index_name}")

        try:
            helpers.bulk(es, docs)
            print(f"[SUCCESS] Indexed {len(docs)} docs to {index_name}")
        except Exception as e:
            print(f"[ERROR] Elasticsearch insert failed: {e}")

if __name__ == "__main__":
    import_all_parquet_files(ROOT_DIR)
