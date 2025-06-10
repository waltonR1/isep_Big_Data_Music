from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch, helpers
from datetime import datetime

def import_all_parquet_files():
    spark = (SparkSession.builder
             .appName("ImportParquetToES")
             .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
             .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566")
             .config("spark.hadoop.fs.s3a.access.key", "dummy")
             .config("spark.hadoop.fs.s3a.secret.key", "dummy")
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
             .getOrCreate()
             )

    es = Elasticsearch(hosts=["http://localhost:9200"], basic_auth=('elastic', 'bm1tNqK2'))
    # 遍历所有逻辑表目录
    groups = ["artist_track_count_metrics", "hot_tracks"]

    today = datetime.today().strftime("%Y%m%d")

    for index_name in groups:
        path = f"s3a://usage-data-music/combined/{index_name}/{today}/{index_name}.parquet"
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
    import_all_parquet_files()
