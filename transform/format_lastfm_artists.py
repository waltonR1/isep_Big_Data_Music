from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, to_utc_timestamp
from datetime import datetime
from pyspark.sql.functions import col, expr

def format_lastfm_artists():
    layerLast = "raw"
    layer = "formatted"
    group = "lastfm"
    tableName = "topArtists"
    today = datetime.today().strftime("%Y%m%d")
    filename = "topArtists"

    # 初始化 SparkSession，配置 S3A 连接
    spark = SparkSession.builder \
        .appName("FormatLastfmArtists") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566") \
        .config("spark.hadoop.fs.s3a.access.key", "dummy") \
        .config("spark.hadoop.fs.s3a.secret.key", "dummy") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    # 路径
    input_path = f"s3a://{layerLast}-data-music/{group}/{tableName}/{today}/{filename}.json"
    output_path = f"s3a://{layer}-data-music/{group}/{tableName}/{today}/{filename}.parquet"
    local_output_path = f"../data/{layer}/{group}/{tableName}/{today}/{filename}.parquet"

    # 读取 JSON
    df = spark.read.option("multiLine", "true").json(input_path)

    # 提取字段
    formatted = df.select(
        col("name").alias("artist_name"),
        col("playcount").cast("long"),
        col("listeners").cast("long"),
        col("mbid"),
        col("url").alias("artist_url"),
        expr("filter(image, x -> x.size = 'extralarge')[0]['#text']").alias("cover_image"),
        to_utc_timestamp(current_timestamp(), "UTC").alias("ingestion_time_utc")  # 添加标准 UTC 时间戳
    )

    # 写入 Parquet
    formatted.write.mode("overwrite").parquet(output_path)
    formatted.write.mode("overwrite").parquet(local_output_path)

    print(f"[SUCCESS](Last.fm Top Tracks) Last.fm Top Artists Formatting completed and uploaded to s3 → {output_path}")
    print(f"[SUCCESS](Last.fm Top Tracks) Locally saved → {local_output_path}")

if __name__ == "__main__":
    format_lastfm_artists()