from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, to_utc_timestamp
from datetime import datetime
from pyspark.sql.functions import col, expr

def format_lastfm_artists():
    layer_source = "raw"
    layer_target = "formatted"
    group = "lastfm"
    table_name = "top_artists"
    today = datetime.today().strftime("%Y%m%d")
    file_name = "top_artists"

    # 初始化 SparkSession，配置 S3A 连接
    spark = (SparkSession.builder
        .appName("FormatLastfmArtists")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566")
        .config("spark.hadoop.fs.s3a.access.key", "dummy")
        .config("spark.hadoop.fs.s3a.secret.key", "dummy")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate())

    # 路径
    input_path = f"s3a://{layer_source}-data-music/{group}/{table_name}/{today}/{file_name}.json"
    s3_output_path = f"s3a://{layer_target}-data-music/{group}/{table_name}/{today}/{file_name}.parquet"
    local_output_path = f"../data/{layer_target}/{group}/{table_name}/{today}/{file_name}.parquet"

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
    formatted.write.mode("overwrite").parquet(s3_output_path)
    print(f"[SUCCESS](Last.fm Top Tracks) Last.fm Top Artists Formatting completed and uploaded to s3 → {s3_output_path}")

    formatted.write.mode("overwrite").parquet(local_output_path)
    print(f"[SUCCESS](Last.fm Top Tracks) Locally saved → {local_output_path}")

def format_lastfm_tracks():
    layer_source = "raw"
    layer_target = "formatted"
    group = "lastfm"
    table_name = "top_tracks"
    today = datetime.today().strftime("%Y%m%d")
    file_name = "top_tracks"

    # 初始化 SparkSession，配置 S3A 连接
    spark = (SparkSession.builder
        .appName("FormatLastfmTracks")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566")
        .config("spark.hadoop.fs.s3a.access.key", "dummy")
        .config("spark.hadoop.fs.s3a.secret.key", "dummy")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate())

    # 路径
    input_path = f"s3a://{layer_source}-data-music/{group}/{table_name}/{today}/{file_name}.json"
    s3_output_path = f"s3a://{layer_target}-data-music/{group}/{table_name}/{today}/{file_name}.parquet"
    local_output_path = f"../data/{layer_target}/{group}/{table_name}/{today}/{file_name}.parquet"

    # 读取 JSON
    df = spark.read.option("multiLine", "true").json(input_path)

    # 提取字段
    formatted = df.select(
        col("name").alias("track_name"),  # 歌曲名
        col("artist.name").alias("artist_name"),  # 艺人名
        col("playcount").cast("long"),  # 播放次数
        col("listeners").cast("long"),  # 听众数
        col("duration").cast("long").alias("duration_ms"),  # 歌曲时长（毫秒）
        col("mbid").alias("track_mbid"),  # 歌曲唯一 ID（MusicBrainz）
        col("artist.mbid").alias("artist_mbid"),  # 艺人唯一 ID
        col("url").alias("track_url"),  # 歌曲页面
        col("artist.url").alias("artist_url"),  # 艺人页面
        expr("filter(image, x -> x.size = 'extralarge')[0]['#text']").alias("cover_image"),  # 封面图
        to_utc_timestamp(current_timestamp(), "UTC").alias("ingestion_time_utc")  # 添加标准 UTC 时间戳
    )

    # 写入 Parquet
    formatted.write.mode("overwrite").parquet(s3_output_path)
    print(f"[SUCCESS](Last.fm Top Tracks)  Formatting completed and uploaded to s3 → {s3_output_path}")

    formatted.write.mode("overwrite").parquet(local_output_path)
    print(f"[SUCCESS](Last.fm Top Tracks) Locally saved → {local_output_path}")
if __name__ == "__main__":
    format_lastfm_artists()
    format_lastfm_tracks()