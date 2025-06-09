from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, to_utc_timestamp
from datetime import datetime
from pyspark.sql.functions import col, expr

def format_spotify_tracklist():
    layer_source = "raw"
    layer_target = "formatted"
    group = "spotify"
    table_name = "track_lists"
    today = datetime.today().strftime("%Y%m%d")
    file_name = "track_lists"

    # 初始化 SparkSession，配置 S3A 连接
    spark = (SparkSession.builder
        .appName("FormatSpotifyTrackLists")
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
        col("name").alias("track_name"),
        col("id").alias("track_id"),
        col("duration_ms"),
        col("explicit"),
        col("popularity"),
        col("preview_url"),
        col("track_number"),
        col("disc_number"),
        col("external_ids.isrc").alias("isrc"),
        col("external_urls.spotify").alias("track_url"),
        col("uri").alias("track_uri"),
        col("is_playable"),
        col("is_local"),
        col("available_markets"),

        # 主艺人信息：取第一个（可扩展为 explode 多艺人）
        col("artists")[0]["name"].alias("artist_name"),
        col("artists")[0]["id"].alias("artist_id"),
        col("artists")[0]["external_urls"]["spotify"].alias("artist_url"),
        col("artists")[0]["uri"].alias("artist_uri"),

        # 专辑信息
        col("album.name").alias("album_name"),
        col("album.id").alias("album_id"),
        col("album.album_type"),
        col("album.release_date"),
        col("album.release_date_precision"),
        col("album.total_tracks").alias("album_total_tracks"),
        col("album.external_urls.spotify").alias("album_url"),
        col("album.uri").alias("album_uri"),

        # 封面图：专辑封面中最大尺寸的那张
        expr("filter(album.images, x -> x.height >= 300)[0].url").alias("cover_image"),

        to_utc_timestamp(current_timestamp(), "UTC").alias("ingestion_time_utc")  # 添加标准 UTC 时间戳
    )


    # 写入 Parquet
    formatted.write.mode("overwrite").parquet(s3_output_path)
    print(f"[SUCCESS](Spotify Track Lists)  Formatting completed and uploaded to s3 → {s3_output_path}")

    formatted.write.mode("overwrite").parquet(local_output_path)
    print(f"[SUCCESS](Spotify Track Lists) Locally saved → {local_output_path}")

if __name__ == "__main__":
    format_spotify_tracklist()