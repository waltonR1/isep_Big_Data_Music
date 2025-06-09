from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, expr, round as _round, avg, countDistinct, size, desc, current_timestamp, to_utc_timestamp,sha2,concat_ws
from datetime import datetime

layer_source = "formatted"
layer_target = "usage"

group_lastfm = "lastfm"
group_spotify = "spotify"
group_output = "combined"

today = datetime.today().strftime("%Y%m%d")

def s3_output_path(table_name, file_name):
    return f"s3a://{layer_target}-data-music/{group_output}/{table_name}/{today}/{file_name}.parquet"

def local_output_path(table_name, file_name):
    return f"../data/{layer_target}/{group_output}/{table_name}/{today}/{file_name}.parquet"

def init_spark():
    return (SparkSession.builder
            .appName("CombineMusicData")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566")
            .config("spark.hadoop.fs.s3a.access.key", "dummy")
            .config("spark.hadoop.fs.s3a.secret.key", "dummy")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .getOrCreate())


def load_tables(spark):
    """读取 3 张 Parquet 并统一生成 join key"""
    path_lastfm_tracks = f"s3a://{layer_source}-data-music/{group_lastfm}/topTracks/{today}/topTracks.parquet"
    path_lastfm_artists = f"s3a://{layer_source}-data-music/{group_lastfm}/topArtists/{today}/topArtists.parquet"
    path_spotify_tracks = f"s3a://{layer_source}-data-music/{group_spotify}/trackLists/{today}/trackLists.parquet"

    lft = spark.read.parquet(path_lastfm_tracks).alias("lft")
    lfa = spark.read.parquet(path_lastfm_artists).alias("lfa")
    spt = spark.read.parquet(path_spotify_tracks).alias("spt")

    lft = lft.withColumn("track_key", lower(trim(col("track_name")))).withColumn("artist_key", lower(trim(col("artist_name"))))

    lfa = lfa.withColumn("artist_key", lower(trim(col("artist_name"))))

    spt = spt.withColumn("track_key", lower(trim(col("track_name")))).withColumn("artist_key", lower(trim(col("artist_name"))))

    return lft, lfa, spt


def create_artist_retention_metrics(lfa):
    hot = lfa.select("artist_key", "artist_name", "playcount", "listeners")

    # 计算复听率
    retention = lfa.withColumn("repeat_rate",_round(col("listeners") / col("playcount"), 4)).select("artist_key", "repeat_rate")

    artist_retention_metrics = hot.join(retention, "artist_key", "left")

    artist_retention_metrics = artist_retention_metrics.withColumn(
        "processed_at_utc", to_utc_timestamp(current_timestamp(), "UTC")
    )

    artist_retention_metrics = artist_retention_metrics.orderBy(desc("repeat_rate"))

    artist_retention_metrics.write.mode("overwrite").parquet(local_output_path("artist_retention_metrics", "artist_retention_metrics"))
    artist_retention_metrics.write.mode("overwrite").parquet(s3_output_path("artist_retention_metrics", "artist_retention_metrics"))

def create_artist_track_count_metrics(spt):
    base = spt.select("artist_id", "artist_name", "track_id")

    track_count = (
        base.groupBy("artist_id", "artist_name")
            .agg(countDistinct("track_id").alias("track_count"))
    )

    artist_track_metrics = track_count.withColumn(
        "processed_at_utc", to_utc_timestamp(current_timestamp(), "UTC")
    )

    artist_track_metrics = artist_track_metrics.orderBy(desc("track_count"))

    artist_track_metrics.write.mode("overwrite").parquet(local_output_path("artist_track_count_metrics", "artist_track_count_metrics"))
    artist_track_metrics.write.mode("overwrite").parquet(s3_output_path("artist_track_count_metrics", "artist_track_count_metrics"))

def get_hot_track(lft, spt):
    spt_sel = (
        spt.select(
            "track_key",
            "artist_key",
            "popularity",
            col("duration_ms").alias("duration_ms_sp"),
            "album_name",
            "album_id",
            "available_markets",
        )
    )

    tracks = (
        lft.join(spt_sel, ["track_key", "artist_key"], "inner")
        .withColumn("dual_score", col("playcount") * col("popularity"))
        .withColumn("duration_min", _round(col("duration_ms_sp") / 60_000, 2))
        .withColumn("country_cnt", size(col("available_markets")))
    )

    # 热门歌曲
    hot_tracks = tracks.orderBy(desc("dual_score")).select("track_name", "artist_name", "dual_score","playcount","listeners","country_cnt")
    hot_tracks.write.mode("overwrite").parquet(local_output_path("hot_tracks", "hot_tracks"))
    hot_tracks.write.mode("overwrite").parquet(s3_output_path("hot_tracks", "hot_tracks"))

def run_all_analyses():
    spark = init_spark()
    lft, lfa, spt = load_tables(spark)
    create_artist_retention_metrics(lfa)
    create_artist_track_count_metrics(spt)
    get_hot_track(lft, spt)
    spark.stop()
    print("[SUCCESS] All analysis tasks completed")


if __name__ == "__main__":
    run_all_analyses()
