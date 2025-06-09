from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, expr, round as _round, avg, countDistinct, size, desc
from datetime import datetime

layer_source = "formatted"
layer_target = "usage"

group_lastfm = "lastfm"
group_spotify = "spotify"
group_output = "combined"

today = datetime.today().strftime("%Y%m%d")

def s3_output_path(table_name, file_name):
    return f"s3a://{layer_target}/{group_output}/{table_name}/{today}/{file_name}.parquet"

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


# ── 1. Artist Metrics ───────────────────────────────────────────
def create_artist_metrics(lfa, spt):
    """热门艺人 / 平均流行度 / 受众分布"""
    # 热门艺人按 playcount
    hot = lfa.select("artist_key", "artist_name", "playcount", "listeners")

    # 各艺人平均 Spotify popularity
    avg_pop = spt.groupBy("artist_key").agg(_round(avg("popularity"), 2).alias("avg_popularity"))

    # 受众分布：listeners 与 playcount 比值
    ratio = lfa.withColumn("audience_ratio",_round(col("listeners") / col("playcount"), 4)).select("artist_key", "audience_ratio")

    artist_metrics = hot.join(avg_pop, "artist_key", "left").join(ratio, "artist_key", "left").orderBy(desc("playcount"))

    artist_metrics.write.mode("overwrite").parquet(local_output_path("artist_metrics", "artist_metrics"))


# ── 2. Track Insights ──────────────────────────────────────────
def create_track_insights(lft, spt):
    """热门歌曲榜 / 时长分布字段 / 可播放国家数 / 专辑聚合"""
    spt_sel = (
        spt.select(
            "track_key",
            "artist_key",
            "popularity",
            col("duration_ms").alias("duration_ms_sp"),  # ★ 改名
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

    # a) 全量 track-level 表（含所有衍生字段）
    tracks.write.mode("overwrite").parquet(local_output_path("track_level", "track_level"))

    # b) 热门歌曲榜单
    tracks.orderBy(desc("dual_score")).select("track_name", "artist_name", "dual_score").write.mode("overwrite").parquet(local_output_path("hot_tracks", "hot_tracks"))

    # c) 专辑聚合
    (tracks.groupBy("album_id", "album_name")
     .agg(countDistinct("track_key").alias("track_cnt"),
          _round(avg("popularity"), 2).alias("avg_popularity"))
     .orderBy(desc("track_cnt"))
     .write.mode("overwrite")
     .parquet(local_output_path("album_stats","album_stats")))


# ── 3. Combination Analysis ───────────────────────────────────
def create_combination(lft, spt):
    """Last.fm 播放热度 vs Spotify 流行度"""
    joined = (lft.join(
        spt.select("track_key", "artist_key", "popularity"),
        ["track_key", "artist_key"], "inner")
              .withColumn("playcount_norm", expr("playcount / 1000.0"))
              .withColumn("diff", _round(col("popularity") - col("playcount_norm"), 2))
              )

    joined.write.mode("overwrite").parquet(local_output_path("combination","full_join"))

    # 冷门金曲：播放量高 popularity 低
    joined.filter("playcount_norm > 500 and popularity < 30").orderBy(desc("playcount_norm")).write.mode("overwrite").parquet(local_output_path("combination","hidden_gems"))

    # 流媒体宠儿：popularity 高 播放量低
    joined.filter("popularity > 70 and playcount_norm < 200").orderBy(desc("popularity")).write.mode("overwrite").parquet(local_output_path("combination","streaming_darlings"))


# ── 主程序 ────────────────────────────────────────────────────
if __name__ == "__main__":
    spark = init_spark()

    lft, lfa, spt = load_tables(spark)

    create_artist_metrics(lfa, spt)
    create_track_insights(lft, spt)
    create_combination(lft, spt)

    spark.stop()
    print(f"✅ 数据集已全部写入")
