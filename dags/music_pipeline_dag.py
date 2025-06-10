import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime
from ingestion.fetch_lastfm import fetch_top_tracks
from ingestion.fetch_spotify import fetch_tracks
from transform.format_lastfm import format_lastfm_tracks
from transform.format_spotify import format_spotify_tracklist
from transform.combine_music_data import create_artist_track_count_metrics,get_hot_track
from index.bulk_import_to_es import import_all_parquet_files

with DAG(
    "music_data_pipeline",
    default_args={
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
},
    catchup=False,
    start_date=datetime(2025,1,1),
    max_active_tasks=4,
    concurrency=8,
    description="Fetch → Format → Combine → Index music data",
) as dag:
    dag.doc_md = """
    Fetch → Format → Combine → Index music data
    """

    # === Ingestion ===
    fetch_lastfm_top_tracks = PythonOperator(
        task_id="fetch_lastfm_top_tracks",
        python_callable=fetch_top_tracks,
    )

    fetch_spotify = PythonOperator(
        task_id="fetch_spotify",
        python_callable=fetch_tracks,
    )

    # === Formatting ===
    format_lastfm_track = PythonOperator(
        task_id="format_lastfm_track",
        python_callable=format_lastfm_tracks,
    )

    format_spotify = PythonOperator(
        task_id="format_spotify",
        python_callable=format_spotify_tracklist,
    )

    artist_track_count = PythonOperator(
        task_id="artist_track_count",
        python_callable=create_artist_track_count_metrics,
    )

    hot_track = PythonOperator(
        task_id="hot_track",
        python_callable=get_hot_track,
    )

    index_to_es = PythonOperator(
        task_id="index_to_elasticsearch",
        python_callable=import_all_parquet_files,
    )

    fetch_lastfm_top_tracks >> format_lastfm_track
    fetch_spotify >> format_spotify

    format_spotify >> artist_track_count
    format_lastfm_track >> hot_track
    format_spotify >> hot_track

    for upstream in [artist_track_count, hot_track]:
        upstream >> index_to_es


