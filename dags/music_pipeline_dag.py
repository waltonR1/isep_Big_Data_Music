from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from ingestion.fetch_lastfm import fetch_top_tracks,fetch_top_artists
from ingestion.fetch_spotify import fetch_tracks
from transform.format_lastfm import format_lastfm_artists,format_lastfm_tracks
from transform.format_spotify import format_spotify_tracklist
from transform.combine_music_data import run_all_analyses
from index.bulk_import_to_es import import_all_parquet_files

with DAG(
    dag_id="music_data_pipeline",
    default_args={
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
},
    catchup=False,
    start_date=datetime(2025,1,1),
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

    fetch_lastfm_top_artists = PythonOperator(
        task_id="fetch_lastfm_top_artists",
        python_callable=fetch_top_artists,
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

    format_lastfm_artist = PythonOperator(
        task_id="format_lastfm_artist",
        python_callable=format_lastfm_artists,
    )

    format_spotify = PythonOperator(
        task_id="format_spotify",
        python_callable=format_spotify_tracklist,
    )

    combine_data = PythonOperator(
        task_id="combine_data",
        python_callable=run_all_analyses,
    )

    index_to_es = PythonOperator(
        task_id="index_to_elasticsearch",
        python_callable=import_all_parquet_files,
    )

    fetch_lastfm_top_tracks >> format_lastfm_track
    fetch_lastfm_top_artists >> format_lastfm_artist
    fetch_spotify >> format_spotify

    [format_lastfm_track, format_lastfm_artist, format_spotify] >> combine_data
    combine_data >> index_to_es

