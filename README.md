# Big Data Music Project

## Project Overview

Big Data Music is an end-to-end big data processing project that integrates multiple music APIs (such as Last.fm and Spotify) to achieve data collection, cleansing, transformation, analysis, and visualization.
The project aims to build a data processing pipeline based on a data lake architecture, orchestrated by Apache Airflow, transformed by Spark, and analyzed and visualized using ELK (Elasticsearch + Kibana), simulating a real-world distributed big data platform.

The project adopts a distributed architecture design and includes the following key components:

* **Airflow**: Responsible for orchestrating the entire data workflow (runs locally)
* **Apache Spark**: Responsible for data cleansing and transformation (runs locally)
* **Elasticsearch + Kibana (ELK)**: Used for indexing and visualization (runs locally)
* **LocalStack (Docker deployment)**: Used to simulate AWS S3 locally, serving as the storage medium for the layered data lake

All modules work together via a shared `.env` file and unified directory structure, building a complete chain from data collection → processing → analysis.

## Project Structure

```plaintext
big_data_music/
├── dags/                 # DAG files for Airflow
├── ingestion/            # Data ingestion module (API calls)
├── transform/            # Formatting + Spark transformation
├── index/                # Elasticsearch indexing module
├── utils/                # Utility functions (e.g., S3 upload)
├── data/                 # Data lake storage directory (layered) 
│   │                     # (Not actually used; This is for display only)
│   ├── raw/              # Raw data (JSON, CSV)
│   ├── formatted/        # Formatted data (Parquet)
│   └── usage/            # Final output for analysis
├── .env                  # Environment variable configuration (do not submit)
├── .env.example          # Environment variable template
├── docker-compose.yml    # Docker service definition (LocalStack)
├── .gitignore            # gitignore
└── README.md             # Project documentation
```

## Main Modules and Functions

### 1. dags Directory

Contains DAG (Directed Acyclic Graph) files for Airflow to define the data processing workflow.

### 2. ingestion Directory

Responsible for collecting data from various music APIs.

* fetch\_lastfm.py: Fetches top track data from the Last.fm API.
* fetch\_spotify.py: Fetches track data from the Spotify API.

### 3. transform Directory

Formats and transforms the collected raw data using Spark for processing.

* format\_lastfm.py: Formats Last.fm top tracks and converts them to Parquet format for S3 upload.
* format\_spotify.py: Formats Spotify track data.
* combine\_music\_data.py: Merges music data from different sources and generates metrics for analysis.

### 4. index Directory

Indexes the processed data into Elasticsearch for subsequent search and analysis.

* bulk\_import\_to\_es.py: Batch imports Parquet files from S3 into Elasticsearch.

### 5. utils Directory

Contains utility functions such as file saving and S3 uploading.

* file\_utils.py: Provides functions to save data to local files and upload to S3.
* upload\_file\_to\_s3.py: Used to upload files to S3 buckets.

### 6. data Directory

Serves as the storage directory for the data lake, divided into raw, formatted, and usage layers. In practice, LocalStack is used for storage.

* raw/: Stores raw data collected from APIs in JSON or CSV format.
* formatted/: Stores formatted data in Parquet format.
* usage/: Stores data used for final analysis.

## Environment Configuration

### Environment Variable Configuration (.env File)

Copy `.env.example` to `.env` and fill in your API Key.

### Get Spotify API Credentials

1. Log in to the [Spotify Developer Platform](https://developer.spotify.com/)
2. Create a new app
3. View your Client ID and Client Secret
4. Add the following to the `.env` file in your project root:

```dotenv
SPOTIFY_CLIENT_ID=yourClientID
SPOTIFY_CLIENT_SECRET=yourClientSecret
```

### Get Last.fm API Credentials

1. Log in to the [Last.fm Developer API Platform](https://www.last.fm/api)
2. Create a new app
3. View your api\_key
4. Add the following to the `.env` file in your project root:

```dotenv
LASTFM_API_KEY=yourApiKey
```

## Startup Steps

### Clean Up Old Containers and Databases (Recommended for First Setup)

```bash
  docker compose down --volumes --remove-orphans
```

### Start Services (LocalStack)

```bash
  docker compose up -d
```

### Create S3 Buckets (Only Needed Once)

```bash
  docker compose exec localstack awslocal s3 mb s3://raw-data-music
```

```bash
  docker compose exec localstack awslocal s3 mb s3://formatted-data-music
```

```bash
  docker compose exec localstack awslocal s3 mb s3://usage-data-music
```

### Start Local Components

#### Airflow (Recommended to Run in Virtual Environment or Conda)

```bash
    # 1. Enter virtual environment directory (optional)
    cd ~/airflow_venv
    # 2. Activate virtual environment (required)
    source bin/activate
    # 3. Optional: Ensure Airflow commands can be found (needed on some systems)
    export PATH=/home/walton/.local/bin/:$PATH
    # 4. Set DAG path to your project's dags folder (valid for current session)
    export AIRFLOW__CORE__DAGS_FOLDER=/home/usr/Big_Data_Music/dags
    # 5. Start Airflow (use standalone for first run, or use webserver + scheduler)
    airflow standalone
```

#### Spark

For Windows, make sure Spark is properly configured beforehand.

#### Elasticsearch & Kibana (Local Run)

Ensure that your locally installed Elasticsearch and Kibana services are started:

* Elasticsearch running at [http://localhost:9200](http://localhost:9200)
* Kibana running at [http://localhost:5601](http://localhost:5601)

## Sample Raw Data

### topTracks\_example

```
topTracks_example.json
└── [ ]
    ├── name                  # Track name
    ├── duration              # Duration of track
    ├── playcount             # Number of plays
    ├── listeners             # Number of listeners
    ├── mbid                  # MusicBrainz ID (optional)
    ├── url                   # Track page link
    ├── streamable
    │   ├── #text             # Whether streamable
    │   └── fulltrack         # Whether it's a full track
    ├── artist
    │   ├── name              # Artist name
    │   ├── mbid              # Artist ID
    │   └── url               # Artist page link
    └── image [ ]             # Cover images in different sizes
        ├── #text             # Image URL
        └── size              # Size: small/medium/large/extralarge
```

### trackLists\_example

```
trackLists_example.json
└── [ ]                       # Track list (array)
    ├── album                 # Album info
    │   ├── album_type        # Type (album / single / compilation)
    │   ├── artists [ ]       # Album artist list
    │   │   ├── external_urls # External links
    │   │   │   └── spotify   # Spotify link
    │   │   ├── href          # API endpoint
    │   │   ├── id            # Artist unique ID
    │   │   ├── name          # Artist name
    │   │   ├── type          # Object type (artist)
    │   │   └── uri           # Spotify URI
    │   ├── available_markets [ ]  # Available countries (ISO-3166 codes)
    │   ├── external_urls
    │   │   └── spotify       # Spotify album link
    │   ├── href              # Album API endpoint
    │   ├── id                # Album ID
    │   ├── images [ ]        # Album cover images
    │   │   ├── height        # Image height in px
    │   │   ├── width         # Image width in px
    │   │   └── url           # Image URL
    │   ├── is_playable       # Whether playable (bool)
    │   ├── name              # Album name
    │   ├── release_date      # Release date
    │   ├── release_date_precision # Precision (year/month/day)
    │   ├── total_tracks      # Total number of tracks
    │   ├── type              # Object type (album)
    │   └── uri               # Album URI
    ├── artists [ ]           # Track artist list
    │   ├── external_urls
    │   │   └── spotify       # Spotify artist link
    │   ├── href              # Artist API endpoint
    │   ├── id                # Artist ID
    │   ├── name              # Artist name
    │   ├── type              # Object type (artist)
    │   └── uri               # Artist URI
    ├── available_markets [ ] # Markets where track is available
    ├── disc_number           # Disc number
    ├── duration_ms           # Duration in milliseconds
    ├── explicit              # Explicit content (bool)
    ├── external_ids
    │   └── isrc              # International Standard Recording Code (ISRC)
    ├── external_urls
    │   └── spotify           # Spotify track link
    ├── href                  # Track API endpoint
    ├── id                    # Track ID
    ├── is_local              # Whether local track (bool)
    ├── is_playable           # Whether playable (bool)
    ├── name                  # Track name
    ├── popularity            # Popularity score (0–100)
    ├── preview_url           # Preview clip URL
    ├── track_number          # Track number in album
    ├── type                  # Object type (track)
    └── uri                   # Track URI
```

### View LocalStack Data

You can view the contents of S3 buckets in LocalStack using the following commands:

```bash
    docker compose exec localstack awslocal s3 ls s3://raw-data-music --recursive

    docker compose exec localstack awslocal s3 ls s3://formatted-data-music --recursive

    docker compose exec localstack awslocal s3 ls s3://usage-data-music --recursive
```
