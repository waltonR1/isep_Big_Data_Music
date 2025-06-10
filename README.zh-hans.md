# Big Data Music Project

## 项目概述
Big Data Music 是一个端到端的大数据处理项目，结合多个音乐类 API（如 Last.fm 和 Spotify），实现数据采集、清洗转换、分析与可视化。
项目目标是构建一个基于数据湖架构的数据处理流程，通过 Apache Airflow 编排、Spark 转换和 ELK（Elasticsearch + Kibana）分析展示，模拟真实的分布式大数据平台。

项目采用分布式架构设计，包含以下主要组件：

- **Airflow**：负责调度整个数据工作流（运行在本地）
- **Apache Spark**：负责数据清洗与转换（运行在本地）
- **Elasticsearch + Kibana（ELK）**：用于建立索引与可视化展示（运行在本地）
- **LocalStack（Docker 部署）**：用于本地模拟 AWS S3，用作分层数据湖的存储媒介

所有模块通过共享 `.env` 文件及统一目录结构协同工作，构建了完整的数据采集 → 处理 → 分析的链路

## 项目结构
```plaintext
big_data_music/
├── dags/                 # Airflow 的 DAG 文件
├── ingestion/            # 数据采集模块（调用 API）
├── transform/            # 格式化 + Spark 转换
├── index/                # Elasticsearch 索引模块
├── utils/                # 工具函数（比如 S3 上传）
├── data/                 # 数据湖存储目录（分层）(实际并没有使用，而是使用的localstack，此处仅仅是为了展示数据)
│   ├── raw/              # 原始数据（JSON、CSV）
│   ├── formatted/        # 格式化后（Parquet）
│   └── usage/            # 最终输出供分析用
├── .env                  # 环境变量配置（不要提交）
├── .env.example          # 环境变量模板
├── docker-compose.yml    # Docker 服务定义（LocalStack）
├── .gitignore            # gitignore
└── README.md             # 项目说明文档

```

## 主要模块及功能
### 1.dages 目录
包含 Airflow 的 DAG（Directed Acyclic Graph）文件，用于定义数据处理的工作流。

### 2. ingestion 目录
负责从不同的音乐 API 采集数据。
* fetch_lastfm.py：从 Last.fm API 获取热门歌曲数据。
* fetch_spotify.py：从 Spotify API 获取歌曲数据。

### 3. transform 目录
对采集到的原始数据进行格式化和转换，使用 Spark 进行数据处理。
* format_lastfm.py：对 Last.fm 的热门歌曲数据进行格式化，将其转换为 Parquet 格式并上传到 S3。
* format_spotify.py：对 Spotify 的歌曲数据进行格式化。
* combine_music_data.py：合并不同来源的音乐数据，生成用于分析的指标。

### 4. index 目录
将处理后的数据索引到 Elasticsearch 中，方便后续的搜索和分析。
* bulk_import_to_es.py：将 S3 上的 Parquet 文件批量导入到 Elasticsearch 中。

### 5. utils 目录
包含一些工具函数，如文件保存和上传到 S3 等。
* file_utils.py：提供了将数据保存到本地文件并上传到 S3 的功能。
* upload_file_to_s3.py：用于将文件上传到 S3 存储桶。

### 6. data 目录
作为数据湖的存储目录，分为原始数据、格式化后数据和最终使用数据三个层次，但实际使用的是 LocalStack 进行数据存储。
* raw/：存储从 API 采集到的原始数据，格式为 JSON 或 CSV。
* formatted/：存储格式化后的数据，格式为 Parquet。
* usage/：存储最终用于分析的数据。

## 环境配置
### 环境变量配置（.env 文件）
复制 .env.example 为 .env，并填写你的 API Key

### 获取 Spotify API 授权信息
1. 登录 [Spotify 开发者平台](https://developer.spotify.com/)
2. 创建一个新的应用(APP)
3. 查看你的 Client ID 和 Client Secret 
4. 在你的项目根目录 .env 文件中添加：
```dotenv
SPOTIFY_CLIENT_ID=你的ClientID
SPOTIFY_CLIENT_SECRET=你的ClientSecret
```
### 获取 Last.fm API 授权信息
1. 登录[Last.fm 开发者 API 平台](https://www.last.fm/api)
2. 创建一个新的应用(APP)
3. 查看你的 api_key
4. 在你的项目根目录 .env 文件中添加：
```dotenv
LASTFM_API_KEY=你的apikey
```

## 启动步骤
### 清理旧容器及数据库（首次设置建议执行）
```bash
  docker compose down --volumes --remove-orphans
```
### 启动服务(LocalStack)
```bash
  docker compose up -d
```
### 创建 S3 存储桶（只需一次）
```bash
    docker compose exec localstack awslocal s3 mb s3://raw-data-music
```
```bash
    docker compose exec localstack awslocal s3 mb s3://formatted-data-music
```  
```bash
    docker compose exec localstack awslocal s3 mb s3://usage-data-music
```
### 启动本地组件
#### Airflow（推荐使用虚拟环境或 conda 环境中运行）
```bash
    # 1. 进入虚拟环境目录（可选）
    cd ~/airflow_venv
    # 2. 激活虚拟环境（必须）
    source bin/activate
    # 3. 可选：确保 Airflow 命令能找到（有些系统需要）
    export PATH=/home/walton/.local/bin/:$PATH
    # 4. 设置 DAG 路径为你项目中的 dags 文件夹（当前会话中生效）
    export AIRFLOW__CORE__DAGS_FOLDER=/home/usr/Big_Data_Music/dags
    # 5. 启动 Airflow（第一次可以用 standalone，也可以使用 webserver + scheduler）
    airflow standalone
```
#### Spark
若为windows,请先配置好。
#### Elasticsearch & Kibana（本地运行）
请先启动本地安装的 Elasticsearch 与 Kibana 服务，确保：
* Elasticsearch 运行在 http://localhost:9200
* Kibana 运行在 http://localhost:5601

## 原始数据示例
### topTracks_example
```
topTracks_example.json
└── [ ]
    ├── name                  # 歌曲名称
    ├── duration              # 歌曲时长
    ├── playcount             # 播放次数
    ├── listeners             # 听众人数
    ├── mbid                  # 音乐数据库ID（可为空）
    ├── url                   # 歌曲页面链接
    ├── streamable
    │   ├── #text             # 是否可播放
    │   └── fulltrack         # 是否是完整曲目
    ├── artist
    │   ├── name              # 歌手名
    │   ├── mbid              # 歌手ID
    │   └── url               # 歌手页面链接
    └── image [ ]             # 不同尺寸的封面图
        ├── #text             # 封面图链接
        └── size              # 尺寸：small/medium/large/extralarge


```
### trackLists_example
```
trackLists_example.json
└── [ ]                       # 曲目列表（数组）
    ├── album                 # 专辑信息
    │   ├── album_type        # 专辑类型（album / single / compilation）
    │   ├── artists [ ]       # 专辑艺术家列表
    │   │   ├── external_urls # 外部链接
    │   │   │   └── spotify   # Spotify 链接
    │   │   ├── href          # API 端点
    │   │   ├── id            # 艺术家唯一 ID
    │   │   ├── name          # 艺术家名称
    │   │   ├── type          # 对象类型（artist）
    │   │   └── uri           # Spotify URI
    │   ├── available_markets [ ]  # 可播放地区（ISO-3166 国家码）
    │   ├── external_urls
    │   │   └── spotify       # 专辑在 Spotify 的链接
    │   ├── href              # 专辑 API 端点
    │   ├── id                # 专辑 ID
    │   ├── images [ ]        # 专辑封面列表
    │   │   ├── height        # 像素高度
    │   │   ├── width         # 像素宽度
    │   │   └── url           # 图片链接
    │   ├── is_playable       # 是否可播放（布尔）
    │   ├── name              # 专辑名称
    │   ├── release_date      # 发行日期
    │   ├── release_date_precision # 日期精度（year / month / day）
    │   ├── total_tracks      # 专辑曲目总数
    │   ├── type              # 对象类型（album）
    │   └── uri               # 专辑 URI
    ├── artists [ ]           # 曲目艺术家列表
    │   ├── external_urls
    │   │   └── spotify       # 艺术家 Spotify 链接
    │   ├── href              # 艺术家 API 端点
    │   ├── id                # 艺术家 ID
    │   ├── name              # 艺术家名称
    │   ├── type              # 对象类型（artist）
    │   └── uri               # 艺术家 URI
    ├── available_markets [ ] # 单曲可播放地区
    ├── disc_number           # 所属碟序号
    ├── duration_ms           # 时长（毫秒）
    ├── explicit              # 是否含露骨内容（布尔）
    ├── external_ids
    │   └── isrc              # 国际标准录音码 ISRC
    ├── external_urls
    │   └── spotify           # 曲目 Spotify 链接
    ├── href                  # 曲目 API 端点
    ├── id                    # 曲目 ID
    ├── is_local              # 是否本地曲目（布尔）
    ├── is_playable           # 是否可播放（布尔）
    ├── name                  # 曲目名称
    ├── popularity            # 流行度（0–100）
    ├── preview_url           # 试听片段链接
    ├── track_number          # 专辑中的曲目序号
    ├── type                  # 对象类型（track）
    └── uri                   # 曲目 URI

```


### 查看 LocalStack 数据
可以使用以下命令查看 LocalStack 中 S3 存储桶的内容：
```bash
    docker compose exec localstack awslocal s3 ls s3://raw-data-music --recursive

    docker compose exec localstack awslocal s3 ls s3://formatted-data-music --recursive

    docker compose exec localstack awslocal s3 ls s3://usage-data-music --recursive
```