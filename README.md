```
big_data_music/
├── dags/                 # Airflow 的 DAG 文件
├── ingestion/            # 数据采集模块（调用 API）
├── transform/            # 格式化 + Spark 转换
├── index/                # Elasticsearch 索引模块
├── utils/                # 工具函数（比如 S3 上传）
├── data/                 # 数据湖存储目录（分层）
│   ├── raw/              # 原始数据（JSON、CSV）
│   ├── formatted/        # 格式化后（Parquet）
│   └── usage/            # 最终输出供分析用
├── .env                  # 环境变量配置（不要提交）
├── .env.example          # 环境变量模板
├── docker-compose.yml    # Docker 服务定义（如 LocalStack / Airflow）
├── requirements.txt      # Python 依赖列表
├── .gitignore            # gitignore
└── README.md             # 项目说明文档

```

##  环境变量配置（.env 文件）
1. 在项目根目录下创建 .env 文件
2. 可参考 .env.example 模板文件，其中包含了所有必需的变量及其说明。

## Docker 启动
1. 清理旧容器及数据库（首次设置建议执行）
```bash
  docker compose down --volumes --remove-orphans
```
2. 初始化（只需一次）
```bash
  docker compose up airflow-init
```
3. 启动服务
```bash
  docker compose up -d
```
4. 创建bucket(只需一次)
```bash
    docker compose exec localstack awslocal s3 mb s3://raw-data-music
```
```bash
    docker compose exec localstack awslocal s3 mb s3://formatted-data-music
```  
```bash
    docker compose exec localstack awslocal s3 mb s3://usage-data-music
```

## 获取 Spotify API 授权信息

1. 登录 [Spotify 开发者平台](https://developer.spotify.com/)
2. 创建一个新的应用(APP)
3. 查看你的 Client ID 和 Client Secret 
4. 在你的项目根目录 .env 文件中添加：

```dotenv
SPOTIFY_CLIENT_ID=你的ClientID
SPOTIFY_CLIENT_SECRET=你的ClientSecret
```

## 获取 Last.fm API 授权信息

1. 登录[Last.fm 开发者 API 平台](https://www.last.fm/api)
2. 创建一个新的应用(APP)
3. 查看你的 api_key
4. 在你的项目根目录 .env 文件中添加：

```dotenv
LASTFM_API_KEY=你的apikey
```


## topTracks_example
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

## topArtists_example
```
topArtists_example.json
└── [ ] 
    ├── name                  # 歌手名称
    ├── playcount             # 所有歌曲总播放次数
    ├── listeners             # 听众人数
    ├── mbid                  # 音乐数据库ID
    ├── url                   # 歌手在 Last.fm 的主页链接
    ├── streamable            # 是否支持在线播放（0/1）
    └── image [ ]             # 多种尺寸的歌手头像图
        ├── #text             # 图片链接
        └── size              # 尺寸：small / medium / large / extralarge / mega


```

## trackLists_example
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