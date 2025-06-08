```
big_data_music/
├── dags/                 # Airflow 的 DAG 文件
│   └── music_pipeline.py
├── ingestion/            # 数据采集模块（调用 API）
│   ├── fetch_spotify.py
│   └── fetch_trends.py
├── transform/            # 格式化 + Spark 转换
│   ├── format_data.py
│   └── combine_data.py
├── index/                # Elasticsearch 索引模块
│   └── index_elastic.py
├── utils/                # 工具函数（比如 S3 上传）
│   └── s3_utils.py
├── raw/                  # 原始数据（JSON、CSV）
├── formatted/            # 格式化后（Parquet）
├── final/                # 整合结果（用于展示）
├── requirements.txt      # 项目依赖
└── README.md             # 项目说明
```

##  环境变量配置（.env 文件）
1. 在项目根目录下创建 .env 文件
2. 可参考 .env.example 模板文件，其中包含了所有必需的变量及其说明。


1. 清理旧容器及数据库（首次设置建议执行）
```bash
  docker compose down --volumes --remove-orphans
```
2. 初始化（只需一次）
```bash
  docker compose up --abort-on-container-exit airflow-init
```
3. 启动服务
```bash
  docker compose up -d
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