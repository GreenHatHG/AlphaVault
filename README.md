# AlphaVault

面向投资大佬观点追踪的采集与分析系统。  
当前支持：微博 RSS 增量抓取、LLM 投资相关性分析、断言抽取、本地 SQLite 入库、Turso 云端同步。

## 功能概览
- RSS 增量抓取（去重）
- LLM 分析与观点抽取（写入 `posts` + `assertions`）
- 本地数据库 `workdb.sqlite` 作为任务队列与原始数据仓库
- 同步到 Turso 云端数据库（幂等 upsert）

## 目录结构（核心脚本）
- `weibo_rss_incremental.py`：RSS 增量抓取 + 异步 LLM 分析
- `ingest_weibo_to_workdb.py`：从 CSV 批量入库 + LLM 分析
- `init_workdb.py`：初始化本地 SQLite schema
- `sync_workdb_to_turso.py`：本地 → Turso 同步（增量、幂等）
- `requirements.txt`：依赖清单

## 环境要求
- Python 3.10+
- SQLite
- 可用的 LLM 代理或 API

## 安装
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## 本地数据库初始化
```bash
python3 init_workdb.py
```

## RSS 增量抓取 + LLM 分析（推荐）
推荐用环境变量配 AI（Docker/supervisord 也用这一套）：
```bash
export RSS_URLS="https://rsshub.xxx/weibo/user/3962719063?key=YOUR_KEY,https://rsshub.xxx/weibo/user/123?key=YOUR_KEY"
export AI_API_STYLE="litellm"
export AI_API_TYPE="openai"
export AI_MODEL="openai/gpt-5.2"
export OPENAI_BASE_URL="http://localhost:3001/proxy/gpt5-2/v1"
export OPENAI_API_KEY="YOUR_KEY"
export AI_API_MODE="responses"
export AI_STREAM="1"
export AI_TIMEOUT_SEC="1000"
export AI_REASONING_EFFORT="xhigh"
export AI_RPM="12"
export AI_RETRIES="11"
export AI_MAX_INFLIGHT="30"
export AI_TRACE_OUT="trace.txt"

# 本地数据保留（按 ingested_at=抓到时间；默认 10 天）
export WORKDB_RETENTION_DAYS="10"

python3 weibo_rss_incremental.py \
  --enable-sync \
  --db-path workdb.sqlite \
  --verbose \
  --progress-every 1 \
  --interval-seconds 600 \
  --worker-threads 30
```

说明：
- `--interval-seconds 600`：每 10 分钟轮询一次 RSS。
- LLM 分析在后台线程池执行，不阻塞 RSS 抓取。
- `RSS_URLS` 支持逗号/换行分隔；也可以用 `RSS_URL`（只传 1 个）。
- `--author/--user-id` 都是可选的：为空时会尽量从 RSS/URL 自动推断。
  - AI 配置优先读：`AI_*`（比如 `AI_MODEL/AI_API_STYLE/AI_API_TYPE`），其次读 `OPENAI_BASE_URL/OPENAI_API_KEY`。

## CSV 批量入库（历史数据）
```bash
python3 ingest_weibo_to_workdb.py \
  --db workdb.sqlite \
  --csv weibo/挖地瓜的超级鹿鼎公/3962719063.csv \
  --api-type openai \
  --base-url http://localhost:3001/proxy/gpt5-2/v1 \
  --api-key 1 \
  --ai-stream \
  --api-mode responses \
  --model openai/gpt-5.2 \
  --ai-timeout-sec 1000 \
  --ai-reasoning-effort xhigh \
  --ai-rpm 12 \
  --ai-retries 11 \
  --ai-max-inflight 30 \
  --trace-out trace.txt \
  --verbose \
  --progress-every 1
```

## 本地 → Turso 同步
首次同步会自动建表（`posts` + `assertions`）。

说明：
- 同步需要环境变量：`TURSO_DATABASE_URL` + `TURSO_AUTH_TOKEN`

一次性同步：
```bash
python3 sync_workdb_to_turso.py \
  --local-db workdb.sqlite \
  --batch-size 500 \
  --verbose
```

定时同步（每 10 分钟）：
```bash
python3 sync_workdb_to_turso.py \
  --local-db workdb.sqlite \
  --batch-size 200 \
  --interval-seconds 600 \
  --verbose
```

## 本地数据库结构
见需求文档：`chat-export-1772618779602.md`。

## Streamlit 前端
```bash
streamlit run streamlit_app.py
```

说明：
- 读云端 Turso：`export STREAMLIT_DB_MODE=cloud`（需要 `TURSO_DATABASE_URL` + `TURSO_AUTH_TOKEN`）
- 读本地 WorkDB：默认读 `workdb.sqlite`；Docker 里建议设 `WORKDB_SQLITE_PATH=/data/workdb.sqlite`

## 线上 Docker（推荐）

容器默认用 `supervisord.conf` 常驻跑 2 件事：
- Web：Streamlit
- Worker：`weibo_rss_incremental.py`（定时抓取 + 后台 LLM + 后台 sync）

小提示：
- 如果本地 `/data/workdb.sqlite` 是空的，但 Turso 云端已经有数据：worker 第一次抓到的帖子会先查 Turso，查到就把结果回填到本地并跳过 LLM/sync（省时间/省成本）。

定时（通过 env 配）：
- 简化 cron（推荐）：`RSS_CRON="*/15 6-22 * * *"` 表示 6 点到 22 点，每 15 分钟跑一次

```bash
docker build -t alphavault .

docker run -d --name alphavault \
  -p 8080:8080 \
  -v alphavault-data:/data \
  -e RSS_URLS="https://rsshub.xxx/weibo/user/3962719063?key=YOUR_KEY" \
  -e RSS_CRON="*/15 6-22 * * *" \
  -e SYNC_CRON="*/15 6-22 * * *" \
  -e WORKDB_RETENTION_DAYS="10" \
  -e AI_API_STYLE="litellm" \
  -e AI_API_TYPE="openai" \
  -e AI_MODEL="openai/gpt-5.2" \
  -e AI_API_MODE="responses" \
  -e AI_STREAM="1" \
  -e AI_TIMEOUT_SEC="1000" \
  -e AI_REASONING_EFFORT="xhigh" \
  -e AI_RPM="12" \
  -e AI_RETRIES="11" \
  -e AI_MAX_INFLIGHT="30" \
  -e AI_TRACE_OUT="/data/trace.txt" \
  -e OPENAI_BASE_URL="http://xxx/v1" \
  -e OPENAI_API_KEY="YOUR_KEY" \
  -e TURSO_DATABASE_URL="libsql://xxx.turso.io" \
  -e TURSO_AUTH_TOKEN="YOUR_TOKEN" \
  -e STREAMLIT_DB_MODE=cloud \
  alphavault
```

也可以用 `docker-compose.yml`：
- `cp .env.example .env` 然后填值
- `docker compose up -d --build`

`docker-compose.yml` 只是本地省事用的；像 Render 这类平台一般只认 `Dockerfile`，你直接在它的 UI 里填 env 就行。

## 常见问题
- `auth role not found`：Turso token 与 DB 不匹配，请重新生成 token 并核对 URL。
- Turso 连不上/鉴权失败：看本地 `posts.sync_status / posts.sync_error / posts.next_sync_at`（会自动 retry）。
- RSS 抓取正常但 LLM 不跑：确认 `AI_*` / `OPENAI_API_KEY` / `OPENAI_BASE_URL` 配置正确。

## 交付与协作建议
- 本地库用于采集与处理中间态。
- 云端 Turso 用于归档与查询。
- 先同步历史数据，后开启定时同步。
