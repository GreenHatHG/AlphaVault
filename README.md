# AlphaVault

面向投资大佬观点追踪的采集与分析系统。  
当前支持：微博 RSS 抓取、LLM 投资相关性分析、断言抽取、写入 Turso 云端库、Streamlit 可视化。

## 功能概览
- RSS 增量抓取（去重）
- LLM 分析与观点抽取（写入 `posts` + `assertions`）
- Turso 云端数据库作为队列 + 归档（幂等 upsert）
- 可选 Redis：Turso 临时挂了时先排队（避免丢）

## 目录结构（核心脚本）
- `weibo_rss_turso_worker.py`：Worker（RSS → spool → Turso → AI → Turso）
- `turso_queue.py`：Turso 队列字段与读写
- `turso_db.py`：Turso engine + 基础表（posts/assertions）
- `streamlit_app.py`：Web（只读 Turso）

## 环境要求
- Python 3.10+
- 可用的 LLM 代理或 API

## 安装
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Worker（RSS → Turso → AI）
推荐用环境变量配 AI（Docker/supervisord 也用这一套）：
```bash
export RSS_URLS="https://rsshub.xxx/weibo/user/3962719063?key=YOUR_KEY,https://rsshub.xxx/weibo/user/123?key=YOUR_KEY"

	export TURSO_DATABASE_URL="libsql://xxx.turso.io"
	export TURSO_AUTH_TOKEN="YOUR_TOKEN"

	export AI_MODEL="openai/gpt-5.2"
	export AI_BASE_URL="http://localhost:3001/proxy/gpt5-2/v1"
	export AI_API_KEY="YOUR_KEY"
	export AI_API_MODE="responses"
	export AI_STREAM="1"
	export AI_TIMEOUT_SEC="1000"
	export AI_REASONING_EFFORT="xhigh"
export AI_RPM="12"
export AI_RETRIES="11"
export AI_MAX_INFLIGHT="30"
export AI_TRACE_OUT="trace.txt"

python3 weibo_rss_turso_worker.py --verbose
```

说明：
- `RSS_URLS` 支持逗号/换行分隔；也可以用 `RSS_URL`（只传 1 个）。
- `--author/--user-id` 都是可选的：为空时会尽量从 RSS/URL 自动推断。
- Worker 会先写本地 `spool` 文件；Turso 写失败时会保留 `spool`，并且（可选）推到 Redis。
- Streamlit 只展示 `processed_at IS NOT NULL` 的帖子（避免 “pending 占位” 被当成 irrelevant）。

## Streamlit 前端
```bash
streamlit run streamlit_app.py
```

需要：`TURSO_DATABASE_URL`（可选 `TURSO_AUTH_TOKEN`）。

## 线上 Docker（推荐）

容器默认用 `supervisord.conf` 常驻跑 2 件事：
- Web：Streamlit
- Worker：`weibo_rss_turso_worker.py`

定时（通过 env 配）：
- 简化 cron（推荐）：`RSS_CRON="*/15 6-22 * * *"` 表示 6 点到 22 点，每 15 分钟跑一次

```bash
docker build -t alphavault .

docker run -d --name alphavault \
	  -p 8080:8080 \
	  -e RSS_URLS="https://rsshub.xxx/weibo/user/3962719063?key=YOUR_KEY" \
	  -e RSS_CRON="*/15 6-22 * * *" \
	  -e AI_MODEL="openai/gpt-5.2" \
	  -e AI_API_MODE="responses" \
	  -e AI_STREAM="1" \
	  -e AI_TIMEOUT_SEC="1000" \
	  -e AI_REASONING_EFFORT="xhigh" \
	  -e AI_RPM="12" \
	  -e AI_RETRIES="11" \
	  -e AI_MAX_INFLIGHT="30" \
	  -e AI_TRACE_OUT="/data/trace.txt" \
	  -e AI_BASE_URL="http://xxx/v1" \
	  -e AI_API_KEY="YOUR_KEY" \
	  -e TURSO_DATABASE_URL="libsql://xxx.turso.io" \
	  -e TURSO_AUTH_TOKEN="YOUR_TOKEN" \
	  -e REDIS_URL="redis://:pass@host:6379/0" \
	  -e SPOOL_DIR="/tmp/alphavault-spool" \
	  alphavault
```

也可以用 `docker-compose.yml`：
- `cp .env.example .env` 然后填值
- `docker compose up -d --build`

`docker-compose.yml` 只是本地省事用的；像 Render 这类平台一般只认 `Dockerfile`，你直接在它的 UI 里填 env 就行。

## 常见问题
- `auth role not found`：Turso token 与 DB 不匹配，请重新生成 token 并核对 URL。
- Turso 连不上：worker 不会退出，会先写 `spool`/Redis；恢复后会自动 flush + 重试 AI。
- Streamlit 看不到数据：因为只展示 `processed_at IS NOT NULL`（AI 还没跑完就不会显示）。
- RSS 抓取正常但 LLM 不跑：确认 `AI_API_KEY` / `AI_MODEL` / `AI_BASE_URL` 配置正确。
