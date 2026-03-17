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
示例参数参考你当前 LLM 代理配置：
```bash
python3 weibo_rss_incremental.py \
  --db-path workdb.sqlite \
  --rss-url "https://rsshub-flyctl.fly.dev/weibo/user/3962719063?key=YOUR_KEY" \
  --author "挖地瓜的超级鹿鼎公" \
  --user-id 3962719063 \
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
  --progress-every 1 \
  --interval-seconds 600 \
  --worker-threads 30
```

说明：
- `--interval-seconds 600`：每 10 分钟轮询一次 RSS。
- LLM 分析在后台线程池执行，不阻塞 RSS 抓取。

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

一次性同步：
```bash
python3 sync_workdb_to_turso.py \
  --local-db workdb.sqlite \
  --turso-url $TURSO_DATABASE_URL \
  --turso-token $TURSO_AUTH_TOKEN \
  --batch-size 500 \
  --verbose
```

定时同步（每 10 分钟）：
```bash
python3 sync_workdb_to_turso.py \
  --local-db workdb.sqlite \
  --turso-url $TURSO_DATABASE_URL \
  --turso-token $TURSO_AUTH_TOKEN \
  --batch-size 200 \
  --interval-seconds 600 \
  --verbose
```

## 本地数据库结构
见需求文档：`chat-export-1772618779602.md`。

## 常见问题
- `auth role not found`：Turso token 与 DB 不匹配，请重新生成 token 并核对 URL。
- RSS 抓取正常但 LLM 不跑：确认 LLM 参数与代理配置一致，且 `--api-key` 已设置。

## 交付与协作建议
- 本地库用于采集与处理中间态。
- 云端 Turso 用于归档与查询。
- 先同步历史数据，后开启定时同步。
