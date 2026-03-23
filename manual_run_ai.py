from __future__ import annotations

import argparse
import os
import re
import time
from pathlib import Path

from alphavault.env import load_dotenv_if_present

from alphavault.ai.analyze import (
    AI_MODE_COMPLETION,
    AI_MODE_RESPONSES,
    DEFAULT_AI_MODE,
    DEFAULT_AI_REASONING_EFFORT,
    DEFAULT_AI_RETRY_COUNT,
    DEFAULT_AI_TEMPERATURE,
    DEFAULT_MODEL,
    DEFAULT_PROMPT_VERSION,
)
from alphavault.constants import (
    ENV_AI_API_KEY,
    ENV_AI_API_MODE,
    ENV_AI_BASE_URL,
    ENV_AI_MODEL,
    ENV_AI_PROMPT_VERSION,
    ENV_AI_REASONING_EFFORT,
    ENV_AI_RETRIES,
    ENV_AI_RPM,
    ENV_AI_STREAM,
    ENV_AI_TEMPERATURE,
    ENV_AI_TIMEOUT_SEC,
    ENV_AI_TRACE_OUT,
)
from alphavault.db.turso_db import get_turso_engine_from_env
from alphavault.db.turso_queue import ensure_cloud_queue_schema, try_mark_ai_running
from alphavault.rss.utils import RateLimiter, env_bool, env_float, env_int
from alphavault.worker.worker import LLMConfig, _process_one_post_uid


DEFAULT_RELEVANT_THRESHOLD = 0.35


def parse_args() -> argparse.Namespace:
    ai_retries_env = env_int(ENV_AI_RETRIES)
    ai_rpm_env = env_float(ENV_AI_RPM)
    ai_timeout_env = env_float(ENV_AI_TIMEOUT_SEC)

    parser = argparse.ArgumentParser(
        description="Manual run AI for specific post_uid(s)"
    )
    parser.add_argument(
        "--post-uids",
        type=str,
        default="",
        help="要跑哪些 post_uid（逗号/换行分隔；纯数字会自动加 weibo:）",
    )

    # AI config (same meaning as worker)
    parser.add_argument("--model", default=os.getenv(ENV_AI_MODEL, DEFAULT_MODEL))
    parser.add_argument(
        "--base-url",
        default=os.getenv(ENV_AI_BASE_URL, "").strip(),
        help="可选：OpenAI 兼容接口 base_url（也可用 AI_BASE_URL）",
    )
    parser.add_argument(
        "--api-key", default=None, help="可选：API Key（默认读 AI_API_KEY）"
    )
    parser.add_argument(
        "--api-mode",
        default=os.getenv(ENV_AI_API_MODE, DEFAULT_AI_MODE),
        choices=[AI_MODE_COMPLETION, AI_MODE_RESPONSES],
    )
    parser.add_argument(
        "--ai-stream", action="store_true", help="可选：打开 stream（默认按环境变量）"
    )
    parser.add_argument(
        "--prompt-version",
        default=os.getenv(ENV_AI_PROMPT_VERSION, DEFAULT_PROMPT_VERSION),
    )
    parser.add_argument(
        "--ai-retries",
        type=int,
        default=ai_retries_env
        if ai_retries_env is not None
        else DEFAULT_AI_RETRY_COUNT,
    )
    parser.add_argument(
        "--ai-temperature",
        type=float,
        default=float(os.getenv(ENV_AI_TEMPERATURE, str(DEFAULT_AI_TEMPERATURE))),
    )
    parser.add_argument(
        "--ai-reasoning-effort",
        default=os.getenv(ENV_AI_REASONING_EFFORT, DEFAULT_AI_REASONING_EFFORT),
        choices=["none", "minimal", "low", "medium", "high", "xhigh"],
    )
    parser.add_argument(
        "--ai-rpm",
        type=float,
        default=ai_rpm_env if ai_rpm_env is not None else 0.0,
        help="每分钟最多多少次（0=不等）",
    )
    parser.add_argument(
        "--ai-timeout-sec",
        type=float,
        default=ai_timeout_env if ai_timeout_env is not None else 1000.0,
    )
    parser.add_argument("--trace-out", type=Path, default=None)
    parser.add_argument(
        "--relevant-threshold", type=float, default=DEFAULT_RELEVANT_THRESHOLD
    )
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def _parse_post_uids(value: str) -> list[str]:
    raw = str(value or "").strip()
    if not raw:
        return []
    items: list[str] = []
    seen: set[str] = set()
    for part in re.split(r"[,\n]+", raw):
        uid = str(part or "").strip()
        if not uid:
            continue
        if uid.isdigit():
            uid = f"weibo:{uid}"
        if uid in seen:
            continue
        seen.add(uid)
        items.append(uid)
    return items


def _build_config(args: argparse.Namespace) -> LLMConfig:
    ai_stream_env = env_bool(ENV_AI_STREAM)
    ai_stream = True
    if ai_stream_env is not None:
        ai_stream = bool(ai_stream_env)
    elif args.ai_stream:
        ai_stream = True

    trace_out = args.trace_out
    trace_out_env = os.getenv(ENV_AI_TRACE_OUT, "").strip()
    if trace_out_env and trace_out is None:
        trace_out = Path(trace_out_env)

    api_key = ""
    if args.api_key:
        api_key = str(args.api_key).strip()
    else:
        api_key = os.getenv(ENV_AI_API_KEY, "").strip()
    if not api_key:
        raise RuntimeError(f"Missing {ENV_AI_API_KEY}. Set {ENV_AI_API_KEY}.")

    return LLMConfig(
        api_key=api_key,
        model=str(args.model or DEFAULT_MODEL),
        prompt_version=str(args.prompt_version or DEFAULT_PROMPT_VERSION),
        relevant_threshold=max(0.0, min(1.0, float(args.relevant_threshold))),
        base_url=str(args.base_url or ""),
        api_mode=str(args.api_mode or DEFAULT_AI_MODE),
        ai_stream=ai_stream,
        ai_retries=max(0, int(args.ai_retries)),
        ai_temperature=float(args.ai_temperature),
        ai_reasoning_effort=str(
            args.ai_reasoning_effort or DEFAULT_AI_REASONING_EFFORT
        ),
        ai_rpm=max(0.0, float(args.ai_rpm or 0.0)),
        ai_timeout_seconds=max(1.0, float(args.ai_timeout_sec)),
        trace_out=trace_out,
        verbose=bool(args.verbose),
    )


def main() -> None:
    load_dotenv_if_present()
    args = parse_args()
    post_uids = _parse_post_uids(args.post_uids)
    if not post_uids:
        raise SystemExit("缺参数：请传 --post-uids")

    config = _build_config(args)
    limiter = RateLimiter(config.ai_rpm)
    engine = get_turso_engine_from_env()
    ensure_cloud_queue_schema(engine, verbose=bool(config.verbose))

    now_epoch = int(time.time())
    ok = 0
    skipped = 0
    for post_uid in post_uids:
        try:
            marked = try_mark_ai_running(engine, post_uid=post_uid, now_epoch=now_epoch)
        except Exception as e:
            skipped += 1
            print(
                f"[manual] skip post_uid={post_uid} mark_running_error={type(e).__name__}",
                flush=True,
            )
            continue
        if not marked:
            skipped += 1
            print(
                f"[manual] skip post_uid={post_uid} reason=not_due_or_processed",
                flush=True,
            )
            continue

        print(
            f"[manual] run post_uid={post_uid} prompt_version={config.prompt_version}",
            flush=True,
        )
        _process_one_post_uid(
            engine=engine, post_uid=post_uid, config=config, limiter=limiter
        )
        ok += 1

    print(f"[manual] done ok={ok} skipped={skipped}", flush=True)


if __name__ == "__main__":
    main()
