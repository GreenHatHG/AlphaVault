#!/bin/sh
set -eu

mkdir -p /data

echo "[entrypoint] running startup healthcheck..."
python3 -u /app/startup_healthcheck.py

parse_simple_cron() {
  name="$1"
  cron="$2"
  (
    set -eu
    set -f

    # shellcheck disable=SC2086
    set -- $cron
    if [ "$#" -ne 5 ]; then
      echo "[entrypoint] ${name} invalid: need 5 fields, got $# (${cron})" >&2
      exit 1
    fi

    minute="$1"
    hour="$2"
    dom="$3"
    month="$4"
    dow="$5"

    if [ "$dom" != "*" ] || [ "$month" != "*" ] || [ "$dow" != "*" ]; then
      echo "[entrypoint] ${name} invalid: only supports '* * *' for day/month/week (${cron})" >&2
      exit 1
    fi

    case "$minute" in
      \*/[0-9]*)
        interval_min="${minute#*/}"
        ;;
      *)
        echo "[entrypoint] ${name} invalid: minute must be like '*/15' (${cron})" >&2
        exit 1
        ;;
    esac

    case "$interval_min" in
      ''|*[!0-9]*)
        echo "[entrypoint] ${name} invalid: minute step not a number (${cron})" >&2
        exit 1
        ;;
    esac

    if [ "$interval_min" -le 0 ]; then
      echo "[entrypoint] ${name} invalid: minute step must be > 0 (${cron})" >&2
      exit 1
    fi

    seconds=$((interval_min * 60))

    if [ "$hour" = "*" ]; then
      hours=""
    else
      case "$hour" in
        *-*)
          start_h="${hour%%-*}"
          end_h="${hour#*-}"
          ;;
        *)
          start_h="$hour"
          end_h="$hour"
          ;;
      esac

      case "$start_h" in ''|*[!0-9]*) echo "[entrypoint] ${name} invalid hour (${cron})" >&2; exit 1 ;; esac
      case "$end_h" in ''|*[!0-9]*) echo "[entrypoint] ${name} invalid hour (${cron})" >&2; exit 1 ;; esac

      if [ "$start_h" -lt 0 ] || [ "$start_h" -gt 23 ] || [ "$end_h" -lt 0 ] || [ "$end_h" -gt 23 ]; then
        echo "[entrypoint] ${name} invalid hour range (${cron})" >&2
        exit 1
      fi
      hours="${start_h}-${end_h}"
    fi

    echo "${seconds}|${hours}"
  )
}

# Defaults
# - Worker: controls the whole loop (AI/flush/RSS).
# - RSS: only controls RSS ingest.
WORKER_INTERVAL_SECONDS="600"
WORKER_ACTIVE_HOURS=""

RSS_INTERVAL_SECONDS="600"
RSS_ACTIVE_HOURS=""

# Worker cron shortcut (simple pattern)
# Example: WORKER_CRON="*/10 * * * *" (every 10 minutes, all day)
if [ -n "${WORKER_CRON:-}" ]; then
  out="$(parse_simple_cron "WORKER_CRON" "$WORKER_CRON")"
  WORKER_INTERVAL_SECONDS="${out%%|*}"
  WORKER_ACTIVE_HOURS="${out#*|}"
fi

# Cron shortcut (simple pattern)
# Example: RSS_CRON="*/15 6-22 * * *"
if [ -n "${RSS_CRON:-}" ]; then
  out="$(parse_simple_cron "RSS_CRON" "$RSS_CRON")"
  RSS_INTERVAL_SECONDS="${out%%|*}"
  RSS_ACTIVE_HOURS="${out#*|}"
fi

export WORKER_INTERVAL_SECONDS WORKER_ACTIVE_HOURS
export RSS_INTERVAL_SECONDS RSS_ACTIVE_HOURS

echo "[entrypoint] worker_interval=${WORKER_INTERVAL_SECONDS}s worker_active=${WORKER_ACTIVE_HOURS:-all}"
echo "[entrypoint] rss_interval=${RSS_INTERVAL_SECONDS}s rss_active=${RSS_ACTIVE_HOURS:-all}"
echo "[entrypoint] starting supervisord..."
exec supervisord -c /app/supervisord.conf
