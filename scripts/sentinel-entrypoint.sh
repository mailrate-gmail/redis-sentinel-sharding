#!/bin/sh
set -eu

CONF="/data/sentinel.conf"

if [ ! -f "$CONF" ]; then
  cat > "$CONF" <<EOF
port ${SENTINEL_PORT}
bind 0.0.0.0
dir /data
protected-mode no

sentinel resolve-hostnames yes
sentinel announce-hostnames yes
sentinel monitor ${SHARD_NAME} ${MASTER_HOST} ${MASTER_PORT} 2
sentinel auth-pass ${SHARD_NAME} ${REDIS_PASSWORD}
sentinel down-after-milliseconds ${SHARD_NAME} 5000
sentinel failover-timeout ${SHARD_NAME} 60000
sentinel parallel-syncs ${SHARD_NAME} 1
EOF
fi

exec redis-sentinel "$CONF"
