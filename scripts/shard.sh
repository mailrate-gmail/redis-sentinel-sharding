#!/bin/sh
set -eu

SHARDS="${SHARDS:-2}"
PASSWORD="${REDIS_PASSWORD:-}"
SENTINEL_PORT="${SENTINEL_PORT:-26379}"
SENTINELS_PER_SHARD="${SENTINELS_PER_SHARD:-3}"

usage() {
  cat >&2 <<'EOF'
Usage: shard.sh <command> <key> [args...]

The 2nd argument (key) is hashed to choose a shard. Commands are sent to the
current master of that shard, discovered through its 3 sentinels.

Examples:
  shard.sh SET user:1 alice
  shard.sh GET user:1
  shard.sh DEL user:1

NOTE: multi-key commands (MGET/MSET/...) are not supported because different
keys may live on different shards.
EOF
  exit 1
}

[ "$#" -ge 2 ] || usage
KEY="$2"

redis_auth() {
  if [ -n "$PASSWORD" ]; then
    redis-cli --no-auth-warning -a "$PASSWORD" "$@"
  else
    redis-cli "$@"
  fi
}

discover_master() {
  shard="$1"
  i=1
  while [ "$i" -le "$SENTINELS_PER_SHARD" ]; do
    host="shard${shard}-sentinel${i}"
    out=$(redis-cli -h "$host" -p "$SENTINEL_PORT" \
      sentinel get-master-addr-by-name "shard${shard}" 2>/dev/null || true)
    h=$(printf '%s\n' "$out" | sed -n '1p')
    p=$(printf '%s\n' "$out" | sed -n '2p')
    if [ -n "$h" ] && [ -n "$p" ]; then
      printf '%s\n%s\n' "$h" "$p"
      return 0
    fi
    i=$((i + 1))
  done
  return 1
}

HASH=$(printf '%s' "$KEY" | cksum | awk '{print $1}')
SHARD=$(( HASH % SHARDS + 1 ))

if ! master=$(discover_master "$SHARD"); then
  echo "ERROR: cannot discover master for shard${SHARD} through sentinels" >&2
  exit 1
fi

MASTER_HOST=$(printf '%s\n' "$master" | sed -n '1p')
MASTER_PORT=$(printf '%s\n' "$master" | sed -n '2p')

echo "[shard.sh] key='${KEY}' -> shard${SHARD} (${MASTER_HOST}:${MASTER_PORT})" >&2

redis_auth -h "$MASTER_HOST" -p "$MASTER_PORT" "$@"
