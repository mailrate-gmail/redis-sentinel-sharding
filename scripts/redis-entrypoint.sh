#!/bin/sh
set -eu

self="$(hostname)"
SHARD_NAME="${SHARD_NAME:-${self%-*}}"
SENTINEL_PORT="${SENTINEL_PORT:-26379}"
SENTINELS_PER_SHARD="${SENTINELS_PER_SHARD:-3}"
WAIT_SECONDS="${WAIT_SECONDS:-120}"

self_ip="$(hostname -i 2>/dev/null | awk '{print $1}' || true)"

discover_master() {
  i=1
  while [ "$i" -le "$SENTINELS_PER_SHARD" ]; do
    host="${SHARD_NAME}-sentinel${i}"
    out=$(redis-cli -h "$host" -p "$SENTINEL_PORT" \
      sentinel get-master-addr-by-name "$SHARD_NAME" 2>/dev/null || true)
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

master_addr=""
elapsed=0
while :; do
  if master_addr=$(discover_master); then
    break
  fi
  if [ "$elapsed" -ge "$WAIT_SECONDS" ]; then
    echo "ERROR: no ${SHARD_NAME} sentinel answered after ${WAIT_SECONDS}s" >&2
    exit 1
  fi
  sleep 1
  elapsed=$((elapsed + 1))
done

master_host=$(printf '%s\n' "$master_addr" | sed -n '1p')
master_port=$(printf '%s\n' "$master_addr" | sed -n '2p')

echo "[role-resolver] self=${self} shard=${SHARD_NAME} master=${master_host}:${master_port}" >&2

if [ "$master_host" = "$self" ] || \
   { [ -n "$self_ip" ] && [ "$master_host" = "$self_ip" ]; }; then
  echo "[role-resolver] ${self} starts as MASTER" >&2
  exec redis-server "$@" --replica-announce-ip "$self"
else
  echo "[role-resolver] ${self} starts as REPLICA of ${master_host}:${master_port}" >&2
  exec redis-server "$@" --replica-announce-ip "$self" --replicaof "$master_host" "$master_port"
fi
