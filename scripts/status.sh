#!/bin/sh
set -eu

SHARDS="${SHARDS:-2}"
PASSWORD="${REDIS_PASSWORD:-}"
SENTINEL_PORT="${SENTINEL_PORT:-26379}"

redis_auth() {
  if [ -n "$PASSWORD" ]; then
    redis-cli --no-auth-warning -a "$PASSWORD" "$@"
  else
    redis-cli "$@"
  fi
}

for s in $(seq 1 "$SHARDS"); do
  echo "===== shard${s} ====="

  master=""
  sentinel=""
  for i in 1 2 3; do
    out=$(redis-cli -h "shard${s}-sentinel${i}" -p "$SENTINEL_PORT" \
      sentinel get-master-addr-by-name "shard${s}" 2>/dev/null || true)
    if [ -n "$out" ]; then
      master="$out"
      sentinel="shard${s}-sentinel${i}"
      break
    fi
  done

  if [ -z "$master" ]; then
    echo "  UNREACHABLE (no sentinel answered)"
    echo
    continue
  fi

  master_host=$(printf '%s\n' "$master" | sed -n '1p')
  master_port=$(printf '%s\n' "$master" | sed -n '2p')
  echo "  master : ${master_host}:${master_port}"
  echo "  via    : ${sentinel}"

  role=$(redis_auth -h "$master_host" -p "$master_port" info replication 2>/dev/null \
    | awk -F: '/^role:/ {print $2}' | tr -d '\r' || true)
  keys=$(redis_auth -h "$master_host" -p "$master_port" dbsize 2>/dev/null || true)
  echo "  role   : ${role:-unknown}"
  echo "  keys   : ${keys:-unknown}"
  echo "  replicas:"

  redis-cli -h "$sentinel" -p "$SENTINEL_PORT" sentinel replicas "shard${s}" 2>/dev/null \
    | awk '
      /^name$/  { getline name }
      /^flags$/ { getline flags; printf "    - %s [%s]\n", name, flags }
    ' || true
  echo
done
