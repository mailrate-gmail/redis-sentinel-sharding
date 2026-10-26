#!/usr/bin/env bash
# 公共函数库：被 test/ 目录下的验证脚本 source 使用。
# 所有命令都通过 docker compose 在宿主机执行，不依赖容器内安装额外工具。

set -euo pipefail

TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${TEST_DIR}/.." && pwd)"
COMPOSE=(docker compose --project-directory "${PROJECT_DIR}" -f "${PROJECT_DIR}/docker-compose.yml")

if [[ -f "${PROJECT_DIR}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${PROJECT_DIR}/.env"
  set +a
fi

SHARDS="${SHARDS:-2}"
REDIS_PASSWORD="${REDIS_PASSWORD:-redispass}"
SENTINEL_PORT="${SENTINEL_PORT:-26379}"
SENTINELS_PER_SHARD="${SENTINELS_PER_SHARD:-3}"

log()  { printf '[test] %s\n' "$*"; }
pass() { printf '[PASS] %s\n' "$*"; }
fail() { printf '[FAIL] %s\n' "$*" >&2; exit 1; }

dc() { "${COMPOSE[@]}" "$@"; }

# 输出 "host port"，host 可能是服务名，也可能是 IP。
sentinel_get_master() {
  local shard="$1" i out host port
  [[ "${shard}" == shard* ]] || shard="shard${shard}"
  for (( i = 1; i <= SENTINELS_PER_SHARD; i++ )); do
    out=$(dc exec -T "${shard}-sentinel${i}" redis-cli -p "${SENTINEL_PORT}" \
      sentinel get-master-addr-by-name "${shard}" 2>/dev/null || true)
    host=$(printf '%s\n' "${out}" | sed -n '1p')
    port=$(printf '%s\n' "${out}" | sed -n '2p')
    if [[ -n "${host}" && -n "${port}" ]]; then
      printf '%s %s\n' "${host}" "${port}"
      return 0
    fi
  done
  return 1
}

# 从 redis-cli 工具容器访问任意节点。
node_cli() {
  local host="$1" port="$2"
  shift 2
  dc exec -T redis-cli redis-cli --no-auth-warning -a "${REDIS_PASSWORD}" \
    -h "${host}" -p "${port}" "$@"
}

node_role() {
  node_cli "$1" "$2" info replication 2>/dev/null \
    | awk -F: '/^role:/ {print $2}' | tr -d '\r' || true
}

is_master() {
  [[ "$(node_role "$1" "$2")" == "master" ]]
}

is_replica_synced() {
  local info role link
  info=$(node_cli "$1" "$2" info replication 2>/dev/null || true)
  role=$(printf '%s\n' "${info}" | awk -F: '/^role:/ {print $2}' | tr -d '\r')
  link=$(printf '%s\n' "${info}" | awk -F: '/^master_link_status:/ {print $2}' | tr -d '\r')
  [[ "${role}" == "slave" && "${link}" == "up" ]]
}

shard_cmd() { dc exec -T redis-cli sh /scripts/shard.sh "$@"; }
shard_set() { shard_cmd SET "$1" "$2" >/dev/null 2>&1; }
shard_get() { shard_cmd GET "$1" 2>/dev/null; }
shard_del() { shard_cmd DEL "$1" >/dev/null 2>&1 || true; }

wait_until() {
  local timeout="$1" desc="$2"
  shift 2
  local start=${SECONDS}
  while true; do
    if "$@"; then
      return 0
    fi
    if (( SECONDS - start >= timeout )); then
      fail "timeout after ${timeout}s: ${desc}"
    fi
    sleep 1
  done
}

wait_for_cluster() {
  local timeout="${1:-180}" start=${SECONDS} s addr host port all_ok
  while (( SECONDS - start < timeout )); do
    all_ok=1
    for (( s = 1; s <= SHARDS; s++ )); do
      if ! addr=$(sentinel_get_master "$s"); then
        all_ok=0
        break
      fi
      host=${addr% *}
      port=${addr##* }
      if ! is_master "${host}" "${port}"; then
        all_ok=0
        break
      fi
    done
    if (( all_ok == 1 )); then
      return 0
    fi
    sleep 1
  done
  fail "cluster did not become ready within ${timeout}s"
}

ensure_stack() {
  log "ensuring docker compose stack is running"
  dc up -d >/dev/null 2>&1
  wait_for_cluster 180
}

# 把 Sentinel 返回的 host（服务名或 IP）映射为容器名。
container_for_host() {
  local host="$1" cid name ip
  if dc ps --all --format '{{.Name}}' | grep -Fqx -- "${host}"; then
    printf '%s\n' "${host}"
    return 0
  fi
  for cid in $(dc ps -q); do
    name=$(docker inspect -f '{{.Name}}' "${cid}" | sed 's#^/##')
    ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}' "${cid}" | awk '{print $1}')
    if [[ "${ip}" == "${host}" ]]; then
      printf '%s\n' "${name}"
      return 0
    fi
  done
  return 1
}

# 生成一个确定落在指定分片的 key，算法与 shard.sh 保持一致。
key_for_shard() {
  local target="$1" prefix="$2" i=0 key hash shard
  while (( i < 100000 )); do
    key="${prefix}:${i}"
    hash=$(printf '%s' "${key}" | cksum | awk '{print $1}')
    shard=$(( hash % SHARDS + 1 ))
    if (( shard == target )); then
      printf '%s\n' "${key}"
      return 0
    fi
    i=$((i + 1))
  done
  return 1
}
