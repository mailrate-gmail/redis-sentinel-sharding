#!/usr/bin/env bash
# 验证整体重启后拓扑与数据保持：
#   1. 记录每个分片当前 master 及其容器
#   2. 每个分片写入一个测试 key
#   3. docker compose down（保留数据卷）后重新 up
#   4. 确认每个分片的 master 仍是重启前的容器，且测试 key 仍可读取
set -euo pipefail

# shellcheck source=lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

RUN_ID="$(date +%s)-$$"
PREFIX="test:restart:${RUN_ID}"

ensure_stack

declare -A before_host=()
declare -A before_port=()
declare -A before_container=()
declare -A test_keys=()
declare -A test_values=()

for (( s = 1; s <= SHARDS; s++ )); do
  if ! addr=$(sentinel_get_master "$s"); then
    fail "cannot discover master for shard${s}"
  fi
  host=${addr% *}
  port=${addr##* }
  if ! container=$(container_for_host "${host}"); then
    fail "cannot map shard${s} master ${host} to a container"
  fi
  if ! key=$(key_for_shard "$s" "${PREFIX}"); then
    fail "cannot generate a key for shard${s}"
  fi

  before_host[$s]="${host}"
  before_port[$s]="${port}"
  before_container[$s]="${container}"
  test_keys[$s]="${key}"
  test_values[$s]="value-shard-${s}-${RUN_ID}"

  shard_set "${key}" "${test_values[$s]}"
  if [[ "$(shard_get "${key}")" != "${test_values[$s]}" ]]; then
    fail "pre-restart read mismatch for ${key}"
  fi
  log "shard${s}: master=${host}:${port} container=${container} key=${key}"
done
pass "pre-restart topology recorded and test data written"

log "running docker compose down (data volumes are kept)"
dc down >/dev/null 2>&1
log "running docker compose up -d"
dc up -d >/dev/null 2>&1
wait_for_cluster 240
pass "cluster is ready after full restart"

for (( s = 1; s <= SHARDS; s++ )); do
  if ! addr=$(sentinel_get_master "$s"); then
    fail "cannot re-discover master for shard${s} after restart"
  fi
  host=${addr% *}
  port=${addr##* }
  if ! container=$(container_for_host "${host}"); then
    fail "cannot map post-restart shard${s} master ${host} to a container"
  fi

  if [[ "${container}" != "${before_container[$s]}" ]]; then
    fail "shard${s} master changed across restart: ${before_container[$s]} -> ${container}"
  fi
  if [[ "${port}" != "${before_port[$s]}" ]]; then
    fail "shard${s} master port changed across restart: ${before_port[$s]} -> ${port}"
  fi
  if ! is_master "${host}" "${port}"; then
    fail "shard${s} reported master ${host}:${port} is not role=master"
  fi

  got=$(shard_get "${test_keys[$s]}")
  if [[ "${got}" != "${test_values[$s]}" ]]; then
    fail "shard${s} data lost across restart: got '${got}'"
  fi

  pass "shard${s} topology and data preserved (${host}:${port})"
  shard_del "${test_keys[$s]}"
done

pass "verify-restart completed"
