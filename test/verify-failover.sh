#!/usr/bin/env bash
# 验证 Sentinel 故障转移：
#   1. 写入数据并确认至少一个副本已同步
#   2. 停掉当前 master，等待 Sentinel 提升新 master
#   3. 通过 shard.sh 仍能读到旧数据并写入新数据
#   4. 重启旧 master，确认它作为新 master 的副本重新加入并完成同步
#
# 注意：脚本结束后分片拓扑会改变（新的 master 保持为提升后的副本），
# 这是预期行为。可继续运行 verify-restart.sh 验证重启后拓扑与数据保持。
set -euo pipefail

# shellcheck source=lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

SHARD="${SHARD:-1}"
RUN_ID="$(date +%s)-$$"

ensure_stack

if ! addr=$(sentinel_get_master "${SHARD}"); then
  fail "cannot discover master for shard${SHARD}"
fi
old_master=${addr% *}
old_port=${addr##* }
if ! old_container=$(container_for_host "${old_master}"); then
  fail "cannot map master ${old_master} to a container"
fi

if ! key=$(key_for_shard "${SHARD}" "test:failover:${RUN_ID}"); then
  fail "cannot generate a key for shard${SHARD}"
fi
value_before="before-${RUN_ID}"
value_after="after-${RUN_ID}"

log "shard${SHARD} master before failover: ${old_master}:${old_port} (container=${old_container})"
log "writing ${key}=${value_before}"
shard_set "${key}" "${value_before}"
if [[ "$(shard_get "${key}")" != "${value_before}" ]]; then
  fail "pre-failover read mismatch for ${key}"
fi

acked=$(node_cli "${old_master}" "${old_port}" WAIT 1 5000 2>/dev/null || true)
if (( acked < 1 )); then
  fail "write was not acknowledged by any replica before failover (acked=${acked})"
fi
log "at least one replica acknowledged the write (WAIT=${acked})"

log "stopping ${old_container} to trigger failover"
dc stop "${old_container}" >/dev/null 2>&1

new_addr=""
start=${SECONDS}
while (( SECONDS - start < 90 )); do
  if addr=$(sentinel_get_master "${SHARD}"); then
    host=${addr% *}
    if [[ "${host}" != "${old_master}" ]]; then
      new_addr="${addr}"
      break
    fi
  fi
  sleep 1
done

if [[ -z "${new_addr}" ]]; then
  log "failover did not happen, restoring old master"
  dc start "${old_container}" >/dev/null 2>&1 || true
  fail "sentinel did not promote a new master within 90s"
fi

new_master=${new_addr% *}
new_port=${new_addr##* }
log "new shard${SHARD} master: ${new_master}:${new_port}"
wait_until 60 "new master ${new_master} to report role=master" is_master "${new_master}" "${new_port}"
pass "sentinel promoted ${new_master} as the new master"

if [[ "$(shard_get "${key}")" != "${value_before}" ]]; then
  fail "pre-failover data is not readable after failover for ${key}"
fi
pass "pre-failover data survived and is readable through shard.sh"

shard_set "${key}" "${value_after}"
if [[ "$(shard_get "${key}")" != "${value_after}" ]]; then
  fail "post-failover write/read mismatch for ${key}"
fi
pass "write/read still works after failover"

log "restarting old master ${old_container}; it should rejoin as a replica"
dc start "${old_container}" >/dev/null 2>&1
wait_until 120 "old master ${old_master} to rejoin as synced replica" \
  is_replica_synced "${old_master}" "${old_port}"
pass "old master rejoined as a synced replica"

if ! addr=$(sentinel_get_master "${SHARD}"); then
  fail "cannot re-discover master for shard${SHARD}"
fi
if [[ "${addr% *}" != "${new_master}" ]]; then
  fail "sentinel master changed unexpectedly to ${addr% *}"
fi
pass "sentinel still reports ${new_master} as master"

shard_del "${key}"

log "expected new topology: shard${SHARD} master = ${new_master}:${new_port}"
pass "verify-failover completed"
