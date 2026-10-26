#!/usr/bin/env bash
# 验证客户端分片：写入一批 key 后，能从各分片 master 上看到数据，并且能正确读回。
set -euo pipefail

# shellcheck source=lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

COUNT="${COUNT:-40}"
RUN_ID="$(date +%s)-$$"
PREFIX="test:shard:${RUN_ID}"

ensure_stack

log "writing ${COUNT} keys through shard.sh (prefix=${PREFIX})"
for (( i = 1; i <= COUNT; i++ )); do
  shard_set "${PREFIX}:${i}" "value-${i}"
done

log "reading ${COUNT} keys back through shard.sh"
for (( i = 1; i <= COUNT; i++ )); do
  got=$(shard_get "${PREFIX}:${i}")
  if [[ "${got}" != "value-${i}" ]]; then
    fail "value mismatch for ${PREFIX}:${i}: got '${got}'"
  fi
done
pass "write/read through shard.sh OK (${COUNT} keys)"

distributed=0
for (( s = 1; s <= SHARDS; s++ )); do
  if ! addr=$(sentinel_get_master "$s"); then
    fail "cannot discover master for shard${s}"
  fi
  host=${addr% *}
  port=${addr##* }
  count=$(node_cli "${host}" "${port}" --scan --pattern "${PREFIX}:*" | wc -l | tr -d ' ')
  log "shard${s} master ${host}:${port} holds ${count} test keys"
  if (( count > 0 )); then
    distributed=$((distributed + 1))
  fi
done

if (( distributed != SHARDS )); then
  fail "expected keys on all ${SHARDS} shards, found ${distributed}"
fi
pass "keys are distributed across all ${distributed} shards"

log "cleaning up test keys"
for (( i = 1; i <= COUNT; i++ )); do
  shard_del "${PREFIX}:${i}"
done

pass "verify-sharding completed"
