#!/usr/bin/env bash
# 依次执行全部验证脚本。
# 顺序很重要：先验证分片，再制造一次故障转移，最后验证整体重启后
# 仍然保持故障转移后的拓扑与数据。
set -euo pipefail

TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"${TEST_DIR}/verify-sharding.sh"
"${TEST_DIR}/verify-failover.sh"
"${TEST_DIR}/verify-restart.sh"

printf '[PASS] all verification scripts completed\n'
