# 验证脚本

这些脚本在宿主机执行，通过 `docker compose exec` 操作集群，用于验证：

1. 客户端分片是否真的把数据分散到不同 master
2. Sentinel 故障转移是否工作，旧 master 恢复后是否能作为副本重新加入
3. `docker compose down` / `up`（保留数据卷）后拓扑和数据是否保持

## 前置条件

- 已安装 `docker` 和 `docker compose`
- 宿主机有 `bash` 4+（脚本使用关联数组）
- 脚本与 `docker-compose.yml` 位于同一项目目录的 `test/` 下
- 项目可用：`cd .. && docker compose up -d`
- 默认密码 `redispass`，如修改过 `.env`，脚本会自动读取 `.env`

## 脚本说明

| 脚本 | 验证内容 | 是否改变拓扑 |
|------|----------|--------------|
| `verify-sharding.sh` | 写入/读取 40 个 key，并确认两个分片 master 上都有数据 | 否 |
| `verify-failover.sh` | 停掉 shard1 当前 master，等待 Sentinel 提升新 master，验证读写和旧 master 回归 | 是，shard1 master 会变成被提升的副本 |
| `verify-restart.sh` | 记录当前拓扑，整体 `down`/`up` 后确认 master 容器和测试数据都不变 | 否，但会短暂停止全部容器 |
| `run-all.sh` | 依次运行以上三个脚本 | 是（包含 failover） |

## 使用方式

```bash
cd /home/opc/redis-sentinel-sharding
chmod +x test/*.sh

# 单独运行
./test/verify-sharding.sh
./test/verify-failover.sh
./test/verify-restart.sh

# 全部运行（推荐按此顺序）
./test/run-all.sh
```

所有脚本成功时以 `[PASS] ...` 输出并以 0 退出；失败时输出 `[FAIL] ...` 并以非 0 退出。

## 可调环境变量

```bash
# 分片数，必须与 .env 中的 SHARDS 一致（脚本会自动读取 .env）
export SHARDS=2

# 分片验证写入的 key 数量
export COUNT=100

# verify-failover 选择哪个分片做故障转移
export SHARD=1

# Redis 密码（通常直接使用 .env 中的值）
export REDIS_PASSWORD=redispass
```

## 副作用与清理

- `verify-sharding.sh` 和 `verify-restart.sh` 会删除自己写入的 `test:*` key。
- `verify-failover.sh` 会真正停止一次 master 并触发 Sentinel 选主，
  结束后不会自动切回原 master；这是为了验证真实故障转移。
- 如需恢复初始拓扑并清空所有数据：

  ```bash
  cd /home/opc/redis-sentinel-sharding
  docker compose down -v
  docker compose up -d
  ```

  `-v` 会删除全部 Redis/Sentinel 数据卷，请勿在有数据的环境随意执行。

## 典型输出

```
[test] ensuring docker compose stack is running
[test] writing 40 keys through shard.sh (prefix=test:shard:...)
[PASS] write/read through shard.sh OK (40 keys)
[test] shard1 master shard1-master:6379 holds 18 test keys
[test] shard2 master shard2-master:6379 holds 22 test keys
[PASS] keys are distributed across all 2 shards
[PASS] verify-sharding completed
```
