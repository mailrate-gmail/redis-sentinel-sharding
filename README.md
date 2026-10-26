# Redis 多分片 Sentinel 集群（1 主 2 从 + 3 哨兵 / 分片）

每个分片是一套独立的 **Redis Sentinel 高可用组**：

```
分片 1: shard1-master  <- shard1-replica1 / shard1-replica2
        shard1-sentinel1/2/3 (quorum=2)

分片 2: shard2-master  <- shard2-replica1 / shard2-replica2
        shard2-sentinel1/2/3 (quorum=2)
```

数据分片由客户端完成：`scripts/shard.sh` 对 key 做哈希后选择分片，再通过该分片的
Sentinel 发现当前 master 并执行命令。Sentinel 本身不提供分片，这是 Sentinel 架构的
标准做法。

可运行的 Python 客户端示例见 `scripts/client_sharding.py`（默认与 `shard.sh` 使用
相同的 `cksum % N` 路由，支持 Sentinel 故障转移后的 master 刷新）。

## 启动

```bash
cd /home/opc/redis-sentinel-sharding
docker compose up -d
docker compose ps
```

首次启动约 12 个 Redis/Sentinel 容器 + 1 个 `redis-cli` 工具容器。

## 直接操作节点（redis-cli 容器内）

```bash
docker compose exec redis-cli redis-cli -h shard1-master -a redispass ping
docker compose exec redis-cli redis-cli -h shard1-master -a redispass role
docker compose exec redis-cli redis-cli -h shard2-master -a redispass info replication

# 查看 Sentinel 认定的 master
docker compose exec redis-cli redis-cli -h shard1-sentinel1 -p 26379 \
  sentinel get-master-addr-by-name shard1

# 查看整体状态（master / replicas / key 数量）
docker compose exec redis-cli sh /scripts/status.sh
```

宿主机也发布了端口，可直接连指定节点：

| 分片 | master | replica1 | replica2 | sentinels |
|------|--------|----------|----------|-----------|
| 1 | 7001 | 7002 | 7003 | 27001-27003 |
| 2 | 7004 | 7005 | 7006 | 27004-27006 |

```bash
redis-cli -h 127.0.0.1 -p 7001 -a redispass
```

端口固定映射到容器；发生故障转移后，原来的 `shard1-master` 容器可能已经是
replica。需要写入时请使用 `shard.sh`，或先通过 Sentinel 查询当前 master。

## 使用分片读写

`shard.sh <命令> <key> [参数...]`，第二个参数用于选择分片：

```bash
docker compose exec redis-cli sh /scripts/shard.sh SET user:1 alice
docker compose exec redis-cli sh /scripts/shard.sh GET user:1
docker compose exec redis-cli sh /scripts/shard.sh SET user:2 bob

# 观察 key 被分散到不同 master
docker compose exec redis-cli sh /scripts/status.sh

# 注意：第一个 redis-cli 是 Compose 服务名（不是命令），
# 第二个才是容器内执行的 redis-cli 命令。
docker compose exec -T -e REDISCLI_AUTH=redispass redis-cli \
  redis-cli -h shard1-master --no-auth-warning --scan --pattern '*' | sort -u
docker compose exec -T -e REDISCLI_AUTH=redispass redis-cli \
  redis-cli -h shard2-master --no-auth-warning --scan --pattern '*' | sort -u
```

路由过程会打印到 stderr，例如：

```
[shard.sh] key='user:1' -> shard1 (shard1-master:6379)
```

限制：`shard.sh` 不会对 `MGET/MSET/DEL key1 key2` 等多 key 命令做跨分片聚合，
需要业务侧拆分；`scripts/client_sharding.py` 已演示 `MGET` / `DEL` 的按分片拆分。

## 故障转移验证

```bash
# 停掉 shard1 的 master
docker compose stop shard1-master

# 等待约 5-10 秒，Sentinel 会选出一个 replica 提升为 master
docker compose exec redis-cli sh /scripts/status.sh

# 通过 Sentinel 发现新 master 后，分片脚本无需改动即可继续读写
docker compose exec redis-cli sh /scripts/shard.sh SET user:1 alice-new
docker compose exec redis-cli sh /scripts/shard.sh GET user:1

# 恢复原 master（会作为新 master 的 replica 重新加入）
docker compose start shard1-master

# 验证整体重启后拓扑与数据仍然保持（保留卷）
docker compose down
docker compose up -d
docker compose exec redis-cli sh /scripts/status.sh
```

## 配置说明

核心参数在 `.env`：

```dotenv
REDIS_PASSWORD=redispass
SHARDS=2
```

- 密码同时用于 `requirepass`、`masterauth`、`sentinel auth-pass`。
- 每个 Sentinel 首次启动时由 `scripts/sentinel-entrypoint.sh` 生成
  `/data/sentinel.conf`，之后保留其重写后的配置（故障转移状态不会因重启丢失）。
- Sentinel 使用 `resolve-hostnames yes` / `announce-hostnames yes`，便于容器间按
  服务名发现节点。
- `scripts/redis-entrypoint.sh` 会在 Redis 节点启动时向本分片 Sentinel 查询当前
  master，再决定自己以 master 还是 replica 角色启动。因此 `docker compose down`
  后重新 `up`（保留数据卷）会保持故障转移后的角色，已写入的数据不会丢失。
- `docker compose down -v` 会删除全部数据卷，并把拓扑重置为初始的
  `shardN-master`。
- 宿主机端口固定映射到容器，但故障转移后节点角色会变化；判断当前 master 请以
  Sentinel 或 `scripts/status.sh` 的结果为准。

## 扩展分片

当前为 2 个分片。增加分片需要：

1. 在 `docker-compose.yml` 中复制一组 `shardN-*` 服务（命名卷、端口号递增）。
2. 更新 `.env` 的 `SHARDS`。
3. 扩容已有数据需要额外的迁移工具（如 `redis-shake`），本示例不自动 rehash。

## 更多文档

- 学生入门实验（场景、分步操作、故障转移、检查点）：[`LAB.md`](LAB.md)
- 详细运维文档（架构、全部配置参数、`shard.sh` 原理、`redis-cli` 命令、备份恢复、故障排查）：[`OPERATIONS.md`](OPERATIONS.md)
- 分片 / 故障转移 / 重启保持性验证脚本：[`test/README.md`](test/README.md)

```bash
./test/verify-sharding.sh
./test/verify-failover.sh
./test/verify-restart.sh
# 或一次执行全部
./test/run-all.sh
```

## 生产建议

- 客户端侧分片建议使用支持 Sentinel + 分片的一致性哈希库（Jedis/Lettuce 等），
  不要让脚本承担线上流量。
- 如需服务端自动分片与 rehash，请改用 **Redis Cluster**（至少 3 主），而不是 Sentinel。
- 对外暴露时请限制端口访问，并替换 `.env` 中的默认密码。
