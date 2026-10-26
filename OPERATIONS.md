# Redis 多分片 Sentinel 集群运维文档

本文档比 `README.md` 更完整，覆盖架构、配置参数、`shard.sh` 客户端分片脚本、
`redis-cli` 常用命令、日常运维、故障排查、备份恢复和验证脚本。

项目目录：`/home/opc/redis-sentinel-sharding`

---

## 1. 架构说明

### 1.1 总体拓扑

当前部署为 **2 个分片**，每个分片是一套独立的 **Redis Sentinel 高可用组**：

```
                        +----------------------+
                        |     redis-cli        |
                        |  (运维/分片脚本容器)  |
                        +----------+-----------+
                                   |
              +--------------------+--------------------+
              |                                         |
   ===== Shard 1 =====                       ===== Shard 2 =====
   shard1-master :6379                       shard2-master :6379
     |          |                              |          |
 shard1-      shard1-                       shard2-      shard2-
 replica1     replica2                      replica1     replica2
   :6379        :6379                         :6379        :6379
     ^          ^                              ^          ^
     |          |                              |          |
 shard1-      shard1-      shard1-          shard2-      shard2-      shard2-
 sentinel1    sentinel2    sentinel3        sentinel1    sentinel2    sentinel3
 :26379       :26379       :26379           :26379       :26379       :26379
```

- 每个分片：**1 主 + 2 从 + 3 哨兵**。
- 哨兵 quorum = **2**，即 3 个哨兵中至少 2 个认为 master 下线才触发故障转移。
- 总共 12 个 Redis/Sentinel 容器，外加 1 个 `redis-cli` 工具容器。
- 所有容器在同一个 Docker bridge 网络 `redis-sentinel-sharding_redisnet` 中，
  通过服务名互相发现。

### 1.2 为什么不是 Redis Cluster

Sentinel 和 Redis Cluster 解决的是两个不同问题：

| 能力 | Sentinel | Redis Cluster |
|------|----------|---------------|
| 主从复制 | 支持 | 支持 |
| 自动故障转移 | 支持 | 支持 |
| 服务端数据分片 | **不支持** | 支持 |
| 自动 rehash / 槽迁移 | **不支持** | 支持 |
| 客户端要求 | 普通客户端 + Sentinel 支持 | 必须支持 Cluster 协议 |
| 多 key 跨槽操作 | 需要业务拆分 | 受限，需要 hash tag |

因此本方案采用 **多分片 Sentinel + 客户端分片**：

- 每个分片内部由 Sentinel 负责高可用。
- 分片之间由客户端脚本（本项目的 `scripts/shard.sh`）或业务客户端负责路由。
- 不提供 Redis Cluster 的 `CLUSTER` 命令、槽位、自动 rehash 能力。

如果最终需要服务端自动分片和在线扩容，应改用 Redis Cluster（至少 3 主）。

### 1.3 组件与端口

| 容器 | 内部端口 | 宿主机端口 | 角色 |
|------|----------|------------|------|
| `shard1-master` | 6379 | 7001 | Shard 1 初始 master |
| `shard1-replica1` | 6379 | 7002 | Shard 1 副本 |
| `shard1-replica2` | 6379 | 7003 | Shard 1 副本 |
| `shard1-sentinel1` | 26379 | 27001 | Shard 1 哨兵 |
| `shard1-sentinel2` | 26379 | 27002 | Shard 1 哨兵 |
| `shard1-sentinel3` | 26379 | 27003 | Shard 1 哨兵 |
| `shard2-master` | 6379 | 7004 | Shard 2 初始 master |
| `shard2-replica1` | 6379 | 7005 | Shard 2 副本 |
| `shard2-replica2` | 6379 | 7006 | Shard 2 副本 |
| `shard2-sentinel1` | 26379 | 27004 | Shard 2 哨兵 |
| `shard2-sentinel2` | 26379 | 27005 | Shard 2 哨兵 |
| `shard2-sentinel3` | 26379 | 27006 | Shard 2 哨兵 |
| `redis-cli` | - | - | 运维工具容器，常驻 `sleep infinity` |

注意：

- 宿主机端口是**固定映射到容器**的。发生故障转移后，原来的 `shard1-master`
  容器可能已经变成 replica，端口 7001 仍然指向这个容器。
- 判断“当前 master”不要看容器名，要看 Sentinel 或 `scripts/status.sh`。

### 1.4 读写路径

写入一个 key（通过 `shard.sh`）的流程：

```
key
  -> cksum(key) % SHARDS + 1        选择分片
  -> 向该分片的 3 个 Sentinel 询问 master
  -> 得到当前 master（主机名/IP + 端口）
  -> redis-cli SET/GET/... 发往该 master
```

读取同理。副本默认 `replica-read-only yes`，因此：

- 写操作只能走 master。
- 如果业务需要读副本，需要业务客户端自己连接副本，并接受复制延迟。
- `shard.sh` 始终写/读当前 master，不自动做读写分离。

### 1.5 故障转移流程

以 `shard1-master` 宕机为例：

1. 3 个 Sentinel 持续对 master 和副本做 PING。
2. master 超过 `down-after-milliseconds`（5 秒）无响应，Sentinel 标记主观下线。
3. 至少 2 个 Sentinel 同意后，标记客观下线并开始选举 leader。
4. leader Sentinel 从 2 个副本中选择一个（复制偏移量、优先级等）提升为 master。
5. Sentinel 向其余副本发送 `REPLICAOF`，让它们复制新 master。
6. Sentinel 将新拓扑写入各自的 `/data/sentinel.conf`。
7. 客户端下次询问 Sentinel 时会得到新 master，`shard.sh` 自动跟随。

故障转移后，原 master 恢复时：

- `scripts/redis-entrypoint.sh` 会先向 Sentinel 查询当前 master。
- 发现当前 master 不是自己，因此以 **replica** 身份启动并同步新 master。
- 不会出现“原 master 以 master 身份重新加入导致双主”的情况。

### 1.6 重启时的角色恢复机制

Redis 节点不是使用写死的 `--replicaof` 启动的，而是由
`scripts/redis-entrypoint.sh` 在启动时动态决定角色：

```
读取自己的 hostname
  -> 推导 SHARD_NAME（shard1-xxx -> shard1）
  -> 轮询 shardN-sentinel1/2/3 的
     SENTINEL GET-MASTER-ADDR-BY-NAME shardN
  -> 如果返回的 master 是自己：以 master 启动
  -> 否则：以 replica 启动，并 REPLICAOF 返回的 master
  -> 始终设置 --replica-announce-ip <自己的 hostname>
```

这样做的目的：

- `docker compose down` 后重新 `up`（保留数据卷）时，保持故障转移后的拓扑。
- 避免静态 `--replicaof` 导致重启后角色回退、数据被旧 master 覆盖。
- `replica-announce-ip` 使用服务名，避免容器重建后 IP 变化导致 Sentinel
  记录失效。

### 1.7 数据持久化

- Redis：`appendonly yes`，`appendfsync everysec`。
- AOF 文件默认位于容器 `/data/appendonlydir/`。
- 同时保留 Redis 默认的 RDB 快照策略（未显式关闭 `save`）。
- 每个 Redis 节点有独立命名卷：
  `redis-sentinel-sharding_<service>-data`。
- 每个 Sentinel 也有独立命名卷，用于保存故障转移后的 `sentinel.conf`。

`docker compose down -v` 会删除所有数据卷，等于：

- 清空所有 Redis 数据；
- 清空 Sentinel 的故障转移状态；
- 下次启动回到初始拓扑（`shardN-master` 为 master）。

---

## 2. 部署与生命周期管理

### 2.1 启动

```bash
cd /home/opc/redis-sentinel-sharding
docker compose up -d
docker compose ps
```

首次启动需要拉取 `redis:7.4-alpine` 镜像。启动后等待约 10-20 秒，
让副本完成同步、Sentinel 发现所有节点。

### 2.2 查看集群状态

```bash
# 容器状态
docker compose ps

# master / replicas / key 数量
docker compose exec redis-cli sh /scripts/status.sh

# 某个分片的 Sentinel 视角
docker compose exec redis-cli redis-cli -h shard1-sentinel1 -p 26379 \
  sentinel master shard1
```

### 2.3 停止与销毁

```bash
# 停止但保留容器和数据
docker compose stop

# 停止并删除容器，保留数据卷
docker compose down

# 删除容器和全部数据卷（不可恢复，慎用）
docker compose down -v
```

### 2.4 重启单个节点

```bash
docker compose restart shard1-replica1

# 或 stop/start，保持容器和网络配置不变
docker compose stop shard1-replica1
docker compose start shard1-replica1
```

重启后 `redis-entrypoint.sh` 会重新向 Sentinel 确认角色。

### 2.5 日志

```bash
# 全部
docker compose logs -f --tail=100

# 单个节点
docker compose logs -f --tail=200 shard1-master
docker compose logs -f --tail=200 shard1-sentinel1

# 最近一次启动日志
docker compose logs --tail=200 shard1-sentinel1
```

---

## 3. 配置文件关键参数说明

### 3.1 `.env`

```dotenv
REDIS_PASSWORD=redispass
SHARDS=2
```

| 变量 | 作用 | 注意事项 |
|------|------|----------|
| `REDIS_PASSWORD` | Redis 的 `requirepass`、`masterauth`、Sentinel 的 `auth-pass` | 修改后需要按第 3.7 节流程处理，否则 Sentinel 配置中的旧密码会导致认证失败 |
| `SHARDS` | `shard.sh` / `status.sh` 使用的分片数 | 必须与实际分片数一致；修改会导致 key 重新取模，见 4.5 |

`.env` 由 Docker Compose 自动读取，同时 `test/*.sh` 也会读取。

### 3.2 `docker-compose.yml` 结构

文件使用 YAML 锚点减少重复：

- `x-redis-base`：所有 Redis 节点的公共配置。
  - 镜像、重启策略、网络、entrypoint、环境变量。
  - `command`：传给 `redis-server` 的启动参数。
  - `healthcheck`：使用带密码的 `PING`。
- `x-sentinel`：所有 Sentinel 的公共配置。
  - entrypoint 脚本、环境变量、健康检查。
- `services`：2 个分片的具体服务。
- `volumes`：12 个命名数据卷。

关键点：

- Redis 节点没有写死 `--replicaof`，由 `redis-entrypoint.sh` 动态决定。
- Sentinel 使用 `sentinel-entrypoint.sh` 生成或复用 `/data/sentinel.conf`。
- `restart: unless-stopped` 保证容器异常退出后自动重启。
- Redis 节点依赖本分片的 3 个 Sentinel 先启动（`service_started`），
  避免启动时查询不到 Sentinel；脚本本身也有重试。

### 3.3 Redis 启动参数

`x-redis-base.command` 中的参数：

| 参数 | 值 | 说明 |
|------|----|------|
| `--port` | `6379` | 容器内 Redis 端口 |
| `--bind` | `0.0.0.0` | 监听所有地址，配合 Docker 网络 |
| `--protected-mode` | `no` | 容器网络内需要跨容器访问；不要在不安全网络中这样部署 |
| `--requirepass` | `${REDIS_PASSWORD}` | 客户端连接密码 |
| `--masterauth` | `${REDIS_PASSWORD}` | 副本连接 master 的密码；**master 也配置**，以便被降级后仍能同步 |
| `--appendonly` | `yes` | 开启 AOF |
| `--appendfsync` | `everysec` | 每秒 fsync，兼顾性能与最多约 1 秒数据丢失 |
| `--replica-announce-ip` | 自己的 hostname | 由 `redis-entrypoint.sh` 追加，保证 Sentinel 记录稳定的服务名 |
| `--replicaof <host> <port>` | 动态 | 仅当 Sentinel 认为当前节点是副本时由脚本追加 |

### 3.4 Sentinel 配置参数

`scripts/sentinel-entrypoint.sh` 首次启动时生成 `/data/sentinel.conf`：

| 配置 | 值 | 说明 |
|------|----|------|
| `port` | `26379` | Sentinel 端口 |
| `bind` | `0.0.0.0` | 监听所有地址 |
| `dir` | `/data` | Sentinel 状态文件目录 |
| `protected-mode` | `no` | 容器网络内使用 |
| `sentinel resolve-hostnames` | `yes` | 允许用服务名作为节点地址 |
| `sentinel announce-hostnames` | `yes` | Sentinel 之间按主机名通告 |
| `sentinel monitor <name>` | `shard1` / `shard2` | 监控的分片名和初始 master |
| quorum | `2` | 3 个 Sentinel 中 2 个同意才客观下线 |
| `sentinel auth-pass` | `${REDIS_PASSWORD}` | 访问 Redis master/副本的密码 |
| `sentinel down-after-milliseconds` | `5000` | 5 秒无响应判定主观下线 |
| `sentinel failover-timeout` | `60000` | 故障转移相关超时 |
| `sentinel parallel-syncs` | `1` | 故障转移后同时向新 master 同步的副本数 |

Sentinel 会重写自己的配置文件（例如 `sentinel monitor` 指向新 master、
记录 known-replica / known-sentinel / epoch）。因此：

- 不要手工随意编辑运行中 Sentinel 的 `/data/sentinel.conf`。
- 需要彻底重置某个 Sentinel 时，删除对应数据卷后重建容器。

### 3.5 脚本关键环境变量

`scripts/redis-entrypoint.sh`：

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `SHARD_NAME` | 从 hostname 推导 | Redis 节点当前分片名 |
| `SENTINEL_PORT` | `26379` | 查询 Sentinel 的端口 |
| `SENTINELS_PER_SHARD` | `3` | 每个分片的 Sentinel 数量 |
| `WAIT_SECONDS` | `120` | 启动时等待 Sentinel 可用的最长时间 |

`scripts/shard.sh` / `scripts/status.sh`：

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `SHARDS` | `2` | 分片数，必须与 `.env` 一致 |
| `REDIS_PASSWORD` | 空或 `.env` 值 | Redis 密码 |
| `SENTINEL_PORT` | `26379` | Sentinel 端口 |
| `SENTINELS_PER_SHARD` | `3` | 轮询的 Sentinel 数量 |

### 3.6 修改密码

密码同时存在于：

1. Redis 节点的 `requirepass` / `masterauth`；
2. Sentinel 的 `sentinel auth-pass`；
3. `shard.sh` / `status.sh` 的环境变量。

修改步骤（会短暂中断服务）：

```bash
cd /home/opc/redis-sentinel-sharding

# 1. 修改 .env 中的 REDIS_PASSWORD
# 2. 删除 Sentinel 数据卷，使其用新密码重新生成配置
docker compose down
docker volume rm redis-sentinel-sharding_shard1-sentinel1-data \
  redis-sentinel-sharding_shard1-sentinel2-data \
  redis-sentinel-sharding_shard1-sentinel3-data \
  redis-sentinel-sharding_shard2-sentinel1-data \
  redis-sentinel-sharding_shard2-sentinel2-data \
  redis-sentinel-sharding_shard2-sentinel3-data

# 3. 重新启动（Redis 数据卷保留）
docker compose up -d
```

如果不删除 Sentinel 卷，旧 `sentinel.conf` 中仍然是旧 `auth-pass`，
Sentinel 将无法正确监控和故障转移。

### 3.7 修改分片数量

当前 `shard.sh` 使用 `hash % SHARDS` 的简单取模。**直接修改 `SHARDS`
会导致几乎所有已有 key 路由到不同分片，表现为“数据丢失”。**

增加分片的正确做法见 4.5。若只是新增一个空分片、不迁移旧数据，
旧 key 会有一部分无法按原路由读到，必须迁移或接受重新分布。

---

## 4. `shard.sh` 使用说明

脚本位置：`scripts/shard.sh`，在 `redis-cli` 容器内执行。

### 4.1 工作原理

```sh
HASH=$(printf '%s' "$KEY" | cksum | awk '{print $1}')
SHARD=$(( HASH % SHARDS + 1 ))
```

- 使用 POSIX `cksum`（CRC32）计算 key 的哈希。
- 取模 `SHARDS`，得到 1..N 的分片号。
- 依次询问 `shardN-sentinel1/2/3`：
  `SENTINEL GET-MASTER-ADDR-BY-NAME shardN`。
- 拿到当前 master 后，用 `redis-cli` 把原命令发给该 master。
- 认证由脚本自动加上 `-a $REDIS_PASSWORD --no-auth-warning`。
- 路由信息输出到 **stderr**，命令结果输出到 **stdout**。

关键特性：

- 自动跟随 Sentinel 故障转移，不需要修改脚本。
- 同一个 key 始终路由到同一个分片（前提是 `SHARDS` 不变）。
- 无中心代理，不存在额外单点。

### 4.2 用法

```bash
docker compose exec redis-cli sh /scripts/shard.sh <命令> <key> [参数...]
```

第二个参数必须是 key，用于路由；其余参数原样传给 `redis-cli`。

示例：

```bash
# 字符串
docker compose exec redis-cli sh /scripts/shard.sh SET user:1 alice
docker compose exec redis-cli sh /scripts/shard.sh GET user:1
docker compose exec redis-cli sh /scripts/shard.sh DEL user:1
docker compose exec redis-cli sh /scripts/shard.sh EXPIRE user:1 3600
docker compose exec redis-cli sh /scripts/shard.sh TTL user:1

# Hash
docker compose exec redis-cli sh /scripts/shard.sh HSET user:2 name bob age 20
docker compose exec redis-cli sh /scripts/shard.sh HGETALL user:2

# List
docker compose exec redis-cli sh /scripts/shard.sh LPUSH queue:1 a b c
docker compose exec redis-cli sh /scripts/shard.sh LRANGE queue:1 0 -1

# Set / ZSet
docker compose exec redis-cli sh /scripts/shard.sh SADD tags:1 a b
docker compose exec redis-cli sh /scripts/shard.sh ZADD rank:1 100 alice
```

查看路由：

```bash
docker compose exec redis-cli sh /scripts/shard.sh SET demo:1 x
# stderr: [shard.sh] key='demo:1' -> shard1 (shard1-master:6379)
# stdout: OK
```

### 4.3 输出与退出码

- stdout：Redis 命令本身的返回。
- stderr：`[shard.sh] key='...' -> shardN (host:port)` 路由日志。
- 找不到任何 Sentinel 的 master 时，脚本以非 0 退出并打印错误。
- Redis 命令失败时，`redis-cli` 的退出码会传播给脚本（非 0）。

脚本依赖 `cksum`、`sed`、`awk`，这些在 `redis:7.4-alpine` 中都有。

### 4.4 限制

1. **只支持单 key 命令**
   `MGET k1 k2`、`MSET k1 v1 k2 v2`、`DEL k1 k2` 等不会跨分片聚合。
   业务需要拆成多次单 key 调用。
2. **不支持跨分片事务 / Lua**
   不同 key 可能在不同 master，无法用 `MULTI/EXEC` 包裹。
3. **没有自动 rehash**
   修改 `SHARDS` 会重新取模，旧 key 路由改变。
4. **不是 Redis Cluster**
   不要使用 `redis-cli -c`、`CLUSTER` 命令、`{hash tag}` 等 Cluster 语义。
5. **读操作也走 master**
   脚本不做读写分离；需要读副本时请在业务客户端实现。
6. **哈希算法是简单取模**
   增删分片会导致大量 key 迁移，生产建议使用一致性哈希或支持分片迁移的客户端。

### 4.5 分片扩容 / 缩容

当前脚本的 `hash % SHARDS` 不具备一致性哈希特性。扩容步骤示例
（从 2 分片扩到 3 分片）：

1. 在 `docker-compose.yml` 新增 `shard3-*` 一组服务（1 主 2 从 3 哨兵），
   使用新的卷名和宿主机端口。
2. 更新 `.env` 的 `SHARDS=3`。
3. `docker compose up -d` 创建新节点和重建 `redis-cli` 容器。
4. **迁移数据**：按旧路由和新路由计算每个 key 的新位置，把需要移动的 key
   从旧分片迁移到新分片。可使用 `redis-shake`、`MIGRATE` 或自研脚本。
5. 迁移完成并校验后，客户端再切换到 `SHARDS=3`。

⚠️ 在没有迁移数据的情况下直接改 `SHARDS`，大量 key 会被路由到错误分片。
如果业务不能停机迁移，应考虑：

- 使用一致性哈希的客户端（例如 Jedis/Lettuce 的 sharded 实现）并只迁移
  受影响区间；
- 或直接改用 Redis Cluster（服务端自动分片和 rehash）。

### 4.6 生产替代方案

`shard.sh` 适合运维和验证，不建议承载线上高并发流量。生产建议：

- Java：Jedis / Lettuce 的 Sentinel + sharding 方案，或 Redisson。
- 其他语言：使用支持 Sentinel 发现和客户端分片的库。
- 需要服务端自动分片：使用 Redis Cluster + 支持 Cluster 的客户端。
- 需要代理层：Twemproxy、Envoy Redis Proxy 等，但要确认其对 Sentinel
  故障转移的支持程度。

---

## 5. `redis-cli` 常用命令

### 5.1 连接与认证

推荐在容器内操作：

```bash
# 进入 redis-cli 工具容器
docker compose exec redis-cli sh

# 连接当前分片 master（先查 Sentinel）
docker compose exec redis-cli redis-cli -h shard1-master -a redispass --no-auth-warning

# 或直接执行单条命令
docker compose exec redis-cli redis-cli -h shard1-master -a redispass --no-auth-warning ping
```

避免密码进入 shell 历史，可以使用 `REDISCLI_AUTH`：

```bash
docker compose exec -e REDISCLI_AUTH=redispass redis-cli \
  redis-cli -h shard1-master --no-auth-warning ping
```

宿主机直连发布端口：

```bash
redis-cli -h 127.0.0.1 -p 7001 -a redispass --no-auth-warning
```

再次强调：7001 固定映射到 `shard1-master` 容器，故障转移后它可能已是副本。

### 5.2 基础命令

| 命令 | 说明 |
|------|------|
| `PING` | 连通性检查 |
| `AUTH <password>` | 交互式认证 |
| `SELECT <db>` | 切换数据库（本方案客户端分片，不依赖 db 隔离） |
| `DBSIZE` | 当前库 key 数量 |
| `GET key` / `SET key value` | 字符串读写 |
| `DEL key [key...]` | 删除 key（多 key 跨分片需拆分） |
| `EXISTS key` | 是否存在 |
| `TYPE key` | 数据类型 |
| `TTL key` / `PTTL key` | 剩余过期时间 |
| `EXPIRE key seconds` | 设置过期 |
| `PERSIST key` | 移除过期时间 |
| `SCAN cursor MATCH pattern COUNT n` | 渐进式遍历，生产替代 `KEYS` |
| `KEYS pattern` | 全量遍历，**生产禁用**（阻塞） |
| `RANDOMKEY` | 随机 key |
| `RENAME key newkey` | 重命名（需同分片） |

### 5.3 各数据类型

```bash
# Hash
HSET user:1 name alice age 20
HGET user:1 name
HGETALL user:1
HDEL user:1 age

# List
LPUSH queue:1 a b c
RPUSH queue:1 d
LRANGE queue:1 0 -1
LPOP queue:1

# Set
SADD tags:1 a b c
SMEMBERS tags:1
SISMEMBER tags:1 a

# Sorted Set
ZADD rank:1 100 alice 200 bob
ZRANGE rank:1 0 -1 WITHSCORES
ZRANK rank:1 bob

# Bitmap / HyperLogLog
SETBIT b:1 7 1
GETBIT b:1 7
PFADD hll:1 a b c
PFCOUNT hll:1
```

### 5.4 复制与 Sentinel

```bash
# 查看当前节点复制状态
INFO replication
ROLE

# 查看 master 下的副本
docker compose exec redis-cli redis-cli -h shard1-master -a redispass --no-auth-warning \
  info replication

# Sentinel 状态
docker compose exec redis-cli redis-cli -h shard1-sentinel1 -p 26379 sentinel masters
docker compose exec redis-cli redis-cli -h shard1-sentinel1 -p 26379 sentinel master shard1
docker compose exec redis-cli redis-cli -h shard1-sentinel1 -p 26379 sentinel replicas shard1
docker compose exec redis-cli redis-cli -h shard1-sentinel1 -p 26379 sentinel sentinels shard1

# 查询当前 master 地址
docker compose exec redis-cli redis-cli -h shard1-sentinel1 -p 26379 \
  sentinel get-master-addr-by-name shard1

# 检查是否有足够 Sentinel 达成 quorum
docker compose exec redis-cli redis-cli -h shard1-sentinel1 -p 26379 \
  sentinel ckquorum shard1

# 手动触发故障转移
docker compose exec redis-cli redis-cli -h shard1-sentinel1 -p 26379 \
  sentinel failover shard1
```

`INFO replication` 关键字段：

| 字段 | 含义 |
|------|------|
| `role:master` / `role:slave` | 当前节点角色 |
| `connected_slaves` | master 的副本数 |
| `master_host` / `master_port` | 副本指向的 master |
| `master_link_status:up/down` | 副本与 master 的连接状态 |
| `master_last_io_seconds_ago` | 最近一次与 master 通信的时间 |
| `slave_repl_offset` / `master_repl_offset` | 复制偏移量，差距过大说明延迟 |
| `slave_read_only` | 副本是否只读 |

### 5.5 诊断与监控

```bash
# 服务信息
INFO
INFO server
INFO clients
INFO memory
INFO persistence
INFO stats
INFO replication
INFO keyspace
INFO sentinel

# 慢查询
SLOWLOG GET 10
SLOWLOG LEN
SLOWLOG RESET

# 延迟
LATENCY LATEST
LATENCY HISTORY <event>
LATENCY RESET

# 客户端
CLIENT LIST
CLIENT INFO

# 内存
MEMORY USAGE key
MEMORY STATS
MEMORY DOCTOR

# 持久化
LASTSAVE
BGSAVE
BGREWRITEAOF

# 实时命令流（排障用，注意性能）
MONITOR
```

### 5.6 危险命令

以下命令在生产环境要非常谨慎：

| 命令 | 风险 |
|------|------|
| `FLUSHDB` / `FLUSHALL` | 清空数据，不可恢复 |
| `KEYS *` | 阻塞主线程，数据量大时可能导致雪崩 |
| `CONFIG SET` | 直接改运行参数，可能破坏 Sentinel 预期 |
| `CONFIG REWRITE` | 覆盖当前配置，需确认内容 |
| `REPLICAOF NO ONE` | 把一个副本提升为独立 master，可能造成双主/数据分叉 |
| `SLAVEOF` / `REPLICAOF` | 手工改复制关系，Sentinel 可能再次纠正，但期间可能数据异常 |
| `SHUTDOWN` | 关闭节点，触发故障转移 |
| `DEBUG` | 调试命令，生产禁用 |
| `MIGRATE` | 跨实例迁移 key，参数错误可能丢数据 |

---

## 6. 日常运维

### 6.1 健康检查清单

```bash
cd /home/opc/redis-sentinel-sharding

# 1. 容器都健康
docker compose ps

# 2. 两个分片都有 master，副本在线
docker compose exec redis-cli sh /scripts/status.sh

# 3. 每个分片 Sentinel quorum 正常
for s in 1 2; do
  docker compose exec redis-cli redis-cli -h shard${s}-sentinel1 -p 26379 \
    sentinel ckquorum shard${s}
done

# 4. 副本复制正常
docker compose exec redis-cli redis-cli -h shard1-master -a redispass --no-auth-warning \
  info replication
docker compose exec redis-cli redis-cli -h shard2-master -a redispass --no-auth-warning \
  info replication

# 5. 磁盘空间
df -h /
docker system df
```

### 6.2 计划内主从切换

```bash
# 1. 确认 quorum 正常
docker compose exec redis-cli redis-cli -h shard1-sentinel1 -p 26379 \
  sentinel ckquorum shard1

# 2. 手动触发切换
docker compose exec redis-cli redis-cli -h shard1-sentinel1 -p 26379 \
  sentinel failover shard1

# 3. 观察新 master
docker compose exec redis-cli sh /scripts/status.sh

# 4. 确认旧 master 成为副本且 master_link_status:up
docker compose exec redis-cli redis-cli -h shard1-master -a redispass --no-auth-warning \
  info replication

# 5. 之后可以安全维护旧 master（例如升级、迁移）
```

### 6.3 增加副本

1. 在 `docker-compose.yml` 复制一个 `shardN-replicaX` 服务：
   - 新的容器名和 hostname，必须以 `shardN-` 开头；
   - 新的命名卷；
   - 新的宿主机端口（或删除 ports，仅内网访问）；
   - `depends_on` 本分片 3 个 Sentinel。
2. `docker compose up -d`。
3. 新节点启动时由 `redis-entrypoint.sh` 查询 Sentinel，自动以副本身份
   复制当前 master。
4. master 的 `INFO replication` 中出现新副本后，Sentinel 会自动发现它。
5. `sentinel replicas shardN` 确认。

### 6.4 移除副本

```bash
# 先停止，避免 Sentinel 看到它
docker compose stop shard1-replica2
docker compose rm -f shard1-replica2

# 可选：清理数据卷（确认不再需要）
docker volume rm redis-sentinel-sharding_shard1-replica2-data
```

Sentinel 会把该副本标记为 down。若想清理 Sentinel 的 known-replica 记录，
可以在**每个** Sentinel 上执行 `SENTINEL RESET shardN`，但要了解这会触发
重新发现，短时间内监控状态会抖动。

### 6.5 备份

#### 方式一：RDB 快照

```bash
# 在每个 master 上触发快照
docker compose exec redis-cli redis-cli -h shard1-master -a redispass --no-auth-warning BGSAVE
docker compose exec redis-cli redis-cli -h shard2-master -a redispass --no-auth-warning BGSAVE

# 等待完成
docker compose exec redis-cli redis-cli -h shard1-master -a redispass --no-auth-warning LASTSAVE
docker compose exec redis-cli redis-cli -h shard2-master -a redispass --no-auth-warning LASTSAVE

# 从容器复制出来
docker cp shard1-master:/data/dump.rdb ./backup-shard1-dump.rdb
docker cp shard2-master:/data/dump.rdb ./backup-shard2-dump.rdb
```

#### 方式二：文件级备份 AOF（推荐一致性要求高时停机）

```bash
# 1. 停止写入（业务侧）
# 2. 等待 AOF fsync
docker compose exec redis-cli redis-cli -h shard1-master -a redispass --no-auth-warning WAITAOF 1 0 5000
docker compose exec redis-cli redis-cli -h shard2-master -a redispass --no-auth-warning WAITAOF 1 0 5000

# 3. 停整个栈，保证文件一致
docker compose stop

# 4. 备份数据卷目录（示例路径，可用 docker volume inspect 确认）
docker volume inspect redis-sentinel-sharding_shard1-master-data
sudo tar -czf backup-redis-data-$(date +%F).tar.gz \
  /var/lib/docker/volumes/redis-sentinel-sharding_shard1-master-data/_data \
  /var/lib/docker/volumes/redis-sentinel-sharding_shard2-master-data/_data

# 5. 启动
docker compose start
```

生产建议使用存储层快照或专门的备份工具，并定期做恢复演练。

### 6.6 恢复

1. `docker compose down`（保留或删除现有卷，取决于恢复策略）。
2. 将备份的 AOF/RDB 文件放回对应节点的数据卷。
3. 如果同时恢复的是整个集群，建议**同时恢复 Sentinel 数据卷**，
   保持 master 拓扑与数据一致；或者清空 Sentinel 卷让拓扑回到初始状态，
   但这样可能和备份数据的角色不一致，需要人工确认。
4. `docker compose up -d`。
5. 用 `scripts/status.sh` 和测试脚本验证数据。

恢复前务必确认：

- 备份来自哪个分片、哪个 master；
- 当前期望的拓扑是什么；
- Sentinel 记录的 master 是否与恢复的数据匹配。

### 6.7 监控指标建议

| 指标 | 来源 | 建议 |
|------|------|------|
| 节点存活 | `PING` / 容器健康 | 连续失败告警 |
| 复制状态 | `INFO replication` 的 `master_link_status` | 出现 `down` 告警 |
| 复制延迟 | `master_repl_offset - slave_repl_offset` | 超过阈值告警 |
| 内存 | `INFO memory` 的 `used_memory` | 结合 maxmemory 设置 |
| 连接数 | `INFO clients` 的 `connected_clients` | 接近上限告警 |
| 命中率 | `keyspace_hits / (hits+misses)` | 明显下降时排查 |
| 慢查询 | `SLOWLOG LEN` | 持续增长时排查 |
| 持久化 | `INFO persistence` 的 `aof_last_bgrewrite_status`、`rdb_last_bgsave_status` | 非 ok 告警 |
| Sentinel | `INFO sentinel`、`SENTINEL MASTER` 的 flags | 出现 `s_down` / `o_down` 告警 |
| 磁盘 | `df`、`docker system df` | 使用率超阈值告警 |

本项目未内置 Prometheus exporter。生产可加 `redis_exporter`，分别抓取
每个 Redis 节点和 Sentinel。

---

## 7. 故障排查

### 7.1 常见问题

| 现象 | 可能原因 | 处理 |
|------|----------|------|
| `NOAUTH Authentication required` | 密码不一致或未带 `-a` / `REDISCLI_AUTH` | 检查 `.env`、Sentinel 配置中的 `auth-pass` |
| `Could not connect ... Name does not resolve` | 容器未启动、服务名错误、网络异常 | `docker compose ps`、`docker compose logs`、检查 hostname |
| Sentinel 日志 `Failed to resolve hostname 'shardN-master'` | 对应 master 容器停止或正在重建 | 启动/等待容器；故障转移期间出现属正常 |
| Sentinel 不触发故障转移 | quorum 不足、Sentinel 之间网络不通、master 仍可响应 | `sentinel ckquorum shardN`，检查 3 个 Sentinel 日志 |
| 副本 `master_link_status:down` | masterauth 错误、master 不可达、正在全量同步 | 检查日志、`CONFIG GET masterauth`、等待同步完成 |
| 副本日志 `MASTER aborted replication ... NOAUTH` | 副本没有正确的 `masterauth` | 确认节点 command 包含 `--masterauth`；当前由 compose 统一配置 |
| 修改 `SHARDS` 后“数据丢失” | 取模变化导致 key 路由到其他分片 | 恢复原 `SHARDS`，或迁移数据后再切换 |
| `down -v` 后数据全没 | 数据卷被删除 | 这是预期行为，需从备份恢复 |
| 宿主机端口被占用 | 7001-7006 / 27001-27006 与其他服务冲突 | 修改 `docker-compose.yml` 左侧宿主机端口 |
| 容器不断重启 | 密码/配置错误、端口冲突、磁盘满、Sentinel 不可达 | `docker compose logs`、`df -h` |
| 出现两个 master | 网络分区或手工 `REPLICAOF NO ONE` | 检查 Sentinel 日志；让 quorum 恢复；避免手工提升 |
| AOF 文件过大 | 长时间未重写 | `BGREWRITEAOF`，并检查 `auto-aof-rewrite-percentage` |
| 内存持续增长 / OOM | 未设置 maxmemory 或淘汰策略 | 配置 `maxmemory` 和 `maxmemory-policy`，监控内存 |
| `redis-cli -c` 行为异常 | `-c` 是 Cluster 模式，本方案不是 Cluster | 去掉 `-c`，使用 `shard.sh` 或直连分片 master |

### 7.2 定位问题的顺序

1. `docker compose ps`：容器是否 running / healthy。
2. `docker compose logs --tail=200 <container>`：启动错误、认证错误、复制错误。
3. `scripts/status.sh`：Sentinel 认为的 master 和副本状态。
4. 直连疑似异常节点：`PING`、`INFO replication`、`INFO persistence`。
5. 检查 Sentinel：`SENTINEL MASTER`、`SENTINEL REPLICAS`、`CKQUORUM`。
6. 检查宿主机：磁盘、内存、网络、时间同步（NTP）。
7. 用 `test/` 下的脚本复现和验证。

### 7.3 单主机部署的局限

当前所有容器在**同一台宿主机**上：

- 宿主机宕机时，主从和 Sentinel 会一起失效，无法真正实现机房级高可用。
- Sentinel quorum 只能防止单进程/单容器故障，不能防止宿主机故障。
- 生产环境应将 master、replica、sentinel 分散到不同物理机/可用区，
  并使用独立网络和存储。

---

## 8. 验证脚本

`test/` 目录提供三个脚本和一个汇总脚本：

| 脚本 | 作用 |
|------|------|
| `test/verify-sharding.sh` | 写入多 key，验证读写正确且数据分布到两个分片 |
| `test/verify-failover.sh` | 停 master，验证 Sentinel 选主、数据可读、旧 master 回归 |
| `test/verify-restart.sh` | 整体 down/up 后，验证 master 容器和数据保持不变 |
| `test/run-all.sh` | 依次执行以上三个 |

```bash
cd /home/opc/redis-sentinel-sharding
chmod +x test/*.sh

./test/verify-sharding.sh
./test/verify-failover.sh
./test/verify-restart.sh
# 或
./test/run-all.sh
```

详细说明见 `test/README.md`。

注意：

- `verify-failover.sh` 会真实停掉一个 master 并触发选主，执行后拓扑会改变。
- `verify-restart.sh` 会短暂停止全部容器（保留数据卷）。
- 测试脚本会自行清理测试 key，但不会自动把故障转移后的 master 切回原节点。

---

## 9. 安全建议

当前配置是**内网/演示可用**，生产需要加强：

1. 修改默认密码 `redispass`，使用强密码并妥善管理。
2. Sentinel 当前没有设置密码，且 `protected-mode no`、`bind 0.0.0.0`。
   不要把 26379 端口暴露到不可信网络；必要时为 Sentinel 配置 `requirepass`
   并同步修改客户端脚本。
3. 宿主机发布端口（7001-7006、27001-27006）增加了暴露面。生产建议：
   - 只让应用容器通过内部网络访问，不发布宿主机端口；
   - 或使用防火墙限制来源 IP。
4. 未启用 TLS。跨主机部署时应启用 Redis TLS 和 Sentinel TLS。
5. `.env` 中明文密码仅适合演示。生产使用 Docker secrets、KMS 或配置中心。
6. 使用 Redis ACL 按应用划分用户和命令权限，避免所有应用共用 default 用户。
7. 限制危险命令（`FLUSHALL`、`CONFIG`、`DEBUG`、`SHUTDOWN` 等）。
8. 定期轮换密码，并按第 3.6 节流程操作。

---

## 10. 已知限制

- Sentinel 不提供数据分片，分片由客户端负责。
- 无自动 rehash / 在线扩容；修改 `SHARDS` 会影响 key 路由。
- 不支持跨分片多 key 命令、事务和 Lua。
- 单宿主机部署无法抵御宿主机级故障。
- `appendfsync everysec` 在极端断电情况下可能丢失最多约 1 秒写入。
- 没有内置监控、告警、限流、TLS、ACL 和资源限制，需要按生产要求补充。
- `shard.sh` 仅用于运维/验证，不建议作为线上流量入口。

---

## 11. 常用命令速查

```bash
cd /home/opc/redis-sentinel-sharding

# 启停
docker compose up -d
docker compose stop
docker compose down          # 保留数据
docker compose down -v       # 删除数据，慎用

# 状态
docker compose ps
docker compose exec redis-cli sh /scripts/status.sh

# 分片读写
docker compose exec redis-cli sh /scripts/shard.sh SET user:1 alice
docker compose exec redis-cli sh /scripts/shard.sh GET user:1

# 查当前 master
docker compose exec redis-cli redis-cli -h shard1-sentinel1 -p 26379 \
  sentinel get-master-addr-by-name shard1

# 复制状态
docker compose exec redis-cli redis-cli -h shard1-master -a redispass --no-auth-warning \
  info replication

# 哨兵状态
docker compose exec redis-cli redis-cli -h shard1-sentinel1 -p 26379 sentinel master shard1
docker compose exec redis-cli redis-cli -h shard1-sentinel1 -p 26379 sentinel replicas shard1
docker compose exec redis-cli redis-cli -h shard1-sentinel1 -p 26379 sentinel ckquorum shard1

# 手动故障转移
docker compose exec redis-cli redis-cli -h shard1-sentinel1 -p 26379 sentinel failover shard1

# 日志
docker compose logs -f --tail=200 shard1-master shard1-sentinel1

# 验证
./test/verify-sharding.sh
./test/verify-failover.sh
./test/verify-restart.sh
./test/run-all.sh
```
