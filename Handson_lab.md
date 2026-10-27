# Hands-on Lab：Redis Sentinel 多分片高可用入门

> **适用环境**：`/home/opc/redis-sentinel-sharding`（2 个分片，每分片 1 主 2 从 3 哨兵）<br>
> **难度**：入门级<br>
> **建议时长**：90 分钟<br>
> **前置知识**：Linux 命令行基础；会使用 `SET` / `GET` 即可<br>
> **实验数据**：全部是演示数据，可以随时重建

---

## 1. 实验场景

你是一所大学“校园活动报名系统”的后端实习生。系统使用 Redis 保存：

- 学生登录会话，例如 `session:1001`
- 活动余量，例如 `activity:2001:quota`
- 报名状态，例如 `signup:2001:1001`

最近活动报名流量变大，单台 Redis 有两个问题：

1. **容量和压力集中**：所有 key 都在一台 Redis 上；
2. **单点故障**：这台 Redis 挂了，报名系统就不能读写。

团队决定采用：

- **2 个 Redis 分片**，把 key 分散到不同机器；
- **每个分片 1 主 2 从**，主节点故障时自动切换；
- **每个分片 3 个 Sentinel**，负责监控和自动选主；
- **客户端分片**：业务根据 key 的哈希选择分片，再向该分片的 Sentinel 查询当前 master。

你的任务是：启动环境、验证分片、模拟 master 故障、观察 Sentinel 自动选主，并确认重启后数据仍然存在。

> 本实验是 **Sentinel + 客户端分片**，不是 Redis Cluster。
> Sentinel 不提供 slot、自动 rehash 或服务端分片，这些需要额外代理或 Redis Cluster。

---

## 2. 你将学到

完成本实验后，你应该能够：

- [ ] 启动并检查 12 个 Redis/Sentinel 容器和 1 个 `redis-cli` 工具容器；
- [ ] 使用 Sentinel 查询每个分片当前的 master；
- [ ] 使用 `shard.sh` 按 key 路由，并验证 key 只存在于一个分片；
- [ ] 手动停止 master，观察 Sentinel 自动提升 replica；
- [ ] 观察旧 master 重启后作为 replica 重新加入；
- [ ] 验证 `docker compose down` / `up` 后拓扑和数据仍然保持；
- [ ] 解释 Sentinel、客户端分片和 Redis Cluster 的区别。

---

## 3. 实验环境与拓扑

### 3.1 项目目录

```text
/home/opc/redis-sentinel-sharding/
├── docker-compose.yml       # 2 个分片的完整定义
├── .env                     # REDIS_PASSWORD=redispass, SHARDS=2
├── scripts/
│   ├── shard.sh             # 运维用客户端分片脚本
│   ├── status.sh            # 查看每个分片的 master / 副本 / key 数
│   ├── client_sharding.py   # Python 客户端分片示例（选做）
│   └── ...
├── test/                    # 自动化验证脚本
├── README.md
├── OPERATIONS.md
└── Handson_lab.md          # 本文档
```

### 3.2 拓扑图

```text
                        客户端 / redis-cli 工具容器
                                  |
                     key -> cksum(key) % 2 + 1
                                  |
              +-------------------+-------------------+
              |                                       |
       ===== Shard 1 =====                     ===== Shard 2 =====
       shard1-master:6379                      shard2-master:6379
         |            |                          |            |
   shard1-       shard1-                    shard2-       shard2-
   replica1      replica2                   replica1      replica2
     :6379        :6379                      :6379        :6379
       ^            ^                          ^            ^
       |            |                          |            |
   shard1-      shard1-      shard1-       shard2-      shard2-      shard2-
   sentinel1    sentinel2    sentinel3     sentinel1    sentinel2    sentinel3
    :26379       :26379       :26379        :26379       :26379       :26379
```

### 3.3 端口表

| 分片 | master | replica1 | replica2 | Sentinel |
|------|--------|----------|----------|----------|
| shard1 | 宿主机 `7001` | `7002` | `7003` | `27001` - `27003` |
| shard2 | 宿主机 `7004` | `7005` | `7006` | `27004` - `27006` |

容器内部端口：Redis `6379`，Sentinel `26379`。

### 3.4 关键配置

| 配置 | 值 | 含义 |
|------|----|------|
| `SHARDS` | `2` | 分片数量 |
| Sentinel quorum | `2` | 3 个 Sentinel 中至少 2 个认为 master 下线才切换 |
| `down-after-milliseconds` | `5000` | 5 秒无响应判定主观下线 |
| `failover-timeout` | `60000` | 故障转移相关超时 |
| 持久化 | AOF `everysec` | 每秒刷盘，重启后恢复数据 |
| Redis 密码 | `redispass` | 演示密码，生产环境不要使用 |

---

## 4. 安全与约定

1. **不要修改 `.env` 和 `SHARDS`**。本实验固定为 2 个分片，修改会导致 key 重新取模。
2. **不要使用 `KEYS *`**。它会阻塞 Redis；本实验统一使用 `SCAN`。
3. **不要随便执行 `docker compose down -v`**。`-v` 会删除所有数据卷。只在“重置实验”时使用。
4. **容器名 ≠ 当前 master**。故障转移后，`shard1-master` 容器可能已经是 replica。
   判断当前 master 要看 Sentinel 或 `status.sh` 的结果。
5. **故障转移需要等待**。停止 master 后通常要等 10-30 秒，不要连续快速执行命令。
6. 所有命令都在项目目录 `/home/opc/redis-sentinel-sharding` 下执行。

---

## 5. 准备工作

### 5.1 检查环境

```bash
cd /home/opc/redis-sentinel-sharding

docker --version
docker compose version
cat .env
```

你应该看到：

```dotenv
REDIS_PASSWORD=redispass
SHARDS=2
```

### 5.2 启动集群

```bash
docker compose up -d
docker compose ps
```

首次启动会创建 12 个 Redis/Sentinel 容器和 1 个 `redis-cli` 工具容器。等待约 10-30 秒，直到状态都是 `running` / `healthy`。

如果你的环境不是初始状态（例如上一次实验留下的数据，或 master 已经不是
`shard1-master` / `shard2-master`），先按本文 **11.3 完全重置** 再开始。

### 5.3 加载两个快捷函数

为了避免每条命令都写密码，本实验环境已经把两个快捷函数写入 `~/.profile`。
新开终端后先加载：

```bash
source ~/.profile
```

函数内容如下，阅读即可，不需要重复粘贴：

```bash
# Redis Sentinel sharding lab helpers.
# These functions let the handson lab commands work from any directory.
export REDIS_LAB_DIR="/home/opc/redis-sentinel-sharding"

redis_lab_password() {
  awk -F= '$1=="REDIS_PASSWORD"{print $2}' "$REDIS_LAB_DIR/.env"
}

rc() {
  local password="${REDIS_LAB_PASSWORD:-$(redis_lab_password)}"
  docker compose --project-directory "$REDIS_LAB_DIR" \
    -f "$REDIS_LAB_DIR/docker-compose.yml" \
    exec -T -e REDISCLI_AUTH="$password" redis-cli \
    redis-cli --no-auth-warning "$@"
}

sc() {
  docker compose --project-directory "$REDIS_LAB_DIR" \
    -f "$REDIS_LAB_DIR/docker-compose.yml" \
    exec -T redis-cli \
    redis-cli -p 26379 "$@"
}
```

说明：

- `rc` 用于访问 Redis 数据节点，例如 `rc -h shard1-master INFO replication`；
- `sc` 用于访问 Sentinel，例如 `sc -h shard1-sentinel1 sentinel ...`；
- 函数使用 `REDIS_LAB_DIR` 指向项目目录，所以可以从任意目录执行；
- `rc` 会自动读取 `.env` 中的 `REDIS_PASSWORD`；
- 如果要临时覆盖密码，可以执行 `export REDIS_LAB_PASSWORD=新密码`；
- 如果新开终端后提示 `rc: command not found`，先执行 `source ~/.profile`。

> 后面的命令会大量使用 `rc` 和 `sc`。第一次看到时如果不放心，可以把
> `rc` 展开成完整的 `docker compose --project-directory ... exec ... redis-cli ...` 命令。

---

## 6. 实验一：认识 Sentinel 拓扑

### 步骤 1：查看整体状态

```bash
docker compose exec -T redis-cli sh /scripts/status.sh
```

初始状态下应该类似：

```text
===== shard1 =====
  master : shard1-master:6379
  via    : shard1-sentinel1
  role   : master
  keys   : 0
  replicas:
    - shard1-replica1:6379 [slave]
    - shard1-replica2:6379 [slave]

===== shard2 =====
  master : shard2-master:6379
  via    : shard2-sentinel1
  role   : master
  keys   : 0
  replicas:
    - shard2-replica1:6379 [slave]
    - shard2-replica2:6379 [slave]
```

观察并填写：

| 项目 | shard1 | shard2 |
|------|--------|--------|
| Sentinel 报告的 master | | |
| master 节点角色 | | |
| replica 数量 | | |

### 步骤 2：直接问 Sentinel

```bash
sc -h shard1-sentinel1 sentinel get-master-addr-by-name shard1
sc -h shard2-sentinel1 sentinel get-master-addr-by-name shard2
```

输出是两行：

```text
shard1-master
6379
```

第一行是 master 主机名，第二行是端口。

### 步骤 3：查看 master 和副本

```bash
rc -h shard1-master INFO replication
```

重点关注：

```text
role:master
connected_slaves:2
slave0:ip=...,port=6379,state=online,...
slave1:ip=...,port=6379,state=online,...
```

再查看一个副本：

```bash
rc -h shard1-replica1 INFO replication
```

重点关注：

```text
role:slave
master_host:shard1-master
master_link_status:up
```

> `master_host` 和 IP 可能随启动顺序、故障转移而变化；以 Sentinel 的结果为准。

### 检查点

- [ ] 能说出两个分片当前 master 的容器名；
- [ ] 能说出每个分片有几个 replica；
- [ ] 能解释为什么 3 个 Sentinel 的 quorum 是 2。

---

## 7. 实验二：验证客户端分片

### 步骤 1：写入 3 个学生会话

```bash
docker compose exec -T redis-cli sh /scripts/shard.sh SET student:1001 alice
docker compose exec -T redis-cli sh /scripts/shard.sh SET student:1002 bob
docker compose exec -T redis-cli sh /scripts/shard.sh SET student:1003 carol
```

输出示例：

```text
[shard.sh] key='student:1001' -> shard2 (shard2-master:6379)
OK
[shard.sh] key='student:1002' -> shard1 (shard1-master:6379)
OK
[shard.sh] key='student:1003' -> shard1 (shard1-master:6379)
OK
```

注意：

- `[shard.sh] ...` 路由日志输出到 **stderr**；
- `OK` 是 Redis 命令结果，输出到 **stdout**；
- 在当前 2 分片、`cksum` 算法下，这 3 个 key 的分布是固定的。

### 步骤 2：查看分片后的 key 数

```bash
docker compose exec -T redis-cli sh /scripts/status.sh
```

预期：

```text
shard1 keys = 2
shard2 keys = 1
```

这里 `DBSIZE` 只能说明“每个分片有多少 key”，不能说明具体是哪些 key。

### 步骤 3：确认同一个 key 只在一个分片

```bash
for h in shard1-master shard2-master; do
  for k in student:1001 student:1002 student:1003; do
    printf '%-15s %-15s ' "$h" "$k"
    rc -h "$h" EXISTS "$k"
  done
done
```

预期结果：

```text
shard1-master   student:1001    0
shard1-master   student:1002    1
shard1-master   student:1003    1
shard2-master   student:1001    1
shard2-master   student:1002    0
shard2-master   student:1003    0
```

`1` 表示存在，`0` 表示不存在。可以看到每个 key 只出现在一个分片。

### 步骤 4：用 SCAN 列出具体 key

不要使用 `KEYS *`。使用非阻塞的 `SCAN`：

```bash
for h in shard1-master shard2-master; do
  echo "===== $h ====="
  rc -h "$h" --scan --pattern 'student:*' | sort -u
done
```

也可以统计每个分片的 `student:*` 数量：

```bash
for h in shard1-master shard2-master; do
  n=$(rc -h "$h" --scan --pattern 'student:*' | sort -u | wc -l)
  echo "$h: $n"
done
```

### 步骤 5：理解取模

`shard.sh` 的核心逻辑是：

```sh
HASH=$(printf '%s' "KEY" | cksum | awk '{print $1}')
SHARD=$(( HASH % SHARDS + 1 ))
```

你可以自己算一下：

```bash
printf '%s' student:1001 | cksum
```

如果结果是 `706556905`，那么：

```text
706556905 % 2 + 1 = 2  ->  shard2
```

所以 `student:1001` 总是去 shard2。只要 `SHARDS` 不变，同一个 key 的路由就不变。

### 检查点

- [ ] 能用 `EXISTS` 证明一个 key 只在某个分片；
- [ ] 能解释 `DBSIZE` 和 `SCAN` 的区别；
- [ ] 能说出为什么不能使用 `KEYS *`；
- [ ] 能解释修改 `SHARDS` 后为什么会“看起来丢数据”。

---

## 8. 实验三：模拟 master 故障与自动选主

> 这个实验会真正停止 `shard1-master`，并触发 Sentinel 选主。
> 实验结束后，`shard1-master` 容器会以 **replica** 身份重新加入。

### 步骤 1：记录故障前状态

先确认 `student:1002` 在 shard1：

```bash
docker compose exec -T redis-cli sh /scripts/shard.sh GET student:1002
sc -h shard1-sentinel1 sentinel get-master-addr-by-name shard1
docker compose exec -T redis-cli sh /scripts/status.sh
```

记下当前 shard1 master，例如 `shard1-master`。

### 步骤 2：停止 shard1 master

```bash
docker compose stop shard1-master
```

确认容器已停止：

```bash
docker compose ps -a
```

### 步骤 3：等待 Sentinel 选主

等待约 10-15 秒：

```bash
sleep 10
docker compose exec -T redis-cli sh /scripts/status.sh
```

如果还没切换，再等 5-10 秒后重复执行。预期 shard1 的 master 变成
`shard1-replica1` 或 `shard1-replica2`。

也可以直接问 Sentinel：

```bash
sc -h shard1-sentinel1 sentinel get-master-addr-by-name shard1
```

### 步骤 4：向新 master 读写

```bash
docker compose exec -T redis-cli sh /scripts/shard.sh GET student:1002
docker compose exec -T redis-cli sh /scripts/shard.sh SET student:1002 bob-after-failover
docker compose exec -T redis-cli sh /scripts/shard.sh GET student:1002
```

预期能读到原来的 `bob`，然后写入并读到 `bob-after-failover`。
`shard.sh` 会自动跟随 Sentinel，不需要修改脚本。

### 步骤 5：恢复旧 master

```bash
docker compose start shard1-master
sleep 8
docker compose exec -T redis-cli sh /scripts/status.sh
rc -h shard1-master INFO replication
```

预期 `shard1-master` 现在是副本：

```text
role:slave
master_host:shard1-replica1
master_link_status:up
```

`master_host` 可能是 `shard1-replica1` 或 `shard1-replica2`，取决于 Sentinel
提升了谁。等待 `master_link_status:up` 后，旧 master 会重新同步新 master 的数据。

### 步骤 6：确认数据仍在

```bash
docker compose exec -T redis-cli sh /scripts/shard.sh GET student:1002
```

预期仍然是：

```text
bob-after-failover
```

### 检查点

- [ ] 能说出故障前和故障后的 shard1 master；
- [ ] 能解释为什么 `shard1-master` 容器重启后不是 master；
- [ ] 能解释 quorum=2 的作用；
- [ ] 能说明旧 master 是如何重新加入的。

---

## 9. 实验四：整体重启与数据持久化

这个实验验证：`docker compose down`（**不带 `-v`**）后重新启动，数据卷仍然存在，
拓扑也会保持。

### 步骤 1：记录当前状态

```bash
docker compose exec -T redis-cli sh /scripts/status.sh
docker compose exec -T redis-cli sh /scripts/shard.sh GET student:1002
```

记下 shard1 当前 master 和数据值。

### 步骤 2：整体停止（保留数据卷）

```bash
docker compose down
```

注意：**不要加 `-v`**。`-v` 会删除数据卷。

### 步骤 3：重新启动

```bash
docker compose up -d
sleep 15
docker compose exec -T redis-cli sh /scripts/status.sh
```

如果状态还没完全就绪，再等 10 秒并重复 `status.sh`。

### 步骤 4：验证数据和拓扑

```bash
docker compose exec -T redis-cli sh /scripts/shard.sh GET student:1002
sc -h shard1-sentinel1 sentinel get-master-addr-by-name shard1
```

预期：

- 数据仍然是 `bob-after-failover`；
- shard1 master 仍然是故障转移后选出的那个 replica，而不是自动切回 `shard1-master`。

这是因为：

1. Redis 使用 AOF 持久化，数据卷保留；
2. Sentinel 的配置保存在自己的数据卷中；
3. `scripts/redis-entrypoint.sh` 启动时会先问 Sentinel 当前 master，再决定自己
   以 master 还是 replica 身份启动。

### 检查点

- [ ] `down` / `up` 后数据没有丢失；
- [ ] 拓扑没有自动回到初始状态；
- [ ] 能解释 AOF、Sentinel 配置卷和 entrypoint 的作用。

---

## 10. 实验五（选做）：使用 Python 客户端

这个实验把 `shard.sh` 的“每次启动 redis-cli”替换成常驻连接池的 Python 客户端。
需要 Docker 能拉取 `python:3.12-slim` 镜像，并且容器能访问外网安装依赖。

如果提示网络不存在，先用下面的命令确认真实网络名，再替换 `--network` 参数：

```bash
docker network ls | grep redis
```

### 步骤 1：运行 Python 客户端

```bash
docker run --rm --network redis-sentinel-sharding_redisnet \
  -v "$PWD/scripts:/scripts:ro" \
  -e REDIS_PASSWORD=redispass \
  python:3.12-slim sh -c '
    pip install -q -r /scripts/requirements-client.txt
    python /scripts/client_sharding.py status
    python /scripts/client_sharding.py set student:2001 david
    python /scripts/client_sharding.py get student:2001
    python /scripts/client_sharding.py route student:2001 student:2002
  '
```

预期类似：

```text
shard1: master=shard1-replica1:6379 ping=True
shard2: master=shard2-master:6379 ping=True
OK
david
student:2001 -> shard1 (shard1-replica1:6379)
student:2002 -> shard2 (shard2-master:6379)
```

> master 具体是哪个容器，取决于你是否做过故障转移。

### 步骤 2：清理 Python 客户端的测试 key

```bash
docker compose exec -T redis-cli sh /scripts/shard.sh DEL student:2001
```

### 步骤 3（可选）：一致性哈希模式

```bash
docker run --rm --network redis-sentinel-sharding_redisnet \
  -v "$PWD/scripts:/scripts:ro" \
  -e REDIS_PASSWORD=redispass -e SHARD_ROUTING=ring \
  python:3.12-slim sh -c '
    pip install -q -r /scripts/requirements-client.txt
    python /scripts/client_sharding.py route student:2001 student:2002
  '
```

注意：`ring` 模式与 `shard.sh` 的 `cksum % N` **不兼容**。如果已经有数据，
直接切换会导致部分 key 路由到错误分片，必须先迁移数据。

### 检查点

- [ ] 能说出 `status`、`set`、`get`、`route` 各自的作用；
- [ ] 能解释为什么客户端要缓存 Sentinels 和连接池；
- [ ] 能解释“一致性哈希减少迁移”不等于“自动 rehash”。

---

## 11. 清理与重置

### 11.1 只删除实验 key

```bash
for k in student:1001 student:1002 student:1003 student:2001; do
  docker compose exec -T redis-cli sh /scripts/shard.sh DEL "$k" >/dev/null
done
```

### 11.2 正常停止（保留数据）

```bash
docker compose down
```

### 11.3 完全重置（清空数据并恢复初始拓扑）

> **警告**：下面的 `-v` 会删除所有 Redis/Sentinel 数据卷。
> 只在实验环境使用。

```bash
docker compose down -v
docker compose up -d
sleep 15
docker compose exec -T redis-cli sh /scripts/status.sh
```

重置后应该看到：

- shard1 master 是 `shard1-master`；
- shard2 master 是 `shard2-master`；
- 两个分片的 `keys` 都是 `0`。

---

## 12. 常见问题排查

### 12.1 容器没有全部运行

```bash
docker compose ps
docker compose logs --tail=50 shard1-master
docker compose logs --tail=50 shard1-sentinel1
```

启动需要时间。等待 30 秒后再看。如果某个容器反复重启，先看日志中的错误。

### 12.2 端口被占用

```bash
ss -lntp | grep -E ':(7001|7002|7003|7004|7005|7006|27001|27002|27003|27004|27005|27006)\b'
```

如果有其他 Redis 或容器占用端口，先停止冲突服务，再执行 `docker compose up -d`。

### 12.3 `NOAUTH Authentication required`

说明访问 Redis 数据节点时没有带密码。使用本文定义的 `rc` 函数，或显式设置：

```bash
export REDISCLI_AUTH=redispass
docker compose exec -T -e REDISCLI_AUTH redis-cli \
  redis-cli -h shard1-master --no-auth-warning PING
```

### 12.4 宿主机上提示 `shard1-master: Name or service not known`

容器主机名只在 Docker 网络内可解析。请通过 `redis-cli` 工具容器执行：

```bash
docker compose exec -T redis-cli redis-cli -h shard1-master -p 6379 ...
```

不要直接在宿主机上用 `redis-cli -h shard1-master`，除非你额外配置了主机名解析。

### 12.5 停止 master 后没有自动切换

依次检查：

```bash
docker compose ps
sc -h shard1-sentinel1 sentinel master shard1
sc -h shard1-sentinel2 sentinel master shard1
sc -h shard1-sentinel3 sentinel master shard1
```

至少要有 2 个 Sentinel 存活才能达到 quorum。再等待 10-30 秒。如果仍然不切换，
查看 Sentinel 日志：

```bash
docker compose logs --tail=100 shard1-sentinel1
```

### 12.6 旧 master 启动后仍然是 master

等待几秒后检查：

```bash
rc -h shard1-master INFO replication
```

如果 `role:master`，说明它还没有按 Sentinel 的拓扑切换成 replica。查看启动日志：

```bash
docker compose logs --tail=100 shard1-master
```

确认 3 个 Sentinel 已经先启动并可用，再重启该容器：

```bash
docker compose restart shard1-master
```

### 12.7 修改 `SHARDS` 后 key 读不到

这是预期行为。`shard.sh` 使用 `cksum(key) % SHARDS + 1`，修改分片数会改变绝大多数
key 的路由。正确做法是先迁移数据，再切换客户端配置，或改用 Redis Cluster。

### 12.8 为什么不能用 `redis-cli -c`

`-c` 是 Redis Cluster 的 MOVED/ASK 重定向模式。本实验是 Sentinel + 客户端分片，
没有 slot 和 MOVED，使用 `-c` 不会获得自动分片能力。请使用 `shard.sh` 或
`client_sharding.py`。

---

## 13. 自测题

1. 为什么每个分片要部署 3 个 Sentinel，而不是 1 个？
2. quorum=2 时，如果 2 个 Sentinel 同时宕机，还能自动切换吗？
3. 为什么 `student:1001` 每次都路由到同一个分片？
4. `DBSIZE` 和 `SCAN` 分别能观察到什么？为什么不能用 `KEYS *`？
5. 故障转移后，`shard1-master` 容器的角色是什么？如何确认当前 master？
6. 旧 master 重新启动后，为什么没有重新变成 master？
7. 修改 `SHARDS` 后，为什么已有 key 可能“消失”？
8. Sentinel 能实现服务端分片和自动 rehash 吗？
9. 什么场景应该从本方案切换到 Redis Cluster？
10. 为什么 Redis 节点既要配置 `requirepass` 又要配置 `masterauth`？

---

## 14. 命令速查

| 目的 | 命令 |
|------|------|
| 启动 | `docker compose up -d` |
| 查看容器 | `docker compose ps` |
| 查看分片状态 | `docker compose exec -T redis-cli sh /scripts/status.sh` |
| 写入 | `docker compose exec -T redis-cli sh /scripts/shard.sh SET key value` |
| 读取 | `docker compose exec -T redis-cli sh /scripts/shard.sh GET key` |
| 删除 | `docker compose exec -T redis-cli sh /scripts/shard.sh DEL key` |
| 查 Sentinel master | `sc -h shard1-sentinel1 sentinel get-master-addr-by-name shard1` |
| 查节点角色 | `rc -h shard1-master INFO replication` |
| 安全遍历 key | `rc -h shard1-master --scan --pattern 'student:*'` |
| 停止一个 master | `docker compose stop shard1-master` |
| 恢复一个节点 | `docker compose start shard1-master` |
| 查看日志 | `docker compose logs --tail=100 shard1-sentinel1` |
| 保留数据停止 | `docker compose down` |
| 清空数据重置 | `docker compose down -v && docker compose up -d` |

---

## 附录 A：Sentinel、客户端分片与 Redis Cluster 对比

| 能力 | 本实验（Sentinel + 客户端分片） | Redis Cluster |
|------|-------------------------------|---------------|
| 主从复制 | 支持 | 支持 |
| 自动故障转移 | 支持（Sentinel） | 支持（内置） |
| 服务端分片 | 不支持 | 支持 |
| 自动 rehash / 槽迁移 | 不支持 | 支持 |
| 客户端要求 | 需要自己路由 | 必须支持 Cluster 协议 |
| 多 key 跨分片 | 需要业务拆分 | 仅同一 slot 可用 |
| 运维复杂度 | 较低，但客户端要自己实现路由 | 较高，但有标准协议 |

结论：

- 只需要简单的高可用、分片数量固定、可以接受客户端路由：本方案合适；
- 需要服务端自动分片、在线扩容和自动 rehash：应使用 Redis Cluster。

---

## 附录 B：实验检查点汇总

| # | 检查点 | 通过标准 |
|---|--------|----------|
| 1 | 环境启动 | 13 个容器运行，`status.sh` 能看到两个分片 |
| 2 | Sentinel 拓扑 | 能通过 Sentinel 查到两个分片的 master |
| 3 | 客户端分片 | 3 个 key 分布到两个分片，`EXISTS` 只在一边为 1 |
| 4 | 故障转移 | 停掉 master 后 Sentinel 提升 replica，读写继续可用 |
| 5 | 旧 master 回归 | 旧 master 启动后 `role:slave` 且 `master_link_status:up` |
| 6 | 持久化 | `down` / `up` 后数据仍在，拓扑保持 |
| 7 | 原理理解 | 能解释 Sentinel、客户端分片、自动 rehash 的区别 |
