#!/usr/bin/env python3
"""Sentinel 多分片客户端分片示例。

本项目是「多分片 Sentinel + 客户端分片」：
- 每个 shardN 是一套独立的 Redis + Sentinel 高可用组；
- Sentinel 只负责该分片内部的主从切换，不知道其他分片；
- 分片路由由本脚本（或业务客户端）自己完成。

本脚本演示生产客户端最核心的几件事：
1. 根据 key 选择分片；
2. 通过该分片的 Sentinel 发现当前 master；
3. 使用连接池访问 master，并在故障转移后刷新连接；
4. 对 MGET / DEL 这类多 key 命令按分片拆分后合并结果。

依赖安装：
    python3 -m pip install -r scripts/requirements-client.txt

运行位置：
    推荐在 Docker 网络内运行，因为 Sentinel 返回的 master 地址是容器主机名
    （例如 shard1-master）。在宿主机直接运行时，即使把 Sentinel 地址改成
    127.0.0.1:27001，宿主机也解析不了 shard1-master。
    可以这样临时运行：
      docker run --rm --network redis-sentinel-sharding_redisnet \
        -v "$PWD/scripts:/scripts" -e REDIS_PASSWORD=redispass \
        python:3.12-slim sh -c \
        "pip install -q -r /scripts/requirements-client.txt && \
         python /scripts/client_sharding.py status"

常用示例：
    # 查看每个分片当前 master
    python3 scripts/client_sharding.py status

    # 与 scripts/shard.sh 相同的 cksum 取模路由（默认）
    python3 scripts/client_sharding.py set user:1 alice
    python3 scripts/client_sharding.py get user:1
    python3 scripts/client_sharding.py mget user:1 user:2
    python3 scripts/client_sharding.py del user:1
    python3 scripts/client_sharding.py route user:1 user:2

    # 一致性哈希路由（与 shard.sh 的 cksum 取模不兼容，存量数据需迁移）
    SHARD_ROUTING=ring python3 scripts/client_sharding.py route user:1

环境变量：
    REDIS_PASSWORD       Redis 密码，默认 redispass
    SENTINEL_PASSWORD    Sentinel 密码，默认空（本项目未启用 Sentinel 认证）
    SHARD1_SENTINELS     shard1 的 Sentinel 列表，host:port 逗号分隔
    SHARD2_SENTINELS     shard2 的 Sentinel 列表，host:port 逗号分隔
    SHARD_ROUTING        cksum（默认，兼容 shard.sh）或 ring（一致性哈希）
    SENTINEL_TIMEOUT     Sentinel 查询超时秒数，默认 0.5
    REDIS_TIMEOUT        Redis 连接/读写超时秒数，默认 2.0
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Callable, Dict, Iterable, List, Optional, Tuple

try:
    import redis
    from redis.exceptions import (
        ConnectionError as RedisConnectionError,
        ReadOnlyError,
        TimeoutError as RedisTimeoutError,
    )
    from redis.sentinel import Sentinel
except ImportError as exc:  # pragma: no cover - 依赖缺失时给出明确提示
    raise SystemExit(
        "缺少 Python Redis 依赖，请先执行：\n"
        "  python3 -m pip install -r scripts/requirements-client.txt"
    ) from exc


# ---------------------------------------------------------------------------
# 分片元数据
# ---------------------------------------------------------------------------
# Sentinel 不提供「全局分片列表」，所以分片列表必须由客户端自己维护。
# 默认值与 docker-compose.yml 中的 Sentinel 服务名一致。
#
# 如果新增 shard3，需要同时：
#   1. 在 DEFAULT_SENTINELS 中增加 shard3；
#   2. 在 docker-compose.yml 中增加 shard3 的 Redis/Sentinel；
#   3. 迁移数据后，再让所有客户端使用新的分片列表。
DEFAULT_SENTINELS: Dict[str, str] = {
    "shard1": "shard1-sentinel1:26379,shard1-sentinel2:26379,shard1-sentinel3:26379",
    "shard2": "shard2-sentinel1:26379,shard2-sentinel2:26379,shard2-sentinel3:26379",
}


def _parse_sentinels(raw: str) -> List[Tuple[str, int]]:
    """把 "host1:26379,host2:26379" 解析成 [(host1, 26379), ...]。"""
    result: List[Tuple[str, int]] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        host, _, port = item.rpartition(":")
        if not host or not port.isdigit():
            raise SystemExit(f"无效的 Sentinel 地址: {item!r}，应为 host:port")
        result.append((host, int(port)))
    if not result:
        raise SystemExit("Sentinel 地址列表不能为空")
    return result


# 每个分片独立维护自己的 Sentinel 地址列表。
# 环境变量名由分片名大写得到，例如 shard1 -> SHARD1_SENTINELS。
SHARD_SENTINELS: Dict[str, List[Tuple[str, int]]] = {
    name: _parse_sentinels(os.getenv(f"{name.upper()}_SENTINELS", default))
    for name, default in DEFAULT_SENTINELS.items()
}
SHARD_NAMES = list(SHARD_SENTINELS)

PASSWORD = os.getenv("REDIS_PASSWORD", "redispass")
SENTINEL_PASSWORD = os.getenv("SENTINEL_PASSWORD", "")
SENTINEL_TIMEOUT = float(os.getenv("SENTINEL_TIMEOUT", "0.5"))
REDIS_TIMEOUT = float(os.getenv("REDIS_TIMEOUT", "2.0"))

# 路由算法：
# - cksum：与 scripts/shard.sh 完全一致，适合读取已有数据；
# - ring：一致性哈希，增删分片时只迁移约 1/N 的 key，但与旧数据不兼容。
ROUTING = os.getenv("SHARD_ROUTING", "cksum").strip().lower()
if ROUTING not in ("cksum", "ring"):
    raise SystemExit("SHARD_ROUTING 只能是 cksum 或 ring")


# ---------------------------------------------------------------------------
# POSIX cksum 实现
# ---------------------------------------------------------------------------
# scripts/shard.sh 使用系统命令 `cksum` 计算 key 的哈希。Python 标准库没有
# 直接对应的函数（zlib.crc32 的算法与 POSIX cksum 不同），所以这里手写一份，
# 保证 Python 客户端和 shard.sh 对同一个 key 得到相同的分片。
def _build_cksum_table() -> List[int]:
    """生成 POSIX cksum 使用的 CRC-32 查表（多项式 0x04C11DB7）。"""
    table: List[int] = []
    for value in range(256):
        crc = value << 24
        for _ in range(8):
            if crc & 0x80000000:
                crc = ((crc << 1) ^ 0x04C11DB7) & 0xFFFFFFFF
            else:
                crc = (crc << 1) & 0xFFFFFFFF
        table.append(crc)
    return table


_CKSUM_TABLE = _build_cksum_table()


def posix_cksum(data: bytes) -> int:
    """返回与 POSIX `cksum` 命令第一个字段一致的 CRC32 值。"""
    crc = 0
    for byte in data:
        crc = ((crc << 8) & 0xFFFFFFFF) ^ _CKSUM_TABLE[((crc >> 24) ^ byte) & 0xFF]

    # POSIX cksum 会把数据长度也追加到 CRC 计算中。
    length = len(data)
    while length:
        crc = (
            ((crc << 8) & 0xFFFFFFFF)
            ^ _CKSUM_TABLE[((crc >> 24) ^ (length & 0xFF)) & 0xFF]
        )
        length >>= 8

    return (~crc) & 0xFFFFFFFF


# ---------------------------------------------------------------------------
# 一致性哈希（可选）
# ---------------------------------------------------------------------------
_ring = None


def _get_ring():
    """延迟导入 uhashring，只有 SHARD_ROUTING=ring 时才需要它。"""
    global _ring
    if _ring is None:
        try:
            from uhashring import HashRing
        except ImportError as exc:
            raise SystemExit(
                "SHARD_ROUTING=ring 需要 uhashring，请执行：\n"
                "  python3 -m pip install uhashring"
            ) from exc
        _ring = HashRing(nodes=SHARD_NAMES)
    return _ring


def shard_for(key: str) -> str:
    """根据 key 返回目标分片名。

    注意：
    - 所有客户端必须使用相同的算法、相同的分片列表和顺序；
    - cksum 模式与 shard.sh 兼容，但修改分片数会导致大量 key 重新映射；
    - ring 模式只减少迁移量，并不会自动迁移已有数据。
    """
    if ROUTING == "ring":
        return _get_ring().get_node(key)

    index = posix_cksum(key.encode("utf-8")) % len(SHARD_NAMES)
    return SHARD_NAMES[index]


# ---------------------------------------------------------------------------
# Sentinel 发现与连接池
# ---------------------------------------------------------------------------
_sentinels: Dict[str, Sentinel] = {}
_clients: Dict[str, redis.Redis] = {}


def _get_sentinel(shard: str) -> Sentinel:
    """获取（并缓存）某个分片的 Sentinel 客户端。

    Sentinel 客户端内部会轮询列表中的多个 Sentinel，单个 Sentinel 故障时
    仍能查询到 master。
    """
    if shard not in _sentinels:
        kwargs = {"socket_timeout": SENTINEL_TIMEOUT}
        if SENTINEL_PASSWORD:
            kwargs["sentinel_kwargs"] = {"password": SENTINEL_PASSWORD}
        _sentinels[shard] = Sentinel(SHARD_SENTINELS[shard], **kwargs)
    return _sentinels[shard]


def _get_client(shard: str) -> redis.Redis:
    """获取（并缓存）某个分片当前 master 的连接池。

    master_for() 使用 SentinelConnectionPool：池中的连接会通过 Sentinel
    发现当前 master。客户端仍需要在下述错误发生时丢弃旧客户端并重新创建。
    """
    if shard not in _clients:
        _clients[shard] = _get_sentinel(shard).master_for(
            shard,
            password=PASSWORD or None,
            socket_timeout=REDIS_TIMEOUT,
            socket_connect_timeout=REDIS_TIMEOUT,
            decode_responses=True,
        )
    return _clients[shard]


def _drop_client(shard: str) -> None:
    """关闭并移除某个分片的客户端，下次访问时会重新向 Sentinel 查询 master。"""
    client = _clients.pop(shard, None)
    if client is not None:
        try:
            client.close()
        except Exception:
            # 关闭连接失败不应掩盖原始错误。
            pass


def _run(shard: str, operation: Callable[[redis.Redis], object]):
    """在指定分片上执行操作，连接类错误时刷新 master 并重试一次。

    这里的自动重试只适合幂等操作（GET/SET/DEL）。生产环境中的 INCR 等
    非幂等命令不要盲目重试，否则可能重复执行。
    """
    for attempt in (1, 2):
        client = _get_client(shard)
        try:
            return operation(client)
        except (RedisConnectionError, RedisTimeoutError, ReadOnlyError):
            _drop_client(shard)
            if attempt == 2:
                raise

    # 循环一定会在 attempt == 2 时 return 或 raise，这里只是让类型检查器满意。
    raise RuntimeError("unreachable")


def master_addr(shard: str) -> Tuple[str, int]:
    """向该分片的 Sentinel 查询当前 master 地址。"""
    return _get_sentinel(shard).discover_master(shard)


# ---------------------------------------------------------------------------
# 单 key / 多 key 操作
# ---------------------------------------------------------------------------
def get_value(key: str):
    return _run(shard_for(key), lambda client: client.get(key))


def set_value(key: str, value: str) -> bool:
    return bool(_run(shard_for(key), lambda client: client.set(key, value)))


def delete_keys(keys: Iterable[str]) -> int:
    """DEL 多 key：按分片拆分后分别执行，返回删除总数。"""
    grouped: Dict[str, List[str]] = {}
    for key in keys:
        grouped.setdefault(shard_for(key), []).append(key)

    deleted = 0
    for shard, shard_keys in grouped.items():
        deleted += int(
            _run(shard, lambda client, ks=shard_keys: client.delete(*ks))
        )
    return deleted


def mget_values(keys: Iterable[str]) -> Dict[str, Optional[str]]:
    """MGET 多 key：按分片拆分后分别执行，再按原 key 合并结果。"""
    key_list = list(keys)
    grouped: Dict[str, List[str]] = {}
    for key in key_list:
        grouped.setdefault(shard_for(key), []).append(key)

    result: Dict[str, Optional[str]] = {}
    for shard, shard_keys in grouped.items():
        values = _run(shard, lambda client, ks=shard_keys: client.mget(ks))
        result.update(zip(shard_keys, values))
    return result


def route_info(key: str) -> Tuple[str, str]:
    """返回 key 的目标分片和该分片当前 master 地址，仅用于排查。"""
    shard = shard_for(key)
    try:
        host, port = master_addr(shard)
        return shard, f"{host}:{port}"
    except Exception as exc:
        return shard, f"<发现失败: {exc}>"


# ---------------------------------------------------------------------------
# 命令行入口
# ---------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Sentinel 多分片客户端示例：按 key 路由到对应分片 master。",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "示例:\n"
            "  %(prog)s status\n"
            "  %(prog)s set user:1 alice\n"
            "  %(prog)s get user:1\n"
            "  %(prog)s mget user:1 user:2\n"
            "  %(prog)s del user:1\n"
            "  %(prog)s route user:1 user:2\n"
        ),
    )
    parser.add_argument(
        "--routing",
        choices=("cksum", "ring"),
        default=ROUTING,
        help="路由算法：cksum=兼容 shard.sh；ring=一致性哈希（默认取 SHARD_ROUTING）",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    p_set = sub.add_parser("set", help="SET key value")
    p_set.add_argument("key")
    p_set.add_argument("value")

    p_get = sub.add_parser("get", help="GET key")
    p_get.add_argument("key")

    p_del = sub.add_parser("del", help="DEL key [key ...]，自动按分片拆分")
    p_del.add_argument("keys", nargs="+")

    p_mget = sub.add_parser("mget", help="MGET key [key ...]，自动按分片拆分")
    p_mget.add_argument("keys", nargs="+")

    p_route = sub.add_parser("route", help="只显示 key 的分片和当前 master")
    p_route.add_argument("keys", nargs="+")

    sub.add_parser("status", help="显示每个分片的 master 并执行 PING")

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    global ROUTING

    args = build_parser().parse_args(argv)
    ROUTING = args.routing

    if args.command == "set":
        ok = set_value(args.key, args.value)
        print("OK" if ok else "FAILED")
        return 0 if ok else 1

    if args.command == "get":
        value = get_value(args.key)
        print("(nil)" if value is None else value)
        return 0

    if args.command == "del":
        print(delete_keys(args.keys))
        return 0

    if args.command == "mget":
        result = mget_values(args.keys)
        for key in args.keys:
            value = result.get(key)
            print(f"{key} => {'(nil)' if value is None else value}")
        return 0

    if args.command == "route":
        for key in args.keys:
            shard, address = route_info(key)
            print(f"{key} -> {shard} ({address})")
        return 0

    if args.command == "status":
        failed = 0
        for shard in SHARD_NAMES:
            try:
                host, port = master_addr(shard)
                pong = _run(shard, lambda client: client.ping())
                print(f"{shard}: master={host}:{port} ping={pong}")
            except Exception as exc:
                failed += 1
                print(f"{shard}: ERROR {exc}", file=sys.stderr)
        return 1 if failed else 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
