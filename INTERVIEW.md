# 面试自述：NimbusKV

> 适用于分布式基础架构 / 云原生后端 / 系统软件方向的面试

## 30 秒电梯演讲

我用 Python 标准库从零实现了一个 Raft 一致性协议的分布式 KV 存储。没有调用 etcd、ZooKeeper 或任何第三方 Raft 库，所有代码（选举、日志复制、快照、线性一致读）都是自己写的。11 个测试全部通过，包括三节点集群杀 leader 的故障转移集成测试。

## 1 分钟概述

- **解决的问题**：如何让一个分布式系统的多个节点在任意故障下保证数据一致。这是分布式系统最核心的难题，也是 etcd、TiDB、CockroachDB 等工业级系统的理论基础。
- **技术栈**：Python 3.10+，只用了 `socket`、`threading`、`json`、`unittest` 等标准库。
- **实现的算法**：Raft 共识协议的 leader election、log replication、log compaction（snapshot）和 read index。
- **工程亮点**：
  - 零依赖，代码 ~1500 行，可读性优先。
  - WAL 持久化 + 原子快照，支持崩溃恢复。
  - 客户端自动发现 leader，节点宕机后无需手动切换。
  - 真实三进程集成测试 + 故障注入基准（循环杀 leader 后写探针）。
  - GitHub Actions CI + 演示 GIF。

## 可以主动展示的细节

1. **数据不会丢**：写入后 kill 掉 leader，等新 leader 选出，`get` 之前写的数据，值还在。
2. **不是包装层**：翻 `nimbus_kv/raft.py`，能看到完整的 RequestVote、AppendEntries、commit、snapshot 和 ReadIndex 逻辑。
3. **为什么用 Python**：Python 的阅读成本最低，在面试场景下能快速证明你对算法的掌握。工业使用当然换 Rust/Go。
4. **怎么验证正确性**：单元测试（log conflict、storage persistence） + 集成测试（跨进程故障转移） + 故障注入脚本（多次杀 leader 统计恢复时间）。
5. **安全不脑裂**：Raft 的 term 唯一性、选票只投一票、多数派提交、线性一致读保证了即使在网络分区中也不会返回过期数据。CPU 爆涨也不会出现 data corruption。

## 可能被追问的问题 & 回答建议

**Q：Raft 和 Paxos 的区别？**
A：Raft 把共识过程拆成了三个清晰的子问题：选举、日志复制、安全性。可理解性比 Paxos 强很多，这也是它被工业界广泛采用的原因。

**Q：为什么 leader 当选后要先提交一条 no-op？**
A：为了推进上一任 leader 已提交但还没在 follower 上 applied 的日志。否则新 leader 在第一次读请求时可能返回尚未 applied 的过期数据。

**Q：网络分区的时候会脑裂吗？**
A：不会。旧 leader 在分区中仍认为自己是 leader，但它无法获得多数派的 AppendEntries 确认，commit_index 不会推进；线性一致读也会因为 heartbeat quorum 失败而拒绝返回数据。新 majority 分区会选出新 leader。两个分区中最多只有一个 true leader。

**Q：性能瓶颈在哪里？**
A：当前实现是 TCP + JSON 短连接，适合教学和验证。生产级可换成 gRPC 流式连接、批量提交（pipeline）和二进制编码。

**Q：如果让你生产化这个项目，会怎么做？**
A：1) 用 Rust 或 Go 重写热路径；2) 替换 JSON 为 Protocol Buffers；3) 使用 gRPC 双向流；4) 加入 batching 和 pipeline；5) 支持成员变更实现在线扩缩容。

**Q：和 Redis Sentinel / Cluster 有什么不同？**
A：Redis 的复制是 async 的，无法保证强一致性（可能丢数据）。Raft 要求每次写必须多数派确认，强一致性。Redis Cluster 无中心化分片，raft 是单 group 的强一致性副本控制。两者可以结合（如 TiKV：Raft 做一致性，range 做分片）。

## 仓库结构（面试官翻代码时可以指路）

| 文件 | 看什么 |
| --- | --- |
| `nimbus_kv/raft.py` | Raft 核心逻辑，600+ 行 |
| `nimbus_kv/log.py` | 日志冲突截断、快照基索引 |
| `nimbus_kv/persistent.py` | WAL + 原子快照 |
| `nimbus_kv/rpc.py` | 长度前缀 JSON 协议 |
| `nimbus_kv/client.py` | 客户端自动重定向 |
| `tests/test_cluster.py` | 杀 leader 后故障转移集成测试 |
| `demo.py` | 面试用一键演示脚本 |
| `docs/design.md` | 技术设计文档 |

## 面试演示步骤

```bash
# 1. 运行 demo（会自动启动集群、演示故障转移）
python demo.py

# 2. 或者手动复现
# 终端 1-3 启动三节点
python -m nimbus_kv server --id n1 --nodes "n1=127.0.0.1:8001,n2=127.0.0.1:8002,n3=127.0.0.1:8003" --data-dir data
# ... 另外两个类似 ...

# 终端 4 操作
python -m nimbus_kv client --nodes "n1=127.0.0.1:8001,n2=127.0.0.1:8002,n3=127.0.0.1:8003" put hello world
# 杀掉当前 leader 进程
python -m nimbus_kv client --nodes "n1=127.0.0.1:8001,n2=127.0.0.1:8002,n3=127.0.0.1:8003" get hello
# 输出: world

# 3. 故障注入基准
python benchmarks/fault_injection.py --rounds 10
```
