# NimbusKV 设计文档

## 1. 目标

- 用 Python 标准库实现一个可运行、可测试、可讲解的 Raft 集群。
- 保证已提交的 KV 写操作在多数派节点存活时不会丢失。
- 在 leader 崩溃后，集群可在选举超时内自动恢复服务。

## 2. 模块划分

| 模块 | 职责 |
| --- | --- |
| `log.py` | 带快照基地址的复制日志，支持冲突截断 |
| `persistent.py` | 元数据、WAL、快照的原子持久化 |
| `state_machine.py` | 确定性的 KV 状态机 |
| `rpc.py` | 长度前缀 JSON 消息协议 |
| `raft.py` | 选举、复制、提交、快照、线性一致读 |
| `server.py` | TCP 服务与 HTTP dashboard |
| `client.py` | leader 自动发现与重试 |

## 3. 网络协议

每个 TCP 连接发送一条消息：

```
[4-byte big-endian length][UTF-8 JSON payload]
```

消息类型包括 `request_vote`、`append_entries`、`install_snapshot`、`client_request` 和 `status_request`。使用短连接简化了并发控制，也便于测试。

## 4. 领导者选举

1. 每个节点在随机选举超时后转为 candidate，`term + 1` 并投票给自己。
2. candidate 向所有 peer 并行发送 `RequestVote`。
3. 收到多数派选票后成为 leader；收到更高 term 时立即退回 follower。
4. leader 周期性发送空的 `AppendEntries` 作为心跳，阻止其他节点发起选举。

随机超时减少分票概率。选举安全性由以下规则保证：

- 每个 term 最多投一票；
- 只有日志“至少和自己一样新”的 candidate 才能获得选票。

新 leader 上任后先提交一条当前 term 的 no-op 日志，以推进上一任 leader
已提交但尚未在 follower 上 apply 的日志，避免新 leader 过早提供读服务。

## 5. 日志复制与提交

1. 客户端写请求只由 leader 接受。
2. leader 先把命令追加到本地日志并持久化到 WAL。
3. leader 并行向 follower 发送 `AppendEntries`。
4. 当日志条目被多数派节点持久化后，leader 推进 `commit_index`。
5. follower 根据 `leader_commit` 推进自己的 commit 并应用状态机。
6. 客户端只有在日志提交并应用到状态机后才收到成功响应。

`AppendEntries` 的一致性检查使用 `prev_log_index` 和 `prev_log_term`。失败时 leader 回退 `next_index`，直到匹配点对齐。

## 6. 快照与日志压缩

当内存日志超过阈值时，节点把状态机快照和 `last_included_index/last_included_term` 写入磁盘，并截断旧日志。若 follower 落后太多，leader 发送 `InstallSnapshot` 而不是逐条补日志。

`RaftLog` 的 `base_index` 表示快照后的日志起点，从而让“日志已空但全局 index 仍有效”的情况可以正确表达。

## 7. 线性一致读

NimbusKV 的读请求不直接读本地状态，而是先执行一次 heartbeat quorum：

1. leader 向多数派节点发送心跳；
2. 收到多数派确认后，leader 才认为自己仍然是合法 leader；
3. 随后返回已应用状态中的值。

这避免了网络分区中旧 leader 返回过期数据的风险。

## 8. 持久化

- `meta.json`：`current_term` 和 `voted_for`，每次变化后原子替换。
- `wal.jsonl`：每条日志一行，`flush + fsync`。
- `snapshot.json`：状态机快照，写完后清空 WAL。

所有文件写入都先写临时文件再 `os.replace`，避免半写文件。

## 9. 已知边界

- 这是教学与竞赛级实现，优先保证正确性和可读性，不追求极限吞吐。
- 当前使用 TCP + JSON，生产环境可替换为 gRPC 和更紧凑的二进制编码。
- 当前未实现成员变更（joint consensus）和租约式读，但这些是明确的扩展点。
- 单线程状态机天然串行，性能受 Python 单节点状态机限制，但可通过批处理优化。

## 10. 后续工作

- 成员变更与动态扩缩容
- 多状态机插件：计数器、分布式队列、分布式锁
- 批量管道、二进制协议和零拷贝快照
- Jepsen 风格的形式化故障注入
