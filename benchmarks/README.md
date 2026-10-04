# Benchmarks

## 故障注入

`fault_injection.py` 会启动一个三节点集群，反复杀掉当前 leader，等待新 leader 出现后写入探针，最后输出平均恢复时间。

```bash
python benchmarks/fault_injection.py --rounds 10 --port-base 25001
```

## 吞吐测试

先启动三节点集群，再运行内置 benchmark：

```bash
python -m nimbus_kv server --id n1 --nodes "n1=127.0.0.1:8001,n2=127.0.0.1:8002,n3=127.0.0.1:8003" --data-dir data
python -m nimbus_kv server --id n2 --nodes "n1=127.0.0.1:8001,n2=127.0.0.1:8002,n3=127.0.0.1:8003" --data-dir data
python -m nimbus_kv server --id n3 --nodes "n1=127.0.0.1:8001,n2=127.0.0.1:8002,n3=127.0.0.1:8003" --data-dir data

python -m nimbus_kv benchmark --nodes "n1=127.0.0.1:8001,n2=127.0.0.1:8002,n3=127.0.0.1:8003" --ops 1000 --clients 8
```
