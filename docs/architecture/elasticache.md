# ElastiCache engine modes

`--elasticache-engine` (`ELASTICACHE_ENGINE`) selects `embedded` (default), `stub` or `docker`. MemoryDB has the same modes via `--memorydb-engine`.

| Mode | Data plane | Use when |
|------|-----------|----------|
| `embedded` | In-process [miniredis](https://github.com/alicebob/miniredis) | Tests that need a real Redis connection |
| `stub` | None. The endpoint does not accept connections | You only need the control plane (for example IaC tests) |
| `docker` | Real Redis or Valkey containers | You need Lua, modules, ACLs, TLS or cluster slots |

## embedded

Each cluster gets a miniredis server on a port from `PORT_RANGE_START`-`PORT_RANGE_END` (default 10000-10100). All [miniredis commands](https://github.com/alicebob/miniredis#implemented-commands) work.

`DescribeCacheClusters` returns a synthetic AWS-style hostname (`<id>.<suffix>.<region>.cache.amazonaws.com`) and the port. The hostname resolves only if you enable the [embedded DNS](dns.md). Without DNS, connect to `localhost` on the returned port.

```bash
aws --endpoint-url http://localhost:8000 elasticache describe-cache-clusters \
    --cache-cluster-id my-redis --show-cache-node-info \
    --query 'CacheClusters[0].CacheNodes[0].Endpoint'

redis-cli -h localhost -p <Port> ping    # PONG
```

> [!NOTE]
> In `embedded` mode replication groups are not simulated. Members of a group share the primary's server.

## docker

Containers start through the container runtime (Docker or Podman). Images are `redis:<major>.<minor>-alpine`, or `valkey/valkey:<major>.<minor>-alpine` for Valkey. Ports come from the port allocator.

- Standalone clusters: one container.
- Replication groups without cluster mode: one primary plus replicas wired with `REPLICAOF`. `PrimaryEndpoint` targets the primary, `ReaderEndpoint` the first replica.
- Cluster-mode groups: `NumNodeGroups x (1 + replicas)` containers with `cluster-enabled yes`, formed into a real cluster over the 16384-slot map (`NodeGroupConfiguration.Slots` is honoured). Nodes announce `127.0.0.1`, or `ELASTICACHE_HOST` if set, so `MOVED` redirects reach the host. Each node has its own port in `NodeGroupMembers[].ReadEndpoint`.
- `TransitEncryptionEnabled` serves TLS only, with certificates from the emulator CA. Mode `preferred` serves plaintext.
- `AuthToken` protects the `default` user. RBAC users and user groups become `ACL SETUSER` accounts on every node and are re-synced when users, groups or replication groups change.
- `CreateSnapshot` reads each primary's RDB. `SnapshotName` seeds new primaries from it.

## Test from Go

```go
out, _ := client.DescribeCacheClusters(ctx, &elasticache.DescribeCacheClustersInput{
    CacheClusterId:    aws.String("test-cluster"),
    ShowCacheNodeInfo: aws.Bool(true),
})
ep := out.CacheClusters[0].CacheNodes[0].Endpoint
rdb := redis.NewClient(&redis.Options{Addr: fmt.Sprintf("localhost:%d", *ep.Port)})
rdb.Set(ctx, "key", "value", 0)
```
