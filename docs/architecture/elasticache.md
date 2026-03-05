# ElastiCache Engine Modes

Gopherstack supports three engine modes for ElastiCache, controlled by the `--elasticache-engine` flag or `ELASTICACHE_ENGINE` environment variable.

## `embedded` (default)

A real Redis-compatible server ([miniredis](https://github.com/alicebob/miniredis)) is started in-process when the first cluster is created.

**When to use:** Integration tests that need a real Redis connection — real data can be read and written.

```bash
gopherstack --elasticache-engine embedded
```

Each cluster gets a port allocated from the port range (`PORT_RANGE_START`–`PORT_RANGE_END`, default 10000–10100). The endpoint is returned in `DescribeCacheClusters`.

### Connecting to the embedded Redis

```bash
# Get the endpoint
ENDPOINT=$(aws --endpoint-url http://localhost:8000 elasticache describe-cache-clusters \
    --cache-cluster-id my-redis \
    --show-cache-node-info \
    --query 'CacheClusters[0].CacheNodes[0].Endpoint' --output json)

HOST=$(echo "$ENDPOINT" | jq -r '.Address')
PORT=$(echo "$ENDPOINT" | jq -r '.Port')

redis-cli -h "$HOST" -p "$PORT" ping
# PONG
```

### Supported Redis commands

All [miniredis-supported commands](https://github.com/alicebob/miniredis#implemented-commands) are available: GET/SET, HGET/HSET, LPUSH/LPOP, EXPIRE, TTL, SUBSCRIBE/PUBLISH, and many more.

## `stub`

The API layer records cluster metadata but no Redis server is started. All ElastiCache API calls succeed, but the returned endpoint does not accept Redis connections.

**When to use:** When you only need the control-plane API (e.g. to test IaC code that creates clusters) and do not need a real Redis connection.

```bash
gopherstack --elasticache-engine stub
```

## `docker`

A real Redis container is started via the Docker daemon when a cluster is created, and stopped when the cluster is deleted.

**When to use:** When you need the full Redis stack (Lua scripts, modules, AUTH, TLS) that miniredis does not support.

```bash
gopherstack --elasticache-engine docker
```

Requires Docker to be running. The image `redis:7-alpine` is used by default. Port allocation follows the same range as `embedded` mode.

## Configuration reference

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--elasticache-engine` | `ELASTICACHE_ENGINE` | `embedded` | Engine mode: `embedded`, `stub`, or `docker` |
| `--port-range-start` | `PORT_RANGE_START` | `10000` | Start of port range for resource endpoints |
| `--port-range-end` | `PORT_RANGE_END` | `10100` | End (exclusive) of port range |

## Replication groups

`CreateReplicationGroup` and `DescribeReplicationGroups` are supported in all modes. In `embedded` mode, a single miniredis instance is started regardless of `NumCacheClusters`; true replication is not simulated.

## Example: testing with Elasticache

```go
// In your test setup, read the endpoint from Gopherstack
clusterOut, _ := elasticacheClient.DescribeCacheClusters(ctx, &elasticache.DescribeCacheClustersInput{
    CacheClusterId:     aws.String("test-cluster"),
    ShowCacheNodeInfo:  aws.Bool(true),
})
endpoint := clusterOut.CacheClusters[0].CacheNodes[0].Endpoint
redisAddr := fmt.Sprintf("%s:%d", *endpoint.Address, *endpoint.Port)

rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
rdb.Set(ctx, "key", "value", 0)
```
