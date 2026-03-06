# ElastiCache

ElastiCache implementation supporting cluster and replication group management. The actual Redis/Memcached engine is configurable via the `--elasticache-engine` flag.

For detailed engine mode documentation, see [ElastiCache architecture](../architecture/elasticache.md).

## Supported Operations

| Operation | Description |
|-----------|-------------|
| `CreateCacheCluster` | Create a new cache cluster |
| `DeleteCacheCluster` | Delete a cache cluster |
| `DescribeCacheClusters` | List cache clusters |
| `ListTagsForResource` | List tags on a cluster |
| `CreateReplicationGroup` | Create a replication group |
| `DeleteReplicationGroup` | Delete a replication group |
| `DescribeReplicationGroups` | List replication groups |

## AWS CLI Examples

```bash
# Create a single-node Redis cluster
aws --endpoint-url http://localhost:8000 elasticache create-cache-cluster \
    --cache-cluster-id my-redis \
    --engine redis \
    --cache-node-type cache.t3.micro \
    --num-cache-nodes 1

# Describe clusters
aws --endpoint-url http://localhost:8000 elasticache describe-cache-clusters \
    --show-cache-node-info

# Create a replication group
aws --endpoint-url http://localhost:8000 elasticache create-replication-group \
    --replication-group-id my-rg \
    --replication-group-description "Test replication group" \
    --num-cache-clusters 2

# Delete a cluster
aws --endpoint-url http://localhost:8000 elasticache delete-cache-cluster \
    --cache-cluster-id my-redis
```

## Connecting to the Embedded Redis

In `embedded` mode (default), a real Redis-compatible server (miniredis) is started on a port allocated from the configured range (`PORT_RANGE_START`–`PORT_RANGE_END`, default 10000–10100). The `DescribeCacheClusters` response includes the endpoint hostname and port.

```bash
# Get the endpoint
ENDPOINT=$(aws --endpoint-url http://localhost:8000 elasticache describe-cache-clusters \
    --cache-cluster-id my-redis \
    --show-cache-node-info \
    --query 'CacheClusters[0].CacheNodes[0].Endpoint' --output json)

# Connect with redis-cli
redis-cli -h $(echo $ENDPOINT | jq -r .Address) -p $(echo $ENDPOINT | jq -r .Port)
```

## Configuration

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--elasticache-engine` | `ELASTICACHE_ENGINE` | `embedded` | Engine mode: `embedded`, `stub`, or `docker` |
| `--port-range-start` | `PORT_RANGE_START` | `10000` | Start of port range for resource endpoints |
| `--port-range-end` | `PORT_RANGE_END` | `10100` | End (exclusive) of port range |
