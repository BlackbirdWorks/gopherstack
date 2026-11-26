# ElastiCache

ElastiCache clusters, replication groups, serverless caches and users. By default each cluster starts an embedded Redis-compatible server (miniredis). Engine details: [ElastiCache architecture](../architecture/elasticache.md).

## Quick start

```bash
export AWS_ENDPOINT_URL=http://localhost:8000

aws elasticache create-cache-cluster --cache-cluster-id my-redis \
    --engine redis --cache-node-type cache.t3.micro --num-cache-nodes 1

ENDPOINT=$(aws elasticache describe-cache-clusters --cache-cluster-id my-redis \
    --show-cache-node-info --query 'CacheClusters[0].CacheNodes[0].Endpoint' --output json)
redis-cli -h $(echo $ENDPOINT | jq -r .Address) -p $(echo $ENDPOINT | jq -r .Port)

aws elasticache create-replication-group --replication-group-id my-rg \
    --replication-group-description "Test" --num-cache-clusters 2
```

## Supported operations

| Area | Operations |
|------|------------|
| Cache clusters | `CreateCacheCluster`, `DescribeCacheClusters`, `ModifyCacheCluster`, `RebootCacheCluster`, `DeleteCacheCluster` |
| Replication groups | `CreateReplicationGroup`, `DescribeReplicationGroups`, `ModifyReplicationGroup`, `DeleteReplicationGroup`, `TestFailover`, `IncreaseReplicaCount`, `DecreaseReplicaCount`, `ModifyReplicationGroupShardConfiguration` |
| Global replication groups | create, describe, modify, delete, disassociate, failover, increase and decrease node groups, rebalance slots |
| Serverless | `CreateServerlessCache`, `DescribeServerlessCaches`, `ModifyServerlessCache`, `DeleteServerlessCache`, serverless cache snapshots (create, copy, export, delete, describe) |
| Snapshots | `CreateSnapshot`, `CopySnapshot`, `DescribeSnapshots`, `DeleteSnapshot` |
| Parameter groups | create, describe, modify, reset, delete, `DescribeCacheParameters`, `DescribeEngineDefaultParameters` |
| Subnet and security groups | cache subnet groups, cache security groups with ingress authorize and revoke |
| Users | `CreateUser`, `DescribeUsers`, `ModifyUser`, `DeleteUser`, and user groups |
| Reserved nodes | `DescribeReservedCacheNodes`, `DescribeReservedCacheNodesOfferings`, `PurchaseReservedCacheNodesOffering` |
| Updates and migration | `DescribeServiceUpdates`, `DescribeUpdateActions`, `BatchApplyUpdateAction`, `BatchStopUpdateAction`, `StartMigration`, `TestMigration`, `CompleteMigration` |
| Other | `DescribeCacheEngineVersions`, `DescribeEvents`, `ListAllowedNodeTypeModifications`, `AddTagsToResource`, `RemoveTagsFromResource`, `ListTagsForResource` |

## Configuration

| Flag | Env | Default | Purpose |
|------|-----|---------|---------|
| `--elasticache-engine` | `ELASTICACHE_ENGINE` | `embedded` | `embedded` (miniredis), `stub` (metadata only) or `docker` |
| `--port-range-start` | `PORT_RANGE_START` | `10000` | Start of the port range for cluster endpoints |
| `--port-range-end` | `PORT_RANGE_END` | `10100` | End of the range (exclusive) |

In `embedded` mode the endpoint address and port are in the `DescribeCacheClusters` response.

## Differences from AWS

> [!NOTE]
> `SnapshotArns` and `CopySnapshot` `TargetBucket` are validated but nothing is imported or exported: there is no S3 wiring and miniredis has no RDB reader or writer. Restoring by `SnapshotName` uses a private gzip/gob keyspace format.

- Embedded mode takes listener ports from the allocator, so a caller-chosen `Port` is not honoured. Other modes honour it.
- `ModifyCacheCluster` `ScaleConfig` has no effect: Memcached node provisioning completes instantly.
