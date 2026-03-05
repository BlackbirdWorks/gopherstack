# Redshift

Stub Redshift implementation for cluster lifecycle management.

## Supported Operations

| Operation | Description |
|-----------|-------------|
| `CreateCluster` | Create a new Redshift cluster record |
| `DeleteCluster` | Delete a cluster record |
| `DescribeClusters` | List clusters with optional identifier filter |
| `DescribeLoggingStatus` | Get the logging configuration for a cluster |
| `DescribeTags` | List tags on a resource |
| `CreateTags` | Add tags to a resource |
| `DeleteTags` | Remove tags from a resource |

## AWS CLI Examples

```bash
# Create a cluster
aws --endpoint-url http://localhost:8000 redshift create-cluster \
    --cluster-identifier my-cluster \
    --node-type dc2.large \
    --master-username admin \
    --master-user-password Password1 \
    --number-of-nodes 2

# Describe clusters
aws --endpoint-url http://localhost:8000 redshift describe-clusters

# Describe a specific cluster
aws --endpoint-url http://localhost:8000 redshift describe-clusters \
    --cluster-identifier my-cluster

# Add tags
aws --endpoint-url http://localhost:8000 redshift create-tags \
    --resource-name arn:aws:redshift:us-east-1:000000000000:cluster:my-cluster \
    --tags Key=env,Value=dev

# Delete a cluster
aws --endpoint-url http://localhost:8000 redshift delete-cluster \
    --cluster-identifier my-cluster \
    --skip-final-cluster-snapshot
```

## Known Limitations

- No real database processes are started. The cluster endpoint is synthetic.
- SQL queries cannot be executed against the stub cluster.
- Parameter groups, subnet groups, and cluster snapshots are not implemented.

## DNS Integration

With the DNS server enabled (`--dns-addr :10053`), Redshift cluster endpoints are automatically registered. See [DNS setup](../architecture/dns.md).
