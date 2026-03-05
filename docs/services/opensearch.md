# OpenSearch Service

Stub OpenSearch implementation for domain lifecycle management. Does not start a real OpenSearch/Elasticsearch process.

## Supported Operations

| Operation | Description |
|-----------|-------------|
| `CreateDomain` | Create an OpenSearch domain record |
| `DescribeDomain` | Get domain configuration and status |
| `DeleteDomain` | Delete a domain |
| `ListDomainNames` | List all domains |

## AWS CLI Examples

```bash
# Create a domain
aws --endpoint-url http://localhost:8000 opensearch create-domain \
    --domain-name my-search \
    --engine-version OpenSearch_2.11 \
    --cluster-config InstanceType=t3.small.search,InstanceCount=1

# Describe a domain
aws --endpoint-url http://localhost:8000 opensearch describe-domain \
    --domain-name my-search

# List domains
aws --endpoint-url http://localhost:8000 opensearch list-domain-names

# Delete a domain
aws --endpoint-url http://localhost:8000 opensearch delete-domain \
    --domain-name my-search
```

## Known Limitations

- No real OpenSearch/Elasticsearch process is started. The endpoint returned is a synthetic hostname.
- Index management, search, and aggregation APIs are not implemented.
- Fine-grained access control and encryption at rest are not enforced.

## Configuration

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--opensearch-engine` | `OPENSEARCH_ENGINE` | `stub` | Engine mode: `stub` or `docker` |

In `docker` mode, a real OpenSearch container is started when a domain is created.
