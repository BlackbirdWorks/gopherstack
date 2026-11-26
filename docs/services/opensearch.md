# OpenSearch Service

In-memory OpenSearch Service control plane with a built-in data-plane engine, and OpenSearch Serverless operations on the same handler.

## Quick start

```bash
export AWS_ENDPOINT_URL=http://localhost:8000

aws opensearch create-domain --domain-name my-search \
    --engine-version OpenSearch_2.11 \
    --cluster-config InstanceType=t3.small.search,InstanceCount=1
aws opensearch describe-domain --domain-name my-search
aws opensearch list-domain-names
aws opensearch delete-domain --domain-name my-search
```

## Supported operations

| Area | Operations |
|------|------------|
| Domains | `CreateDomain`, `DescribeDomain`, `DescribeDomains`, `ListDomainNames`, `DeleteDomain`, `DescribeDomainConfig`, `UpdateDomainConfig`, `DescribeDomainNodes`, domain change progress, auto-tune, maintenance and health operations |
| Versions and upgrades | `ListVersions`, `GetCompatibleVersions`, `UpgradeDomain`, `GetUpgradeStatus`, `GetUpgradeHistory`, `DescribeInstanceTypeLimits`, `ListInstanceTypeDetails` |
| Software updates | `StartServiceSoftwareUpdate`, `CancelServiceSoftwareUpdate`, scheduled actions |
| Packages | create, describe, update, delete, associate, dissociate, list, version history |
| Connections | cross-cluster inbound and outbound connections: create, accept, reject, describe, delete |
| VPC endpoints | `CreateVpcEndpoint`, `UpdateVpcEndpoint`, `DeleteVpcEndpoint`, `ListVpcEndpoints`, `DescribeVpcEndpoints`, `ListVpcEndpointsForDomain`, `AuthorizeVpcEndpointAccess`, `RevokeVpcEndpointAccess`, `ListVpcEndpointAccess` |
| Applications and data sources | applications, data sources, direct-query data sources, default application settings |
| Reserved instances | `DescribeReservedInstanceOfferings`, `PurchaseReservedInstanceOffering`, `DescribeReservedInstances` |
| Indices | `CreateIndex`, `GetIndex`, `UpdateIndex`, `DeleteIndex` |
| Insights and migrations | insights, migrations |
| Tags | `AddTags`, `RemoveTags`, `ListTags` |
| OpenSearch Serverless | collections, collection groups, security policies and configs, access policies, lifecycle policies, VPC endpoints, indices, account settings, tags |

Data plane on the built-in engine: index create, get and delete, document index, get and delete, `_search`, `_count` and `_bulk`.

## Configuration

| Flag | Env | Default | Purpose |
|------|-----|---------|---------|
| `--opensearch-engine` | `OPENSEARCH_ENGINE` | `stub` | `stub` or `docker`. `docker` is reserved: both modes use the built-in in-memory engine |
| `--opensearch-endpoint-strategy` | `OPENSEARCH_ENDPOINT_STRATEGY` | AWS-style `es.amazonaws.com` host | Domain endpoint format: `domain` or `path` |
| `--opensearch-multi-cluster` | `OPENSEARCH_MULTI_CLUSTER` | `true` | `false` makes every domain in a region share one index store |
| `--opensearch-custom-backend` | `OPENSEARCH_CUSTOM_BACKEND` | none | URL of an existing OpenSearch cluster that serves every domain's data-plane requests |

## Differences from AWS

> [!IMPORTANT]
> No real OpenSearch or Elasticsearch process runs. Domain endpoints are served by a built-in engine that covers the routes above, not the full OpenSearch API. Set `OPENSEARCH_CUSTOM_BACKEND` to use a real cluster for the data plane.

- Fine-grained access control covers the routes the built-in engine serves. Document-level security, field-level security and masking apply to `_search`, `_count` and document get only. A `_bulk` request lacking `indices:data/write/bulk` is rejected whole, and a bulk item denied for permissions is reported per item.
- With `OPENSEARCH_CUSTOM_BACKEND` set, requests are proxied untouched and the upstream security plugin judges credentials. Emulated users, roles and mappings are not mirrored into it.
- Security plugin tenants are stored and served by the REST API, but `tenant_permissions` are not enforced. JWT, SAML and IAM federation options are stored, but no JWT or SAML login is performed. Role-mapping hosts are not evaluated.
- `MigrationSummary.Error` is never emitted: migrations always settle `SUCCEEDED`.
