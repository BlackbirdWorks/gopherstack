# Redshift

In-memory Redshift control plane for provisioned clusters and Redshift Serverless. No SQL engine unless you enable the Docker engine.

## Quick start

```bash
export AWS_ENDPOINT_URL=http://localhost:8000

aws redshift create-cluster --cluster-identifier my-cluster \
    --node-type dc2.large --master-username admin \
    --master-user-password Password1 --number-of-nodes 2
aws redshift describe-clusters --cluster-identifier my-cluster
aws redshift create-tags \
    --resource-name arn:aws:redshift:us-east-1:000000000000:cluster:my-cluster \
    --tags Key=env,Value=dev
aws redshift delete-cluster --cluster-identifier my-cluster --skip-final-cluster-snapshot
```

## Supported operations

| Area | Operations |
|------|------------|
| Clusters | create, describe, modify, delete, reboot, resize, pause, resume, restore from snapshot, rotate encryption key, maintenance, logging, DB revisions, tracks and versions |
| Snapshots | cluster snapshots (create, copy, delete, modify, authorize and revoke access), snapshot schedules, snapshot copy and copy grants, table restore |
| Parameter and subnet groups | cluster parameter groups, cluster subnet groups, cluster security groups |
| Security | IAM roles on clusters, HSM client certificates and configurations, authentication profiles, `GetClusterCredentials`, `GetClusterCredentialsWithIAM`, IAM Identity Center applications |
| Networking | endpoint access and authorization, custom domain associations |
| Data sharing | data shares, producer and consumer operations |
| Events | event categories, events, event subscriptions |
| Scheduling and limits | scheduled actions, usage limits |
| Reserved nodes | offerings, purchase, exchange |
| Integrations | zero-ETL integrations, partners |
| Tags | `CreateTags`, `DeleteTags`, `DescribeTags` |
| Redshift Serverless | namespaces, workgroups, endpoint access, snapshots, recovery points, restore, table restore, usage limits, scheduled actions, custom domains, resource policies, tagging |

## Configuration

| Flag | Env | Default | Purpose |
|------|-----|---------|---------|
| `--redshift-engine` | `REDSHIFT_ENGINE` | `stub` | `stub` or `docker` |

With the DNS server enabled (`--dns-addr :10053`), cluster endpoints are registered automatically. See [DNS setup](../architecture/dns.md).

## Differences from AWS

> [!WARNING]
> With `--redshift-engine=docker` each provisioned cluster and each Serverless workgroup starts one plaintext `postgres:17` container. It is PostgreSQL, not Redshift, so Redshift-only SQL does not work. A workgroup stays `CREATING` until its engine answers, and is dropped if it never starts.

- Without the engine there is no SQL. `RestoreTableFromClusterSnapshot` `EnableCaseSensitiveIdentifier`, `GetClusterCredentials` `DbGroups`, `CreateCluster` `LoadSampleData` and the Terraform data-share resources (`CREATE DATASHARE`) have no effect.
- Serverless `Workgroup.CrossAccountVpcs`, `PatchVersion`, `PendingTrackName`, `WorkgroupVersion` and `Endpoint.VpcEndpoints` are not modelled.
- `ModifyCluster` `MaintenanceTrackName` stays in `PendingModifiedValues`: no maintenance window runs.
- `ElasticIp` is syntax-checked only. `MasterPasswordSecretArn` is generated without creating a Secrets Manager secret.
