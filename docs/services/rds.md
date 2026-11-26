# RDS

RDS and Aurora control plane. By default only metadata is tracked. With the Docker engine, MySQL, MariaDB and PostgreSQL instances run real database containers.

## Quick start

```bash
export AWS_ENDPOINT_URL=http://localhost:8000

aws rds create-db-instance --db-instance-identifier my-db \
    --db-instance-class db.t3.micro --engine mysql \
    --master-username admin --master-user-password password \
    --allocated-storage 20
aws rds describe-db-instances
aws rds create-db-snapshot --db-instance-identifier my-db --db-snapshot-identifier my-db-snap-1
aws rds create-db-subnet-group --db-subnet-group-name my-subnet-group \
    --db-subnet-group-description "Test" \
    --subnet-ids subnet-12345678 subnet-87654321
```

For a real database, start Gopherstack with `--rds-engine=docker` (or `RDS_ENGINE=docker`) and connect to the instance endpoint.

## Supported operations

| Area | Operations |
|------|------------|
| Instances | create, describe, modify, delete, start, stop, reboot, promote read replica, restore from snapshot or point in time, read replicas, automated backups |
| Clusters | create, describe, modify, delete, start, stop, failover, restore from snapshot or point in time, backtracks, cluster endpoints, global clusters, read-replica clusters |
| Snapshots | DB and cluster snapshots (create, copy, delete, share attributes), automated backups |
| Parameter, option and subnet groups | DB and cluster parameter groups, option groups, subnet groups, DB security groups |
| Proxies | DB proxies, endpoints, target groups, targets |
| Blue/green | create, describe, switchover, delete |
| Events | event categories, events, event subscriptions |
| Roles and integrations | associated IAM roles, zero-ETL integrations, export tasks, activity streams |
| Reserved instances | offerings, purchase, describe |
| Reference data | engine versions, orderable options, default parameters, valid modifications, certificates, account attributes, pending maintenance actions, log files |
| Other | Data API (HTTP endpoint), shard groups, tenant databases, recommendations, tags |

## Configuration

| Env | Default | Purpose |
|-----|---------|---------|
| `RDS_ENGINE` (`--rds-engine`) | `stub` | `stub` for metadata only, `docker` for real database containers |
| `RDS_SQLSERVER_ENGINE` | off | With `docker`, run `sqlserver-*` instances as SQL Server containers |
| `RDS_POSTGRES_EXTENSIONS` | on | `false` runs stock `postgres:<major>` instead of the PostGIS and pgvector image |
| `RDS_AUTO_PAUSE_DISABLED` | off | Disable Aurora Serverless v2 auto-pause |
| `RDS_AUTO_PAUSE_IDLE_SCALE` | `1` | Multiplies `SecondsUntilAutoPause` (`0.01` turns 300 s into 3 s) |
| `RDS_AUTO_PAUSE_RESUME_DELAY` | none | Extra time a resume takes |

With the DNS server enabled (`--dns-addr :10053`), instance hostnames are registered automatically and resolve to `127.0.0.1`, or `--dns-resolve-ip`. See [DNS setup](../architecture/dns.md).

## Differences from AWS

> [!IMPORTANT]
> Without `--rds-engine=docker`, no database runs. Endpoints are synthetic hostnames and nothing accepts connections.

- Docker engine: read replicas, snapshot and point-in-time restores, custom cluster endpoints and `DBPortNumber` changes run real engines. Snapshots and periodic restore points (every 5 minutes for units with `BackupRetentionPeriod > 0`) capture a logical dump.
- SQL Server runs `mcr.microsoft.com/mssql/server:<2017|2019|2022>-latest`, chosen from the major in `EngineVersion` (14, 15 or 16; anything else, including 13, uses 2022). amd64 hosts only.
- PostgreSQL units run `postgis/postgis:<major>-3.5` with pgvector 0.8.6, so `CREATE EXTENSION postgis` and `vector` work.
- MySQL and MariaDB IAM database authentication proxies the handshake only. The session runs as the engine master login, so per-IAM-user privileges are not applied.
- `CreateDBInstance` accepts `EngineVersion` values outside the small built-in catalog.
- Snapshot sharing is account-aware only for snapshots created or copied by an explicit-account caller. Other resources are not account-scoped, and snapshot identifiers are global.
- No cross-region copy (`SourceRegion`, `PreSignedUrl`). `IncludePublic` and `IncludeShared` have no effect.
- `MaxAllocatedStorage` is stored but never autoscales. Every operation applies synchronously, so `CertificateRotationRestart`, `SwitchoverTimeout` and storage-operation progress fields describe windows that never exist.
- `GetPerformanceInsightsMetrics` is kept as seeded functionality. It is not a real RDS operation (the real one is `GetResourceMetrics` on the `pi` client).
