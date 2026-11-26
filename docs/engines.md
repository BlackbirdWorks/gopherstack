# Engine modes

Container-backed services default to in-memory stubs (ElastiCache and MemoryDB default to `embedded`). `--engines` (env `GOPHERSTACK_ENGINES`) switches them all at once:

| Value | Behaviour |
|-------|-----------|
| `stub` (default) | Each service keeps its own default |
| `auto` | Probes Docker, then Podman, once at startup (5 s limit). If reachable, real containers; otherwise stubs and a warning. Startup never fails |
| `docker` | Requests real containers. A service whose runtime is unavailable logs a warning and stays metadata-only |

With `auto`, if Podman is the reachable runtime and `CONTAINER_RUNTIME` is unset, it is set to `podman`.

## Per-service flags

| Flag | Env var | Fallback |
|------|---------|----------|
| `--rds-engine` | `RDS_ENGINE` | `stub` |
| `--docdb-engine` | `DOCDB_ENGINE` | `stub` |
| `--neptune-engine` | `NEPTUNE_ENGINE` | `stub` (also `embedded`) |
| `--redshift-engine` | `REDSHIFT_ENGINE` | `stub` |
| `--dms-engine` | `DMS_ENGINE` | `stub` |
| `--dsql-engine` | `DSQL_ENGINE` | `stub` |
| `--kafka-engine` | `KAFKA_ENGINE` | `stub` |
| `--mq-engine` | `MQ_ENGINE` | `stub` |
| `--flink-engine` | `FLINK_ENGINE` | `stub` |
| `--mwaa-engine` | `MWAA_ENGINE` | `stub` |
| `--eks-engine` | `EKS_ENGINE` | `stub` |
| `--gke-engine` | `GKE_ENGINE` | `stub` |
| `--oci-oke-engine` | `OKE_ENGINE` | `stub` |
| `--oci-mysql-engine` | `OCI_MYSQL_ENGINE` | `stub` |
| `--oci-database-engine` | `OCI_DATABASE_ENGINE` | `stub` |
| `--opensearch-engine` | `OPENSEARCH_ENGINE` | `stub` |
| `--elasticsearch-engine` | `ELASTICSEARCH_ENGINE` | `stub` |
| `--sagemaker-engine` | `SAGEMAKER_ENGINE` | `stub` |
| `--codebuild-engine` | `CODEBUILD_ENGINE` | `stub` |
| `--glue-engine` | `GLUE_ENGINE` | `stub` |
| `--transcribe-engine` | `TRANSCRIBE_ENGINE` | `stub` |
| `--elasticache-engine` | `ELASTICACHE_ENGINE` | `embedded` |
| `--memorydb-engine` | `MEMORYDB_ENGINE` | `embedded` |
| `--lambdaweb-engine` | `LAMBDAWEB_ENGINE` | `docker` |

Other switches: `--ecs-runtime` / `GOPHERSTACK_ECS_RUNTIME` (`none` or `docker`; unset follows `--engines`; with `CONTAINER_RUNTIME=kubernetes` each task is a Pod, see [Kubernetes](kubernetes.md)) and `TIMESTREAM_INFLUXDB_ENGINE`. Run `gopherstack --help` for each flag's accepted values.

## Precedence

A per-service flag or env var, or any non-default value, always beats `--engines`. `GOPHERSTACK_ENGINES=auto RDS_ENGINE=stub` runs everything real except RDS.

Resolved modes are not written to persisted config.

Lambda starts containers on demand and EC2 has its own `--ec2-provider` flag. Neither is affected by `--engines`.

## Containers left by a crashed Gopherstack

Containers started through `pkgs/container` carry these labels:

| Label | Value |
|-------|-------|
| `gopherstack.managed` | `true` |
| `gopherstack.owner` | Derived from the data dir when persistence is on (stable across restarts), fresh per run otherwise |
| `gopherstack.pid` | `pid:starttime` of the launcher |
| `gopherstack.ephemeral` | `true` when persistence is off |
| `gopherstack.service`, `gopherstack.resource` | Set by services; `resource` only when they can re-attach |

At startup, before services restore:

- Same-owner containers whose launcher is gone and that have no `gopherstack.resource` are removed.
- Same-owner containers with a `gopherstack.resource` are held. A service restoring that resource adopts it (`ClaimOrphan`). Anything unclaimed after 3 minutes is removed.
- Another owner's containers are removed only if `gopherstack.ephemeral=true` and the recorded process (pid and start time) is dead. Live processes, other owners and unlabelled containers are never touched.

Services that adopt: Cloud SQL (`gcpsql`), EC2 docker instances, ECS docker tasks, AKS clusters.

- **EC2**: a restored pending or running instance claims its container by instance ID. It is adopted if the container is the recorded one, runs the same AMI and is running; the recorded address is refreshed. Not adopted (container removed): providers mapping EBS volumes, and instances behind security-group relays. An instance's env-based metadata endpoint points at a per-run port, so it is stale after adoption unless the link-local relay is on.
- **ECS**: a restored RUNNING task claims one container per container definition (`taskArn|containerName`). All must exist, run and use the definition's image. Then exit, health and log watching resume (logs from adoption time on). Otherwise all claimed containers are removed and the task is STOPPED ("Task containers were lost when the emulator restarted"). Containers behind security-group relays and awsvpc tasks are never adopted.
- **Batch**: containers have identity and service labels but no resource key. A job lost to a crash is failed ("Container execution was interrupted") and its container removed at next startup.
- **AKS**: a restored running cluster claims its k3s node by `<resource group>/<cluster>` (lower case). The node's CA is read back and its API server must answer; otherwise the node is removed and a fresh one starts on the cluster's data volume (`gopherstack-aks-<hash>`), which keeps Kubernetes state. The volume is removed with the cluster, and at restore if no restored cluster owns it.

RDS, EKS, GKE and OKE label their containers; these are cleaned up at startup and relaunched.
