# Engine modes

Container-backed services (RDS/Aurora, MSK, Amazon MQ, OpenSearch, Elasticsearch, DocumentDB, Neptune,
Redshift, EKS, ElastiCache, MemoryDB, CodeBuild, Glue interactive sessions, Timestream for InfluxDB, ECS tasks) default to in-memory
stubs. `--engines` (env `GOPHERSTACK_ENGINES`) switches them all at once:

| Value | Behaviour |
|-------|-----------|
| `stub` (default) | Today's behaviour: each service uses its own default (`stub`, or `embedded` for ElastiCache/MemoryDB). |
| `auto` | Probe Docker, then Podman, once at startup (5s bound). Reachable: real containers. Not reachable: stub, with a warning logged. Startup never fails. |
| `docker` | Always request real containers. A service whose runtime is unavailable logs a warning and stays metadata-only, as with its own `docker` flag. |

If Podman is the reachable runtime and `CONTAINER_RUNTIME` is unset, it is set to `podman` for the services.

## Precedence

A per-service flag (`--rds-engine`, ...), its env var (`RDS_ENGINE`, ...) or a non-default value always wins over
`--engines`, so `GOPHERSTACK_ENGINES=auto RDS_ENGINE=stub` runs everything real except RDS. The ECS task runtime is
`--ecs-runtime` / `GOPHERSTACK_ECS_RUNTIME` (`none` or `docker`); Timestream for InfluxDB follows
`TIMESTREAM_INFLUXDB_ENGINE`.

Resolved modes are not written to persisted config.

## Containers left by a crashed gopherstack

Every container started through `pkgs/container` carries `gopherstack.managed=true`, `gopherstack.owner`,
`gopherstack.pid` (`pid:starttime` of the launcher) and, with persistence off, `gopherstack.ephemeral=true`.
Services add `gopherstack.service` and, when they can re-attach, `gopherstack.resource`. The owner is derived from
the data dir when persistence is on (stable across restarts) and is fresh per run otherwise.

At startup, before services restore:

- Same-owner containers whose launcher process is gone and that have no `gopherstack.resource` are removed.
- Same-owner, resource-labelled containers are held; a service restoring that resource adopts it (`ClaimOrphan`),
  and anything unclaimed after 3 minutes is removed.
- Another owner's containers are removed only when `gopherstack.ephemeral=true` and the recorded process (pid and
  start time) is dead. Live processes, other owners and unlabelled containers are never touched.

Adopting services: Cloud SQL (gcpsql), EC2 docker instances, ECS docker tasks and AKS clusters.

- EC2: a restored pending or running instance claims its container by instance id; it is adopted when the container
  is the recorded one, runs the same AMI and is running, and the recorded address is refreshed from it. Not adopted
  (container removed): providers mapping EBS volumes, and instances behind security-group relays (no resource key).
  An instance's env-based metadata endpoint points at a per-run port, so it is stale after adoption unless the
  link-local relay is on.
- ECS: a restored RUNNING task claims one container per container definition (`taskArn|containerName`). All must
  exist, run, and use the definition's image; then exit, health and log watching resume (logs from adoption time on).
  Otherwise every claimed container is removed and the task is STOPPED ("Task containers were lost when the
  emulator restarted"). Containers behind security-group relays are never adopted.
- Batch: containers carry the identity and service labels but no resource key. A job lost to a crash is failed
  ("Container execution was interrupted") and its container removed at the next startup.

- AKS: a restored running cluster claims its k3s node by `<resource group>/<cluster>` (lower case). The node's CA
  is read back from the container and its API server must answer before it is used; otherwise it is removed and a
  fresh node starts on the cluster's data volume (`gopherstack-aks-<hash>`), which keeps the Kubernetes state. The
  volume is removed with the cluster, and at restore when no restored cluster owns it.

RDS, EKS, GKE and OKE label the service and are cleaned up at startup, then relaunch as before.

## Not covered

Lambda already starts containers on demand and the EC2 docker provider has its own `--ec2-provider` flag; neither
is affected by `--engines`.
