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

## Not covered

Lambda already starts containers on demand and the EC2 docker provider has its own `--ec2-provider` flag; neither
is affected by `--engines`.
