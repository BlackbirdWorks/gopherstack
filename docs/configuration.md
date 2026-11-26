# Configuration

Every `gopherstack serve` flag has an environment variable; a flag wins over its env var. With `--persist`, the persisted config sits between: defaults, then persisted config, then env vars and flags.

> [!NOTE]
> `gopherstack --help` and `gopherstack serve --help` are authoritative. This page is checked against them.

## Server

Listener, identity, startup and housekeeping.

> [!WARNING]
> A non-loopback `--host` exposes an unauthenticated emulator; the server logs a warning unless `--allow-remote` is set.

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--port` | `PORT` | `8000` | HTTP server port |
| `--host` | `GOPHERSTACK_HOST` | — | Listen host for the HTTP listener. Empty = all interfaces (unchanged default) |
| `--allow-remote` | `GOPHERSTACK_ALLOW_REMOTE` | `false` | Acknowledge a non-loopback listen address (silences the startup warning) |
| `--extra-listen` | `GOPHERSTACK_EXTRA_LISTEN` | — | Additional ports to serve the same endpoint on (comma-separated) |
| `--region` | `REGION` | `us-east-1` | AWS region (also read from AWS_DEFAULT_REGION and AWS_REGION) |
| `--account-id` | `ACCOUNT_ID` | `000000000000` | Mock AWS account ID used in ARNs |
| `--log-level` | `LOG_LEVEL` | `info` | Log level (debug\|info\|warn\|error) |
| `--services` | `GOPHERSTACK_SERVICES` | — | Only enable these services (comma-separated, case-insensitive; LocalStack names accepted). Empty = all |
| `--disable-services` | `GOPHERSTACK_DISABLE_SERVICES` | — | Disable these services (comma-separated, case-insensitive). Applied after --services |
| `--demo` | `DEMO` | `false` | Load demo data on startup |
| `--compression` | `COMPRESSION` | `on` | Runtime response compression: zstd/br/gzip for the dashboard, gzip for DynamoDB clients that opt in |
| `--data-dir` | `GOPHERSTACK_DATA_DIR` | — | Directory for persistence data files (default: ~/.gopherstack/data, or /data in containers) |
| `--auto-purge-ttl` | `AUTO_PURGE_TTL` | — | If set, automatically reset all services on a timer based on the TTL (e.g., 10m) |
| `--extension` | `GOPHERSTACK_EXTENSIONS` | — | Extension as name=command args, or name=image:REF (repeatable) |
| `--extensions-config` | `GOPHERSTACK_EXTENSIONS_CONFIG` | — | YAML/JSON file declaring extensions (executables or container images) |
| `--init-dir` | `INIT_DIR` | — | Extra init-hook root holding boot.d/start.d/ready.d/shutdown.d (repeatable; /etc/localstack/init and /etc/gopherstack/init are always checked) |
| `--init-script` | `INIT_SCRIPTS` | — | Shell scripts to run on startup (may be specified multiple times) |
| `--init-timeout` | `INIT_TIMEOUT` | `30s` | Per-script timeout for init hooks |
| `--janitor-timeout` | `JANITOR_TIMEOUT` | `30s` | Per-task timeout for janitor operations (TTL sweeps, table cleaners, etc.). Zero disables per-task timeouts. Higher values prevent deadlocks; lower values keep the janitor loop responsive |
| `--latency-ms` | `LATENCY_MS` | `0` | Inject random latency [0,N) ms per request (0 = disabled). Values near the 30 s write timeout may cause connection errors |
| `--port-range-end` | `PORT_RANGE_END` | `10100` | End (exclusive) of the port range for resource endpoints |
| `--port-range-start` | `PORT_RANGE_START` | `10000` | Start of the port range for resource endpoints |
| `--s3-bucket` | `S3_BUCKETS` | — | S3 bucket names to create on startup (may be specified multiple times or as a comma-separated list) |

## TLS and CORS

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--cors-allow-credentials` | `GOPHERSTACK_CORS_ALLOW_CREDENTIALS` | `false` | Send Access-Control-Allow-Credentials: true |
| `--cors-allow-headers` | `GOPHERSTACK_CORS_ALLOW_HEADERS`, `EXTRA_CORS_ALLOWED_HEADERS` | — | Extra headers allowed in CORS requests (comma-separated) |
| `--cors-allow-origins` | `GOPHERSTACK_CORS_ALLOW_ORIGINS`, `EXTRA_CORS_ALLOWED_ORIGINS` | — | Origins allowed by CORS (comma-separated, or *). Empty = no global CORS handling |
| `--cors-disable-checks` | `GOPHERSTACK_DISABLE_CORS_CHECKS`, `DISABLE_CORS_CHECKS` | `false` | Allow any origin (LocalStack DISABLE_CORS_CHECKS) |
| `--cors-disable-custom-apigateway` | `GOPHERSTACK_CORS_DISABLE_CUSTOM_APIGATEWAY`, `DISABLE_CUSTOM_CORS_APIGATEWAY` | `false` | Ignore API Gateway CORS configuration so only the global CORS settings apply (LocalStack DISABLE_CUSTOM_CORS_APIGATEWAY) |
| `--cors-expose-headers` | `GOPHERSTACK_CORS_EXPOSE_HEADERS`, `EXTRA_CORS_EXPOSE_HEADERS` | — | Response headers exposed to browsers (comma-separated) |
| `--tls` | `TLS` | `false` | Serve over HTTPS. With --tls-cert/--tls-key uses those files; otherwise a self-signed certificate is generated on demand |
| `--tls-cert` | `TLS_CERT` | — | Path to a TLS certificate (PEM). Enables an HTTPS listener; requires --tls-key. Empty = HTTP only |
| `--tls-key` | `TLS_KEY` | — | Path to a TLS private key (PEM). Required with --tls-cert |

## Persistence

> [!WARNING]
> Persistence is off by default; without `--persist`, all state is lost on exit.

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--persist` | `PERSIST` | `false` | Enable snapshot-based persistence across restarts |
| `--persist-exclude` | `GOPHERSTACK_PERSIST_EXCLUDE` | — | Do not persist these services (comma-separated, case-insensitive). Applied after --persist-services |
| `--persist-flush-interval` | `GOPHERSTACK_PERSIST_FLUSH_INTERVAL` | `5s` | Hybrid mode flush interval; worst-case data loss on a crash |
| `--persist-load` | `GOPHERSTACK_PERSIST_LOAD` | `startup` | When persisted state loads: startup (default), request (lazily per service on its first request) or manual (only via POST /_gopherstack/persist/load) |
| `--persist-mode` | `GOPHERSTACK_PERSIST_MODE` | `snapshot` | Persistence mode: snapshot (debounced per-request), hybrid (periodic flush of changed services), shutdown (save only at graceful shutdown) or manual (save only via POST /_gopherstack/persist/save) |
| `--persist-service-mode` | `GOPHERSTACK_PERSIST_SERVICE_MODE` | — | Per-service mode override as service=mode (comma-separated), e.g. s3=hybrid,sqs=snapshot |
| `--persist-services` | `GOPHERSTACK_PERSIST_SERVICES` | — | Only persist these services (comma-separated, case-insensitive). Empty = all |

## Pods

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--pod-autoload` | `GOPHERSTACK_POD_AUTOLOAD`, `AUTO_LOAD_POD` | — | Load pods NAME[:VERSION] (comma-separated, in order) at startup |
| `--pod-dir` | `GOPHERSTACK_POD_DIR` | — | Local pod directory (default: &lt;data-dir>/pods) |
| `--pod-merge-strategy` | `GOPHERSTACK_POD_MERGE_STRATEGY`, `MERGE_STRATEGY` | — | How autoloaded pods combine with current state: overwrite, service-merge or account-region-merge |
| `--pod-s3-access-key` | `GOPHERSTACK_POD_S3_ACCESS_KEY` | — | Access key for the pod S3 bucket (default credential chain if empty) |
| `--pod-s3-bucket` | `GOPHERSTACK_POD_S3_BUCKET` | — | Store pods in this S3 bucket instead of the local directory |
| `--pod-s3-endpoint` | `GOPHERSTACK_POD_S3_ENDPOINT` | — | S3-compatible endpoint URL (empty means real AWS S3) |
| `--pod-s3-path-style` | `GOPHERSTACK_POD_S3_PATH_STYLE` | `false` | Use path-style S3 addressing |
| `--pod-s3-prefix` | `GOPHERSTACK_POD_S3_PREFIX` | — | Key prefix for pods in the S3 bucket |
| `--pod-s3-region` | `GOPHERSTACK_POD_S3_REGION` | `us-east-1` | Region of the pod S3 bucket |
| `--pod-s3-secret-key` | `GOPHERSTACK_POD_S3_SECRET_KEY` | — | Secret key for the pod S3 bucket |

## IAM and request security

> [!WARNING]
> IAM enforcement and SigV4 validation are off by default: every request is accepted and signatures are never checked. Do not expose an instance beyond loopback.

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--enforce-iam` | `GOPHERSTACK_ENFORCE_IAM`, `ENFORCE_IAM` | `false` | Enable IAM policy enforcement. When true, every AWS API request is evaluated against attached IAM policies |
| `--enforce-security-groups` | `GOPHERSTACK_ENFORCE_SECURITY_GROUPS` | `false` | Enforce EC2 security-group ingress on container-backed endpoints (RDS, EC2, DocDB, MSK, caches, MQ, Neptune, ECS); ingress only. Default off |
| `--enforce-service-quotas` | `GOPHERSTACK_ENFORCE_SERVICE_QUOTAS` | `false` | Make S3, EC2, IAM, EKS and DynamoDB reject creates beyond the Service Quotas applied limit. Default off |
| `--iam-audit` | `GOPHERSTACK_IAM_AUDIT`, `IAM_SOFT_MODE` | `false` | IAM audit (soft) mode: evaluate and record every request in the IAM Policy Stream but never deny |
| `--iam-enforce-scps` | `GOPHERSTACK_IAM_ENFORCE_SCPS` | `true` | With IAM enforcement on, also evaluate Organizations service control policies for member-account principals. Set false to skip SCPs |
| `--sigv4-secret` | `SIGV4_SECRET` | `test` | Secret access key SigV4 validation signs against (used only when --validate-sigv4 is set) |
| `--validate-sigv4` | `VALIDATE_SIGV4` | `false` | Cryptographically validate AWS SigV4 request signatures (opt-in). Signed requests whose signature does not match --sigv4-secret are rejected |

## LocalStack compatibility

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--localstack-compat` | `GOPHERSTACK_LOCALSTACK_COMPAT` | `false` | LocalStack drop-in mode: also listen on 4566 (auto-enabled when LOCALSTACK_*, GATEWAY_LISTEN, LS_LOG or PERSISTENCE is set) |
| `--localstack-health-kill` | `GOPHERSTACK_LOCALSTACK_HEALTH_KILL` | `false` | Allow POST /_localstack/health with action=kill (loopback peers only) to gracefully stop the process |
| `--localstack-response-header` | `GOPHERSTACK_LOCALSTACK_RESPONSE_HEADER`, `LOCALSTACK_RESPONSE_HEADER_ENABLED` | `false` | Add the X-Localstack: true response header |

## DNS and transparent mode

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--dns-addr` | `DNS_ADDR` | — | Address for embedded DNS server (e.g. :10053). Empty = disabled |
| `--dns-resolve-ip` | `DNS_RESOLVE_IP` | `127.0.0.1` | IP address synthetic hostnames resolve to |
| `--transparent-ca-dir` | `TRANSPARENT_CA_DIR` | — | Directory persisting the CA (default: ephemeral) |
| `--transparent-container-ca-path` | `TRANSPARENT_CONTAINER_CA_PATH` | — | CA PEM path visible to the Docker daemon (required when gopherstack runs in Docker) |
| `--transparent-container-dns` | `TRANSPARENT_CONTAINER_DNS` | — | DNS IP for spawned containers |
| `--transparent-disable-injection` | `TRANSPARENT_DISABLE_ENDPOINT_INJECTION` | `false` | Force transparent endpoint injection off even when --transparent-dns is set |
| `--transparent-dns` | `TRANSPARENT_DNS` | `false` | Resolve AWS endpoint names here and serve HTTPS |
| `--transparent-dns-skip` | `TRANSPARENT_DNS_SKIP` | — | Names/zones that keep resolving upstream |
| `--transparent-dns-skip-patterns` | `TRANSPARENT_DNS_SKIP_PATTERNS` | — | Regexes (whole name) that keep resolving upstream |
| `--transparent-dns-upstream` | `TRANSPARENT_DNS_UPSTREAM` | — | Upstream resolvers host:port |
| `--transparent-http-addr` | `TRANSPARENT_HTTP_ADDR` | — | Optional plain-HTTP listen addr (e.g. :80) for http:// AWS endpoint URLs; empty disables |
| `--transparent-https-addr` | `TRANSPARENT_HTTPS_ADDR` | `:443` | HTTPS listen addr |

## MCP server

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--mcp-enabled` | `GOPHERSTACK_MCP` | `false` | Serve MCP (streamable HTTP) at /_gopherstack/mcp |
| `--mcp-read-only` | `GOPHERSTACK_MCP_READ_ONLY` | `false` | Expose only read-only MCP tools (no snapshots, resets, chaos or init hook runs) |
| `--mcp-token` | `GOPHERSTACK_MCP_TOKEN` | — | Bearer token required by the MCP endpoint. Without it only loopback clients are accepted |

## Container runtime and engines

`auto` probes the Docker then Podman socket at startup; per-service flags override `--engines`.

Also documented in [Engines](engines.md), [elasticache](services/elasticache.md), [opensearch](services/opensearch.md), [rds](services/rds.md), [redshift](services/redshift.md), [transcribe](services/transcribe.md).

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--agentcore-engine` | `AGENTCORE_ENGINE` | `auto` | Container engine for agent runtimes and the code interpreter: auto (container runtime if reachable) or none |
| `--codebuild-engine-image` | `CODEBUILD_ENGINE_IMAGE` | `ubuntu:22.04` | Image standing in for aws/codebuild/* curated images in docker engine mode |
| `--docker-registry-auth` | `GOPHERSTACK_DOCKER_REGISTRY_AUTH` | — | Registry credentials host=user:pass (repeatable) |
| `--docker-registry-mirror` | `GOPHERSTACK_DOCKER_REGISTRY_MIRROR` | — | Pull mirrors as registry=mirror-host, e.g. docker.io=mirror.example.com (comma-separated or repeated) |
| `--dsql-engine-image` | `DSQL_ENGINE_IMAGE` | `postgres:17` | PostgreSQL image |
| `--ecs-runtime` | `GOPHERSTACK_ECS_RUNTIME` | — | ECS task runtime: none or docker. Unset follows --engines |
| `--engines` | `GOPHERSTACK_ENGINES` | `stub` | Engine default: stub, auto (docker/podman if reachable, else stub), or docker. Per-service flags override |
| `--glue-engine-image` | `GLUE_ENGINE_IMAGE` | — | Image for docker-engine sessions; empty picks the AWS Glue libs image matching the session GlueVersion |

## Janitors and TTLs

Sweep intervals and retention per service.

Also documented in [stepfunctions](services/stepfunctions.md).

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--athena-execution-ttl` | `ATHENA_EXECUTION_TTL` | `24h` | TTL for completed query executions before they are evicted |
| `--athena-janitor-interval` | `ATHENA_JANITOR_INTERVAL` | `1m` | Janitor tick interval |
| `--backup-janitor-interval` | `BACKUP_JANITOR_INTERVAL` | `1m` | Janitor tick interval |
| `--backup-job-ttl` | `BACKUP_JOB_TTL` | `24h` | TTL for completed backup jobs before they are evicted |
| `--batch-completed-job-ttl` | `BATCH_COMPLETED_JOB_TTL` | `24h` | TTL for completed or failed jobs before they are evicted |
| `--batch-inactive-job-def-ttl` | `BATCH_INACTIVE_JOB_DEF_TTL` | `24h` | TTL for inactive job definitions before they are evicted |
| `--batch-janitor-interval` | `BATCH_JANITOR_INTERVAL` | `1m` | Janitor tick interval |
| `--cloudwatchlogs-janitor-interval` | `CLOUDWATCHLOGS_JANITOR_INTERVAL` | `1m` | Janitor tick interval |
| `--codebuild-build-ttl` | `CODEBUILD_BUILD_TTL` | `24h` | TTL for completed builds before they are evicted |
| `--codebuild-janitor-interval` | `CODEBUILD_JANITOR_INTERVAL` | `1m` | Janitor tick interval |
| `--dynamodb-janitor-interval` | `DYNAMODB_JANITOR_INTERVAL` | `500ms` | Janitor interval |
| `--ec2-cancelled-spot-ttl` | `EC2_CANCELLED_SPOT_TTL` | `6h` | TTL for cancelled spot requests before they are evicted |
| `--ec2-janitor-interval` | `EC2_JANITOR_INTERVAL` | `1m` | Janitor tick interval |
| `--ec2-terminated-ttl` | `EC2_TERMINATED_TTL` | `1h` | TTL for terminated instances before they are evicted |
| `--emr-janitor-interval` | `EMR_JANITOR_INTERVAL` | `1m` | Janitor tick interval |
| `--emr-terminated-ttl` | `EMR_TERMINATED_TTL` | `1h` | TTL for terminated clusters before they are evicted |
| `--fis-experiment-ttl` | `FIS_EXPERIMENT_TTL` | `24h` | Done-exp TTL |
| `--fis-janitor-interval` | `FIS_JANITOR_INTERVAL` | `1m` | Janitor tick |
| `--kinesis-janitor-interval` | `KINESIS_JANITOR_INTERVAL` | `1m` | Janitor tick interval |
| `--kms-janitor-interval` | `KMS_JANITOR_INTERVAL` | `1m` | Janitor tick interval |
| `--s3-janitor-interval` | `S3_JANITOR_INTERVAL` | `500ms` | Janitor tick interval |
| `--ses-email-ttl` | `SES_EMAIL_TTL` | `24h` | TTL for stored sent emails before they are evicted |
| `--ses-janitor-interval` | `SES_JANITOR_INTERVAL` | `1m` | Janitor tick interval |
| `--ssm-command-ttl` | `SSM_COMMAND_TTL` | `1h` | TTL for SendCommand results before they expire (matches AWS 1h default) |
| `--ssm-janitor-interval` | `SSM_JANITOR_INTERVAL` | `30s` | Janitor tick interval |
| `--sts-janitor-interval` | `STS_JANITOR_INTERVAL` | `30s` | Janitor tick interval |
| `--xray-janitor-interval` | `XRAY_JANITOR_INTERVAL` | `1m` | Janitor tick interval |
| `--xray-trace-ttl` | `XRAY_TRACE_TTL` | `30m` | TTL for stored traces before they are evicted |

## Lifecycle delays

Dwell time in transitional states before a resource settles; `0s` settles instantly.

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--lifecycle-accessanalyzer` | `ACCESSANALYZER_LIFECYCLE_DELAY` | `0s` | IAM Access Analyzer analyzer CREATING window |
| `--lifecycle-account` | `ACCOUNT_LIFECYCLE_DELAY` | `0s` | Account region ENABLING/DISABLING window (built-in 100ms) |
| `--lifecycle-acmpca` | `ACMPCA_AUDIT_REPORT_DELAY` | `0s` | ACM PCA audit report CREATING window |
| `--lifecycle-amp` | `AMP_LIFECYCLE_DELAY` | `0s` | Managed Service for Prometheus transitional window (built-in 500ms) |
| `--lifecycle-apprunner` | `APPRUNNER_OPERATION_DELAY` | `0s` | App Runner service OPERATION_IN_PROGRESS window after each operation |
| `--lifecycle-appstream` | `APPSTREAM_LIFECYCLE_DELAY` | `0s` | AppStream fleet and image builder STARTING/STOPPING/PENDING window |
| `--lifecycle-arczonalshift` | `ARCZONALSHIFT_PRACTICE_RUN_DURATION` | `0s` | ARC zonal shift practice run ACTIVE window; 0 keeps the built-in 30m; ignores the global delay |
| `--lifecycle-autoscaling` | `AUTOSCALING_LAUNCH_DELAY` | `0s` | Auto Scaling instance Pending window before InService |
| `--lifecycle-awsconfig` | `AWSCONFIG_LIFECYCLE_DELAY` | `0s` | AWS Config conformance pack CREATE_IN_PROGRESS window |
| `--lifecycle-backup` | `BACKUP_LIFECYCLE_DELAY` | `0s` | Backup restore and copy job PENDING/CREATED/RUNNING window before COMPLETED |
| `--lifecycle-bedrock` | `BEDROCK_JOB_COMPLETION_DELAY` | `0s` | Bedrock job InProgress window; 0 keeps the built-in window |
| `--lifecycle-bedrockagent` | `BEDROCKAGENT_LIFECYCLE_DELAY` | `0s` | Bedrock Agents CREATING/PREPARING/UPDATING/ingestion window |
| `--lifecycle-bedrockagentcore` | `BEDROCKAGENTCORE_LIFECYCLE_DELAY` | `0s` | Bedrock AgentCore control-plane CREATING/UPDATING/DELETING window |
| `--lifecycle-cleanrooms` | `CLEANROOMS_LIFECYCLE_DELAY` | `0s` | Clean Rooms protected query and job SUBMITTED window |
| `--lifecycle-cloudcontrol` | `CLOUDCONTROL_OPERATION_DELAY` | `0s` | Cloud Control Create/Update/DeleteResource IN_PROGRESS window |
| `--lifecycle-cloudfront` | `CLOUDFRONT_DEPLOY_DELAY` | `0s` | CloudFront distribution InProgress window; 0 keeps the built-in 100ms |
| `--lifecycle-cloudhsmv2` | `CLOUDHSMV2_LIFECYCLE_DELAY` | `0s` | CloudHSM v2 cluster, HSM and backup transitional window (built-in 500ms) |
| `--lifecycle-codeconnections` | `CODECONNECTIONS_LIFECYCLE_DELAY` | `0s` | CodeConnections connection handshake and host setup PENDING window |
| `--lifecycle-codegurureviewer` | `CODEGURUREVIEWER_LIFECYCLE_DELAY` | `0s` | CodeGuru Reviewer repository association and code review transitional window (built-in 500ms) |
| `--lifecycle-codestarconnections` | `CODESTARCONNECTIONS_LIFECYCLE_DELAY` | `0s` | CodeStar Connections connection handshake and host setup PENDING window; hosts stay PENDING when 0 |
| `--lifecycle-comprehend` | `COMPREHEND_LIFECYCLE_DELAY` | `0s` | Comprehend async job and classifier/recognizer training stage window |
| `--lifecycle-connect` | `CONNECT_LIFECYCLE_DELAY` | `0s` | Amazon Connect instance CREATION_IN_PROGRESS window (built-in 1s) |
| `--lifecycle-databrew` | `DATABREW_LIFECYCLE_DELAY` | `0s` | DataBrew job run STARTING-to-SUCCEEDED window (built-in 100ms) |
| `--lifecycle-dax` | `DAX_LIFECYCLE_DELAY` | `0s` | DAX cluster and node creating/modifying/deleting window (built-in 1s) |
| `--lifecycle-delay` | `GOPHERSTACK_LIFECYCLE_DELAY` | `0s` | Default dwell in transitional states for every service below; 0 settles instantly |
| `--lifecycle-detective` | `DETECTIVE_LIFECYCLE_DELAY` | `0s` | Detective investigation RUNNING window |
| `--lifecycle-directoryservice` | `DIRECTORYSERVICE_LIFECYCLE_DELAY` | `0s` | Directory Service transitional-state window; 0 keeps the built-in sub-second dwell |
| `--lifecycle-docdb` | `DOCDB_LIFECYCLE_DELAY` | `0s` | DocumentDB cluster and instance creating dwell |
| `--lifecycle-dsql` | `DSQL_LIFECYCLE_DELAY` | `0s` | Aurora DSQL cluster and stream transitional window (built-in 750ms/500ms) |
| `--lifecycle-ec2` | `EC2_LIFECYCLE_DELAY` | `0s` | EBS volume creating window before available |
| `--lifecycle-ecs-start` | `ECS_START_DELAY` | `0s` | ECS per-phase task start delay |
| `--lifecycle-ecs-stop` | `ECS_STOP_DELAY` | `0s` | ECS per-phase task stop delay |
| `--lifecycle-efs` | `EFS_ACTIVATION_DELAY` | `0s` | EFS file system creating window |
| `--lifecycle-eks` | `EKS_LIFECYCLE_DELAY` | `0s` | EKS transitional-state window (built-in 100ms) |
| `--lifecycle-elasticache` | `ELASTICACHE_LIFECYCLE_DELAY` | `0s` | ElastiCache creating dwell |
| `--lifecycle-elasticbeanstalk` | `ELASTICBEANSTALK_LIFECYCLE_DELAY` | `0s` | Elastic Beanstalk environment Launching/Updating/Terminating window |
| `--lifecycle-fis` | `FIS_LIFECYCLE_DELAY` | `0s` | FIS experiment state step delay (built-in 10ms) |
| `--lifecycle-forecast` | `FORECAST_LIFECYCLE_DELAY` | `0s` | Forecast resource CREATE_PENDING/CREATE_IN_PROGRESS window |
| `--lifecycle-fsx` | `FSX_LIFECYCLE_DELAY` | `0s` | FSx resource CREATING window |
| `--lifecycle-globalaccelerator` | `GLOBALACCELERATOR_LIFECYCLE_DELAY` | `0s` | Global Accelerator transitional window (built-in 500ms) |
| `--lifecycle-grafana` | `GRAFANA_LIFECYCLE_DELAY` | `0s` | Grafana workspace CREATING/UPDATING transitional window (built-in 100ms) |
| `--lifecycle-inspector2` | `INSPECTOR2_LIFECYCLE_DELAY` | `0s` | Inspector2 ENABLING/DISABLING window |
| `--lifecycle-iotanalytics` | `IOTANALYTICS_LIFECYCLE_DELAY` | `0s` | IoT Analytics channel/datastore/dataset CREATING and dataset content CREATING window |
| `--lifecycle-iotwireless` | `IOTWIRELESS_LIFECYCLE_DELAY` | `0s` | IoT Wireless FUOTA task FuotaSession_Waiting/In_FuotaSession stage window |
| `--lifecycle-kafka` | `KAFKA_LIFECYCLE_DELAY` | `0s` | MSK cluster creating and update window |
| `--lifecycle-kafkaconnect` | `KAFKACONNECT_LIFECYCLE_DELAY` | `0s` | MSK Connect connector/plugin transitional window (built-in 500ms create, 300ms delete) |
| `--lifecycle-kinesisanalyticsv2` | `KINESISANALYTICSV2_LIFECYCLE_DELAY` | `0s` | Managed Flink application STARTING/STOPPING/UPDATING window |
| `--lifecycle-lambda-activation` | `LAMBDA_ACTIVATION_DELAY` | `0s` | Lambda function Pending window |
| `--lifecycle-lambda-provisioned-concurrency` | `LAMBDA_PROVISIONED_CONCURRENCY_DELAY` | `0s` | Lambda provisioned concurrency IN_PROGRESS window |
| `--lifecycle-lambdaweb` | `LAMBDAWEB_LIFECYCLE_DELAY` | `0s` | Lambda Web Functions function, revision and endpoint transitional window (built-in 500ms-1s) |
| `--lifecycle-macie2` | `MACIE2_LIFECYCLE_DELAY` | `0s` | Macie one-time classification job RUNNING window (built-in 5s) |
| `--lifecycle-managedblockchain` | `MANAGEDBLOCKCHAIN_LIFECYCLE_DELAY` | `0s` | Managed Blockchain network/member/node CREATING window |
| `--lifecycle-medialive` | `MEDIALIVE_LIFECYCLE_DELAY` | `0s` | MediaLive channel/multiplex CREATING/STARTING/STOPPING/DELETING window |
| `--lifecycle-mediapackage` | `MEDIAPACKAGE_HARVEST_JOB_DELAY` | `0s` | MediaPackage harvest job IN_PROGRESS window; 0 keeps the built-in 1s |
| `--lifecycle-mediastore` | `MEDIASTORE_ACTIVATION_DELAY` | `0s` | MediaStore container CREATING/DELETING window |
| `--lifecycle-memorydb` | `MEMORYDB_LIFECYCLE_DELAY` | `0s` | MemoryDB cluster creating dwell |
| `--lifecycle-mgn` | `MGN_LIFECYCLE_DELAY` | `0s` | Application Migration Service replication/job/import per-hop window (built-in 100ms) |
| `--lifecycle-mwaa` | `MWAA_LIFECYCLE_DELAY` | `0s` | MWAA environment CREATING/UPDATING/DELETING window |
| `--lifecycle-neptune` | `NEPTUNE_LIFECYCLE_DELAY` | `0s` | Neptune cluster and instance creating dwell |
| `--lifecycle-networkfirewall` | `NETWORKFIREWALL_LIFECYCLE_DELAY` | `0s` | Network Firewall PROVISIONING/DELETING and sync window (built-in 500ms) |
| `--lifecycle-networkmanager` | `NETWORKMANAGER_LIFECYCLE_DELAY` | `0s` | Network Manager PENDING/DELETING window (built-in 100ms) |
| `--lifecycle-networkmonitor` | `NETWORKMONITOR_LIFECYCLE_DELAY` | `0s` | Network Monitor monitor and probe PENDING window |
| `--lifecycle-omics` | `OMICS_LIFECYCLE_DELAY` | `0s` | HealthOmics workflow/store/share/run and import/export/activation job per-hop window |
| `--lifecycle-opensearch-processing` | `OPENSEARCH_PROCESSING_DELAY` | `0s` | OpenSearch domain Processing window |
| `--lifecycle-opsworks` | `OPSWORKS_LIFECYCLE_DELAY` | `0s` | OpsWorks instance transitional window |
| `--lifecycle-organizations` | `ORGANIZATIONS_LIFECYCLE_DELAY` | `0s` | Organizations CreateAccount/CreateGovCloudAccount IN_PROGRESS window |
| `--lifecycle-outposts` | `OUTPOSTS_TRANSITION_DELAY` | `0s` | Outposts transitional window (built-in 100ms) |
| `--lifecycle-personalize` | `PERSONALIZE_LIFECYCLE_DELAY` | `0s` | Personalize resource CREATE PENDING/IN PROGRESS window |
| `--lifecycle-pinpoint` | `PINPOINT_LIFECYCLE_DELAY` | `0s` | Pinpoint export/import job CREATED/PROCESSING window before COMPLETED |
| `--lifecycle-polly` | `POLLY_LIFECYCLE_DELAY` | `0s` | Polly speech synthesis task stage window |
| `--lifecycle-quicksight` | `QUICKSIGHT_CREATION_DELAY` | `0s` | QuickSight dashboard, analysis and data source CREATION_IN_PROGRESS window |
| `--lifecycle-quicksight-ingestion` | `QUICKSIGHT_INGESTION_DELAY` | `0s` | QuickSight manual ingestion RUNNING window; 0 keeps the built-in 1s; ignores the global delay |
| `--lifecycle-redshift` | `REDSHIFT_ACTIVATION_DELAY` | `0s` | Redshift cluster creating window |
| `--lifecycle-redshiftdata` | `REDSHIFTDATA_LIFECYCLE_DELAY` | `0s` | Redshift Data statement SUBMITTED/STARTED window before FINISHED |
| `--lifecycle-redshiftserverless` | `REDSHIFTSERVERLESS_LIFECYCLE_DELAY` | `0s` | Redshift Serverless workgroup CREATING/MODIFYING/DELETING and namespace MODIFYING/DELETING window |
| `--lifecycle-rekognition` | `REKOGNITION_LIFECYCLE_DELAY` | `0s` | Rekognition stream processor, project version and dataset transitional window |
| `--lifecycle-resiliencehub` | `RESILIENCEHUB_LIFECYCLE_DELAY` | `0s` | Resilience Hub assessment/import/resolution per-hop window (built-in 100ms) |
| `--lifecycle-resourcegroups` | `RESOURCEGROUPS_LIFECYCLE_DELAY` | `0s` | Resource Groups transitional window |
| `--lifecycle-route53-recovery` | `ROUTE53_RECOVERY_LIFECYCLE_DELAY` | `0s` | Route 53 ARC control plane PENDING/PENDING_DELETION window |
| `--lifecycle-route53resolver` | `ROUTE53RESOLVER_LIFECYCLE_DELAY` | `0s` | Route 53 Resolver endpoint CREATING/UPDATING and rule association CREATING window |
| `--lifecycle-sagemaker` | `SAGEMAKER_LIFECYCLE_DELAY` | `0s` | SageMaker transitional-state dwell; 0 keeps the built-in dwell |
| `--lifecycle-serverlessrepo` | `SERVERLESSREPO_LIFECYCLE_DELAY` | `0s` | Serverless Application Repository template PREPARING window |
| `--lifecycle-servicequotas` | `SERVICEQUOTAS_LIFECYCLE_DELAY` | `0s` | Service Quotas increase request and utilization report stage window (built-in 500ms) |
| `--lifecycle-ssm-automation` | `SSM_AUTOMATION_EXEC_DELAY` | `0s` | SSM automation execution InProgress window |
| `--lifecycle-ssm-command` | `SSM_COMMAND_EXEC_DELAY` | `0s` | SSM SendCommand InProgress window |
| `--lifecycle-support` | `SUPPORT_CASE_PROGRESS_DELAY` | `0s` | Support case progress window |
| `--lifecycle-textract` | `TEXTRACT_ASYNC_JOB_DELAY` | `0s` | Textract async job IN_PROGRESS window; 0 keeps the built-in 200ms; ignores the global delay |
| `--lifecycle-timestreaminfluxdb` | `TIMESTREAMINFLUXDB_LIFECYCLE_DELAY` | `0s` | Timestream for InfluxDB transitional window (built-in 500ms) |
| `--lifecycle-transcribe` | `TRANSCRIBE_LIFECYCLE_DELAY` | `0s` | Transcribe job, vocabulary and language model IN_PROGRESS/PENDING window |
| `--lifecycle-translate` | `TRANSLATE_LIFECYCLE_DELAY` | `0s` | Translate batch job and parallel data stage window |
| `--lifecycle-vpclattice` | `VPCLATTICE_LIFECYCLE_DELAY` | `0s` | VPC Lattice service/target group/association CREATE_IN_PROGRESS and target INITIAL window |
| `--lifecycle-workmail` | `WORKMAIL_LIFECYCLE_DELAY` | `0s` | WorkMail transitional window |
| `--lifecycle-workspaces` | `WORKSPACES_LIFECYCLE_DELAY` | `0s` | WorkSpaces PENDING and directory REGISTERING window |

## Azure

Also documented in [azureblob](services/azureblob.md), [azurequeue](services/azurequeue.md), [azureservicebus](services/azureservicebus.md), [azuretable](services/azuretable.md).

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--azure-acr-port` | `AZURE_ACR_PORT` | `10011` | Fixed TCP port for the Container Registry data plane (Docker Registry v2, token and /acr/v1 APIs); startup fails if it is unavailable |
| `--azure-acr-remote-imports` | `AZURE_ACR_REMOTE_IMPORTS` | `false` | Let importImage copy from external registries (such as mcr.microsoft.com) over the network. Off by default: only registries emulated by this process can be imported from |
| `--azure-acr-tls-cert` | `AZURE_ACR_TLS_CERT` | — | Path to a PEM certificate; with --azure-acr-tls-key the registry listener serves HTTPS. Empty: plain HTTP (docker treats localhost registries as insecure) |
| `--azure-acr-tls-key` | `AZURE_ACR_TLS_KEY` | — | Path to the PEM key matching --azure-acr-tls-cert |
| `--azure-appconfig-auto-create` | `AZURE_APPCONFIG_AUTO_CREATE` | `true` | Create a store on first data-plane use when it was not created through ARM |
| `--azure-appconfig-port` | `AZURE_APPCONFIG_PORT` | `10008` | Fixed TCP port for the App Configuration HTTPS listener; startup fails if it is unavailable |
| `--azure-appconfig-tls-cert` | `AZURE_APPCONFIG_TLS_CERT` | — | Path to a PEM certificate for the App Configuration HTTPS listener. Empty: generate a self-signed certificate on every start. Requires --azure-appconfig-tls-key |
| `--azure-appconfig-tls-key` | `AZURE_APPCONFIG_TLS_KEY` | — | Path to the PEM key matching --azure-appconfig-tls-cert |
| `--azure-appconfig-validate-hmac` | `AZURE_APPCONFIG_VALIDATE_HMAC` | `false` | Cryptographically validate HMAC-SHA256 request signatures against the store's access keys (opt-in); by default any credential is accepted |
| `--azure-arm-aci-engine` | `AZURE_ARM_ACI_ENGINE` | `auto` | Container Instances engine: auto (use the container runtime) or none (groups fail to provision) |
| `--azure-arm-acr-port` | `AZURE_ARM_ACR_PORT` | `10011` | The services/azurecontainerregistry listener's port, used to build loginServer when --azure-arm-advertise-acr isn't set. Must match --azure-acr-port |
| `--azure-arm-advertise-acr` | `AZURE_ARM_ADVERTISE_ACR_ENDPOINT` | — | Override the host:port advertised in container registry loginServer (after the {registry}.azurecr. prefix). Default: derived from the request's Host and --azure-arm-acr-port |
| `--azure-arm-advertise-appconfig` | `AZURE_ARM_ADVERTISE_APPCONFIG_ENDPOINT` | — | Override the host:port advertised in configuration store endpoints (after the {store}. prefix). Default: derived from the request's Host and --azure-arm-appconfig-port |
| `--azure-arm-advertise-cosmosdb` | `AZURE_ARM_ADVERTISE_COSMOSDB_ENDPOINT` | — | Override the host:port advertised in documentEndpoint. Default: derived from the request's Host and the Cosmos DB listener's port |
| `--azure-arm-advertise-eventgrid` | `AZURE_ARM_ADVERTISE_EVENTGRID_ENDPOINT` | — | Override the host:port advertised in Event Grid topic endpoints and namespace hostnames. Default: derived from the request's Host and --azure-arm-eventgrid-port |
| `--azure-arm-advertise-eventhub` | `AZURE_ARM_ADVERTISE_EVENTHUB_ENDPOINT` | — | Override the host:port advertised as the AMQP endpoint in Event Hubs connection strings. Default: derived from the request's Host and --azure-arm-eventhub-amqp-port |
| `--azure-arm-advertise-graph` | `AZURE_ARM_ADVERTISE_GRAPH_ENDPOINT` | — | Override the host:port advertised as the Microsoft Graph endpoint and resource ID. Default: derived from the request's Host and --azure-arm-graph-port |
| `--azure-arm-advertise-keyvault` | `AZURE_ARM_ADVERTISE_KEYVAULT_ENDPOINT` | — | Override the host:port advertised in vaultUri (after the {vault}. prefix). Default: derived from the request's Host and --azure-arm-keyvault-port |
| `--azure-arm-advertise-search` | `AZURE_ARM_ADVERTISE_SEARCH_ENDPOINT` | — | Override the host:port advertised in search service endpoints (after the {service}. prefix). Default: derived from the request's Host |
| `--azure-arm-advertise-servicebus` | `AZURE_ARM_ADVERTISE_SERVICEBUS_ENDPOINT` | — | Override the host:port advertised as serviceBusEndpoint. Default: derived from the request's Host and --azure-arm-servicebus-port |
| `--azure-arm-advertise-storage-vhost` | `AZURE_ARM_ADVERTISE_STORAGE_VHOST` | — | Override the host:port advertised in storage account primaryEndpoints (before the {account}.{blob,queue,table}. prefix). Default: derived from the request's Host and --azure-arm-storage-vhost-port |
| `--azure-arm-aks-engine` | `AKS_ENGINE` | `stub` | Kubernetes Service engine: stub (ARM state only) or docker (a real single-node k3s cluster per managed cluster) |
| `--azure-arm-apim-allow-external` | `AZURE_ARM_APIM_ALLOW_EXTERNAL` | `false` | Let API Management gateway backends reach hosts outside loopback (opt-in; refused by default so the gateway cannot be used as an open proxy) |
| `--azure-arm-appconfig-port` | `AZURE_ARM_APPCONFIG_PORT` | `10008` | The services/azureappconfig HTTPS listener's port, used to build store endpoints when --azure-arm-advertise-appconfig isn't set. Must match --azure-appconfig-port |
| `--azure-arm-client-id` | `AZURE_ARM_CLIENT_ID` | `00000000-0000-0000-0000-000000000000` | Fixed dev AAD application (client) ID |
| `--azure-arm-client-secret` | `AZURE_ARM_CLIENT_SECRET` | `gopherstack` | Fixed dev client secret; any value is accepted unless --azure-arm-validate-tokens is set |
| `--azure-arm-db-engine` | `AZURE_ARM_DB_ENGINE` | `stub` | PostgreSQL and MySQL flexible server engine: stub (ARM state only) or docker (a real container per server) |
| `--azure-arm-enforce-rbac` | `AZURE_ARM_ENFORCE_RBAC` | `false` | Evaluate role assignments on ARM requests (opt-in): a request whose bearer token principal holds no role granting the derived action at or above the target scope answers 403 AuthorizationFailed. Implies bearer token validation |
| `--azure-arm-environment` | `AZURE_ARM_ENVIRONMENT` | `gopherstack` | Environment name advertised by the metadata document; must match the Terraform provider's environment setting |
| `--azure-arm-eventgrid-port` | `AZURE_ARM_EVENTGRID_PORT` | `10007` | The services/azureeventgrid HTTPS listener's port, used to build topic endpoints when --azure-arm-advertise-eventgrid isn't set. Must match --azure-eventgrid-port |
| `--azure-arm-eventhub-amqp-port` | `AZURE_ARM_EVENTHUB_AMQP_PORT` | `5673` | The services/azureeventhubs plain AMQP port, used to build connection strings when --azure-arm-advertise-eventhub isn't set. Must match --azure-eventhubs-amqp-port |
| `--azure-arm-eventhub-port` | `AZURE_ARM_EVENTHUB_PORT` | `10005` | The services/azureeventhubs HTTP listener's port, used for serviceBusEndpoint. Must match --azure-eventhubs-port |
| `--azure-arm-flex-dump-dir` | `AZURE_ARM_FLEX_DUMP_DIR` | — | Directory holding flexible server backup and replica seed dumps in the docker engine; empty uses a temporary directory removed on shutdown, or &lt;data-dir>/azure-flex-dumps with --persist |
| `--azure-arm-graph-port` | `AZURE_ARM_GRAPH_PORT` | `10014` | The services/azuregraph listener's port, advertised as the environment's Microsoft Graph endpoint when --azure-arm-advertise-graph isn't set. Must match --azure-graph-port |
| `--azure-arm-keyvault-port` | `AZURE_ARM_KEYVAULT_PORT` | `10004` | The services/azurekeyvault listener's port, used to build vaultUri when --azure-arm-advertise-keyvault isn't set. Must match --azure-keyvault-port |
| `--azure-arm-location` | `AZURE_ARM_LOCATION` | `local` | Default Azure location value used when a request doesn't specify one |
| `--azure-arm-logic-allow-external` | `AZURE_ARM_LOGIC_ALLOW_EXTERNAL` | `false` | Let Logic Apps HTTP actions reach hosts outside loopback (opt-in; refused by default so workflows cannot be used as an open proxy) |
| `--azure-arm-port` | `AZURE_ARM_PORT` | `10006` | Fixed TCP port for the dedicated ARM listener; startup fails if it's unavailable (no fallback pool) |
| `--azure-arm-rbac-superusers` | `AZURE_ARM_RBAC_SUPERUSERS` | — | Comma-separated principal object IDs exempt from --azure-arm-enforce-rbac, so a first role assignment can be made |
| `--azure-arm-servicebus-port` | `AZURE_ARM_SERVICEBUS_PORT` | `10003` | The services/azureservicebus listener's port, used to build serviceBusEndpoint when --azure-arm-advertise-servicebus isn't set. Must match --azure-servicebus-port |
| `--azure-arm-storage-vhost-port` | `AZURE_ARM_STORAGE_VHOST_PORT` | `10010` | The shared services/azurestoragevhost listener's port, used to build primaryEndpoints when --azure-arm-advertise-storage-vhost isn't set. Must match --azure-storage-vhost-port |
| `--azure-arm-subscription-id` | `AZURE_ARM_SUBSCRIPTION_ID` | `00000000-0000-0000-0000-000000000000` | Fixed dev Azure subscription ID |
| `--azure-arm-tenant-id` | `AZURE_ARM_TENANT_ID` | `00000000-0000-0000-0000-000000000000` | Fixed dev AAD tenant ID advertised by the metadata/token endpoints |
| `--azure-arm-tls-cert` | `AZURE_ARM_TLS_CERT` | — | Path to a PEM certificate for the ARM HTTPS listener. Empty (default): generate a self-signed certificate on every start. Requires --azure-arm-tls-key |
| `--azure-arm-tls-key` | `AZURE_ARM_TLS_KEY` | — | Path to the PEM key matching --azure-arm-tls-cert. Required when --azure-arm-tls-cert is set |
| `--azure-arm-validate-principals` | `AZURE_ARM_VALIDATE_PRINCIPALS` | `false` | Reject role assignments whose principalId is neither a Microsoft Graph directory object nor a user-assigned managed identity (PrincipalNotFound). Needs the Graph service; system-assigned resource identities are not recognised |
| `--azure-arm-validate-tokens` | `AZURE_ARM_VALIDATE_TOKENS` | `false` | Cryptographically validate ARM bearer tokens (opt-in); by default any token, or none, is accepted |
| `--azure-eventgrid-advertise` | `AZURE_EVENTGRID_ADVERTISE_URL` | — | Base URL (scheme://host:port) used in webhook validationUrl values. Default: https://localhost:&lt;port> |
| `--azure-eventgrid-port` | `AZURE_EVENTGRID_PORT` | `10007` | Fixed TCP port for the Event Grid HTTPS listener; startup fails if it is unavailable |
| `--azure-eventgrid-tls-cert` | `AZURE_EVENTGRID_TLS_CERT` | — | Path to a PEM certificate for the Event Grid HTTPS listener. Empty: generate a self-signed certificate on every start. Requires --azure-eventgrid-tls-key |
| `--azure-eventgrid-tls-key` | `AZURE_EVENTGRID_TLS_KEY` | — | Path to the PEM key matching --azure-eventgrid-tls-cert |
| `--azure-eventgrid-validate-sas` | `AZURE_EVENTGRID_VALIDATE_SAS` | `false` | Cryptographically validate aeg-sas-key / SAS tokens against topic and namespace keys (opt-in); by default any credential is accepted |
| `--azure-eventhubs-amqp-port` | `AZURE_EVENTHUBS_AMQP_PORT` | `5673` | Plain-text AMQP 1.0 listener port for Event Hubs SDKs (0 disables) |
| `--azure-eventhubs-amqp-tls-port` | `AZURE_EVENTHUBS_AMQP_TLS_PORT` | `5674` | AMQP 1.0 over TLS listener port, self-signed dev certificate (0 disables) |
| `--azure-eventhubs-kafka-port` | `AZURE_EVENTHUBS_KAFKA_PORT` | `9093` | Kafka protocol listener port (SASL PLAIN over TLS, self-signed dev certificate; 0 disables) |
| `--azure-eventhubs-port` | `AZURE_EVENTHUBS_PORT` | `10005` | Fixed TCP port for the Event Hubs HTTP (REST/Atom) listener; startup fails if it's unavailable |
| `--azure-eventhubs-validate-sas` | `AZURE_EVENTHUBS_VALIDATE_SAS` | `false` | Cryptographically validate Event Hubs SAS tokens against authorization-rule keys (opt-in) |
| `--azure-file-port` | `AZURE_FILE_PORT` | `10009` | Fixed TCP port for the dedicated Azure Files listener; startup fails if it's unavailable (no fallback pool) |
| `--azure-file-verify-auth` | `AZURE_FILE_VERIFY_AUTH` | `false` | Reject Shared Key and SAS requests for the development account whose signature is wrong or expired |
| `--azure-functions-container-host` | `AZURE_FUNCTIONS_CONTAINER_HOST` | — | Address function host containers use to reach the emulator's Storage, Service Bus and Event Hubs listeners; empty uses LAMBDA_DOCKER_HOST or the container runtime's default gateway |
| `--azure-functions-domain` | `AZURE_FUNCTIONS_DOMAIN` | `azurewebsites.localhost` | Hostname suffix: &lt;app>.&lt;domain> reaches the app's functions and &lt;app>.scm.&lt;domain> its Kudu API; an unknown host reaches the only app, if there is exactly one |
| `--azure-functions-engine` | `AZURE_FUNCTIONS_ENGINE` | `auto` | Function engine: auto (container runtime if reachable) or none (never start containers) |
| `--azure-functions-port` | `AZURE_FUNCTIONS_PORT` | `10012` | Fixed TCP port for the Azure Functions listener (function invocation and Kudu); startup fails if it is unavailable |
| `--azure-functions-startup-timeout` | `AZURE_FUNCTIONS_STARTUP_TIMEOUT` | `5m` | How long a function host container may take to pull its image and accept connections |
| `--azure-graph-default-domain` | `AZURE_GRAPH_DEFAULT_DOMAIN` | `gopherstack.onmicrosoft.com` | The tenant's default verified domain; user principal names must use a verified domain |
| `--azure-graph-dev-client-id` | `AZURE_GRAPH_DEV_CLIENT_ID` | `00000000-0000-0000-0000-000000000000` | Client ID of the built-in dev service principal that accepts any secret at the token endpoint; must match --azure-arm-client-id |
| `--azure-graph-extra-domains` | `AZURE_GRAPH_EXTRA_DOMAINS` | — | Comma-separated additional verified domains |
| `--azure-graph-port` | `AZURE_GRAPH_PORT` | `10014` | Fixed TCP port for the Microsoft Graph HTTPS listener; startup fails if it is unavailable |
| `--azure-graph-tenant-id` | `AZURE_GRAPH_TENANT_ID` | `00000000-0000-0000-0000-000000000000` | Tenant ID of the directory; must match --azure-arm-tenant-id |
| `--azure-graph-tls-cert` | `AZURE_GRAPH_TLS_CERT` | — | Path to a PEM certificate for the Microsoft Graph HTTPS listener. Empty: generate a self-signed certificate on every start. Requires --azure-graph-tls-key |
| `--azure-graph-tls-key` | `AZURE_GRAPH_TLS_KEY` | — | Path to the PEM key matching --azure-graph-tls-cert |
| `--azure-graph-validate-tokens` | `AZURE_GRAPH_VALIDATE_TOKENS` | `false` | Verify bearer token signature, expiry and audience against the emulator's Entra issuer (opt-in); by default any well-formed bearer token is accepted |
| `--azure-keyvault-auto-create-vaults` | `AZURE_KEYVAULT_AUTO_CREATE_VAULTS` | `true` | Create a vault on first data-plane use when it was not created through ARM |
| `--azure-keyvault-port` | `AZURE_KEYVAULT_PORT` | `10004` | Fixed TCP port for the Key Vault HTTPS listener; startup fails if it is unavailable |
| `--azure-keyvault-tls-cert` | `AZURE_KEYVAULT_TLS_CERT` | — | Path to a PEM certificate for the Key Vault HTTPS listener. Empty: generate a self-signed certificate on every start. Requires --azure-keyvault-tls-key |
| `--azure-keyvault-tls-key` | `AZURE_KEYVAULT_TLS_KEY` | — | Path to the PEM key matching --azure-keyvault-tls-cert |
| `--azure-keyvault-validate-tokens` | `AZURE_KEYVAULT_VALIDATE_TOKENS` | `false` | Require a well-formed, unexpired JWT bearer token (opt-in); by default any bearer token is accepted after the challenge |
| `--azure-monitor-advertise` | `AZURE_MONITOR_ADVERTISE_URL` | — | Base URL (scheme://host:port) advertised as the logs ingestion endpoint of data collection endpoints. Default: https://&lt;ARM request host>:&lt;port> |
| `--azure-monitor-port` | `AZURE_MONITOR_PORT` | `10013` | Fixed TCP port for the Azure Monitor Logs HTTPS listener; startup fails if it is unavailable |
| `--azure-monitor-strict-ingest` | `AZURE_MONITOR_STRICT_INGEST` | `false` | Reject an upload with 400 when a record mismatches the stream schema. Default: accept the upload and drop bad records silently, as Azure does asynchronously |
| `--azure-monitor-tls-cert` | `AZURE_MONITOR_TLS_CERT` | — | Path to a PEM certificate for the Azure Monitor Logs HTTPS listener. Empty: generate a self-signed certificate on every start. Requires --azure-monitor-tls-key |
| `--azure-monitor-tls-key` | `AZURE_MONITOR_TLS_KEY` | — | Path to the PEM key matching --azure-monitor-tls-cert |
| `--azure-monitor-validate-tokens` | `AZURE_MONITOR_VALIDATE_TOKENS` | `false` | Require a well-formed, unexpired JWT bearer token (opt-in); by default any bearer token is accepted |
| `--azure-redis-tls-cert` | `AZURE_REDIS_TLS_CERT` | — | Path to a PEM certificate for the caches' TLS endpoints. Empty: generate a self-signed certificate on every start. Requires --azure-redis-tls-key |
| `--azure-redis-tls-key` | `AZURE_REDIS_TLS_KEY` | — | Path to the PEM key matching --azure-redis-tls-cert |
| `--azure-search-port` | `AZURE_SEARCH_PORT` | `10015` | Fixed TCP port for the Azure AI Search HTTPS listener; startup fails if it is unavailable |
| `--azure-search-validate-tokens` | `AZURE_SEARCH_VALIDATE_TOKENS` | `false` | Validate Microsoft Entra bearer tokens and enforce Search role assignments on the data plane (opt-in); by default any bearer token is an administrator |
| `--azure-sql-engine` | `AZURE_SQL_ENGINE` | `auto` | Database engine: auto (SQL Server containers if a container runtime is reachable, else state-only) or none |
| `--azure-sql-image` | `AZURE_SQL_IMAGE` | `mcr.microsoft.com/mssql/server:2022-CU26-ubuntu-22.04` | SQL Server container image (linux/amd64 or arm64 per host) |
| `--azure-sql-startup-timeout` | `AZURE_SQL_STARTUP_TIMEOUT` | `5m` | How long a server container may take to pull its image and accept connections |
| `--azure-storage-vhost-port` | `AZURE_STORAGE_VHOST_PORT` | `10010` | Fixed TCP port for the shared Azure Storage virtual-hosted-style listener (Blob/Queue/Table, dispatched by Host header); startup fails if it's unavailable (no fallback pool) |

## GCP

Also documented in [Engines](engines.md).

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--gcp-apigateway-allow-external-backends` | `GCP_APIGATEWAY_ALLOW_EXTERNAL_BACKENDS` | `false` | Let gateway backends and remote JWKS URIs reach hosts outside loopback; by default the data plane only dials 127.0.0.0/8 and ::1 (Cloud Run and Functions *.localhost URLs included) |
| `--gcp-apigateway-port` | `GCP_APIGATEWAY_PORT` | `8146` | Fixed TCP port for the dedicated API Gateway listener (control plane and gateway data plane); startup fails if it is unavailable |
| `--gcp-artifacts-operation-delay` | `GCP_ARTIFACTREGISTRY_OPERATION_DELAY` | `0s` | How long long-running operations stay pending before they finish; 0 finishes them before the call returns |
| `--gcp-artifacts-port` | `GCP_ARTIFACTREGISTRY_PORT` | `8134` | Fixed TCP port for the dedicated Artifact Registry gRPC, REST and Docker Registry listener; startup fails if it is unavailable |
| `--gcp-artifacts-registry-host` | `GCP_ARTIFACTREGISTRY_REGISTRY_HOST` | `localhost` | Host (without the port) advertised in a repository's registry_uri and docker image URIs |
| `--gcp-artifacts-remote-upstreams` | `GCP_ARTIFACTREGISTRY_REMOTE_UPSTREAMS` | `false` | Let remote repositories fetch from their upstream (Docker Hub, Maven Central, npmjs, PyPI, custom URIs) over the network and cache the result. Off by default: an uncached remote read answers 502 |
| `--gcp-batch-engine` | `GCP_BATCH_ENGINE` | `none` | Task execution: none (jobs progress QUEUED, SCHEDULED, RUNNING, SUCCEEDED on the clock without running anything) or docker (run each task's runnables as containers) |
| `--gcp-batch-port` | `GCP_BATCH_PORT` | `8147` | Fixed TCP port for the dedicated Batch gRPC and REST listener; startup fails if it is unavailable |
| `--gcp-batch-script-image` | `GCP_BATCH_SCRIPT_IMAGE` | `debian:bookworm-slim` | Container image that executes script runnables when the docker engine is on (Batch runs them on the VM host) |
| `--gcp-bigquery-port` | `GCP_BIGQUERY_PORT` | `9050` | Fixed TCP port for the dedicated Google BigQuery listener; startup fails if it is unavailable |
| `--gcp-bigtable-port` | `GCP_BIGTABLE_PORT` | `8137` | Fixed TCP port for the dedicated Google Cloud Bigtable gRPC and REST listener (BIGTABLE_EMULATOR_HOST); startup fails if it is unavailable |
| `--gcp-cloudbuild-engine` | `GCP_CLOUDBUILD_ENGINE` | `auto` | Build execution: auto (run each build step as a container when a runtime is reachable) or none (builds fail with a status detail) |
| `--gcp-cloudbuild-port` | `GCP_CLOUDBUILD_PORT` | `8136` | Fixed TCP port for the dedicated Cloud Build gRPC and REST listener; startup fails if it is unavailable |
| `--gcp-cloudtasks-appengine-url` | `GCP_CLOUDTASKS_APPENGINE_URL` | — | Base URL that AppEngineHttpRequest tasks are dispatched to; when empty they fail with FAILED_PRECONDITION |
| `--gcp-cloudtasks-port` | `GCP_CLOUDTASKS_PORT` | `8123` | Fixed TCP port for the dedicated Google Cloud Tasks gRPC listener; startup fails if it is unavailable |
| `--gcp-compute-port` | `GCP_COMPUTE_PORT` | `8142` | Fixed TCP port for the dedicated Compute Engine REST listener; startup fails if it is unavailable |
| `--gcp-container-port` | `GCP_CONTAINER_PORT` | `8144` | Fixed TCP port for the dedicated Kubernetes Engine gRPC and REST listener; startup fails if it is unavailable |
| `--gcp-datastore-enforce-indexes` | `GCP_DATASTORE_ENFORCE_INDEXES` | `true` | Fail queries that need a composite index with FAILED_PRECONDITION unless a matching index was created through the admin API; false accepts every query |
| `--gcp-datastore-port` | `GCP_DATASTORE_PORT` | `8143` | Fixed TCP port for the dedicated Google Cloud Datastore gRPC and REST listener (DATASTORE_EMULATOR_HOST); startup fails if it is unavailable |
| `--gcp-dns-dns-port` | `GCP_DNS_SERVER_PORT` | `8141` | Fixed UDP and TCP port of the authoritative DNS server that answers for public zones; startup fails if it is unavailable |
| `--gcp-dns-port` | `GCP_DNS_PORT` | `8140` | Fixed TCP port for the dedicated Cloud DNS REST listener; startup fails if it is unavailable |
| `--gcp-enforce-projects` | `GCP_ENFORCE_PROJECTS` | `false` | Reject GCP requests whose project does not exist or is DELETE_REQUESTED (needs the gcpresourcemanager service). Default off: any project id is accepted |
| `--gcp-enforce-service-enablement` | `GCP_ENFORCE_SERVICE_ENABLEMENT` | `false` | Reject GCP requests to a service whose API is not enabled for the project (SERVICE_DISABLED); implies the project check (needs the gcpresourcemanager service). Default off |
| `--gcp-eventarc-port` | `GCP_EVENTARC_PORT` | `8138` | Fixed TCP port for the dedicated Google Cloud Eventarc gRPC and REST listener; startup fails if it is unavailable |
| `--gcp-firestore-port` | `GCP_FIRESTORE_PORT` | `8080` | Fixed TCP port for the dedicated Google Cloud Firestore gRPC listener (FIRESTORE_EMULATOR_HOST); startup fails if it is unavailable |
| `--gcp-functions-engine` | `GCP_FUNCTIONS_ENGINE` | `auto` | Function engine: auto (container runtime if reachable) or none (never start containers) |
| `--gcp-functions-ingress-port` | `GCP_FUNCTIONS_INGRESS_PORT` | `8129` | Fixed TCP port of the HTTP ingress that routes &lt;function>-&lt;hash>-&lt;region>.localhost hosts to functions and serves source upload/download URLs; startup fails if taken |
| `--gcp-functions-port` | `GCP_FUNCTIONS_PORT` | `8128` | Fixed TCP port for the dedicated Google Cloud Functions gRPC listener; startup fails if it is unavailable |
| `--gcp-functions-startup-timeout` | `GCP_FUNCTIONS_STARTUP_TIMEOUT` | `5m` | How long a function container may take to install dependencies and accept connections |
| `--gcp-iam-port` | `GCP_IAM_PORT` | `8125` | Fixed TCP port for the dedicated Google Cloud IAM admin gRPC listener; startup fails if it is unavailable |
| `--gcp-identitytoolkit-allow-external-blocking-functions` | `GCP_IDENTITYTOOLKIT_ALLOW_EXTERNAL_BLOCKING_FUNCTIONS` | `false` | Let blocking functions reach hosts outside loopback (opt-in; refused by default so a project config cannot be used as an open proxy) |
| `--gcp-identitytoolkit-port` | `GCP_IDENTITYTOOLKIT_PORT` | `8145` | Fixed TCP port for the dedicated Identity Platform / Firebase Auth listener (FIREBASE_AUTH_EMULATOR_HOST); startup fails if it is unavailable |
| `--gcp-identitytoolkit-project` | `GCP_IDENTITYTOOLKIT_PROJECT` | `gopherstack-local` | Project that client calls (API-key requests without a /projects/ path) resolve to |
| `--gcp-kms-port` | `GCP_KMS_PORT` | `8087` | Fixed TCP port for the dedicated Google Cloud KMS gRPC listener; startup fails if it is unavailable |
| `--gcp-logging-port` | `GCP_LOGGING_PORT` | `8131` | Fixed TCP port for the dedicated Google Cloud Logging gRPC and REST listener; startup fails if it is unavailable |
| `--gcp-monitoring-port` | `GCP_MONITORING_PORT` | `8132` | Fixed TCP port for the dedicated Google Cloud Monitoring gRPC and REST listener; startup fails if it is unavailable |
| `--gcp-pubsub-port` | `GCP_PUBSUB_PORT` | `8085` | Fixed TCP port for the dedicated Google Cloud Pub/Sub gRPC listener; startup fails if it is unavailable |
| `--gcp-redis-port` | `GCP_REDIS_PORT` | `8133` | Fixed TCP port for the dedicated Google Memorystore for Redis gRPC listener; startup fails if it is unavailable |
| `--gcp-resourcemanager-delete-retention` | `GCP_RESOURCEMANAGER_DELETE_RETENTION` | `720h` | How long a DELETE_REQUESTED project or folder can be undeleted before it is purged (GCP: about 30 days). 0 purges at once and frees the id |
| `--gcp-resourcemanager-domain` | `GCP_RESOURCEMANAGER_DOMAIN` | `gopherstack.local` | Display name (domain) of the emulated organization |
| `--gcp-resourcemanager-organization-id` | `GCP_RESOURCEMANAGER_ORGANIZATION_ID` | `123456789012` | Numeric id of the one emulated organization (organizations/ID) |
| `--gcp-resourcemanager-port` | `GCP_RESOURCEMANAGER_PORT` | `8148` | Fixed TCP port for the dedicated Resource Manager and Service Usage gRPC and REST listener; startup fails if it is unavailable |
| `--gcp-resourcemanager-seed-projects` | `GCP_RESOURCEMANAGER_SEED_PROJECTS` | `gopherstack-test` | Comma-separated project ids created at startup, with no parent, so they can be read and used for IAM and Service Usage |
| `--gcp-run-engine` | `GCP_RUN_ENGINE` | `auto` | Workload engine: auto (container runtime if reachable) or none (never start containers) |
| `--gcp-run-ingress-port` | `GCP_RUN_INGRESS_PORT` | `8127` | Fixed TCP port of the HTTP ingress that routes &lt;service>-&lt;hash>-&lt;region>.localhost hosts to deployed services; startup fails if taken |
| `--gcp-run-port` | `GCP_RUN_PORT` | `8126` | Fixed TCP port for the dedicated Google Cloud Run gRPC listener; startup fails if it is unavailable |
| `--gcp-run-startup-timeout` | `GCP_RUN_STARTUP_TIMEOUT` | `4m` | How long a new revision may take to accept connections before it is marked failed |
| `--gcp-scheduler-appengine-url` | `GCP_SCHEDULER_APPENGINE_URL` | — | Base URL that AppEngineHttpTarget jobs are dispatched to; when empty they fail with FAILED_PRECONDITION |
| `--gcp-scheduler-port` | `GCP_SCHEDULER_PORT` | `8124` | Fixed TCP port for the dedicated Google Cloud Scheduler gRPC listener; startup fails if it is unavailable |
| `--gcp-secretmanager-port` | `GCP_SECRETMANAGER_PORT` | `8086` | Fixed TCP port for the dedicated Google Secret Manager gRPC listener; startup fails if it is unavailable |
| `--gcp-spanner-engine` | `GCP_SPANNER_ENGINE` | `auto` | Data plane: auto (run the Spanner emulator container if a runtime is reachable, else metadata only) or none |
| `--gcp-spanner-image` | `GCP_SPANNER_IMAGE` | `gcr.io/cloud-spanner-emulator/emulator:1.5.40` | Cloud Spanner emulator image serving databases, sessions and queries |
| `--gcp-spanner-port` | `GCP_SPANNER_PORT` | `8135` | Fixed TCP port for the dedicated Cloud Spanner gRPC and REST listener; startup fails if it is unavailable |
| `--gcp-spanner-startup-timeout` | `GCP_SPANNER_STARTUP_TIMEOUT` | `5m` | How long the emulator container may take to pull its image and accept calls |
| `--gcp-sql-engine` | `GCP_SQL_ENGINE` | `auto` | Database engine: auto (container runtime if reachable, else state-only) or none (never start containers) |
| `--gcp-sql-port` | `GCP_SQL_PORT` | `8130` | Fixed TCP port for the dedicated Cloud SQL Admin listener; startup fails if it is unavailable |
| `--gcp-sql-startup-timeout` | `GCP_SQL_STARTUP_TIMEOUT` | `5m` | How long a database container may take to pull its image and accept connections |
| `--gcp-storage-port` | `GCP_STORAGE_PORT` | `4443` | Fixed TCP port for the dedicated Google Cloud Storage listener; startup fails if it is unavailable |
| `--gcp-storage-require-auth` | `GCP_STORAGE_REQUIRE_AUTH` | `false` | Reject Cloud Storage requests that are neither signed (V2/V4 signed URL) nor carry an Authorization header. Signed requests are always verified |
| `--gcp-workflows-port` | `GCP_WORKFLOWS_PORT` | `8139` | Fixed TCP port for the dedicated Google Cloud Workflows gRPC and REST listener; startup fails if it is unavailable |

## OCI

Also documented in [Engines](engines.md).

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--oci-apigateway-allow-external-backends` | `OCI_APIGATEWAY_ALLOW_EXTERNAL_BACKENDS` | `false` | Let HTTP backends and remote JWKS URIs reach hosts outside loopback; by default the data plane only dials 127.0.0.0/8 and ::1 |
| `--oci-apigateway-enforce-auth` | `OCI_APIGATEWAY_ENFORCE_AUTH` | `false` | Reject unsigned control-plane requests and signatures from API keys not registered with the emulator; off by default so any signing config works |
| `--oci-apigateway-port` | `OCI_APIGATEWAY_PORT` | `8163` | Fixed TCP port for the dedicated OCI API Gateway listener (control plane and data plane); startup fails if it is unavailable |
| `--oci-apigateway-tls-port` | `OCI_APIGATEWAY_TLS_PORT` | `0` | Extra TCP port that serves only the TLS data plane (set 443 to reach gateways through the real https://&lt;hostname> form); 0 disables it |
| `--oci-artifacts-enforce-auth` | `OCI_ARTIFACTS_ENFORCE_AUTH` | `false` | Reject unsigned control-plane requests and signatures from API keys not registered with the emulator; off by default so any signing config works. Registry logins always need a namespace/user name and a non-empty auth token |
| `--oci-artifacts-port` | `OCI_ARTIFACTS_PORT` | `8166` | Fixed TCP port for the dedicated OCI Artifacts and Container Registry listener (control plane, generic artifact content and Docker Registry v2); startup fails if it is unavailable |
| `--oci-container-instances-enforce-auth` | `OCI_CONTAINER_INSTANCES_ENFORCE_AUTH` | `false` | Reject unsigned control-plane requests and signatures from API keys not registered with the emulator; off by default so any signing config works |
| `--oci-container-instances-engine` | `OCI_CONTAINER_INSTANCES_ENGINE` | `state` | Container Instances engine: state (lifecycle only, no containers run) or docker (run the containers in the container runtime) |
| `--oci-container-instances-port` | `OCI_CONTAINER_INSTANCES_PORT` | `8167` | Fixed TCP port for the dedicated OCI Container Instances listener; startup fails if it is unavailable |
| `--oci-core-enforce-auth` | `OCI_CORE_ENFORCE_AUTH` | `false` | Reject unsigned requests and signatures from API keys not registered with the emulator; off by default so any signing config works |
| `--oci-core-port` | `OCI_CORE_PORT` | `8155` | Fixed TCP port for the dedicated OCI Core Services (networking) listener; startup fails if it is unavailable |
| `--oci-database-enforce-auth` | `OCI_DATABASE_ENFORCE_AUTH` | `false` | Reject unsigned control-plane requests and signatures from API keys not registered with the emulator; off by default so any signing config works |
| `--oci-database-port` | `OCI_DATABASE_PORT` | `8170` | Fixed TCP port for the dedicated OCI Database listener; startup fails if it is unavailable |
| `--oci-dns-enforce-auth` | `OCI_DNS_ENFORCE_AUTH` | `false` | Reject unsigned control-plane requests and signatures from API keys not registered with the emulator; off by default so any signing config works |
| `--oci-dns-port` | `OCI_DNS_PORT` | `8168` | Fixed TCP port for the dedicated OCI DNS control-plane listener; startup fails if it is unavailable |
| `--oci-dns-port-dns` | `OCI_DNS_PORT_DNS` | `8169` | Fixed UDP and TCP port of the authoritative DNS server that answers for GLOBAL zones; 0 disables the data plane; startup fails if the port is unavailable |
| `--oci-events-enforce-auth` | `OCI_EVENTS_ENFORCE_AUTH` | `false` | Reject unsigned control-plane requests and signatures from API keys not registered with the emulator; off by default so any signing config works |
| `--oci-events-port` | `OCI_EVENTS_PORT` | `8164` | Fixed TCP port for the dedicated OCI Events listener; startup fails if it is unavailable |
| `--oci-filestorage-data-dir` | `OCI_FILESTORAGE_DATA_DIR` | — | Directory holding file system data in nfs mode; empty uses a temporary directory removed on shutdown |
| `--oci-filestorage-enforce-auth` | `OCI_FILESTORAGE_ENFORCE_AUTH` | `false` | Reject unsigned control-plane requests and signatures from API keys not registered with the emulator; off by default so any signing config works |
| `--oci-filestorage-engine` | `OCI_FILESTORAGE_ENGINE` | `stub` | OCI File Storage engine: stub (control plane only) or nfs (each mount target serves its exports over NFSv3 on a loopback port, backed by a directory per file system) |
| `--oci-filestorage-port` | `OCI_FILESTORAGE_PORT` | `8171` | Fixed TCP port for the dedicated OCI File Storage control-plane listener; startup fails if it is unavailable |
| `--oci-functions-concurrency-mb` | `OCI_FUNCTIONS_CONCURRENCY_MB` | `61440` | Service limit total-concurrency-mb: memory of all concurrently running invocations; beyond it calls answer 429 |
| `--oci-functions-disable-engine` | `OCI_FUNCTIONS_DISABLE_ENGINE` | `false` | Never start containers: invocations fail with 503 (management APIs keep working) |
| `--oci-functions-enforce-auth` | `OCI_FUNCTIONS_ENFORCE_AUTH` | `false` | Reject unsigned requests and signatures from API keys not registered with the emulator; off by default so any signing config works |
| `--oci-functions-idle-timeout` | `OCI_FUNCTIONS_IDLE_TIMEOUT` | `10m` | How long an idle warm function container is kept before it is removed |
| `--oci-functions-max-applications` | `OCI_FUNCTIONS_MAX_APPLICATIONS` | `20` | Service limit application-count: applications that may exist at once |
| `--oci-functions-max-containers` | `OCI_FUNCTIONS_MAX_CONTAINERS` | `32` | Maximum number of live function containers (busy plus warm); further concurrent invocations are throttled with 429 |
| `--oci-functions-max-functions` | `OCI_FUNCTIONS_MAX_FUNCTIONS` | `500` | Service limit function-count: functions that may exist at once |
| `--oci-functions-port` | `OCI_FUNCTIONS_PORT` | `8154` | Fixed TCP port for the dedicated OCI Functions (management and invoke) listener; startup fails if it is unavailable |
| `--oci-functions-provisioned-mb` | `OCI_FUNCTIONS_PROVISIONED_MB` | `40960` | Service limit provisioned-concurrency-mb: memory reserved by provisioned concurrency across functions |
| `--oci-functions-startup-timeout` | `OCI_FUNCTIONS_STARTUP_TIMEOUT` | `5m` | How long a function container may take to pull its image and open its FDK socket |
| `--oci-identity-enforce-auth` | `OCI_IDENTITY_ENFORCE_AUTH` | `false` | Reject unsigned requests and signatures from API keys not registered with the emulator; off by default so any signing config works |
| `--oci-identity-port` | `OCI_IDENTITY_PORT` | `8153` | Fixed TCP port for the dedicated OCI Identity (IAM) listener; startup fails if it is unavailable |
| `--oci-loadbalancer-allow-external-backends` | `OCI_LOADBALANCER_ALLOW_EXTERNAL_BACKENDS` | `false` | Let load balancer listeners forward to, and health-check, backends outside loopback; by default the data plane only dials 127.0.0.0/8 and ::1 |
| `--oci-loadbalancer-enforce-auth` | `OCI_LOADBALANCER_ENFORCE_AUTH` | `false` | Reject unsigned control-plane requests and signatures from API keys not registered with the emulator; off by default so any signing config works |
| `--oci-loadbalancer-port` | `OCI_LOADBALANCER_PORT` | `8165` | Fixed TCP port for the dedicated OCI Load Balancer control-plane listener; startup fails if it is unavailable |
| `--oci-logging-enforce-auth` | `OCI_LOGGING_ENFORCE_AUTH` | `false` | Reject unsigned requests and signatures from API keys not registered with the emulator; off by default so any signing config works |
| `--oci-logging-port` | `OCI_LOGGING_PORT` | `8160` | Fixed TCP port for the dedicated OCI Logging (control plane, ingestion and search) listener; startup fails if it is unavailable |
| `--oci-monitoring-enforce-auth` | `OCI_MONITORING_ENFORCE_AUTH` | `false` | Reject unsigned requests and signatures from API keys not registered with the emulator; off by default so any signing config works |
| `--oci-monitoring-port` | `OCI_MONITORING_PORT` | `8161` | Fixed TCP port for the dedicated OCI Monitoring (alarms, metric ingestion and metric queries) listener; startup fails if it is unavailable |
| `--oci-mysql-enforce-auth` | `OCI_MYSQL_ENFORCE_AUTH` | `false` | Reject unsigned requests and signatures from API keys not registered with the emulator; off by default so any signing config works |
| `--oci-mysql-max-manual-backups` | `OCI_MYSQL_MAX_MANUAL_BACKUPS` | `1200` | Service limit: manual backups that may exist at once |
| `--oci-mysql-max-parallel-copies` | `OCI_MYSQL_MAX_PARALLEL_COPIES` | `5` | Service limit: cross-region backup copies in progress at once |
| `--oci-mysql-max-shape-instances` | `OCI_MYSQL_MAX_SHAPE_INSTANCES` | `100` | Service limit: DB systems of one shape per availability domain (a tenth of it for MySQL.256) |
| `--oci-mysql-max-storage-gb` | `OCI_MYSQL_MAX_STORAGE_GB` | `100000` | Service limit: data storage in GB per availability domain |
| `--oci-mysql-port` | `OCI_MYSQL_PORT` | `8159` | Fixed TCP port for the dedicated OCI MySQL HeatWave Database Service listener; startup fails if it is unavailable |
| `--oci-nosql-enforce-auth` | `OCI_NOSQL_ENFORCE_AUTH` | `false` | Reject unsigned requests and signatures from API keys not registered with the emulator; off by default so any signing config works |
| `--oci-nosql-enforce-throughput` | `OCI_NOSQL_ENFORCE_THROUGHPUT` | `false` | Reject row and query operations with 429 TooManyRequests once a PROVISIONED table's read or write units for the current second are spent; off by default so bulk loads never fail |
| `--oci-nosql-port` | `OCI_NOSQL_PORT` | `8162` | Fixed TCP port for the dedicated OCI NoSQL Database listener; startup fails if it is unavailable |
| `--oci-notifications-enforce-auth` | `OCI_NOTIFICATIONS_ENFORCE_AUTH` | `false` | Reject unsigned requests and signatures from API keys not registered with the emulator; off by default so any signing config works |
| `--oci-notifications-port` | `OCI_NOTIFICATIONS_PORT` | `8157` | Fixed TCP port for the dedicated OCI Notifications (topics, subscriptions and publish) listener; startup fails if it is unavailable |
| `--oci-objectstorage-enforce-auth` | `OCI_OBJECTSTORAGE_ENFORCE_AUTH` | `false` | Reject unsigned requests and signatures from API keys not registered with the emulator; off by default so any signing config works |
| `--oci-objectstorage-namespace` | `OCI_OBJECTSTORAGE_NAMESPACE` | `gopherstack` | Object Storage namespace the emulator serves; other namespaces return NamespaceNotFound |
| `--oci-objectstorage-port` | `OCI_OBJECTSTORAGE_PORT` | `8150` | Fixed TCP port for the dedicated OCI Object Storage listener; startup fails if it is unavailable |
| `--oci-oke-enforce-auth` | `OCI_CONTAINER_ENGINE_ENFORCE_AUTH` | `false` | Reject unsigned requests and signatures from API keys not registered with the emulator; off by default so any signing config works |
| `--oci-oke-port` | `OCI_CONTAINER_ENGINE_PORT` | `8158` | Fixed TCP port for the dedicated OCI Container Engine for Kubernetes (OKE) listener; startup fails if it is unavailable |
| `--oci-queue-enforce-auth` | `OCI_QUEUE_ENFORCE_AUTH` | `false` | Reject unsigned requests and signatures from API keys not registered with the emulator; off by default so any signing config works |
| `--oci-queue-enforce-rate-limits` | `OCI_QUEUE_ENFORCE_RATE_LIMITS` | `false` | Answer 429 TooManyRequests once a queue spends its documented 1,000 requests per second per API or 10 MB per second of ingress or egress; off by default so bulk loads never fail |
| `--oci-queue-port` | `OCI_QUEUE_PORT` | `8156` | Fixed TCP port for the dedicated OCI Queue (queue admin and messages) listener; startup fails if it is unavailable |
| `--oci-streaming-enforce-auth` | `OCI_STREAMING_ENFORCE_AUTH` | `false` | Reject unsigned requests and signatures from API keys not registered with the emulator; off by default so any signing config works |
| `--oci-streaming-kafka-port` | `OCI_STREAMING_KAFKA_PORT` | `0` | TCP port of the Kafka-compatible listener (SASL PLAIN over TLS with a self-signed dev certificate, user tenancy/domain/user/streamPoolOCID, password an OCI Identity auth token; 0 disables) |
| `--oci-streaming-partition-limit` | `OCI_STREAMING_PARTITION_LIMIT` | `200` | Partitions allowed across all streams (Oracle default 200 on Monthly Universal Credits, 50 on Pay-as-You-Go); 0 disables the limit |
| `--oci-streaming-port` | `OCI_STREAMING_PORT` | `8152` | Fixed TCP port for the dedicated OCI Streaming (stream admin and messages) listener; startup fails if it is unavailable |
| `--oci-vault-enforce-auth` | `OCI_VAULT_ENFORCE_AUTH` | `false` | Reject unsigned requests and signatures from API keys not registered with the emulator; off by default so any signing config works |
| `--oci-vault-port` | `OCI_VAULT_PORT` | `8151` | Fixed TCP port for the dedicated OCI Vault (KMS, Vault secrets, Secret retrieval) listener; startup fails if it is unavailable |

## Per-service settings

Settings already tabulated on [cloudformation](services/cloudformation.md), [cosmosdb](services/cosmosdb.md), [ec2](services/ec2.md), [kinesis](services/kinesis.md), [lambda](services/lambda.md), [opensearch](services/opensearch.md), [sqs](services/sqs.md), [stepfunctions](services/stepfunctions.md), [transcribe](services/transcribe.md) are not repeated here.

### agentcore

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--agentcore-registry-host` | `AGENTCORE_REGISTRY_HOST` | — | host:port that replaces the ECR registry host when pulling agent runtime images (for example the emulated ECR registry) |
| `--agentcore-python-image` | `AGENTCORE_PYTHON_IMAGE` | — | Code interpreter sandbox image; empty uses python:3.12-slim |
| `--agentcore-gateway-extra-ca-file` | `AGENTCORE_GATEWAY_EXTRA_CA_FILE` | — | PEM file of extra CA certificates trusted when gateway targets, JWKS and OAuth endpoints use TLS (certificates are always verified) |
| `--agentcore-allow-external-gateway-targets` | `AGENTCORE_ALLOW_EXTERNAL_GATEWAY_TARGETS` | `false` | Let AgentCore gateway targets (OpenAPI, MCP server) and JWT discovery reach hosts outside loopback (opt-in; refused by default so a gateway cannot be used as an open proxy) |
| `--agentcore-idle-timeout` | `AGENTCORE_IDLE_TIMEOUT` | `15m` | Idle time after which a warm agent runtime session is stopped when the runtime sets no lifecycle timeout |

### amp

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--amp-ingestion-rate` | `AMP_INGESTION_RATE` | `1666666` | Samples per second one workspace may ingest; also the token bucket refill rate |
| `--amp-ingestion-burst` | `AMP_INGESTION_BURST` | — | Token bucket size in samples; 0 means one second of the ingestion rate |
| `--amp-active-series` | `AMP_ACTIVE_SERIES` | `50000000` | Active series (a sample in the last 2 hours) one workspace may hold |
| `--amp-enforce-ingestion-quotas` | `AMP_ENFORCE_INGESTION_QUOTAS` | `false` | Reject remote write beyond the ingestion rate (429) and active series (400) quotas; off by default so bulk loads never fail |

### batch

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--batch-execute-jobs` | `BATCH_EXECUTE_JOBS` | `false` | Run submitted container jobs in Docker instead of simulating completion |

### ce

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--ce-resource-level` | `GOPHERSTACK_CE_RESOURCE_LEVEL` | `false` | Keep resource ids and tags on metered Cost Explorer rows for 14 days (resource-level data). Default off |

### cloudwatchlogs

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--cloudwatchlogs-max-retention-days` | `CLOUDWATCHLOGS_MAX_RETENTION_DAYS` | `14` | Global maximum log retention in days for groups without explicit policy |

### dynamodb

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--dynamodb-default-region` | `DYNAMODB_REGION` | `us-east-1` | Default region for DynamoDB |
| `--dynamodb-ttl-sweep-batch-size` | `DYNAMODB_TTL_SWEEP_BATCH_SIZE` | `1000` | Maximum items checked per TTL sweep lock acquisition |
| `--dynamodb-create-delay` | `DYNAMODB_CREATE_DELAY` | `0s` | Simulated CREATING→ACTIVE delay. 0 disables lifecycle |
| `--dynamodb-enforce-throughput` | `DYNAMODB_ENFORCE_THROUGHPUT` | `false` | Enforce provisioned throughput limits (token bucket per table) |

### ecr

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--ecr-endpoint-strategy` | `ECR_ENDPOINT_STRATEGY` | — | ECR repositoryUri host format: domain or off (empty keeps the AWS-style host) |

### elbv2

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--elbv2-listeners` | `ELBV2_LISTENERS` | `routed` | ELBv2 data plane: routed (path/host routing on the emulator port) or real (a local socket per listener, TLS termination for HTTPS/TLS) |
| `--elbv2-listener-host` | `ELBV2_LISTENER_HOST` | `127.0.0.1` | Bind address for real ELBv2 listeners. Requested ports below 1024 or already taken map to a port from the shared pool; see GET /_gopherstack/elbv2/_listeners |

### es

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--es-processing-delay` | `ELASTICSEARCH_PROCESSING_DELAY` | `0s` | Elasticsearch domain Processing window; 0 settles instantly |

### eventbridge

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--eventbridge-allow-schedule-on-custom-bus` | `EVENTBRIDGE_ALLOW_SCHEDULE_ON_CUSTOM_BUS` | `false` | Accept scheduled rules on custom event buses (real AWS allows them only on the default bus) |

### graphql

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--graphql-endpoint-strategy` | `GRAPHQL_ENDPOINT_STRATEGY` | — | AppSync GraphQL URL format: legacy, domain or path (empty keeps /v1/apis/ID/graphql) |

### iot

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--iot-mqtt-port` | `IOT_MQTT_PORT` | `1883` | TCP port for the embedded IoT MQTT broker; 0 picks a free port |
| `--iot-mqtt-auth` | `IOT_MQTT_AUTH` | `false` | Require X.509 client certificates on the IoT MQTT broker (TLS) and authorise CONNECT/PUBLISH/SUBSCRIBE/RECEIVE with attached IoT policies (opt-in) |

### lambdaweb

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--lambdaweb-runtime-image` | `LAMBDAWEB_RUNTIME_IMAGE` | `docker.io/library/node:24-slim` | Container image that runs nodejs24.x web function revisions (needs node on PATH) |

### s3

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--s3-default-region` | `S3_REGION` | `us-east-1` | Default region for S3 |
| `--s3-skip-kms-key-validation` | `GOPHERSTACK_S3_SKIP_KMS_KEY_VALIDATION`, `S3_SKIP_KMS_KEY_VALIDATION` | `false` | Accept SSE-KMS key ids that do not exist in KMS instead of failing with KMS.NotFoundException (LocalStack S3_SKIP_KMS_KEY_VALIDATION) |
| `--s3-validate-signatures` | `GOPHERSTACK_S3_VALIDATE_SIGNATURES`, `S3_VALIDATE_SIGNATURES` | `false` | Cryptographically verify S3 request and presigned-URL signatures against --sigv4-secret, for S3 only (LocalStack S3_VALIDATE_SIGNATURES; S3_SKIP_SIGNATURE_VALIDATION=0 enables it too) |
| `--s3-disable-custom-cors` | `GOPHERSTACK_S3_DISABLE_CUSTOM_CORS`, `DISABLE_CUSTOM_CORS_S3` | `false` | Ignore per-bucket CORS configuration so only the global CORS settings apply (LocalStack DISABLE_CUSTOM_CORS_S3) |
| `--s3-compression-min-bytes` | `S3_COMPRESSION_MIN_BYTES` | `1024` | Minimum object size in bytes for zstd compression. Set to 0 to compress all objects regardless of size |

### ses

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--ses-inbound-addr` | `SES_INBOUND_ADDR` | `127.0.0.1:2525` | Listen address of the inbound SMTP server |
| `--ses-inbound-regions` | `SES_INBOUND_REGIONS` | — | Extra per-region inbound SMTP listeners as region=host:port,region=host:port |
| `--ses-inbound-max-size` | `SES_INBOUND_MAX_SIZE` | `41943040` | Maximum inbound message size in bytes |
| `--ses-inbound-enabled` | `SES_INBOUND_ENABLED` | `false` | Run an SMTP listener that applies the active receipt rule set to received mail |

### sqs

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--sqs-endpoint-strategy` | `SQS_ENDPOINT_STRATEGY` | — | Queue URL format: standard, domain, path, off or dynamic (empty keeps http://host/account/name) |

### transfer

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--transfer-sftp-host` | `TRANSFER_SFTP_HOST` | `127.0.0.1` | Host the Transfer Family SFTP listener binds to |
| `--transfer-sftp` | `TRANSFER_SFTP` | `false` | Serve a real SFTP listener per Transfer Family server |

### verifiedpermissions

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--verifiedpermissions-oidcca-file` | `GOPHERSTACK_VERIFIEDPERMISSIONS_OIDC_CA_FILE` | — | PEM bundle of extra CAs trusted for OIDC discovery/JWKS (self-signed issuers) |
| `--verifiedpermissions-disable-jwt-verification` | `VERIFIEDPERMISSIONS_DISABLE_JWT_VERIFICATION` | `false` | Skip JWT signature/expiry/issuer checks in IsAuthorizedWithToken (claims still parsed) |

## Environment-only variables

No flag; read directly from the environment.

| Env var | Default | Description |
|---|---|---|
| `AWS_DEFAULT_REGION` | unset | Overrides `--region`; wins over `AWS_REGION`. |
| `AWS_REGION` | unset | Overrides `--region`. |
| `AWS_ACCESS_KEY_ID` | `dummy` | Credentials for the server's own AWS SDK clients; incoming credentials are not validated. |
| `AWS_SECRET_ACCESS_KEY` | `dummy` | Paired with `AWS_ACCESS_KEY_ID`. |
| `CONTAINER_HOST` | unset | Podman socket endpoint override. |
| `GOPHERSTACK_PARTITION_STRICT` | `true` | `false` stops rejecting ARNs from a partition other than the signing region's. |
| `GOPHERSTACK_PPROF_ADDR` | unset | Opt-in pprof listener (for example `localhost:6060`); never enable in shared environments. |
| `SMTP_HOST` | unset | Relay SES and SESv2 mail to this SMTP server (`host[:port]`, default port 25). |
| `SMTP_USER` | unset | SMTP login username. |
| `SMTP_PASS` | unset | SMTP login password. |
