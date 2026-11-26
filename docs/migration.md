# Migrating from LocalStack

Point your clients at port `8000` instead of `4566`, or run in [compat mode](#drop-in-compat-mode) to keep `4566`.

## Endpoint

```bash
# before
AWS_ENDPOINT_URL=http://localhost:4566
# after
AWS_ENDPOINT_URL=http://localhost:8000
```

Both use one port for all AWS services. Profile:

```ini
# ~/.aws/config
[profile gopherstack]
region = us-east-1
endpoint_url = http://localhost:8000
```

Or use the [`awsgs`](quickstart.md#skip---endpoint-url) wrapper.

## Compose

```yaml
# before
services:
  localstack:
    image: localstack/localstack:latest
    ports: ["4566:4566"]
    environment:
      - SERVICES=s3,sqs,dynamodb,lambda
# after
services:
  gopherstack:
    image: ghcr.io/blackbirdworks/gopherstack:latest
    ports: ["8000:8000"]
    environment:
      - PERSIST=true
```

All services are in one binary. `SERVICES` / `--services` only restricts them.

## SDK configuration

Change only the endpoint URL.

```python
boto3.client("s3", endpoint_url="http://localhost:8000")
```

```go
cfg, _ := config.LoadDefaultConfig(ctx, config.WithBaseEndpoint("http://localhost:8000"))
```

```java
S3Client.builder().endpointOverride(URI.create("http://localhost:8000")).build();
```

```javascript
new S3Client({ endpoint: "http://localhost:8000" });
```

## Terraform / OpenTofu

Use the standard AWS provider:

```hcl
provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    s3       = "http://localhost:8000"
    sqs      = "http://localhost:8000"
    dynamodb = "http://localhost:8000"
    # every service you use
  }
}
```

## Real backends are opt-in

These services answer from metadata by default and run a real backend only when configured. See [Engines](engines.md).

| Service | Default | Real backend |
|---------|---------|--------------|
| RDS, DocumentDB, Redshift, MSK, MQ, EKS, DMS, ... | `stub` | `--<service>-engine docker` |
| ElastiCache, MemoryDB | `embedded` (in-process miniredis) | `docker` |
| OpenSearch, Elasticsearch | `stub` | `docker` |
| EC2 | `inmemory` | `--ec2-provider docker` launches containers as instances |

Lambda runs functions in containers, so it needs a container runtime (Docker, Podman or Kubernetes). See [Lambda](architecture/lambda.md).

## Differences from LocalStack

| | Gopherstack |
|---|---|
| Implementation | Go, single binary |
| Persistence | `--persist` / `PERSIST=true`; see [Persistence](persistence.md) |
| Dashboard | Built in at `/dashboard` |
| IAM enforcement | Opt-in: `--enforce-iam` / `GOPHERSTACK_ENFORCE_IAM` |
| State snapshots | [Pods](quickstart.md#pods) |
| Extensions | Out-of-process only: [Extensions](extensions.md) |
| Per-service DNS/proxy endpoint injection | Not provided; single port. Optional embedded [DNS](architecture/dns.md) |

## LocalStack internal endpoints

Served on the main port:

| Endpoint | Behaviour |
|---|---|
| `GET /_aws/ses[?id=&email=]`, `DELETE /_aws/ses[?id=]` | Sent SES and SESv2 messages as `{"messages":[...]}`; DELETE clears all or one by `id` |
| `GET /_aws/sqs/messages?QueueUrl=...`, `/_aws/sqs/messages/<region>/<account>/<queue>` | Peek at messages without changing visibility or receive counts. XML by default, JSON with `Accept: application/json`. Supports `ShowInvisible`, `ShowDelayed` |
| `GET /_aws/cloudwatch/metrics/raw` | `{"metrics":[{"ns","n","v","t","d","account","region"}]}` for single-value datapoints |
| `GET /_aws/lambda/runtimes[?filter=supported\|deprecated\|all]` | `{"Runtimes":[...]}` runtime identifiers; unknown `filter` is 400 |
| `POST /_localstack/state/reset` | Alias of `POST /_gopherstack/reset` |
| `GET/POST/PUT/PATCH /_localstack/pods`, `DELETE /_localstack/pods/<name>` | Aliases onto `/_gopherstack/pods`. GET lists `{"cloudpods":[{pod_name,max_version,last_change}]}`; POST/PUT saves `{pod_name, message?, services?}`; PATCH loads `{pod_name, version?, merge?, services?}`. Shapes are best-effort |
| `GET/POST /_localstack/state/export`, `POST /_localstack/state/import[?merge=true]` | Export or import all state as a `gopherstack-snapshot/v1` bundle |
| `GET /_localstack/health`, `/_aws/health`, `/_localstack/info` | Service list, version, edition |
| `POST /_localstack/health` | `restart` resets all state. `kill` needs `--localstack-health-kill` |
| `GET /_localstack/init`, `/_localstack/init/<stage>` | Init state: `completed` per stage and `scripts[]` with `UNKNOWN`/`RUNNING`/`SUCCESSFUL`/`ERROR` |
| `GET /_localstack/config` | Read-only config subset. `POST` returns 400 |
| `GET /_localstack/diagnose`, `/_localstack/plugins` | Version, config, service list, runtime stats |
| `GET/POST/PATCH/DELETE /_localstack/chaos/faults`, `GET/POST /_localstack/chaos/effects` | LocalStack Chaos API on the same fault store as `/_gopherstack/chaos/*`. See [Chaos](chaos.md) |

Not provided: `/health` (it would shadow an S3 bucket named `health`), `/_aws/sns/*`, `/_aws/dynamodb/expired`.

## Drop-in compat mode

`GOPHERSTACK_LOCALSTACK_COMPAT=true` (`--localstack-compat`) also serves on port `4566`. It turns on automatically if any `LOCALSTACK_*`, `GATEWAY_LISTEN`, `LS_LOG`, `HOSTNAME_EXTERNAL` or `PERSISTENCE` variable is set. `GOPHERSTACK_EXTRA_LISTEN=4571,4572` adds ports. If a port is taken, a warning is logged and the main listener is unaffected.

LocalStack variables are translated at startup. A variable already set under its Gopherstack name wins.

| LocalStack | Gopherstack |
|---|---|
| `DEBUG=1`, `LS_LOG` | `LOG_LEVEL` (`trace` maps to `debug`) |
| `PERSISTENCE=1`, `LOCALSTACK_PERSISTENCE=1` | `PERSIST=true` |
| `DATA_DIR` | `GOPHERSTACK_DATA_DIR` |
| `GATEWAY_LISTEN=host:port,...` | First port is `PORT`, the rest `GOPHERSTACK_EXTRA_LISTEN` |
| `DEFAULT_REGION`, `AWS_DEFAULT_REGION` | `REGION` |
| `LAMBDA_REMOVE_CONTAINERS=0` | `LAMBDA_KEEP_CONTAINERS=true` |
| `SERVICES` | Read directly. Case-insensitive, LocalStack aliases (`logs`, `events`, `es`, `sfn`) accepted. Unknown names are skipped with a warning; with `--services` they fail startup |
| `EXTRA_CORS_ALLOWED_ORIGINS`, `EXTRA_CORS_ALLOWED_HEADERS`, `EXTRA_CORS_EXPOSE_HEADERS`, `DISABLE_CORS_CHECKS` | Aliases of `--cors-allow-origins`, `--cors-allow-headers`, `--cors-expose-headers`, `--cors-disable-checks` |
| `DISABLE_CUSTOM_CORS_S3`, `DISABLE_CUSTOM_CORS_APIGATEWAY` | `GOPHERSTACK_S3_DISABLE_CUSTOM_CORS`, `GOPHERSTACK_CORS_DISABLE_CUSTOM_APIGATEWAY` |
| `ENFORCE_IAM`, `IAM_SOFT_MODE` | `GOPHERSTACK_ENFORCE_IAM`, `GOPHERSTACK_IAM_AUDIT` |
| `DNS_ADDRESS` | `DNS_ADDR` (`0` and `0.0.0.0` leave DNS off; port 53 if omitted) |
| `DNS_SERVER` | `TRANSPARENT_DNS_UPSTREAM` |
| `DNS_NAME_PATTERNS_TO_RESOLVE_UPSTREAM` | `TRANSPARENT_DNS_SKIP_PATTERNS` |
| `DISABLE_TRANSPARENT_ENDPOINT_INJECTION` | `TRANSPARENT_DISABLE_ENDPOINT_INJECTION` |
| `EXTERNAL_SERVICE_PORTS_START`, `_END` | `PORT_RANGE_START`, `PORT_RANGE_END` (end is inclusive in LocalStack, exclusive here: +1) |
| `SNAPSHOT_SAVE_STRATEGY` `ON_REQUEST`, `SCHEDULED`, `ON_SHUTDOWN`, `MANUAL` | `GOPHERSTACK_PERSIST_MODE` = `snapshot`, `hybrid`, `shutdown`, `manual`. `manual` saves only via `POST /_gopherstack/persist/save` |
| `SNAPSHOT_FLUSH_INTERVAL` (seconds) | `GOPHERSTACK_PERSIST_FLUSH_INTERVAL` |
| `SNAPSHOT_LOAD_STRATEGY` `ON_STARTUP`, `ON_REQUEST`, `MANUAL` | `GOPHERSTACK_PERSIST_LOAD` = `startup`, `request` (each service loads on its first request), `manual` (only via `POST /_gopherstack/persist/load`) |
| `S3_SKIP_KMS_KEY_VALIDATION` | `GOPHERSTACK_S3_SKIP_KMS_KEY_VALIDATION`, also read under the LocalStack name |
| `S3_VALIDATE_SIGNATURES`, `S3_SKIP_SIGNATURE_VALIDATION=0` | `GOPHERSTACK_S3_VALIDATE_SIGNATURES`: verify S3 header and presigned signatures against `--sigv4-secret` |
| `AUTO_LOAD_POD`, `MERGE_STRATEGY` | `GOPHERSTACK_POD_AUTOLOAD`, `GOPHERSTACK_POD_MERGE_STRATEGY` |
| `SQS_DELAY_PURGE_RETRY`, `SQS_DELAY_RECENTLY_DELETED`, `SQS_ENABLE_MESSAGE_RETENTION_PERIOD` | A false value sets `GOPHERSTACK_SQS_DISABLE_PURGE_COOLDOWN`, `..._RECENTLY_DELETED_COOLDOWN`, `..._MESSAGE_RETENTION` |
| `SQS_DISABLE_CLOUDWATCH_METRICS` | Read as is |
| `KINESIS_ERROR_PROBABILITY`, `DYNAMODB_ERROR_PROBABILITY` (0 to 1) | Seed a `ProvisionedThroughputExceededException` fault rule (HTTP 400) at startup |
| `KINESIS_LATENCY` (ms), `KINESIS_SHARD_LIMIT`, `KINESIS_ON_DEMAND_STREAM_COUNT_LIMIT` | `KINESIS_RESPONSE_LATENCY`, `KINESIS_ACCOUNT_SHARD_LIMIT`, `KINESIS_ACCOUNT_ON_DEMAND_STREAM_LIMIT` (`infinity`/`unlimited` = `-1`) |
| `LAMBDA_LIMITS_*` (concurrent executions, minimum unreserved concurrency, total code size, zipped/unzipped code size, create-function request size, env var size), `LAMBDA_RUNTIME_VALIDATION` | `LAMBDA_ACCOUNT_*`, `LAMBDA_CODE_SIZE_*`, `LAMBDA_CREATE_FUNCTION_REQUEST_SIZE`, `LAMBDA_MAX_ENVIRONMENT_BYTES`, `LAMBDA_STRICT_RUNTIME_VALIDATION` |
| `LAMBDA_KEEPALIVE_MS`, `HOSTNAME_FROM_LAMBDA` | `LAMBDA_IDLE_TIMEOUT`, `LAMBDA_DOCKER_HOST` |
| `ENABLE_DMS` | `DMS_ENGINE=docker` |
| `OUTBOUND_HTTP_PROXY`, `OUTBOUND_HTTPS_PROXY`, `REQUESTS_CA_BUNDLE` | `HTTP_PROXY`, `HTTPS_PROXY`, `SSL_CERT_FILE` |
| `LOCALSTACK_RESPONSE_HEADER_ENABLED` | `GOPHERSTACK_LOCALSTACK_RESPONSE_HEADER`: adds `X-Localstack: true` |
| `SMTP_HOST`, `SMTP_USER`, `SMTP_PASS`, `SFN_MOCK_CONFIG`, `SQS_ENDPOINT_STRATEGY`, `OPENSEARCH_ENDPOINT_STRATEGY`, `GRAPHQL_ENDPOINT_STRATEGY` | Read under the same names |

Unsupported options (logged once at warn as `LocalStack option X is not supported`) include `USE_SSL`, `LOCALSTACK_HOST`, `ECR_ENDPOINT_STRATEGY`, `LAMBDA_DOCKER_NETWORK`, `LAMBDA_RUNTIME_EXECUTOR`, `DYNAMODB_SHARE_DB` and other `LAMBDA_*`, `EC2_*`, `RDS_*`, `CFN_*` tuning options (list: `localstackUnsupportedKeys` in `internal/app/localstack_options.go`). Unknown `LOCALSTACK_*` variables also warn.

LocalStack infrastructure options with no equivalent (`EAGER_SERVICE_LOADING`, `LOCALSTACK_AUTH_TOKEN`, `DOCKER_FLAGS`, `*_DOCKER_FLAGS`, `PROVIDER_OVERRIDE_*`, JVM, Docker and Kubernetes plumbing) are logged once at debug.

## Pod load semantics

Loading a pod is atomic. Every section is validated first (a corrupt section returns 422 and touches no state). If a service fails to restore, all services roll back to their pre-load state.

`?merge=true` (CLI `--merge`) merges per account and region: records in the pod's accounts and regions replace the current ones for those scopes and other scopes are kept. Services whose snapshot has no account/region records are replaced whole.
