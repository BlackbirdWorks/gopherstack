# Migrating from LocalStack

Gopherstack is a drop-in replacement for LocalStack for the services it supports. This guide covers the changes needed to switch your project.

## Endpoint URL change

| | LocalStack | Gopherstack |
|---|---|---|
| Default endpoint | `http://localhost:4566` | `http://localhost:8000` |
| Per-service ports | No (single port) | No (single port) |

Update your endpoint URL:

```bash
# Before
AWS_ENDPOINT_URL=http://localhost:4566

# After
AWS_ENDPOINT_URL=http://localhost:8000
```

## AWS CLI profiles

If you use a named profile for LocalStack, update the `endpoint_url`:

```ini
# ~/.aws/config
[profile gopherstack]
region = us-east-1
output = json
endpoint_url = http://localhost:8000
```

```bash
aws --profile gopherstack s3 ls
```

Or use the `awsgs` wrapper to avoid repeating the endpoint URL:

```bash
go install github.com/blackbirdworks/gopherstack/cmd/awsgs@latest
awsgs s3 ls
awsgs dynamodb list-tables
```

## Docker Compose migration

**Before (LocalStack):**

```yaml
services:
  localstack:
    image: localstack/localstack:latest
    ports:
      - "4566:4566"
    environment:
      - SERVICES=s3,sqs,dynamodb,lambda
      - DEBUG=1
```

**After (Gopherstack):**

```yaml
services:
  gopherstack:
    image: ghcr.io/blackbirdworks/gopherstack:latest
    ports:
      - "8000:8000"
    environment:
      - PERSIST=true
```

No `SERVICES` list is needed — all services run in a single binary (set `SERVICES` / `--services` only to restrict them).

## SDK configuration migration

### Python (boto3)

```python
# Before
import boto3
client = boto3.client("s3", endpoint_url="http://localhost:4566")

# After
import boto3
client = boto3.client("s3", endpoint_url="http://localhost:8000")
```

### Go (AWS SDK v2)

```go
// Before
cfg, _ := config.LoadDefaultConfig(ctx)
cfg.EndpointResolverWithOptions = aws.EndpointResolverWithOptionsFunc(
    func(service, region string, options ...interface{}) (aws.Endpoint, error) {
        return aws.Endpoint{URL: "http://localhost:4566"}, nil
    },
)

// After — use the new endpoint resolution API
import "github.com/aws/aws-sdk-go-v2/config"

cfg, _ := config.LoadDefaultConfig(ctx,
    config.WithBaseEndpoint("http://localhost:8000"),
)
```

### Java (AWS SDK v2)

```java
// Before
S3Client s3 = S3Client.builder()
    .endpointOverride(URI.create("http://localhost:4566"))
    .build();

// After
S3Client s3 = S3Client.builder()
    .endpointOverride(URI.create("http://localhost:8000"))
    .build();
```

### Node.js (AWS SDK v3)

```javascript
// Before
const client = new S3Client({ endpoint: "http://localhost:4566" });

// After
const client = new S3Client({ endpoint: "http://localhost:8000" });
```

## Feature comparison

| Service | LocalStack (Community) | Gopherstack |
|---------|----------------------|-------------|
| S3 | ✅ Full | ✅ Full (versioning, multipart, tagging) |
| SQS | ✅ Full | ✅ Full (FIFO, batch, visibility) |
| SNS | ✅ Full | ✅ Full (fan-out; HTTP/HTTPS, Lambda, and Firehose subscription delivery with DLQ) |
| DynamoDB | ✅ Full | ✅ Full (GSI, LSI, transactions, streams) |
| Lambda | ✅ Zip + Image | ✅ Zip + Image (zip archives run on matching AWS runtime base images via the Lambda Runtime API in Docker/Podman; code can be deployed from S3 via `Code.S3Bucket`/`S3Key`) |
| IAM | ✅ Partial | ✅ CRUD, with optional enforcement (`--enforce-iam` / `GOPHERSTACK_ENFORCE_IAM` evaluates attached policies on every request) |
| KMS | ✅ Partial | ✅ Symmetric and asymmetric (RSA/ECC sign/verify, ECDH, GenerateDataKeyPair) |
| Secrets Manager | ✅ Full | ✅ Full |
| SSM Parameter Store | ✅ Full | ✅ Full |
| Kinesis | ✅ Full | ✅ Streams, shards, records (Event Source Mapping polls into Lambda) |
| EventBridge | ✅ Full | ✅ Buses, rules, targets, event routing (archives/replay, API destinations, schema registry) |
| CloudWatch | ✅ Partial | ✅ Full (metrics, alarms, composite/anomaly alarms, dashboards, contributor insights, metric streams) |
| CloudWatch Logs | ✅ Full | ✅ Groups, streams, filtering |
| Firehose | ✅ Partial | ✅ Full (real delivery to S3, Redshift, OpenSearch, Elasticsearch, Splunk, HTTP endpoints, Snowflake, and Iceberg, with Lambda transform, format conversion, and retry/error-output routing) |
| Step Functions | ✅ Full | ✅ Full ASL execution |
| RDS | ✅ Partial | ⚠️ Metadata only (no real DB engine) |
| ElastiCache | ❌ Pro only | ✅ embedded/stub/docker modes |
| OpenSearch | ❌ Pro only | ⚠️ Metadata only (stub or docker) |
| EC2 | ✅ Partial | ⚠️ In-memory instance/VPC/SG stubs by default; `--ec2-provider docker` optionally launches real containers as instances |
| CloudFormation | ✅ Full | ✅ Full (creates real resources for ~78 resource types by calling the actual service backends — S3, DynamoDB, IAM, Lambda, EventBridge, API Gateway, etc.) |
| Route 53 | ✅ Partial | ✅ Hosted zones, record sets |
| ACM | ✅ Partial | ✅ Certificate lifecycle |
| SES | ✅ Partial | ⚠️ Accepted with mailbox-simulator bounce/complaint/delivery events published to SNS and configuration-set destinations; no real SMTP send |
| Scheduler | ❌ Pro only | ✅ Full (cron/rate schedules fire real targets — e.g. Lambda — via a ticker-driven runner with retries and DLQ) |
| Transcribe | ❌ Pro only | ⚠️ Job lifecycle is real; transcript text is synthetically generated (no real speech-to-text) |
| Redshift | ❌ Pro only | ⚠️ Metadata only (cluster/serverless management plane; RedshiftData `ExecuteStatement` returns canned demo rows, no real query engine) |
| STS | ✅ Full | ✅ AssumeRole, GetCallerIdentity |
| Aurora DSQL | ❌ Pro only | ⚠️ Control plane only (clusters CRUD/tags; no SQL data plane) |

**Legend:** ✅ Full / equivalent — ⚠️ Partial or stub — ❌ Not available

Gopherstack emulates 162 AWS services (excluding its separate Azure storage/queue
emulators) — a superset of LocalStack's documented service list, including Aurora
DSQL (control plane only), which LocalStack offers only on its Pro tier. Roughly 65 of those 162 services
don't appear in LocalStack's documentation at all.

## Key differences

| | LocalStack | Gopherstack |
|---|---|---|
| Language | Python | Go (single binary) |
| Memory footprint | 300 MB+ | ~30 MB |
| Cold start | 5–30 s | <100 ms |
| Persistence | Volume mount | `--persist` flag / `PERSIST=true` |
| Dashboard | ✅ (LocalStack UI) | ✅ (built-in at `/dashboard`) |
| Policy enforcement | ✅ Pro | ✅ Opt-in (`--enforce-iam` / `GOPHERSTACK_ENFORCE_IAM`) |
| Real DB/cache | ❌ | ✅ ElastiCache embedded mode (miniredis); RDS and Redshift remain metadata-only |
| Lambda runtimes | Zip + Image | Zip + Image (requires a Docker or Podman daemon) |
| Extensions / plugins | ✅ (LocalStack extensions) | ⚠️ Out-of-process extensions ([docs](extensions.md)); no in-process plugins |
| Endpoint injection | ✅ Transparent (per-service DNS/proxy) | ❌ Single port only |
| Cloud Pods | ✅ (state sharing/snapshots) | ❌ None (local `--persist` snapshots only) |

## LocalStack internal endpoints

These LocalStack developer endpoints are served on the edge port:

| Endpoint | Behaviour |
|---|---|
| `GET /_aws/ses[?id=&email=]`, `DELETE /_aws/ses[?id=]` | Sent SES and SESv2 messages as `{"messages":[...]}`; DELETE clears all (or one by `id`) |
| `GET /_aws/sqs/messages?QueueUrl=...` and `/_aws/sqs/messages/<region>/<account>/<queue>` | Peek at queue messages without changing visibility or receive counts; XML by default, JSON with `Accept: application/json`; `ShowInvisible`/`ShowDelayed` supported |
| `GET /_aws/cloudwatch/metrics/raw` | `{"metrics":[{"ns","n","v","t","d","account","region"}]}` for single-value datapoints |
| `POST /_localstack/state/reset` | Alias for `POST /_gopherstack/reset` |
| `GET/POST/PUT/PATCH /_localstack/pods`, `DELETE /_localstack/pods/<name>` | Cloud Pods-style aliases onto `/_gopherstack/pods`: GET lists `{"cloudpods":[{pod_name,max_version,last_change}]}`; POST/PUT saves `{pod_name, message?, services?}`; PATCH loads `{pod_name, version?, merge?, services?}`; DELETE removes a pod. Shapes are best-effort; LocalStack's pod API is not publicly specified |
| `GET/POST /_localstack/state/export`, `POST /_localstack/state/import[?merge=true]` | Export or import all service state as a `gopherstack-snapshot/v1` bundle |
| `GET /_localstack/health`, `/_aws/health`, `/_localstack/info` | Service list, version and `edition` |
| `GET /_localstack/init`, `/_localstack/init/<stage>` | Real init state (`--init-script` as READY plus `{boot,start,ready,shutdown}.d` stage dirs): `completed` per stage plus `scripts[]` with `UNKNOWN`/`RUNNING`/`SUCCESSFUL`/`ERROR` |
| `GET /_localstack/config` | Read-only runtime config subset (`DEFAULT_REGION`, `GATEWAY_LISTEN`, `PERSISTENCE`, ...). `POST` returns 400: runtime updates are unsupported |
| `GET /_localstack/diagnose`, `/_localstack/plugins` | Version/config/service list and runtime stats; services listed as `localstack.aws.provider` plugins |
| `GET/POST/PATCH/DELETE /_localstack/chaos/faults`, `GET/POST /_localstack/chaos/effects` | LocalStack Chaos API on the real fault engine: rules `{service, region, operation, probability, error:{statusCode, code}}` (omitted field = wildcard, first match wins, `[{}]` fails everything, `[]` clears), effects `{"latency": ms}`. POST replaces, PATCH appends, DELETE removes rules equal in every field. Omitted `probability` always fires, explicit `0` never fires. Probability outside 0..1 or a status outside 100..599 returns 400. Injected errors use the service's protocol (JSON with `__type` and `x-amzn-ErrorType`, Query `<ErrorResponse>`, EC2 `<Response><Errors>`, REST-XML `<Error>`). Same state as `/_gopherstack/chaos/*` |

Not provided: `/health` (it would shadow an S3 bucket named `health`), `/_aws/sns/*` (SNS keeps no SMS or platform-endpoint message log), `/_aws/lambda/runtimes`, `/_aws/dynamodb/expired`. Bucket names cannot start with `_`, so `/_localstack/*` never collides with S3 path-style routing.

## Drop-in compat mode

Set `GOPHERSTACK_LOCALSTACK_COMPAT=true` (or `--localstack-compat`) to serve on port 4566 in addition to the main port, so `localstack/localstack` can be swapped out with `-p 4566:4566` untouched. It turns on automatically when any `LOCALSTACK_*`, `GATEWAY_LISTEN`, `LS_LOG`, `HOSTNAME_EXTERNAL` or `PERSISTENCE` variable is set. `GOPHERSTACK_EXTRA_LISTEN=4571,4572` adds further ports. If a port is taken, a warning is logged and the main listener is unaffected.

LocalStack variables are translated at startup; a variable already set under its Gopherstack name wins.

| LocalStack | Gopherstack |
|---|---|
| `DEBUG=1`, `LS_LOG=trace\|debug\|info\|warn\|error` | `LOG_LEVEL` |
| `PERSISTENCE=1`, `LOCALSTACK_PERSISTENCE=1` | `PERSIST=true` |
| `DATA_DIR` | `GOPHERSTACK_DATA_DIR` |
| `GATEWAY_LISTEN=host:port,...` | first port becomes `PORT`, the rest `GOPHERSTACK_EXTRA_LISTEN` |
| `DEFAULT_REGION`, `AWS_DEFAULT_REGION` | `REGION` |
| `LAMBDA_REMOVE_CONTAINERS=0` | `LAMBDA_KEEP_CONTAINERS=true` |
| `LOCALSTACK_HOST`, `HOSTNAME_EXTERNAL` | Not needed: SQS queue URLs and similar follow the request `Host` header |
| `SERVICES` | Supported: `--services` / `GOPHERSTACK_SERVICES` (`SERVICES` is read directly; names are case-insensitive, LocalStack aliases like `logs`, `events`, `es`, `sfn` accepted; unknown names in `SERVICES` are skipped with a warning, unlike `--services`, where they fail startup) |
| `EXTRA_CORS_ALLOWED_ORIGINS`, `EXTRA_CORS_ALLOWED_HEADERS`, `EXTRA_CORS_EXPOSE_HEADERS`, `DISABLE_CORS_CHECKS` | Supported as aliases of `--cors-allow-origins`, `--cors-allow-headers`, `--cors-expose-headers`, `--cors-disable-checks` |
| `ENFORCE_IAM`, `IAM_SOFT_MODE` | `GOPHERSTACK_ENFORCE_IAM`, `GOPHERSTACK_IAM_AUDIT` |
| `DNS_ADDRESS`, `DNS_SERVER` | `DNS_ADDR` (`0` / `0.0.0.0` leave DNS off), `TRANSPARENT_DNS_UPSTREAM`; port 53 when omitted |
| `EXTERNAL_SERVICE_PORTS_START` / `_END` | `PORT_RANGE_START` / `PORT_RANGE_END` (the end is inclusive in LocalStack, exclusive here, so +1) |
| `SNAPSHOT_SAVE_STRATEGY` (`ON_REQUEST`, `SCHEDULED`, `ON_SHUTDOWN`, `MANUAL`), `SNAPSHOT_FLUSH_INTERVAL` | `GOPHERSTACK_PERSIST_MODE` = `snapshot` / `hybrid` / `shutdown` / `manual`, `GOPHERSTACK_PERSIST_FLUSH_INTERVAL`. `MANUAL` saves only through `POST /_gopherstack/persist/save` |
| `SNAPSHOT_LOAD_STRATEGY` (`ON_STARTUP`, `ON_REQUEST`, `MANUAL`) | `GOPHERSTACK_PERSIST_LOAD` = `startup` / `request` (each service loads on its first request through the HTTP gateway) / `manual` (only through `POST /_gopherstack/persist/load`) |
| `S3_SKIP_KMS_KEY_VALIDATION` | `GOPHERSTACK_S3_SKIP_KMS_KEY_VALIDATION` (`--s3-skip-kms-key-validation`). Gopherstack follows AWS and rejects SSE-KMS key ids unknown to its KMS with `KMS.NotFoundException` by default; LocalStack's default is `1` (skip). Set `S3_SKIP_KMS_KEY_VALIDATION=1` to accept unknown keys |
| `S3_VALIDATE_SIGNATURES`, `S3_SKIP_SIGNATURE_VALIDATION=0` | `GOPHERSTACK_S3_VALIDATE_SIGNATURES`: verify S3 header and presigned-URL signatures against `--sigv4-secret`, for S3 only. Off by default, like LocalStack's `S3_SKIP_SIGNATURE_VALIDATION=1` |
| `DISABLE_CUSTOM_CORS_S3`, `DISABLE_CUSTOM_CORS_APIGATEWAY` | `GOPHERSTACK_S3_DISABLE_CUSTOM_CORS`, `GOPHERSTACK_CORS_DISABLE_CUSTOM_APIGATEWAY`: ignore per-bucket S3 CORS / API Gateway resource CORS so only the global CORS settings apply |
| `AUTO_LOAD_POD`, `MERGE_STRATEGY` (`overwrite`, `service-merge`, `account-region-merge`) | `GOPHERSTACK_POD_AUTOLOAD`, `GOPHERSTACK_POD_MERGE_STRATEGY` |
| `SQS_DELAY_PURGE_RETRY`, `SQS_DELAY_RECENTLY_DELETED`, `SQS_ENABLE_MESSAGE_RETENTION_PERIOD`, `SQS_DISABLE_CLOUDWATCH_METRICS` | `SQS_DISABLE_CLOUDWATCH_METRICS` is read as is. The other three are inverted onto `GOPHERSTACK_SQS_DISABLE_*` when set to a false value; Gopherstack defaults to the AWS behaviour (cooldowns and retention on) |
| `KINESIS_ERROR_PROBABILITY`, `DYNAMODB_ERROR_PROBABILITY` (0 to 1) | Seed a probabilistic `ProvisionedThroughputExceededException` fault rule (HTTP 400) in the chaos fault store at startup; replaceable via `/_localstack/chaos/faults` |
| `LAMBDA_KEEPALIVE_MS`, `HOSTNAME_FROM_LAMBDA` | `LAMBDA_IDLE_TIMEOUT`, `LAMBDA_DOCKER_HOST` |
| `OUTBOUND_HTTP_PROXY`, `OUTBOUND_HTTPS_PROXY`, `REQUESTS_CA_BUNDLE` | `HTTP_PROXY`, `HTTPS_PROXY`, `SSL_CERT_FILE` |
| `LOCALSTACK_RESPONSE_HEADER_ENABLED` | `GOPHERSTACK_LOCALSTACK_RESPONSE_HEADER` (opt-in `X-Localstack: true`) |
| `SMTP_HOST`, `SMTP_USER`, `SMTP_PASS`, `SFN_MOCK_CONFIG`, `*_ENDPOINT_STRATEGY` | Read under the same names |
| `LAMBDA_LIMITS_*`, `KINESIS_LATENCY`, `KINESIS_SHARD_LIMIT`, `LAMBDA_RUNTIME_*`, `USE_SSL`, and other documented options with a behaviour Gopherstack lacks | Unsupported; each one that is set logs `LocalStack option X is not supported` once at warn |
| `EAGER_SERVICE_LOADING`, `LOCALSTACK_AUTH_TOKEN`, `*_DOCKER_FLAGS`, `PROVIDER_OVERRIDE_*`, JVM, Docker and Kubernetes plumbing options | No equivalent needed; logged once at debug |

## Terraform / OpenTofu provider

If you use the [LocalStack Terraform provider](https://github.com/localstack/terraform-provider-aws-localstack), replace it with the standard AWS provider pointing at Gopherstack:

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
    # Add all services you use
  }
}
```

## Pod load semantics

Loading a pod is atomic: every section is validated first (a corrupt section returns 422 with no state touched), and if a service fails to restore, all services are rolled back to their pre-load state. `?merge=true` (CLI `--merge`) merges per account/region: records in the pod's account/regions replace the current ones for those scopes and other scopes are kept. Services whose snapshot has no account/region records are replaced whole.
