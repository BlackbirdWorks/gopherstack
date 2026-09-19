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

No `SERVICES` list is needed — all services run in a single binary.

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
| Lambda | ✅ Zip + Image | ✅ Zip + Image (zip archives run on matching AWS runtime base images via the Lambda Runtime API in Docker/Podman; no S3 code deployment) |
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
| Aurora DSQL | ❌ Pro only | ❌ Not implemented (planned) |

**Legend:** ✅ Full / equivalent — ⚠️ Partial or stub — ❌ Not available

Gopherstack emulates 162 AWS services (excluding its separate Azure storage/queue
emulators) — a superset of LocalStack's documented service list except for Aurora
DSQL, which LocalStack offers only on its Pro tier. Roughly 65 of those 162 services
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
| Extensions / plugins | ✅ (LocalStack extensions) | ❌ None |
| Endpoint injection | ✅ Transparent (per-service DNS/proxy) | ❌ Single port only |
| Cloud Pods | ✅ (state sharing/snapshots) | ❌ None (local `--persist` snapshots only) |

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
