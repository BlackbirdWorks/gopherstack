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
| SNS | ✅ Full | ✅ Full (fan-out, subscriptions) |
| DynamoDB | ✅ Full | ✅ Full (GSI, LSI, transactions, streams) |
| Lambda | ✅ Zip + Image | ⚠️ Image-only (no zip/S3 deployment) |
| IAM | ✅ Partial | ✅ CRUD (policies stored, not enforced) |
| KMS | ✅ Partial | ✅ Symmetric keys, encrypt/decrypt |
| Secrets Manager | ✅ Full | ✅ Full |
| SSM Parameter Store | ✅ Full | ✅ Full |
| Kinesis | ✅ Full | ✅ Streams, shards, records |
| EventBridge | ✅ Full | ✅ Buses, rules, targets, event routing |
| CloudWatch | ✅ Partial | ✅ Metrics, alarms |
| CloudWatch Logs | ✅ Full | ✅ Groups, streams, filtering |
| Firehose | ✅ Partial | ⚠️ Records accepted, not delivered |
| Step Functions | ✅ Full | ✅ Full ASL execution |
| RDS | ✅ Partial | ⚠️ Metadata only (no real DB) |
| ElastiCache | ❌ Pro only | ✅ embedded/stub/docker modes |
| OpenSearch | ❌ Pro only | ⚠️ Metadata only (stub or docker) |
| EC2 | ✅ Partial | ⚠️ Instance/VPC/SG stubs |
| CloudFormation | ✅ Full | ⚠️ Stack lifecycle (resources not created) |
| Route 53 | ✅ Partial | ✅ Hosted zones, record sets |
| ACM | ✅ Partial | ✅ Certificate lifecycle |
| SES | ✅ Partial | ⚠️ Accepted, not sent |
| Scheduler | ❌ Pro only | ⚠️ Stored, not executed |
| Transcribe | ❌ Pro only | ⚠️ Stub (no real transcription) |
| Redshift | ❌ Pro only | ⚠️ Metadata only |
| STS | ✅ Full | ✅ AssumeRole, GetCallerIdentity |

**Legend:** ✅ Full / equivalent — ⚠️ Partial or stub — ❌ Not available

## Key differences

| | LocalStack | Gopherstack |
|---|---|---|
| Language | Python | Go (single binary) |
| Memory footprint | 300 MB+ | ~30 MB |
| Cold start | 5–30 s | <100 ms |
| Persistence | Volume mount | `--persist` flag / `PERSIST=true` |
| Dashboard | ✅ (LocalStack UI) | ✅ (built-in at `/dashboard`) |
| Policy enforcement | ✅ Pro | ❌ (IAM stored, not enforced) |
| Real DB/cache | ❌ | ✅ ElastiCache embedded mode |
| Lambda runtimes | Zip + Image | Image only |

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
