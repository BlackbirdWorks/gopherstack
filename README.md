# Gopherstack

<p align="center">
  <img src="assets/logo.png" width="400" alt="Gopherstack Logo">
</p>

[![Release](https://github.com/blackbirdworks/gopherstack/actions/workflows/release.yml/badge.svg)](https://github.com/blackbirdworks/gopherstack/actions/workflows/release.yml)
[![Build](https://github.com/blackbirdworks/gopherstack/actions/workflows/release.yml/badge.svg?label=build)](https://github.com/blackbirdworks/gopherstack/actions/workflows/release.yml)
[![Coverage](https://raw.githubusercontent.com/blackbirdworks/gopherstack/badges/.badges/coverage.svg?v=1)](https://github.com/blackbirdworks/gopherstack/actions/workflows/main.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/blackbirdworks/gopherstack)](https://goreportcard.com/report/github.com/blackbirdworks/gopherstack)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Gopherstack is a lightweight, in-memory AWS stack implementation for Go. It provides high-performance, mock-compatible versions of core AWS services, designed for rapid development, testing, and CI/CD pipelines. It currently supports DynamoDB, S3, SSM Parameter Store, IAM, STS, SNS, SQS, KMS, Secrets Manager, and **Lambda (image-based)**.

> [!IMPORTANT]
> **This project is vibe coded.** 🚀 It's built for speed, performance, and developer experience.

## Quick Start

The fastest way to get started is to pull and run the Gopherstack Docker image:

```bash
docker run -p 8000:8000 ghcr.io/blackbirdworks/gopherstack:latest
```

Once running, open the built-in web dashboard in your browser:

```
http://localhost:8000/dashboard
```

The dashboard lets you browse and manage DynamoDB tables, S3 buckets, and more — no AWS credentials required. Point any AWS SDK or CLI at `http://localhost:8000` as the endpoint URL and start building.

## Features

### DynamoDB
- **In-Memory Storage**: Blazing fast in-memory storage for tables and items.
- **Secondary Indexes**: Full support for Global Secondary Indexes (GSI) and Local Secondary Indexes (LSI).
- **Rich Querying**: Complex queries with Sort Key conditions, pagination (`Limit`, `ExclusiveStartKey`), and ordering control.
- **Efficient Scanning**: Flexible table scans with filtering and projection supporting DynamoDB expressions.
- **Expression Support**: Robust handling of Expression Attribute Values and Names.
- **Optimized Memory Layout**: Struct field alignment optimized for minimal memory footprint.

### S3
- **Bucket Management**: Complete lifecycle management for versioned and unversioned buckets.
- **Object Operations**: Reliable Get, Put, Head, and List operations.
- **Versioning & Tagging**: First-class support for object versioning and metadata tagging.
- **Data Integrity**: Automatic checksum calculation supporting CRC32, CRC32C, SHA1, and SHA256.
- **Compression**: Integrated Gzip compression for efficient memory usage.

### Lambda (image-based only)

Gopherstack supports AWS Lambda with **Docker image-based functions only** (`PackageType: Image`).

> **Important:** Only `PackageType: Image` is supported. Zip deployments, S3-based code delivery, and direct Go binary execution on the host are **not supported**. Your function must be packaged as a Docker image (e.g. a standard AWS base image or your own custom image).

- **Supported operations**: `CreateFunction`, `GetFunction`, `ListFunctions`, `DeleteFunction`, `UpdateFunctionCode`, `UpdateFunctionConfiguration`, `Invoke`
- **Invocation modes**: `RequestResponse` (synchronous) and `Event` (asynchronous / fire-and-forget)
- **Lambda Runtime API**: Full implementation of the [Lambda Runtime API](https://docs.aws.amazon.com/lambda/latest/dg/runtimes-api.html) — standard AWS base images work without modification
- **Warm container pool**: Configurable per-function pool keeps containers warm to reduce cold-start latency
- **Environment variables**: Passed directly to the container
- **Requires Docker**: Lambda functions need a running Docker daemon. All other Gopherstack services continue to work without Docker.

#### Lambda CLI examples

```bash
# Create an image-based Lambda function
aws lambda create-function \
    --endpoint-url http://localhost:8000 \
    --function-name my-function \
    --package-type Image \
    --code ImageUri=public.ecr.aws/lambda/python:3.12 \
    --role arn:aws:iam::000000000000:role/my-role

# Invoke synchronously
aws lambda invoke \
    --endpoint-url http://localhost:8000 \
    --function-name my-function \
    --payload '{"key":"value"}' \
    response.json

# List functions
aws lambda list-functions --endpoint-url http://localhost:8000
```

#### Lambda configuration

| Flag | Env var | Default | Description |
|------|---------|---------|-------------|
| `--lambda-docker-host` | `LAMBDA_DOCKER_HOST` | `172.17.0.1` | Host/IP that Lambda containers use to reach Gopherstack's Runtime API |
| `--lambda-pool-size` | `LAMBDA_POOL_SIZE` | `3` | Maximum warm containers per function |
| `--lambda-idle-timeout` | `LAMBDA_IDLE_TIMEOUT` | `10m` | Idle container lifetime before reaping |
| `--container-runtime` | `CONTAINER_RUNTIME` | `docker` | Container runtime to use: `docker`, `podman`, or `auto` |

#### Using Podman

Gopherstack supports [Podman](https://podman.io/) as a drop-in replacement for Docker via
Podman's Docker-compatible API socket.

**Rootless Podman setup (Linux):**

```bash
# Enable the Podman socket for your user
systemctl --user enable --now podman.socket

# Point Gopherstack at Podman
export CONTAINER_RUNTIME=podman
# Optional: override the socket path
export CONTAINER_HOST=unix://${XDG_RUNTIME_DIR}/podman/podman.sock
```

**Rootless networking note:** In rootless Podman the Docker bridge (`172.17.0.1`) is not
available.  Use the host's routable IP or `host.containers.internal` instead:

```bash
export LAMBDA_DOCKER_HOST=host.containers.internal
```

**Auto-detection:** Set `CONTAINER_RUNTIME=auto` to let Gopherstack probe Docker first,
then Podman, and use whichever socket is reachable.



Gopherstack includes a built-in web dashboard for managing DynamoDB tables and S3 buckets.

Access the dashboard at: `http://localhost:8000/dashboard`

Features:
- **DynamoDB**:
  - List tables
  - View table details (keys, indexes, item count)
  - Query and Scan tables
  - Create new tables
- **S3**:
  - List buckets
  - File browser with folder support
  - Upload and Download files
  - Manage versioning
  - View object metadata

## Usage

### Prerequisites
- Go 1.26+
- Docker or Podman (optional, required for Lambda `PackageType: Image` invocations)
- AWS CLI (optional, for testing)

### Development
```bash
# Run all checks (lint + all tests with coverage)
make all

# Run only unit tests (short mode)
make test

# Run all tests (unit, integration, and E2E) with combined coverage
make total-coverage

# Check linting
make lint
```

### Integration
You can use Gopherstack directly in your Go tests by initializing the in-memory backends:

```go
import "github.com/blackbirdworks/gopherstack/dynamodb"

db := dynamodb.NewInMemoryDB()
// Use db for your application logic...
```

## Docker

Gopherstack is available as a lightweight Docker image.

### Docker Compose
You can run Gopherstack as a service in your `docker-compose.yml`:

```yaml
services:
  gopherstack:
    image: ghcr.io/blackbirdworks/gopherstack:latest
    ports:
      - "8000:8000"
    environment:
      - LOG_LEVEL=info
```

Run with: `docker compose up -d`

## AWS CLI Examples

Gopherstack is fully compatible with the AWS CLI. Simply provide the `--endpoint-url`.

### DynamoDB
```bash
# Create a table
aws dynamodb create-table \
    --endpoint-url http://localhost:8000 \
    --table-name Users \
    --attribute-definitions AttributeName=ID,AttributeType=S \
    --key-schema AttributeName=ID,KeyType=HASH \
    --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5

# List tables
aws dynamodb list-tables --endpoint-url http://localhost:8000
```

### S3
```bash
# Create a bucket
aws s3 mb s3://my-bucket --endpoint-url http://localhost:8000

# Upload a file
aws s3 cp myfile.txt s3://my-bucket/ --endpoint-url http://localhost:8000

# List objects
aws s3 ls s3://my-bucket/ --endpoint-url http://localhost:8000
```

## Testcontainers Module

Gopherstack ships a reusable [Testcontainers for Go](https://golang.testcontainers.org/) module so you can spin up all AWS mock services in a single call from any Go test suite.

### Installation

```bash
go get github.com/blackbirdworks/gopherstack/modules/gopherstack
```

### Usage

```go
import (
    "context"
    "testing"

    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/credentials"
    "github.com/aws/aws-sdk-go-v2/service/dynamodb"
    "github.com/testcontainers/testcontainers-go"

    gopherstack "github.com/blackbirdworks/gopherstack/modules/gopherstack"
)

func TestMyService(t *testing.T) {
    ctx := context.Background()

    container, err := gopherstack.Run(ctx, gopherstack.DefaultImage)
    if err != nil {
        t.Fatal(err)
    }
    defer testcontainers.TerminateContainer(container)

    endpoint, err := container.BaseURL(ctx)
    if err != nil {
        t.Fatal(err)
    }

    cfg, _ := config.LoadDefaultConfig(ctx,
        config.WithRegion("us-east-1"),
        config.WithCredentialsProvider(
            credentials.NewStaticCredentialsProvider("test", "test", ""),
        ),
    )

    ddb := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
        o.BaseEndpoint = aws.String(endpoint)
    })

    // … use ddb in your tests
}
```

Pass environment variables with `gopherstack.WithEnv`:

```go
container, err := gopherstack.Run(ctx, gopherstack.DefaultImage,
    gopherstack.WithEnv(map[string]string{
        "LOG_LEVEL": "debug",
        "DEMO":      "true",
    }),
)
```

## Terraform Compatibility

Point the AWS provider at Gopherstack by overriding the service endpoints. No
real credentials are required — any non-empty string works.

```hcl
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    dynamodb       = "http://localhost:8000"
    s3             = "http://localhost:8000"
    sqs            = "http://localhost:8000"
    sns            = "http://localhost:8000"
    ssm            = "http://localhost:8000"
    kms            = "http://localhost:8000"
    secretsmanager = "http://localhost:8000"
    iam            = "http://localhost:8000"
    sts            = "http://localhost:8000"
    lambda         = "http://localhost:8000"
    cloudformation = "http://localhost:8000"
    cloudwatch     = "http://localhost:8000"
    cloudwatchlogs = "http://localhost:8000"
    stepfunctions  = "http://localhost:8000"
    eventbridge    = "http://localhost:8000"
    apigateway     = "http://localhost:8000"
  }
}
```

Terraform uses path-style S3 URLs. Set `use_path_style = true` on the S3
resource or provider if you create `aws_s3_bucket` resources:

```hcl
resource "aws_s3_bucket" "example" {
  bucket = "my-bucket"

  # Path-style is required when using Gopherstack
  force_destroy = true
}
```

Start Gopherstack, then run your plan/apply as usual:

```bash
docker compose up -d   # or: ./gopherstack --port 8000
terraform init
terraform apply
```

## AWS CDK Compatibility

CDK synthesises CloudFormation templates locally and deploys them via the AWS
SDK. Point the SDK at Gopherstack by setting `AWS_ENDPOINT_URL` (AWS CLI / SDK
v2 unified endpoint) before running `cdk deploy`.

```bash
export AWS_ENDPOINT_URL=http://localhost:8000
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export CDK_DEFAULT_ACCOUNT=000000000000
export CDK_DEFAULT_REGION=us-east-1
```

Deploy your CDK app:

```bash
docker compose up -d   # start Gopherstack
cdk bootstrap          # creates the CDKToolkit stack (uses CloudFormation)
cdk deploy             # deploy your stack
```

For CDK apps written in TypeScript / Python that configure the SDK explicitly:

```typescript
// cdk.json or app code — override the endpoint for local development
const app = new cdk.App();
const env: cdk.Environment = {
  account: process.env.CDK_DEFAULT_ACCOUNT ?? "000000000000",
  region:  process.env.CDK_DEFAULT_REGION  ?? "us-east-1",
};
new MyStack(app, "MyStack", { env });
```

The `AWS_ENDPOINT_URL` environment variable is picked up automatically by the
AWS SDK v2 used by the CDK CLI.

## License

Gopherstack is released under the [MIT License](LICENSE).
