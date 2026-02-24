# Gopherstack

<p align="center">
  <img src="assets/logo.png" width="400" alt="Gopherstack Logo">
</p>

[![Release](https://github.com/agbishop/Gopherstack/actions/workflows/release.yml/badge.svg)](https://github.com/agbishop/Gopherstack/actions/workflows/release.yml)
[![Build](https://github.com/agbishop/Gopherstack/actions/workflows/release.yml/badge.svg?label=build)](https://github.com/agbishop/Gopherstack/actions/workflows/release.yml)
[![Coverage](https://raw.githubusercontent.com/agbishop/Gopherstack/badges/.badges/coverage.svg?v=1)](https://github.com/agbishop/Gopherstack/actions/workflows/main.yml)
[![Go Report Card](https://raw.githubusercontent.com/agbishop/Gopherstack/badges/.badges/goreportcard.svg?v=1)](https://goreportcard.com/report/github.com/agbishop/Gopherstack)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Gopherstack is a lightweight, in-memory AWS stack implementation for Go. It provides high-performance, mock-compatible versions of core AWS services, designed for rapid development, testing, and CI/CD pipelines. It currently supports DynamoDB, S3, SSM Parameter Store, IAM, STS, SNS, SQS, KMS, Secrets Manager, and **Lambda (image-based)**.

> [!IMPORTANT]
> **This project is vibe coded.** 🚀 It's built for speed, performance, and developer experience.

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
- Docker (optional)
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

## License

Gopherstack is released under the [MIT License](LICENSE).
