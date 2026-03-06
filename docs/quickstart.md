# Quickstart

Get Gopherstack running locally in under two minutes and start using it as an AWS endpoint.

## Prerequisites

- [AWS CLI v2](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) — for interacting with the mock services
- Go 1.21+ (for `go install`) **or** a pre-built binary (see below)
- Docker (optional, required for Lambda image-based functions only)

## Installation

### Option 1 — `go install`

```bash
go install github.com/blackbirdworks/gopherstack@latest
```

### Option 2 — Pre-built binary

Download the latest release for your platform from [GitHub Releases](https://github.com/blackbirdworks/gopherstack/releases/latest):

```bash
# Linux / macOS (amd64)
curl -sSfL https://github.com/blackbirdworks/gopherstack/releases/latest/download/gopherstack_linux_amd64.tar.gz | tar xz
chmod +x gopherstack && mv gopherstack /usr/local/bin/

# macOS (Apple Silicon)
curl -sSfL https://github.com/blackbirdworks/gopherstack/releases/latest/download/gopherstack_darwin_arm64.tar.gz | tar xz
chmod +x gopherstack && mv gopherstack /usr/local/bin/
```

### Option 3 — Docker

```bash
docker run --rm -p 8000:8000 ghcr.io/blackbirdworks/gopherstack:latest
```

See [docker.md](docker.md) for a full Docker Compose setup.

## Start the server

```bash
gopherstack
# INFO starting server addr=:8000 region=us-east-1
```

The server is ready when you see the startup log. It listens on port `8000` by default.

```bash
# Verify it is running
gopherstack health
# OK
```

## First commands with the AWS CLI

Gopherstack speaks the native AWS wire protocol — no SDK changes needed, just point the CLI at `http://localhost:8000`.

```bash
# S3
aws --endpoint-url http://localhost:8000 s3 mb s3://my-bucket
aws --endpoint-url http://localhost:8000 s3 cp README.md s3://my-bucket/
aws --endpoint-url http://localhost:8000 s3 ls s3://my-bucket/

# SQS
aws --endpoint-url http://localhost:8000 sqs create-queue --queue-name my-queue
aws --endpoint-url http://localhost:8000 sqs send-message \
    --queue-url http://localhost:8000/000000000000/my-queue \
    --message-body "hello"

# DynamoDB
aws --endpoint-url http://localhost:8000 dynamodb create-table \
    --table-name Users \
    --attribute-definitions AttributeName=UserId,AttributeType=S \
    --key-schema AttributeName=UserId,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST

# STS (always returns the mock account)
aws --endpoint-url http://localhost:8000 sts get-caller-identity
```

## Skip typing `--endpoint-url` with `awsgs`

Install the `awsgs` wrapper to avoid repeating `--endpoint-url` on every command:

```bash
go install github.com/blackbirdworks/gopherstack/cmd/awsgs@latest
```

Then replace `aws` with `awsgs`:

```bash
awsgs s3 mb s3://my-bucket
awsgs sqs create-queue --queue-name my-queue
awsgs dynamodb list-tables
```

`awsgs` injects `--endpoint-url http://localhost:8000` automatically. Override the port with `--awsgs-port` or `AWSGS_PORT`:

```bash
awsgs --awsgs-port 9000 s3 ls
AWSGS_PORT=9000 awsgs dynamodb list-tables
```

## Using AWS environment variables

AWS CLI uses `AWS_ENDPOINT_URL` (v2.13+) as a global endpoint override — no wrapper needed:

```bash
export AWS_ENDPOINT_URL=http://localhost:8000
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1

aws s3 ls
aws dynamodb list-tables
```

## Dashboard

A web dashboard is available at [http://localhost:8000/dashboard](http://localhost:8000/dashboard) for browsing S3 buckets, DynamoDB tables, queues, and service metrics.

## Configuration reference

All options can be set as CLI flags or environment variables:

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--port` | `PORT` | `8000` | HTTP server port |
| `--region` | `REGION` | `us-east-1` | Default AWS region |
| `--account-id` | `ACCOUNT_ID` | `000000000000` | Mock AWS account ID |
| `--log-level` | `LOG_LEVEL` | `info` | Log level (`debug`, `info`, `warn`, `error`) |
| `--persist` | `PERSIST` | `false` | Enable snapshot persistence across restarts |
| `--data-dir` | `GOPHERSTACK_DATA_DIR` | `~/.gopherstack/data` | Persistence data directory |
| `--demo` | `DEMO` | `false` | Load demo data on startup |
| `--latency-ms` | `LATENCY_MS` | `0` | Inject random latency per request (ms) |
| `--elasticache-engine` | `ELASTICACHE_ENGINE` | `embedded` | ElastiCache engine: `embedded`, `stub`, `docker` |
| `--opensearch-engine` | `OPENSEARCH_ENGINE` | `stub` | OpenSearch engine: `stub`, `docker` |
| `--dns-addr` | `DNS_ADDR` | `` | Enable embedded DNS on this address (e.g. `:10053`) |
| `--dns-resolve-ip` | `DNS_RESOLVE_IP` | `127.0.0.1` | IP synthetic hostnames resolve to |

## Persistence

Start with `--persist` (or `PERSIST=true`) to save state across restarts:

```bash
gopherstack --persist
```

State is saved to `~/.gopherstack/data/` on macOS/Linux or `/data/` in container environments. A debounced snapshot is taken 500 ms after each mutation.

## Next steps

- [docker.md](docker.md) — Docker Compose setup for CI/CD
- [migration.md](migration.md) — Migrating from LocalStack
- [docs/services/](services/) — Per-service documentation
- [docs/architecture/](architecture/) — Internals and advanced configuration
