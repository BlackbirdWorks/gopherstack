# Quickstart

## Install

| Method | Command |
|--------|---------|
| `go install` (Go 1.27+) | `go install github.com/blackbirdworks/gopherstack@latest` |
| Binary | Download the archive for your OS from [GitHub Releases](https://github.com/blackbirdworks/gopherstack/releases/latest) |
| Docker | `docker run --rm -p 8000:8000 ghcr.io/blackbirdworks/gopherstack:latest` |

Release archives cover linux, darwin and windows on amd64 and arm64 (no windows/arm64). See [Docker](docker.md) for Compose.

You also need [AWS CLI v2](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html). Docker is needed only for features that launch containers (Lambda images, the `docker` [engines](engines.md)).

## Start

```bash
gopherstack          # listens on :8000
gopherstack health   # prints "ok" when the server is healthy
```

`gopherstack start` runs the server in a container (or a background process with `--host`). `stop`, `status`, `logs` and `reset` manage that instance.

## Use the AWS CLI

Set any credentials and a region, or the CLI will not sign requests:

```bash
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
```

```bash
aws --endpoint-url http://localhost:8000 s3 mb s3://my-bucket
aws --endpoint-url http://localhost:8000 sqs create-queue --queue-name my-queue
aws --endpoint-url http://localhost:8000 dynamodb create-table \
    --table-name Users \
    --attribute-definitions AttributeName=UserId,AttributeType=S \
    --key-schema AttributeName=UserId,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST
aws --endpoint-url http://localhost:8000 sts get-caller-identity
```

### Skip `--endpoint-url`

AWS CLI v2.13+ honours a global endpoint variable:

```bash
export AWS_ENDPOINT_URL=http://localhost:8000
aws s3 ls
```

Or use the `awsgs` wrapper. It injects `--endpoint-url` unless you pass one.

```bash
go install github.com/blackbirdworks/gopherstack/cmd/awsgs@latest
awsgs s3 mb s3://my-bucket
awsgs --awsgs-port 9000 s3 ls     # or AWSGS_PORT=9000 (GOPHERSTACK_PORT is the fallback)
```

`--awsgs-host` overrides the host.

## Dashboard

[http://localhost:8000/dashboard](http://localhost:8000/dashboard)

## Configuration

Every flag has an environment variable. Common ones:

| Flag | Env var | Default | Description |
|------|---------|---------|-------------|
| `--port` | `PORT` | `8000` | HTTP port |
| `--region` | `REGION` | `us-east-1` | Region (also reads `AWS_DEFAULT_REGION`, `AWS_REGION`) |
| `--account-id` | `ACCOUNT_ID` | `000000000000` | Account ID used in ARNs |
| `--log-level` | `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `--persist` | `PERSIST` | `false` | Save and restore state across restarts |
| `--data-dir` | `GOPHERSTACK_DATA_DIR` | `~/.gopherstack/data`, or `/data` in a container | Persistence directory |
| `--demo` | `DEMO` | `false` | Load demo data |
| `--latency-ms` | `LATENCY_MS` | `0` | Random latency in `[0,N)` ms per request |
| `--s3-bucket` | `S3_BUCKETS` | | Buckets to create at startup (repeatable or comma-separated) |
| `--tls` | `TLS` | `false` | Serve HTTPS; self-signed cert unless `--tls-cert`/`--tls-key` are set |
| `--dns-addr` | `DNS_ADDR` | | Enable embedded [DNS](architecture/dns.md), e.g. `:10053` |
| `--dns-resolve-ip` | `DNS_RESOLVE_IP` | `127.0.0.1` | IP that synthetic hostnames resolve to |
| `--elasticache-engine` | `ELASTICACHE_ENGINE` | `embedded` | `embedded`, `stub`, `docker` |
| `--opensearch-engine` | `OPENSEARCH_ENGINE` | `stub` | `stub`, `docker` |

`gopherstack --help` lists every flag; the full reference is [Configuration](configuration.md). Per-service engine flags are in [Engines](engines.md).

## Persistence

```bash
gopherstack --persist
```

State is snapshotted 500 ms after the last mutation. See [Persistence](persistence.md).

## Pods

A pod is a named series of full-emulator snapshots. Each save adds a version: a zip holding `manifest.json` and one JSON snapshot per service.

```bash
gopherstack pod save demo -m "seeded users"        # all services
gopherstack pod save demo --services s3,dynamodb   # subset
gopherstack pod list
gopherstack pod versions demo
gopherstack pod inspect demo --version 1
gopherstack pod load demo                          # latest, strategy=overwrite
gopherstack pod load demo --version 1 --strategy service-merge
gopherstack pod delete demo --version 1            # omit --version to delete the pod
gopherstack --pod-autoload demo:1                  # load at startup
```

| Strategy | Effect |
|----------|--------|
| `overwrite` | Reset every service, then restore the pod |
| `service-merge` | Replace only the services in the pod |

`pod load --merge` merges per account and region instead of replacing whole services. For autoload, `--pod-merge-strategy` (`GOPHERSTACK_POD_MERGE_STRATEGY`) also accepts `account-region-merge`.

Pods are stored in `<data-dir>/pods` (`--pod-dir`). For an S3 store set `--pod-s3-bucket`, plus `--pod-s3-prefix`, `--pod-s3-endpoint`, `--pod-s3-region`, `--pod-s3-access-key`, `--pod-s3-secret-key`, `--pod-s3-path-style`. Without an endpoint it uses real AWS S3 and the default credential chain.

The `pod` subcommands call a running server (`--endpoint`, `GOPHERSTACK_ENDPOINT`, default `http://localhost:8000`).

HTTP API:

| Method and path | Purpose |
|-----------------|---------|
| `GET /_gopherstack/pods` | List pods |
| `POST /_gopherstack/pods/{name}` | Save; body `{"message","services"}` |
| `GET /_gopherstack/pods/{name}` | List versions |
| `DELETE /_gopherstack/pods/{name}` | Delete; `?version=` for one version |
| `GET /_gopherstack/pods/{name}/versions/{version\|latest}` | Inspect |
| `POST /_gopherstack/pods/{name}/load?version=&strategy=&services=&merge=` | Load |

`POST /_gopherstack/snapshot` returns the raw per-service snapshot data.

## Next

- [Docker](docker.md)
- [Migrating from LocalStack](migration.md)
- [Services](services/)
- [Architecture](architecture/)
