<p align="center">
  <img src="assets/logo.png" width="640" alt="Gopherstack">
</p>

<p align="center">
  <strong>150+ AWS services. One Go binary. Milliseconds, not minutes.</strong>
</p>

<p align="center">
  <a href="https://github.com/blackbirdworks/gopherstack/actions/workflows/ci.yml"><img src="https://github.com/blackbirdworks/gopherstack/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://github.com/blackbirdworks/gopherstack/actions/workflows/release.yml"><img src="https://github.com/blackbirdworks/gopherstack/actions/workflows/release.yml/badge.svg" alt="Release"></a>
  <a href="https://codecov.io/gh/BlackbirdWorks/gopherstack"><img src="https://codecov.io/gh/BlackbirdWorks/gopherstack/branch/main/graph/badge.svg" alt="codecov"></a>
  <a href="https://www.codefactor.io/repository/github/blackbirdworks/gopherstack"><img src="https://www.codefactor.io/repository/github/blackbirdworks/gopherstack/badge" alt="CodeFactor"></a>
  <a href="https://scorecard.dev/viewer/?uri=github.com/BlackbirdWorks/gopherstack"><img src="https://api.scorecard.dev/projects/github.com/BlackbirdWorks/gopherstack/badge" alt="OpenSSF Scorecard"></a>
  <a href="https://pkg.go.dev/github.com/blackbirdworks/gopherstack"><img src="https://pkg.go.dev/badge/github.com/blackbirdworks/gopherstack.svg" alt="Go Reference"></a>
</p>

<p align="center">
  <a href="#services"><img src=".badges/operations.svg" alt="PARITY entries"></a>
  <a href="#services"><img src=".badges/services.svg" alt="AWS services"></a>
  <a href="#services"><img src=".badges/parity.svg" alt="parity"></a>
  <a href="go.mod"><img src=".badges/go.svg" alt="go"></a>
  <a href="LICENSE"><img src=".badges/license.svg" alt="license"></a>
</p>

Gopherstack is a lightweight, in-memory AWS cloud stack you can run locally. It emulates
**150+ AWS service APIs** in a single Go binary — no AWS account, no credentials, no network
calls — so you can develop and test against realistic AWS behaviour in milliseconds.

Point any AWS SDK, the AWS CLI, Terraform, or the CDK at `http://localhost:8000` and it just
works. Beyond simple CRUD mocks, Gopherstack implements real cross-service integration:
**Event Source Mappings**, **EventBridge Scheduler**, **EventBridge Pipes**, **DynamoDB
Streams → Lambda**, **SNS → SQS fan-out**, and container-backed **Lambda** execution.

> [!TIP]
> Gopherstack is significantly faster and lighter than LocalStack, making it ideal for unit
> and integration tests where speed is critical.

> [!IMPORTANT]
> **This project is vibe coded.** 🚀 It's built for speed, performance, and developer experience.

---

## Quick Start

Pull and run the image:

```bash
docker run -p 8000:8000 ghcr.io/blackbirdworks/gopherstack:latest
```

Open the built-in web dashboard:

```
http://localhost:8000/dashboard
```

Then point any AWS tooling at the endpoint — no credentials required:

```bash
aws dynamodb list-tables --endpoint-url http://localhost:8000
aws s3 mb s3://my-bucket --endpoint-url http://localhost:8000
```

## Managing a local instance

The `gopherstack` binary can run and manage a local instance itself. It binds to loopback only unless you pass `--bind`.

```bash
gopherstack start                      # Docker container from ghcr.io/blackbirdworks/gopherstack:latest
gopherstack start --host --persist     # background host process, state kept under the state dir
gopherstack status [--json]            # version, uptime, per-cloud services and their state
gopherstack logs -f -n 200             # tail / follow the container or host-process log
gopherstack reset [--service S3]       # clear in-memory state on the running server
gopherstack snapshot save|load|list    # named state snapshots (same machinery as `gopherstack pod`)
gopherstack stop                       # stop and remove it (safe to repeat)
```

`start` flags: `--image`, `--tag`, `--host`, `--bind` (default `127.0.0.1`), `--port` (default `8000`), `-p/--publish` for extra ports, `--data-dir` (mounted at `/data` in the container), `--persist`, `--foreground`, `--docker-socket`, `--docker-socket-path`, `-e/--env KEY=VALUE` (or `KEY` to copy from your shell), `--timeout`, `--name`, `--state-dir`. It waits for `/_gopherstack/health` and, on failure, prints the tail of the container or process log. The pidfile, state and host log live in `$XDG_STATE_HOME/gopherstack` (`~/.local/state/gopherstack`). `--foreground` attaches (container mode): it streams the container's logs, turns SIGINT/SIGTERM into a graceful stop and exits with the container's exit code. `--docker-socket` is an explicit opt-in that mounts the host Docker socket (`--docker-socket-path`, else a unix `DOCKER_HOST`, else `/var/run/docker.sock`) at `/var/run/docker.sock` so engine-backed services can start containers. **Security:** anyone who can reach the server can then control the host Docker daemon, which is root-equivalent on the host; it is off by default and a warning is printed. Neither flag applies to `--host`. `status` lists per service the state, port, health (from `/_localstack/health`), plus uptime and heap/goroutines; `--json` carries the same. All services share one process, so port and uptime are server-wide, and no per-service resource counts are exposed by the server. On Windows, `stop` of a `--host` process sends CTRL_BREAK and falls back to terminating it, which skips the shutdown persistence flush. Commands that talk to a running server use `--endpoint` / `GOPHERSTACK_ENDPOINT` (default `http://localhost:8000`).

## MCP server for AI assistants

Gopherstack ships a built-in [Model Context Protocol](https://modelcontextprotocol.io) server so assistants can inspect and control a running instance: health and enabled services, state snapshots, resets, chaos faults, request traces, IAM denials and init hooks. Secrets (IAM secret keys, KMS material, Secrets Manager values, request headers and bodies) are never returned.

```bash
gopherstack mcp --endpoint http://localhost:8000        # stdio server for MCP clients
gopherstack serve --mcp-enabled                          # also serve streamable HTTP at /_gopherstack/mcp
```

The HTTP endpoint accepts loopback clients only unless `--mcp-token` / `GOPHERSTACK_MCP_TOKEN` is set, in which case every request needs `Authorization: Bearer <token>`. See [docs/mcp.md](docs/mcp.md) for client configuration and the tool list.

## Installation

### Docker

```bash
docker pull ghcr.io/blackbirdworks/gopherstack:latest
docker run -p 8000:8000 ghcr.io/blackbirdworks/gopherstack:latest
```

The API and the dashboard are both served on port `8000`.

### Desktop app

Each release also ships `gopherstack-desktop`, a native window around the same
server and dashboard (system webview, no browser needed). Download
`gopherstack-desktop_<version>_<OS>_<arch>` from the
[releases page](https://github.com/blackbirdworks/gopherstack/releases):

- macOS: unzip and open `Gopherstack.app`. It is ad-hoc signed, not notarized, so the first launch needs approval: System Settings → Privacy & Security → Open Anyway, or `xattr -dr com.apple.quarantine Gopherstack.app`
- Windows: unzip and run `gopherstack-desktop.exe` (needs the WebView2 runtime, preinstalled on Windows 11)
- Linux: extract and run `gopherstack-desktop` (needs GTK 4 and WebKitGTK 6.0: `libgtk-4-1` and `libwebkitgtk-6.0-4`)

It serves on port 8000 if free, otherwise a random loopback port, and accepts
the same flags and environment variables as the headless binary (e.g. `--port`).
Build it locally with `task build-desktop` (CGO and the webview libraries are
required; the headless binary stays pure Go).

### Docker Compose

```yaml
services:
  gopherstack:
    image: ghcr.io/blackbirdworks/gopherstack:latest
    ports:
      - "8000:8000"
    environment:
      - LOG_LEVEL=info
```

```bash
docker compose up -d
```

For Lambda support the container needs access to a container runtime — see
[docs/docker.md](docs/docker.md).

### Testcontainers (Go)

Gopherstack ships a reusable [Testcontainers for Go](https://golang.testcontainers.org/)
module so you can spin up the whole stack from any Go test suite:

```bash
go get github.com/blackbirdworks/gopherstack/modules/gopherstack
```

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

### As a Go library

Use the in-memory backends directly, without any HTTP server:

```go
import "github.com/blackbirdworks/gopherstack/services/dynamodb"

db := dynamodb.NewInMemoryDB()
// Use db for your application logic…
```

### From source

```bash
git clone https://github.com/blackbirdworks/gopherstack.git
cd gopherstack
go run .            # serves on :8000
```

## Features

### Web Dashboard

A built-in UI at `http://localhost:8000/dashboard` for inspecting and managing local state:

- **DynamoDB** — list tables, view details (keys, indexes, item count), run Query and Scan,
  create tables
- **S3** — list buckets, browse files with folder support, upload/download, manage
  versioning, view object metadata
- **Cloud switcher** — a top-nav dropdown scopes the dashboard to AWS, Azure or GCP;
  Azure pages cover Resource Manager, Blob Storage and Queue Storage

### Desktop App

`gopherstack-desktop` runs the full emulator and its dashboard as a native app on
macOS, Windows and Linux, with no browser or terminal needed. It's built with Wails v3
on the system webview, so it adds only a few MB over the headless binary, and it
ships with every release next to the headless `gopherstack`. See
[Desktop app](#desktop-app) for install steps.

### Configuration

Every setting has a CLI flag and (usually) an equivalent env var; a flag always wins. When
`--persist` is enabled, values loaded from the persisted config file sit between the two:
precedence is **defaults < persisted config < env vars / CLI flags**.

#### Server

| Flag | Env var | Default | Description |
|------|---------|---------|-------------|
| `--port` | `PORT` | `8000` | HTTP server port. |
| `--host` | `GOPHERSTACK_HOST` | *(empty = all interfaces)* | Listen host for the HTTP listener. A non-loopback host logs a security warning unless acknowledged. |
| `--allow-remote` | `GOPHERSTACK_ALLOW_REMOTE` | `false` | Acknowledge a non-loopback listen address (silences the warning; inside a container the warning is skipped automatically). |
| `--services` | `GOPHERSTACK_SERVICES` (LocalStack `SERVICES` also read) | *(empty = all)* | Comma-separated services to enable (case-insensitive provider names plus LocalStack aliases such as `logs`, `events`, `es`, `sfn`). Unknown names in the flag or `GOPHERSTACK_SERVICES` fail startup with the valid list; unknown names from `SERVICES` are skipped with a warning. |
| `--disable-services` | `GOPHERSTACK_DISABLE_SERVICES` | *(empty)* | Services to disable, applied after `--services`. Disabled services answer with a protocol-correct `ServiceNotEnabled` (HTTP 501) error and are hidden from the dashboard. |
| `--cors-allow-origins` | `GOPHERSTACK_CORS_ALLOW_ORIGINS`, `EXTRA_CORS_ALLOWED_ORIGINS` | *(empty)* | Origins allowed by the global CORS middleware (or `*`). Empty leaves CORS handling unchanged. |
| `--cors-allow-headers` | `GOPHERSTACK_CORS_ALLOW_HEADERS`, `EXTRA_CORS_ALLOWED_HEADERS` | *(empty)* | Extra headers allowed on preflight. |
| `--cors-expose-headers` | `GOPHERSTACK_CORS_EXPOSE_HEADERS`, `EXTRA_CORS_EXPOSE_HEADERS` | *(empty)* | Response headers exposed to browsers. |
| `--cors-allow-credentials` | `GOPHERSTACK_CORS_ALLOW_CREDENTIALS` | `false` | Send `Access-Control-Allow-Credentials: true`. |
| `--cors-disable-checks` | `GOPHERSTACK_DISABLE_CORS_CHECKS`, `DISABLE_CORS_CHECKS` | `false` | Allow any origin. |
| `--region` | `REGION` | `us-east-1` | Mock AWS region (also honors `AWS_REGION` / `AWS_DEFAULT_REGION`, see below). |
| `--log-level` | `LOG_LEVEL` | `info` | Log level: `debug`, `info`, `warn`, or `error`. |
| `--data-dir` | `GOPHERSTACK_DATA_DIR` | *(empty)* | Directory for persistence data files. Empty resolves to `~/.gopherstack/data`, or `/data` inside a container. |
| `--account-id` | `ACCOUNT_ID` | `000000000000` | Mock AWS account ID used in ARNs. |
| `--persist` | `PERSIST` | `false` | Enable snapshot-based persistence across restarts. |
| `--persist-mode` | `GOPHERSTACK_PERSIST_MODE` | `snapshot` | `snapshot`, `hybrid`, `shutdown` or `manual` (see [docs/persistence.md](docs/persistence.md)). |
| `--persist-load` | `GOPHERSTACK_PERSIST_LOAD` | `startup` | `startup`, `request` (lazy per service) or `manual` (see [docs/persistence.md](docs/persistence.md)). |
| `--s3-skip-kms-key-validation` | `GOPHERSTACK_S3_SKIP_KMS_KEY_VALIDATION` | `false` | SSE-KMS key ids unknown to KMS are rejected with `KMS.NotFoundException`, as on AWS (LocalStack skips by default); set to accept them. |
| `--persist-services` / `--persist-exclude` | `GOPHERSTACK_PERSIST_SERVICES` / `GOPHERSTACK_PERSIST_EXCLUDE` | *(all)* | Allow/deny lists of services to persist (case-insensitive). |
| `--persist-service-mode` | `GOPHERSTACK_PERSIST_SERVICE_MODE` | *(none)* | Per-service mode override, e.g. `s3=hybrid,sqs=snapshot`. |
| `--persist-flush-interval` | `GOPHERSTACK_PERSIST_FLUSH_INTERVAL` | `5s` | Hybrid flush interval (worst-case loss on a crash). |
| `--demo` | `DEMO` | `false` | Load demo data on startup. |
| `--enforce-iam` | `GOPHERSTACK_ENFORCE_IAM` (alias `ENFORCE_IAM`) | `false` | Evaluate every request against attached IAM policies. |
| `--enforce-security-groups` | `GOPHERSTACK_ENFORCE_SECURITY_GROUPS` | `false` | Admit clients to container-backed endpoints only when a security-group ingress rule allows them (RDS, EC2 docker instances, DocumentDB, MSK, ElastiCache, MemoryDB, Amazon MQ, Neptune, ECS awsvpc tasks). Egress is not enforced. See [docs/security-groups.md](docs/security-groups.md). |
| `--tls` | `TLS` | `false` | Serve over HTTPS (self-signed certificate unless `--tls-cert`/`--tls-key` are set). |
| `--tls-cert` | `TLS_CERT` | *(empty)* | Path to a TLS certificate (PEM). Requires `--tls-key`. |
| `--tls-key` | `TLS_KEY` | *(empty)* | Path to a TLS private key (PEM). |
| `--compression` | `COMPRESSION` | `on` | Runtime response compression (`on`/`off`). See [Response compression](#response-compression). |
| `--validate-sigv4` | `VALIDATE_SIGV4` | `false` | Cryptographically validate AWS SigV4 request signatures (opt-in). |
| `--sqs-disable-purge-cooldown` | `GOPHERSTACK_SQS_DISABLE_PURGE_COOLDOWN` (LocalStack `SQS_DELAY_PURGE_RETRY=0`) | `false` | Allow `PurgeQueue` within 60 seconds of the previous purge. |
| `--sqs-disable-recently-deleted-wait` | `GOPHERSTACK_SQS_DISABLE_RECENTLY_DELETED_COOLDOWN` (LocalStack `SQS_DELAY_RECENTLY_DELETED=0`) | `false` | Allow recreating a queue within 60 seconds of deleting it. |
| `--sqs-disable-message-retention` | `GOPHERSTACK_SQS_DISABLE_MESSAGE_RETENTION` (LocalStack `SQS_ENABLE_MESSAGE_RETENTION_PERIOD=0`) | `false` | Never expire messages by `MessageRetentionPeriod`. |
| `--sqs-disable-cloudwatch-metrics` | `SQS_DISABLE_CLOUDWATCH_METRICS` | `false` | Do not publish `AWS/SQS` metrics to CloudWatch. |
| `--pod-autoload` | `GOPHERSTACK_POD_AUTOLOAD` (alias `AUTO_LOAD_POD`) | *(empty)* | Comma-separated `NAME[:VERSION]` pods loaded in order at startup. |
| `--pod-merge-strategy` | `GOPHERSTACK_POD_MERGE_STRATEGY` (alias `MERGE_STRATEGY`) | `overwrite` | How autoloaded pods combine with current state: `overwrite`, `service-merge` or `account-region-merge`. |
| `--localstack-response-header` | `GOPHERSTACK_LOCALSTACK_RESPONSE_HEADER` (alias `LOCALSTACK_RESPONSE_HEADER_ENABLED`) | `false` | Add `X-Localstack: true` to every response. |
| (env only) | `GOPHERSTACK_PARTITION_STRICT` | `true` | Reject signed requests that reference an ARN from another partition than the signing region (e.g. `arn:aws-cn:` from `us-east-1`). In `cn-*`/`us-gov-*` regions `arn:aws:` ARNs are tolerated while IAM/S3 still emit them. Set `false` to disable. |
| `--sigv4-secret` | `SIGV4_SECRET` | `test` | Secret access key SigV4 validation signs against (only used with `--validate-sigv4`). |
| `--dns-addr` | `DNS_ADDR` | *(empty)* | Address for the embedded DNS server (e.g. `:10053`). Empty disables it. |
| `--dns-resolve-ip` | `DNS_RESOLVE_IP` | `127.0.0.1` | IP address synthetic hostnames resolve to. |
| `--port-range-start` | `PORT_RANGE_START` | `10000` | Start of the port range used for allocated resource endpoints. |
| `--port-range-end` | `PORT_RANGE_END` | `10100` | End (exclusive) of that port range. |
| `--iot-mqtt-port` | `IOT_MQTT_PORT` | `1883` | TCP port of the embedded IoT MQTT broker. `0` picks a free port. |
| `--iot-mqtt-auth` | `IOT_MQTT_AUTH` | `false` | Opt-in: the IoT MQTT port speaks mutual TLS, clients must present an ACTIVE registered certificate, and CONNECT/PUBLISH/SUBSCRIBE/RECEIVE are authorised by the IoT policies attached to it. |
| `--init-script` | `INIT_SCRIPTS` | *(none)* | Shell script(s) to run on startup (repeatable flag / comma-separated env var). |
| `--init-dir` | `INIT_DIR` | *(none)* | Extra init-hook root with `boot.d`/`start.d`/`ready.d`/`shutdown.d` of `.sh`/`.py` scripts, run in lexicographic order (repeatable). `/etc/localstack/init` and `/etc/gopherstack/init` are always checked. Scripts get `AWS_ENDPOINT_URL`, `AWS_DEFAULT_REGION` and dummy credentials; `.py` needs `python3`. |
| `--init-timeout` | `INIT_TIMEOUT` | `30s` | Per-script timeout for init hooks. |
| `--s3-bucket` | `S3_BUCKETS` | *(none)* | S3 bucket(s) to create on startup (repeatable flag / comma-separated env var). |
| `--janitor-timeout` | `JANITOR_TIMEOUT` | `30s` | Per-task timeout for the global janitor loop (TTL sweeps, cleaners). `0` disables it. |
| `--latency-ms` | `LATENCY_MS` | `0` | Inject random latency `[0,N)` ms per request. `0` disables it. |
| `--auto-purge-ttl` | `AUTO_PURGE_TTL` | *(disabled)* | If set, automatically reset all services on a timer (e.g. `10m`). |
| *(none)* | `GOPHERSTACK_PPROF_ADDR` | *(empty)* | Opt-in pprof debug address (e.g. `localhost:6060`) for local profiling / PGO capture. Off unless set; never enable in shared environments. |

#### AWS credentials & region overrides

Gopherstack's own AWS SDK clients read a few standard AWS environment variables, so tooling
that already exports them (e.g. `awslocal`) works without remapping. These are not flags.

| Env var | Default | Description |
|---------|---------|-------------|
| `AWS_DEFAULT_REGION` | *(unset)* | Highest-precedence region override — wins over `AWS_REGION` and `--region`/`REGION`. |
| `AWS_REGION` | *(unset)* | Region override — wins over `--region`/`REGION` but loses to `AWS_DEFAULT_REGION`. |
| `AWS_ACCESS_KEY_ID` | `dummy` | Used for Gopherstack's own internal AWS SDK clients. Incoming request credentials are never validated. |
| `AWS_SECRET_ACCESS_KEY` | `dummy` | Paired with `AWS_ACCESS_KEY_ID` for Gopherstack's internal SDK clients. Incoming request credentials are never validated. |

#### Per-service engine & provider selection

A single switch picks the default for every container-backed engine (RDS/Aurora, MSK, MQ, OpenSearch, Elasticsearch, DocumentDB, Neptune, Redshift, EKS, ElastiCache, MemoryDB, CodeBuild, Timestream for InfluxDB, ECS tasks). `auto` probes the Docker then Podman socket once at startup (5s bound, result logged) and uses real containers when one is reachable, otherwise stays on the stub; it never fails startup. Per-service flags or env vars always override it. See [docs/engines.md](docs/engines.md).

| Flag | Env var | Default | Description |
|------|---------|---------|-------------|
| `--engines` | `GOPHERSTACK_ENGINES` | `stub` | Global engine default: `stub`, `auto` (docker/podman if reachable, else stub), or `docker`. |
| `--ecs-runtime` | `GOPHERSTACK_ECS_RUNTIME` | *(follows `--engines`)* | ECS task runtime: `none` or `docker`. |
| `--elasticache-engine` | `ELASTICACHE_ENGINE` | `embedded` | ElastiCache engine mode: `embedded` (miniredis), `stub`, or `docker`. |
| `--memorydb-engine` | `MEMORYDB_ENGINE` | `embedded` | MemoryDB engine mode: `embedded` (miniredis per cluster, ACL passwords enforced via AUTH), `stub`, or `docker` (no server, as ElastiCache). |
| `--kafka-engine` | `KAFKA_ENGINE` | `stub` | MSK engine mode: `stub` (metadata only) or `docker` (real single-node `apache/kafka` broker per provisioned cluster; `KAFKA_BROKER_HOST` overrides the advertised host, default `127.0.0.1`; ports bind loopback only unless it is non-loopback, then all interfaces). |
| `--elbv2-listeners` | `ELBV2_LISTENERS` | `routed` | ELBv2 data plane: `routed` (path/host routing on the emulator port) or `real` (a local socket per listener; HTTPS/TLS terminate with ACM certificates, SNI and security-policy TLS versions; the port map is at `GET /_gopherstack/elbv2/_listeners`). |
| `--elbv2-listener-host` | `ELBV2_LISTENER_HOST` | `127.0.0.1` | Bind address for real ELBv2 listeners. Requested ports below 1024 or already taken map to a port from the shared pool. |
| `--eks-engine` | `EKS_ENGINE` | `stub` | EKS engine mode: `stub` (metadata only) or `docker` (one privileged single-node `rancher/k3s` container per cluster, tag mapped from the requested version; `DescribeCluster` returns the real endpoint and CA; clusters accept the static admin bearer token `EKS_CLUSTER_TOKEN`, default `gopherstack-eks-cluster-token`; `EKS_CLUSTER_HOST` sets the advertised host, default `127.0.0.1`, and the port binds loopback only unless it is non-loopback, which then requires `EKS_CLUSTER_TOKEN`; `EKS_K3S_IMAGE` overrides the image; `EKS_K3S_IMAGE_TAGS=1.31=v1.31.5-k3s1,...` pins the k3s tag per Kubernetes minor, and a version with no image is rejected with `InvalidParameterException`; `EKS_LB_PORTS=8080,8443` publishes those node ports on free host ports; cluster tags `gopherstack:volume-mount=host:container[:ro],...` (absolute paths, no `..`; off unless `EKS_VOLUME_MOUNT_ALLOW=/abs/prefix,...` allowlists the host paths, symlinks resolved) and `gopherstack:lb-ports` apply at `CreateCluster`, and published ports are listed at `GET /_gopherstack/eks/clusters/<name>/ports`; `EKS_KUBECONFIG` + `EKS_KUBECONFIG_CONTEXT` front an existing cluster instead of starting k3s: `DescribeCluster` returns its endpoint and CA and `DeleteCluster` never touches it). |
| `--mq-engine` | `MQ_ENGINE` | `stub` | Amazon MQ engine mode: `stub` (metadata only) or `docker` (one `rabbitmq:3.13.7-management` or `apache/activemq-classic:5.18.7` container per broker with a user; plaintext endpoints; `MQ_BROKER_HOST` overrides the advertised host, default `127.0.0.1`; ports bind loopback only unless it is non-loopback, then all interfaces). |
| `--transfer-sftp` | `TRANSFER_SFTP` | `false` | Serve a real SFTP listener per Transfer Family server. |
| `--transfer-sftp-host` | `TRANSFER_SFTP_HOST` | `127.0.0.1` | Host the Transfer Family SFTP listener binds to. |
| `--transparent-dns` | `TRANSPARENT_DNS` | `false` | Resolve `*.amazonaws.com` (and `.com.cn`, `api.aws`) to this server and serve HTTPS for them. |
| `--transparent-https-addr` | `TRANSPARENT_HTTPS_ADDR` | `:443` | HTTPS listen address for real AWS endpoint names. |
| `--transparent-http-addr` | `TRANSPARENT_HTTP_ADDR` | off | Optional plain-HTTP listener (e.g. `:80`) for `http://` AWS endpoint URLs; bind failure only warns. |
| `--transparent-dns-skip` | `TRANSPARENT_DNS_SKIP` | none | Names/zones that keep resolving upstream (e.g. `sts.amazonaws.com`). |
| `--transparent-dns-upstream` | `TRANSPARENT_DNS_UPSTREAM` | `/etc/resolv.conf` | Upstream resolvers `host:port` for non-AWS names. |
| `--transparent-container-dns` | `TRANSPARENT_CONTAINER_DNS` | `--dns-resolve-ip` unless loopback | DNS IP handed to spawned containers. |
| `--transparent-ca-dir` | `TRANSPARENT_CA_DIR` | ephemeral | Directory to persist the CA so it is trusted once. |
| `--transparent-container-ca-path` | `TRANSPARENT_CONTAINER_CA_PATH` | temp file (skipped when containerised) | CA PEM path visible to the Docker daemon; required to inject the CA when gopherstack itself runs in Docker. |
| `--rds-engine` | `RDS_ENGINE` | `stub` | RDS engine mode: `stub` (metadata only) or `docker` (real `postgres`/`mysql`/`mariadb` container per DB instance, one per Aurora cluster; Data API stays on SQLite; `RDS_DB_HOST` overrides the advertised host, default `127.0.0.1`; ports bind loopback only unless it is non-loopback, then all interfaces; Postgres runs `postgis/postgis` + pgvector with RDS-style `aws_s3`/`aws_lambda` extensions unless `RDS_POSTGRES_EXTENSIONS=false`; `RDS_SQLSERVER_ENGINE=docker` opts `sqlserver-*` into a real SQL Server container (amd64 only; accepts the Microsoft EULA); `RDS_CALLOUT_HOST`/`RDS_CALLOUT_BIND` set the address containers dial / the callout listener binds (default: container bridge gateway, never a wildcard)). |
| `--docdb-engine` | `DOCDB_ENGINE` | `stub` | DocumentDB engine mode: `stub` (metadata only) or `docker` (one plaintext `mongo` container per cluster, `mongo:4.4` for 3.6/4.0 and `mongo:5.0` for 5.0; `DOCDB_HOST` overrides the advertised host, default `127.0.0.1`; ports bind loopback only unless it is non-loopback, then all interfaces). |
| `--redshift-engine` | `REDSHIFT_ENGINE` | `stub` | Redshift engine mode: `stub` (metadata only) or `docker` (one `postgres:17` container per provisioned cluster with MasterUsername/MasterUserPassword/DBName; Endpoint reports the published `127.0.0.1` port and the Redshift Data API runs SQL against it; `REDSHIFT_HOST` overrides the advertised host; Serverless workgroups stay metadata-only). |
| `--neptune-engine` | `NEPTUNE_ENGINE` | `stub` | Neptune engine mode: `stub` (metadata only), `embedded` (in-process Gremlin, openCypher and Bolt over one persisted property graph; no container runtime) or `docker` (the same in-process graph by default; `NEPTUNE_GREMLIN=container` runs one `tinkerpop/gremlin-server` container per cluster instead, TinkerPop 3.4.13 for engine 1.0/1.1, 3.5.7 for 1.2, 3.6.8 for 1.3, 3.7.3 later). Endpoint, ReaderEndpoint, Port and member instances report the published `127.0.0.1` port, Gremlin over WebSocket/HTTP at `/gremlin`; `NEPTUNE_HOST` overrides the advertised host; no IAM auth. |
| `--opensearch-engine` | `OPENSEARCH_ENGINE` | `stub` | OpenSearch engine mode: `stub` (API-only) or `docker`. |
| `--sqs-endpoint-strategy` | `SQS_ENDPOINT_STRATEGY` | *(empty)* | Queue URL format: `standard` (`sqs.<region>.localhost.localstack.cloud:PORT/<acct>/<name>`), `domain` (`<region>.queue.localhost.localstack.cloud:PORT/...`, region omitted for us-east-1), `path` (`localhost:PORT/queue/<region>/<acct>/<name>`), `off`, `dynamic`. Empty keeps `http://localhost:PORT/<acct>/<name>` (same as `off`). |
| `--opensearch-endpoint-strategy` | `OPENSEARCH_ENDPOINT_STRATEGY` | *(empty)* | Domain endpoint format: `domain` (`<name>.<region>.<opensearch\|es>.localhost.localstack.cloud:PORT`) or `path` (`localhost:PORT/<opensearch\|es>/<region>/<name>`). `port` is rejected: there is no per-domain listener. Empty keeps `search-<name>-<acct>.<region>.es.amazonaws.com`. |
| `--graphql-endpoint-strategy` | `GRAPHQL_ENDPOINT_STRATEGY` | *(empty)* | AppSync `uris.GRAPHQL` format: `legacy` (`/graphql/<id>`), `path` (`/appsync-api/<id>/graphql`), `domain` (`<id>.appsync-api.localhost.localstack.cloud:PORT/graphql`). Empty keeps `/v1/apis/<id>/graphql`. |
| `--ecr-endpoint-strategy` | `ECR_ENDPOINT_STRATEGY` | *(empty)* | ECR `repositoryUri` host: `domain` (`<acct>.dkr.ecr.<region>.localhost.localstack.cloud:PORT`) or `off` (`localhost.localstack.cloud:PORT`). Empty keeps the AWS-style host. |
| `--elasticsearch-engine` | `ELASTICSEARCH_ENGINE` | `stub` | Elasticsearch engine mode: `stub` (API-only) or `docker`. |
| `--ec2-provider` | `EC2_PROVIDER` | `inmemory` | EC2 compute provider: `inmemory` (stub) or `docker` (launches real containers as instances). |
| `--ec2-docker-image` | `EC2_DOCKER_IMAGE` | `amazonlinux:2` | Docker image used by the EC2 docker provider. |
| `--ec2-docker-network` | `EC2_DOCKER_NETWORK` | *(empty)* | Docker network EC2-docker containers attach to. Empty uses the daemon default bridge. |
| `--ec2-docker-ssh-host-ip` | `EC2_DOCKER_SSH_HOST_IP` | `127.0.0.1` | Host IP that mapped EC2-docker SSH ports bind to. |
| `--ec2-docker-ssh-port-min` | `EC2_DOCKER_SSH_PORT_MIN` | `0` | Lower bound of the host port range for EC2-docker SSH mapping (`0` = let Docker pick). |
| `--ec2-docker-ssh-port-max` | `EC2_DOCKER_SSH_PORT_MAX` | `0` | Upper bound of that host port range. |
| `--ec2-docker-flags` | `GOPHERSTACK_EC2_DOCKER_FLAGS` | *(empty)* | Extra docker-run options for every EC2-docker container: `--privileged`, `-e K=V`, `-v src:dst[:ro]`, `--cap-add/--cap-drop X`, `-m/--memory N`, `--cpus N`, `--network NAME`, `-p [ip:]host:ctr[/proto]`. Anything else fails startup. |
| `--ec2-docker-ebs-volumes` | `GOPHERSTACK_EC2_DOCKER_EBS_VOLUMES` | `false` | Map EBS volumes to Docker named volumes mounted at `/mnt/ebs/<device>` (see [`examples/ec2-docker`](examples/ec2-docker)). |

#### Lambda & containers

The core Lambda/container-runtime flags — `LAMBDA_DOCKER_HOST`, `LAMBDA_POOL_SIZE`,
`LAMBDA_IDLE_TIMEOUT`, `CONTAINER_RUNTIME` — are documented in [Lambda
configuration](#lambda-configuration) below. A few more exist:

| Flag | Env var | Default | Description |
|------|---------|---------|-------------|
| `--lambda-max-runtimes` | `LAMBDA_MAX_RUNTIMES` | `50` | Maximum number of simultaneous per-function Lambda runtimes. |
| *(none)* | `LAMBDA_HOT_RELOAD_BUCKET` | `hot-reload` | Magic S3 bucket name whose S3Key is a local directory mounted live (Lambda hot reloading). |
| *(none)* | `LAMBDA_HOT_RELOAD_ROOTS` | home dir and temp dir | Path-list (`:` separated) of absolute directories hot-reload S3Keys must live under; others are rejected. |
| `--lambda-hot-reload-interval-ms` | `LAMBDA_HOT_RELOAD_INTERVAL_MS` | `250` | Minimum milliseconds between hot-reload change scans. |
| `--lambda-disable-hot-reload` | `LAMBDA_DISABLE_HOT_RELOAD` | `false` | Disable Lambda hot reloading from local directories. |
| `--lambda-keep-containers` | `LAMBDA_KEEP_CONTAINERS` | `false` | If true, keep Lambda containers alive for debugging. |
| *(none)* | `CONTAINER_HOST` | *(empty)* | Generic container endpoint override (e.g. a Podman socket URL). Takes precedence over runtime auto-detection. |
| `--ecs-runtime` | `GOPHERSTACK_ECS_RUNTIME` | *(unset = no-op)* | Set to `docker` to run ECS tasks as real containers; also driven by `--engines`. Unset or any other value is a no-op runner. |
| *(none)* | `GOPHERSTACK_ENABLE_LOCAL_REGISTRY` | `false` (set to `1` to enable) | Run a real embedded Docker Registry v2 backend for ECR instead of the in-memory stub. |
| *(none)* | `GOPHERSTACK_BEDROCK_BACKEND` | `mock` | Bedrock Runtime inference: `mock` returns deterministic placeholder output (not a model); `ollama` forwards Converse/InvokeModel to a local Ollama. |
| *(none)* | `GOPHERSTACK_BEDROCK_OLLAMA_URL` | `http://localhost:11434` | Ollama base URL for the `ollama` Bedrock backend. |
| *(none)* | `GOPHERSTACK_BEDROCK_MODEL_MAP` | *(empty)* | Comma list of `bedrock-id-glob=ollama-model` rules, e.g. `anthropic.claude-*=llama3.2,amazon.titan-embed-*=nomic-embed-text`; unmapped models return ResourceNotFoundException. |

#### Per-service tuning (janitor intervals & TTLs)

Most services expose a background "janitor" tick interval and TTLs for evicting stale data.
These have CLI flags too (run `gopherstack serve --help` for exact names) but are most
commonly set via env var:

| Env var | Default | Description |
|---|---|---|
| `ATHENA_JANITOR_INTERVAL` | `1m` | Athena janitor tick interval. |
| `ATHENA_EXECUTION_TTL` | `24h` | TTL for completed Athena query executions. |
| `BACKUP_JANITOR_INTERVAL` | `1m` | Backup janitor tick interval. |
| `BACKUP_JOB_TTL` | `24h` | TTL for completed Backup jobs. |
| `BATCH_JANITOR_INTERVAL` | `1m` | Batch janitor tick interval. |
| `BATCH_INACTIVE_JOB_DEF_TTL` | `24h` | TTL for inactive Batch job definitions. |
| `BATCH_COMPLETED_JOB_TTL` | `24h` | TTL for completed or failed Batch jobs. |
| `CLOUDWATCHLOGS_JANITOR_INTERVAL` | `1m` | CloudWatch Logs janitor tick interval. |
| `CLOUDWATCHLOGS_MAX_RETENTION_DAYS` | `14` | Global max log retention for groups without an explicit policy. |
| `CODEBUILD_JANITOR_INTERVAL` | `1m` | CodeBuild janitor tick interval. |
| `CODEBUILD_BUILD_TTL` | `24h` | TTL for completed CodeBuild builds. |
| `DYNAMODB_REGION` | `us-east-1` | Default region for DynamoDB (independent of `--region`). |
| `DYNAMODB_JANITOR_INTERVAL` | `500ms` | DynamoDB janitor tick interval. |
| `DYNAMODB_TTL_SWEEP_BATCH_SIZE` | `1000` | Max items checked per TTL sweep lock acquisition. |
| `DYNAMODB_CREATE_DELAY` | `0s` | Simulated CREATING→ACTIVE delay. `0` disables the lifecycle transition. |
| `DYNAMODB_ENFORCE_THROUGHPUT` | `false` | Enforce provisioned RCU/WCU limits (token bucket per table). |
| `EC2_JANITOR_INTERVAL` | `1m` | EC2 janitor tick interval. |
| `EC2_TERMINATED_TTL` | `1h` | TTL for terminated EC2 instances. |
| `EC2_CANCELLED_SPOT_TTL` | `6h` | TTL for cancelled EC2 spot requests. |
| `EMR_JANITOR_INTERVAL` | `1m` | EMR janitor tick interval. |
| `EMR_TERMINATED_TTL` | `1h` | TTL for terminated EMR clusters. |
| `FIS_JANITOR_INTERVAL` | `1m` | FIS janitor tick interval. |
| `FIS_EXPERIMENT_TTL` | `24h` | TTL for completed FIS experiments. |
| `KINESIS_JANITOR_INTERVAL` | `1m` | Kinesis janitor tick interval. |
| `KMS_JANITOR_INTERVAL` | `1m` | KMS janitor tick interval. |
| `REDSHIFTDATA_JANITOR_INTERVAL` | `1m` | Redshift Data janitor tick interval. |
| `REDSHIFTDATA_STATEMENT_TTL` | `24h` | TTL for completed Redshift Data statements. |
| `S3_REGION` | `us-east-1` | Default region for S3 (independent of `--region`). |
| `S3_JANITOR_INTERVAL` | `500ms` | S3 janitor tick interval. |
| `S3_COMPRESSION_MIN_BYTES` | `1024` | Minimum object size for gzip compression. `0` compresses everything. |
| `SES_JANITOR_INTERVAL` | `1m` | SES janitor tick interval. |
| `SES_INBOUND_ENABLED` | `false` | Run an SMTP listener that applies the active SES receipt rule set to received mail. |
| `SES_INBOUND_ADDR` | `127.0.0.1:2525` | Listen address of the SES inbound SMTP server. |
| `SES_INBOUND_MAX_SIZE` | `41943040` | Maximum SES inbound message size in bytes. |
| `SES_EMAIL_TTL` | `24h` | TTL for stored sent emails. |
| `SMTP_HOST` | unset | Relay SES/SESv2 mail to this SMTP server (`host[:port]`, port 25 default) as well as the simulator. |
| `SMTP_USER` | unset | SMTP login username; sent only over STARTTLS or to localhost. |
| `SMTP_PASS` | unset | SMTP login password. |
| `SFN_EXECUTION_RETENTION` | `24h` | How long Step Functions execution history is retained. |
| `SFN_JANITOR_INTERVAL` | `1m` | Step Functions janitor tick interval. |
| `SFN_TASK_TOKEN_TTL` | `1h` | Max lifetime of an unreceived Step Functions task token. |
| `SFN_JSONATA_MAP_OBJECT_ITEMS` | `true` | Accept an object as a JSONata Map `Items` value (the Inline Map docs say array only). |
| `SSM_JANITOR_INTERVAL` | `30s` | SSM janitor tick interval. |
| `SSM_COMMAND_TTL` | `1h` | TTL for SendCommand results (matches AWS's 1h default). |
| `STS_JANITOR_INTERVAL` | `30s` | STS janitor tick interval. |
| `XRAY_JANITOR_INTERVAL` | `1m` | X-Ray janitor tick interval. |
| `XRAY_TRACE_TTL` | `30m` | TTL for stored X-Ray traces. |

#### Lifecycle realism (transitional-state dwell)

By default resources settle instantly. Set a dwell to make Creating/Processing/Pending/InProgress states observable to SDK waiters. `--lifecycle-delay` is the default for every row below; a per-service value overrides it. All default to `0s` (CloudFront keeps its built-in `100ms`).

| Flag | Env | Effect |
|------|-----|--------|
| `--lifecycle-delay` | `GOPHERSTACK_LIFECYCLE_DELAY` | Default dwell for all rows below, `ELASTICSEARCH_PROCESSING_DELAY` and `DYNAMODB_CREATE_DELAY`. |
| `--lifecycle-opensearch-processing` | `OPENSEARCH_PROCESSING_DELAY` | OpenSearch domain `Processing=true` window. |
| `--es-processing-delay` | `ELASTICSEARCH_PROCESSING_DELAY` | Elasticsearch domain `Processing=true` window. |
| `--lifecycle-elasticache` | `ELASTICACHE_LIFECYCLE_DELAY` | ElastiCache `creating` window. |
| `--lifecycle-memorydb` | `MEMORYDB_LIFECYCLE_DELAY` | MemoryDB cluster `creating` window. |
| `--lifecycle-lambda-activation` | `LAMBDA_ACTIVATION_DELAY` | Lambda function `Pending` window. |
| `--lifecycle-lambda-provisioned-concurrency` | `LAMBDA_PROVISIONED_CONCURRENCY_DELAY` | Provisioned concurrency `IN_PROGRESS` window. |
| `--lifecycle-ecs-start` / `--lifecycle-ecs-stop` | `ECS_START_DELAY` / `ECS_STOP_DELAY` | Per-phase ECS task start/stop delay (no container runtime). |
| `--lifecycle-mediastore` | `MEDIASTORE_ACTIVATION_DELAY` | MediaStore container `CREATING`/`DELETING` window. |
| `--lifecycle-efs` | `EFS_ACTIVATION_DELAY` | EFS file system `creating` window. |
| `--lifecycle-redshift` | `REDSHIFT_ACTIVATION_DELAY` | Redshift cluster `creating` window. |
| `--lifecycle-redshiftdata` | `REDSHIFTDATA_LIFECYCLE_DELAY` | Redshift Data statement `SUBMITTED`/`STARTED` window before `FINISHED`. |
| `--lifecycle-redshiftserverless` | `REDSHIFTSERVERLESS_LIFECYCLE_DELAY` | Redshift Serverless workgroup `CREATING`/`MODIFYING`/`DELETING` and namespace `MODIFYING`/`DELETING` window. |
| `--lifecycle-ec2` | `EC2_LIFECYCLE_DELAY` | EBS volume `creating` window before `available`. |
| `--lifecycle-dax` | `DAX_LIFECYCLE_DELAY` | DAX cluster/node `creating`/`modifying`/`deleting` window (built-in `1s`). |
| `--lifecycle-pinpoint` | `PINPOINT_LIFECYCLE_DELAY` | Pinpoint export/import job `CREATED`/`PROCESSING` window before `COMPLETED`. |
| `--lifecycle-autoscaling` | `AUTOSCALING_LAUNCH_DELAY` | Auto Scaling instance `Pending` window (with an `InProgress` activity) before `InService`. |
| `--lifecycle-vpclattice` | `VPCLATTICE_LIFECYCLE_DELAY` | VPC Lattice service/target group/association `CREATE_IN_PROGRESS` (and deletes `DELETE_IN_PROGRESS`) and registered-target `INITIAL`/`DRAINING` window. |
| `--lifecycle-kafkaconnect` | `KAFKACONNECT_LIFECYCLE_DELAY` | MSK Connect connector/custom plugin transitional window (built-in `500ms` create, `300ms` delete). |
| `--lifecycle-grafana` | `GRAFANA_LIFECYCLE_DELAY` | Grafana workspace `CREATING`/`UPDATING` window (built-in `100ms`). |
| `--lifecycle-acmpca` | `ACMPCA_AUDIT_REPORT_DELAY` | ACM PCA audit report `CREATING` window. |
| `--lifecycle-accessanalyzer` | `ACCESSANALYZER_LIFECYCLE_DELAY` | IAM Access Analyzer analyzer `CREATING` window. |
| `--lifecycle-appstream` | `APPSTREAM_LIFECYCLE_DELAY` | AppStream fleet and image builder `STARTING`/`STOPPING`/`PENDING` window. |
| `--lifecycle-fis` | `FIS_LIFECYCLE_DELAY` | FIS experiment state step delay (built-in `10ms`). |
| `--lifecycle-dsql` | `DSQL_LIFECYCLE_DELAY` | Aurora DSQL cluster and stream transitional window (built-in `750ms` clusters, `500ms` streams). |
| `--lifecycle-workmail` | `WORKMAIL_LIFECYCLE_DELAY` | WorkMail transitional window. |
| `--lifecycle-personalize` | `PERSONALIZE_LIFECYCLE_DELAY` | Personalize resource CREATE PENDING/IN PROGRESS window. |
| `--lifecycle-opsworks` | `OPSWORKS_LIFECYCLE_DELAY` | OpsWorks instance transitional window. |
| `--lifecycle-outposts` | `OUTPOSTS_TRANSITION_DELAY` | Outposts transitional window (built-in 100ms). |
| `--lifecycle-support` | `SUPPORT_CASE_PROGRESS_DELAY` | Support case progress window. |
| `--lifecycle-macie2` | `MACIE2_LIFECYCLE_DELAY` | Macie one-time classification job `RUNNING` window before `COMPLETE` (built-in `5s`). |
| `--lifecycle-detective` | `DETECTIVE_LIFECYCLE_DELAY` | Detective investigation `RUNNING` window before `SUCCESSFUL`. |
| `--lifecycle-eks` | `EKS_LIFECYCLE_DELAY` | EKS transitional-state window (built-in `100ms`). |
| `--lifecycle-cloudcontrol` | `CLOUDCONTROL_OPERATION_DELAY` | Cloud Control Create/Update/DeleteResource `IN_PROGRESS` window. |
| `--lifecycle-mediapackage` | `MEDIAPACKAGE_HARVEST_JOB_DELAY` | MediaPackage harvest job `IN_PROGRESS` window (built-in `1s`). |
| `--lifecycle-mwaa` | `MWAA_LIFECYCLE_DELAY` | MWAA environment `CREATING`/`UPDATING`/`DELETING` window. |
| `--lifecycle-omics` | `OMICS_LIFECYCLE_DELAY` | HealthOmics workflow/store/share/run and import/export/activation job per-hop window. |
| `--lifecycle-resiliencehub` | `RESILIENCEHUB_LIFECYCLE_DELAY` | Resilience Hub assessment/import/resolution per-hop window (built-in `100ms`). |
| `--lifecycle-databrew` | `DATABREW_LIFECYCLE_DELAY` | DataBrew job run `STARTING`-to-`SUCCEEDED` window (built-in `100ms`). |
| `--lifecycle-mgn` | `MGN_LIFECYCLE_DELAY` | Application Migration Service replication/job/import per-hop window (built-in `100ms`). |
| `--lifecycle-forecast` | `FORECAST_LIFECYCLE_DELAY` | Forecast resource CREATE_PENDING/CREATE_IN_PROGRESS window. |
| `--lifecycle-networkmonitor` | `NETWORKMONITOR_LIFECYCLE_DELAY` | Network Monitor monitor and probe PENDING window. |
| `--lifecycle-route53-recovery` | `ROUTE53_RECOVERY_LIFECYCLE_DELAY` | Route 53 ARC control plane PENDING and PENDING_DELETION window for clusters, control panels, routing controls and safety rules. |
| `--lifecycle-arczonalshift` | `ARCZONALSHIFT_PRACTICE_RUN_DURATION` | ARC zonal shift practice run ACTIVE window; 0 keeps the built-in 30m; ignores the global delay. |
| `--lifecycle-managedblockchain` | `MANAGEDBLOCKCHAIN_LIFECYCLE_DELAY` | Managed Blockchain network/member/node CREATING window. |
| `--lifecycle-cleanrooms` | `CLEANROOMS_LIFECYCLE_DELAY` | Clean Rooms protected query and job SUBMITTED window. |
| `--lifecycle-resourcegroups` | `RESOURCEGROUPS_LIFECYCLE_DELAY` | Resource Groups transitional window. |
| `--lifecycle-serverlessrepo` | `SERVERLESSREPO_LIFECYCLE_DELAY` | Serverless Application Repository template PREPARING window. |
| `--lifecycle-codeconnections` | `CODECONNECTIONS_LIFECYCLE_DELAY` | CodeConnections connection handshake and host setup PENDING window. |
| `--lifecycle-codestarconnections` | `CODESTARCONNECTIONS_LIFECYCLE_DELAY` | CodeStar Connections connection handshake and host setup PENDING window; hosts stay PENDING when 0. |
| `--lifecycle-docdb` | `DOCDB_LIFECYCLE_DELAY` | DocumentDB cluster/instance `creating` window. |
| `--lifecycle-neptune` | `NEPTUNE_LIFECYCLE_DELAY` | Neptune cluster/instance `creating` window. |
| `--lifecycle-kafka` | `KAFKA_LIFECYCLE_DELAY` | MSK cluster `CREATING`/`UPDATING` window. |
| `--lifecycle-awsconfig` | `AWSCONFIG_LIFECYCLE_DELAY` | AWS Config conformance pack `CREATE_IN_PROGRESS` window. |
| `--lifecycle-inspector2` | `INSPECTOR2_LIFECYCLE_DELAY` | Inspector2 `ENABLING`/`DISABLING` window. |
| `--lifecycle-apprunner` | `APPRUNNER_OPERATION_DELAY` | App Runner service `OPERATION_IN_PROGRESS` window after create/update/pause/resume/deploy/delete. |
| `--lifecycle-fsx` | `FSX_LIFECYCLE_DELAY` | FSx file system/backup/volume/snapshot `CREATING` window. |
| `--lifecycle-networkmanager` | `NETWORKMANAGER_LIFECYCLE_DELAY` | Network Manager `PENDING`/`DELETING` window (built-in `100ms`). |
| `--lifecycle-organizations` | `ORGANIZATIONS_LIFECYCLE_DELAY` | Organizations `CreateAccount`/`CreateGovCloudAccount` `IN_PROGRESS` window before `SUCCEEDED`/`FAILED`. |
| `--lifecycle-account` | `ACCOUNT_LIFECYCLE_DELAY` | Account region `ENABLING`/`DISABLING` window (built-in `100ms`). |
| `--lifecycle-comprehend` | `COMPREHEND_LIFECYCLE_DELAY` | Comprehend async job and classifier/recognizer training stage window. |
| `--lifecycle-rekognition` | `REKOGNITION_LIFECYCLE_DELAY` | Rekognition stream processor, project version and dataset transitional window. |
| `--lifecycle-textract` | `TEXTRACT_ASYNC_JOB_DELAY` | Textract async job `IN_PROGRESS` window (built-in `200ms`; ignores the global delay). |
| `--lifecycle-translate` | `TRANSLATE_LIFECYCLE_DELAY` | Translate batch job and parallel data stage window. |
| `--lifecycle-polly` | `POLLY_LIFECYCLE_DELAY` | Polly speech synthesis task stage window. |
| `--lifecycle-transcribe` | `TRANSCRIBE_LIFECYCLE_DELAY` | Transcribe job, vocabulary and language model `IN_PROGRESS`/`PENDING` window. |
| `--lifecycle-iotanalytics` | `IOTANALYTICS_LIFECYCLE_DELAY` | IoT Analytics channel/datastore/dataset `CREATING` and dataset content `CREATING` window. |
| `--lifecycle-iotwireless` | `IOTWIRELESS_LIFECYCLE_DELAY` | IoT Wireless FUOTA task `FuotaSession_Waiting`/`In_FuotaSession` stage window. |
| `--lifecycle-workspaces` | `WORKSPACES_LIFECYCLE_DELAY` | WorkSpaces `PENDING` / directory `REGISTERING` window. |
| `--lifecycle-quicksight` | `QUICKSIGHT_CREATION_DELAY` | QuickSight dashboard/analysis/data source `CREATION_IN_PROGRESS` window. |
| `--lifecycle-quicksight-ingestion` | `QUICKSIGHT_INGESTION_DELAY` | QuickSight manual ingestion `RUNNING` window (built-in `1s`; ignores the global delay). |
| `--lifecycle-kinesisanalyticsv2` | `KINESISANALYTICSV2_LIFECYCLE_DELAY` | Managed Flink application `STARTING`/`STOPPING`/`UPDATING` window. |
| `--lifecycle-sagemaker` | `SAGEMAKER_LIFECYCLE_DELAY` | SageMaker transitional-state window (unset keeps the built-in dwell). |
| `--lifecycle-bedrock` | `BEDROCK_JOB_COMPLETION_DELAY` | Bedrock job `InProgress` window (unset keeps the built-in window). |
| `--lifecycle-bedrockagent` | `BEDROCKAGENT_LIFECYCLE_DELAY` | Bedrock Agents `CREATING`/`PREPARING`/`UPDATING` and ingestion window. |
| `--lifecycle-elasticbeanstalk` | `ELASTICBEANSTALK_LIFECYCLE_DELAY` | Elastic Beanstalk environment `Launching`/`Updating`/`Terminating` window. |
| `--lifecycle-route53resolver` | `ROUTE53RESOLVER_LIFECYCLE_DELAY` | Route 53 Resolver endpoint `CREATING`/`UPDATING` and rule association `CREATING` window. |
| `--lifecycle-directoryservice` | `DIRECTORYSERVICE_LIFECYCLE_DELAY` | Directory Service `Requested`/`Creating`, restore and trust/snapshot/share/setting transitions (unset keeps the built-in sub-second dwell). |
| `--lifecycle-medialive` | `MEDIALIVE_LIFECYCLE_DELAY` | MediaLive channel/multiplex `CREATING`/`STARTING`/`STOPPING`/`DELETING` window. |
| `--lifecycle-cloudfront` | `CLOUDFRONT_DEPLOY_DELAY` | CloudFront distribution `InProgress` window (default `100ms`). |
| `--lifecycle-ssm-command` / `--lifecycle-ssm-automation` | `SSM_COMMAND_EXEC_DELAY` / `SSM_AUTOMATION_EXEC_DELAY` | SendCommand / automation `InProgress` window. |
| *(none)* | `DYNAMODB_CREATE_DELAY` | DynamoDB table `CREATING` window. |

### DynamoDB

- **In-memory storage** — blazing fast tables and items
- **Secondary indexes** — full Global (GSI) and Local (LSI) secondary index support
- **Rich querying** — sort-key conditions, pagination (`Limit`, `ExclusiveStartKey`), ordering
- **Efficient scanning** — filtering and projection with DynamoDB expressions
- **Expression support** — expression attribute values and names
- **Streams** — change capture that can drive Lambda via Event Source Mappings

### S3

- **Bucket management** — full lifecycle for versioned and unversioned buckets
- **Object operations** — Get, Put, Head, List, multipart uploads
- **Versioning & tagging** — first-class object versioning and metadata tagging
- **Data integrity** — automatic checksums (CRC32, CRC32C, SHA1, SHA256)
- **Compression** — integrated gzip compression for efficient memory usage

### Lambda (Zip and Image packaging)

Gopherstack runs real Lambda functions in containers, supporting both **Zip**
(`PackageType: Zip`) and **container image** (`PackageType: Image`) packaging.

- **Zip functions** — the archive is extracted and run on the matching AWS runtime base
  image, so standard managed runtimes work unmodified: `python3.9`–`python3.13`,
  `nodejs18.x`/`20.x`/`22.x`, `java11`/`17`/`21`, `dotnet8`/`dotnet9`, `ruby3.2`/`3.3`, and
  `provided.al2`/`provided.al2023`
- **Image functions** — provide an `ImageUri` (an AWS base image or your own)
- **Lambda Runtime API** — full implementation, so standard AWS base images work as-is
- **Invocation modes** — `RequestResponse` (sync) and `Event` (async / fire-and-forget)
- **Warm container pool** — configurable per-function pool to cut cold starts
- **Reserved concurrency** — enforced for both sync and async calls
- **Async realism** — AWS-realistic retry semantics and dead-letter queues (via SNS/SQS)
- **Environment variables** — passed straight through to the container

> [!IMPORTANT]
> Both packaging modes require a running Docker (or Podman) daemon to execute invocations.
> S3-based code delivery and direct Go binary execution on the host are not supported.
> **All other Gopherstack services work without Docker.**

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
| `--lambda-container-runtime` | `CONTAINER_RUNTIME` | `docker` | Container runtime: `docker`, `podman`, or `auto` |

#### Using Podman

[Podman](https://podman.io/) works as a drop-in replacement for Docker via its
Docker-compatible API socket.

```bash
# Enable the Podman socket for your user (rootless, Linux)
systemctl --user enable --now podman.socket

# Point Gopherstack at Podman
export CONTAINER_RUNTIME=podman

# Optional: override the socket path
export CONTAINER_HOST=unix://${XDG_RUNTIME_DIR}/podman/podman.sock
```

**Rootless networking note:** in rootless Podman the Docker bridge (`172.17.0.1`) is not
available — use the host's routable IP or `host.containers.internal`:

```bash
export LAMBDA_DOCKER_HOST=host.containers.internal
```

Set `CONTAINER_RUNTIME=auto` to probe Docker first, then Podman, and use whichever socket is
reachable.

### Event Source Mappings (ESM)

Automatically trigger Lambda functions from **DynamoDB Streams** — Gopherstack manages the
polling and invocation for you once an ESM exists. Respects batch size, starting position
(`TRIM_HORIZON`, `LATEST`), and enable/disable state.

### EventBridge Scheduler & Pipes

- **Scheduler** — recurring or one-time scheduled tasks that trigger AWS targets (Lambda,
  SQS, SNS, and more)
- **Pipes** — point-to-point integrations from sources (SQS) to targets (Lambda, Step
  Functions) with optional filtering and enrichment

### Performance & Scalability

- **O(1) ARN indexing** — tagging and resource lookup use a centralized ARN index, so
  operations stay fast with thousands of resources
- **Memory optimization** — struct field alignment and efficient data structures keep the
  footprint small
- **Profile-guided optimization** — the binary ships with a PGO profile captured from real
  workloads (see [PGO](#profile-guided-optimization-pgo))

### Response compression

Responses are compressed at runtime (never at build time) when the client asks via
`Accept-Encoding`; the server prefers `zstd`, then `br`, then `gzip` when weights tie.

- **Dashboard** — assets and JSON API responses of at least 1 KiB are compressed; embedded
  assets are compressed once per coding and cached in memory. Streams (SSE, Connect/gRPC),
  `Range`, `HEAD` and already-encoded responses are never touched.
- **AWS APIs** — SDK clients send `Accept-Encoding: identity`, so nothing changes for them.
  Only DynamoDB answers `gzip` to clients that opt in (`EnableAcceptEncodingGzip`), with
  `X-Amz-Crc32` computed over the compressed bytes like the real service. No other AWS
  service, S3 object body or proxied Lambda/API Gateway response is compressed.
- **Opt out** — `--compression off` (or `COMPRESSION=off`) disables all of it.

## Examples

Complete, runnable examples live in [`examples/`](examples/) — each ships a
`docker-compose.yml` and a `demo.sh` so you can run it end to end.

| Example | What it demonstrates | Requires |
|---------|----------------------|----------|
| [`apigw-websocket-chat`](examples/apigw-websocket-chat) | Real-time chat over API Gateway WebSockets with a Node.js Lambda backend | OpenTofu, Node.js (`wscat`) |
| [`cognito-api-auth`](examples/cognito-api-auth) | Securing API Gateway with a Cognito User Pool authorizer and JWTs | OpenTofu, Bash |
| [`ddb-lambda-chain`](examples/ddb-lambda-chain) | 3-table event pipeline where DynamoDB Streams trigger a Go Lambda | Go, Bash |
| [`ec2-docker`](examples/ec2-docker) | Provisioning EC2 instances and running SSH commands against them | Bash (`ssh`, `dig`) |
| [`elasticache-valkey`](examples/elasticache-valkey) | ElastiCache Valkey cluster reachable via embedded DNS | Bash (`valkey-cli`/`redis-cli`) |
| [`eventbridge-sqs`](examples/eventbridge-sqs) | Routing custom EventBridge events to an SQS queue | OpenTofu |
| [`kinesis-lambda-aggregator`](examples/kinesis-lambda-aggregator) | Kinesis stream invoking a Node.js Lambda that aggregates into DynamoDB | OpenTofu, Bash |
| [`s3-access-logs`](examples/s3-access-logs) | Configuring S3 server access logging | OpenTofu |
| [`s3-lambda-processor`](examples/s3-lambda-processor) | S3 uploads triggering a Go Lambda to process the object | OpenTofu, Go, Bash |
| [`sns-sqs-fanout`](examples/sns-sqs-fanout) | Pub/sub fan-out from SNS to multiple SQS queues | OpenTofu |
| [`stepfunctions-order-workflow`](examples/stepfunctions-order-workflow) | Order-processing state machine orchestrated by Step Functions | OpenTofu, Bash |

Running one:

```bash
cd examples/kinesis-lambda-aggregator
docker compose up -d
./demo.sh
```

See [`examples/README.md`](examples/README.md) for prerequisites and teardown.

<!-- BEGIN GENERATED SERVICES -->
## Services

Every service links to its own page with a coverage breakdown — audited operations, known gaps, deferred items and resource-leak status — generated from that service's `PARITY.md` audit.

**Parity** is the overall grade recorded by that service's most recent audit. **PARITY Entries** is the number of `ops:` entries audited in that service's PARITY.md -- a hand-grouped audit unit, not a raw operation count (one entry can name more than one real operation); a dash means the service is tracked by feature family instead. Run `task docs` to refresh this table.

### Compute

| Service | Parity | PARITY Entries | Notes |
|---|---|---|---|
| [App Runner](services/apprunner/README.md) | A | 37 | clean |
| [Auto Scaling](services/autoscaling/README.md) | A | 66 | 1 gap; 2 structural gaps |
| [Batch](services/batch/README.md) | A | 48 | 3 structural gaps |
| [EC2](services/ec2/README.md) | A | — | 22 families; 5 gaps; 18 structural gaps; 5 deferred |
| [Elastic Beanstalk](services/elasticbeanstalk/README.md) | A | 47 | 4 structural gaps |
| [Lambda](services/lambda/README.md) | A | — | 11 families; 1 gap; 11 structural gaps |

### Containers

| Service | Parity | PARITY Entries | Notes |
|---|---|---|---|
| [ECR](services/ecr/README.md) | A | 58 | 2 structural gaps; 2 deferred |
| [ECS](services/ecs/README.md) | A | 65 | 5 gaps; 5 structural gaps |
| [EKS](services/eks/README.md) | A | 71 | 1 gap; 5 structural gaps; 1 deferred |

### Storage

| Service | Parity | PARITY Entries | Notes |
|---|---|---|---|
| [Backup](services/backup/README.md) | A | 66 | 2 gaps; 3 structural gaps |
| [Data Lifecycle Manager](services/dlm/README.md) | A | 8 | 2 gaps |
| [EFS](services/efs/README.md) | A | 31 | 4 gaps; 1 deferred |
| [FSx](services/fsx/README.md) | A | — | 13 families; 2 structural gaps |
| [S3](services/s3/README.md) | A | 26 | 3 gaps; 4 structural gaps |
| [S3 Control](services/s3control/README.md) | A | 44 | 3 gaps; 6 structural gaps; 2 deferred |
| [S3 Files](services/s3files/README.md) | B | 21 | 7 gaps; 1 structural gap |
| [S3 Glacier](services/glacier/README.md) | A | 33 | clean |
| [S3 Tables](services/s3tables/README.md) | A | 49 | clean |

### Database

| Service | Parity | PARITY Entries | Notes |
|---|---|---|---|
| [DAX](services/dax/README.md) | A | 21 | 1 structural gap; 1 deferred |
| [DocumentDB](services/docdb/README.md) | A | 55 | 2 gaps; 5 structural gaps; 1 deferred |
| [DynamoDB](services/dynamodb/README.md) | A | — | 15 families; 2 gaps; 3 structural gaps; 2 deferred |
| [DynamoDB Streams](services/dynamodbstreams/README.md) | A | 4 | clean |
| [ElastiCache](services/elasticache/README.md) | A | 75 | 2 gaps; 4 structural gaps; 1 deferred |
| [MemoryDB](services/memorydb/README.md) | A | 45 | 3 gaps; 2 structural gaps; 3 deferred |
| [Neptune](services/neptune/README.md) | A | — | 13 families; 6 gaps; 2 structural gaps; 2 deferred |
| [QLDB](services/qldb/README.md) | Removed | — | removed service |
| [QLDB Session](services/qldbsession/README.md) | Removed | — | removed service |
| [RDS](services/rds/README.md) | A | 52 | 7 gaps; 7 structural gaps |
| [RDS Data](services/rdsdata/README.md) | A | 7 | 4 structural gaps |
| [Redshift](services/redshift/README.md) | A | 9 | 5 gaps; 5 structural gaps |
| [Redshift Data](services/redshiftdata/README.md) | A | 12 | 4 structural gaps; 1 deferred |
| [Timestream Query](services/timestreamquery/README.md) | A | 12 | 2 structural gaps; 1 deferred |
| [Timestream Write](services/timestreamwrite/README.md) | A | 19 | 1 structural gap |

### Networking & Content Delivery

| Service | Parity | PARITY Entries | Notes |
|---|---|---|---|
| [API Gateway](services/apigateway/README.md) | A | 123 | 2 gaps; 1 structural gap; 2 deferred |
| [API Gateway Management API](services/apigatewaymanagementapi/README.md) | A | 3 | 2 deferred |
| [API Gateway v2](services/apigatewayv2/README.md) | A | 77 | 6 gaps; 1 structural gap |
| [ARC Zonal Shift](services/arczonalshift/README.md) | A | 15 | 1 structural gap |
| [App Mesh](services/appmesh/README.md) | A | 38 | 1 structural gap |
| [Cloud Map](services/servicediscovery/README.md) | A | 30 | 4 structural gaps; 1 deferred |
| [CloudFront](services/cloudfront/README.md) | A | 60 | 4 gaps; 4 structural gaps; 4 deferred |
| [CloudWatch Network Monitor](services/networkmonitor/README.md) | A | 12 | 1 deferred |
| [ELB (Classic)](services/elb/README.md) | A | 29 | 1 gap; 1 deferred |
| [ELBv2](services/elbv2/README.md) | A | 51 | 2 gaps; 2 structural gaps; 2 deferred |
| [Route 53](services/route53/README.md) | A | 67 | 3 deferred |
| [Route 53 Recovery Cluster](services/route53recoverycluster/README.md) | A | 4 | clean |
| [Route 53 Recovery Control Config](services/route53recoverycontrolconfig/README.md) | A | 25 | 1 structural gap |
| [Route 53 Recovery Readiness](services/route53recoveryreadiness/README.md) | A | 32 | 1 structural gap |
| [Route 53 Resolver](services/route53resolver/README.md) | A | 72 | 4 gaps; 1 structural gap; 1 deferred |
| [VPC Lattice](services/vpclattice/README.md) | A | 73 | 1 gap; 3 structural gaps |

### Messaging & Integration

| Service | Parity | PARITY Entries | Notes |
|---|---|---|---|
| [Amazon MQ](services/mq/README.md) | A | 25 | 2 structural gaps; 1 deferred |
| [AppSync](services/appsync/README.md) | A | 74 | 6 gaps; 1 structural gap; 2 deferred |
| [EventBridge](services/eventbridge/README.md) | A | 66 | 2 gaps; 3 structural gaps; 2 deferred |
| [EventBridge Pipes](services/pipes/README.md) | A | 10 | 2 structural gaps |
| [EventBridge Scheduler](services/scheduler/README.md) | A | 12 | 1 gap |
| [Pinpoint](services/pinpoint/README.md) | A | 51 | 2 structural gaps; 3 deferred |
| [SES](services/ses/README.md) | A | 71 | 2 gaps; 3 structural gaps; 1 deferred |
| [SES v2](services/sesv2/README.md) | A | 116 | 2 gaps; 2 structural gaps |
| [SNS](services/sns/README.md) | A | 34 | 1 structural gap; 1 deferred |
| [SQS](services/sqs/README.md) | A | 20 | 2 gaps; 1 structural gap |
| [SWF](services/swf/README.md) | A | 39 | clean |
| [Step Functions](services/stepfunctions/README.md) | A | 37 | 4 gaps; 3 structural gaps |
| [WorkMail](services/workmail/README.md) | A | 92 | 2 gaps; 3 structural gaps |

### Analytics

| Service | Parity | PARITY Entries | Notes |
|---|---|---|---|
| [Athena](services/athena/README.md) | A | 27 | 10 gaps; 6 structural gaps; 1 deferred |
| [Clean Rooms](services/cleanrooms/README.md) | A | — | 18 families; 5 structural gaps |
| [EMR](services/emr/README.md) | A | 65 | 7 structural gaps |
| [EMR Serverless](services/emrserverless/README.md) | A | 22 | 1 structural gap |
| [Elasticsearch](services/elasticsearch/README.md) | A | 51 | 1 structural gap |
| [Glue](services/glue/README.md) | A | 59 | 5 gaps; 5 structural gaps; 2 deferred |
| [Glue DataBrew](services/databrew/README.md) | A | 44 | 1 structural gap |
| [Kinesis](services/kinesis/README.md) | A | 39 | 1 gap; 5 structural gaps |
| [Kinesis Analytics](services/kinesisanalytics/README.md) | A | 20 | clean |
| [Kinesis Analytics v2](services/kinesisanalyticsv2/README.md) | A | 33 | 3 gaps; 1 structural gap; 1 deferred |
| [Kinesis Data Firehose](services/firehose/README.md) | A | 12 | 4 structural gaps |
| [Lake Formation](services/lakeformation/README.md) | A | 61 | 1 gap; 2 structural gaps |
| [Managed Streaming for Kafka](services/kafka/README.md) | A | 64 | 3 gaps; 1 structural gap |
| [Managed Workflows for Apache Airflow](services/mwaa/README.md) | A | 12 | 4 gaps; 1 structural gap; 1 deferred |
| [OpenSearch](services/opensearch/README.md) | A | 19 | 1 gap; 1 structural gap |
| [QuickSight](services/quicksight/README.md) | A | 81 | 3 gaps; 7 structural gaps |

### Security

| Service | Parity | PARITY Entries | Notes |
|---|---|---|---|
| [ACM](services/acm/README.md) | A | 39 | 1 gap; 4 structural gaps; 1 deferred |
| [ACM PCA](services/acmpca/README.md) | A | 23 | 1 structural gap |
| [Detective](services/detective/README.md) | A | 29 | 4 structural gaps; 2 deferred |
| [GuardDuty](services/guardduty/README.md) | A | 66 | 1 gap; 3 structural gaps |
| [Inspector](services/inspector2/README.md) | A | 13 | 4 structural gaps; 1 deferred |
| [KMS](services/kms/README.md) | A | 54 | 2 gaps; 1 structural gap; 1 deferred |
| [Macie](services/macie2/README.md) | A | 81 | clean |
| [Secrets Manager](services/secretsmanager/README.md) | A | 24 | 1 structural gap; 2 deferred |
| [Security Hub](services/securityhub/README.md) | A | 120 | 4 gaps; 4 structural gaps |
| [Shield](services/shield/README.md) | A | 36 | 1 structural gap; 1 deferred |
| [Verified Permissions](services/verifiedpermissions/README.md) | A | 34 | 2 gaps; 1 structural gap |
| [WAF](services/waf/README.md) | A | 4 | 3 structural gaps |
| [WAFv2](services/wafv2/README.md) | A | 59 | 2 gaps; 2 structural gaps |

### Identity & Access

| Service | Parity | PARITY Entries | Notes |
|---|---|---|---|
| [Cognito Identity](services/cognitoidentity/README.md) | A | 23 | 1 gap; 4 deferred |
| [Cognito Identity Provider](services/cognitoidp/README.md) | A | 71 | 6 gaps; 3 structural gaps |
| [Directory Service](services/directoryservice/README.md) | A | 80 | 1 gap; 2 structural gaps; 1 deferred |
| [IAM](services/iam/README.md) | A | 38 | 2 gaps; 4 structural gaps |
| [IAM Access Analyzer](services/accessanalyzer/README.md) | A | 39 | 1 gap; 5 structural gaps; 1 deferred |
| [IAM Identity Center (SSO)](services/ssoadmin/README.md) | A | 56 | 1 gap |
| [IAM Identity Center OIDC](services/ssooidc/README.md) | B | 4 | 1 gap; 1 structural gap |
| [IAM Identity Center Portal](services/sso/README.md) | B | 4 | 2 gaps; 1 structural gap |
| [IAM Roles Anywhere](services/rolesanywhere/README.md) | A | 30 | 1 gap; 2 structural gaps |
| [Identity Store](services/identitystore/README.md) | A | 19 | 2 gaps; 1 deferred |
| [STS](services/sts/README.md) | A | 11 | 1 structural gap; 1 deferred |
| [Sign-In](services/signin/README.md) | B | 11 | 7 gaps; 1 structural gap |

### Management & Governance

| Service | Parity | PARITY Entries | Notes |
|---|---|---|---|
| [Account](services/account/README.md) | A | 18 | 3 structural gaps; 1 deferred |
| [AppConfig](services/appconfig/README.md) | A | 56 | 2 gaps; 1 structural gap |
| [AppConfig Data](services/appconfigdata/README.md) | A | 2 | clean |
| [Application Auto Scaling](services/applicationautoscaling/README.md) | A | 14 | 1 gap; 2 structural gaps; 2 deferred |
| [Cloud Control API](services/cloudcontrol/README.md) | A | 8 | 1 gap; 3 structural gaps |
| [CloudFormation](services/cloudformation/README.md) | A | 73 | 6 gaps; 2 structural gaps |
| [CloudTrail](services/cloudtrail/README.md) | A | 60 | 2 gaps; 4 structural gaps |
| [CloudWatch](services/cloudwatch/README.md) | A | 55 | 1 structural gap; 16 deferred |
| [CloudWatch Logs](services/cloudwatchlogs/README.md) | A | 86 | 5 gaps; 9 structural gaps |
| [Config](services/awsconfig/README.md) | A | 102 | 4 gaps; 2 structural gaps; 1 deferred |
| [Cost Explorer](services/ce/README.md) | A | 37 | 1 gap; 5 structural gaps; 2 deferred |
| [Fault Injection Simulator](services/fis/README.md) | A | 26 | 2 structural gaps; 1 deferred |
| [OpsWorks](services/opsworks/README.md) | A | 32 | 1 gap; 2 structural gaps; 1 deferred |
| [Organizations](services/organizations/README.md) | A | 63 | 2 structural gaps |
| [Resource Access Manager](services/ram/README.md) | A | 36 | 2 gaps; 1 structural gap; 2 deferred |
| [Resource Groups](services/resourcegroups/README.md) | A | 23 | 2 structural gaps |
| [Resource Groups Tagging API](services/resourcegroupstaggingapi/README.md) | A | 9 | 2 structural gaps; 1 deferred |
| [Systems Manager](services/ssm/README.md) | A | 105 | 5 gaps; 14 structural gaps |

### Developer Tools

| Service | Parity | PARITY Entries | Notes |
|---|---|---|---|
| [Amplify](services/amplify/README.md) | A | 37 | 3 structural gaps |
| [CodeArtifact](services/codeartifact/README.md) | A | 48 | 3 gaps; 3 structural gaps |
| [CodeBuild](services/codebuild/README.md) | A | 59 | 6 gaps; 2 structural gaps; 1 deferred |
| [CodeCommit](services/codecommit/README.md) | A | 80 | 1 gap; 1 structural gap |
| [CodeConnections](services/codeconnections/README.md) | A | 27 | 1 gap |
| [CodeDeploy](services/codedeploy/README.md) | A | 47 | 3 structural gaps; 1 deferred |
| [CodePipeline](services/codepipeline/README.md) | A | 22 | 6 gaps; 7 structural gaps; 1 deferred |
| [CodeStar Connections](services/codestarconnections/README.md) | A | 27 | 1 gap; 2 structural gaps |
| [Serverless Application Repository](services/serverlessrepo/README.md) | A | 14 | clean |
| [X-Ray](services/xray/README.md) | A | 38 | 2 gaps; 6 structural gaps; 1 deferred |

### Machine Learning

| Service | Parity | PARITY Entries | Notes |
|---|---|---|---|
| [Bedrock](services/bedrock/README.md) | A | 80 | 1 gap; 3 structural gaps |
| [Bedrock Agent](services/bedrockagent/README.md) | A | 77 | 2 gaps; 3 structural gaps; 2 deferred |
| [Bedrock Runtime](services/bedrockruntime/README.md) | A | 11 | 4 gaps; 5 structural gaps |
| [Comprehend](services/comprehend/README.md) | A | 28 | 1 gap; 1 structural gap; 1 deferred |
| [Forecast](services/forecast/README.md) | A | 21 | 1 gap |
| [Personalize](services/personalize/README.md) | A | 74 | clean |
| [Polly](services/polly/README.md) | A | 10 | clean |
| [Rekognition](services/rekognition/README.md) | A | 50 | 3 structural gaps; 3 deferred |
| [SageMaker](services/sagemaker/README.md) | A | 70 | 1 gap; 19 structural gaps |
| [SageMaker Runtime](services/sagemakerruntime/README.md) | A | 3 | 1 gap; 1 structural gap |
| [Textract](services/textract/README.md) | A | 25 | 2 structural gaps; 1 deferred |
| [Transcribe](services/transcribe/README.md) | A | 44 | 2 structural gaps |
| [Translate](services/translate/README.md) | A | 19 | 1 gap; 2 structural gaps |

### Media

| Service | Parity | PARITY Entries | Notes |
|---|---|---|---|
| [MediaConvert](services/mediaconvert/README.md) | A | 34 | 2 structural gaps; 1 deferred |
| [MediaLive](services/medialive/README.md) | A | — | 26 families; 2 gaps; 1 structural gap |
| [MediaPackage](services/mediapackage/README.md) | A | 19 | 1 deferred |
| [MediaStore](services/mediastore/README.md) | A | 21 | 1 gap |
| [MediaStore Data](services/mediastoredata/README.md) | A | 5 | 1 gap; 1 structural gap; 1 deferred |
| [MediaTailor](services/mediatailor/README.md) | A | 48 | 3 gaps; 2 structural gaps |

### IoT

| Service | Parity | PARITY Entries | Notes |
|---|---|---|---|
| [IoT Analytics](services/iotanalytics/README.md) | A | 34 | 1 gap; 1 structural gap |
| [IoT Core](services/iot/README.md) | A | 88 | 10 gaps; 4 structural gaps |
| [IoT Data Plane](services/iotdataplane/README.md) | A | 11 | 1 structural gap; 1 deferred |
| [IoT Wireless](services/iotwireless/README.md) | A | 21 | 2 structural gaps |

### Migration & Transfer

| Service | Parity | PARITY Entries | Notes |
|---|---|---|---|
| [DataSync](services/datasync/README.md) | A | 53 | 2 gaps; 3 structural gaps; 1 deferred |
| [Database Migration Service](services/dms/README.md) | A | 97 | 3 gaps; 3 structural gaps |
| [Transfer Family](services/transfer/README.md) | A | — | 20 families; 4 structural gaps |

### Azure

| Service | Parity | PARITY Entries | Notes |
|---|---|---|---|
| [Azure App Configuration](services/azureappconfig/README.md) | B | 14 | 13 gaps |
| [Azure Blob Storage](services/azureblob/README.md) | C | 29 | 5 gaps; 3 deferred |
| [Azure Cache for Redis](services/azureredis/README.md) | B | 11 | 13 gaps |
| [Azure Container Registry](services/azurecontainerregistry/README.md) | B | 24 | 10 gaps |
| [Azure Cosmos DB](services/cosmosdb/README.md) | A | 35 | clean |
| [Azure Event Grid](services/azureeventgrid/README.md) | B | 9 | 15 gaps |
| [Azure Event Hubs](services/azureeventhubs/README.md) | B | 15 | 9 gaps |
| [Azure Files](services/azurefile/README.md) | B | 38 | 11 gaps |
| [Azure Functions](services/azurefunctions/README.md) | B | 6 | 12 gaps |
| [Azure Key Vault](services/azurekeyvault/README.md) | B | 15 | 4 gaps |
| [Azure Monitor Logs](services/azuremonitor/README.md) | B | 20 | 5 gaps |
| [Azure Queue Storage](services/azurequeue/README.md) | C | 13 | 6 gaps; 1 deferred |
| [Azure SQL Database](services/azuresql/README.md) | B | 14 | 11 gaps |
| [Azure Service Bus](services/azureservicebus/README.md) | B | 25 | 3 gaps; 4 deferred |
| [Azure Table Storage](services/azuretable/README.md) | C | 14 | 4 gaps; 3 deferred |
| [Microsoft Graph](services/azuregraph/README.md) | B | 28 | 10 gaps |

### GCP

| Service | Parity | PARITY Entries | Notes |
|---|---|---|---|
| [API Gateway](services/gcpapigateway/README.md) | B | 25 | 7 gaps |
| [Artifact Registry](services/gcpartifactregistry/README.md) | B | 40 | 13 gaps |
| [BigQuery](services/gcpbigquery/README.md) | B | 23 | 8 gaps |
| [Cloud Bigtable](services/gcpbigtable/README.md) | B | 43 | 8 gaps |
| [Cloud Build](services/gcpcloudbuild/README.md) | B | 20 | 11 gaps |
| [Cloud DNS](services/gcpdns/README.md) | B | — | 8 families; 5 gaps |
| [Cloud Datastore](services/gcpdatastore/README.md) | B | 13 | 5 gaps |
| [Cloud Functions](services/gcpfunctions/README.md) | B | 8 | 8 gaps |
| [Cloud IAM](services/gcpiam/README.md) | B | 31 | 5 gaps |
| [Cloud Logging](services/gcplogging/README.md) | B | 40 | 7 gaps |
| [Cloud Monitoring](services/gcpmonitoring/README.md) | B | 53 | 11 gaps |
| [Cloud Run](services/gcprun/README.md) | B | 20 | 5 gaps |
| [Cloud SQL](services/gcpsql/README.md) | B | 42 | 7 gaps |
| [Cloud Scheduler](services/gcpscheduler/README.md) | B | 10 | 4 gaps |
| [Cloud Spanner](services/gcpspanner/README.md) | B | 18 | 6 gaps |
| [Cloud Tasks](services/gcpcloudtasks/README.md) | B | 20 | 3 gaps |
| [Compute Engine](services/gcpcompute/README.md) | B | — | 8 families; 10 gaps |
| [Eventarc](services/gcpeventarc/README.md) | B | 24 | 10 gaps |
| [Google Cloud Batch](services/gcpbatch/README.md) | B | 14 | 9 gaps |
| [Google Cloud Firestore](services/gcpfirestore/README.md) | B | 15 | 4 gaps |
| [Google Cloud KMS](services/gcpkms/README.md) | B | 35 | 4 gaps |
| [Google Cloud Pub/Sub](services/gcppubsub/README.md) | B | 38 | 6 gaps |
| [Google Cloud Resource Manager and Service Usage](services/gcpresourcemanager/README.md) | B | 39 | 9 gaps |
| [Google Cloud Storage](services/gcpstorage/README.md) | B | 28 | 8 gaps |
| [Google Secret Manager](services/gcpsecretmanager/README.md) | B | 18 | 5 gaps |
| [Identity Platform](services/gcpidentitytoolkit/README.md) | B | 1 | 6 gaps |
| [Kubernetes Engine](services/gcpcontainer/README.md) | B | — | 5 families; 5 gaps |
| [Memorystore for Redis](services/gcpredis/README.md) | B | 14 | 7 gaps |
| [Workflows](services/gcpworkflows/README.md) | B | 13 | 8 gaps |

### OCI

| Service | Parity | PARITY Entries | Notes |
|---|---|---|---|
| [OCI API Gateway](services/ociapigateway/README.md) | B | 22 | 10 gaps |
| [OCI Artifacts and Container Registry](services/ociartifacts/README.md) | B | 37 | 7 gaps |
| [OCI Container Engine for Kubernetes](services/ocicontainerengine/README.md) | B | 20 | 9 gaps |
| [OCI Container Instances](services/ocicontainerinstances/README.md) | B | 18 | 7 gaps |
| [OCI Core Services](services/ocicore/README.md) | B | 135 | 11 gaps |
| [OCI Events](services/ocievents/README.md) | B | 6 | 6 gaps |
| [OCI Functions](services/ocifunctions/README.md) | B | 16 | 9 gaps |
| [OCI Identity](services/ociidentity/README.md) | B | 70 | 9 gaps |
| [OCI Load Balancer](services/ociloadbalancer/README.md) | B | 61 | 8 gaps |
| [OCI Logging](services/ocilogging/README.md) | B | 26 | 8 gaps |
| [OCI Monitoring](services/ocimonitoring/README.md) | B | 11 | 9 gaps |
| [OCI MySQL HeatWave Database Service](services/ocimysql/README.md) | B | 32 | 9 gaps |
| [OCI NoSQL Database Cloud Service](services/ocinosql/README.md) | B | 21 | 9 gaps |
| [OCI Notifications](services/ocinotifications/README.md) | B | 16 | 7 gaps |
| [OCI Object Storage](services/ociobjectstorage/README.md) | B | 45 | 11 gaps |
| [OCI Queue](services/ociqueue/README.md) | B | 19 | 9 gaps |
| [OCI Streaming](services/ocistreaming/README.md) | B | 30 | 9 gaps |
| [OCI Vault](services/ocivault/README.md) | B | 45 | 11 gaps |

### Other

| Service | Parity | PARITY Entries | Notes |
|---|---|---|---|
| [Amp](services/amp/README.md) | B | 44 | 9 gaps; 2 structural gaps |
| [AppStream 2.0](services/appstream/README.md) | A | 44 | 5 structural gaps |
| [Appintegrations](services/appintegrations/README.md) | B | 23 | 3 gaps; 1 structural gap |
| [Azurearm](services/azurearm/README.md) | B | 20 | 68 gaps; 3 deferred |
| [Azurestoragevhost](services/azurestoragevhost/README.md) | B | 2 | 2 gaps; 1 deferred |
| [Bcmdataexports](services/bcmdataexports/README.md) | B | 12 | 6 gaps |
| [Bedrockagentcore](services/bedrockagentcore/README.md) | B | 30 | 11 gaps; 2 structural gaps |
| [Bedrockagentcorecontrol](services/bedrockagentcorecontrol/README.md) | B | 171 | 12 gaps; 2 structural gaps |
| [Budgets](services/budgets/README.md) | B | 26 | 8 gaps |
| [Cloudfrontkeyvaluestore](services/cloudfrontkeyvaluestore/README.md) | A | 6 | 2 structural gaps |
| [Cloudhsmv2](services/cloudhsmv2/README.md) | B | 18 | 7 gaps; 3 structural gaps |
| [Codegurureviewer](services/codegurureviewer/README.md) | B | 14 | 5 gaps; 2 structural gaps |
| [Connect](services/connect/README.md) | B | 312 | 21 gaps; 2 structural gaps |
| [Cur](services/cur/README.md) | B | 7 | 6 gaps |
| [Directconnect](services/directconnect/README.md) | A | 73 | 2 gaps; 8 structural gaps; 1 deferred |
| [Dsql](services/dsql/README.md) | A | 16 | 3 structural gaps |
| [Ecrpublic](services/ecrpublic/README.md) | A | 23 | 3 structural gaps |
| [Globalaccelerator](services/globalaccelerator/README.md) | B | 56 | 4 gaps; 2 structural gaps |
| [Grafana](services/grafana/README.md) | A | 25 | 1 structural gap |
| [HealthOmics](services/omics/README.md) | A | — | 25 families; 5 structural gaps; 1 deferred |
| [Kafkaconnect](services/kafkaconnect/README.md) | A | 19 | 3 gaps; 3 structural gaps |
| [Kinesisvideo](services/kinesisvideo/README.md) | A | 32 | 3 structural gaps |
| [Lightsail](services/lightsail/README.md) | A | — | 29 families; 1 gap; 6 structural gaps; 2 deferred |
| [Managed Blockchain](services/managedblockchain/README.md) | A | 27 | 3 structural gaps |
| [Marketplaceentitlementservice](services/marketplaceentitlementservice/README.md) | B | 1 | 3 gaps; 1 structural gap |
| [Marketplacemetering](services/marketplacemetering/README.md) | B | 4 | 5 gaps; 2 structural gaps |
| [Mgn](services/mgn/README.md) | A | 95 | 1 gap; 8 structural gaps; 1 deferred |
| [Networkfirewall](services/networkfirewall/README.md) | B | 85 | 4 gaps; 3 structural gaps |
| [Networkmanager](services/networkmanager/README.md) | A | 95 | 4 gaps; 2 structural gaps |
| [Oam](services/oam/README.md) | B | 15 | 3 gaps; 1 structural gap |
| [Outposts](services/outposts/README.md) | A | 45 | 2 gaps; 7 structural gaps |
| [Pricing](services/pricing/README.md) | B | 5 | 5 gaps |
| [Resiliencehub](services/resiliencehub/README.md) | A | 63 | 7 structural gaps |
| [Resourceexplorer2](services/resourceexplorer2/README.md) | B | 27 | 13 gaps; 1 structural gap |
| [Rum](services/rum/README.md) | B | 20 | 6 gaps; 2 structural gaps |
| [S3vectors](services/s3vectors/README.md) | B | 21 | 8 gaps; 1 structural gap |
| [Servicecatalog](services/servicecatalog/README.md) | B | 90 | 7 gaps; 1 structural gap |
| [Servicequotas](services/servicequotas/README.md) | B | 26 | 8 gaps; 1 structural gap |
| [Support](services/support/README.md) | A | 20 | 1 structural gap; 1 deferred |
| [Timestreaminfluxdb](services/timestreaminfluxdb/README.md) | B | 24 | 4 gaps; 3 structural gaps |
| [WorkSpaces](services/workspaces/README.md) | A | 34 | 4 gaps; 2 structural gaps |
<!-- END GENERATED SERVICES -->

## Using Gopherstack

### AWS CLI

Everything works with the standard AWS CLI — just pass `--endpoint-url`:

```bash
# DynamoDB
aws dynamodb create-table \
    --endpoint-url http://localhost:8000 \
    --table-name Users \
    --attribute-definitions AttributeName=ID,AttributeType=S \
    --key-schema AttributeName=ID,KeyType=HASH \
    --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5

aws dynamodb list-tables --endpoint-url http://localhost:8000

# S3
aws s3 mb s3://my-bucket --endpoint-url http://localhost:8000
aws s3 cp myfile.txt s3://my-bucket/ --endpoint-url http://localhost:8000
aws s3 ls s3://my-bucket/ --endpoint-url http://localhost:8000
```

Alternatively set `AWS_ENDPOINT_URL=http://localhost:8000` once and drop the flag.

### Terraform / OpenTofu

Point the AWS provider at Gopherstack by overriding the service endpoints. No real
credentials are needed — any non-empty string works.

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

Terraform uses path-style S3 URLs, so set `use_path_style = true` on the S3
provider/resource when creating `aws_s3_bucket` resources.

```bash
docker compose up -d   # or: go run .
terraform init
terraform apply
```

### AWS CDK

CDK synthesises CloudFormation locally and deploys it via the AWS SDK, so pointing the SDK at
Gopherstack is enough:

```bash
export AWS_ENDPOINT_URL=http://localhost:8000
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export CDK_DEFAULT_ACCOUNT=000000000000
export CDK_DEFAULT_REGION=us-east-1

docker compose up -d   # start Gopherstack
cdk bootstrap          # creates the CDKToolkit stack via CloudFormation
cdk deploy
```

`AWS_ENDPOINT_URL` is picked up automatically by the AWS SDK v2 used by the CDK CLI.

## Documentation

- [Quickstart](docs/quickstart.md)
- [Running with Docker](docs/docker.md)
- [Security group enforcement](docs/security-groups.md)
- [Migrating from LocalStack](docs/migration.md)
- [Architecture](docs/architecture/README.md)
- [Examples](examples/README.md)
- [Chaos Testing](docs/chaos.md)
- [MCP server](docs/mcp.md)

## Development

**Prerequisites:** Go 1.26+, Docker or Podman (only for Lambda invocations), AWS CLI
(optional).

```bash
task all             # lint + all tests with coverage
task test            # unit tests (short mode)
task total-coverage  # unit + integration + E2E with combined coverage
task lint            # linters
task docs            # regenerate the service docs from each PARITY.md
task pgo             # regenerate the PGO profile
```

**Scoped lint.** `task lint-changed` (branch vs `origin/main` plus working tree) and
`task lint-wip` (working tree + `HEAD`) lint only the touched packages, in batches to bound
memory. Tunables: `LINT_BATCH` (packages per invocation, default 15), `LINT_TIMEOUT`
(per batch, default 30m; a timeout names the batch), `LINT_CONCURRENCY` (default from
`GOFLAGS=-p=N`, else half the cores), `LINT_LOCK_RETRIES` (waits 15s each while another
`golangci-lint` holds the lock). The golangci-lint cache is reused, never cleaned.

**Build cache.** `task gocache-trim` deletes `GOCACHE` entries older than
`GOCACHE_TRIM_MINUTES` (default 180), or runs `go clean -cache` if the cache exceeds
`GOCACHE_CLEAN_GB` (default 200), and prints before/after sizes. `lint-changed`, `build-check`,
`lint`, `test` and `integration-test` run it automatically when the `GOCACHE` filesystem has
less than `GOCACHE_MIN_FREE_GB` (default 5) free.

## Versioning

Releases are tagged **`v1.<major>.<minor>`** — a major bump moves the middle number, a minor
bump moves the last one. Releases are cut from the **Release** workflow, which only offers
those two increments.

The leading `1` is deliberate and stays put. The Go module path is
`github.com/blackbirdworks/gopherstack` with no `/vN` suffix, and Go requires any module at
major version 2 or higher to carry that suffix in its path. A tag like `v18.0.0` is therefore
invisible to the Go module proxy and pkg.go.dev — staying on the `v1.x.y` line keeps
`go get` working without rewriting every import across the tree on each release.

`v1.0.x` is reserved and must never be reused. An earlier release line published
`v1.0.0`–`v1.0.18`, and `proxy.golang.org` caches module versions permanently while
`sum.golang.org` records their checksums — re-tagging one of those numbers would serve the
old cached code and break checksum verification for anyone fetching it. Releases therefore
start at `v1.1.0`, which also sorts above the highest cached version. The release workflow
enforces both rules and refuses to create a tag that violates them.

## Profile-Guided Optimization (PGO)

The repo root ships `default.pgo`, a CPU profile that Go's
[Profile-Guided Optimization](https://go.dev/doc/pgo) automatically consumes from the main
package directory on every `go build` — no extra flags. It's captured from a representative
workload (heavy DynamoDB GSI/LSI and S3 traffic, plus broad multi-service coverage) so the
compiler can optimize the real hot paths.

Regenerate it with:

```bash
task pgo
```

This runs `scripts/pgo.sh`, which builds the server and the `cmd/pgoload` load generator, runs
the server with pprof enabled, captures CPU profiles while driving load against it, and writes
the merged result to `default.pgo`. It validates the profile with `go tool pprof` and
`go build -pgo=auto` before finishing. Knobs (capture/load duration, concurrency, …) are
environment variables — see the header of `scripts/pgo.sh`.

**Per-PR:** if your change measurably shifts the server's hot paths, run `task pgo` and commit
the updated `default.pgo` alongside it.

**CI:** a weekly workflow (`.github/workflows/pgo.yml`) also runs `task pgo` and opens a pull
request with a refreshed profile.

## License

Gopherstack is released under the [MIT License](LICENSE).
