# Lambda runtime

Functions run in containers and talk to Gopherstack over the [Lambda Runtime API](https://docs.aws.amazon.com/lambda/latest/dg/runtimes-api.html). Both `Zip` and `Image` package types are supported. Zip functions are mounted into an AWS runtime base image. Zip code comes from `Code.ZipFile` or `Code.S3Bucket` plus `Code.S3Key`.

A container runtime is required: Docker, Podman, or Kubernetes (see the Kubernetes Pod executor in the Kubernetes guide).

## Invocation flow

1. Gopherstack pulls the image if it is not cached, and starts a container with `AWS_LAMBDA_RUNTIME_API=<LAMBDA_DOCKER_HOST>:<port>`.
2. The runtime polls `GET /2018-06-01/runtime/invocation/next`.
3. Gopherstack sends the payload. The runtime posts the result to `/2018-06-01/runtime/invocation/<id>/response` (or `/error`).
4. The container returns to a warm pool and is reused.

The Runtime API listens on a port allocated from `PORT_RANGE_START`-`PORT_RANGE_END`. The container must be able to reach `LAMBDA_DOCKER_HOST` (on Linux with the default Docker bridge, `172.17.0.1`).

Supported endpoints: `/2018-06-01/runtime/invocation/next`, `/2018-06-01/runtime/invocation/<id>/response`, `/2018-06-01/runtime/invocation/<id>/error`, `/2018-06-01/runtime/init/error`, plus the Extensions API (`register`, `event/next`, `init/error`, `exit/error`) and the Telemetry and Logs subscription APIs.

## Warm pool

Each function keeps up to `--lambda-pool-size` warm containers. Containers idle longer than `--lambda-idle-timeout` are stopped and removed.

## Settings

| Flag | Env var | Default | Description |
|------|---------|---------|-------------|
| `--lambda-docker-host` | `LAMBDA_DOCKER_HOST` | `172.17.0.1` | Host containers use to reach Gopherstack |
| `--lambda-container-runtime` | `CONTAINER_RUNTIME` | `docker` | `docker`, `podman`, `auto` or `kubernetes` |
| `--lambda-isolation` | `LAMBDA_ISOLATION` | `container` | `container`, `gvisor`, `kata` (`firecracker` is not supported) |
| `--lambda-pool-size` | `LAMBDA_POOL_SIZE` | `3` | Warm containers per function |
| `--lambda-idle-timeout` | `LAMBDA_IDLE_TIMEOUT` | `10m` | Idle container lifetime |
| `--lambda-max-runtimes` | `LAMBDA_MAX_RUNTIMES` | `50` | Maximum simultaneous runtimes |
| `--lambda-keep-containers` | `LAMBDA_KEEP_CONTAINERS` | `false` | Keep containers after the function stops |
| `--lambda-docker-flags` | `LAMBDA_DOCKER_FLAGS` | empty | Extra docker options for function containers |
| `--lambda-strict-runtime-validation` | `LAMBDA_STRICT_RUNTIME_VALIDATION` | `false` | Reject deprecated runtimes |
| `--lambda-disable-hot-reload` | `LAMBDA_DISABLE_HOT_RELOAD` | `false` | Disable hot reload from local directories |
| `--lambda-hot-reload-interval-ms` | `LAMBDA_HOT_RELOAD_INTERVAL_MS` | `250` | Minimum time between hot-reload change scans |

Account limits, all default `0` (unset): `--lambda-account-concurrent-executions`, `--lambda-account-min-unreserved-concurrency`, `--lambda-account-total-code-size`, `--lambda-code-size-zipped`, `--lambda-code-size-unzipped`, `--lambda-create-function-request-size`, `--lambda-max-environment-bytes`.

`CONTAINER_HOST` overrides the Podman socket path.

## Invoking

```bash
# synchronous
aws --endpoint-url http://localhost:8000 lambda invoke \
    --function-name my-function --payload '{"key":"value"}' response.json

# asynchronous: returns 202 immediately
aws --endpoint-url http://localhost:8000 lambda invoke \
    --function-name my-function --invocation-type Event --payload '{"key":"value"}' /dev/null
```

## Event source mappings

SQS, Kinesis, DynamoDB Streams and Kafka sources are supported. Gopherstack polls the source and delivers batches.

```bash
aws --endpoint-url http://localhost:8000 lambda create-event-source-mapping \
    --function-name my-function \
    --event-source-arn arn:aws:sqs:us-east-1:000000000000:my-queue \
    --batch-size 10
```

## Function URLs

```bash
aws --endpoint-url http://localhost:8000 lambda create-function-url-config \
    --function-name my-function --auth-type NONE
```

The URL host is AWS-shaped: `<url-id>.lambda-url.<region>.on.aws`. Each URL gets a port from the allocator range. With [embedded DNS](dns.md) the name resolves to Gopherstack.

## Hot reload

A function created with `Code.S3Bucket=hot-reload` and `Code.S3Key` set to a host directory runs that directory and reloads it when files change. Some host paths (`/proc`, `/etc`, `/root`, Docker and Podman state directories, ...) are refused.

## Podman

```bash
systemctl --user enable --now podman.socket
export CONTAINER_RUNTIME=podman
export CONTAINER_HOST=unix://${XDG_RUNTIME_DIR}/podman/podman.sock   # optional
export LAMBDA_DOCKER_HOST=host.containers.internal                   # rootless networking
gopherstack
```

## Troubleshooting

| Problem | Fix |
|---------|-----|
| Container cannot reach Gopherstack | Set `LAMBDA_DOCKER_HOST` to the Docker bridge IP (`ip addr show docker0`) |
| Image pull fails | Check that `docker pull <image>` works from your shell (registry credentials and mirrors are in the Docker guide) |
| Cold starts are slow | Raise `--lambda-pool-size` |
