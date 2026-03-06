# Lambda Runtime

Gopherstack implements the [AWS Lambda Runtime API](https://docs.aws.amazon.com/lambda/latest/dg/runtimes-api.html) for Docker image-based functions.

> **Important:** Only `PackageType: Image` is supported. Zip-based and S3-based deployments are not implemented.

## How it works

When you invoke a Lambda function, Gopherstack:

1. Pulls the Docker image (if not already cached locally).
2. Starts a container from the image, passing environment variables including `AWS_LAMBDA_RUNTIME_API`.
3. The container's runtime bootstrapper calls `GET /runtime/invocation/next` on the Runtime API.
4. Gopherstack sends the invocation payload.
5. The runtime handler calls `POST /runtime/invocation/<id>/response` with the result.
6. The container is returned to the **warm pool** for reuse.

### Warm container pool

Each function has a pool of pre-warmed containers (size configurable via `--lambda-pool-size`, default `3`). After a successful invocation the container is kept alive and reused for the next invocation. Containers that idle longer than `--lambda-idle-timeout` (default `10m`) are stopped and removed.

### Runtime API endpoints

The Runtime API is served at `http://<LAMBDA_DOCKER_HOST>:<PORT>/_lambda_runtime/`:

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/runtime/invocation/next` | Long-poll for the next invocation |
| `POST` | `/runtime/invocation/{id}/response` | Report a successful result |
| `POST` | `/runtime/invocation/{id}/error` | Report a runtime error |
| `POST` | `/runtime/init/error` | Report an initialisation error |

`LAMBDA_DOCKER_HOST` must be the IP or hostname the container can use to reach Gopherstack. On Linux with the default Docker bridge this is `172.17.0.1`.

## Configuration

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--lambda-docker-host` | `LAMBDA_DOCKER_HOST` | `172.17.0.1` | Host Lambda containers use to reach Gopherstack |
| `--lambda-pool-size` | `LAMBDA_POOL_SIZE` | `3` | Warm container pool size per function |
| `--lambda-idle-timeout` | `LAMBDA_IDLE_TIMEOUT` | `10m` | Idle container lifetime |
| `--container-runtime` | `CONTAINER_RUNTIME` | `docker` | `docker`, `podman`, or `auto` |
| `--container-host` | `CONTAINER_HOST` | `` | Override the Docker/Podman socket path |

## Invocation modes

### Synchronous (`RequestResponse`)

The caller waits for the function to complete. The response body is returned directly.

```bash
aws --endpoint-url http://localhost:8000 lambda invoke \
    --function-name my-function \
    --payload '{"key":"value"}' \
    response.json
cat response.json
```

### Asynchronous (`Event`)

The invocation returns immediately with HTTP 202. The function runs in the background.

```bash
aws --endpoint-url http://localhost:8000 lambda invoke \
    --function-name my-function \
    --invocation-type Event \
    --payload '{"key":"value"}' \
    /dev/null
```

## Event Source Mappings (SQS)

Create an ESM to trigger a function when messages arrive in an SQS queue:

```bash
aws --endpoint-url http://localhost:8000 lambda create-event-source-mapping \
    --function-name my-function \
    --event-source-arn arn:aws:sqs:us-east-1:000000000000:my-queue \
    --batch-size 10 \
    --enabled
```

Gopherstack polls the SQS queue and delivers message batches to the function automatically.

## Function URLs

Create a URL that can invoke the function directly via HTTP:

```bash
aws --endpoint-url http://localhost:8000 lambda create-function-url-config \
    --function-name my-function \
    --auth-type NONE
```

The returned URL points to the Gopherstack endpoint.

## Podman support

Gopherstack supports Podman as a Docker-compatible runtime.

```bash
# Enable the Podman socket
systemctl --user enable --now podman.socket

# Point Gopherstack at Podman
export CONTAINER_RUNTIME=podman
# Optional: set socket path explicitly
export CONTAINER_HOST=unix://${XDG_RUNTIME_DIR}/podman/podman.sock

# Rootless networking — use host's routable IP
export LAMBDA_DOCKER_HOST=host.containers.internal

gopherstack
```

## Troubleshooting

**Container can't reach Gopherstack:**
Set `LAMBDA_DOCKER_HOST` to the IP of the Docker bridge interface (`ip addr show docker0` on Linux).

**Image pull fails:**
Ensure `docker pull <image>` works from your shell before using Gopherstack.

**Function times out:**
Increase the function timeout with `--timeout` in `UpdateFunctionConfiguration`. Default is 3 seconds.

**Cold starts are slow:**
Increase `--lambda-pool-size` to keep more warm containers per function.
