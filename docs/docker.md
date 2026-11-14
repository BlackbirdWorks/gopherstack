# Docker & Docker Compose

Run Gopherstack as a container with a single command — no Go toolchain required.

## Quick start

```bash
docker run --rm -p 8000:8000 ghcr.io/blackbirdworks/gopherstack:latest
```

Verify it is running:

```bash
curl http://localhost:8000/health
# {"status":"ok"}
```

Testcontainers modules for Go, Python and Node: see [testcontainers.md](testcontainers.md).

## Docker Compose

The repository ships a ready-to-use `docker-compose.yml`:

```yaml
services:
  gopherstack:
    image: ghcr.io/blackbirdworks/gopherstack:latest
    environment:
      - PERSIST=true
      - DEMO=false
    ports:
      - "8000:8000"
    healthcheck:
      test: ["CMD", "./gopherstack", "health"]
      interval: 10s
      timeout: 5s
      retries: 3
      start_period: 5s
    volumes:
      - gopherstack-data:/data

volumes:
  gopherstack-data:
```

Start it:

```bash
docker compose up -d
docker compose ps       # check status
docker compose logs -f  # follow logs
```

## Connecting the AWS CLI

```bash
export AWS_ENDPOINT_URL=http://localhost:8000
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1

aws s3 mb s3://test-bucket
aws sqs create-queue --queue-name my-queue
aws dynamodb list-tables
```

## Environment variables

Customise the container with environment variables:

```yaml
environment:
  - PORT=8000
  - REGION=eu-west-1
  - ACCOUNT_ID=123456789012
  - PERSIST=true
  - DEMO=true
  - LOG_LEVEL=debug
  - LATENCY_MS=50          # inject up to 50 ms of random latency
  - ELASTICACHE_ENGINE=embedded
```

## Running as non-root

The image is built on `gcr.io/distroless/static:nonroot` and runs as `65532:65532` by default. The binary is `/usr/local/bin/gopherstack` (the working directory, so `./gopherstack health` keeps working), `/data` is owned by 65532 and `/tmp` is `1777`. The image used to be `FROM scratch` and ran as root with the binary at `/root/gopherstack`; that path is gone.

To run as root again (for example to keep an older root-owned volume without a `chown`), pass `--user 0:0` / `user: "0:0"`.

## Volume-backed persistence

State snapshots are written to `/data` inside the container. Mount a named volume or host path to keep state across container restarts:

```yaml
volumes:
  - gopherstack-data:/data       # named volume (shown above)
  - ./local-data:/data           # host path
```

A new named volume inherits the ownership of `/data` (65532) and just works. A host path keeps the host's ownership, so give the container user write access, using either of:

```bash
mkdir -p local-data && sudo chown 65532:65532 local-data
docker run --rm -p 8000:8000 -e PERSIST=true -v "$PWD/local-data:/data" ghcr.io/blackbirdworks/gopherstack:latest

# or run as your own user
docker run --rm --user "$(id -u):$(id -g)" -p 8000:8000 -e PERSIST=true -v "$PWD/local-data:/data" ghcr.io/blackbirdworks/gopherstack:latest
```

Without write access Gopherstack logs `mkdir /data/...: permission denied`. A named volume created by an older, root-running image is still owned by root; fix it once with `docker run --rm -v gopherstack-data:/data alpine chown -R 65532:65532 /data`.

## Multi-service application example

Combine Gopherstack with your application in a single Compose file:

```yaml
services:
  gopherstack:
    image: ghcr.io/blackbirdworks/gopherstack:latest
    environment:
      - PERSIST=true
    ports:
      - "8000:8000"
    healthcheck:
      test: ["CMD", "./gopherstack", "health"]
      interval: 10s
      retries: 3
    volumes:
      - gs-data:/data

  app:
    build: .
    environment:
      - AWS_ENDPOINT_URL=http://gopherstack:8000
      - AWS_ACCESS_KEY_ID=test
      - AWS_SECRET_ACCESS_KEY=test
      - AWS_DEFAULT_REGION=us-east-1
    depends_on:
      gopherstack:
        condition: service_healthy

volumes:
  gs-data:
```

## GitHub Actions CI/CD

```yaml
jobs:
  test:
    runs-on: ubuntu-latest
    services:
      gopherstack:
        image: ghcr.io/blackbirdworks/gopherstack:latest
        ports:
          - "8000:8000"
        options: >-
          --health-cmd "./gopherstack health"
          --health-interval 10s
          --health-retries 3

    steps:
      - uses: actions/checkout@v4

      - name: Run tests
        env:
          AWS_ENDPOINT_URL: http://localhost:8000
          AWS_ACCESS_KEY_ID: test
          AWS_SECRET_ACCESS_KEY: test
          AWS_DEFAULT_REGION: us-east-1
        run: go test ./...
```

## Building the image locally

```bash
git clone https://github.com/blackbirdworks/gopherstack
cd gopherstack
docker build -t gopherstack:local .
docker run --rm -p 8000:8000 gopherstack:local
```

## Lambda support in Docker

Lambda image-based functions require the Docker daemon. When running Gopherstack itself inside Docker, you need to mount the host Docker socket and set `LAMBDA_DOCKER_HOST` to the host IP:

The container user (65532) is not in the socket's group, so add that group with `--group-add`/`group_add`:

```bash
docker run --rm -p 8000:8000 -v /var/run/docker.sock:/var/run/docker.sock \
  --group-add "$(stat -c %g /var/run/docker.sock)" ghcr.io/blackbirdworks/gopherstack:latest
```

```yaml
services:
  gopherstack:
    image: ghcr.io/blackbirdworks/gopherstack:latest
    group_add:
      - "${DOCKER_GID}"   # export DOCKER_GID=$(stat -c %g /var/run/docker.sock)
    environment:
      - LAMBDA_DOCKER_HOST=host-gateway  # or your host IP
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - gs-data:/data
    ports:
      - "8000:8000"
```

On Docker Desktop the socket is typically `root:root` inside the VM; use `--group-add 0` or `--user 0:0`. Without the group, engine-backed services (Lambda, ECS, ...) cannot reach the daemon and stay metadata-only.

See [Lambda architecture](architecture/lambda.md) for details.

## Registry credentials and mirrors

Every image pull Gopherstack makes (Lambda container and runtime images, ECS tasks, EC2-docker, CodeBuild, extensions, and so on) goes through `pkgs/registryauth`.

Credentials are resolved per registry host, in this order:

1. `--docker-registry-auth host=user:pass` (env `GOPHERSTACK_DOCKER_REGISTRY_AUTH`, one entry per line; repeat the flag for several registries).
2. Gopherstack's own ECR emulator registry host (automatic, using the token `GetAuthorizationToken` returns).
3. The standard Docker config: `$DOCKER_CONFIG/config.json` or `~/.docker/config.json`. Static `auths` entries (`auth` base64 `user:pass`, `username`/`password`, or `identitytoken`) and `credHelpers` / `credsStore` helpers (`docker-credential-<name> get`) are supported.

`docker.io`, `index.docker.io` and `registry-1.docker.io` are treated as the same registry. Secrets are never logged.

Pull mirrors rewrite the registry host of an image reference, keeping its tag:

```
--docker-registry-mirror docker.io=mirror.example.com,ghcr.io=ghcr-mirror.example.com
```

(env `GOPHERSTACK_DOCKER_REGISTRY_MIRROR`). The image is pulled from the mirror, using credentials for the mirror host, and retagged to the original reference so lookups by name keep working. If the mirror pull fails, a warning is logged and the original registry is used. References pinned by digest bypass mirrors.
