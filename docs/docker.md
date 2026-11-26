# Docker and Compose

```bash
docker run --rm -p 8000:8000 ghcr.io/blackbirdworks/gopherstack:latest
curl http://localhost:8000/_gopherstack/health     # {"status":"ok",...}
```

Image: `ghcr.io/blackbirdworks/gopherstack` (`latest` and release tags). Exposed ports: `8000` (AWS API, dashboard) and `10000-10003`, `10006`, `8081` (Azure emulators). For Testcontainers modules see [Testcontainers](testcontainers.md).

## Compose

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
      timeout: 5s
      retries: 3
      start_period: 5s
    volumes:
      - gopherstack-data:/data

volumes:
  gopherstack-data:
```

The repository's `docker-compose.yml` is similar, but builds the local `Dockerfile` and sets `DEMO=true`.

The container takes the same environment variables as the CLI (`PORT`, `REGION`, `ACCOUNT_ID`, `PERSIST`, `DEMO`, `LOG_LEVEL`, `LATENCY_MS`, ...). See [Quickstart](quickstart.md#configuration).

## Application stack

```yaml
services:
  gopherstack:
    image: ghcr.io/blackbirdworks/gopherstack:latest
    ports: ["8000:8000"]
    healthcheck:
      test: ["CMD", "./gopherstack", "health"]
      interval: 10s
      retries: 3

  app:
    build: .
    environment:
      AWS_ENDPOINT_URL: http://gopherstack:8000
      AWS_ACCESS_KEY_ID: test
      AWS_SECRET_ACCESS_KEY: test
      AWS_DEFAULT_REGION: us-east-1
    depends_on:
      gopherstack:
        condition: service_healthy
```

## GitHub Actions

```yaml
jobs:
  test:
    runs-on: ubuntu-latest
    services:
      gopherstack:
        image: ghcr.io/blackbirdworks/gopherstack:latest
        ports: ["8000:8000"]
        options: >-
          --health-cmd "./gopherstack health"
          --health-interval 10s
          --health-retries 3
    steps:
      - uses: actions/checkout@v4
      - run: go test ./...
        env:
          AWS_ENDPOINT_URL: http://localhost:8000
          AWS_ACCESS_KEY_ID: test
          AWS_SECRET_ACCESS_KEY: test
          AWS_DEFAULT_REGION: us-east-1
```

## Non-root user

The image is `gcr.io/distroless/static:nonroot`, running as `65532:65532`. The binary is `/usr/local/bin/gopherstack` (the working directory, so `./gopherstack health` works). `/data` is owned by 65532.

To run as root, pass `--user 0:0` (Compose: `user: "0:0"`).

## Persistence volumes

With `PERSIST=true`, snapshots go to `/data`:

```yaml
volumes:
  - gopherstack-data:/data   # named volume
  - ./local-data:/data       # host path
```

A new named volume inherits ownership of `/data`. A host path keeps the host's ownership: give 65532 write access, or run as your own user.

```bash
mkdir -p local-data && sudo chown 65532:65532 local-data
# or
docker run --rm --user "$(id -u):$(id -g)" -p 8000:8000 -e PERSIST=true \
  -v "$PWD/local-data:/data" ghcr.io/blackbirdworks/gopherstack:latest
```

> [!WARNING]
> Without write access Gopherstack logs `mkdir /data/...: permission denied`. A named volume created by an older root-running image stays root-owned. Fix it once: `docker run --rm -v gopherstack-data:/data alpine chown -R 65532:65532 /data`.

## Docker-backed features (Lambda, ECS, engines)

These need the host Docker socket. The container user is not in the socket's group, so add it:

```yaml
services:
  gopherstack:
    image: ghcr.io/blackbirdworks/gopherstack:latest
    group_add: ["${DOCKER_GID}"]   # export DOCKER_GID=$(stat -c %g /var/run/docker.sock)
    environment:
      - LAMBDA_DOCKER_HOST=172.17.0.1
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - gopherstack-data:/data
    ports: ["8000:8000"]
```

```bash
docker run --rm -p 8000:8000 -v /var/run/docker.sock:/var/run/docker.sock \
  --group-add "$(stat -c %g /var/run/docker.sock)" ghcr.io/blackbirdworks/gopherstack:latest
```

`LAMBDA_DOCKER_HOST` is the host Lambda containers use to reach the Runtime API (default `172.17.0.1`). On Docker Desktop the socket is usually `root:root` in the VM; use `--group-add 0` or `--user 0:0`.

> [!WARNING]
> Mounting the Docker socket gives the container control of the host Docker daemon. Without it, engine-backed services stay metadata-only.

See [Lambda architecture](architecture/lambda.md).

## Registry credentials and mirrors

Every image pull (Lambda, ECS, EC2-docker, CodeBuild, extensions) goes through `pkgs/registryauth`.

Credentials are resolved per registry host. First match wins:

1. `--docker-registry-auth host=user:pass` (env `GOPHERSTACK_DOCKER_REGISTRY_AUTH`, one entry per line; repeat the flag for several registries).
2. The built-in ECR emulator's registry host, using the token from `GetAuthorizationToken`.
3. Docker config: `$DOCKER_CONFIG/config.json` or `~/.docker/config.json`. Supports `auths` (`auth`, `username`/`password`, `identitytoken`), `credHelpers` and `credsStore` (`docker-credential-<name> get`).

`docker.io`, `index.docker.io` and `registry-1.docker.io` are the same registry. Secrets are not logged.

Mirrors rewrite the registry host and keep the tag:

```
--docker-registry-mirror docker.io=mirror.example.com,ghcr.io=ghcr-mirror.example.com
```

Env: `GOPHERSTACK_DOCKER_REGISTRY_MIRROR`. The image is pulled from the mirror with the mirror host's credentials, then retagged to the original reference. If the mirror pull fails, a warning is logged and the original registry is used. Digest-pinned references bypass mirrors.

## Build locally

```bash
docker build -t gopherstack:local .
docker run --rm -p 8000:8000 gopherstack:local
```
