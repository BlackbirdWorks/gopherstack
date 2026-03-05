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

## Volume-backed persistence

State snapshots are written to `/data` inside the container. Mount a named volume or host path to keep state across container restarts:

```yaml
volumes:
  - gopherstack-data:/data       # named volume (shown above)
  - ./local-data:/data           # host path
```

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

```yaml
services:
  gopherstack:
    image: ghcr.io/blackbirdworks/gopherstack:latest
    environment:
      - LAMBDA_DOCKER_HOST=host-gateway  # or your host IP
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - gs-data:/data
    ports:
      - "8000:8000"
```

See [Lambda architecture](architecture/lambda.md) for details.
