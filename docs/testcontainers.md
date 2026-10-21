# Testcontainers

Gopherstack ships Testcontainers modules for Go, Python and Node under
[`testcontainers/`](../testcontainers/README.md). Each starts the published
image, waits on `/_gopherstack/health`, and exposes helpers for AWS, Azure
(Azurite-style connection string) and GCP (fixed per-service ports) clients.

```go
gs, _ := gopherstack.Run(ctx, "", gopherstack.WithServices("s3", "sqs"))
cfg, _ := gs.AWSConfig(ctx)
```

See the [module README](../testcontainers/README.md) for options, persistence
and the Docker-socket warning (Lambda/RDS/ECS docker engines need the host
socket, which grants control of the host Docker daemon).
