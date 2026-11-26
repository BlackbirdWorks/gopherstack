# Testcontainers

Modules for Go, Python and Node live in [`testcontainers/`](../testcontainers/README.md). Each starts the published image, waits on `/_gopherstack/health`, and provides helpers for AWS, Azure (Azurite-style connection string) and GCP (fixed per-service ports) clients.

| Language | Package |
|----------|---------|
| Go | `github.com/blackbirdworks/gopherstack/testcontainers/go` |
| Python | `testcontainers-gopherstack` |
| Node | `@blackbirdworks/testcontainers-gopherstack` |

```go
gs, _ := gopherstack.Run(ctx, "", gopherstack.WithServices("s3", "sqs"))
cfg, _ := gs.AWSConfig(ctx)
```

> [!WARNING]
> The Docker socket option (`WithDockerSocket`, `with_docker_socket`, `withDockerSocket`) lets the Lambda, RDS and ECS docker engines start sibling containers, but gives Gopherstack control of the host Docker daemon. Use it only on trusted dev and CI machines.

Options, persistence and per-language details: [module README](../testcontainers/README.md).
