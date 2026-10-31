# Gopherstack Testcontainers modules

Run Gopherstack (AWS, Azure and GCP emulation) as a throwaway container from
your tests. Same API shape in every language.

| Language | Path | Package |
|----------|------|---------|
| Go | [`go/`](go/README.md) | `github.com/blackbirdworks/gopherstack/testcontainers/go` |
| Python | [`python/`](python/README.md) | `testcontainers-gopherstack` |
| Node | [`node/`](node/README.md) | `@blackbirdworks/testcontainers-gopherstack` |

Java and .NET modules are not provided yet. Use the generic container with
`ghcr.io/blackbirdworks/gopherstack:latest`, port 8000 and the wait path
`/_gopherstack/health`.

## Shared behaviour

- Default image `ghcr.io/blackbirdworks/gopherstack:latest` (override per call).
- Readiness: `GET /_gopherstack/health` on port 8000.
- Exposed: 8000 (AWS), Azure 10000-10014, 8081, GCP 4443, 8080, 8085-8087, 8123-8126, 8128, 8130-8147 (8141 is the Cloud DNS data plane: TCP is exposed, map UDP yourself for UDP queries), 9050, OCI 8150-8166.
- Options: services filter (`GOPHERSTACK_SERVICES`), persistence (bind mount at `/data` with `PERSIST=true`), Docker socket, extra env.
- Azure: well-known `devstoreaccount1` identity; connection string covers blob, queue and table.
- GCP: `storage` endpoint is the value for `STORAGE_EMULATOR_HOST`.

## Docker socket warning

`WithDockerSocket` / `with_docker_socket` / `withDockerSocket` mounts
`/var/run/docker.sock` so the Lambda, RDS and ECS docker engines can start
sibling containers. That gives the emulator root-equivalent control of the host
Docker daemon. Use it only on trusted dev and CI machines.
