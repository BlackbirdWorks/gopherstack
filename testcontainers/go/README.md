# Gopherstack testcontainers module (Go)

```go
import gopherstack "github.com/blackbirdworks/gopherstack/testcontainers/go"

gs, err := gopherstack.Run(ctx, "", // "" = ghcr.io/blackbirdworks/gopherstack:latest
    gopherstack.WithServices("s3", "sqs"),
)
testcontainers.CleanupContainer(t, gs.Container)

cfg, _ := gs.AWSConfig(ctx)               // static creds + BaseEndpoint
s3c := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })

endpoint, _ := gs.Endpoint(ctx)            // http://host:mappedPort
cs, _ := gs.AzureConnectionString(ctx)     // devstoreaccount1 blob/queue/table/file
hp, _ := gs.GCPEndpoint(ctx, "storage")    // host:port
```

Options: `WithServices(...)`, `WithPersistence(hostDir)`, `WithDockerSocket()`
(see the [socket warning](../README.md#docker-socket-warning)), `WithEnv(k, v)`,
`WithCustomizer(...)`. `AzureEndpoints(ctx)` returns every Azure listener URL.

This is a separate Go module, so the root module carries no testcontainers
dependency.

## Tests

`gopherstack_test.go` runs S3, SQS, Azure Blob and GCP Storage round trips
against a real container and skips when Docker is unavailable.

```bash
task build-linux && docker build -f Dockerfile.test -t gopherstack:tc-local .
cd testcontainers/go && GOPHERSTACK_TEST_IMAGE=gopherstack:tc-local go test ./...
```
