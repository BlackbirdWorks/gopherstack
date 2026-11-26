# Architecture overview

Gopherstack is one Go binary that serves AWS, Azure, GCP and OCI emulators. State is in memory; persistence is opt-in. Every service speaks the real wire protocol of its API (JSON, query, REST-XML, REST-JSON, CBOR), so unmodified SDKs work.

## Request routing

HTTP is served by [Echo](https://echo.labstack.com/). Each service implements `service.Registerable`:

```go
type Service interface {
    Name() string
    Handler() echo.HandlerFunc
    RouteMatcher() Matcher                  // does this request belong to me?
    GetSupportedOperations() []string
}
type ResourceObserver interface {
    ExtractOperation(c *echo.Context) string
    ExtractResource(c *echo.Context) string
}
// Registerable = Service + ResourceObserver + MatchPriority() int
```

The registry sorts services by `MatchPriority()`, highest first, and the first `RouteMatcher` that returns true wins. A prebuilt index keyed on request prefixes (`X-Amz-Target`, path) means a request only visits services whose gates could match.

Priorities (`pkgs/service/priorities.go`):

| Constant | Value | For |
|----------|-------|-----|
| `PriorityHeaderExact` | 100 | Exact `X-Amz-Target` prefix |
| `PriorityHeaderPartial` | 95 | Partial header prefix (Lambda, KMS, Secrets Manager) |
| `PriorityFormEncoded` | 90 | STS form-encoded POST |
| `PriorityPathVersioned` | 85 | Versioned path prefix (S3 Control); also DocumentDB and Neptune form requests |
| `PriorityFormRDS` | 84 | RDS query protocol |
| `PriorityFormRedshift` | 83 | Redshift query protocol |
| `PriorityPathSubdomain` | 82 | Path-prefix matchers (OpenSearch, ElastiCache) |
| `PriorityFormStandard` | 80 | Standard form-encoded query protocol (EC2, IAM, SES) |
| `PriorityTargetPrefixed` | 75 | Versioned `X-Amz-Target` prefix (Kinesis) |
| `PriorityPathUI` | 50 | Dashboard and UI paths |
| `PriorityCatchAll` | 0 | S3 |

Each handler is wrapped by `telemetry.WrapEchoHandler`. It adds the service name to the logger context, records Prometheus metrics (`operations_total`, `operation_duration_seconds`) and logs operation, duration and errors. Global middleware (CORS, request ID, latency injection) wraps the handler.

## Backends and locking

Each service has an in-memory backend holding its state in Go maps behind one coarse lock per backend. AWS operations are cross-map transactions and snapshots need one consistent view.

## Persistence

Backends implement `persistence.Persistable`:

```go
type Persistable interface {
    Snapshot(ctx context.Context) []byte   // JSON
    Restore(ctx context.Context, data []byte) error
}
```

With `--persist`, the persistence manager restores snapshots at startup, snapshots a service 500 ms after its last mutation, and saves everything on SIGTERM/SIGINT. Files are `<data-dir>/<service>/snapshot.json`; the data dir defaults to `~/.gopherstack/data`, or `/data` in containers. Other modes are in the Persistence guide.

## Regions

One process serves many regions. The region comes from the SigV4 credential scope, then the `X-Amz-Region` header, then the configured default (`awsmeta.Region(ctx)`).

Services handle regions in one of three ways:

- **Global.** No region key (IAM, Route 53, CloudFront, Organizations, ...).
- **Regional with per-request keys.** The backend keys its resources by region.
- **Regional via sibling handlers.** Single-region handlers get one lazily built sibling per extra region (`pkgs/regionpeers`, at most 64 per service). Snapshots carry an optional `regions` key.

Cross-service calls follow the region of the originating resource or of the target ARN: for example SQS metrics go to the CloudWatch of the queue's region, and Lambda invocations resolve the function by its ARN region.

The authoritative per-service list is the isolation test manifest in `internal/app/region_isolation_cases_*_test.go`. A case marked `knownCollision` is a service that is still single-region per process: same-named resources in two regions collide. The test fails once such a service is fixed.

## DNS

An optional embedded DNS server registers AWS-style hostnames for resources and can take over real AWS endpoint names. See [DNS](dns.md).

## Service integrations

Wired in-process at startup, with no network hop:

- SNS fan-out to subscribed SQS queues.
- EventBridge targets: Lambda, SQS, SNS.
- Step Functions task integrations: SQS, SNS, DynamoDB.
- Lambda event source mappings poll SQS, Kinesis and DynamoDB Streams, then invoke functions.

## Port allocator

Services that open their own TCP ports (ElastiCache `embedded`, Lambda runtimes and function URLs, and the `docker` engines such as RDS, Kafka and MQ) take ports from a shared `portalloc.Allocator`. The range is `PORT_RANGE_START` to `PORT_RANGE_END` (default 10000 to 10100, end exclusive). Dedicated Azure, GCP and OCI listeners use fixed ports instead (see the Kubernetes guide).

## More

- [ElastiCache engines](elasticache.md)
- [DNS](dns.md)
- [Lambda runtime](lambda.md)
