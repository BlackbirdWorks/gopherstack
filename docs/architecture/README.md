# Architecture Overview

Gopherstack is a single Go binary that implements the AWS wire protocol for over 25 services. All state is kept in memory with optional disk persistence.

## Design principles

- **Single binary** — all services run in one process; no containers, no databases, no external dependencies (except Docker for Lambda).
- **Native wire protocol** — every service speaks the exact same JSON/XML/form-encoded protocol as the real AWS SDK, so any AWS SDK works without modification.
- **In-memory by default** — startup is sub-100 ms and teardown is instant. Persistence is opt-in.
- **Production-shaped** — the same service interfaces, ARN formats, and pagination patterns as real AWS, so code that works locally works in production.

## Request routing

Gopherstack uses [Echo](https://echo.labstack.com/) as the HTTP server. Each service registers a `Registerable` handler that implements:

```go
type Registerable interface {
    RouteMatcher() Matcher       // decides whether this request belongs to this service
    MatchPriority() int          // higher = matched first (breaks ties)
    ExtractOperation(*echo.Context) string  // human-readable op name for metrics/logs
    Handle(*echo.Context) error  // processes the request
}
```

On every incoming request the registry walks all registered handlers in priority order and calls `RouteMatcher()`. The first match wins. This allows services to share a single port without URL-prefix collisions (e.g. S3 path-style vs. virtual-hosted style).

Priority constants (lower number = higher priority):
- `PriorityHeaderExact` — matched by exact `X-Amz-Target` value
- `PriorityTargetPrefixed` — matched by `X-Amz-Target` prefix
- `PriorityPathSubdomain` — matched by URL path or form field
- `PriorityFallback` — last resort

### Telemetry wrapping

Every handler is wrapped with `WrapEchoHandler` which:
1. Injects the service name into the logger context (`logger.AddAttrs`).
2. Records a Prometheus counter and histogram for every operation.
3. Logs the operation name, duration, and any errors.

Global middlewares (e.g. CORS, request ID, latency injection) run outside the telemetry timer.

## In-memory backends

Each service has an `InMemoryBackend` (or `InMemoryDB` for DynamoDB) that stores state in Go maps protected by `sync.RWMutex`. Backends are created at startup and shared for the lifetime of the process.

### Persistence

Backends that implement the `Persistable` interface:

```go
type Persistable interface {
    Snapshot() []byte    // serialize current state to JSON
    Restore([]byte) error // restore state from JSON
}
```

When `--persist` (`PERSIST=true`) is set:
1. At startup, `Manager.RestoreAll` loads snapshots from disk and calls `Restore`.
2. On any mutation, `Manager.Notify(serviceName)` triggers a 500 ms debounced `Snapshot` + write to the `FileStore`.
3. On SIGTERM/SIGINT, `Manager.SaveAll` writes all pending snapshots synchronously before exit.

Snapshots are stored in `~/.gopherstack/data/<service>/snapshot` (or `/data/` in containers).

## DNS server

An optional embedded DNS server (based on `miekg/dns`) can be enabled with `--dns-addr :10053`. Services that create network-addressable resources (RDS, Redshift, ElastiCache, OpenSearch) automatically register/deregister synthetic hostnames when resources are created/deleted.

See [dns.md](dns.md) for configuration and OS integration.

## Service integrations

Some services are wired together at startup:

- **SNS → SQS**: SNS fan-out delivers to subscribed SQS queues in-process.
- **EventBridge → Lambda/SQS/SNS**: Target invocations happen in the same process.
- **Step Functions → SQS/SNS/DynamoDB**: Task integrations call the in-process backends.
- **Lambda ESM → SQS**: Event source mappings poll the SQS backend and invoke Lambda containers.

## Port allocator

Services that open their own TCP ports (ElastiCache `embedded`, OpenSearch `docker`, RDS `docker`) use a shared `portalloc.Allocator` that assigns ports from the configured range (`PORT_RANGE_START`–`PORT_RANGE_END`, default 10000–10100).

## Further reading

- [ElastiCache engine modes](elasticache.md)
- [DNS setup](dns.md)
- [Lambda runtime](lambda.md)
