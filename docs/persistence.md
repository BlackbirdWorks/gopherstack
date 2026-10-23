# Persistence modes

All modes need `--persist` (data under `--data-dir`) and apply to every service automatically. `--persist-mode` sets the default; `--persist-service-mode s3=hybrid,sqs=snapshot` overrides it per service.

| Mode | Behaviour | Worst-case loss on crash |
|------|-----------|--------------------------|
| `snapshot` (default) | Full snapshot of a service 500 ms after its last mutating request, plus all at clean shutdown. | ~500 ms |
| `hybrid` | Mutations only mark the service dirty. Every `--persist-flush-interval` (default 5s) only services changed since their last flush are snapshotted, which suits write-heavy workloads; everything is flushed at clean shutdown. | one flush interval |

The dirty signal is central: any `POST/PUT/PATCH/DELETE` whose operation name (`X-Amz-Target` suffix or `Action`) does not start with `Get`, `List`, `Describe`, `Head`, `Query`, `Scan` or `BatchGet` marks the routed service dirty. Unknown operations count as writes.

## Service selection

`--persist-services s3,dynamodb` persists only those services; `--persist-exclude` removes services afterwards. Names are case-insensitive. Unselected services are neither saved nor restored.
