# Persistence modes

All modes need `--persist` (data under `--data-dir`) and apply to every service automatically. `--persist-mode` sets the default; `--persist-service-mode s3=hybrid,sqs=snapshot` overrides it per service.

| Mode | Behaviour | Worst-case loss on crash |
|------|-----------|--------------------------|
| `snapshot` (default) | Full snapshot of a service 500 ms after its last mutating request, plus all at clean shutdown. | ~500 ms |
| `shutdown` | No saves while running; everything is saved at graceful shutdown. | everything since start |
| `manual` | Saves only through `POST /_gopherstack/persist/save` (writes every persisted service now). Never saves at shutdown. | everything since the last explicit save |
| `hybrid` | Mutations only mark the service dirty. Every `--persist-flush-interval` (default 5s) only services changed since their last flush are snapshotted, which suits write-heavy workloads; everything is flushed at clean shutdown. | one flush interval |

The dirty signal is central: any `POST/PUT/PATCH/DELETE` whose operation name (`X-Amz-Target` suffix or `Action`) does not start with `Get`, `List`, `Describe`, `Head`, `Query`, `Scan` or `BatchGet` marks the routed service dirty. Unknown operations count as writes.

## Service selection

`--persist-services s3,dynamodb` persists only those services; `--persist-exclude` removes services afterwards. Names are case-insensitive. Unselected services are neither saved nor restored.

## Load strategy

`--persist-load` (`GOPHERSTACK_PERSIST_LOAD`) decides when stored state is restored.

| Strategy | Behaviour |
|----------|-----------|
| `startup` (default) | Every service restores at startup. |
| `request` | A service restores right before its first request through the HTTP gateway. In-process calls between services do not trigger the load. Services never touched keep their stored snapshot untouched at shutdown. |
| `manual` | Nothing restores until `POST /_gopherstack/persist/load`; mutating a service before that overwrites its stored snapshot at the next save. |

Both endpoints exist only with `--persist` and respond with `{"status":"ok","saved":N}` / `{"status":"ok","loaded":N}`. LocalStack's `SNAPSHOT_SAVE_STRATEGY` and `SNAPSHOT_LOAD_STRATEGY` map onto these (see [migration.md](migration.md)).
