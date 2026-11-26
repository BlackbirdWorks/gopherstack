# Persistence modes

All modes need `--persist` (data under `--data-dir`) and apply to every service. `--persist-mode` (`GOPHERSTACK_PERSIST_MODE`) sets the default; `--persist-service-mode s3=hybrid,sqs=snapshot` overrides it per service.

| Mode | Behaviour | Worst-case loss on crash |
|------|-----------|--------------------------|
| `snapshot` (default) | Snapshots a service 500 ms after its last mutating request, and all services at clean shutdown | About 500 ms |
| `shutdown` | No saves while running; saves everything at graceful shutdown | Everything since start |
| `manual` | Saves only via `POST /_gopherstack/persist/save`. Never saves at shutdown | Everything since the last explicit save |
| `hybrid` | Mutations mark the service dirty. Every `--persist-flush-interval` (default `5s`), only changed services are snapshotted. Everything is flushed at clean shutdown. Suits write-heavy workloads | One flush interval |

A `POST`, `PUT`, `PATCH` or `DELETE` marks the routed service dirty unless its operation name (`X-Amz-Target` suffix or `Action`) starts with `Get`, `List`, `Describe`, `Head`, `Query`, `Scan` or `BatchGet`. Unknown operations count as writes.

## Service selection

`--persist-services s3,dynamodb` persists only those services. `--persist-exclude` removes services afterwards. Names are case-insensitive. Unselected services are neither saved nor restored.

## Load strategy

`--persist-load` (`GOPHERSTACK_PERSIST_LOAD`) sets when stored state is restored.

| Strategy | Behaviour |
|----------|-----------|
| `startup` (default) | Every service restores at startup |
| `request` | A service restores just before its first request through the HTTP gateway. In-process calls between services do not trigger it. Services never touched keep their stored snapshot at shutdown |
| `manual` | Nothing restores until `POST /_gopherstack/persist/load` |

> [!WARNING]
> With `manual` load, mutating a service before loading overwrites its stored snapshot at the next save.

`/_gopherstack/persist/save` and `/_gopherstack/persist/load` exist only with `--persist`. They return `{"status":"ok","saved":N}` and `{"status":"ok","loaded":N}`.

LocalStack's `SNAPSHOT_SAVE_STRATEGY` and `SNAPSHOT_LOAD_STRATEGY` map onto these settings; see [Migration](migration.md#drop-in-compat-mode).
