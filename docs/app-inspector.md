# App Inspector

A bounded in-memory trace of every AWS API call through the router. Open it in the
dashboard at `/dashboard/apptrace` (Settings & Resilience, "App Inspector").

Each entry records time, service, operation, resource, region, account, principal,
HTTP status, error code, latency, request/response sizes and the request id. Calls
made on behalf of a request (for example an SNS/EventBridge delivery into SQS) are
recorded as child spans (`kind: internal`) whose `parentId` is the triggering request id
and which share its `traceId`. Child spans exist only where the delivery code carries the
request context; today that is the shared SQS sender used by SNS, EventBridge, API Gateway
and the scheduler. Background work started without the request context is not linked.

Relation to the Live API Console (`/_gopherstack/requests`): the console is a flat 100-entry
log of raw request headers/body. The inspector adds region/account/principal/operation,
response data, sizes, filtering, error codes and causality.

## Capture modes

| Mode | Default | Contents |
| --- | --- | --- |
| Metadata | on | the fields above only |
| Capture bodies | off | plus request/response headers, query string, and request/response bodies truncated to `MAX_BODY` bytes |

Bodies are redacted before storage: `Authorization`, `X-Amz-Security-Token`, cookies,
signatures and any header/field/XML element/form key whose name contains `password`,
`secret`, `token`, `privatekey`, `apikey`, `credential`, `plaintext` or `signature`
(`SecretString`, `SecretAccessKey`, `SessionToken`, ...). Paging cursors such as
`NextToken` are kept. Binary or compressed bodies are replaced by a size note.

Toggle both at runtime from the page header, or with the environment:

| Variable | Default |
| --- | --- |
| `GOPHERSTACK_APPTRACE` | on (`off`/`false` disables) |
| `GOPHERSTACK_APPTRACE_BODIES` | `false` |
| `GOPHERSTACK_APPTRACE_CAPACITY` | `2000` spans (fixed ring, oldest evicted) |
| `GOPHERSTACK_APPTRACE_MAX_BODY` | `4096` bytes |

## API

| Method | Path | |
| --- | --- | --- |
| GET | `/_gopherstack/apptrace/requests` | list, newest first. Query: `q`, `service`, `operation`, `region`, `account`, `principal`, `errorCode`, `traceId`, `parentId`, `kind` (`request`/`internal`), `errorsOnly`, `minStatus`, `maxStatus`, `afterSeq`, `limit` (default 200, max 1000), `offset` |
| GET | `/_gopherstack/apptrace/requests/{id}` | one span with `detail` plus the whole `trace` |
| DELETE | `/_gopherstack/apptrace/requests` | clear |
| GET/PUT | `/_gopherstack/apptrace/config` | `{enabled, detail}` (PUT accepts either field) |

## Overhead

`go test -bench . ./pkgs/apptrace` (full echo request, in-process): metadata-only adds about
1.1 microseconds and 8 allocations per request over no tracing; a disabled tracer costs one
atomic load. Detail capture is opt-in and costs more.
