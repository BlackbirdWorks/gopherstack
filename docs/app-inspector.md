# App Inspector

A bounded in-memory trace of every AWS API call through the router. Open it at `/dashboard/apptrace`.

Each entry records time, service, operation, resource, region, account, principal, HTTP status, error code, latency, request and response sizes, and the request ID.

Calls made on behalf of a request (for example an SNS or EventBridge delivery into SQS) are child spans (`kind: internal`). Their `parentId` is the triggering request ID and they share its `traceId`. Child spans exist only where the delivery code carries the request context: today, the shared SQS sender used by SNS, EventBridge, API Gateway and the scheduler. Background work started without the request context is not linked.

The Live API Console (`/_gopherstack/requests`) is a flat log of raw request headers and bodies. The inspector adds region, account, principal, operation, response data, sizes, filtering, error codes and causality.

## Capture modes

| Mode | Default | Contents |
| --- | --- | --- |
| Metadata | on | The fields above |
| Capture bodies | off | Also request/response headers, query string, and bodies truncated to `MAX_BODY` bytes |

Bodies are redacted before storage:

- `Authorization`, `X-Amz-Security-Token`, cookies and signatures.
- Any header, field, XML element or form key whose name contains `password`, `passwd`, `secret`, `token`, `privatekey`, `apikey`, `credential`, `plaintext` or `signature` (`SecretString`, `SecretAccessKey`, `SessionToken`, ...).
- Paging cursors such as `NextToken` are kept. Binary or compressed bodies are replaced by a size note.

Toggle both from the page header, or with environment variables:

| Variable | Default |
| --- | --- |
| `GOPHERSTACK_APPTRACE` | on; `off` or `false` disables |
| `GOPHERSTACK_APPTRACE_BODIES` | `false` |
| `GOPHERSTACK_APPTRACE_CAPACITY` | `2000` spans (ring buffer, oldest evicted) |
| `GOPHERSTACK_APPTRACE_MAX_BODY` | `4096` bytes |

## API

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/_gopherstack/apptrace/requests` | List, newest first. Query: `q`, `service`, `operation`, `region`, `account`, `principal`, `errorCode`, `traceId`, `parentId`, `kind` (`request`/`internal`), `errorsOnly`, `minStatus`, `maxStatus`, `afterSeq`, `limit` (default 200, max 1000), `offset` |
| GET | `/_gopherstack/apptrace/requests/{id}` | One span with `detail`, plus the whole `trace` |
| DELETE | `/_gopherstack/apptrace/requests` | Clear |
| GET, PUT | `/_gopherstack/apptrace/config` | `{enabled, detail}`; PUT accepts either field |
