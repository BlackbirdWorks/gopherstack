# Extensions

Extensions are out-of-process plugins (any language, or a container image) that add
endpoints and hooks to a running gopherstack. They are the gopherstack analogue of
LocalStack Pro extensions. There is no in-process plugin loading.

## Declaring extensions

```
gopherstack --extensions-config ext.yaml        # or GOPHERSTACK_EXTENSIONS_CONFIG
gopherstack --extension hello="./hello --flag"  # repeatable; name=command args
gopherstack --extension box=image:ghcr.io/me/box:1
```

```yaml
fail_open: true        # default for request hooks (true)
hook_timeout: 5s
extensions:
  - name: hello
    command: [./hello]          # or image: + args:
    env: {FOO: bar}
    port: 0                     # 0 = pick a free loopback port
    restart: on-failure         # never | on-failure | always
    max_restarts: 5
    restart_backoff: 1s
    start_timeout: 30s          # time allowed to register
    health_path: /health        # empty = TCP dial on the port
    health_interval: 10s
    health_failures: 3
    allow_override: false       # may shadow built-in services
    control_url: ""             # containers: URL of gopherstack as seen from inside
```

Container extensions need a reachable control URL; on Linux set `control_url` to the
docker bridge gateway (for example `http://172.17.0.1:8000`) or use Docker Desktop's
`host.docker.internal` (the default is `http://127.0.0.1:<port>`, which only works for
host processes). Container ports are published on `127.0.0.1:<port>`.

## Environment given to an extension

| Variable | Meaning |
| --- | --- |
| `GOPHERSTACK_EXTENSION_NAME` | declared name |
| `GOPHERSTACK_EXTENSION_TOKEN` | per-spawn secret for the control API (redacted from captured output) |
| `GOPHERSTACK_EXTENSION_PORT` | port the extension must listen on (127.0.0.1) |
| `GOPHERSTACK_CONTROL_URL` | gopherstack base URL |

The token changes on every (re)start. It is never returned by the API or logged.

## Registration

After listening, the extension calls `POST /_gopherstack/extensions/register` with
`Authorization: Bearer $GOPHERSTACK_EXTENSION_TOKEN`. Process extensions must call from
loopback.

```json
{
  "version": "1.0.0",
  "endpoint": "http://127.0.0.1:PORT",
  "routes": {
    "path_prefixes": ["/hello"],
    "hosts": ["hello.test"],
    "target_prefixes": ["MyService."]
  },
  "hooks": {
    "on_startup": "/hooks/startup",
    "on_ready": "/hooks/ready",
    "on_shutdown": "/hooks/shutdown",
    "request": [
      {"services": ["S3"], "pre": "/hooks/pre", "post": "/hooks/post",
       "timeout_ms": 500, "fail_open": true}
    ]
  }
}
```

`endpoint` is optional (defaults to `http://127.0.0.1:<port>`) and must be loopback.
Claims may not overlap `/_gopherstack`, and two extensions cannot claim the same prefix,
host or target. A path prefix of `/` requires `allow_override`.

### Routes

Matching requests are reverse-proxied to the extension with path, query, headers, body
and Host preserved. A built-in service that already matched wins, except the S3
catch-all, unless the extension sets `allow_override: true`. While an extension is
unhealthy its routes answer `503`.

### Lifecycle hooks

gopherstack POSTs `{"event": "...", "extension": "..."}` to the hook path:
`on_startup` right after registration, `on_ready` once the listener accepts
connections (or at registration if already ready), `on_shutdown` before extensions are
stopped. Failures are logged and ignored.

### Request hooks

For requests handled by a listed built-in service (`*` for all), gopherstack POSTs JSON
to `pre` before and `post` after the service runs:

```json
{"phase": "pre", "extension": "x", "service": "S3", "method": "PUT", "path": "/b/k",
 "query": "", "headers": {}, "body": "<base64>", "body_truncated": false}
```

`post` additionally carries `status`, `response_headers` and `response_body`.

A `pre` reply may be empty, `{"set_headers": {...}}` to add request headers, or
`{"respond": {"status": 403, "headers": {}, "body": "<base64>"}}` to answer directly.
A `post` reply may set `status`, `set_headers` and `body` (base64) to rewrite the
response. Bodies over 4 MiB are not sent (`body_truncated`). Timeouts and errors are
fail-open by default; with `fail_open: false` the client gets `502`. Services that
stream (event streams, websockets) should not use `post` hooks, which buffer the response.

## Management

`GET /_gopherstack/extensions` lists name, type (`process`/`container`), status
(`starting`, `ready`, `unhealthy`, `restarting`, `failed`, `stopped`), restarts, last
error, endpoint and the accepted registration. LocalStack has no public HTTP equivalent,
so there is no `/_localstack/extensions` alias.

## Example

`examples/extensions/hello` adds `/hello` and logs S3 requests.
