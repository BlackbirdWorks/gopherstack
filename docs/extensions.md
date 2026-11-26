# Extensions

Extensions are out-of-process plugins (any language, or a container image) that add endpoints and hooks to a running Gopherstack. There is no in-process plugin loading.

## Declaring extensions

```bash
gopherstack --extensions-config ext.yaml         # env GOPHERSTACK_EXTENSIONS_CONFIG
gopherstack --extension hello="./hello --flag"   # repeatable; env GOPHERSTACK_EXTENSIONS
gopherstack --extension box=image:ghcr.io/me/box:1
```

```yaml
fail_open: true          # default for request hooks
hook_timeout: 5s
extensions:
  - name: hello          # [A-Za-z0-9._-]+
    command: [./hello]   # exactly one of command or image
    # image: ghcr.io/me/box:1
    # args: []
    env: {FOO: bar}
    port: 0                # 0 = free loopback port
    restart: on-failure    # never | on-failure (default) | always
    max_restarts: 5
    restart_backoff: 1s
    start_timeout: 30s     # time allowed to register
    health_path: /health   # must start with /; empty = TCP dial on the port
    health_interval: 10s
    health_timeout: 2s
    health_failures: 3
    allow_override: false  # may shadow built-in services
    control_url: ""        # containers: Gopherstack URL as seen from inside
```

Values shown are the defaults.

Container extensions need a reachable `control_url`. The default, `http://127.0.0.1:<port>`, only works for host processes. On Linux use the Docker bridge gateway (for example `http://172.17.0.1:8000`); on Docker Desktop use `http://host.docker.internal:8000`. Container ports are published on `127.0.0.1:<port>`.

## Environment given to an extension

| Variable | Meaning |
| --- | --- |
| `GOPHERSTACK_EXTENSION_NAME` | Declared name |
| `GOPHERSTACK_EXTENSION_TOKEN` | Per-spawn secret for the control API. Redacted from captured output |
| `GOPHERSTACK_EXTENSION_PORT` | Port to listen on (127.0.0.1) |
| `GOPHERSTACK_CONTROL_URL` | Gopherstack base URL |

The token changes on every (re)start and is never returned by the API or logged.

## Registration

Once listening, the extension calls `POST /_gopherstack/extensions/register` with `Authorization: Bearer $GOPHERSTACK_EXTENSION_TOKEN`. Process extensions must call from loopback.

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

- `endpoint` is optional (default `http://127.0.0.1:<port>`) and must be loopback.
- Claims may not overlap `/_gopherstack`. Two extensions cannot claim the same prefix, host or target.
- A path prefix of `/` requires `allow_override`.

### Routes

Matching requests are reverse-proxied with path, query, headers, body and Host preserved. A built-in service that already matched wins (except the S3 catch-all) unless the extension sets `allow_override: true`. While an extension is unhealthy, its routes answer `503`.

### Lifecycle hooks

Gopherstack POSTs `{"event": "...", "extension": "..."}` to the hook path:

| Hook | When |
| --- | --- |
| `on_startup` | Right after registration |
| `on_ready` | When the listener accepts connections (or at registration if already ready) |
| `on_shutdown` | Before extensions are stopped |

Failures are logged and ignored.

### Request hooks

For requests handled by a listed built-in service (`*` for all), Gopherstack POSTs to `pre` before and `post` after the service runs:

```json
{"phase": "pre", "extension": "x", "service": "S3", "method": "PUT", "path": "/b/k",
 "query": "", "headers": {}, "body": "<base64>", "body_truncated": false}
```

`post` also carries `status`, `response_headers` and `response_body`.

| Reply | Effect |
| --- | --- |
| `pre`: empty | Continue |
| `pre`: `{"set_headers": {...}}` | Add request headers |
| `pre`: `{"respond": {"status": 403, "headers": {}, "body": "<base64>"}}` | Answer directly |
| `post`: `status`, `set_headers`, `body` (base64) | Rewrite the response |

Bodies over 4 MiB are not sent (`body_truncated`). Timeouts and errors are fail-open by default; with `fail_open: false` the client gets `502`.

> [!WARNING]
> `post` hooks buffer the whole response. Do not use them on streaming services (event streams, websockets).

## Management

`GET /_gopherstack/extensions` lists name, type (`process` or `container`), status (`starting`, `ready`, `unhealthy`, `restarting`, `failed`, `stopped`), restarts, last error, endpoint and the accepted registration. `GET /_localstack/extensions` serves the same list.

| Endpoint | Purpose |
| --- | --- |
| `POST /_gopherstack/extensions/{name}/restart` | Recycle a running extension, or relaunch a failed or stopped one (resets its restart count) |
| `GET /_gopherstack/extensions/{name}/output` | Last 500 stdout/stderr lines of a process extension, token redacted |

Both are loopback-only. Container extensions have no captured output. The dashboard's Extensions page uses these.

Process extensions run in their own process group. Stopping one sends `SIGTERM` to the group, then `SIGKILL` after 5 s, so `sh -c` and `go run` wrappers leave no orphans. On Windows only the direct child is terminated.

## Example

`examples/extensions/hello` adds `/hello` and logs S3 requests.
