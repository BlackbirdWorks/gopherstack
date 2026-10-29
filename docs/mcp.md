# MCP server

Gopherstack embeds a [Model Context Protocol](https://modelcontextprotocol.io) server
(built on the official Go SDK) that lets AI assistants inspect and control a running
instance. Every tool is backed by an existing admin endpoint; nothing new is exposed.

## Transports

### stdio

`gopherstack mcp` speaks MCP over stdin/stdout and proxies a running instance's admin
API (`--endpoint`, `GOPHERSTACK_ENDPOINT`, default `http://localhost:8000`).

```json
{
  "mcpServers": {
    "gopherstack": {
      "command": "gopherstack",
      "args": ["mcp", "--endpoint", "http://localhost:8000"]
    }
  }
}
```

### Streamable HTTP

Opt in with `--mcp-enabled` (`GOPHERSTACK_MCP=true`). The server then answers MCP at
`/_gopherstack/mcp` on the main port.

```json
{
  "mcpServers": {
    "gopherstack": {
      "type": "http",
      "url": "http://localhost:8000/_gopherstack/mcp",
      "headers": { "Authorization": "Bearer ${GOPHERSTACK_MCP_TOKEN}" }
    }
  }
}
```

## Authentication

| `--mcp-token` | Who may connect |
| --- | --- |
| unset (default) | Loopback clients only; others get `403`. A published container port is not loopback to the server, so set a token there. |
| set (or `GOPHERSTACK_MCP_TOKEN`) | Any client presenting `Authorization: Bearer <token>`; others get `401`. |

The stdio transport inherits the trust of whoever launches it.

## Tools

| Tool | Hints | Backing endpoint |
| --- | --- | --- |
| `gopherstack_health` | read-only | `GET /_gopherstack/health` |
| `snapshot_list` | read-only | `GET /_gopherstack/pods` |
| `snapshot_save` | additive | `POST /_gopherstack/pods/:name` |
| `snapshot_load` | destructive | `POST /_gopherstack/pods/:name/load` |
| `snapshot_delete` | destructive | `DELETE /_gopherstack/pods/:name` |
| `reset_state` | destructive | `POST /_gopherstack/reset` |
| `chaos_list_faults` | read-only | `GET /_gopherstack/chaos/faults` |
| `chaos_set_faults` | destructive | `POST` / `PATCH /_gopherstack/chaos/faults` |
| `chaos_clear_faults` | destructive | `POST /_gopherstack/chaos/faults/clear` |
| `query_traces` | read-only | `GET /_gopherstack/requests` |
| `iam_denials` | read-only | `GET /_gopherstack/iam/policy-stream?decision=deny` |
| `init_hooks_list` | read-only | `GET /_localstack/init` |

Init hooks run once at startup, so there is no tool to run them on demand.
`query_traces` returns method, path, service, operation, status, duration and error
details only; headers, bodies and signature query parameters are dropped. The
resource `gopherstack://info` returns version, uptime and enabled services.
