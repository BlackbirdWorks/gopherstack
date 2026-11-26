# MCP server

Gopherstack includes a [Model Context Protocol](https://modelcontextprotocol.io) server that lets AI assistants inspect and control a running instance. Every tool calls an existing admin endpoint.

## Transports

### stdio

`gopherstack mcp` speaks MCP over stdin/stdout and proxies a running instance's admin API (`--endpoint`, env `GOPHERSTACK_ENDPOINT`, default `http://localhost:8000`).

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

Start the server with `--mcp-enabled` (`GOPHERSTACK_MCP=true`). MCP is then served at `/_gopherstack/mcp` on the main port.

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

| `--mcp-token` (`GOPHERSTACK_MCP_TOKEN`) | Who may connect |
| --- | --- |
| Unset (default) | Loopback clients only. Others get `403` |
| Set | Clients sending `Authorization: Bearer <token>`. Others get `401` |

> [!IMPORTANT]
> A published container port is not loopback to the server. Set a token when running in a container.

The stdio transport has the trust of whoever launches it.

## Tools

| Tool | Hint | Backing endpoint |
| --- | --- | --- |
| `gopherstack_health` | read-only | `GET /_gopherstack/health` |
| `snapshot_list` | read-only | `GET /_gopherstack/pods` |
| `snapshot_save` | additive | `POST /_gopherstack/pods/:name` |
| `snapshot_load` | destructive | `POST /_gopherstack/pods/:name/load` |
| `snapshot_delete` | destructive | `DELETE /_gopherstack/pods/:name` |
| `reset_state` | destructive | `POST /_gopherstack/reset` |
| `chaos_list_faults` | read-only | `GET /_gopherstack/chaos/faults` |
| `chaos_set_faults` | destructive | `POST` or `PATCH /_gopherstack/chaos/faults` |
| `chaos_clear_faults` | destructive | `POST /_gopherstack/chaos/faults/clear` |
| `query_traces` | read-only | `GET /_gopherstack/requests` |
| `iam_denials` | read-only | `GET /_gopherstack/iam/policy-stream?decision=deny` |
| `init_hooks_list` | read-only | `GET /_localstack/init` |
| `init_hooks_run` | destructive | `POST /_gopherstack/init/run` (loopback peers only) |

- `init_hooks_run` runs a stage, or one script (`stage`, optional `script` file name), already present in the configured stage directories. Names are matched against discovered scripts, so paths, traversal and inline content are rejected.
- `query_traces` returns method, path, service, operation, status, duration and error details. Headers, bodies and signature query parameters are dropped.
- The resource `gopherstack://info` returns version, uptime and enabled services.

`--mcp-read-only` (`GOPHERSTACK_MCP_READ_ONLY`; also `gopherstack mcp --read-only`) registers only the read-only tools.
