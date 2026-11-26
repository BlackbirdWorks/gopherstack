# CLI commands

`gopherstack` is one binary: bare flags run the server, subcommands manage and talk to it.

| Command | Purpose |
| --- | --- |
| [`serve`](#serve) | Run the server (the default when no subcommand is given) |
| [`health`](#health) | Check a local server's health endpoint |
| [`start`](#start) | Start a managed instance (container or host process) |
| [`stop`](#stop) | Stop the instance started by `start` |
| [`status`](#status) | Show state, version, uptime and services |
| [`logs`](#logs) | Show or follow the managed instance's logs |
| [`reset`](#reset) | Clear in-memory state on a running server |
| [`pod`](#pod) | Save, load and manage named state snapshots |
| [`snapshot`](#snapshot) | Alias of `pod save`, `pod list` and `pod load` |
| [`replicate`](#replicate) | Copy resources from real AWS into a running server |
| [`mcp`](#mcp) | Serve MCP tools over stdio |

Errors print `Error: ...` to stderr and exit 1.

## serve

Runs the server. Flags and environment variables are listed in [Configuration](configuration.md).

## health

Checks `GET /_gopherstack/health` on `localhost:<port>`. Meant for a Docker `HEALTHCHECK`.

```bash
gopherstack health [--port PORT]
```

| Flag | Env | Default | Purpose |
| --- | --- | --- | --- |
| `--port` | `PORT` | `8000` | Port of the running server |

- Prints `ok` and exits 0 on HTTP 200; otherwise exits 1.

## start

Starts a managed instance in a container (default) or as a background host process.

```bash
gopherstack start [flags]
```

| Flag | Env | Default | Purpose |
| --- | --- | --- | --- |
| `--name` | `GOPHERSTACK_NAME` | `gopherstack` | Instance name (container name, state file name) |
| `--state-dir` | `GOPHERSTACK_STATE_DIR` | `$XDG_STATE_HOME/gopherstack` | Directory for state file, logs and data |
| `--image` | `GOPHERSTACK_IMAGE` | `ghcr.io/blackbirdworks/gopherstack` | Container image repository |
| `--tag` | `GOPHERSTACK_TAG` | `latest` | Image tag |
| `--bind` | | `127.0.0.1` | Host address the published ports bind to |
| `--port` | `PORT` | `8000` | Host port for the main HTTP endpoint |
| `--data-dir` | | | Host directory mounted as the data dir |
| `--docker-socket-path` | | | Docker socket to mount; needs `--docker-socket` |
| `-p`, `--publish` | | | Extra ports to publish, `[HOSTPORT:]CONTAINERPORT`; repeatable |
| `-e`, `--env` | | | Server environment, `KEY=VALUE` or `KEY` (copied from your shell); repeatable |
| `--timeout` | | `90s` | How long to wait for the server to become healthy |
| `--host` | | `false` | Run as a background host process, not a container |
| `--foreground` | | `false` | Attach to the container; container mode only |
| `--docker-socket` | | `false` | Mount the host Docker socket; container mode only |
| `--persist` | | `false` | Snapshot-based persistence across restarts |

> [!WARNING]
> `--docker-socket` mounts the host Docker socket into the container so engine-backed services (Lambda, ECS, RDS and others) can start containers. That gives anyone who can reach the server root-equivalent control of the host Docker daemon. It is off by default, and `start` prints a warning to stderr when you enable it. Keep `--bind` on loopback.

- If a server already answers at the bind address and port, prints `gopherstack is already running at <url>` and exits 0.
- Container mode replaces any existing container with the same name, then waits for `GET /_gopherstack/health` (polled every 500 ms) up to `--timeout`.
- Host mode runs the current binary as a background process and writes its log to `<state-dir>/<name>.log`. It also stops waiting early if the process exits.
- If the server is not healthy in time, `start` prints the last 40 lines of output, stops the instance and exits 1.
- State file: `<state-dir>/<name>.json`. The state dir falls back to `~/.local/state/gopherstack`, then to the system temp dir.
- `--persist` uses `<state-dir>/data-<name>` as the data dir unless `--data-dir` is set. The data dir is mounted at `/data` in container mode.
- `--env` with no `=` copies the variable from your shell and is skipped if it is empty or unset.
- `--foreground` streams container logs, stops the container on SIGINT or SIGTERM, and exits with the container's exit code.
- `--host` cannot be combined with `--foreground` or `--docker-socket`. `--docker-socket-path` without `--docker-socket` is an error.
- Socket path order: `--docker-socket-path`, then a `unix://` `DOCKER_HOST` from your shell, then `/var/run/docker.sock`. It is mounted at `/var/run/docker.sock` inside the container.
- With `--data-dir`, `--persist` or `--docker-socket`, the container runs as root (`0:0`).

## stop

Stops the instance `start` created.

```bash
gopherstack stop [--name NAME] [--state-dir DIR]
```

| Flag | Env | Default | Purpose |
| --- | --- | --- | --- |
| `--name` | `GOPHERSTACK_NAME` | `gopherstack` | Instance name |
| `--state-dir` | `GOPHERSTACK_STATE_DIR` | `$XDG_STATE_HOME/gopherstack` | Directory holding the state file |

- Container mode stops and removes the container. Host mode sends a termination signal, waits up to 15 s, then kills the process.
- Prints `gopherstack is not running` and exits 0 if nothing is running.
- Removes the state file.

## status

Shows whether the server is up, with version, uptime, memory and a per-service table.

```bash
gopherstack status [flags]
```

| Flag | Env | Default | Purpose |
| --- | --- | --- | --- |
| `--name` | `GOPHERSTACK_NAME` | `gopherstack` | Instance name |
| `--state-dir` | `GOPHERSTACK_STATE_DIR` | `$XDG_STATE_HOME/gopherstack` | Directory holding the state file |
| `--endpoint` | `GOPHERSTACK_ENDPOINT` | `http://localhost:8000` | Server base URL |
| `--json` | | `false` | Print machine-readable JSON |

- Reads `GET /_gopherstack/health`, and per-service availability from `GET /_localstack/health`.
- If a state file exists and `--endpoint` is left at its default, uses the endpoint recorded in the state file.
- Output includes the managed container or host pid when a state file exists.
- When the server is down it prints `gopherstack is not running (<url>)` and exits 1. With `--json` it prints `{"endpoint": ..., "running": false}` first.

## logs

Prints the managed instance's logs.

```bash
gopherstack logs [-f] [-n LINES] [--name NAME] [--state-dir DIR]
```

| Flag | Env | Default | Purpose |
| --- | --- | --- | --- |
| `--name` | `GOPHERSTACK_NAME` | `gopherstack` | Instance name |
| `--state-dir` | `GOPHERSTACK_STATE_DIR` | `$XDG_STATE_HOME/gopherstack` | Directory holding the state file |
| `-f`, `--follow` | | `false` | Stream new output |
| `-n`, `--tail` | | `100` | Lines from the end |

- Host mode reads `<state-dir>/<name>.log`; container mode reads the container's logs.
- Exits 1 with `gopherstack is not running` if the container does not exist.

## reset

Clears the in-memory state of a running server, for every service or only the ones you name.

```bash
gopherstack reset [--service NAME ...] [--endpoint URL]
```

| Flag | Env | Default | Purpose |
| --- | --- | --- | --- |
| `--endpoint` | `GOPHERSTACK_ENDPOINT` | `http://localhost:8000` | Server base URL |
| `--service` | | all | Only reset these services; repeatable or comma-separated |

> [!CAUTION]
> `reset` discards the current state of the targeted services and cannot be undone. Save a [pod](#pod) first if you may want it back.

- Calls `POST /_gopherstack/reset`, once per `--service`.
- Prints the server's message, for example `reset 3 service(s)`.
- A `--service` name that matches no service resets nothing; the count shows `0`.

## pod

Saves, lists and loads named snapshots ("pods") on a running server. Detail and storage options: [Quickstart: Pods](quickstart.md#pods).

```bash
gopherstack pod <save|list|versions|inspect|load|delete> [name] [flags]
```

Every subcommand takes:

| Flag | Env | Default | Purpose |
| --- | --- | --- | --- |
| `--endpoint` | `GOPHERSTACK_ENDPOINT` | `http://localhost:8000` | Server base URL |

| Subcommand | Purpose | Extra flags |
| --- | --- | --- |
| `pod save <name>` | Save current state as a new version | `-m`/`--message`, `--services` |
| `pod list` | List pods | none |
| `pod versions <name>` | List versions of a pod | none |
| `pod inspect <name>` | Show the manifest of a version | `--version` |
| `pod load <name>` | Load a version into the running server | `--version`, `--strategy`, `--services`, `--merge` |
| `pod delete <name>` | Delete a pod or one version | `--version` |

| Flag | Applies to | Default | Purpose |
| --- | --- | --- | --- |
| `-m`, `--message` | `save` | | Version message |
| `--services` | `save`, `load` | all | Only save or load these services; comma-separated |
| `--version` | `inspect`, `load`, `delete` | `latest` (`inspect`, `load`); whole pod (`delete`) | Version number, or `latest` |
| `--strategy` | `load` | `overwrite` | `overwrite` or `service-merge` |
| `--merge` | `load` | `false` | Merge per account and region instead of replacing whole services |

> [!CAUTION]
> `pod load` with the default `overwrite` strategy replaces all current state. `pod delete` without `--version` deletes the whole pod.

- Prints the server's JSON response, indented. Non-2xx responses exit 1 with the status and body.
- Requests time out after 10 minutes.

## snapshot

Same as `pod`, limited to three subcommands, with identical flags: `snapshot save <name>`, `snapshot list`, `snapshot load <name>`. See [pod](#pod).

## replicate

Copies resources from real AWS (or another AWS-compatible endpoint) into a running server. Resource types and limits: [AWS Replicator](replicator.md).

```bash
gopherstack replicate [flags]
```

| Flag | Env | Default | Purpose |
| --- | --- | --- | --- |
| `--source-profile` | | | AWS shared-config profile for the source account |
| `--source-endpoint` | | | Source endpoint URL; empty means real AWS |
| `--region` | `AWS_REGION` | `us-east-1` | Source region |
| `--target` | `GOPHERSTACK_ENDPOINT` | `http://localhost:8000` | Server base URL to write to |
| `--target-region` | | source region | Target region |
| `--identifier` | | | Only replicate the resource with this name |
| `--resource-type` | | all | Resource type to replicate; repeatable |
| `--max-object-bytes` | | `67108864` | Size cap for copied S3 objects and Lambda code |
| `--dry-run` | | `false` | Show the plan without writing to the target |
| `--include-data` | | `false` | Also copy S3 objects and DynamoDB items |
| `--with-decryption` | | `false` | Decrypt SecureString parameters so values can be copied |
| `--json` | | `false` | Print the report as JSON |

- Prints a `TYPE / ID / ACTION / DETAIL` table and a `created= updated= skipped= failed=` line.
- Exits 1 if any resource failed.
- SIGINT or SIGTERM cancels the run.

## mcp

Serves MCP tools over stdin/stdout, backed by a running server. See [MCP server](mcp.md).

```bash
gopherstack mcp [--endpoint URL] [--read-only]
```

| Flag | Env | Default | Purpose |
| --- | --- | --- | --- |
| `--endpoint` | `GOPHERSTACK_ENDPOINT` | `http://localhost:8000` | Server base URL |
| `--read-only` | `GOPHERSTACK_MCP_READ_ONLY` | `false` | Expose only read-only tools |

- Runs until the client disconnects or the process gets SIGINT or SIGTERM.
