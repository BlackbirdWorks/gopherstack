# apigw-websocket-chat

An API Gateway WebSocket API backed by a Node.js Lambda that echoes each message back to the sender.

## What it shows

- OpenTofu (`main.tf`) creates a WebSocket API `chat-api` (`route_selection_expression = "$request.body.action"`), a `nodejs20.x` Lambda `chat-handler`, an `AWS_PROXY` integration, and the `$connect`, `$disconnect` and `$default` routes.
- A stage `prod` with `auto_deploy`.
- On `$connect` and `$disconnect` the Lambda logs and returns 200. On any other message it calls `PostToConnection` (API Gateway Management API) at `AWS_ENDPOINT_URL` and sends `Echo: <body>` back.

## Prerequisites

- Docker, OpenTofu (`tofu`), `wscat` (`npm install -g wscat`).

> [!WARNING]
> The compose file mounts `/var/run/docker.sock` and runs Gopherstack as root so it can start the Lambda container.

> [!NOTE]
> `main.tf` points the AWS provider at `http://localhost:8000`, so Gopherstack must listen there.

## Run

```bash
docker compose up -d --build
docker compose ps        # wait until healthy
./demo.sh
```

`demo.sh` runs `tofu init` and `tofu apply`, then prints the `wscat` command. It does not open a connection itself.

On Docker Desktop, start the stack with `LAMBDA_DOCKER_HOST=host.docker.internal docker compose up -d --build`. The default is `172.17.0.1`, the Linux Docker bridge gateway.

## Expected output

```
websocket_url = "https://<api-id>.execute-api.us-east-1.amazonaws.com/prod"
=== Done ===
The WebSocket API has been provisioned. To test it, use wscat:
  wscat -c ws://localhost:8000/v2proxy/<api-id>/prod
Then send any text; the Lambda echoes it back as 'Echo: <text>'.
```

The `websocket_url` output is the AWS-style host and is not reachable. Connect with the `ws://localhost:8000/v2proxy/<api-id>/prod` URL the script prints:

```
$ wscat -c ws://localhost:8000/v2proxy/<api-id>/prod
Connected (press CTRL+C to quit)
> {"action":"msg","text":"hi"}
< Echo: {"action":"msg","text":"hi"}
```

## Files

| File | Purpose |
|------|---------|
| `main.tf` | API, routes, stage, Lambda, IAM role |
| `src/index.js` | Handler for `$connect`, `$disconnect` and the echo |
| `demo.sh` | `tofu init`, `tofu apply`, prints the `wscat` URL |

## Clean up

```bash
tofu destroy -auto-approve
docker compose down -v
```
