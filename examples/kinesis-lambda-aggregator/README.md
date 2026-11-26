# kinesis-lambda-aggregator

A Kinesis stream triggers a Node.js Lambda that counts events by type and adds the counts to a DynamoDB table.

```
put-record -> event-stream --event source mapping--> event-processor --UpdateItem--> event-metrics
```

## What it shows

- OpenTofu (`main.tf`) creates the stream `event-stream` (1 shard), the `nodejs20.x` Lambda `event-processor` (`TABLE_NAME=event-metrics`) and an event source mapping with `TRIM_HORIZON` and batch size 100.
- The table `event-metrics` (key `eventType`) is created by a `local-exec` provisioner that runs the AWS CLI, so the AWS CLI must be on the path when `tofu apply` runs.
- `demo.sh` puts five records (`click` x3, `purchase` x2), waits 5 s, then scans the table.
- The Lambda decodes each base64 record, counts `type`, and runs `UpdateItem` with `ADD eventCount`. It uses the injected `AWS_ENDPOINT_URL`.

## Prerequisites

- Docker, OpenTofu (`tofu`), AWS CLI v2.

> [!WARNING]
> The compose file mounts `/var/run/docker.sock` and runs Gopherstack as root so it can start the Lambda container.

> [!NOTE]
> `main.tf` and `demo.sh` use `http://localhost:8000`, so Gopherstack must listen there. `demo.sh` uses `--cli-binary-format raw-in-base64-out`, which needs AWS CLI v2.

## Run

```bash
docker compose up -d --build
docker compose ps        # wait until healthy
./demo.sh
```

On Docker Desktop, start the stack with `LAMBDA_DOCKER_HOST=host.docker.internal docker compose up -d --build`. The default is `172.17.0.1`, the Linux Docker bridge gateway.

## Expected output

After the `tofu apply` output and five `put-record` responses (`ShardId`, `SequenceNumber`):

```
=== Waiting for Event Source Mapping to Poll (5s) ===

=== Aggregated Metrics in DynamoDB ===
[
    {"eventCount": {"N": "3"}, "eventType": {"S": "click"}},
    {"eventCount": {"N": "2"}, "eventType": {"S": "purchase"}}
]
```

(The CLI prints each object over several lines.) The fixed 5 s wait includes the Lambda container start. Re-running `demo.sh` adds to the counts.

## Files

| File | Purpose |
|------|---------|
| `main.tf` | Stream, table, role, Lambda, event source mapping |
| `src/index.js` | Aggregating handler |
| `demo.sh` | Deploy, send events, scan |

## Clean up

```bash
tofu destroy -auto-approve
docker compose down -v
```
