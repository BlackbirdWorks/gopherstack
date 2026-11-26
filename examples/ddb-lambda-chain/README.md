# ddb-lambda-chain

One `PutItem` into `chain-1` cascades through three DynamoDB tables. DynamoDB Streams trigger a Go Lambda that copies the item to the next table.

```
put item -> chain-1 --stream--> chain-fn --put--> chain-2 --stream--> chain-fn --put--> chain-3
```

The Lambda reads the table name from the stream ARN. `chain-3` has no successor, so the chain stops. The handler reacts only to `INSERT` events.

## What it shows

- DynamoDB Streams with the `NEW_IMAGE` view type.
- Event source mappings from stream to function, batch size 10, starting position `LATEST`.
- A Go zip Lambda on `provided.al2` (`bootstrap` entrypoint) running in Docker.
- The Lambda calling DynamoDB back through Gopherstack with the injected `AWS_ENDPOINT_URL` and role credentials.

## Prerequisites

- Docker, AWS CLI v2, Go, `zip`.

> [!WARNING]
> The compose file mounts `/var/run/docker.sock` and runs Gopherstack as root so it can start the Lambda container.

> [!NOTE]
> Gopherstack injects `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_REGION` and `AWS_ENDPOINT_URL` into the function container. The credentials come from assuming the function's execution role as `lambda.amazonaws.com`, so the role must exist in IAM with a Lambda trust policy. `demo.sh` creates `chain-fn-role`. The credential and region keys are reserved and `create-function` rejects them in `Environment`.

## Run

```bash
docker compose up -d --build
docker compose ps        # wait until healthy
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
./demo.sh
```

`demo.sh` builds `lambda/` into `function.zip`, creates the tables, the `chain-fn-role` role, the function and the mappings, inserts the seed item, waits up to 30 s for `chain-3`, then prints each table and the Lambda logs.

Set `ENDPOINT` to use a server other than `http://localhost:8000`. The function reaches Gopherstack at `LAMBDA_DOCKER_HOST`, which the compose file sets to `172.17.0.1` (the Linux Docker bridge gateway). On Docker Desktop run `LAMBDA_DOCKER_HOST=host.docker.internal docker compose up -d --build`.

## Expected output

The tables are printed as DynamoDB JSON. Each holds `id=order-42`, with `hop` 1, 2 and 3 and `from` set to `seed`, `chain-1` and `chain-2`. The script ends with:

```
SUCCESS: event chained chain-1 -> chain-2 -> chain-3 (3 DynamoDB events).
```

Otherwise it prints `FAIL: chain-3 has N items (expected 1).` and exits 1. It deletes the tables, function and role on exit.

## Files

| File | Purpose |
|------|---------|
| `lambda/main.go` | Handler: `events.DynamoDBEvent` to `PutItem` into the next table |
| `demo.sh` | Build, create resources, seed, verify |
| `docker-compose.yml` | Gopherstack with the Docker socket |

## Clean up

```bash
docker compose down -v
```
