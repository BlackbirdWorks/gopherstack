# azure-services

Blob, Queue and Table Storage plus Cosmos DB (SQL API), all in one Gopherstack instance, driven with `curl`.

## What it shows

| Service | Port | Steps |
|---------|------|-------|
| Blob Storage | `10000` | create container, upload, list, download, delete |
| Queue Storage | `10001` | create queue, enqueue, dequeue, delete message, delete queue |
| Table Storage | `10002` | create table, insert entity, read it back, delete |
| Cosmos DB | `8081` | account root call, create database, container and document, parameterized SQL query, delete |

No credentials are sent. Auth verification is off by default for every Azure service, so no SharedKey signing or Cosmos master key is needed. See [`docs/services/`](../../docs/services/).

## Prerequisites

- Docker (or Go), `curl`, `grep`, `head`, `sed`.

## Run

```bash
docker compose up -d --build
./demo.sh
```

Without Docker, run `go run .` in the repo root instead of the first command.

## Expected output

The script prints each step and ends with:

```
=== All four Azure services exercised successfully ===
```

A non-2xx response prints `FAILED: <step> returned HTTP <status>` and exits non-zero.

## Clean up

```bash
docker compose down -v
```

## See also

- [Blob](../../docs/services/azureblob.md), [Queue](../../docs/services/azurequeue.md), [Table](../../docs/services/azuretable.md), [Cosmos DB](../../docs/services/cosmosdb.md)
- [AZURE.md](../../AZURE.md)
