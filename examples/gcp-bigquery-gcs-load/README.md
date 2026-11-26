# gcp-bigquery-gcs-load

Upload a CSV to Cloud Storage, load it into BigQuery, and query it.

## What it shows

1. Upload `sales.csv` to `gs://sales-lake/in/sales.csv`.
2. Run a BigQuery load job (`gs://` URI, CSV, explicit schema, header row skipped) into `sales.orders`. BigQuery reads the object from the Cloud Storage emulator.
3. Run an aggregate query and print the result.

| Service | Port |
|---------|------|
| Cloud Storage | `4443` |
| BigQuery | `9050` |

The app uses the official `cloud.google.com/go/storage` and `cloud.google.com/go/bigquery` clients with no authentication.

## Prerequisites

- Docker, Go, `curl`.

## Run

```bash
docker compose up -d --build
./demo.sh
```

Without Docker, run `go run .` in the repo root instead of the first command. Override endpoints with `GCS_URL` and `BIGQUERY_URL`.

## Expected output

```
uploaded gs://sales-lake/in/sales.csv (145 bytes)
load job done: sales.orders has 6 rows
region    units    revenue
amer         20     110.00
apac         25      86.00
emea         14      65.00
query returned 3 region(s)
PASS: gcp-bigquery-gcs-load (CSV in GCS loaded into BigQuery, aggregate verified)
```

Failures print `FAIL:` and exit non-zero. Re-running is safe: the table is truncated on each load.

## Clean up

```bash
docker compose down -v
```
