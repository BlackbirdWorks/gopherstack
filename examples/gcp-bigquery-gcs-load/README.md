# gcp-bigquery-gcs-load

The classic batch-load path on Google Cloud, against gopherstack:

1. Upload `sales.csv` to a Cloud Storage bucket (`gs://sales-lake/in/sales.csv`).
2. Run a BigQuery **load job** (`gs://` URI, CSV, explicit schema, header row skipped) into `sales.orders`. BigQuery reads the object from the Cloud Storage emulator.
3. Run an aggregate query and print the result.

Ports: Cloud Storage `4443`, BigQuery `9050`. No credentials are used (`WithoutAuthentication`).

The app is plain Go on the official `cloud.google.com/go/storage` and `cloud.google.com/go/bigquery` clients (own `go.mod`, same versions as the root module, so it builds offline from the module cache). The `google` Terraform/OpenTofu provider can create the same bucket and dataset (see `test/terraform/gcp`), but it needs a network download of the provider, so this example stays Go-only.

## Prerequisites

- A Go toolchain.
- `curl`.
- Gopherstack running: `docker-compose up -d` here, or `go run ../..` from the repo root.

## Running

```bash
docker-compose up -d
./demo.sh
```

Override endpoints with `GCS_URL` and `BIGQUERY_URL`.

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

The script exits non-zero with a `FAIL:` line otherwise. Re-running is safe (the table is truncated on each load).

## Teardown

```bash
docker-compose down -v
```
