# s3-access-logs

Requests against one bucket write S3 server access log records into a second bucket.

## What it shows

1. Create `app-upload-source` and `app-upload-access-logs`.
2. Enable server access logging on the source bucket, with the `access/` prefix.
3. Upload `incoming/order-1001.json` and read it back. The read produces a `REST.GET.OBJECT` record.
4. Poll the log bucket until a log object appears.
5. Download it and check that it names the source bucket, the object key and status `200`.

## Prerequisites

- Docker, AWS CLI v2.

## Run

```bash
docker compose up -d --build
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
./demo.sh
```

Set `ENDPOINT` to use a server other than `http://localhost:8000`.

## Expected output

Ends with:

```
=== SUCCESS: S3 wrote realistic server access logs to the target bucket ===
```

If no log object appears, the script prints `ERROR: no access log object appeared` and exits 1. It deletes both buckets on exit.

## Clean up

```bash
docker compose down -v
```
