# S3 server access logging example

This example demonstrates gopherstack simulating S3 server access logging:
requests against one bucket asynchronously write AWS-style log records into a
separate target bucket.

## Architecture

```text
┌──────────────┐       s3api get-object        ┌─────────────────────┐
│ demo.sh      │ ────────────────────────────► │ source S3 bucket    │
│ aws-cli      │                               │ app-upload-source   │
└──────┬───────┘                               └──────────┬──────────┘
       │                                                   │
       │ list/get log object                               │ server access log dispatch
       ▼                                                   ▼
┌──────────────────────────────┐              ┌────────────────────────────┐
│ log S3 bucket                │ ◄─────────── │ gopherstack access logger  │
│ app-upload-access-logs/access│              └────────────────────────────┘
└──────────────────────────────┘
```

## What the demo does

1. Creates a source bucket and a target log bucket.
2. Enables server access logging on the source bucket.
3. Uploads a JSON order document to the source bucket.
4. Reads the object back to produce a `REST.GET.OBJECT` access-log record.
5. Polls the log bucket until the access log object appears.
6. Downloads the log record and verifies it names the source bucket and object.

## Run it

```sh
cd examples/s3-access-logs
docker compose up --build --abort-on-container-exit
docker compose down --remove-orphans
```

Expected output ends with:

```text
=== SUCCESS: S3 wrote realistic server access logs to the target bucket ===
```
