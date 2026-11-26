# s3-lambda-processor

Uploading an object to S3 invokes a Go Lambda, which reads the object back and logs its content.

## What it shows

- `demo.sh` builds `lambda/` into a `provided.al2` zip (`bootstrap`), then runs `tofu apply` (or `terraform apply`) with `main.tf`.
- `main.tf` creates bucket `incoming-uploads`, role `s3-lambda-exec-role`, Lambda `s3-processor`, a Lambda permission for `s3.amazonaws.com`, and an `s3:ObjectCreated:*` bucket notification.
- The script uploads `testfile.txt` ("Hello from S3 Event Notifications!"), polls the Lambda log group `/aws/lambda/s3-processor` for up to 30 s, prints the function container logs from the Docker socket, and checks for `Successfully read object!`.
- The handler gets the object with `GetObject` (path-style) at the injected `AWS_ENDPOINT_URL`.

## Prerequisites

- Docker, OpenTofu or Terraform, AWS CLI v2, Go, `zip`, `curl`.

> [!WARNING]
> The compose file mounts `/var/run/docker.sock` and runs Gopherstack as root so it can start the Lambda container. `demo.sh` also reads the host Docker socket directly with `curl --unix-socket`.

> [!NOTE]
> `demo.sh` writes `bootstrap` and `function.zip` into this directory. `ENDPOINT` (default `http://localhost:8000`) is passed to the provider as `provider_endpoint`.

## Run

```bash
docker compose up -d --build
docker compose ps        # wait until healthy
./demo.sh
```

On Docker Desktop, start the stack with `LAMBDA_DOCKER_HOST=host.docker.internal docker compose up -d --build`. The default is `172.17.0.1`, the Linux Docker bridge gateway.

## Expected output

```
=== Uploading object to S3 ===
upload: <tmp>/testfile.txt to s3://incoming-uploads/testfile.txt
=== Waiting for Lambda execution ===
=== Lambda Container Logs ===
Logs for gopherstack-lambda-s3-processor-<id>:
... Successfully read object! Size = 35 bytes, Content = "Hello from S3 Event Notifications!\n"
=== Lambda Logs ===
{ "events": [ ... "message": "\"Successfully read object!\"" ... ] }

SUCCESS: Lambda correctly processed the S3 event notification!
```

The script exits 1 with `FAIL: Could not find success message in Lambda logs.` otherwise. It waits a fixed 15 s before printing logs, so a run takes about a minute.

## Files

| File | Purpose |
|------|---------|
| `lambda/main.go` | Handler: `events.S3Event` to `GetObject` |
| `main.tf` | Bucket, role, function, permission, notification |
| `demo.sh` | Build, apply, upload, verify |

## Clean up

```bash
tofu destroy -auto-approve   # or terraform destroy
docker compose down -v
```
