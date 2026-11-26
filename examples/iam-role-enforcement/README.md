# iam-role-enforcement

IAM enforcement with STS `AssumeRole`: an assumed role can use one bucket and one table, and is denied on another.

## What it shows

1. The server runs with `--enforce-iam` (`ENFORCE_IAM=true`).
2. Admin creates buckets `public-reports` and `confidential-payroll`, and tables `public-inventory` and `confidential-employees`.
3. Admin creates role `ReportAnalystRole`. Its policy allows:
   - `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` on `public-reports`
   - `dynamodb:GetItem`, `dynamodb:PutItem`, `dynamodb:Scan`, `dynamodb:DescribeTable` on `public-inventory`
   - `sts:GetCallerIdentity`
4. User `analyst-user` assumes the role and gets temporary `ASIA...` credentials plus a session token.
5. With those credentials:

| Resource | Result |
|----------|--------|
| `public-reports` (S3) | allowed |
| `public-inventory` (DynamoDB) | allowed |
| `confidential-payroll` (S3) | denied, HTTP 403 |
| `confidential-employees` (DynamoDB) | denied, `AccessDeniedException` |

## Prerequisites

- Docker (or Go), AWS CLI v2.

## Run

```bash
docker compose up -d --build   # starts Gopherstack with ENFORCE_IAM=true
docker compose ps              # wait until healthy
./demo.sh
```

Without Docker, run `go run . --enforce-iam` from the repo root instead.

Set `ENDPOINT` to use a server other than `http://localhost:8000`.

## Expected output

The script prints numbered sections. It ends with:

```
 ALL TESTS PASSED: IAM & STS Enforcement Verified Live!
```

It exits non-zero if a denied call succeeds. It deletes everything it created on exit.

## Clean up

```bash
docker compose down -v
```
