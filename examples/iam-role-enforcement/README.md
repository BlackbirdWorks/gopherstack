# IAM Role Enforcement & STS AssumeRole Demo

Demonstrates strict IAM enforcement, STS AssumeRole temporary credentials, and granular cross-service permissions in Gopherstack.

## Scenario

1. Gopherstack runs with `--enforce-iam`.
2. Admin creates two S3 buckets (`public-reports`, `confidential-payroll`) and two DynamoDB tables (`public-inventory`, `confidential-employees`).
3. Admin creates an IAM Role `ReportAnalystRole` with an IAM policy granting:
   - `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` on `arn:aws:s3:::public-reports` and `arn:aws:s3:::public-reports/*`
   - `dynamodb:GetItem`, `dynamodb:PutItem`, `dynamodb:Scan`, `dynamodb:DescribeTable` on `arn:aws:dynamodb:us-east-1:000000000000:table/public-inventory`
   - `sts:GetCallerIdentity`
4. The client assumes `ReportAnalystRole` via `sts:AssumeRole` and receives temporary `ASIA...` credentials with a session token.
5. The assumed role performs:
   - **Allowed**: Read/Write to `public-reports` S3 bucket -> `200 OK`
   - **Allowed**: Read/Write to `public-inventory` DynamoDB table -> `200 OK`
   - **Denied**: Read/Write to `confidential-payroll` S3 bucket -> `403 AccessDenied`
   - **Denied**: Read/Write to `confidential-employees` DynamoDB table -> `403 AccessDenied`

## Running the Demo

```bash
./demo.sh
```
