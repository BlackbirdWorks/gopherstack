# AWS Replicator

Copy resources from a real AWS account (or any AWS-compatible endpoint) into a running
Gopherstack. The replicator reads the source with the AWS SDK and writes the target through
the normal SDK clients pointed at Gopherstack, so every write exercises the real API.

## CLI

```
gopherstack replicate \
  --source-profile prod --region eu-west-1 \
  --target http://localhost:8000 \
  --resource-type sqs --resource-type sns \
  [--identifier NAME] [--dry-run] [--include-data] [--with-decryption] [--json]
```

| Flag | Meaning |
| --- | --- |
| `--source-profile` | Shared-config profile for the source; default credential chain otherwise |
| `--source-endpoint` | Source endpoint URL (another Gopherstack, LocalStack, ...); empty means real AWS |
| `--region` / `--target-region` | Source region; target region defaults to it |
| `--target` | Running Gopherstack base URL (default `http://localhost:8000`, `GOPHERSTACK_ENDPOINT`) |
| `--resource-type` | Repeatable; default is every type below |
| `--identifier` | Replicate only the resource with this name |
| `--dry-run` | Read and plan only; nothing is written to the target |
| `--include-data` | Also copy S3 objects and DynamoDB items |
| `--max-object-bytes` | Size cap for S3 objects and Lambda code (default 64 MiB); larger objects are skipped |
| `--with-decryption` | Decrypt SSM SecureString parameters so they can be copied |

The command exits non-zero when any resource failed.

## HTTP API

`POST /_gopherstack/replicate` starts a job and returns `202 {"id": "repl-...", "status": "running"}`.
`GET /_gopherstack/replicate/{id}` returns status, counts and per-resource results as they complete.

```json
{
  "source": {"region": "eu-west-1", "access_key_id": "...", "secret_access_key": "...", "session_token": "..."},
  "target": {"endpoint": "http://localhost:8000"},
  "resource_types": ["sqs", "sns"],
  "dry_run": false,
  "include_data": false,
  "with_decryption": false
}
```

`target.endpoint` defaults to the server receiving the request. Source credentials live only in
the job goroutine: they are never logged, never persisted and never returned by the status
endpoint. `source.endpoint` and `source.profile` are also accepted.

## Supported types

Types run in dependency order (keys and roles first, rules last):
`kms, iam, logs, kinesis, s3, dynamodb, ssm, secretsmanager, sqs, lambda, sns, eventbridge`.

| Type | Copied |
| --- | --- |
| `s3` | Buckets, versioning, tags, lifecycle, policy; objects with `--include-data` (size capped) |
| `sqs` | Queues, attributes, tags; dead-letter queues are created before the queues that redrive to them |
| `sns` | Topics, attributes, tags, and confirmed subscriptions; SQS/Lambda/Firehose endpoints are remapped to replicated ARNs |
| `dynamodb` | Schema, GSIs/LSIs, billing mode, streams spec, SSE, table class, tags, TTL; items with `--include-data` |
| `iam` | Customer-managed policies, roles (trust policy, description, tags, boundary), attached policies, inline policies |
| `lambda` | Zip functions: configuration, environment, tags and code (downloaded from the presigned `Code.Location`) |
| `ssm` | Parameters and tags; SecureString only with `--with-decryption` (skipped otherwise) |
| `secretsmanager` | Secrets (current value), description, tags, resource policy |
| `kms` | Customer-managed key metadata, key policy, tags, aliases |
| `eventbridge` | Custom buses, rules, tags and targets (the default bus gets its rules only) |
| `kinesis` | Streams (shards or on-demand), retention, tags |
| `logs` | Log groups, retention, tags |

Account id and region in ARNs (including inside policy documents and JSON attributes) are
rewritten to the target's account and region. Replicated KMS key ids are mapped too.

## Idempotency and results

Re-running updates or skips what already exists. Each resource yields `created`, `updated`,
`skipped` or `failed` with a reason; a dry run reports what each action would be.

## Limitations

- KMS key material cannot be exported: keys are recreated empty with the same metadata and aliases.
- Lambda container-image functions are skipped; VPC config, aliases, versions and event source
  mappings are not copied. Layers are referenced by remapped ARN and must exist in the target.
- SNS subscriptions pending confirmation are skipped.
- IAM users, groups and instance profiles are not copied.
- Secrets Manager: only the current version is copied; KMS key bindings are dropped.
- Objects and items are streamed through the process; very large data sets are slow.
