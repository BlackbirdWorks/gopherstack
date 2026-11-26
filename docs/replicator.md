# AWS Replicator

Copies resources from real AWS (or any AWS-compatible endpoint) into a running Gopherstack. It reads the source with the AWS SDK and writes to the target through SDK clients, so each write goes through the real API.

## CLI

```bash
gopherstack replicate \
  --source-profile prod --region eu-west-1 \
  --target http://localhost:8000 \
  --resource-type sqs --resource-type sns
```

| Flag | Meaning |
| --- | --- |
| `--source-profile` | Shared-config profile for the source. Default credential chain if unset |
| `--source-endpoint` | Source endpoint URL (another Gopherstack, LocalStack, ...). Empty means real AWS |
| `--region` | Source region (env `AWS_REGION`, default `us-east-1`) |
| `--target-region` | Target region. Defaults to the source region |
| `--target` | Gopherstack base URL (env `GOPHERSTACK_ENDPOINT`, default `http://localhost:8000`) |
| `--resource-type` | Repeatable. Default: all types below |
| `--identifier` | Replicate only the resource with this name |
| `--dry-run` | Read and plan only; write nothing |
| `--include-data` | Also copy S3 objects and DynamoDB items |
| `--max-object-bytes` | Size cap for S3 objects and Lambda code (default 64 MiB). Larger ones are skipped |
| `--with-decryption` | Decrypt SSM SecureString parameters so they can be copied |
| `--json` | Print the report as JSON |

Exits non-zero if any resource failed.

## HTTP API

`POST /_gopherstack/replicate` starts a job and returns `202 {"id": "repl-...", "status": "running"}`. `GET /_gopherstack/replicate/{id}` returns status, counts and per-resource results.

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

Other body fields: `identifier`, `max_object_bytes`, `source.endpoint`, `source.profile`. `target.endpoint` defaults to the server that received the request.

Source credentials stay in the job goroutine. They are not logged, persisted or returned by the status endpoint.

## Resource types

Types run in dependency order (keys and roles first): `kms, iam, logs, kinesis, s3, dynamodb, ssm, secretsmanager, sqs, lambda, sns, eventbridge`.

| Type | Copied |
| --- | --- |
| `s3` | Buckets, versioning, tags, lifecycle, policy. Objects with `--include-data` |
| `sqs` | Queues, attributes, tags. Dead-letter queues are created first |
| `sns` | Topics, attributes, tags, confirmed subscriptions. SQS, Lambda and Firehose endpoints are remapped to replicated ARNs |
| `dynamodb` | Schema, GSIs/LSIs, billing mode, streams spec, SSE, table class, tags, TTL. Items with `--include-data` |
| `iam` | Customer-managed policies; roles (trust policy, description, tags, boundary); groups; users (tags, boundary, group membership); instance profiles with roles; attached and inline policies |
| `lambda` | Zip functions: configuration, environment, tags, VPC config, code; published versions, aliases (with routing weights), SQS/Kinesis/DynamoDB-stream event source mappings |
| `ssm` | Parameters and tags. SecureString only with `--with-decryption` |
| `secretsmanager` | Current value and version ids, labelled previous versions, description, tags, resource policy, `KmsKeyId` remapped to the replicated key |
| `kms` | Customer-managed key metadata, key policy, tags, aliases |
| `eventbridge` | Custom buses, rules, tags, targets (including `EcsParameters`, `BatchParameters` with remapped ARNs). The default bus gets its rules only |
| `kinesis` | Streams (shards or on-demand), retention, tags |
| `logs` | Log groups, retention, tags |

Account ID and region in ARNs (including inside policy documents and JSON attributes) are rewritten to the target's. Replicated KMS key IDs are mapped too.

Reruns update or skip what exists. Each resource reports `created`, `updated`, `skipped` or `failed` with a reason. A dry run reports what each action would be.

## Lambda details

- Versions are republished in source order. For each missing version the target's `$LATEST` is pointed at that version's code and configuration, published, then restored. Source and target versions pair by position, so reruns publish nothing new. If the source has gaps, the result says `published as version N`. Aliases map to the target version numbers, including `AdditionalVersionWeights`.
- If the source does not expose a version's code (Gopherstack does not), that version is published with the `$LATEST` code and the result says so.
- Event source mappings run after queues, streams and tables. Source ARNs are remapped; a DynamoDB stream maps to the target table's latest stream. A mapping whose source is missing in the target, or whose type is MSK, MQ or DocumentDB, is `skipped`.
- VPC config is applied only if every subnet and security group ID exists in the target (IDs are not remapped). Otherwise the function is copied without it and a `<name>:vpc` result is `skipped`.
- Container-image functions are skipped, with their versions, aliases and mappings. ECR replication is not available.

## Limitations

> [!WARNING]
> KMS key material cannot be exported. Keys are recreated empty with the same metadata and aliases.

- Access keys, login profiles, MFA devices and SSH keys are never copied.
- Lambda layers are referenced by remapped ARN and must already exist in the target.
- SNS subscriptions pending confirmation are skipped.
- Secrets Manager: unlabelled (deprecated) versions are not copied. A `KmsKeyId` whose key was not replicated (or an AWS-managed key) falls back to the target default key, and the result says so.
- EventBridge target `Input` JSON is copied verbatim. Only target, role, dead-letter, ECS task definition and Batch job definition ARNs are remapped.
- Objects and items stream through the process, so large data sets are slow.
