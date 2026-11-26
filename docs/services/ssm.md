# Systems Manager

In-memory Systems Manager. Parameter Store, documents, Run Command, Automation, Maintenance Windows, Patch Manager, State Manager associations, Inventory, OpsCenter and Session Manager.

## Quick start

```bash
export AWS_ENDPOINT_URL=http://localhost:8000

aws ssm put-parameter --name "/myapp/config/region" --value "us-east-1" --type String
aws ssm put-parameter --name "/myapp/secrets/db" --value "supersecret" --type SecureString
aws ssm get-parameter --name "/myapp/secrets/db" --with-decryption
aws ssm get-parameters-by-path --path "/myapp/config" --recursive
aws ssm put-parameter --name "/myapp/config/region" --value "eu-west-1" --type String --overwrite
```

## Supported operations

| Area | Operations |
|------|------------|
| Parameter Store | `PutParameter`, `GetParameter`, `GetParameters`, `GetParametersByPath`, `GetParameterHistory`, `DescribeParameters`, `DeleteParameter`, `DeleteParameters`, `LabelParameterVersion`, `UnlabelParameterVersion` |
| Documents | create, get, update, delete, list, describe, versions, default version, permissions, metadata |
| Run Command | `SendCommand`, `ListCommands`, `ListCommandInvocations`, `GetCommandInvocation`, `CancelCommand` |
| Automation | start, stop, signal, describe and list executions, step executions, execution preview |
| Maintenance windows | windows, targets, tasks, executions, schedules |
| Patch Manager | patch baselines, patch groups, patch states, available patches, default baseline |
| Associations | State Manager associations, versions, executions and statuses |
| Inventory | inventory entries, schema, deletions, compliance items and summaries |
| OpsCenter | OpsItems, events, related items, OpsMetadata, summaries |
| Managed instances | instance information, activations, managed instance role, connection status |
| Sessions | session start, resume, terminate and describe |
| Resource policies | put, get, delete |
| Other | resource data sync, service settings, cloud connectors, access requests, tags |

Public parameters under `/aws/service/...` and `/aws/reference/secretsmanager/<name>` references are resolved.

## Behaviour

- `SecureString` values are stored encrypted: with the KMS service when a `KeyId` is given, otherwise with a built-in cipher.
- Standard and Advanced tiers are enforced. A value over the standard size limit, or any parameter policy, upgrades the parameter to Advanced.
- Parameter policies (expiration and no-change notifications) are supported.
- `SendCommand` runs scripts through `docker exec` for Docker-backed EC2 instances with an instance profile, and for instances whose agent polls `ec2messages`. Other targets are simulated from the document's echo and exit statements.

## Configuration

| Env | Default | Purpose |
|-----|---------|---------|
| `SSM_JANITOR_INTERVAL` | `30s` | Janitor tick |
| `SSM_COMMAND_TTL` | `1h` | How long `SendCommand` results are kept |

## Differences from AWS

> [!NOTE]
> Maintenance window `StartDate`, `EndDate`, `ScheduleTimezone` and `ScheduleOffset` are stored but not evaluated. A window has one synthetic always-on execution.

- `RejectedPatchesAction` and `PatchBaseline.GlobalFilters` need a real patch dependency graph: the synthetic patch catalogue is recomputed per run.
- `GetInventory` `Aggregators` and `ResultAttributes` are not supported.
- `ListResourceDataSync` items never report a completed sync: there is no sync engine.
- `ValidateCloudConnector` makes no real Azure call. Findings derive from the stored configuration.
- `GetParameter*` `SourceResult` is filled only for Secrets Manager references, not for `aws:ec2:image` advanced parameters.
- `DocumentHash`, `AlarmConfiguration`, `NotificationConfig` and `TriggeredAlarms` on simulated commands stay simulated.
