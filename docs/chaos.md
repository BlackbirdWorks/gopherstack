# Chaos testing

The chaos middleware evaluates every AWS request. With no rules or effects configured it does nothing. Configure it at runtime through `/_gopherstack/chaos`.

| Endpoint | Purpose |
| --- | --- |
| `GET /query` | Faults, effects and recent activity together |
| `GET /faults` | Current fault rules |
| `POST /faults` | Replace all rules |
| `PATCH /faults` | Append rules |
| `DELETE /faults` | Remove matching rules (body: rules to remove) |
| `DELETE /faults/by-index` | Remove one rule; body `{"index": N}` |
| `POST /faults/clear` | Clear all rules |
| `GET /effects` | Current network effects |
| `POST /effects` | Set network effects |
| `POST /effects/reset` | Reset effects to zero |
| `GET /activity` | Recent injected faults, newest first (last 100) |
| `GET /targets` | Discovered services, operations and regions you can target |

All paths are under `/_gopherstack/chaos`. A LocalStack-compatible subset is at `/_localstack/chaos/faults` and `/_localstack/chaos/effects`; see [Migration](migration.md#localstack-internal-endpoints).

## Network effects

```json
{"latency": 100, "jitter": 50, "latencyRange": {"min": 50, "max": 200}}
```

All fields are optional, in milliseconds. Total delay = `latency` + random `0..jitter` + random `min..max`. Effects supersede `--latency-ms` while any field is non-zero.

## Fault rules

`POST /faults` takes an array:

```json
[
  {"service": "dynamodb", "operation": "PutItem", "region": "us-east-1",
   "probability": 0.5,
   "error": {"code": "ProvisionedThroughputExceededException", "statusCode": 400}},
  {"service": "s3", "error": {"code": "ServiceUnavailable", "statusCode": 503}}
]
```

| Field | Meaning |
| --- | --- |
| `service`, `operation`, `region` | Match fields. An omitted field matches anything |
| `probability` | `0.0` to `1.0`. Omitted always fires; an explicit `0` never fires |
| `error` | `code` and `statusCode`. Default: 503 `ServiceUnavailable` |

### `dynamodb:transaction-conflict`

A rule with `effect` is applied by the owning service instead of returning an HTTP error. This one makes targeted DynamoDB items behave as if an unfinished `TransactWriteItems` held them:

- `PutItem`, `UpdateItem`, `DeleteItem` fail with `TransactionConflictException`.
- Transactions fail with `TransactionCanceledException` and `TransactionConflict` reasons.
- `BatchWriteItem` returns `UnprocessedItems`.
- `GetItem` is never affected.

Without a rule, DynamoDB still raises these errors when requests genuinely overlap.

```json
[{"effect": "dynamodb:transaction-conflict", "operation": "PutItem",
  "target": {"table": "orders", "key": {"pk": "order-1"}},
  "probability": 0.3, "holdOps": 2, "holdMs": 500}]
```

- `target.table` and `target.key` narrow the rule. Key values use the scalar string form of the key attributes.
- `holdOps`: after the first forced conflict on an item, the next N operations on it also conflict. `holdMs`: the same for N milliseconds.

## Fault Injection Service

Running a FIS experiment with `aws:fis:inject-api-*` actions creates chaos rules for its duration and removes them afterwards.
