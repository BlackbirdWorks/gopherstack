---
service: dynamodb
sdk_module: aws-sdk-go-v2/service/dynamodb   # version: see go.mod (backfilled)
last_audit_commit: f459c9fa
last_audit_date: 2026-07-04
overall: A   # modest 419 LOC — heavily hardened by prior sweeps; 3 real fixes incl. a state-corruption bug
protocol: json-1.0 (DynamoDB_20120810 targets)
families:
  item_crud:    {status: ok, note: PROVEN — condition eval, all ReturnValues, ItemCollectionMetrics/LSI 10GB, WCU/RCU formulas}
  query_scan:   {status: ok, note: PROVEN pagination (LastEvaluatedKey w/ base-PK fusion for GSI/LSI, 1MB/Limit); FIXED Select/COUNT omits Items + Select constraint validation}
  batch:        {status: ok, note: FIXED BatchWriteItem duplicate-key validation (was missing; BatchGetItem had it)}
  transactions: {status: ok, note: FIXED TransactWriteItems Update key-mutation — was NOT validated, silently corrupted pkIndex/pkskIndex (state corruption bug)}
  streams:      {status: ok, note: PROVEN shard-iterator sequence clamping, trim-horizon}
  janitor_ttl:  {status: ok, note: PROVEN batched-lock, ctx-cancel, quickselect eviction, ring-buffer compaction}
gaps: []
deferred:
  - expr/ lexer/parser/evaluator subpackage (has own aws_spec_test.go/evaluator_test.go) — not line-by-line re-audited
  - PartiQL execution
  - TransactWriteItems Put/Update/Delete/ConditionCheck unused-EAN/EAV validation (bd: gopherstack-daa)
leaks: {status: clean, note: TTL sweeper + stream trimming verified, ctx-cancel present}
---

## Notes
- BatchWriteItem rejects same-key Put+Delete / Put+Put / Delete+Delete in one call: "Provided list of item keys contains duplicates" (verified docs + boto3 history). A prior test asserted the opposite — corrected.
- Select=COUNT returns Count/ScannedCount only, Items omitted.
- Select=SPECIFIC_ATTRIBUTES requires a projection; ALL_PROJECTED_ATTRIBUTES invalid on bare table.
