---
service: dynamodb
sdk_module: aws-sdk-go-v2/service/dynamodb   # version: v1.60.0 (go.mod)
last_audit_commit: 33a39b1f
last_audit_date: 2026-07-11
overall: A   # re-audit of Parity sweep 3's map->pkgs/store datalayer refactor; no regressions found
protocol: json-1.0 (DynamoDB_20120810 targets)
families:
  item_crud:    {status: ok, note: PROVEN — condition eval, all ReturnValues, ItemCollectionMetrics/LSI 10GB, WCU/RCU formulas}
  query_scan:   {status: ok, note: PROVEN pagination (LastEvaluatedKey w/ base-PK fusion for GSI/LSI, 1MB/Limit); FIXED Select/COUNT omits Items + Select constraint validation}
  batch:        {status: ok, note: FIXED BatchWriteItem duplicate-key validation (was missing; BatchGetItem had it)}
  transactions: {status: ok, note: FIXED TransactWriteItems Update key-mutation — was NOT validated, silently corrupted pkIndex/pkskIndex (state corruption bug)}
  streams:      {status: ok, note: PROVEN shard-iterator sequence clamping, trim-horizon; streamARNIndex now a store.Table, verified Put/Delete key derivation unchanged}
  janitor_ttl:  {status: ok, note: PROVEN batched-lock, ctx-cancel, quickselect eviction, ring-buffer compaction}
  datalayer:    {status: ok, note: RE-AUDITED — ce30166a converted db.Tables/Backups/GlobalTables/exports/imports/streamARNIndex from raw maps to pkgs/store.Table+Index (composite key tableKey(region,name), region derived by parsing TableArn via tableRegion()). Verified every insertion site (CreateTable, RestoreTable, CreateGlobalTable replicas, cloneTableSchema, applyOneReplicaTableEntry) builds TableArn with the same region string used as the store key *before* Put, so tableRegion(t) round-trips correctly; TableArn is never mutated post-insert. No stale map-key leaks (tablesByRegion Index auto-empties groups on last delete, unlike the old per-region submap). Persistence snapshot reshaped map->sorted slice + added a schema version gate (old snapshots discarded cleanly on upgrade, matching the sqs/ec2 precedent) — intentional, not a parity bug.}
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
- 2026-07-11 re-audit: aws-sdk-go-v2/service/dynamodb bumped f459c9fa's v1.59.2 -> HEAD's v1.60.0 (e51c0de9); diffed api_op_*.go/types.go between the two module versions — zero surface change (v1.60.0's only changelog entry is "Add request serialization snapshot tests"), so no new-op audit was needed this cycle.
- 2026-07-11 re-audit: no real bugs found. All gates pass (build, vet, race tests, go fix -diff empty, golangci-lint 0 issues) with zero working-tree changes required.
