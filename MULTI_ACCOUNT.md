# Multi-Account / Multi-Region Isolation

This document describes gopherstack's current account/region model, why full
multi-account / multi-region isolation is **not yet implemented**, what a faithful
implementation would require, and a migration path. It is a design note, not an
implemented feature.

## Current model: single account, single region

gopherstack runs as a single-tenant simulator with one fixed account ID and one
default region:

- The account ID comes from `--account-id` / `ACCOUNT_ID` (default
  `000000000000`) and the region from `--region` / `REGION` / `AWS_REGION` /
  `AWS_DEFAULT_REGION` (default `us-east-1`). Both are surfaced through
  `pkgs/config/config.go` (`GlobalConfig.GetAccountID`, `GetRegion`).
- Every service backend keys its in-memory state **only by resource name/ID**
  (e.g. an SQS queue is keyed by queue name, a DynamoDB table by table name). The
  account ID and region embedded in a request are read for two narrow purposes
  only:
  - **routing** — `httputils.ExtractRegionFromRequest` / `ExtractServiceFromRequest`
    parse the SigV4 `Authorization` credential scope to pick the target service;
  - **ARN construction** — backends stamp the configured account/region into the
    ARNs they return.
- A handful of services thread a per-request region through to a
  region-partitioned store (e.g. Firehose's `regionStore(region)`), but this is
  not consistent across services and there is **no account dimension** anywhere.

Practical consequence: two clients pointed at different account IDs or regions
share the same underlying state. `arn:aws:sqs:us-east-1:111111111111:q` and
`arn:aws:sqs:eu-west-1:222222222222:q` resolve to the *same* queue if the name
matches. This matches LocalStack's open-tier default historically, but diverges
from real AWS and from LocalStack's account/region-keyed stores.

## What full isolation would require

Real AWS partitions every resource by **(partition, account, region)**. A
faithful implementation in gopherstack would need all of the following:

1. **Request-scoped account+region resolution.** A single middleware that derives
   `(accountID, region)` for every request — from the SigV4 credential scope, the
   `X-Amz-*` headers, the host/SNI, or an explicit override — and places it on the
   `context.Context`. Today only region is partially derived and only for routing.

2. **Account+region-keyed backends.** Every service's in-memory maps would change
   from `map[name]*Resource` to `map[accountID]map[region]map[name]*Resource`
   (or an equivalent composite key). This touches **every** backend in
   `services/*` — dozens of stores — plus their persistence snapshots, janitors,
   TTL sweepers, and reset logic.

3. **Cross-service wiring must carry the scope.** Every event/integration path
   (S3→SQS/SNS/Lambda, SNS→*, EventBridge→*, CloudWatch Logs subscription filters,
   Step Functions, Pipes, Scheduler, ESM pollers) currently passes resource
   names/ARNs. Each would need to resolve and propagate the source resource's
   `(account, region)` so the target lookup happens in the correct partition. ARNs
   already encode account+region, so target resolution can key off the ARN — but
   the source-side context and any name-only lookups must be made scope-aware.

4. **ARN parsing as the source of truth.** Where a target is given by ARN, the
   account/region must be read from the ARN rather than the global config. Where a
   target is given by bare name (many APIs), the *caller's* request scope must be
   used.

5. **Persistence format change.** Snapshot files would need to encode the
   account/region dimension so restored state lands in the right partition; this
   is a breaking change to the on-disk format and requires a migration/versioning
   step in `pkgs/persistence`.

6. **DNS, dashboard, health/reset.** Embedded DNS hostname synthesis, the
   dashboard's resource views, and `POST /_gopherstack/reset[?service=…]` would all
   need an account/region filter to remain coherent.

## Why it is deferred

This is a cross-cutting re-architecture of the state-keying scheme in every
service, the persistence format, and every cross-service wiring path. It is high
risk (touches all stored state and all delivery paths at once), cannot be staged
safely inside an unrelated stacked PR, and would regress existing single-account
clients unless gated. It is intentionally **out of scope** here and tracked as a
standalone effort.

## Migration path (incremental, low-risk)

1. **Introduce request scope (no behavior change).** Add an
   `(accountID, region)` value to the request `context.Context` via middleware,
   defaulting to the global config when absent. Backends ignore it at first.

2. **Add a keying abstraction.** Introduce a `scopeKey{account, region}` helper
   and a generic partitioned-store wrapper. Backends opt in one at a time,
   defaulting all reads/writes to the single global scope so behavior is
   identical until a backend is migrated.

3. **Migrate backends incrementally**, highest-value first (DynamoDB, S3, SQS,
   SNS, Lambda), each behind the default-global-scope shim, with per-service tests
   asserting isolation between two scopes.

4. **Make wiring scope-aware** alongside each migrated service: ARN-targeted
   deliveries resolve scope from the ARN; name-targeted deliveries inherit the
   source request scope.

5. **Version the persistence format** to carry the scope dimension, with a
   loader that maps legacy (scopeless) snapshots into the default global scope.

6. **Flip the default** only once every backend and wiring path is scope-aware,
   optionally behind a `--isolate-accounts` flag for one release to allow
   rollback.
