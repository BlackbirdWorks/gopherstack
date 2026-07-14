---
service: cleanrooms
sdk_module: aws-sdk-go-v2/service/cleanrooms@v1.45.6   # version audited against
last_audit_commit: 42cff5ce624c6c26d806e32ade9b2a0376a0a963
last_audit_date: 2026-07-12
overall: A            # genuine fixes found this pass (route bug, stuck-status bug, tag validation)
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
families:
  Collaboration: {status: ok, note: "CRUD + Members + ChangeRequests audited op-by-op; wire shapes (id/arn/collaborationIdentifier dual keys, epoch createTime/updateTime) verified against deserializers.go"}
  Membership: {status: ok, note: "CRUD audited; DefaultResultConfiguration/PaymentConfiguration pass-through verified"}
  ConfiguredTable: {status: ok, note: "CRUD + AnalysisRule sub-resource audited; cascade delete of analysis rules on DeleteConfiguredTable verified real (not a stub)"}
  ConfiguredTableAssociation: {status: ok, note: "CRUD + nested AnalysisRule audited; cascade delete of ctaAnalysisRules on association delete verified real"}
  AnalysisTemplate: {status: ok, note: "Membership-scoped CRUD + collaboration-scoped Get/List/BatchGet read views audited"}
  Schema/SchemaAnalysisRule: {status: ok, note: "Get/List/BatchGet* read-only views audited; no Create path exists anywhere in this backend (matches real API -- schemas are derived from ConfiguredTable+association state, not directly created), pre-existing and correctly scoped as always-empty until that projection is implemented"}
  ProtectedQuery: {status: ok, note: "FIXED this pass -- see gaps/notes: query used to be permanently stuck at SUBMITTED"}
  ProtectedJob: {status: ok, note: "FIXED this pass -- same stuck-status bug as ProtectedQuery"}
  PrivacyBudgetTemplate: {status: ok, note: "Membership-scoped CRUD + collaboration-scoped read views audited"}
  PrivacyBudget: {status: partial, note: "ListPrivacyBudgets/ListCollaborationPrivacyBudgets always return an empty list -- correct per parity-principles.md rule 4 (validates parent existence then returns real, if empty, state) since there is no CreatePrivacyBudget op in the real API either (budgets are auto-computed from differential-privacy usage, which this backend does not model); documented as a deferred gap, not a stub"}
  IDMappingTable: {status: ok, note: "CRUD + Populate audited"}
  IDNamespaceAssociation: {status: ok, note: "Membership-scoped CRUD + collaboration-scoped read views audited"}
  ConfiguredAudienceModelAssociation: {status: ok, note: "Membership-scoped CRUD + collaboration-scoped read views audited"}
  CollaborationChangeRequest: {status: ok, note: "Create/Get/List/Update audited"}
  Tags: {status: ok, note: "FIXED this pass -- Tag/Untag/ListTagsForResource silently accepted any resourceArn, never returning ResourceNotFoundException for a made-up ARN"}
  RouteMatcher/classifyPath: {status: ok, note: "FIXED this pass -- GetCollaborationAnalysisTemplate was permanently unroutable; see gaps"}
gaps:
  - "ListPrivacyBudgets / ListCollaborationPrivacyBudgets always return an empty budget list. This matches the real API's contract shape (no CreatePrivacyBudget op exists; budgets are server-computed from differential-privacy query usage against a PrivacyBudgetTemplate) but this backend does not model DP budget consumption, so the list is always empty rather than reflecting real usage. Deferred -- would need a differential-privacy accounting model, out of scope for a wire-parity sweep."
  - "PreviewPrivacyImpact returns a fixed empty aggregationCount shape regardless of input parameters -- real AWS computes an actual privacy-impact estimate. Same DP-accounting gap as above."
deferred:
  - "Schema creation/projection from ConfiguredTable+ConfiguredTableAssociation state (pre-existing gap noted in persistence_test.go; not touched this pass, out of scope)"
leaks: {status: clean, note: "Handler.StartWorker is a no-op (no goroutines, no timers, no channels); Reset()/Snapshot()/Restore() only touch store.Registry-managed tables and the plain tagsByArn map. No lifecycle to leak."}
---

## Notes

Protocol: restjson1. Every op's HTTP method + path prefix was cross-checked against
`aws-sdk-go-v2/service/cleanrooms@v1.45.6`'s `serializers.go` httpBindings requestURI
strings (not against this package's own `classifyPath` -- that would be circular).
`services/cleanrooms/handler_route_matcher_test.go` locks this in: it drives every op
through `Handler.RouteMatcher()` + `Handler.ExtractOperation()` directly (unlike every
other test in this package, which registers `h.Handler()` on `e.Any("/*", ...)` and so
never calls `RouteMatcher` at all -- the exact blind spot that hid unroutable op families
in services/backup, eks, s3control, and guardduty in earlier sweeps).

### Bugs fixed this pass

1. **GetCollaborationAnalysisTemplate was permanently unroutable.** Its path parameter,
   `analysisTemplateArn`, is a full ARN of the form
   `arn:aws:cleanrooms:region:account:membership/{id}/analysistemplate/{id}` -- it has two
   literal `/` characters in its *value*. A real aws-sdk-go-v2 client percent-encodes them
   (`%2F`) when building the request, but Go's `net/http` decodes `%2F` back into a literal
   `/` in `req.URL.Path` before this handler ever sees it. `classifyCollabAnalysisTemplates`
   required an exact 4-segment path (`collaborations/{id}/analysistemplates/{arn}`), so any
   real ARN -- which always has embedded slashes for this op -- inflated the segment count
   and fell through to `opUnknown` (404), 100% of the time. Fixed by matching `>=` the
   4-segment floor (mirroring how `classifyTags` already handles `/tags/{resourceArn}`
   correctly via `strings.Join`) and re-joining `segs[3:]` in `injectCollaborationParams`
   instead of taking only `segs[3]`. `AnalysisTemplateArn`/`ResourceArn` are the *only* two
   ARN-shaped URI parameters in this service's SDK model (grepped every `SetURI` call in
   `serializers.go`), so this was the only op affected.

2. **StartProtectedQuery / StartProtectedJob results never left SUBMITTED.** Both ops wrote
   a record with `Status: "SUBMITTED"` and nothing else in this backend ever mutated it --
   no reconciler, no lazy advance, no terminal transition. A real client (Terraform, a
   boto3/Go SDK waiter, or hand-rolled polling code) calling `GetProtectedQuery`/
   `GetProtectedJob` in a loop waiting for a terminal status would poll forever. Fixed with
   a lazy-advance-on-read pattern (`advanceProtectedQueriesLocked` /
   `advanceProtectedJobsLocked`, called from every `Get*`/`List*`): the synchronous
   `StartProtectedQuery`/`StartProtectedJob` response still correctly reports `SUBMITTED`
   (this is the real, AWS-accurate wire contract for the *creation* response -- confirmed by
   the pre-existing `TestParity_ProtectedQueryInitialStatusIsSubmitted`), but any subsequent
   read resolves it to `SUCCESS` (there is no real multi-party compute engine behind this
   emulator to run an actual query/job against, so immediate resolution -- not a fixed delay
   -- is the honest answer, matching the convention `services/athena`'s
   `StartQueryExecution` already uses). `UpdateProtectedQuery`/`UpdateProtectedJob` (the
   cancel op, `targetStatus=CANCELLED`) now also reject cancelling an already-terminal
   query/job with `ConflictException` (new `ErrConflict` sentinel, wired into
   `Handler.handleError`), matching the `ConflictException` AWS documents for
   `UpdateProtectedQuery`/`UpdateProtectedJob` -- without this guard, my status-advance fix
   would have let a client "cancel" an already-succeeded query, a regression the old
   permanently-SUBMITTED behavior couldn't have exhibited.

3. **TagResource / UntagResource / ListTagsForResource never validated the resource ARN.**
   All three silently accepted (or, for List, silently 200'd on) any string as
   `resourceArn`, including ARNs that don't correspond to any real resource. Real AWS
   documents `ResourceNotFoundException` for all three ops. Root cause: `tagsByArn` only
   gains a map entry once a resource is *actually tagged* at creation
   (`if len(tags) > 0`), so a legitimately-existing-but-never-tagged resource is
   indistinguishable from a nonexistent one by map presence alone. Fixed with
   `resourceARNExists`, which scans the 9 taggable resource tables (every struct with both
   an `Arn` field and a `Tags` field: Collaboration, Membership, ConfiguredTable,
   ConfiguredTableAssociation, AnalysisTemplate, PrivacyBudgetTemplate, IDMappingTable,
   IDNamespaceAssociation, ConfiguredAudienceModelAssociation) and returns `ErrNotFound`
   when none match.

### Wire-shape spot checks (no bug found, recorded so the next audit doesn't re-check)

- `createTime`/`updateTime` are epoch-seconds JSON numbers (`float64`), matching
  `smithytime.ParseEpochSeconds` in `deserializers.go` -- correct as-is, no `awstime.Epoch`
  needed since these are already plain numeric fields serialized by `encoding/json`.
- `id`/`collaborationIdentifier` (and the membership/configuredTable/etc. equivalents) are
  intentionally duplicated on every resource struct -- both keys are real, AWS emits both,
  and Terraform's Clean Rooms resources read `id` while some direct SDK call sites still use
  the `*Identifier` key. Not a bug; don't "clean up" the duplication.
- `UntagResource`'s `tagKeys` arrives as a repeated query parameter (`?tagKeys=a&tagKeys=b`),
  confirmed against `serializers.go`'s `encoder.AddQuery("tagKeys")` binding.
  `handleUntagResource`'s query-param fallback is correct.
