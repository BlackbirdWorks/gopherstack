---
service: secretsmanager
sdk_module: aws-sdk-go-v2/service/secretsmanager@v1.42.5
last_audit_commit: ff47d82c
last_audit_date: 2026-07-11
overall: A            # re-audit: zero code drift since prior sweep (ce30166a); confirmed SDK v1.42.5->v1.43.0
                       # bump added zero new/changed ops; corrected one ledger mistake (see error-codes)
ops:
  CreateSecret: {wire: ok, errors: ok, state: ok, persist: ok, note: "added missing ClientRequestToken idempotency contract (matches/mismatches an existing version's content on name collision)"}
  GetSecretValue: {wire: ok, errors: ok, state: ok, persist: ok, note: "VersionId+VersionStage resolution correct; access-day clock now uses injectable b.now()"}
  PutSecretValue: {wire: ok, errors: ok, state: ok, persist: ok, note: "AWSCURRENT/AWSPREVIOUS rotation on staging labels correct; clock consistency fixed"}
  DeleteSecret: {wire: ok, errors: ok, state: ok, persist: ok, note: "force-delete vs 7-30d recovery window, mutual exclusivity with RecoveryWindowInDays, already correct"}
  RestoreSecret: {wire: ok, errors: ok, state: ok, persist: ok}
  ListSecrets: {wire: fixed, errors: ok, state: ok, persist: ok, note: "IncludeDeleted field name was wrong (real key IncludePlannedDeletion); SortBy was entirely unsupported; NextRotationDate was missing from SecretListEntry. All three fixed. RLock no longer lazily mutates the region map (see leaks)."}
  ListSecretVersionIds: {wire: ok, errors: ok, state: ok, persist: ok, note: "RLock no longer lazily mutates the region map (see leaks)"}
  DescribeSecret: {wire: ok, errors: ok, state: ok, persist: ok, note: "RLock no longer lazily mutates the region/replication maps (see leaks); OwnerAccountId is a fabricated field not in the real API — deferred, gopherstack-pct"}
  UpdateSecret: {wire: ok, errors: ok, state: ok, persist: ok, note: "clock consistency fixed (was time.Now(), now b.now())"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  RotateSecret: {wire: ok, errors: partial, state: partial, persist: ok, note: "immediate rotation + Lambda 4-step invocation + AWSPENDING->AWSCURRENT promotion correct; RotateImmediately=false doesn't run the testSecret probe (deferred gopherstack-avt); rotation with no rotation function ever configured is accepted rather than rejected (deferred gopherstack-qqq, intentional test-convenience tradeoff)"}
  GetRandomPassword: {wire: ok, errors: ok, state: ok, note: "length bounds, exclude-chars, require-each-type, crypto/rand rejection sampling all correct"}
  ListAll: {wire: n/a, state: ok, note: "internal dashboard helper, not a wire op"}
  BatchGetSecretValue: {wire: ok, errors: ok, state: ok, persist: ok, note: "clock consistency fixed for LastAccessedDate"}
  CancelRotateSecret: {wire: ok, errors: ok, state: ok, persist: ok}
  GetResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "RLock no longer lazily mutates the region map (see leaks)"}
  PutResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "BlockPublicPolicy default-true + wildcard-principal detection correct"}
  DeleteResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  ReplicateSecretToRegions: {wire: ok, errors: ok, state: ok, persist: ok}
  RemoveRegionsFromReplication: {wire: ok, errors: ok, state: ok, persist: ok}
  StopReplicationToReplica: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateSecretVersionStage: {wire: ok, errors: fixed, state: fixed, persist: ok, note: "was silently stripping a staging label from wherever it happened to be attached, regardless of RemoveFromVersionId; real API requires RemoveFromVersionId to name the current holder or the call fails. Fixed + regression tests added; one existing test (TestRefinement1_UpdateSecretVersionStageAutoStrip) encoded the old wrong behavior and was corrected."}
  ValidateResourcePolicy: {wire: ok, errors: ok, state: ok, note: "RLock no longer lazily mutates the region map (see leaks)"}
families:
  version-staging: {status: ok, note: "AWSCURRENT/AWSPENDING/AWSPREVIOUS transitions, auto-demotion of AWSCURRENT->AWSPREVIOUS on PutSecretValue/UpdateSecret/rotation, max 100 versions with unlabeled-oldest-first pruning — all verified against real semantics"}
  rotation: {status: partial, note: "Lambda 4-step invocation (createSecret/setSecret/testSecret/finishSecret), rate()/cron() schedule parsing and due-date computation, scheduler goroutine with ctx-bounded lifecycle — all correct. Gaps: RotateImmediately=false test-probe (gopherstack-avt), missing-rotation-function validation (gopherstack-qqq)"}
  replication: {status: ok, note: "ReplicateSecretToRegions/RemoveRegionsFromReplication/StopReplicationToReplica + status sync on version change all verified"}
  resource-policy: {status: ok, note: "Get/Put/Delete/Validate + BlockPublicPolicy + MalformedPolicyDocumentException/PublicPolicyException verified"}
  error-codes: {status: ok, note: "ResourceNotFoundException/ResourceExistsException/InvalidRequestException/InvalidParameterException/MalformedPolicyDocumentException/PublicPolicyException all verified against types/errors.go. Re-audit 2026-07-11: fetched the live AWS API reference for TagResource and BatchGetSecretValue — neither operation's documented Errors list includes LimitExceededException (TagResource: InternalServiceError/InvalidParameterException/InvalidRequestException/ResourceNotFoundException only; BatchGetSecretValue adds DecryptionFailure/InvalidNextTokenException, still no LimitExceededException). The previous gopherstack-gvw gap note asserting these ops should return LimitExceededException on tag/SecretIdList limit overflow was an unverified assumption and was WRONG; current InvalidParameterException behavior on both ops is correct AWS parity. CreateSecret's Errors list DOES include LimitExceededException, but AWS doesn't document which specific validation maps to it and CreateSecret's InvalidParameterException-on-tag-overflow is equally consistent with its documented error set, so left as-is (no evidence of a bug, would be speculative to change). gopherstack-gvw should be closed as invalid/works-as-intended."}
  persistence: {status: ok, note: "Snapshot/Restore round-trips all fields including json:\"-\" internal fields via secretSnapshot; Tags.Close() called on replace to avoid Prometheus registry leaks; rotation scheduler re-armed on restore when RotationEnabled"}
  concurrency-locking: {status: fixed, note: "see leaks — RLock-guarded reads were lazily mutating the coarse per-region maps; fixed with non-mutating *StoreRO accessors"}
gaps:
  - RotateSecret accepts rotation with no RotationLambdaARN ever configured on the secret or in the request (real AWS requires an existing rotation strategy or managed rotation) — kept as-is because dozens of existing tests rely on the lenient no-Lambda immediate-value-regen behavior as a test convenience, and gopherstack does not model AWS managed rotation at all (bd: gopherstack-qqq)
  - RotateSecret with RotateImmediately=false does not invoke the Lambda testSecret probe step or create/remove a transient AWSPENDING version (bd: gopherstack-avt) — re-confirmed 2026-07-11 against the live AWS API reference for RotateSecret, which explicitly documents this exact behavior ("Secrets Manager tests the rotation configuration by running the testSecret step... This test creates an AWSPENDING version of the secret and then removes it"), so the gap description is accurate and still open
  - DescribeSecretOutput/SecretListEntry expose a fabricated OwnerAccountId field not present in the real API (harmless — unknown JSON fields are ignored by real deserializers); managed-external-secret fields (ExternalSecretRotationMetadata/RoleArn, OwningService, Type) are entirely unmodeled, so the "owning-service" ListSecrets/BatchGetSecretValue filter always passes rather than matching a tracked owning service (bd: gopherstack-pct)
deferred:
  - Managed rotation (AWS-service-owned secrets, e.g. RDS-managed rotation) — out of scope, not modeled at all
  - Cross-account resource-policy principal evaluation beyond the wildcard-principal BlockPublicPolicy heuristic
leaks: {status: fixed, note: "Found a real data race: ListSecrets/ListSecretVersionIds/DescribeSecret/GetResourcePolicy/ValidateResourcePolicy held only an RLock (shared reader lock) but called the lazily-creating *Store(region) helper, which does `b.secrets[region] = make(...)` on first touch of a region — a concurrent map write happening under a read lock. Confirmed with a regression test (concurrency_race_test.go) that reproduces the `go test -race` data race pre-fix and passes clean post-fix. Fixed by adding non-mutating *StoreRO(region) accessors for read-only call sites (a nil-map read/range is well-defined in Go, so no mutation is needed there). Rotation scheduler goroutine, janitor, and StopRotationScheduler/Shutdown ctx-cancellation lifecycle were already clean (verified, no changes needed)."}
---

## Notes

- **Protocol**: `application/x-amz-json-1.1` (awsJson1_1), matches `secretsmanager.<Operation>`
  `X-Amz-Target` routing already in place in `handler.go`.
- **Wire field names verified directly against `aws-sdk-go-v2/service/secretsmanager@v1.42.5`
  serializers.go/deserializers.go** (not just the Go struct tags), which is how the
  `IncludeDeleted` → `IncludePlannedDeletion` and `owned-by-me` → `owning-service` bugs were
  caught: both were plausible-looking names that don't exist on the wire. A previous audit
  pass invented "owned-by-me" for account-ownership semantics, but the real
  `FilterNameStringType` enum has no such value — the real "owning-service" key is about
  AWS-service-managed secrets (e.g. RDS-managed rotation), a different concept entirely.
  Renamed to the real key while preserving pass-all semantics, since this mock never
  tracks AWS-service ownership of secrets.
- **Timestamps** are epoch-seconds JSON numbers (`float64` via `UnixTimeFloat`), matching
  `smithytime.ParseEpochSeconds` in the real deserializers — already correct throughout.
- **Clock consistency**: `InMemoryBackend.now` is an injectable clock (`SetNowForTest` in
  `export_test.go`) used correctly by `DeleteSecret`/rotation, but `CreateSecret`,
  `seedInitialVersion`, `PutSecretValue`, `UpdateSecret`, `GetSecretValue`, and
  `BatchGetSecretValue`'s access-day tracking all called `time.Now()` directly, bypassing
  the injected clock. Fixed for internal consistency and testability — production behavior
  is unchanged since the default `now` is `time.Now`.
- **RemoveFromVersionId semantics** (`UpdateSecretVersionStage`): the real API requires the
  caller to name the version that currently holds a staging label before it can be moved
  elsewhere — "if the label is attached and you either do not specify [RemoveFromVersionId],
  or the version ID does not match, then the operation fails." This mock silently stripped
  the label from wherever it was, which is a **looks-wrong-but-was-actually-a-bug** case: an
  existing test (`TestRefinement1_UpdateSecretVersionStageAutoStrip`) explicitly asserted the
  permissive (wrong) behavior as correct. Fixed the backend and corrected that test rather
  than working around it.
- **CreateSecret idempotency**: the real API's `ClientRequestToken` doc text is explicit
  about the three-way branch (new version / matching retry ignored / mismatched content
  fails) — this was previously entirely unimplemented for the CreateSecret-level name
  collision case (it existed for `PutSecretValue`/`UpdateSecret`, just not `CreateSecret`).
- **Region-nested maps**: `InMemoryBackend.secrets`/`resourcePolicies`/`replicationConfigs`
  are `map[string]map[string]T]` (outer key = region), lazily created per-region by
  `*Store(region)` helpers. Those helpers **must only be called under `b.mu.Lock()`** (write
  lock) — read paths must use the new `*StoreRO(region)` accessors instead. This is the kind
  of thing that's easy to reintroduce; grep for `RLock(` and confirm no `*Store(` (non-RO)
  calls appear before the matching `RUnlock`/`defer`.
- **`GetSupportedOperations`/dispatch-table op-name strings**: several operation names are
  used in three or more places (the supported-ops list, the dispatch map key, and a
  `lockmetrics` label in `backend.go`) — collapsed the four that tripped `goconst`
  (`DescribeSecret`, `GetResourcePolicy`, `ListSecrets`, `ValidateResourcePolicy`) into shared
  `opXxx` constants. Not exhaustive for every op name (only fixes what already tripped the
  linter); a future full pass could do this for all ~20 ops for consistency.
- **Test files in this package are numerous** (from several prior sweeps —
  `batch1_audit_test.go`, `accuracy_audit_test.go`, `parity_a_test.go`,
  `parity_deepen_test.go`, `handler_refinement1/2_test.go`, etc.). Before adding a new test,
  grep first — there is a good chance similar coverage already exists under a different name.
- **2026-07-11 re-audit**: the ledger's stated `last_audit_commit` (`f093a929`) turned out not
  to be an ancestor of current HEAD (rebased/squashed history elsewhere in the repo); the real
  prior audit commit for this package is `ce30166a` ("Parity sweep 3"), which is the commit
  that wrote this PARITY.md. `git diff ce30166a..HEAD -- services/secretsmanager/` is empty —
  **zero code drift** in this package since that audit. The SDK bumped
  `aws-sdk-go-v2/service/secretsmanager` v1.42.5 → v1.43.0 in the interim
  (`e51c0de9`); diffed the two module versions on disk and confirmed the only changes are
  `CHANGELOG.md`/`generated.json`/`go_module_metadata.go` plus new snapshot-test fixtures —
  no operation or shape changes. Per the re-audit protocol this meant auditing only the ledger's
  non-`ok` rows: fetched the live AWS API reference pages for `TagResource`,
  `BatchGetSecretValue`, `CreateSecret`, and `RotateSecret` to check the two `partial` gaps.
  Result: the `error-codes`/gopherstack-gvw gap was a **false positive from a prior audit** —
  neither `TagResource` nor `BatchGetSecretValue` documents `LimitExceededException` as a
  possible error, so the existing `InvalidParameterException` behavior on tag/SecretIdList
  limit overflow is correct AWS parity and needed no code change (closed the gap in the ledger,
  should close bd `gopherstack-gvw` as invalid). The `RotateSecret` gaps
  (gopherstack-avt/gopherstack-qqq) were re-verified against the live docs and are accurately
  described — left as-is, still open. No code changes were made this pass; gates
  (`build`/`vet`/`test -race`/`go fix -diff`/`golangci-lint`) all pass clean on the unmodified
  tree.
