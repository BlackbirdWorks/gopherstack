---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: directoryservice
sdk_module: aws-sdk-go-v2/service/directoryservice@v1.38.20   # version audited against
last_audit_commit: 1c6af314f4ed210dbc03be80042c6af2aa07448f   # HEAD when this manifest was written
last_audit_date: 2026-07-12
overall: A            # ~1k genuine fixes found (persistence registration + systemic epoch-timestamp wire bug)
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateDirectory: {wire: ok, errors: ok, state: ok, persist: ok, note: "Requested->Creating->Active lifecycle goroutine"}
  CreateMicrosoftAD: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDirectory: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascades all dependent resources"}
  DescribeDirectories: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateAlias: {wire: ok, errors: ok, state: ok, persist: ok}
  EnableSso: {wire: ok, errors: ok, state: ok, persist: ok}
  DisableSso: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDirectoryLimits: {wire: ok, errors: ok, state: ok, persist: n/a, note: "computed, not stored"}
  CreateSnapshot: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteSnapshot: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeSnapshots: {wire: ok, errors: ok, state: ok, persist: ok}
  GetSnapshotLimits: {wire: ok, errors: ok, state: ok, persist: n/a}
  RestoreFromSnapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "Restoring->Active lifecycle goroutine"}
  AddTagsToResource: {wire: ok, errors: ok, state: ok, persist: ok}
  RemoveTagsFromResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  AddIpRoutes: {wire: FIXED, errors: ok, state: ok, persist: ok, note: "AddedDateTime was ISO8601 string, now awstime.Epoch"}
  RemoveIpRoutes: {wire: ok, errors: ok, state: ok, persist: ok}
  ListIpRoutes: {wire: FIXED, errors: ok, state: ok, persist: ok, note: "AddedDateTime epoch fix"}
  AddRegion: {wire: ok, errors: ok, state: ok, persist: ok}
  RemoveRegion: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeRegions: {wire: FIXED, errors: ok, state: ok, persist: ok, note: "LaunchTime epoch fix"}
  StartSchemaExtension: {wire: ok, errors: ok, state: ok, persist: ok}
  CancelSchemaExtension: {wire: ok, errors: ok, state: ok, persist: ok}
  ListSchemaExtensions: {wire: FIXED, errors: ok, state: ok, persist: ok, note: "StartDateTime/EndDateTime epoch fix"}
  CreateConditionalForwarder: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateConditionalForwarder: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteConditionalForwarder: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeConditionalForwarders: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateLogSubscription: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteLogSubscription: {wire: ok, errors: ok, state: ok, persist: ok}
  ListLogSubscriptions: {wire: FIXED, errors: ok, state: ok, persist: ok, note: "SubscriptionCreatedDateTime epoch fix"}
  RegisterEventTopic: {wire: ok, errors: ok, state: ok, persist: ok}
  DeregisterEventTopic: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeEventTopics: {wire: FIXED, errors: ok, state: ok, persist: ok, note: "CreatedDateTime epoch fix"}
  DescribeDomainControllers: {wire: FIXED, errors: ok, state: ok, persist: ok, note: "LaunchTime epoch fix"}
  UpdateNumberOfDomainControllers: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateTrust: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteTrust: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeTrusts: {wire: FIXED, errors: ok, state: ok, persist: ok, note: "CreatedDateTime/LastUpdatedDateTime epoch fix"}
  UpdateTrust: {wire: ok, errors: ok, state: ok, persist: ok}
  VerifyTrust: {wire: ok, errors: ok, state: ok, persist: ok}
  ShareDirectory: {wire: FIXED, errors: ok, state: FIXED, persist: ok, note: "HANDSHAKE now starts PendingAcceptance (was Shared, skipping the handshake); ORGANIZATIONS starts Shared"}
  UnshareDirectory: {wire: ok, errors: ok, state: ok, persist: ok}
  AcceptSharedDirectory: {wire: ok, errors: ok, state: ok, persist: ok}
  RejectSharedDirectory: {wire: ok, errors: ok, state: FIXED, persist: ok, note: "was setting ShareStatus=RejectFailed (the AWS enum value for a FAILED reject) on every SUCCESSFUL reject; now Rejected"}
  DescribeSharedDirectories: {wire: FIXED, errors: ok, state: ok, persist: ok, note: "CreatedDateTime/LastUpdatedDateTime epoch fix"}
  RegisterCertificate: {wire: ok, errors: ok, state: ok, persist: ok, note: "CommonName hardcoded to example.com -- no cert parsing; acceptable emulation shortcut, no wire/error impact"}
  DeregisterCertificate: {wire: ok, errors: ok, state: ok, persist: ok}
  ListCertificates: {wire: FIXED, errors: ok, state: ok, persist: ok, note: "ExpiryDateTime epoch fix"}
  DescribeCertificate: {wire: FIXED, errors: ok, state: ok, persist: ok, note: "RegisteredDateTime/ExpiryDateTime epoch fix"}
  EnableLDAPS: {wire: ok, errors: ok, state: ok, persist: ok}
  DisableLDAPS: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeLDAPSSettings: {wire: FIXED, errors: ok, state: ok, persist: ok, note: "LastUpdatedDateTime/CertificateExpiryDateTime epoch fix"}
  EnableClientAuthentication: {wire: ok, errors: ok, state: ok, persist: ok}
  DisableClientAuthentication: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeClientAuthenticationSettings: {wire: FIXED, errors: ok, state: ok, persist: ok, note: "LastUpdatedDateTime epoch fix"}
  EnableRadius: {wire: ok, errors: ok, state: ok, persist: ok}
  DisableRadius: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateRadius: {wire: ok, errors: ok, state: ok, persist: ok}
  EnableDirectoryDataAccess: {wire: ok, errors: ok, state: ok, persist: ok}
  DisableDirectoryDataAccess: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeDirectoryDataAccess: {wire: ok, errors: ok, state: ok, persist: ok}
  EnableCAEnrollmentPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DisableCAEnrollmentPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeCAEnrollmentPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  StartADAssessment: {wire: ok, errors: ok, state: ok, persist: ok, note: "synchronous Completed; AWS is async but no client-visible divergence for polling clients"}
  DeleteADAssessment: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeADAssessment: {wire: FIXED, errors: ok, state: ok, persist: ok, note: "StartTime epoch fix"}
  ListADAssessments: {wire: FIXED, errors: ok, state: ok, persist: ok, note: "StartTime epoch fix"}
  CreateHybridAD: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateHybridAD: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeHybridADUpdate: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateComputer: {wire: ok, errors: ok, state: ok, persist: n/a, note: "AWS has no Describe/List for computer accounts either; not persisting matches the real API's surface"}
  UpdateSettings: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeSettings: {wire: FIXED, errors: ok, state: ok, persist: ok, note: "LastUpdatedDateTime epoch fix"}
  UpdateDirectorySetup: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeUpdateDirectory: {wire: FIXED, errors: ok, state: ok, persist: ok, note: "StartTime/LastUpdatedDateTime epoch fix"}
  ResetUserPassword: {wire: ok, errors: ok, state: ok, persist: n/a}
  ConnectDirectory: {wire: ok, errors: ok, state: ok, persist: ok}
# Families audited as a group (when per-op is impractical):
families:
  persistence-registration: {status: FIXED, note: "Handler lacked Snapshot(ctx)/Restore(ctx,[]byte) delegation methods, so cli.go's setupPersistence type-assertion against the local `persistable` interface silently failed and directoryservice was NEVER registered with the persistence manager -- a fully-correct BackendSnapshot/Restore on InMemoryBackend was completely unreachable. Fixed by adding delegation methods to Handler (handler.go)."}
  timestamps: {status: FIXED, note: "22 call sites across handler_appendixa.go formatted timestamps as ISO8601 strings (time.Format(\"2006-01-02T15:04:05.000Z\")); confirmed against aws-sdk-go-v2 directoryservice deserializers.go that every timestamp field uses smithytime.ParseEpochSeconds (JSON number), so real SDK clients would fail to deserialize every affected List/Describe response. All converted to awstime.Epoch."}
  sdk_completeness: {status: ok, note: "sdk_completeness_test.go verifies every dssdk.Client op is in GetSupportedOperations(); notImplemented is empty -- full op coverage."}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - RegisterCertificate hardcodes CommonName="example.com" instead of parsing CertificateData's X.509 subject; low-impact emulation shortcut, no wire/error divergence (no bd issue filed -- flag if a client asserts on CommonName)
  - StartADAssessment/CreateTrust/ShareDirectory etc. complete synchronously instead of AWS's async in-progress states (e.g. no "Creating"/"Sharing"/"Verifying" transient states observable by a fast poller); acceptable for emulation, but a client that asserts on an intermediate state would diverge (no bd issue filed)
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - RegionType/RegionStatus enum value fidelity for AddRegion/DescribeRegions (values used: "Additional"/"Active" -- not cross-checked against the full RegionType/DirectoryStage enum in aws-sdk-go-v2/service/directoryservice/types/enums.go)
  - TrustState/TrustDirection/TrustType free-form validation (CreateTrust accepts any string for TrustDirection/TrustType rather than validating against the SDK's TrustDirection/TrustType enums)
  - LDAPSType/AuthType/UpdateType free-form validation (same free-string-accepted pattern across LDAPS, ClientAuthentication and UpdateDirectorySetup)
leaks: {status: clean, note: "transitionDirectoryToActive and RestoreFromSnapshot's goroutine both self-terminate after two bounded time.Sleep stages (50ms/100ms); no unbounded loops, no leaked tickers/timers; isolation_test.go and the -race gate confirm no cross-region/cross-goroutine data races."}
---

## Notes

Protocol: AWS JSON 1.1 (`X-Amz-Target: DirectoryService_20150416.<Op>`, error shape
`{"__type": "<Code>Exception", "message": "..."}`). Confirmed against
aws-sdk-go-v2/service/directoryservice@v1.38.20's deserializers.go: **every** timestamp
field in this API (LaunchTime, StartTime, CreatedDateTime, LastUpdatedDateTime,
ExpiryDateTime, AddedDateTime, etc.) is deserialized via `smithytime.ParseEpochSeconds`,
i.e. wire format is a JSON number of seconds since epoch, NEVER an ISO8601 string. This
is the single biggest wire-shape trap in this service — the base handler.go (audited in
an earlier pass) already used `awstime.Epoch` correctly for Directory.LaunchTime and
Snapshot.StartTime, but every Appendix-A handler (added in a later pass, before this
service had a PARITY.md) used `time.Format("2006-01-02T15:04:05.000Z")` instead. Any
future op added to this service MUST use `awstime.Epoch(...)`, never
`.Format(time.RFC3339...)` or similar — the sdk_completeness test won't catch this class
of bug since it only checks operation-name coverage, not wire shape.

`ShareStatus` enum (aws-sdk-go-v2/service/directoryservice/types/enums.go): Shared,
PendingAcceptance, Rejected, Rejecting, RejectFailed, Sharing, ShareFailed, Deleted,
Deleting. `RejectFailed` means the *reject operation itself* failed asynchronously — it
is NOT the terminal state of a successful reject (that's `Rejected`). Easy to invert by
pattern-matching the string "Reject" without checking AWS's actual semantics; the bug
fixed this pass had exactly this shape.

`ShareMethod` enum: ORGANIZATIONS, HANDSHAKE. Only HANDSHAKE requires the consumer
account to call AcceptSharedDirectory (initial ShareStatus = PendingAcceptance);
ORGANIZATIONS shares are active immediately (ShareStatus = Shared). The handler defaults
ShareMethod to "HANDSHAKE" when the request omits it (matches AWS's own default).

Directory lifecycle (Stage enum): Requested → Creating → Active, each transition on its
own goroutine with a fixed delay (`directoryLifecycleDelay` = 50ms) — this is real state
mutation, not a fabricated instant-Active response. RestoreFromSnapshot similarly drives
Active → Restoring → Active via `restoreLifecycleDelay` (100ms). Both were verified to
actually flip backend state (not just return success) by reading backend.go directly,
per the parity-principles.md "grep for stubs has false positives" caveat — these looked
suspicious as "fire-and-forget goroutine" patterns at first glance but are correct.

Sixteen resource collections use the region-qualified `store.Table[V]` +
`store.Index[V]` pattern (`store_setup.go`); six raw maps (aliases, ipRoutes,
dirDataAccess, caEnrollment, dirSettings, updateInfoEntries) were deliberately left as
plain `map[string]map[string]V` because their values have no independent identity to key
a `store.Table` by — this is documented in-line and is correct, not a shortcut.
`InMemoryBackend` uses the single coarse `lockmetrics.RWMutex` correctly: every method
takes the full backend lock for its whole read/write, matching the "lock granularity
follows invariant granularity" rule (a `DeleteDirectory` cascade touches 12+ tables
atomically).

CreateComputer intentionally does not persist a computer record: AWS Directory Service's
own API surface has no Describe/List operation for computer accounts (they're plain AD
objects, not DS-managed resources), so there's nothing a client could read back — this
looked like a disguised no-op at first glance (RLock used for a "create" op, nothing
stored) but is correct emulation of an op whose only observable effect in real AWS is the
synchronous response itself.
