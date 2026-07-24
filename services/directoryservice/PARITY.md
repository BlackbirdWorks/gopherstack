---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: directoryservice
sdk_module: aws-sdk-go-v2/service/directoryservice@v1.38.20   # version audited against
last_audit_commit: 1c6af314f4ed210dbc03be80042c6af2aa07448f   # stale -- git usage disallowed this pass; see last_audit_date
last_audit_date: 2026-07-23
overall: A            # error-code taxonomy fix (ClientException->InvalidParameterException, ~90 sites) + all 3 deferred enum-validation items closed + real X.509 cert parsing + 3 new wire-shape gaps found and fixed (Trust/Region/Directory)
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateDirectory: {wire: ok, errors: ok, state: ok, persist: ok, note: "Requested->Creating->Active lifecycle goroutine"}
  CreateMicrosoftAD: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDirectory: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascades all dependent resources"}
  DescribeDirectories: {wire: FIXED, errors: ok, state: FIXED, persist: ok, note: "DirectoryDescription was missing StageLastUpdatedDateTime (explicitly flagged bug class) and DnsIpAddrs entirely; added both -- StageLastUpdatedDateTime now stamped by a shared setStage() helper on every Stage transition (create lifecycle + restore lifecycle), DnsIpAddrs synthesized deterministically from the directory ID. ConnectSettings/DesiredNumberOfDomainControllers/DnsIpv6Addrs/HybridSettings/NetworkType/OsVersion/OwnerDirectoryDescription/RadiusStatus/RegionsInfo/ShareMethod/ShareNotes/ShareStatus/StageReason remain unmirrored onto the top-level DirectoryDescription summary -- see gaps."}
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
  AddRegion: {wire: FIXED, errors: FIXED, state: FIXED, persist: ok, note: "VPCSettings is a required AddRegionInput member (DirectoryVpcSettings{VpcId,SubnetIds}) that was silently dropped -- handler used the generic 2-field helper and never parsed it. Now required+parsed+stored+echoed. RegionType=Additional/Status=Active confirmed valid against types.RegionType/DirectoryStage enums (closes the deferred RegionType/RegionStatus item)."}
  RemoveRegion: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeRegions: {wire: FIXED, errors: ok, state: ok, persist: ok, note: "LaunchTime epoch fix (prior pass); this pass added the RegionDescription fields that were completely absent: VpcSettings, DesiredNumberOfDomainControllers (defaulted to 2, AddRegion has no request field for it), StatusLastUpdatedDateTime"}
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
  CreateTrust: {wire: FIXED, errors: FIXED, state: FIXED, persist: ok, note: "TrustDirection (required per SDK) and TrustPassword had zero presence validation; TrustDirection/TrustType/SelectiveAuth accepted any free-form string (closes the deferred TrustDirection/TrustType item) -- now validated against types.TrustDirection/TrustType/SelectiveAuth enums with InvalidParameterException on mismatch. SelectiveAuth was silently ignored and hardcoded to Disabled on every create despite being a real optional CreateTrustInput member -- now wired through."}
  DeleteTrust: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeTrusts: {wire: FIXED, errors: ok, state: ok, persist: ok, note: "CreatedDateTime/LastUpdatedDateTime epoch fix (prior pass); this pass found StateLastUpdatedDateTime and TrustStateReason were tracked in TrustInfo/storedTrust but never serialized into the response map at all -- both real Trust struct members, now included (TrustStateReason omitted when empty, matching AWS's null-omission)."}
  UpdateTrust: {wire: FIXED, errors: ok, state: ok, persist: ok, note: "SelectiveAuth free-form (closes deferred item, now enum-validated); RequestId was fabricated by reusing TrustId instead of being a real per-request identifier (UpdateTrustOutput.RequestId is documented as the AWS request ID, not derived from TrustId) -- now uuid.NewString(), matching the CreateHybridAD/UpdateHybridAD RequestId pattern already used elsewhere in this service."}
  VerifyTrust: {wire: ok, errors: ok, state: ok, persist: ok}
  ShareDirectory: {wire: FIXED, errors: ok, state: FIXED, persist: ok, note: "HANDSHAKE now starts PendingAcceptance (was Shared, skipping the handshake); ORGANIZATIONS starts Shared"}
  UnshareDirectory: {wire: ok, errors: ok, state: ok, persist: ok}
  AcceptSharedDirectory: {wire: ok, errors: ok, state: ok, persist: ok}
  RejectSharedDirectory: {wire: ok, errors: ok, state: FIXED, persist: ok, note: "was setting ShareStatus=RejectFailed (the AWS enum value for a FAILED reject) on every SUCCESSFUL reject; now Rejected"}
  DescribeSharedDirectories: {wire: FIXED, errors: ok, state: ok, persist: ok, note: "CreatedDateTime/LastUpdatedDateTime epoch fix"}
  RegisterCertificate: {wire: FIXED, errors: FIXED, state: FIXED, persist: ok, note: "CLOSED the CommonName=example.com gap: CertificateData is documented as a real PEM string, so it is now decoded (encoding/pem) and parsed (crypto/x509); CommonName comes from cert.Subject.CommonName and ExpiryDateTime from cert.NotAfter (both previously fabricated/hardcoded). Unparseable CertificateData now returns the real InvalidCertificateException (was silently accepted). Type is now validated against CertificateType (ClientLDAPS/ClientCertAuth)."}
  DeregisterCertificate: {wire: ok, errors: ok, state: ok, persist: ok}
  ListCertificates: {wire: FIXED, errors: ok, state: ok, persist: ok, note: "ExpiryDateTime epoch fix"}
  DescribeCertificate: {wire: FIXED, errors: ok, state: ok, persist: ok, note: "RegisteredDateTime/ExpiryDateTime epoch fix"}
  EnableLDAPS: {wire: FIXED, errors: FIXED, state: ok, persist: ok, note: "Type accepted any free-form string; now validated against the LDAPSType enum (only Client is a valid value) -- closes deferred item"}
  DisableLDAPS: {wire: FIXED, errors: FIXED, state: ok, persist: ok, note: "same LDAPSType validation as EnableLDAPS"}
  DescribeLDAPSSettings: {wire: FIXED, errors: ok, state: ok, persist: ok, note: "LastUpdatedDateTime/CertificateExpiryDateTime epoch fix"}
  EnableClientAuthentication: {wire: FIXED, errors: FIXED, state: ok, persist: ok, note: "Type is a required AWS input member but had no presence or enum check at all; now required + validated against ClientAuthenticationType (SmartCard/SmartCardOrPassword) -- closes deferred item"}
  DisableClientAuthentication: {wire: FIXED, errors: FIXED, state: ok, persist: ok, note: "same Type validation as EnableClientAuthentication"}
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
  UpdateDirectorySetup: {wire: FIXED, errors: FIXED, state: ok, persist: ok, note: "UpdateType is a required AWS input member but had no presence or enum check; now required + validated against UpdateType (OS/NETWORK/SIZE) -- closes deferred item"}
  DescribeUpdateDirectory: {wire: FIXED, errors: ok, state: ok, persist: ok, note: "StartTime/LastUpdatedDateTime epoch fix"}
  ResetUserPassword: {wire: ok, errors: ok, state: ok, persist: n/a}
  ConnectDirectory: {wire: ok, errors: ok, state: ok, persist: ok}
# Families audited as a group (when per-op is impractical):
families:
  persistence-registration: {status: FIXED, note: "Handler lacked Snapshot(ctx)/Restore(ctx,[]byte) delegation methods, so cli.go's setupPersistence type-assertion against the local `persistable` interface silently failed and directoryservice was NEVER registered with the persistence manager -- a fully-correct BackendSnapshot/Restore on InMemoryBackend was completely unreachable. Fixed by adding delegation methods to Handler (handler.go)."}
  timestamps: {status: FIXED, note: "22 call sites across handler_appendixa.go formatted timestamps as ISO8601 strings (time.Format(\"2006-01-02T15:04:05.000Z\")); confirmed against aws-sdk-go-v2 directoryservice deserializers.go that every timestamp field uses smithytime.ParseEpochSeconds (JSON number), so real SDK clients would fail to deserialize every affected List/Describe response. All converted to awstime.Epoch."}
  sdk_completeness: {status: ok, note: "sdk_completeness_test.go verifies every dssdk.Client op is in GetSupportedOperations(); notImplemented is empty -- full op coverage."}
  error-taxonomy: {status: FIXED, note: "Systemic error-code bug across ~90 validation call sites in ~20 handler_*.go files: every request-validation failure (missing required field, invalid enum value) returned __type=\"ClientException\" instead of AWS's real InvalidParameterException (confirmed as a distinct documented exception in types/errors.go, present in nearly every op's real Errors list). Also fixed the dead-but-wrong mapError case for the backend awserr.ErrInvalidParameter sentinel (was also \"ClientException\"). Left \"invalid body\"/\"invalid JSON\" transport-parse failures and the backend awserr.ErrConflict case as ClientException (defensible: not a documented-parameter-value problem). Every corresponding test assertion updated; see handler_directories_test.go/handler_directories_extra_test.go/handler_test.go for the renamed expectations."}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - DirectoryDescription (DescribeDirectories/CreateDirectory/CreateMicrosoftAD/ConnectDirectory responses) does not mirror ConnectSettings, DesiredNumberOfDomainControllers, DnsIpv6Addrs, HybridSettings, NetworkType, OsVersion, OwnerDirectoryDescription, RadiusStatus, RegionsInfo, ShareMethod, ShareNotes, ShareStatus, StageReason onto the top-level summary, even though most of that data is independently trackable/retrievable via the dedicated Describe* ops for those sub-resources (DescribeRegions, radius.go, shared_directories.go, hybrid_ad.go). StageLastUpdatedDateTime and DnsIpAddrs were the two highest-value gaps in this set and were fixed this pass (see DescribeDirectories note above); the rest are lower-value summary duplication, not fixed (no bd issue filed).
  - DomainController (DescribeDomainControllers response) is missing DnsIpAddr, DnsIpv6Addr, StatusLastUpdatedDateTime, StatusReason, SubnetId, VpcId -- confirmed against types.DomainController; storedDomainController only tracks ControllerID/DirectoryID/Status/AvailabilityZone/LaunchTime. Not fixed this pass (no bd issue filed); flag if a client asserts on domain-controller IP/subnet/VPC identity.
  - StartADAssessment/CreateTrust/ShareDirectory etc. complete synchronously instead of AWS's async in-progress states (e.g. no "Creating"/"Sharing"/"Verifying" transient states observable by a fast poller); acceptable for emulation, but a client that asserts on an intermediate state would diverge (no bd issue filed)
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - Full field-diff of every remaining "ok"-marked op family (conditional forwarders, log subscriptions, event topics, schema extensions, radius, shared directories, hybrid AD, AD assessments, settings) against their SDK response types was NOT repeated this pass beyond the epoch-timestamp sweep already recorded above; this pass's field-diffs concentrated on trusts/regions/certificates/directories because that's where the (now-closed) enum-validation deferred items and the explicitly-flagged StageLastUpdatedDateTime bug class pointed. Given the real gaps found in 3-for-3 families actually field-diffed this pass (Trust, Region, Directory all had missing/wrong wire fields despite being marked "ok"), the remaining "ok" families should NOT be trusted without an independent field-diff next pass.
leaks: {status: clean, note: "transitionDirectoryToActive and RestoreFromSnapshot's goroutine both self-terminate after two bounded time.Sleep stages (50ms/100ms); no unbounded loops, no leaked tickers/timers; isolation_test.go and the -race gate confirm no cross-region/cross-goroutine data races. This pass added no new goroutines/tickers/locks -- setStage() is a plain synchronous helper called under the existing b.mu lock, verified by -race."}
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

`InvalidParameterException` vs `ClientException` (2026-07-23 pass): both are real,
distinct AWS Directory Service exception types (types/errors.go). Prior code used
`ClientException` uniformly for every request-validation failure. AWS's own per-op Errors
lists document `InvalidParameterException` as the code for "one or more parameters are not
valid" (missing required members, invalid enum values) — that is what this service's
handler-level and backend-level validation checks actually detect, so they now return
`InvalidParameterException`. `ClientException` is now reserved for cases that are not a
specific documented parameter problem: malformed/unparseable request bodies ("invalid
body"/"invalid JSON") and the generic backend `awserr.ErrConflict` sentinel. Any new
validation check added to this service should return `InvalidParameterException`, not
`ClientException`, unless it's genuinely one of those two exempted cases.

`setStage(d *storedDirectory, stage DirectoryStage)` (directories.go) is the single place
that mutates `storedDirectory.Stage`; it also stamps `StageLastUpdatedDateTime = now()`.
Both the create lifecycle (`transitionDirectoryToActive`) and the restore lifecycle
(`RestoreFromSnapshot`'s goroutine) now go through it. Any future code that flips
`Directory.Stage` directly instead of calling `setStage` will silently reintroduce the
StageLastUpdatedDateTime bug class named in this campaign's brief.

`synthesizeDNSIPAddrs(directoryID string) []string` (directories.go) derives two
deterministic `10.0.x.y`-shaped addresses from a SHA-256 of the directory ID for the
`DnsIpAddrs` field. This is a synthesized-but-consistent value (same directory ID always
yields the same IPs, matching real AWS's stable-per-directory DNS IPs), not a random
placeholder — documented here so a future auditor doesn't mistake it for a fabricated/stub
value the "no stub" rule would forbid; the alternative (omitting the field, as before) was
the actual parity bug.
