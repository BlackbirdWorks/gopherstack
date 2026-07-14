---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: guardduty
sdk_module: aws-sdk-go-v2/service/guardduty@v1.78.2
last_audit_commit: 7f3594fa
last_audit_date: 2026-07-12
overall: A            # genuine fixes found: 1 route bug, 3 wire-shape bugs, 1 state-sync bug class
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  UpdateMalwareProtectionPlan: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — was routed on POST, real SDK sends PATCH (the one GuardDuty op that isn't POST/GET/DELETE); was unroutable by a real client despite green unit tests that called h.Handler() directly, bypassing RouteMatcher/method dispatch"}
  DescribePublishingDestination: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — wire key was publishingFailureStartedAt (invented), real key is publishingFailureStartTimestamp; tags were never returned despite CreatePublishingDestination now accepting them"}
  CreatePublishingDestination: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — did not accept/store tags at all; real CreatePublishingDestinationInput.Tags is honored now"}
  GetThreatEntitySet: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — createdAt/updatedAt were omitted entirely; real output has them as epoch-seconds numbers (unlike GetDetectorOutput's ISO8601 strings)"}
  GetTrustedEntitySet: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — same createdAt/updatedAt gap as GetThreatEntitySet"}
  CreateThreatEntitySet: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — creation-time tags were stored on the resource's own Tags field but never written to the generic ARN-keyed tag map, so ListTagsForResource returned {} right after creation"}
  CreateTrustedEntitySet: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — same as CreateThreatEntitySet"}
  GetMalwareProtectionPlan: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — createdAt was a bare time.Time (RFC3339 string on the wire); real GetMalwareProtectionPlanOutput.CreatedAt is epoch seconds"}
  GetMalwareScan: {wire: partial, errors: ok, state: ok, persist: ok, note: "PARTIALLY FIXED — was emitting fields from the wrong Scan shape entirely (accountId/resourceDetails/findings/scanStartTime/scanEndTime, none of which exist on the real GetMalwareScanOutput); renamed the two timestamp fields to the real scanStartedAt/scanCompletedAt (epoch seconds) and dropped the three nonexistent fields. Still missing real optional fields this backend has no state for: adminDetectorId, resourceArn, resourceType, scanCategory, scanConfiguration, scanResultDetails, scanStatusReason, scannedResources(+count), skippedResourcesCount, failedResourcesCount — see gaps"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — now propagates into the owning resource's own Tags field (see families.tags below), not just the generic map"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — same propagation as TagResource"}
  CreateDetector: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDetector: {wire: ok, errors: ok, state: ok, persist: ok, note: "createdAt/updatedAt are ISO8601 strings on this op specifically (GetDetectorOutput.CreatedAt/UpdatedAt are *string, not epoch) — do not \"fix\" this to epoch, it is already correct and differs deliberately from ThreatEntitySet/TrustedEntitySet/MalwareProtectionPlan"}
  UpdateDetector: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDetector: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascade-deletes every detector-nested table + tags; verified via persistence_test.go"}
  ListDetectors: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateFilter: {wire: ok, errors: ok, state: ok, persist: ok}
  GetFilter: {wire: ok, errors: ok, state: ok, persist: ok, note: "real GetFilterOutput has no createdAt/updatedAt — correctly omitted"}
  UpdateFilter: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteFilter: {wire: ok, errors: ok, state: ok, persist: ok}
  ListFilters: {wire: ok, errors: ok, state: ok, persist: ok}
  GetFindings: {wire: ok, errors: ok, state: ok, persist: ok, note: "Finding.CreatedAt/UpdatedAt correctly plain strings (Finding is a \"string\" shape member on the real API, not a timestamp shape)"}
  ListFindings: {wire: ok, errors: ok, state: ok, persist: ok, note: "no FindingCriteria/SortCriteria filtering — see gaps"}
  ArchiveFindings: {wire: ok, errors: ok, state: ok, persist: ok, note: "real mutation: sets Service.Archived + UpdatedAt, verified by reading GetFindings after"}
  UnarchiveFindings: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateSampleFindings: {wire: ok, errors: ok, state: ok, persist: ok}
  GetFindingsStatistics: {wire: partial, errors: ok, state: ok, persist: ok, note: "only emits the deprecated findingStatistics.countBySeverity; real API also supports groupedByAccount/groupedByDate/groupedByFindingType/groupedByResource/groupedBySeverity via the GroupBy request param — not implemented, see gaps"}
  UpdateFindingsFeedback: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateIPSet: {wire: ok, errors: ok, state: ok, persist: ok}
  GetIPSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "real GetIPSetOutput has no createdAt/updatedAt — correctly omitted"}
  UpdateIPSet: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteIPSet: {wire: ok, errors: ok, state: ok, persist: ok}
  ListIPSets: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateThreatIntelSet: {wire: ok, errors: ok, state: ok, persist: ok}
  GetThreatIntelSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "real GetThreatIntelSetOutput has no createdAt/updatedAt — correctly omitted"}
  UpdateThreatIntelSet: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteThreatIntelSet: {wire: ok, errors: ok, state: ok, persist: ok}
  ListThreatIntelSets: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateMembers: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteMembers: {wire: ok, errors: ok, state: ok, persist: ok}
  GetMembers: {wire: ok, errors: ok, state: ok, persist: ok}
  InviteMembers: {wire: ok, errors: ok, state: ok, persist: ok, note: "real relationshipStatus transition Created->Invited verified"}
  ListMembers: {wire: ok, errors: ok, state: ok, persist: ok}
  StartMonitoringMembers: {wire: ok, errors: ok, state: ok, persist: ok}
  StopMonitoringMembers: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateMembers: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteMalwareProtectionPlan: {wire: ok, errors: ok, state: ok, persist: ok}
  ListMalwareProtectionPlans: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateMalwareProtectionPlan: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateMalwareProtectionPlan_state: {wire: ok, errors: ok, state: ok, persist: ok, note: "backend mutation logic itself was already correct — only the route (see above) and createdAt wire format were bugs"}
families:
  detector: {status: ok, note: "CRUD + list audited op-by-op above; one-detector-per-account conflict semantics match AWS doc (\"You can have only one detector per Region\")"}
  filter: {status: ok, note: "CRUD + list audited op-by-op above"}
  ipset: {status: ok, note: "CRUD + list audited op-by-op above"}
  threatintelset: {status: ok, note: "CRUD + list audited op-by-op above"}
  findings: {status: ok, note: "Get/List/Archive/Unarchive/CreateSample/UpdateFeedback audited; GetFindingsStatistics and ListFindings filtering are partial, see gaps"}
  members_invitations: {status: ok, note: "member lifecycle + invitation flows audited; admin/master account relationship handlers (AcceptAdministratorInvitation/AcceptInvitation/GetAdministratorAccount/GetMasterAccount/Disassociate*) read/write real state via the adminAccounts table"}
  publishingDestination: {status: ok, note: "FIXED wire key + added tags support this pass (see ops above)"}
  tags: {status: ok, note: "FIXED this pass: TagResource/UntagResource now sync into the owning resource's own frozen Tags field (Detector/Filter/IPSet/ThreatIntelSet/ThreatEntitySet/TrustedEntitySet/MalwareProtectionPlan/PublishingDestination) via syncResourceTagsFromARN in backend.go, so Get*/Describe* no longer show stale tags after a Tag/UntagResource call. Also fixed CreateThreatEntitySet/CreateTrustedEntitySet not writing the generic ARN-keyed tag map at all."}
  threat_trusted_entity_sets: {status: ok, note: "CRUD audited; createdAt/updatedAt + generic-tag-map write were both fixed this pass (see ops above)"}
  malware_scan_settings: {status: partial, note: "GetMalwareScanSettings/UpdateMalwareScanSettings wire-verified ok; GetMalwareScan itself only partially fixed (see ops above and gaps)"}
  organization: {status: deferred, note: "EnableOrganizationAdminAccount/DisableOrganizationAdminAccount/ListOrganizationAdminAccounts/DescribeOrganizationConfiguration/UpdateOrganizationConfiguration/GetOrganizationStatistics routed correctly and mutate real state, but response shapes (esp. GetOrganizationStatistics' fabricated countByFeature: []) were not deep-audited against types.OrganizationStatistics this pass"}
  coverage_usage_freetrial: {status: deferred, note: "GetCoverageStatistics/ListCoverage/GetUsageStatistics/GetRemainingFreeTrialDays route and error-handle correctly but return synthetic/empty payloads (e.g. ListCoverage always []); not deep-audited against real CoverageResource/UsageStatistics shapes this pass — likely low-traffic ops"}
  malware_protection_plan_actions: {status: deferred, note: "Actions/ProtectedResource are passed through as opaque map[string]any without validating against types.MalwareProtectionPlanActions/CreateProtectedResource — not schema-checked this pass"}
gaps:
  - "GetMalwareScan (services/guardduty/handler_appendixa.go:handleGetMalwareScan) still doesn't emit adminDetectorId/resourceArn/resourceType/scanCategory/scanConfiguration/scanResultDetails/scanStatusReason/scannedResources(+count)/skippedResourcesCount/failedResourcesCount — the real op has a materially richer response than DescribeMalwareScans/ListMalwareScans' Scan shape, and this backend only tracks the older Scan-shape fields. All missing fields are optional on the real output so a real client won't error, just gets nil/absent fields."
  - "GetFindingsStatistics (services/guardduty/backend.go:GetFindingsStatistics) only implements the deprecated countBySeverity; groupedByAccount/groupedByDate/groupedByFindingType/groupedByResource/groupedBySeverity (selected via the request's GroupBy param) are not implemented."
  - "ListFindings/ListDetectors/ListFilters/ListIPSets/ListThreatIntelSets/etc. ignore FindingCriteria/SortCriteria/MaxResults and never emit a NextToken — every list op returns its full result set in one page. NextToken is an optional response field so this is non-fatal to real clients, but large result sets won't paginate."
  - "PublishingDestination lacks ExpectedBucketOwner-style extras present on some other Create*/Get* outputs (e.g. GetThreatEntitySetOutput.ErrorDetails/ExpectedBucketOwner) -- not tracked anywhere in this backend, so always absent."
deferred:
  - organization family (response-shape deep audit)
  - coverage/usage/freeTrial family (response-shape deep audit)
  - malware protection plan Actions/ProtectedResource schema validation
leaks: {status: clean, note: "no goroutines, timers, or background janitors in this service; all state lives in InMemoryBackend's store.Table fields guarded by the single lockmetrics.RWMutex, reset via Reset()/Restore()"}
---

## Notes

Protocol: restjson1 (REST paths like `/detector`, `/detector/{id}/filter/{name}`).

### Route-matcher findings (this pass)

- **UpdateMalwareProtectionPlan is the one GuardDuty op that serializes with
  HTTP PATCH, not POST** (`aws-sdk-go-v2/service/guardduty` serializers.go:
  `awsRestjson1_serializeOpUpdateMalwareProtectionPlan` sets
  `request.Method = "PATCH"`). `parseMalwareProtectionPlanPath` in
  `handler_appendixa.go` was matching `http.MethodPost` for the update case,
  so a real SDK client's PATCH request fell through to `opUnknown` → 404.
  Every existing test exercised this via `auditDo`, which calls
  `h.Handler()(c)` directly and bypasses both `RouteMatcher` and any
  method-aware echo routing, so the bug was invisible to 100% green unit
  tests. Fixed by matching `http.MethodPatch`; added
  `handler_route_matcher_test.go` which drives `RouteMatcher()` +
  `ExtractOperation()` directly (still not a full echo-router integration
  test, but it exercises the actual method-dispatch logic a router would
  rely on) with an explicit regression case asserting POST to that path
  resolves to `Unknown`.
- **`RouteMatcher`'s `/tags/{arn}` prefix check was hardcoded to
  `"arn:aws:guardduty:"`**, which silently rejects well-formed GuardDuty
  ARNs from any non-standard partition (GovCloud `arn:aws-us-gov:guardduty:`,
  China `arn:aws-cn:guardduty:`, ISO `arn:aws-iso*:guardduty:` —
  `pkgs/arn.PartitionForRegion` already produces these). Fixed to check for
  the `:guardduty:` service segment regardless of partition
  (`isGuardDutyTagsPath` in `handler.go`).

### Wire-shape findings (this pass)

GuardDuty timestamps are NOT uniform across ops — this is a real, deliberate
AWS inconsistency, not a bug to "fix" toward one format:

| Shape | Wire format | Verified via |
|---|---|---|
| `GetDetectorOutput.CreatedAt/UpdatedAt` | ISO8601 `*string` | deserializers.go: `ptr.String(jtv)` |
| `Member.UpdatedAt/InvitedAt` | ISO8601 `*string` | deserializers.go: `awsRestjson1_deserializeDocumentMember` |
| `Finding.CreatedAt/UpdatedAt` | ISO8601 `*string` | Finding is a string-shape member, not timestamp |
| `GetThreatEntitySetOutput.CreatedAt/UpdatedAt` | epoch-seconds number | `smithytime.ParseEpochSeconds(f64)` |
| `GetTrustedEntitySetOutput.CreatedAt/UpdatedAt` | epoch-seconds number | same |
| `GetMalwareProtectionPlanOutput.CreatedAt` | epoch-seconds number | same |
| `GetMalwareScanOutput.ScanStartedAt/ScanCompletedAt` | epoch-seconds number | same |
| `DescribePublishingDestinationOutput.PublishingFailureStartTimestamp` | epoch-milliseconds `*int64` | deserializers.go: `json.Number` → `Int64()` |

Use `pkgs/awstime.Epoch` for every epoch-seconds field; do not
`.Format(...)` or let `json.Marshal` fall through to Go's default
`time.Time` encoding for those fields — that produces an ISO8601 string a
real SDK deserializer rejects with "expected Timestamp to be a JSON Number,
got string instead".

`GetMalwareScanOutput` (the individual-scan `GET /malware-scan/{id}` op) is
a **completely different, much richer shape** from the `Scan` type
`DescribeMalwareScans`/`ListMalwareScans` return — they happen to share only
`scanId`/`detectorId`/`scanStatus`/`scanType` field *names* (and even then
the enum *types* differ, though the wire is the same string either way). The
previous handler was built against the wrong shape (`Scan`, not
`GetMalwareScanOutput`), emitting `accountId`/`resourceDetails`/`findings`
(none of which exist on the real output) and naming the timestamps
`scanStartTime`/`scanEndTime` instead of `scanStartedAt`/`scanCompletedAt`.
Fixed the four wrong/renamed fields; the richer optional fields this backend
has no state for remain a documented gap above rather than being faked out.

### Tag state-sync bug (this pass)

Every taggable resource (Detector, Filter, IPSet, ThreatIntelSet,
ThreatEntitySet, TrustedEntitySet, MalwareProtectionPlan,
PublishingDestination) stores a **frozen copy** of its tags on its own
struct, set once at creation and returned by that resource's own
`Get`/`Describe` handler — matching the real `GetDetectorOutput.Tags` etc.
shapes, which embed tags directly rather than requiring a second
`ListTagsForResource` call. `TagResource`/`UntagResource`, however, only
ever mutated `InMemoryBackend.tags` (the ARN-keyed generic map backing
`ListTagsForResource`) and never touched that frozen field, so calling
`TagResource` then immediately calling `GetDetector` returned the *old*
tags while `ListTagsForResource` returned the *new* ones — a real,
observable divergence from AWS behavior (real AWS keeps both views
consistent). Fixed via `syncResourceTagsFromARN` in `backend.go`, which
parses the resource ARN back to its owning table+key and refreshes that
resource's `Tags` field after every `TagResource`/`UntagResource` call.

The inverse gap also existed for the two entity-set families:
`CreateThreatEntitySet`/`CreateTrustedEntitySet` set the creation-time tags
on the resource's own field but never wrote them into the generic
`b.tags[ARN]` map at all (every *other* taggable resource's Create* method
did), so `ListTagsForResource` on a freshly-created entity set returned `{}`
even though `GetThreatEntitySet` showed the tags. Fixed by adding the
missing `b.tags[...]` write (using new `threatEntitySetARN`/
`trustedEntitySetARN`/`publishingDestinationARN` helpers in `backend.go`,
matching the existing `filterARN`/`ipSetARN`/`threatIntelSetARN` pattern).

### Looks-wrong-but-correct traps

- `GetFilterOutput`/`GetIPSetOutput`/`GetThreatIntelSetOutput` genuinely have
  **no** `createdAt`/`updatedAt` members on the real wire — the handlers
  correctly omit them. Do not "fix" this by adding timestamps; it would be
  a new divergence, not a repair.
- `CreateDetector` returning `ConflictException` on a second call is
  consistent with the real API's documented "you can have only one detector
  per Region" constraint (`aws-sdk-go-v2/service/guardduty`
  `api_op_CreateDetector.go` doc comment) — not a bug.
- `errBody`'s `message` field intentionally equals `__type` (both are the
  sentinel's error-code string, e.g. `"ResourceNotFoundException"`) rather
  than a human-readable sentence — this matches the existing repo-wide
  `awserr` convention, not a bug specific to this service.
