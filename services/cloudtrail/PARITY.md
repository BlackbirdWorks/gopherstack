---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: cloudtrail
sdk_module: aws-sdk-go-v2/service/cloudtrail@v1.58.4   # version audited against
last_audit_commit: UNKNOWN_SEE_GIT_LOG   # this pass ran without git access; set on next commit
last_audit_date: 2026-07-23
overall: A            # A = ~1k genuine fixes found; B = already-accurate, proven op-by-op
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateTrail: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed prior pass: response KmsKeyId key case (was KMSKeyId)"}
  GetTrail: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateTrail: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteTrail: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeTrails: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTrails: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: NextToken pagination via pkgs/page (was always one page)"}
  StartLogging: {wire: ok, errors: ok, state: ok, persist: ok}
  StopLogging: {wire: ok, errors: ok, state: ok, persist: ok}
  GetTrailStatus: {wire: ok, errors: ok, state: ok, persist: ok, note: "IsLogging/StartLoggingTime/StopLoggingTime/LatestDeliveryTime as epoch numbers, TimeLoggingStarted/Stopped as RFC3339 strings — matches SDK deserializer exactly"}
  PutEventSelectors: {wire: ok, errors: ok, state: ok, persist: ok}
  GetEventSelectors: {wire: ok, errors: ok, state: ok, persist: ok}
  PutInsightSelectors: {wire: ok, errors: ok, state: ok, persist: ok}
  GetInsightSelectors: {wire: ok, errors: ok, state: ok, persist: ok}
  LookupEvents: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: EventCategory input field now filters (omitted/'Management' -> management events; 'insight' -> none, this backend never synthesizes Insight events); Event gained EventCategory + a matching UnmarshalJSON (see leaks note)"}
  AddTags: {wire: ok, errors: ok, state: ok, persist: ok}
  RemoveTags: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTags: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateChannel: {wire: ok, errors: ok, state: ok, persist: ok}
  GetChannel: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateChannel: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteChannel: {wire: ok, errors: ok, state: ok, persist: ok}
  ListChannels: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: NextToken/MaxResults pagination via pkgs/page"}
  CreateDashboard: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: Widgets/RefreshSchedule/TerminationProtectionEnabled were accepted on the wire but never modeled/stored/echoed (deferred item from last pass); now real Dashboard fields, CreatedTimestamp/UpdatedTimestamp added"}
  GetDashboard: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now returns Widgets/RefreshSchedule/TerminationProtectionEnabled/LastRefreshId/LastRefreshFailureReason/CreatedTimestamp/UpdatedTimestamp"}
  UpdateDashboard: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: removed a gopherstack-invented Name (rename) parameter -- real UpdateDashboardInput has no Name field, dashboards cannot be renamed. Now takes the real fields: Widgets, RefreshSchedule, TerminationProtectionEnabled"}
  DeleteDashboard: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDashboards: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: NextToken/MaxResults pagination; narrowed the per-item shape to the real DashboardDetail{DashboardArn,Type} (previously returned the full dashToMap shape, harmless-extra but now exact); added Type/NamePrefix filters"}
  StartDashboardRefresh: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: real StartDashboardRefreshOutput has exactly one field, RefreshId. Previously returned a fabricated {DashboardArn, Status} shape with Status set to \"REFRESHING\", which is not even a valid DashboardStatus enum value (real values: CREATING/CREATED/UPDATING/UPDATED/DELETING)"}
  CreateEventDataStore: {wire: ok, errors: ok, state: ok, persist: ok}
  GetEventDataStore: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: CreatedTimestamp/UpdatedTimestamp were raw time.Time values marshaled by encoding/json as RFC3339 strings; the real awsjson1.1 deserializer requires epoch-seconds JSON numbers (ParseEpochSeconds), so a real SDK client would fail to decode these fields entirely. Now emitted as float64(t.Unix())"}
  UpdateEventDataStore: {wire: ok, errors: ok, state: ok, persist: ok, note: "same CreatedTimestamp/UpdatedTimestamp epoch fix as GetEventDataStore (shared edsToMap)"}
  DeleteEventDataStore: {wire: ok, errors: ok, state: ok, persist: ok, note: "termination-protection conflict correctly returns EventDataStoreTerminationProtectedException"}
  ListEventDataStores: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: NextToken/MaxResults pagination; same CreatedTimestamp/UpdatedTimestamp epoch fix"}
  RestoreEventDataStore: {wire: ok, errors: ok, state: ok, persist: ok, note: "same CreatedTimestamp/UpdatedTimestamp epoch fix"}
  StartEventDataStoreIngestion: {wire: ok, errors: ok, state: ok, persist: ok}
  StopEventDataStoreIngestion: {wire: ok, errors: ok, state: ok, persist: ok}
  DisableFederation: {wire: ok, errors: ok, state: ok, persist: ok}
  EnableFederation: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  GetResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  PutResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  StartQuery: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: removed a gopherstack-invented \"EventDataStore\" JSON input field -- the real StartQueryInput has no such field; the target event data store is embedded in QueryStatement's FROM clause (real CloudTrail Lake SQL syntax). The handler now derives it via a FROM-clause regex. Added the real QueryAlias/QueryParameters/DeliveryS3Uri/EventDataStoreOwnerAccountId fields (output now returns QueryId + EventDataStoreOwnerAccountId, was QueryId only)"}
  CancelQuery: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed error codes: not-found now returns QueryIdNotFoundException (was incorrectly InactiveQueryException, which per the real SDK means \"query already in a terminal state\" -- a completely different condition); cancelling an already-terminal query now correctly returns InactiveQueryException (was InvalidParameterException)"}
  DescribeQuery: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: real DescribeQueryOutput has no top-level CreationTime field at all -- it was returning a fabricated one, and was entirely missing the real (required) nested QueryStatistics object (QueryStatisticsForDescribeQuery: BytesScanned/CreationTime/EventsMatched/EventsScanned/ExecutionTimeInMillis). Also fixed the QueryIdNotFoundException error code (see CancelQuery)"}
  GetQueryResults: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed (was the #1 deferred item last pass): QueryResultRows was unconditionally empty. Implemented a bounded, honest CloudTrail Lake SQL subset (SELECT <*|cols> FROM <eds> [WHERE col[!]=val [AND ...]] [LIMIT n]) executed lazily against the backend's shared recorded-events log on first read (see query_exec.go); QueryStatistics.BytesScanned/ResultsCount/TotalResultsCount are real, derived counts, not fabricated. Statements outside the supported grammar still reach FINISHED (never rejected) but yield zero rows -- a narrower, more honest version of the previous blanket limitation. Added NextToken/MaxQueryResults pagination over the computed rows"}
  ListQueries: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: NextToken/MaxResults pagination; EventDataStore/QueryStatus filters now applied (EventDataStore is required on the real input but left permissive here -- see gaps); CreationTime epoch-seconds fix (was raw time.Time)"}
  GenerateQuery: {wire: ok, errors: ok, state: ok, persist: n/a}
  StartImport: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed (was a gap last pass): ImportSource.S3 now models all three real (all-required) S3ImportSource fields -- S3LocationUri, S3BucketRegion, S3BucketAccessRoleArn -- not just S3LocationUri; all three are stored and echoed back on Start/Get/Stop via a new ImportSource/S3ImportSource backend type. Import execution itself (actual file replay) remains not real -- unchanged, documented limitation"}
  GetImport: {wire: ok, errors: ok, state: ok, persist: ok, note: "same ImportSource fix as StartImport"}
  ListImports: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: NextToken/MaxResults pagination; Destination/ImportStatus filters added"}
  StopImport: {wire: ok, errors: ok, state: ok, persist: ok, note: "same ImportSource fix as StartImport"}
  ListImportFailures: {wire: ok, errors: ok, state: partial, persist: n/a, note: "always empty — consistent since imports never actually execute/fail in this backend"}
  GetEventConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  PutEventConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  RegisterOrganizationDelegatedAdmin: {wire: ok, errors: ok, state: partial, persist: n/a, note: "re-verified this pass: MemberAccountId field name matches the real (only) input field exactly; accepts/validates only, no org-admin state modeled — genuinely acceptable, since the real CloudTrail API itself has no read-back op for delegated admins either (no GetOrganizationDelegatedAdmins-equivalent exists upstream)"}
  DeregisterOrganizationDelegatedAdmin: {wire: ok, errors: ok, state: partial, persist: n/a, note: "re-verified this pass: DelegatedAdminAccountId field name matches the real (only) input field exactly; same as RegisterOrganizationDelegatedAdmin"}
  SearchSampleQueries: {wire: ok, errors: ok, state: partial, persist: n/a, note: "always empty list; SDK output shape has no other required fields"}
  ListPublicKeys: {wire: ok, errors: ok, state: partial, persist: n/a, note: "always empty; legacy CloudTrail log-file-validation feature, no public keys are ever generated by this backend"}
  ListInsightsData: {wire: ok, errors: ok, state: partial, persist: n/a, note: "always empty; no Insights event generation exists"}
  ListInsightsMetricData: {wire: ok, errors: ok, state: partial, persist: n/a, note: "always empty; same reason"}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "ListQueries' EventDataStore filter is real AWS's required field but left optional/permissive here (an empty filter returns every query) for backward wire compatibility with an existing smoke test that calls ListQueries with no arguments; a real client omitting it would get a client-side validation error before the request is even sent, so this is low-risk."
  - "GetQueryResults' SQL execution only understands a bounded grammar (SELECT <*|cols> FROM <eds> [WHERE col[!]=val [AND ...]] [LIMIT n]); joins, aggregates (COUNT/GROUP BY), OR, LIKE, and subqueries are accepted (the query still reaches FINISHED, never rejected) but always yield zero rows. See query_exec.go's file doc comment."
  - "RegisterOrganizationDelegatedAdmin / DeregisterOrganizationDelegatedAdmin validate input but track no org-admin state (no GetOrganizationDelegatedAdmins-equivalent op exists in gopherstack's CloudTrail service to read it back anyway, and none exists in the real upstream API either)."
  - "PARITY-FOLLOWUP (pkgs/service, out of scope for this service): pkgs/service/cloudtrail_capture.go's wrapCloudTrailCapture records a management event unconditionally after next(c) returns, regardless of the wrapped handler's response status — a failed (4xx/5xx) mutating API call is captured identically to a successful one, and the synthesized CloudTrailEvent detail JSON always sets errorCode/errorMessage-equivalent fields absent (no error info at all). Real CloudTrail records failed calls too, but with populated errorCode/errorMessage. Not broken (chokepoint IS wired correctly end-to-end: RecordManagementEvent -> InMemoryBackend.RecordManagementEvent -> LookupEvents returns real captured events), just an accuracy gap in a shared file outside services/cloudtrail/'s edit scope."
deferred: []              # both prior deferred items (Lake SQL execution, Dashboard Widgets) were implemented this pass — see ops/gaps above
leaks: {status: clean, note: "no goroutines/janitors in this service; Reset() closes every tags.Tags (trails/channels/dashboards/eventDataStores) before clearing tables. Fixed this pass: Event had a hand-written MarshalJSON (epoch-seconds EventTime) but no matching UnmarshalJSON, so any Snapshot containing a recorded event failed Restore entirely (100% data loss of the events log on every restart with in-flight events) -- this was a previously-documented-but-unfixed bug (TestInMemoryBackend_SnapshotRestore_EventsPreexistingBug), now fixed with a real UnmarshalJSON and the test repurposed to assert the round trip succeeds (TestInMemoryBackend_SnapshotRestore_EventsRoundTrip). GetQueryResults/DescribeQuery now mutate on read (materializeQueryLocked lazily executes a QUEUED query) -- both switched from RLock to Lock accordingly, no lock-upgrade race since the mutation happens entirely under the single write lock, not via RLock->Lock promotion."}
---

## Notes

Protocol: awsjson1.1 (single POST endpoint, `X-Amz-Target: CloudTrail_20131101.<Op>` —
verified against the real SDK's `httpBindingEncoder.SetHeader("X-Amz-Target").String(...)`
call sites in serializers.go; the service's `cloudtrailTargetPrefix` constant matches
exactly).

### Real bugs found and fixed this pass (field-diffed against aws-sdk-go-v2/service/cloudtrail@v1.55.7)

1. **`GetQueryResults` always returned empty `QueryResultRows`** (the #1 deferred item from
   the prior pass: "CloudTrail Lake SQL query execution... QueryResultRows is always empty
   by design"). Implemented a bounded, honest SQL subset executor (`query_exec.go`):
   `SELECT <* | col[, col...]> FROM <event-data-store> [WHERE <col>[!]=<'value'|value>
   [AND ...]] [LIMIT n]`, executed lazily (on first `GetQueryResults`/`DescribeQuery` read,
   not eagerly at `StartQuery`, so a query cancelled before being read stays cancellable —
   matching AWS's async `QUEUED`->`RUNNING`->`FINISHED` lifecycle) against the backend's
   shared recorded-events log. `QueryStatistics` (`BytesScanned`/`ResultsCount`/
   `TotalResultsCount`, and `DescribeQuery`'s `EventsMatched`/`EventsScanned`/
   `ExecutionTimeInMillis`) are genuine derived counts, not fabricated. Statements outside
   the supported grammar are never rejected (still reach `FINISHED`) but yield zero rows —
   see gaps.

2. **`StartQuery` accepted a gopherstack-invented `"EventDataStore"` JSON field.** The real
   `StartQueryInput` has exactly five fields (`DeliveryS3Uri`, `EventDataStoreOwnerAccountId`,
   `QueryAlias`, `QueryParameters`, `QueryStatement`) — no `EventDataStore` field exists; the
   target event data store is embedded directly in `QueryStatement`'s `FROM` clause, per real
   CloudTrail Lake SQL syntax. Removed the invented field; the handler now derives the target
   via `extractQueryFromTarget` (a `FROM`-clause regex) instead. Also added the real
   `QueryAlias`/`QueryParameters`/`EventDataStoreOwnerAccountId` fields (output was missing
   `EventDataStoreOwnerAccountId` entirely).

3. **`CancelQuery`/`DescribeQuery`/`GetQueryResults` returned the wrong not-found error
   code.** `ErrQueryNotFound` was mapped to `InactiveQueryException`, but that real SDK
   exception means "the specified query cannot be canceled because it is in the FINISHED,
   FAILED, TIMED_OUT, or CANCELLED state" — an entirely different condition from "no such
   query ID". The real not-found code is `QueryIdNotFoundException`. Split into
   `ErrQueryIDNotFound` (404, all three ops) and `ErrQueryInactive` (400, `CancelQuery`'s
   already-terminal-state case, which previously used the equally-wrong
   `InvalidParameterException`).

4. **`DescribeQuery` had a fabricated top-level `CreationTime` field and was missing the
   real, required, nested `QueryStatistics` object entirely.** Real `DescribeQueryOutput`
   has no top-level `CreationTime` — it's nested inside `QueryStatistics`
   (`QueryStatisticsForDescribeQuery`: `BytesScanned`/`CreationTime`/`EventsMatched`/
   `EventsScanned`/`ExecutionTimeInMillis`), none of which were previously populated. Now
   returns the real nested shape with genuine values from query execution.

5. **`UpdateDashboard` accepted a gopherstack-invented `Name` (rename) parameter.** Real
   `UpdateDashboardInput` has exactly `DashboardId`/`RefreshSchedule`/
   `TerminationProtectionEnabled`/`Widgets` — dashboards cannot be renamed via this or any
   other CloudTrail API. Removed the rename capability; added the three real fields, none of
   which were previously modeled at all (also missing from `CreateDashboard`/`GetDashboard`,
   the #2 deferred item from the prior pass: "Dashboard Widgets modeling"). `Dashboard`
   gained `Widgets`, `RefreshSchedule`, `TerminationProtectionEnabled`, `CreatedTimestamp`/
   `UpdatedTimestamp`, and `LastRefreshId`/`LastRefreshFailureReason`.

6. **`StartDashboardRefresh` returned a fabricated response shape.** Real
   `StartDashboardRefreshOutput` has exactly one field, `RefreshId`. The handler instead
   returned `{DashboardArn, Status}`, with `Status` hardcoded to `"REFRESHING"` — not even a
   valid `DashboardStatus` enum value (real values: `CREATING`/`CREATED`/`UPDATING`/
   `UPDATED`/`DELETING`). Fixed to return only `RefreshId`, generated fresh per call and
   stored on the dashboard as `LastRefreshId`.

7. **`EventDataStore` timestamp epoch-seconds bug** (the flagged bug class: raw `time.Time`
   marshaled where the awsjson1.1 deserializer requires an epoch-seconds JSON number).
   `edsToMap`'s `CreatedTimestamp`/`UpdatedTimestamp` were placed directly as `time.Time`
   values, which `encoding/json` renders as RFC3339 strings; the real deserializer calls
   `smithytime.ParseEpochSeconds(f64)` on these fields (confirmed in `deserializers.go`), so
   a real SDK client would fail to decode them (or, since these aren't pointer fields on the
   Go SDK's `*time.Time`, would just always see `nil`). Affects
   `CreateEventDataStore`/`GetEventDataStore`/`UpdateEventDataStore`/
   `RestoreEventDataStore`/`ListEventDataStores` (all share `edsToMap`) and `ListQueries`'
   `CreationTime` (a separate, similar bug in `handleListQueries`). Fixed by emitting
   `float64(t.Unix())` at the map-building call sites, the same pattern already used
   correctly by `trailToMap`/`GetTrailStatus`/the import handlers/`DescribeQuery`.

8. **`Event.MarshalJSON` had no matching `UnmarshalJSON`** — a pre-existing, previously
   *documented-but-deliberately-unfixed* bug (`TestInMemoryBackend_SnapshotRestore_
   EventsPreexistingBug`, added during an earlier store.Table migration pass with a comment
   explicitly deferring the fix). `Event.EventTime` is hand-encoded as an epoch-seconds JSON
   number for the LookupEvents wire response, but with no matching decoder, any `Snapshot`
   containing so much as one recorded event failed `Restore` outright (`b.events` — the one
   raw, non-`store.Table` field — round-trips through the exact same `Event.MarshalJSON`).
   Added the inverse `UnmarshalJSON` and repurposed the test
   (`TestInMemoryBackend_SnapshotRestore_EventsRoundTrip`) to assert the round trip now
   succeeds losslessly, including the new `EventCategory` field.

9. **`StartImport`'s `ImportSource.S3` only modeled `S3LocationUri`** (a gap from the prior
   pass). Real `S3ImportSource` has three fields, *all* marked `required` in the SDK docs:
   `S3LocationUri`, `S3BucketRegion`, `S3BucketAccessRoleArn`. The latter two were accepted
   on the wire and silently discarded. Restructured `Import.ImportSource` from a bare
   `string` into a real `*ImportSource{S3: *S3ImportSource{...}}` type, storing and echoing
   all three fields on `StartImport`/`GetImport`/`StopImport`.

10. **`LookupEvents` ignored the `EventCategory` input field** (a gap from the prior pass).
    Real semantics: omitting `EventCategory` (or passing anything but `"insight"`, its only
    enum value) returns Management events only; passing `"insight"` returns Insight events
    only. Added an `EventCategory` field to `Event` (defaulted to `"Management"` in
    `RecordEvent`, since this backend only ever records management-plane API calls) and wired
    the filter into `eventMatchesFilters`.

### Deliberately NOT flagged (false-positive checks per parity-principles.md rule 4)

- `ListImportFailures`/`SearchSampleQueries`/`ListPublicKeys`/`ListInsightsData`/
  `ListInsightsMetricData` all return empty lists. Each was re-checked against its real SDK
  output shape: the *shape* is correct, and the emptiness reflects a genuinely unimplemented
  downstream capability (import file replay, Insights event synthesis) rather than a
  populated-but-never-returned map — documented simplifications, not disguised no-ops.
- `dashToMap`'s extra `Status`/`Name` keys (present on some but not all of Create/Get/Update
  Dashboard's real output structs) and similar cross-op-family "superset" response shapes
  are inert: AWS JSON-protocol deserializers ignore unknown response keys (confirmed via
  each `case "...":`/`default: ignore` switch in `deserializers.go`), so returning every
  field any one of Create/Get/Update needs from a single shared `dashToMap`/`edsToMap`/
  `importToMap` helper is harmless, not a bug — only *missing or wrong-cased* keys for
  fields a client actually reads are bugs (see items 6/7 above, which were exactly that).
