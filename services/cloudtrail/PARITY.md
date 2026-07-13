---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: cloudtrail
sdk_module: aws-sdk-go-v2/service/cloudtrail@v1.55.7   # version audited against
last_audit_commit: 174b1f53                             # HEAD when this manifest was written
last_audit_date: 2026-07-12
overall: A            # A = ~1k genuine fixes found; B = already-accurate, proven op-by-op
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateTrail: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: response KmsKeyId key case (was KMSKeyId)"}
  GetTrail: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: response KmsKeyId key case"}
  UpdateTrail: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: response KmsKeyId key case"}
  DeleteTrail: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeTrails: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: response KmsKeyId key case (via trailToMap)"}
  ListTrails: {wire: ok, errors: ok, state: ok, persist: ok, note: "no NextToken pagination — see gaps"}
  StartLogging: {wire: ok, errors: ok, state: ok, persist: ok}
  StopLogging: {wire: ok, errors: ok, state: ok, persist: ok}
  GetTrailStatus: {wire: ok, errors: ok, state: ok, persist: ok, note: "IsLogging/StartLoggingTime/StopLoggingTime/LatestDeliveryTime as epoch numbers, TimeLoggingStarted/Stopped as RFC3339 strings — matches SDK deserializer exactly"}
  PutEventSelectors: {wire: ok, errors: ok, state: ok, persist: ok}
  GetEventSelectors: {wire: ok, errors: ok, state: ok, persist: ok}
  PutInsightSelectors: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: EventDataStore path was unreachable (dead backend method PutEDSInsightSelectors never wired); now branches TrailName vs EventDataStore per real PutInsightSelectorsInput"}
  GetInsightSelectors: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: added GetEDSInsightSelectors + EventDataStore branch, mirroring PutInsightSelectors fix"}
  LookupEvents: {wire: ok, errors: ok, state: ok, persist: ok, note: "EventTime epoch via awstime.Epoch; newest-first; NextToken pagination works. EventCategory input field not filtered (see gaps)"}
  AddTags: {wire: ok, errors: ok, state: ok, persist: ok}
  RemoveTags: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTags: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateChannel: {wire: ok, errors: ok, state: ok, persist: ok}
  GetChannel: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateChannel: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: Name (rename) parameter was accepted on the wire but silently dropped by both handler and backend"}
  DeleteChannel: {wire: ok, errors: ok, state: ok, persist: ok}
  ListChannels: {wire: ok, errors: ok, state: ok, persist: ok, note: "no NextToken pagination — see gaps"}
  CreateDashboard: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDashboard: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDashboard: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDashboard: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDashboards: {wire: ok, errors: ok, state: ok, persist: ok, note: "no NextToken pagination — see gaps"}
  StartDashboardRefresh: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateEventDataStore: {wire: ok, errors: ok, state: ok, persist: ok}
  GetEventDataStore: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateEventDataStore: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteEventDataStore: {wire: ok, errors: ok, state: ok, persist: ok, note: "termination-protection conflict correctly returns EventDataStoreTerminationProtectedException"}
  ListEventDataStores: {wire: ok, errors: ok, state: ok, persist: ok}
  RestoreEventDataStore: {wire: ok, errors: ok, state: ok, persist: ok}
  StartEventDataStoreIngestion: {wire: ok, errors: ok, state: ok, persist: ok}
  StopEventDataStoreIngestion: {wire: ok, errors: ok, state: ok, persist: ok}
  DisableFederation: {wire: ok, errors: ok, state: ok, persist: ok}
  EnableFederation: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  GetResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  PutResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  StartQuery: {wire: ok, errors: ok, state: ok, persist: ok}
  CancelQuery: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeQuery: {wire: ok, errors: ok, state: ok, persist: ok}
  GetQueryResults: {wire: ok, errors: ok, state: partial, persist: ok, note: "QueryResultRows/actual row data always empty — CloudTrail Lake SQL execution genuinely not implemented, documented limitation not a disguised stub (QueryStatus/QueryStatistics shape is real)"}
  ListQueries: {wire: ok, errors: ok, state: ok, persist: ok, note: "no NextToken pagination — see gaps"}
  GenerateQuery: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed: was entirely wrong shape (returned QueryId/QueryString, neither a real GenerateQueryOutput field; ignored the required Prompt input entirely; polluted the queries table with a fake persisted Query). Now returns QueryStatement/QueryAlias/EventDataStoreOwnerAccountId and requires Prompt+EventDataStores"}
  StartImport: {wire: ok, errors: ok, state: ok, persist: ok, note: "S3ImportSource models only S3LocationUri, not S3BucketRegion/S3BucketAccessRoleArn (accepted but unused) — acceptable simplification, import execution is not real"}
  GetImport: {wire: ok, errors: ok, state: ok, persist: ok}
  ListImports: {wire: ok, errors: ok, state: ok, persist: ok, note: "no NextToken pagination — see gaps"}
  StopImport: {wire: ok, errors: ok, state: ok, persist: ok}
  ListImportFailures: {wire: ok, errors: ok, state: partial, persist: n/a, note: "always empty — consistent since imports never actually execute/fail in this backend"}
  GetEventConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: was a hardcoded stub with fabricated field names (ResourceArn/EventConfiguration, neither real). Now resolves TrailName/EventDataStore, persists AggregationConfigurations/ContextKeySelectors/MaxEventSize, returns TrailARN or EventDataStoreArn"}
  PutEventConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: was a no-op stub (validated ResourceArn — a nonexistent input field — and discarded everything else)"}
  RegisterOrganizationDelegatedAdmin: {wire: ok, errors: ok, state: partial, persist: n/a, note: "accepts/validates only; no real org-admin state modeled — acceptable given no cross-account org emulation exists"}
  DeregisterOrganizationDelegatedAdmin: {wire: ok, errors: ok, state: partial, persist: n/a, note: "same as RegisterOrganizationDelegatedAdmin"}
  SearchSampleQueries: {wire: ok, errors: ok, state: partial, persist: n/a, note: "always empty list; SDK output shape has no other required fields"}
  ListPublicKeys: {wire: ok, errors: ok, state: partial, persist: n/a, note: "always empty; legacy CloudTrail log-file-validation feature, no public keys are ever generated by this backend"}
  ListInsightsData: {wire: ok, errors: ok, state: partial, persist: n/a, note: "always empty; no Insights event generation exists"}
  ListInsightsMetricData: {wire: ok, errors: ok, state: partial, persist: n/a, note: "always empty; same reason"}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "ListTrails/ListChannels/ListDashboards/ListImports/ListQueries/ListEventDataStores accept NextToken in the real API but this backend always returns every item in one page (no pagination). Low risk for typical emulator-scale resource counts but a real SDK client passing NextToken from a previous (different) call gets ignored rather than erroring."
  - "LookupEvents ignores the EventCategory input field (only 'Insight' is a valid non-default value upstream); harmless today since this backend never synthesizes Insight-category events, but if a synthetic-Insights feature is ever added this filter needs wiring."
  - "StartImport's ImportSource.S3 only models S3LocationUri; S3BucketRegion and S3BucketAccessRoleArn are accepted on the wire but never stored/echoed. Import execution itself is not real (no actual file replay), so this is a low-priority simplification."
  - "RegisterOrganizationDelegatedAdmin / DeregisterOrganizationDelegatedAdmin validate input but track no org-admin state (no GetOrganizationDelegatedAdmins-equivalent op exists in gopherstack's CloudTrail service to read it back anyway)."
  - "PARITY-FOLLOWUP (pkgs/service, out of scope for this service): pkgs/service/cloudtrail_capture.go's wrapCloudTrailCapture records a management event unconditionally after next(c) returns, regardless of the wrapped handler's response status — a failed (4xx/5xx) mutating API call is captured identically to a successful one, and the synthesized CloudTrailEvent detail JSON always sets errorCode/errorMessage-equivalent fields absent (no error info at all). Real CloudTrail records failed calls too, but with populated errorCode/errorMessage. Not broken (chokepoint IS wired correctly end-to-end: RecordManagementEvent -> InMemoryBackend.RecordManagementEvent -> LookupEvents returns real captured events), just an accuracy gap in a shared file outside services/cloudtrail/'s edit scope."
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - "CloudTrail Lake SQL query execution (StartQuery/GetQueryResults actually evaluating QueryStatement against recorded events) — QueryResultRows is always empty by design; a real implementation would need a SQL-subset interpreter against the events log."
  - "Dashboard Widgets modeling (CreateDashboard/GetDashboard/UpdateDashboard accept but do not model/store the Widgets list)."
leaks: {status: clean, note: "no goroutines/janitors in this service; Reset() closes every tags.Tags (trails/channels/dashboards/eventDataStores) before clearing tables, consistent with prior audits; eventConfigs map added this pass is a plain map reset alongside events, no leak surface"}
---

## Notes

Protocol: awsjson1.1 (single POST endpoint, `X-Amz-Target: CloudTrail_20131101.<Op>` —
verified against the real SDK's `httpBindingEncoder.SetHeader("X-Amz-Target").String(...)`
call sites in serializers.go; the service's `cloudtrailTargetPrefix` constant matches
exactly). Route matcher is a simple prefix check and was NOT a bug this pass.

### Real bugs found and fixed this pass

1. **`KmsKeyId` wire-case bug** (`handler.go` `trailToMap`, plus the `createTrailBody`/
   `updateTrailBody` request tags for consistency). The real SDK's Trail deserializer
   switches on the exact string `"KmsKeyId"`; this service's Trail response wrote
   `"KMSKeyId"` (wrong capitalization of "MS"). A real `aws-sdk-go-v2` client hitting
   `GetTrail`/`DescribeTrails`/`CreateTrail`/`UpdateTrail` on a trail with a KMS key
   would always see `KmsKeyId == nil` — the field silently never round-tripped. Confirmed
   by cross-checking `EventDataStore`'s parallel field, which already used the correct
   `"KmsKeyId"` casing throughout — Trail was the outlier. An existing test
   (`TestCloudTrailTrailWithAllFields/update_trail_optional_string_fields`) asserted the
   buggy key and was updated to assert the correct one.

2. **`GenerateQuery` had an entirely wrong response shape and ignored a required input
   field.** `GenerateQueryOutput` per the real SDK has exactly three fields:
   `EventDataStoreOwnerAccountId`, `QueryAlias`, `QueryStatement`. This service returned
   `QueryId`/`QueryString` (neither exists on `GenerateQueryOutput` — a real client would
   get a struct with every field nil after calling this op) and additionally accepted a
   nonexistent `RequestedQueryMaxResults` field while silently dropping the real
   (required) `Prompt` field. It also polluted the `queries` table with a fabricated,
   never-real `Query` record visible via `ListQueries`/`DescribeQuery`/`CancelQuery`,
   which real `GenerateQuery` never does (it only returns generated SQL text, no
   persisted query object). Rewrote to parse `EventDataStores`+`Prompt` (both required,
   matching the SDK model), removed the `queries` table pollution, and return the correct
   three fields via a new `GeneratedQuery` backend type.

3. **`GetEventConfiguration`/`PutEventConfiguration` were hardcoded/no-op stubs with
   fabricated field names.** The handlers accepted/returned a nonexistent `ResourceArn`
   field; real `GetEventConfigurationInput`/`PutEventConfigurationInput` take
   `TrailName` or `EventDataStore` (mutually exclusive), and the real outputs are
   `AggregationConfigurations`/`ContextKeySelectors`/`MaxEventSize`/`TrailARN`/
   `EventDataStoreArn` — none of which existed before. `PutEventConfiguration` was a
   pure no-op (validated the fake `ResourceArn`, stored nothing). Added a new
   `EventConfiguration` backend type (persisted per-resource-ARN, wired into
   `backendSnapshot`/`Snapshot`/`Restore`/`Reset`), a `resolveEventConfigResource`
   helper reusing existing `GetTrail`/`GetEventDataStore` lookups (so
   `TrailNotFoundException`/`EventDataStoreNotFoundException` fall out for free), and
   real Get/Put backend methods.

4. **`PutInsightSelectors`/`GetInsightSelectors` silently rejected valid
   `EventDataStore`-scoped requests.** Real `PutInsightSelectorsInput`/
   `GetInsightSelectorsInput` accept either `TrailName` or `EventDataStore` (Insights can
   be enabled on an event data store, not just a trail). The handlers only ever read
   `TrailName`, so an `EventDataStore`-only request got `"TrailName is required"` — wrong
   error for a well-formed request. The backend already had an unused, dead
   `PutEDSInsightSelectors` method (never called from any handler) confirming this was a
   known-incomplete wiring, not an intentional omission. Added `GetEDSInsightSelectors`
   (mirroring `GetInsightSelectors`'s `InsightNotEnabledException` behavior) and wired
   both handlers to branch on which parameter was supplied.

5. **`UpdateChannel` silently dropped the `Name` (rename) parameter.** Real
   `UpdateChannelInput` accepts `Name` to rename a channel; the handler's body struct
   didn't parse it and the backend signature had no `name` parameter at all, so a
   channel-rename request returned 200 OK but never renamed anything. An existing test
   (`TestCloudTrailCoverage_Channel`) already sent `"Name": "updated-channel"` in an
   `UpdateChannel` call but only asserted `200 OK` — it would have passed either way,
   which is exactly the "silent gap the existing suite didn't catch" pattern the audit
   was looking for. Fixed with the same delete-mutate-rePut reindexing pattern already
   used for `UpdateDashboard`/`UpdateEventDataStore` renames (`channelsByName` index).

### Deliberately NOT flagged (false-positive checks per parity-principles.md rule 4)

- `GetQueryResults`/`ListImportFailures`/`SearchSampleQueries`/`ListPublicKeys`/
  `ListInsightsData`/`ListInsightsMetricData` all return empty lists/rows. Each was
  checked against its real SDK output shape: the *shape* is correct (real field names,
  real optional-field omission), and the emptiness reflects a genuinely unimplemented
  downstream capability (SQL execution, import file replay, Insights event synthesis)
  rather than a populated-but-never-returned map. These are documented simplifications,
  not disguised no-ops.
- Several outputs include extra fields absent from the real SDK output struct (e.g.
  `dashToMap`'s `Status` key on `CreateDashboardOutput`, which has no `Status` field;
  `DescribeQueryOutput`'s response including `CreationTime`, which doesn't exist on that
  output). AWS JSON-protocol deserializers ignore unknown response keys (confirmed via
  each `case "...":`/`default: ignore` switch in `deserializers.go`), so these are inert,
  not bugs — only *missing or wrong-cased* keys for fields the client actually reads are
  bugs (see the `KmsKeyId` fix above, which was exactly that).
