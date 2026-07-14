---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: kinesisanalyticsv2
sdk_module: aws-sdk-go-v2/service/kinesisanalyticsv2@v1.36.22
last_audit_commit: 782e2a93
last_audit_date: 2026-07-13
overall: A            # genuine fixes found, several disguised no-ops
ops:
  CreateApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "inline ApplicationConfiguration/CloudWatchLoggingOptions were previously silently discarded; now seeded via SeedApplicationConfiguration without bumping past version 1"}
  DescribeApplication: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateApplication: {wire: partial, errors: ok, state: ok, persist: ok, note: "CurrentApplicationVersionId concurrency check was previously ignored entirely (fixed); ApplicationConfigurationUpdate/CloudWatchLoggingOptionUpdates/RunConfigurationUpdate/RuntimeEnvironmentUpdate/ConditionalToken accepted-but-ignored (gap)"}
  DeleteApplication: {wire: partial, errors: ok, state: ok, persist: ok, note: "CreateTimestamp request field parsed but not validated against the app's actual CreateTimestamp (gap, low risk -- leniency, not a false accept/reject)"}
  ListApplications: {wire: ok, errors: ok, state: ok, persist: ok}
  StartApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "now returns OperationId and records an ApplicationOperation (previously fabricated an empty response and never touched the operations map)"}
  StopApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "same fix as StartApplication"}
  RollbackApplication: {wire: ok, errors: ok, state: ok, persist: n/a, note: "previously ALWAYS failed with InvalidArgumentException for any app that had ever been modified, because b.versions only ever held the version-1 CreateApplication snapshot -- fixed by recording version history on every version bump (see checkAndBumpVersion/snapshotVersion). Zero test coverage before this audit."}
  DescribeApplicationOperation: {wire: ok, errors: ok, state: ok, persist: n/a, note: "previously ALWAYS returned ResourceNotFoundException -- b.operations was never populated by any caller. Fixed via recordOperation, wired into Start/Stop/Update/RollbackApplication. Response shape also fixed to include required StartTime/EndTime (epoch, via awstime.Epoch) and to drop the erroneous OperationId field real AWS's ApplicationOperationInfoDetails doesn't have."}
  ListApplicationOperations: {wire: ok, errors: ok, state: ok, persist: n/a, note: "previously ALWAYS returned an empty list for the same reason as DescribeApplicationOperation; same fix. operations/versions remain intentionally unpersisted, matching pre-Phase-3.3 behavior (see persistence.go doc comments) -- both are ephemeral request-tracking history, not resource state."}
  DescribeApplicationVersion: {wire: ok, errors: ok, state: ok, persist: n/a, note: "previously only ever found version 1 for the same root cause as RollbackApplication; fixed by the same version-history recording"}
  ListApplicationVersions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "same fix; zero test coverage before this audit"}
  CreateApplicationSnapshot: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeApplicationSnapshot: {wire: ok, errors: ok, state: ok, persist: ok}
  ListApplicationSnapshots: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteApplicationSnapshot: {wire: ok, errors: ok, state: ok, persist: ok}
  AddApplicationCloudWatchLoggingOption: {wire: ok, errors: ok, state: ok, persist: ok}
  AddApplicationInput: {wire: ok, errors: ok, state: ok, persist: ok}
  AddApplicationInputProcessingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  AddApplicationOutput: {wire: ok, errors: ok, state: ok, persist: ok}
  AddApplicationReferenceDataSource: {wire: ok, errors: ok, state: ok, persist: ok}
  AddApplicationVpcConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteApplicationCloudWatchLoggingOption: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteApplicationInputProcessingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteApplicationOutput: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteApplicationReferenceDataSource: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteApplicationVpcConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateApplicationPresignedUrl: {wire: ok, errors: ok, state: ok, persist: n/a, note: "synthetic AuthorizedUrl, matches emulator convention for presigned-URL ops elsewhere in this repo"}
  UpdateApplicationMaintenanceConfiguration: {wire: partial, errors: ok, state: ok, persist: ok, note: "ApplicationMaintenanceWindowEndTime never computed/returned (gap, low value)"}
  DiscoverInputSchema: {wire: deferred, errors: ok, state: n/a, persist: n/a, note: "always returns a fixed synthetic JSON/UTF-8 schema regardless of the target resource -- real AWS samples live stream data, which this emulator cannot do; documented limitation, not a bug"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  error_mapping: {status: ok, note: "handleError previously mapped ErrConcurrentModification to the generic InvalidArgumentException __type (it wraps awserr.ErrInvalidParameter and was shadowed by that case). Fixed to emit the AWS-accurate ConcurrentModificationException __type -- aws-sdk-go-v2 switches on __type to build *types.ConcurrentModificationException for caller retry logic, so the old behavior silently broke every SDK-level optimistic-concurrency retry loop. Fix matches sibling service kinesisanalytics (v1)'s existing (400, ConcurrentModificationException) precedent."}
gaps:
  - UpdateApplication silently ignores ApplicationConfigurationUpdate, CloudWatchLoggingOptionUpdates, RunConfigurationUpdate, RuntimeEnvironmentUpdate, and ConditionalToken (only CurrentApplicationVersionId concurrency and ServiceExecutionRoleUpdate/ApplicationDescription are implemented). Add*/Delete* ops remain the only supported way to mutate SQL/VPC config after creation. (bd: file follow-up)
  - CreateApplication/UpdateApplication never model ApplicationCodeConfiguration, FlinkApplicationConfiguration, EnvironmentProperties, ApplicationSnapshotConfiguration, ApplicationSystemRollbackConfiguration, ApplicationEncryptionConfiguration, or ZeppelinApplicationConfiguration -- accepted on the wire (to avoid rejecting well-formed requests) but produce no backend state and are never echoed back. (bd: file follow-up)
  - DeleteApplication parses the CreateTimestamp request field but never validates it against the application's actual creation time (real AWS uses it as a safety check). Leniency only -- never causes a false accept/reject, low priority.
  - applicationDetailOutput omits several optional real-AWS fields: LastUpdateTimestamp, ConditionalToken, ApplicationVersionCreateTimestamp, ApplicationVersionRolledBackFrom/To, ApplicationVersionUpdatedFrom, ApplicationMaintenanceConfigurationDescription. None are required fields; SDK clients that don't read them are unaffected.
  - DeleteApplication is synchronous (app removed immediately); real AWS transitions through a DELETING status first. ApplicationStatusDeleting const is defined but unused. Matches the synchronous-delete convention used elsewhere in this codebase; not fixed.
deferred:
  - DiscoverInputSchema (inherently synthetic without live stream sampling)
leaks: {status: clean, note: "DeleteApplication cleans up operations/versions/snapshots map entries; existing TestBackend_DeleteApplication_CleansOperations strengthened this pass to actually populate an operation before asserting cleanup (previously always exercised the trivial always-empty-map path)."}
---

## Notes

Protocol: awsjson1.1 (X-Amz-Target: `KinesisAnalytics_20180523.<Op>`, single POST
endpoint). RouteMatcher/ExtractOperation verified correct against the real SDK's
serializer target-prefix strings (`serializers.go`) -- no change needed.

### Real bugs found and fixed this pass

1. **UpdateApplication ignored CurrentApplicationVersionId entirely** (optimistic
   concurrency never checked) -- `backend.go`. The interface signature didn't even
   accept the parameter; the handler parsed it from the request but dropped it on
   the floor. Fixed by threading it through and reusing `checkAndBumpVersion`,
   matching every other versioned op in this file.

2. **handleError mis-mapped ConcurrentModificationException to the generic
   InvalidArgumentException `__type`** -- `handler.go`. `ErrConcurrentModification`
   wraps `awserr.ErrInvalidParameter`, so it was always caught by the generic
   `errors.Is(err, awserr.ErrInvalidParameter)` case before ever reaching a
   specific check (there wasn't one). aws-sdk-go-v2 builds
   `*types.ConcurrentModificationException` by switching on the response `__type`
   field, so every client-side retry-on-conflict loop was silently broken. Fixed
   by adding a specific case ahead of the generic one, matching sibling service
   kinesisanalytics (v1)'s existing precedent.

3. **`b.operations` was never populated -- DescribeApplicationOperation and
   ListApplicationOperations were permanently broken** (always 404 / always
   empty) -- `backend.go`. This is the "real-looking op filtering a
   never-populated map" bug class called out in
   `.claude/memories/parity-principles.md`. Fixed by adding `recordOperation`
   and wiring it into StartApplication, StopApplication, UpdateApplication, and
   RollbackApplication (all four now also return a real `OperationId` in their
   response, matching the real API shapes).

4. **RollbackApplication could never succeed against real traffic** --
   `backend.go`. Its guard requires at least 2 recorded versions, but nothing
   except `CreateApplication` (which seeds exactly 1) ever appended to
   `b.versions` -- every `Add*`/`Delete*` config op and `UpdateApplication`
   bumped `ApplicationVersionId` but left the version-history map untouched.
   Fixed by having `checkAndBumpVersion`'s callers `defer
   b.snapshotVersion(region, name, app)` (captures state *after* the caller's
   field mutations, not at the moment of the version bump -- see the doc
   comment on `checkAndBumpVersion`/`snapshotVersion`). This also fixes
   `DescribeApplicationVersion`/`ListApplicationVersions`, which had the same
   root cause. All three ops had zero test coverage before this audit.

5. **CreateApplication silently discarded inline `ApplicationConfiguration`
   and `CloudWatchLoggingOptions`** -- `handler.go`/`backend.go`. Real clients
   (Terraform, CloudFormation, the console) overwhelmingly create a
   fully-configured application in one `CreateApplication` call rather than
   `CreateApplication` followed by a series of `Add*` calls; gopherstack
   accepted and 200'd requests carrying `Inputs`/`Outputs`/
   `ReferenceDataSources`/`VpcConfigurations`/`CloudWatchLoggingOptions` and
   threw all of it away. Fixed by adding `SeedApplicationConfiguration`, called
   from `handleCreateApplication` when inline config is present. Deliberately
   does *not* go through the `Add*` backend methods, each of which bumps
   `ApplicationVersionId` -- real AWS keeps a freshly created application, even
   with inline config, at version 1.

6. **Epoch timestamps computed by hand instead of via `pkgs/awstime.Epoch`**
   -- `handler.go`/`backend.go` (`CreateTimestamp`, `SnapshotCreationTimestamp`).
   Not wire-breaking (both produced whole-second epoch numbers), but this is
   exactly the reimplementation `.claude/memories/pkgs-catalog.md` calls out
   as a recurring bug source elsewhere (QuickSight/IoT). Switched to
   `awstime.Epoch` for consistency and correct sub-second precision.

### Traps for the next auditor

- `ApplicationOperation.OperationID`/`StartTimestamp`/`EndTimestamp` are wired
  now -- don't re-flag `b.operations` as unused; `recordOperation` populates it
  from four call sites.
- `checkAndBumpVersion` is deliberately a free function that does NOT append
  version history itself -- callers must `defer
  b.snapshotVersion(region, name, app)` immediately after a successful call,
  so the snapshot captures state *after* the caller's subsequent field
  mutations. Don't fold the two back together without preserving that
  ordering (a naive merge reintroduces the "version 2 missing the actual
  change" bug this pass found and fixed).
- `SeedApplicationConfiguration` intentionally does not bump
  `ApplicationVersionId` or append a *new* version-history entry -- it
  overwrites the existing version-1 snapshot in place so
  `DescribeApplicationVersion(name, 1)` reflects the seeded config too. This
  is correct (matches real AWS) but looks surprising next to every other
  version-bumping method in the file.
- `operations` and `versions` are intentionally NOT persisted (`persistence.go`
  only snapshots `applications`/`snapshots` tables) -- this predates this audit
  and matches pre-Phase-3.3 behavior; don't treat it as a newly-introduced gap.
