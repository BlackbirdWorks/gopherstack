---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: elasticbeanstalk
sdk_module: aws-sdk-go-v2/service/elasticbeanstalk@v1.34.0   # version audited against
last_audit_commit: de5340f8                       # HEAD when this manifest was written
last_audit_date: 2026-07-12
overall: A            # A = genuine fixes found; B = already-accurate, proven op-by-op
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateApplication: {wire: ok, errors: ok, state: fixed, persist: ok, note: "DateUpdated now surfaced on bump path (see UpdateApplication); ResourceLifecycleConfig now rendered"}
  DescribeApplications: {wire: fixed, errors: ok, state: ok, persist: ok, note: "applicationDescType now includes ResourceLifecycleConfig (was stored but unreadable)"}
  UpdateApplication: {wire: ok, errors: ok, state: fixed, persist: ok, note: "was not bumping DateUpdated on description change; fixed"}
  DeleteApplication: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateApplicationResourceLifecycle: {wire: ok, errors: ok, state: ok, persist: ok, note: "stored value now reachable via Describe/Create/UpdateApplication, see applicationDescType fix"}
  CreateApplicationVersion: {wire: ok, errors: fixed, state: fixed, persist: ok, note: "was not validating parent Application exists when AutoCreateApplication=false (AWS-documented InvalidParameterValue case); now validated. Auto-created Application now gets DateCreated/DateUpdated"}
  DescribeApplicationVersions: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateApplicationVersion: {wire: ok, errors: ok, state: fixed, persist: ok, note: "was not bumping DateUpdated; fixed"}
  DeleteApplicationVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateEnvironment: {wire: ok, errors: ok, state: ok, persist: ok, note: "environment created Ready/Green immediately -- no stuck-Launching disguised no-op"}
  DescribeEnvironments: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateEnvironment: {wire: ok, errors: ok, state: ok, persist: ok, note: "already bumped DateUpdated correctly"}
  TerminateEnvironment: {wire: ok, errors: ok, state: ok, persist: ok}
  CloneEnvironment: {wire: n/a, errors: n/a, state: ok, persist: ok, note: "NOT a real AWS Elastic Beanstalk API operation (absent from aws-sdk-go-v2/service/elasticbeanstalk entirely -- no api_op_CloneEnvironment.go, no deserializer). Dead/fabricated action name; real SDK clients can never construct this request. Not fixed (out of scope: removing it changes the dispatch table, no wire-parity benefit since no real client can reach it). Flagged as a gap."}
  ComposeEnvironments: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateConfigurationTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateConfigurationTemplate: {wire: ok, errors: ok, state: fixed, persist: ok, note: "was not bumping DateUpdated; fixed"}
  DeleteConfigurationTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeConfigurationSettings: {wire: partial, errors: ok, state: ok, persist: ok, note: "missing optional DateCreated/DateUpdated/DeploymentStatus/PlatformArn fields on ConfigurationSettingsDescription -- low-traffic, not fixed this pass"}
  DescribeConfigurationOptions: {wire: partial, errors: ok, state: n/a, persist: n/a, note: "returns a small fixed catalog (3 options) regardless of SolutionStackName/PlatformArn/EnvironmentName; real AWS returns hundreds of platform-specific options. Documented as a known limitation, not fixed (large scope: would require a per-platform option catalog)"}
  ValidateConfigurationSettings: {wire: ok, errors: ok, state: ok, persist: n/a, note: "real AWS op (api_op_ValidateConfigurationSettings.go + deserializer exist in the SDK); implementation validates option-setting namespaces against a fixed allowlist -- a reasonable partial emulation of real server-side validation, not a stub"}
  DescribeEnvironmentResources: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeEvents: {wire: ok, errors: ok, state: ok, persist: ok, note: "Severity/StartTime/EnvironmentId filters implemented (fixed in earlier sweep, #2165)"}
  ListTagsForResource: {wire: ok, errors: fixed, state: fixed, persist: ok, note: "now reaches ConfigurationTemplate and PlatformVersion tags (previously only Application/Environment/ApplicationVersion); not-found ARN now returns ResourceNotFoundException instead of InvalidParameterValue"}
  UpdateTagsForResource: {wire: ok, errors: fixed, state: fixed, persist: ok, note: "same ConfigurationTemplate/PlatformVersion + error-code fixes as ListTagsForResource"}
  CreatePlatformVersion: {wire: fixed, errors: ok, state: ok, persist: ok, note: "PlatformArn was built with an empty account ID (arn:aws:elasticbeanstalk:region::platform/...), producing a malformed ARN for what is an account-owned custom-platform resource; fixed to use the caller's account ID"}
  DeletePlatformVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribePlatformVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPlatformVersions: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPlatformBranches: {wire: ok, errors: ok, state: ok, persist: n/a, note: "static catalog with Filters.member support; acceptable emulation of a largely-static AWS list"}
  ListAvailableSolutionStacks: {wire: ok, errors: ok, state: ok, persist: n/a, note: "static catalog; acceptable"}
  DescribeAccountAttributes: {wire: ok, errors: ok, state: ok, persist: n/a}
  DescribeEnvironmentHealth: {wire: ok, errors: ok, state: ok, persist: n/a}
  DescribeInstancesHealth: {wire: ok, errors: ok, state: n/a, persist: n/a, note: "always returns empty list -- correct since the backend never models EC2 instances; not a disguised stub"}
  DescribeEnvironmentManagedActions: {wire: ok, errors: ok, state: n/a, persist: n/a, note: "always empty -- correct, backend never schedules future actions"}
  DescribeEnvironmentManagedActionHistory: {wire: ok, errors: ok, state: ok, persist: ok}
  ApplyEnvironmentManagedAction: {wire: ok, errors: ok, state: ok, persist: ok}
  AbortEnvironmentUpdate: {wire: ok, errors: ok, state: n/a, persist: n/a, note: "no-op is correct: updates complete synchronously in this backend, so there is never anything in-flight to abort"}
  RebuildEnvironment: {wire: ok, errors: ok, state: n/a, persist: n/a, note: "no-op is correct for a backend with no real infra to rebuild"}
  RestartAppServer: {wire: ok, errors: ok, state: n/a, persist: n/a, note: "no-op is correct, same reasoning"}
  RequestEnvironmentInfo: {wire: ok, errors: ok, state: n/a, persist: n/a}
  RetrieveEnvironmentInfo: {wire: ok, errors: ok, state: ok, persist: n/a, note: "always empty EnvironmentInfo list -- correct, no log-tailing state is modeled"}
  CheckDNSAvailability: {wire: ok, errors: ok, state: ok, persist: n/a}
  CreateStorageLocation: {wire: ok, errors: ok, state: ok, persist: n/a}
  SwapEnvironmentCNAMEs: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateEnvironmentOperationsRole: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateEnvironmentOperationsRole: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateApplicationResourceLifecycle_seeAbove: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  ARN construction: {status: fixed, note: "Application/Environment/ApplicationVersion/ConfigurationTemplate ARN patterns verified against https://docs.aws.amazon.com/elasticbeanstalk/latest/dg/AWSHowTo.iam.policies.arn.html (application/{app}, applicationversion/{app}/{ver}, configurationtemplate/{app}/{tmpl}, environment/{app}/{env}, platform/{name}/{version}) -- all correct except CreatePlatformVersion's missing account ID (fixed)"}
  error-code mapping: {status: fixed, note: "handleOpError previously mapped every ErrNotFound to InvalidParameterValue uniformly; ListTagsForResource/UpdateTagsForResource ARN-not-found now maps to the AWS-documented ResourceNotFoundException via new ErrResourceNotFound sentinel"}
  tag reachability: {status: fixed, note: "ConfigurationTemplate and PlatformVersion accept Tags at creation but List/UpdateTagsForResource could never reach them (lookupTagsByARN/ensureTagsByARN only checked Application/Environment/ApplicationVersion) -- disguised-no-op bug class; fixed"}
  DateUpdated bumping: {status: fixed, note: "UpdateApplication, UpdateApplicationVersion, UpdateConfigurationTemplate mutated state but left DateUpdated unchanged; UpdateEnvironment was already correct. Fixed all three"}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "CloneEnvironment is routed/implemented in this service but is NOT a real AWS Elastic Beanstalk API operation (absent from aws-sdk-go-v2/service/elasticbeanstalk@v1.34.0 entirely: no api_op_CloneEnvironment.go, no deserializer). Zero wire-parity risk (unreachable by real SDK clients, since no client can construct this request) but is dead/speculative surface; consider removing or documenting as a gopherstack-only convenience API. Not filed as bd issue this pass -- flagging for triage. (ValidateConfigurationSettings, by contrast, IS a real op -- see ops table.)"
  - "DescribeConfigurationOptions returns a fixed 3-option catalog regardless of SolutionStackName/PlatformArn/EnvironmentName; real AWS returns hundreds of platform-specific options with real default values. Large scope (needs a per-platform option catalog) -- deferred."
  - "DescribeConfigurationSettings omits optional DateCreated/DateUpdated/DeploymentStatus/PlatformArn fields on ConfigurationSettingsDescription. Low traffic; deferred."
  - "CreateApplication behavior on a duplicate ApplicationName (idempotent-return-existing vs InvalidParameterValue error) could not be confirmed with high confidence from AWS docs (only TooManyApplications is a documented error; the ApplicationName parameter doesn't state duplicate-name behavior explicitly, unlike VersionLabel/EnvironmentName which do). Left unchanged (still errors) to avoid an unverified behavior change; worth confirming against real AWS before altering."
  - "CreateApplication real AWS auto-creates a 'Default' ConfigurationTemplate and returns an (empty) Versions list on the ApplicationDescription (confirmed via the official CreateApplication API doc example response); gopherstack does neither. Deferred: touches quota/cascade-delete logic and is lower-traffic than the fixes made this pass."
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - DescribeConfigurationOptions full per-platform option catalog
  - CreateApplication idempotency-on-duplicate-name confirmation
  - CreateApplication auto-provisioned "Default" ConfigurationTemplate + Versions field
leaks: {status: clean, note: "no goroutines/janitors in this service; store.Table/Index-backed maps, coarse lockmetrics.RWMutex per backend -- consistent with pkgs-catalog.md guidance. No new leak surface introduced."}
---

## Notes

Freeform: AWS-behavior specifics worth remembering (exact algorithms, wire quirks,
error-message text, protocol = query-XML / REST-XML / REST-JSON / json-1.0), and any
"looks-wrong-but-correct" traps so the next auditor doesn't re-flag them.

- Protocol: query/XML (`awsAwsquery`), single POST with `Action=` + `Version=2010-12-01`
  form params. Response envelopes are `<XxxResponse><XxxResult>...</XxxResult>
  <ResponseMetadata>...</ResponseMetadata></XxxResponse>` -- verified against the real
  SDK's `deserializers.go` `awsAwsquery_deserializeOpXxx` (outer `GetElement("XxxResult")`)
  and `awsAwsquery_deserializeOpDocumentXxxOutput` (inner document) pairs for
  CreateApplication, CreateApplicationVersion, DescribeApplications,
  DescribeConfigurationSettings, DescribeEnvironments, DescribeEvents,
  ListTagsForResource, TerminateEnvironment, UpdateEnvironment,
  ValidateConfigurationSettings-shaped code -- all match; no missing/extra XML-nesting
  level found (the rds/neptune/docdb over-nesting bug class does NOT reproduce here).

- List wrapper convention confirmed against the real deserializers: `Applications>member`,
  `Environments>member`, `ApplicationVersions>member`, `ConfigurationSettings>member`,
  `OptionSettings>member`, `Events>member`, `ResourceTags>member`,
  `ConfigurationTemplates>member` all use the AWS query-protocol `member` wrapper
  correctly (not `Item` -- this service is NOT REST-XML).

- RouteMatcher requires `Version=2010-12-01` AND `Action` in `GetSupportedOperations()`
  specifically because SES also uses `Version=2010-12-01` and SNS/CloudWatch share action
  names like `ListTagsForResource` under different versions -- this disambiguation is
  correct and load-bearing; do not simplify to Action-only matching.

- `CloneEnvironment` (see gaps above) is NOT part of the real
  `aws-sdk-go-v2/service/elasticbeanstalk` API surface at all (verified: no
  `api_op_CloneEnvironment.go`, no deserializer). `ValidateConfigurationSettings` IS a
  real op (`api_op_ValidateConfigurationSettings.go` + deserializer exist) -- don't
  confuse the two. `ValidateConfigurationSettings`'s implementation here (namespace-only
  validation against a small `knownNamespaces` allowlist) is a reasonable, if partial,
  emulation of real validation and was not flagged as a gap.

- `ErrNotFound` (elasticbeanstalk-local sentinel) intentionally maps to the generic
  `InvalidParameterValue` for all name-based lookups (missing Application/Environment/
  ApplicationVersion/ConfigurationTemplate by name) -- this matches AWS's documented
  behavior for those ops (e.g. CreateApplicationVersion: "If no application is found
  with this name ... returns an InvalidParameterValue error"). Only the ARN-based
  ListTagsForResource/UpdateTagsForResource lookups use the newer, distinct
  `ResourceNotFoundException` (see new `ErrResourceNotFound` sentinel) -- don't collapse
  these two error paths back together in a future refactor.

- `CreateEnvironment` sets `Status: Ready, Health: Green` synchronously at creation time
  (no `Launching` intermediate state) -- this is a deliberate, correct choice that avoids
  the classic "client polls DescribeEnvironments forever" disguised no-op bug class; do
  not "fix" this to a fake `Launching` state without also implementing a background
  transition, or it reintroduces the bug.

- `TerminateEnvironment` deletes the environment from the store immediately after
  capturing a `Status: Terminated` snapshot for the response. This matches AWS's default
  `DescribeEnvironments` behavior (default `IncludeDeleted=false` excludes terminated
  environments), but `IncludeDeleted=true`/`IncludedDeletedBackTo` are not implemented --
  a client explicitly asking to see recently-terminated environments will get nothing.
  Not fixed this pass (low traffic); flagged here so it isn't rediscovered from scratch.
