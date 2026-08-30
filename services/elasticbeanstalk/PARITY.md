---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: elasticbeanstalk
sdk_module: aws-sdk-go-v2/service/elasticbeanstalk@v1.37.4   # version audited against
last_audit_commit:                                # unknown: pass ran without git access at write time, never backfilled -- gopherstack-33in;
  # method (deserializer/serializer key-switch extraction, gopherstack-6flj wrapper-key sweep)
  # is narrower/deeper than the prior Go-struct-level audit below, per the mediatailor/codedeploy
  # sessions' precedent for the same situation
last_audit_date: 2026-07-23
overall: A            # A = genuine fixes found; B = already-accurate, proven op-by-op
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateApplication: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "now auto-provisions a 'Default' ConfigurationTemplate (real AWS: 'Creates an application that has one configuration template named default') and the ApplicationDescription.Versions field is now populated (was always omitted)"}
  DescribeApplications: {wire: fixed, errors: ok, state: ok, persist: ok, note: "applicationDescType now includes Versions in addition to the earlier ResourceLifecycleConfig fix"}
  UpdateApplication: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "response now includes ConfigurationTemplates/Versions like Create/DescribeApplications (previously always rendered empty)"}
  DeleteApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascade-delete now also removes the auto-created Default ConfigurationTemplate -- verified no ghost row survives (TestHandler_DeleteApplication_CascadesDefaultTemplate)"}
  UpdateApplicationResourceLifecycle: {wire: ok, errors: ok, state: ok, persist: ok, note: "stored value now reachable via Describe/Create/UpdateApplication, see applicationDescType fix"}
  CreateApplicationVersion: {wire: ok, errors: fixed, state: fixed, persist: ok, note: "was not validating parent Application exists when AutoCreateApplication=false (AWS-documented InvalidParameterValue case); now validated. Auto-created Application now gets DateCreated/DateUpdated AND the same auto-provisioned Default ConfigurationTemplate as CreateApplication (same underlying app-creation transition)"}
  DescribeApplicationVersions: {wire: fixed, errors: ok, state: ok, persist: ok, note: "gopherstack-6flj: MaxRecords/NextToken (real request/response members) were parsed nowhere -- every call returned the full unpaginated list regardless of MaxRecords, and NextToken was never emitted. Now paginated via pkgs/page. appVersionDescType.BuildArn (real ApplicationVersionDescription member, CodeBuild-deployed versions only) remains unmodeled -- see gaps."}
  UpdateApplicationVersion: {wire: ok, errors: ok, state: fixed, persist: ok, note: "was not bumping DateUpdated; fixed"}
  DeleteApplicationVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateEnvironment: {wire: fixed, errors: ok, state: ok, persist: ok, note: "environment created Ready/Green immediately -- no stuck-Launching disguised no-op. gopherstack-6flj: environmentDescType (shared by Create/Describe/Update/Terminate/ComposeEnvironments) never emitted TemplateName (backend tracked it; wire member dropped), HealthStatus (real EnvironmentHealthStatus enum, e.g. 'Ok' -- distinct from the Health color enum), or AbortableOperationInProgress (real *bool member; omitting it entirely decodes as a nil pointer on a typed client that dereferences it, versus a real client's always-populated true/false). All three fixed; HealthStatus is always envHealthStatusOk ('Ok') and AbortableOperationInProgress always false, matching this backend's synchronous-update invariant. Real EnvironmentDescription.Resources/EnvironmentLinks remain unmodeled -- see gaps."}
  DescribeEnvironments: {wire: fixed, errors: ok, state: ok, persist: ok, note: "gopherstack-6flj: VersionLabel (real DescribeEnvironmentsInput filter) was parsed nowhere -- every call returned every version's environments. MaxRecords/NextToken were likewise discarded (no pagination, NextToken never emitted). Both fixed (VersionLabel filter applied in-handler; pagination via pkgs/page). IncludeDeleted/IncludedDeletedBackTo remain unmodeled -- see gaps. Plus environmentDescType's fixes, see CreateEnvironment."}
  UpdateEnvironment: {wire: fixed, errors: ok, state: ok, persist: ok, note: "already bumped DateUpdated correctly. Plus environmentDescType's fixes, see CreateEnvironment."}
  TerminateEnvironment: {wire: fixed, errors: ok, state: ok, persist: ok, note: "Plus environmentDescType's fixes, see CreateEnvironment."}
  ComposeEnvironments: {wire: fixed, errors: ok, state: ok, persist: ok, note: "Plus environmentDescType's fixes, see CreateEnvironment."}
  CreateConfigurationTemplate: {wire: partial, errors: fixed, state: fixed, persist: ok, note: "OptionSettings and PlatformArn request parameters were parsed nowhere and silently dropped (a config template's OptionSettings could never be set at creation, nor read back via DescribeConfigurationSettings) -- now stored via ConfigurationTemplateParams. Response shape was a bespoke 4-field mini-type; real CreateConfigurationTemplateOutput is the FULL ConfigurationSettingsDescription shape (same as DescribeConfigurationSettings/UpdateConfigurationTemplateOutput) -- now unified via configurationSettingsDescType/toConfigurationSettingsDesc, adding OptionSettings/DateCreated/DateUpdated/PlatformArn to the response. Added the AWS-documented SolutionStackName/PlatformArn mutual-exclusivity validation (InvalidParameterValue). STILL PARTIAL: EnvironmentId/SourceConfiguration (alternate ways to seed a template) are accepted as form fields but silently ignored -- see gaps below, not reclassified to ok"}
  UpdateConfigurationTemplate: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "was not bumping DateUpdated (fixed prior pass); OptionSettings/OptionsToRemove request parameters were parsed nowhere and silently dropped -- now applied via UpdateConfigurationTemplateWithParams/updateOptionSettings (same merge helper UpdateEnvironment already used). Response shape unified to the full ConfigurationSettingsDescription shape, same as CreateConfigurationTemplate above"}
  DeleteConfigurationTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeConfigurationSettings: {wire: fixed, errors: ok, state: ok, persist: ok, note: "ConfigurationSettingsDescription now includes DateCreated/DateUpdated/DeploymentStatus/PlatformArn (previously omitted entirely). DeploymentStatus is 'deployed' for a live environment's settings (this backend applies environment updates synchronously, so there is never a pending/failed draft) and omitted (AWS: null) for a template, which is never associated with a running environment"}
  DescribeConfigurationOptions: {wire: partial, errors: ok, state: n/a, persist: n/a, note: "was a hardcoded 3-option catalog ignoring every request parameter. Now a curated ~48-option catalog across the 16 namespaces this service already recognizes (see knownNamespaces), with real DefaultValue/ChangeSeverity/ValueType/ValueOptions/MinValue fields, genuine filtering via the request's Options parameter (previously unused), and SolutionStackName/PlatformArn now resolved+echoed on the response (previously absent from the response shape entirely). STILL PARTIAL: real AWS varies the option set per solution stack/platform version and returns hundreds of options; this backend applies the same fixed catalog regardless of platform -- see gaps below, not reclassified to ok"}
  ValidateConfigurationSettings: {wire: ok, errors: fixed, state: ok, persist: n/a, note: "real AWS op (api_op_ValidateConfigurationSettings.go + deserializer exist in the SDK); implementation validates option-setting namespaces against a fixed allowlist -- a reasonable partial emulation of real server-side validation, not a stub. FIXED this pass (gopherstack-uhsb): ApplicationName is a required input (api_op_ValidateConfigurationSettings.go: 'the application that the configuration template or environment belongs to') but was parsed nowhere -- any value, including none at all, had zero effect. Now validated for presence and existence, InvalidParameterValue on either failure, same no-application-found precedent CreateApplicationVersion's AutoCreateApplication=false path already uses."}
  DescribeEnvironmentResources: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeEvents: {wire: fixed, errors: ok, state: ok, persist: ok, note: "Severity/StartTime/EnvironmentId filters implemented (fixed in earlier sweep, #2165). gopherstack-6flj: eventDescType never emitted PlatformArn/TemplateName/VersionLabel (real EventDescription members; EventRecord never even captured them at append time) -- fixed, captured on the environment at the moment of the triggering action. EndTime filter was likewise parsed nowhere; fixed (symmetric with the existing StartTime filter). MaxRecords/NextToken pagination added via pkgs/page (events are already returned newest-first, deterministic). RequestId remains unmodeled -- see gaps (this handler has no per-call unique request-ID generation anywhere, not specific to events)."}
  ListTagsForResource: {wire: ok, errors: fixed, state: fixed, persist: ok, note: "now reaches ConfigurationTemplate and PlatformVersion tags (previously only Application/Environment/ApplicationVersion); not-found ARN now returns ResourceNotFoundException instead of InvalidParameterValue"}
  UpdateTagsForResource: {wire: ok, errors: fixed, state: fixed, persist: ok, note: "same ConfigurationTemplate/PlatformVersion + error-code fixes as ListTagsForResource"}
  CreatePlatformVersion: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "PlatformArn was built with an empty account ID (arn:aws:elasticbeanstalk:region::platform/...), producing a malformed ARN for what is an account-owned custom-platform resource; fixed to use the caller's account ID. gopherstack-uhsb: PlatformDefinitionBundle (S3Location, This member is required) was parsed nowhere and silently dropped -- now validated for presence (S3Bucket/S3Key both non-empty, InvalidParameterValue otherwise), matching every other required-field check this handler already runs. STILL A DELIBERATE STRUCTURAL GAP, not fixed further: real AWS fetches the S3 object, validates it exists, and builds the platform's Docker image from its contents (types.Builder/PlatformSummary/PlatformDescription -- verified none of the three response types has an S3Bucket/S3Key field at all, so there is nowhere on the wire to even round-trip a stored value); this backend has no S3 cross-service wiring for elasticbeanstalk (unlike CreateApplicationVersion's SourceBundle, which is stored-but-unvalidated against the real s3 service) and no Docker-build pipeline, so verifying the object exists or building anything from its contents is out of scope, not something to fake. gopherstack-6flj: response reused ONE shared struct for two genuinely different real shapes -- CreatePlatformVersionOutput/DeletePlatformVersionOutput use types.PlatformSummary (which has NO PlatformName member at all), DescribePlatformVersionOutput uses the larger types.PlatformDescription (which does) -- so this response was FABRICATING a PlatformName field real AWS never sends (over-emission, non-observable to a typed client since PlatformSummary simply has no field to bind it to, but a raw-body diff would show it). Split into platformSummaryDescType/platformDescriptionDescType; also added PlatformOwner ('self', real member on both shapes, derivable since every platform this backend creates is a customer-owned custom platform) which neither response emitted before."}
  DeletePlatformVersion: {wire: fixed, errors: ok, state: ok, persist: ok, note: "gopherstack-6flj: same PlatformSummary-shape fix and PlatformOwner addition as CreatePlatformVersion, see that entry."}
  DescribePlatformVersion: {wire: fixed, errors: ok, state: ok, persist: ok, note: "gopherstack-6flj: uses the real, larger PlatformDescription shape now (platformDescriptionDescType) with PlatformOwner added. STILL PARTIAL: CustomAmiList/DateCreated/DateUpdated/Description/Frameworks/Maintainer/OperatingSystemName/OperatingSystemVersion/PlatformBranchLifecycleState/PlatformBranchName/PlatformCategory/PlatformLifecycleState/ProgrammingLanguages/SolutionStackName/SupportedAddonList/SupportedTierList remain unmodeled -- see gaps (no S3 platform-definition-bundle parsing anywhere in this backend, same root cause as CreatePlatformVersion's structural gap above)."}
  ListPlatformVersions: {wire: fixed, errors: ok, state: ok, persist: ok, note: "gopherstack-6flj: item type (platformSummary) only emitted PlatformArn+PlatformStatus; real types.PlatformSummary's PlatformVersion member was never emitted despite the backend tracking it (fixed), and PlatformOwner was added (see CreatePlatformVersion). Filters (real ListPlatformVersionsInput.Filters, PlatformFilter.Type/Values) and MaxRecords/NextToken pagination were both parsed nowhere -- both fixed (Filters matches Type against PlatformName/PlatformVersion/PlatformStatus/PlatformArn by equality only, matching handleListPlatformBranches's existing Operator-agnostic precedent; non-equality Operators and OperatingSystemName/SupportedTier/SupportedAddon/ProgrammingLanguageName/PlatformBranchName/PlatformLifecycleState filter Types are not honored -- disclosed, not modeled, since this backend tracks none of that data). FIXED 2026-08-30 (gopherstack-6flj wrapper-key sweep, workspaces/codebuild/elasticbeanstalk pass): Filters.Values is a real list (types.PlatformFilter.Values, the standard AWS SearchFilter/PlatformFilter OR-list idiom) but only Values.member.1 was ever read -- a caller filtering on multiple candidate values silently lost every value past the first. Now OR-matches against every listed value."}
  ListPlatformBranches: {wire: fixed, errors: ok, state: ok, persist: n/a, note: "static catalog with Filters.member support; acceptable emulation of a largely-static AWS list. gopherstack-6flj: MaxRecords/NextToken pagination added via pkgs/page (previously discarded, always returned the full list). BranchOrder/SupportedTierList (real PlatformBranchSummary members) remain unmodeled -- see gaps. FIXED 2026-08-30 (gopherstack-6flj wrapper-key sweep): same Values.member-truncated-to-first-value bug as ListPlatformVersions (both share the identical Filters.member.N.Values.member.M wire shape) -- fixed identically."}
  ListAvailableSolutionStacks: {wire: ok, errors: ok, state: ok, persist: n/a, note: "static catalog; acceptable"}
  DescribeAccountAttributes: {wire: ok, errors: ok, state: ok, persist: n/a}
  DescribeEnvironmentHealth: {wire: fixed, errors: ok, state: ok, persist: n/a, note: "gopherstack-6flj: HealthStatus was populated from this backend's internal color label (envHealthGreen, 'Green') -- 'Green' is not a member of the real EnvironmentHealthStatus enum at all (that's the separate EnvironmentHealth/Color enum); fixed to always emit 'Ok' (envHealthStatusOk), matching this backend's invariant Green/Ready state. EnvironmentId (real input, alternate to EnvironmentName) was also parsed nowhere -- fixed."}
  DescribeInstancesHealth: {wire: fixed, errors: ok, state: n/a, persist: n/a, note: "always returns empty list -- correct since the backend never models EC2 instances; not a disguised stub. gopherstack-6flj: RefreshedAt (real *time.Time member) was never emitted at all -- omitting it decodes as a nil pointer on a typed client, unlike the always-empty (but non-nil) InstanceHealthList a real client already expects to handle as zero-length. Fixed using the same placeholder DescribeEnvironmentHealth uses."}
  DescribeEnvironmentManagedActions: {wire: ok, errors: ok, state: n/a, persist: n/a, note: "always empty -- correct, backend never schedules future actions"}
  DescribeEnvironmentManagedActionHistory: {wire: fixed, errors: ok, state: ok, persist: ok, note: "gopherstack-6flj: EnvironmentId (real input, alternate to EnvironmentName) was parsed nowhere -- fixed. ExecutedTime (real ManagedActionHistoryItem member) was never emitted -- fixed as equal to FinishedTime (this backend applies managed actions synchronously, so there is no observable gap between start and finish). MaxItems/NextToken pagination added via pkgs/page. FailureDescription/FailureType remain unmodeled -- see gaps (Status is always 'Succeeded', no failure path exists to describe)."}
  ApplyEnvironmentManagedAction: {wire: ok, errors: ok, state: ok, persist: ok}
  AbortEnvironmentUpdate: {wire: ok, errors: ok, state: n/a, persist: n/a, note: "no-op is correct: updates complete synchronously in this backend, so there is never anything in-flight to abort"}
  RebuildEnvironment: {wire: ok, errors: ok, state: n/a, persist: n/a, note: "no-op is correct for a backend with no real infra to rebuild"}
  RestartAppServer: {wire: ok, errors: ok, state: n/a, persist: n/a, note: "no-op is correct, same reasoning"}
  RequestEnvironmentInfo: {wire: ok, errors: fixed, state: n/a, persist: n/a, note: "FIXED this pass (gopherstack-uhsb): InfoType (required; types.EnvironmentInfoType enum tail/bundle/analyze) and EnvironmentName/EnvironmentId were both parsed nowhere -- a request naming a nonexistent environment, or omitting InfoType/the environment entirely, previously got a silent 200. Now validated: InfoType must be one of tail/bundle/analyze, and the named environment must exist (real AWS: 'If no such environment is found, RequestEnvironmentInfo returns an InvalidParameterValue error'), reusing the same DescribeEnvironmentResources resolution pattern (factored into resolveSingleEnvironment). STILL a deliberate structural gap beyond that: real AWS compiles/zips the environment's live EC2 instance log files (tail/bundle) or forwards them to Amazon Bedrock (analyze) -- this backend never models EC2 instances at all (see DescribeInstancesHealth's always-empty list, same reasoning), so there is no genuine log content this no-op could produce; faking log lines would be worse than a documented no-op."}
  RetrieveEnvironmentInfo: {wire: ok, errors: fixed, state: ok, persist: n/a, note: "always empty EnvironmentInfo list -- correct, no log-tailing state is modeled. FIXED this pass (gopherstack-uhsb): same InfoType + environment-existence validation as RequestEnvironmentInfo (previously neither was checked); the empty-list structural gap itself is unchanged and remains correct, not a stub -- see RequestEnvironmentInfo's note for why."}
  CheckDNSAvailability: {wire: ok, errors: ok, state: ok, persist: n/a}
  CreateStorageLocation: {wire: ok, errors: ok, state: ok, persist: n/a}
  SwapEnvironmentCNAMEs: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateEnvironmentOperationsRole: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateEnvironmentOperationsRole: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteEnvironmentConfiguration: {wire: ok, errors: ok, state: n/a, persist: n/a, note: "gopherstack-6flj/21my re-audit (2026-08-28): op was routed and implemented but had NO manifest entry at all -- caught by the routing-table-vs-PARITY.md diff. Verified against elasticbeanstalk@v1.37.4's api_op_DeleteEnvironmentConfiguration.go: real DeleteEnvironmentConfigurationOutput has zero data members, and the handler correctly emits an empty <DeleteEnvironmentConfigurationResponse><ResponseMetadata>...</ResponseMetadata></DeleteEnvironmentConfigurationResponse> with nothing fabricated. Backend method is a documented no-op (no draft-configuration state exists to delete, since this backend applies environment updates synchronously -- same root cause as DeploymentStatus never being 'pending'/'failed'); state: n/a is correct, not a disguised stub."}
families:
  ARN construction: {status: fixed, note: "Application/Environment/ApplicationVersion/ConfigurationTemplate ARN patterns verified against https://docs.aws.amazon.com/elasticbeanstalk/latest/dg/AWSHowTo.iam.policies.arn.html (application/{app}, applicationversion/{app}/{ver}, configurationtemplate/{app}/{tmpl}, environment/{app}/{env}, platform/{name}/{version}) -- all correct except CreatePlatformVersion's missing account ID (fixed)"}
  error-code mapping: {status: fixed, note: "handleOpError previously mapped every ErrNotFound to InvalidParameterValue uniformly; ListTagsForResource/UpdateTagsForResource ARN-not-found now maps to the AWS-documented ResourceNotFoundException via new ErrResourceNotFound sentinel"}
  tag reachability: {status: fixed, note: "ConfigurationTemplate and PlatformVersion accept Tags at creation but List/UpdateTagsForResource could never reach them (lookupTagsByARN/ensureTagsByARN only checked Application/Environment/ApplicationVersion) -- disguised-no-op bug class; fixed"}
  DateUpdated bumping: {status: fixed, note: "UpdateApplication, UpdateApplicationVersion, UpdateConfigurationTemplate mutated state but left DateUpdated unchanged; UpdateEnvironment was already correct. Fixed all three"}
  fabricated-op removal: {status: fixed, note: "CloneEnvironment was routed/implemented in this service (handler.go dispatch table, handler_environments.go, environments.go backend method) but is NOT a real AWS Elastic Beanstalk API operation -- confirmed absent from aws-sdk-go-v2/service/elasticbeanstalk@v1.34.0 entirely (no api_op_CloneEnvironment.go, no deserializer, no CloneEnvironmentInput/Output types). DELETED this pass per gopherstack-invented-surface policy: removed from the ops dispatch table, GetSupportedOperations(), the XML response type, the handler func, and the backend method. TestHandler_CloneEnvironment_NotSupported locks that the action now 400s as UnknownOperationException like any other unrecognized action. (ValidateConfigurationSettings, by contrast, IS a real op -- see ops table -- don't confuse the two.)"}
  CreateApplication Default template + Versions: {status: fixed, note: "real AWS's CreateApplication doc: 'Creates an application that has one configuration template named default' (confirmed via the official API doc example response, which renders it capitalized as 'Default' in <ConfigurationTemplates><member>Default</member></ConfigurationTemplates>, plus an empty <Versions/> element). CreateApplication and the AutoCreateApplication path in CreateApplicationVersion now both call createDefaultConfigurationTemplate; ApplicationDescription.Versions is now populated from DescribeApplicationVersions on Create/Describe/UpdateApplication. Cascade-delete already swept ConfigurationTemplates on DeleteApplication, so the Default template is not a new ghost-row risk (verified: TestHandler_DeleteApplication_CascadesDefaultTemplate)."}
  ConfigurationTemplate OptionSettings/PlatformArn round-trip: {status: fixed, note: "CreateConfigurationTemplate's OptionSettings and PlatformArn parameters, and UpdateConfigurationTemplate's OptionSettings/OptionsToRemove parameters, were parsed nowhere in the handler and silently dropped -- real request fields with no effect, a disguised-stub bug class (parity-principles.md #4). ConfigurationTemplate gained OptionSettings/PlatformArn fields; Create/UpdateConfigurationTemplateWithParams store them; DescribeConfigurationSettings's TemplateName branch (previously hardcoded to an empty OptionSettings list) now reads them back."}
  Create/UpdateConfigurationTemplate response shape: {status: fixed, note: "real CreateConfigurationTemplateOutput and UpdateConfigurationTemplateOutput are NOT a bespoke small type -- they are the exact same ConfigurationSettingsDescription shape DescribeConfigurationSettings returns (ApplicationName/TemplateName/Description/DateCreated/DateUpdated/DeploymentStatus/OptionSettings/PlatformArn/SolutionStackName; confirmed by reading api_op_CreateConfigurationTemplate.go/api_op_UpdateConfigurationTemplate.go in the SDK module). The previous 4-field configurationTemplateDescType silently dropped DateCreated/DateUpdated/OptionSettings/PlatformArn from both responses. Unified onto configurationSettingsDescType via toConfigurationSettingsDesc, shared with DescribeConfigurationSettings' template branch."}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "DescribeConfigurationOptions applies one fixed, curated ~48-option catalog across 16 namespaces regardless of the resolved SolutionStackName/PlatformArn; real AWS returns hundreds of platform-specific options with per-platform default values that vary by solution stack. This pass replaced the previous request-blind 3-option stub with a real, filterable, multi-field catalog (see ops table), which is a substantial improvement, but a genuine per-platform option catalog remains out of scope (large effort: would need a per-solution-stack option table). Not reclassified to ok."
  - "CreateConfigurationTemplate's EnvironmentId and SourceConfiguration parameters (real AWS: alternate ways to seed a template's OptionSettings/SolutionStackName from an existing environment or another template, documented as alternatives to explicitly specifying SolutionStackName/PlatformArn) are accepted as form fields but not read -- only explicit OptionSettings/SolutionStackName/PlatformArn are honored. Lower-traffic than the OptionSettings/PlatformArn fix made this pass; deferred."
  - "CreateApplication behavior on a duplicate ApplicationName (idempotent-return-existing vs InvalidParameterValue error) still could not be confirmed with high confidence. Re-checked this pass via the official CreateApplication API doc and AWS CLI reference: the only documented error is TooManyApplications; the ApplicationName parameter's own documentation does not state duplicate-name behavior explicitly (unlike CreateApplicationVersion's VersionLabel, which does document 'If an application version already exists ... returns an InvalidParameterValue error'). Left unchanged (still errors via ErrAlreadyExists) to avoid an unverified behavior change; worth confirming against real AWS before altering."
  - "(gopherstack-6flj) ApplicationVersionDescription.BuildArn (CodeBuild-deployed version's build ARN) is not modeled -- this backend has no CodeBuild integration anywhere; SourceBuildInformation is stored-but-unvalidated the same way, so there is no real ARN to source."
  - "(gopherstack-6flj) EnvironmentDescription.Resources (nested LoadBalancerDescription: Domain/Listeners/LoadBalancerName) and EnvironmentLinks are not modeled on environmentDescType -- DescribeEnvironmentResources already fabricates a name-only LoadBalancer entry for a *different*, wider response shape (EnvironmentResourceDescription), but extending that same name-only convention to every environmentDescType-returning op (Create/Describe/Update/Terminate/ComposeEnvironments) was judged too speculative to add without a real Domain/Listener data source; left disclosed rather than fabricated. No environment-group linking is modeled at all, so EnvironmentLinks is always genuinely empty."
  - "(gopherstack-6flj) ManagedActionHistoryItem.FailureDescription/FailureType are not modeled -- every managed action this backend applies synchronously succeeds (Status is always 'Succeeded'), so there is no failure state to describe."
  - "(gopherstack-6flj) DescribePlatformVersion's PlatformDescription is missing CustomAmiList/DateCreated/DateUpdated/Description/Frameworks/Maintainer/OperatingSystemName/OperatingSystemVersion/PlatformBranchLifecycleState/PlatformBranchName/PlatformCategory/PlatformLifecycleState/ProgrammingLanguages/SolutionStackName/SupportedAddonList/SupportedTierList -- same root cause as CreatePlatformVersion's existing disclosed gap (no S3 platform-definition-bundle parsing anywhere in this backend, so there is no real platform metadata beyond the four fields PlatformVersion (the domain model) tracks)."
  - "(gopherstack-6flj) PlatformBranchSummary.BranchOrder/SupportedTierList are not modeled -- allPlatformBranches is a static, unordered curated list with no tier-compatibility concept."
  - "(gopherstack-6flj) EventDescription.RequestId is not modeled -- this handler has no per-call unique request-ID generation anywhere at all (every op's ResponseMetadata.RequestID is a fixed literal like \"eb-create-app\"), not something specific to events to invent now."
  - "(gopherstack-6flj) DescribeEnvironmentHealth's AttributeNames request filter (restricts which of ApplicationMetrics/Causes/Color/HealthStatus/InstancesHealth/RefreshedAt/Status are populated) is not honored -- this backend always returns its small fixed field set regardless. ApplicationMetrics/Causes/InstancesHealth (real DescribeEnvironmentHealthOutput members) are not modeled at all -- no request-metrics or per-instance health data exists in this backend (same root cause as DescribeInstancesHealth's always-empty list)."
  - "(gopherstack-6flj) DescribeEnvironments' IncludeDeleted/IncludedDeletedBackTo filter is not modeled -- TerminateEnvironment removes the environment record outright (environmentDeleteKey), so there is no deleted-environment history to include."
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - DescribeConfigurationOptions full per-platform option catalog
  - CreateConfigurationTemplate EnvironmentId/SourceConfiguration-based option seeding
  - CreateApplication idempotency-on-duplicate-name confirmation
leaks: {status: clean, note: "no goroutines/janitors in this service; store.Table/Index-backed maps, coarse lockmetrics.RWMutex per backend -- consistent with pkgs-catalog.md guidance. createDefaultConfigurationTemplate is a private, non-locking helper always called with b.mu already held by its caller (CreateApplication/CreateApplicationVersionWithParams) -- verified no double-lock/deadlock. No new leak surface introduced this pass."}
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

- `CloneEnvironment` was DELETED this pass (see families: fabricated-op removal). It was
  never part of the real `aws-sdk-go-v2/service/elasticbeanstalk` API surface (verified: no
  `api_op_CloneEnvironment.go`, no deserializer, no Input/Output types) -- it was a
  gopherstack-invented action name that no real SDK client could ever construct. If a
  future audit is tempted to "restore" it because some caller depends on it, that caller is
  itself non-AWS-conformant and should be fixed instead. `ValidateConfigurationSettings` IS a
  real op (`api_op_ValidateConfigurationSettings.go` + deserializer exist) -- don't
  confuse the two. `ValidateConfigurationSettings`'s implementation here (namespace-only
  validation against a small `knownNamespaces` allowlist) is a reasonable, if partial,
  emulation of real validation and was not flagged as a gap.

- `CreateConfigurationTemplateOutput`/`UpdateConfigurationTemplateOutput` and
  `DescribeConfigurationSettingsResult`'s `ConfigurationSettings` list members all share
  ONE wire shape: `ConfigurationSettingsDescription` (confirmed by reading
  `api_op_CreateConfigurationTemplate.go`/`api_op_UpdateConfigurationTemplate.go`/
  `api_op_DescribeConfigurationSettings.go` in the SDK module -- all three declare the
  identical field list). gopherstack now models this as one Go type
  (`configurationSettingsDescType`) built by `toConfigurationSettingsDesc`
  (template) / `toEnvironmentConfigurationSettingsDesc` (environment) in
  `handler_configuration_templates.go` -- don't reintroduce a separate bespoke type for
  Create/UpdateConfigurationTemplate's response, that was the previous (fixed) bug.

- `DeploymentStatus` on `ConfigurationSettingsDescription` is real-AWS-documented as
  null when "this configuration is not associated with a running environment" -- always
  true for a `ConfigurationTemplate`, so `toConfigurationSettingsDesc` leaves it empty
  (omitted via `omitempty`). For a live environment's settings it is always `"deployed"`
  (never `"pending"`/`"failed"`) because this backend applies environment updates
  synchronously -- same reasoning as the existing `CreateEnvironment`
  synchronous-Ready/Green note below. Do not "fix" one without the other.

- `DescribeConfigurationOptions`'s static catalog (`configuration_options.go`,
  `configurationOptionsCatalog`) is intentionally curated, not exhaustive: every entry is a
  real, currently-documented Elastic Beanstalk option (namespace, name, default value,
  value type) but the catalog does NOT vary per requested `SolutionStackName`/`PlatformArn`
  the way real AWS's hundreds-of-options-per-platform response does. Filtering via the
  request's `Options` parameter IS real (see `filterConfigurationOptions`); only the
  platform-specificity of the catalog CONTENTS remains a known gap -- see gaps above.

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

**2026-08-22 (gopherstack-ifzn) -- RouteMatcher swallowed a body-read failure as a 404,
masking Handler()'s already-typed InternalFailure**: same shape as autoscaling's entry
(see that entry or gopherstack-3a8t for the full survey/rationale). `RouteMatcher` now
falls back to `service.MatchesUserAgentMarker(r.Header, "api/elasticbeanstalk")` (verified
against the pinned `elasticbeanstalk@v1.37.4/api_client.go:638` `AddSDKAgentKeyValue` call)
only on the `ReadBody` failure branch, leaving the existing `ebAPIVersion`/`Version`/`Action`
matching untouched. Migrated `ExtractOperation`/`ExtractResource`/`Handler()` off
`r.ParseForm()` onto `httputils.ReadBody`+`url.ParseQuery`, per the docdb/neptune precedent
(gopherstack-bahs) -- the pre-existing code was vulnerable to the same double-`ParseForm`-
call landmine (a second call silently sees a cached-empty, non-nil `r.PostForm` instead of
the real read error). Proof: `TestHandler_OversizedBodySurfacesInternalFailure` in
`handler_oversized_body_test.go` drives a real elasticbeanstalk SDK client through
`service.NewRegistry`/`service.NewServiceRouter`, confirmed failing pre-fix with
`UnknownError`; passes now with `InternalFailure`. `TestHandler_NormalSizedBodyStillRoutes`
is the regression guard. Gates: `go build`, `go vet`, `gofmt -l` (clean), `go test -race
./services/elasticbeanstalk/...` (pass), `golangci-lint run ./services/elasticbeanstalk/...`
(0 issues).

**2026-08-28 (gopherstack-6flj/21my re-audit)**: this service was tasked as "unswept," but
this manifest's own entries (dated 2026-07-23, extensively labeled gopherstack-6flj) show
deep per-field work already shipped on main (69bbb940a), well past a wrapper-key-only pass
-- e.g. `AbortableOperationInProgress`/`HealthStatus` enum fixes, and the
`PlatformSummary`/`PlatformDescription` shape split. Independently re-verified against
elasticbeanstalk@v1.37.4's own `awsAwsquery_deserializeDocument*` functions rather than
trusting the manifest: `EnvironmentDescription` (all 21 real fields, confirmed
Resources/EnvironmentLinks are the only omissions, both already disclosed gaps),
`PlatformSummary` (confirmed no `PlatformName` member, matching
`platformSummaryDescType`'s deliberate split from `platformDescriptionDescType`), and no
shared struct carries a stray `XMLName` that could shadow an enclosing field tag (the
route53/gopherstack-m1gl class) -- every `XMLName` in this service is a unique top-level
`<XxxResponse>` root, used once. Also diffed the handler.go routing table against this
manifest's `ops:` keys and found one real gap: `DeleteEnvironmentConfiguration` was routed
and implemented but had no manifest entry at all -- added above (wire verified correct: a
real, memberless `DeleteEnvironmentConfigurationOutput`). No wire-shape bugs found this
pass.

## 2026-08-29: constraint-parameter sweep (a filter/sort/page limit silently not honoured) -- audited, no new bug found

Campaign-wide hunt for the class distinct from wire-shape/error-path
sweeps: a request parameter that constrains the result set but isn't
correctly applied. This service already received a dedicated, thorough
pass for exactly this class (gopherstack-6flj, see the `ops:` entries
above -- `DescribeApplicationVersions`, `DescribeEnvironments`,
`DescribeEvents`, `ListPlatformVersions`, `ListPlatformBranches`,
`DescribeEnvironmentManagedActionHistory`, `DescribeInstancesHealth`,
`CreateConfigurationTemplate`/`UpdateConfigurationTemplate` were all fixed
there for missing filters or unpaginated MaxRecords/NextToken). Per this
campaign's rule to treat a PARITY.md claim as a lead and not proof,
independently re-read the handler code (not just the note) for a sample
before accepting it:

- `DescribeApplications.ApplicationNames` (`api_op_DescribeApplications.go`)
  -- confirmed plumbed end to end: `handler_applications.go`'s
  `parseMembers(vals, "ApplicationNames.member")` into
  `Backend.DescribeApplications`, which filters by exact name when
  non-empty (`applications.go`).
- `DescribeApplicationVersions.ApplicationName`/`VersionLabels` -- both
  applied together as an AND (`application_versions.go`: `appName != ""`
  short-circuits, then `slices.Contains(versionLabels, ...)`), not one
  silently overriding the other.
- `ListPlatformVersions.Filters` -- re-verified the claimed
  equality-only/Operator-agnostic behavior by reading
  `listPlatformVersionsFilterValue`/`handleListPlatformVersions`
  (`handler_platforms.go`) directly: matches the note exactly, including
  the "unknown filter Type matches everything" fallback, which is a
  documented judgment call (no unmodeled-attribute filter should silently
  exclude platforms this backend can't evaluate the filter against) not a
  silent bug.
- `DescribeEnvironmentManagedActions` -- confirmed the handler ignores
  `vals` entirely and always returns an empty list; confirmed structurally
  correct (not a disguised stub) by grepping for any pending/scheduled
  managed-action state anywhere in the backend -- none exists, so there is
  nothing a `Status` filter could ever exclude.
- `DescribeConfigurationOptions.Options`/`SolutionStackName`/`PlatformArn`
  -- confirmed `filterConfigurationOptions(filters)` is called with the
  parsed `Options.member` filters, not discarded.

No new constraint-parameter bug found this pass. Not exhaustively
re-diffed against the pinned SDK op-by-op (that already happened in
gopherstack-6flj); this pass was a spot-check of a sample of its claims
plus the two ops (`DescribeApplications`, `DescribeEnvironmentManagedActions`)
its notes don't explicitly call out, rather than a full re-audit.

Gates: no code changed this service this pass, so no new gate run was
needed beyond the spot-check reads above; the existing `go test -race
-count=1 ./services/elasticbeanstalk/...` suite was left untouched.

### 2026-08-30 gopherstack-6flj wrapper-key sweep (workspaces/codebuild/elasticbeanstalk pass)

Real bug found and fixed: `ListPlatformVersions`/`ListPlatformBranches` both
parsed their `Filters.member.N.Values` list via
`Filters.member.N.Values.member.1` only -- a real `Values` is a list
(`types.PlatformFilter.Values`/`types.SearchFilter.Values`, confirmed real
via `serializers.go`'s `awsAwsquery_serializeDocumentPlatformFilterValueList`/
`awsAwsquery_serializeDocumentSearchFilterValues`, both `.Array("member")`),
and the standard AWS SearchFilter idiom is an OR-match across every listed
value (same pattern as EC2's `Filter.N.Value.M`). A caller filtering on more
than one candidate value silently lost every value past the first. Fixed
both ops to OR-match against the full `Values.member.*` list via the
existing `parseMembers` helper plus `slices.ContainsFunc`.

Worth recording: the 2026-08-23-dated note above ("`ListPlatformVersions.Filters`
-- re-verified the claimed equality-only/Operator-agnostic behavior... matches
the note exactly") checked one dimension (single-value equality, Operator
being ignored) and correctly found it accurate, but never exercised a
multi-value `Values` list, so it did not catch this. A "spot-check confirms
the claim" pass and a "drive every real field shape, including cardinality"
pass are different levels of rigor even against the same function.

`DeleteReportGroup.DeleteReports` and this `Values`-truncation bug were the
only two real findings across the whole three-service batch (workspaces,
codebuild, elasticbeanstalk); see `services/codebuild/PARITY.md` for the
codebuild finding and the type-aware field-usage scan method used there.
Workspaces came back clean (0/90 request-struct fields unreferenced).
