---
service: appconfig
sdk_module: aws-sdk-go-v2/service/appconfig@v1.48.0    # version audited against (bumped from v1.43.11)
last_audit_commit: f86ef17b                            # HEAD when the pre-existing 45 ops were last audited
last_audit_date: 2026-07-25
overall: A-           # DOWNGRADED from A this pass. The pre-existing 45 ops are unchanged and still
                       # hold (deployment state machine + EventLog, extension versioning, GetConfiguration
                       # deployed-vs-latest-created bug, StartDeployment version validation,
                       # CreateHostedConfigurationVersion optimistic-concurrency check, extension-association
                       # cascade-delete leak). The downgrade is earned by the new experiment family added
                       # this pass: real state, real errors, real reference validation, real persistence --
                       # but two core wire behaviors (StartExperimentRun's ExposurePercentage default;
                       # DeleteExperimentDefinition's delete_type default) are DOCUMENTED ASSUMPTIONS, not
                       # verified against real AWS -- the SDK ships no default for either. See the
                       # "experiment family" gaps below.
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateApplication: {wire: ok, errors: ok, state: ok, persist: ok}
  GetApplication: {wire: ok, errors: ok, state: ok, persist: ok}
  ListApplications: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "Description omit-means-unchanged semantics verified against optional *string UpdateApplicationInput members."}
  DeleteApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — cascade delete now also removes ExtensionAssociations targeting the app/env/profile ARNs being deleted (previously left as ghost rows referencing deleted resources) and deployedConfigs tracking entries for the app."}
  CreateEnvironment: {wire: ok, errors: ok, state: ok, persist: ok}
  GetEnvironment: {wire: ok, errors: ok, state: ok, persist: ok}
  ListEnvironments: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateEnvironment: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteEnvironment: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — same ExtensionAssociation + deployedConfigs cascade-cleanup as DeleteApplication."}
  CreateConfigurationProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  GetConfigurationProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  ListConfigurationProfiles: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateConfigurationProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteConfigurationProfile: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — same ExtensionAssociation + deployedConfigs cascade-cleanup."}
  CreateHostedConfigurationVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — the previously-ignored optional 'Latest-Version-Number' request header (an optimistic-concurrency check: real CreateHostedConfigurationVersionInput.LatestVersionNumber must match the profile's current latest version or the SDK client expects a conflict) is now parsed and validated; a stale value now returns ConflictException instead of silently racing another writer. httpPayload response-body/header split (Application-Id/Configuration-Profile-Id/Content-Type/Description/VersionLabel/Version-Number headers, raw content body) verified against deserializers.go, matching the prior audit pass."}
  GetHostedConfigurationVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  ListHostedConfigurationVersions: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteHostedConfigurationVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDeploymentStrategy: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDeploymentStrategy: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDeploymentStrategies: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDeploymentStrategy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDeploymentStrategy: {wire: ok, errors: ok, state: ok, persist: ok, note: "misspelled /deployementstrategies/{Id} DELETE URI (real AWS typo, hard-coded in the SDK serializer) matched correctly."}
  StartDeployment: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (major) — two real bugs closed: (1) ConfigurationVersion was never validated against an actual HostedConfigurationVersion for AppConfig-hosted profiles (LocationUri=='hosted'); a real client got a 201 for a deployment referencing a version that never existed. Now resolved via resolveHostedConfigVersion (accepts version number OR label, matching real semantics) and rejected with ResourceNotFoundException when unresolvable — non-hosted profiles (SSM/S3/...) are intentionally NOT validated since this backend has no way to check the external source. (2) Deployments completed synchronously (State=COMPLETE immediately) regardless of the strategy's DeploymentDurationInMinutes/FinalBakeTimeInMinutes, so a real client's StartDeploymentOutput.State/PercentageComplete/EventLog/GrowthType/GrowthFactor/DeploymentDurationInMinutes/FinalBakeTimeInMinutes/VersionLabel/AppliedExtensions were either zero-valued or wrong. A zero-duration, zero-bake strategy (e.g. AppConfig.AllAtOnce) still completes synchronously (matches real AWS: no growth curve to run), but any other strategy now genuinely progresses DEPLOYING -> [BAKING] -> COMPLETE via a compressed-time background reconciler (see deployments.go's package doc comment for why real minute-scale durations are simulated on a millisecond timescale, mirroring the precedent already set by services/rds and services/acm). EventLog now records DEPLOYMENT_STARTED / PERCENTAGE_UPDATED / BAKE_TIME_STARTED / DEPLOYMENT_COMPLETED events, most-recent-first, matching real AWS ordering. AppliedExtensions is populated from real ExtensionAssociations targeting the app/env/profile ARNs at start time."}
  GetDeployment: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — GetDeploymentOutput's AppliedExtensions/ConfigurationName/ConfigurationLocationUri/DeploymentDurationInMinutes/EventLog/FinalBakeTimeInMinutes/GrowthFactor/GrowthType/VersionLabel fields were entirely absent from the Deployment struct (always zero-valued on a real client) — all now populated. KmsKeyArn/KmsKeyIdentifier remain unmodeled (no KMS integration anywhere in this backend, same acceptable-gap precedent as ConfigurationProfile.KmsKeyIdentifier)."}
  ListDeployments: {wire: ok, errors: ok, state: ok, persist: ok, note: "returns the same (superset) Deployment shape as GetDeployment rather than a separate DeploymentSummary DTO; extra fields are harmless (real deserializers ignore unknown JSON keys), matching the pre-existing CreatedAt/UpdatedAt precedent on Application/Environment/DeploymentStrategy."}
  StopDeployment: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (major) — real StopDeploymentInput.AllowRevert (bound to the 'Allow-Revert' request header, not a body/query field) was not modeled at all: any call, including on an already-COMPLETE deployment, was unconditionally accepted and force-set to ROLLED_BACK. Now: (1) AllowRevert is parsed from the real header; (2) a non-terminal deployment (BAKING/DEPLOYING/VALIDATING) stops to ROLLED_BACK as before; (3) a COMPLETE deployment can ONLY be stopped via AllowRevert=true, moving it to REVERTED and reverting deployedConfigs to the previous COMPLETE deployment's ConfigurationVersion for that environment/profile (or clearing it if there was none) — previously a COMPLETE deployment could be silently rolled back with no AllowRevert check at all, and GetConfiguration/CurrentDeployedConfiguration would still have served the (self-)deployed version. StopDeployment on a COMPLETE deployment without AllowRevert now correctly returns BadRequestException."}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateExtension: {wire: ok, errors: ok, state: ok, persist: ok, note: "creates version 1 of a versioned resource — see the family-wide versioning note under GetExtension."}
  GetExtension: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (major, closes prior gap) — extensions are versioned resources in real AWS AppConfig: GetExtensionInput's optional 'version_number' query param must resolve a SPECIFIC historical version, not always 'whatever is current'. This backend previously stored Extension as one mutable record overwritten in place by every UpdateExtension, so version_number was always ignored and prior versions were unrecoverable. The extensions table is now keyed by composite (extensionID, versionNumber); UpdateExtension inserts a new row instead of mutating, and GetExtension honors an explicit version_number or defaults to the highest version (matching 'If no version number was defined, AppConfig uses the highest version')."}
  ListExtensions: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — DELETED the gopherstack-invented 'extension_version_number' filter parameter: real ListExtensionsInput has no version filter at all (confirmed via api_op_ListExtensions.go), and a real SDK client can never send it. ListExtensions now summarizes one row per distinct extension ID at its latest version, matching real AWS (there is no ListExtensionVersions API)."}
  UpdateExtension: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — now creates a new, independently addressable version (VersionNumber = latest+1) rather than mutating the existing record in place, so a prior version remains gettable via GetExtension?version_number=N after an update, matching real AWS."}
  DeleteExtension: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (major, closes prior gap) — DeleteExtensionInput's optional 'version' query param now deletes ONLY that specific version (or the highest version, if omitted — matching 'If omitted, the highest version is deleted', NOT a full wipe of every version as the pre-fix single-record model implicitly did). Deleting an extension's last remaining version removes the extension (and its tags) entirely. Also FIXED: deleting a version still referenced by an ExtensionAssociation now returns ConflictException instead of silently succeeding and leaving the association pointing at a deleted extension version."}
  CreateExtensionAssociation: {wire: ok, errors: ok, state: ok, persist: ok, note: "explicit ExtensionVersionNumber is now validated to actually exist (returns ResourceNotFoundException if not); previously any integer was accepted uncritically."}
  GetExtensionAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  ListExtensionAssociations: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateExtensionAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteExtensionAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAccountSettings: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAccountSettings: {wire: ok, errors: ok, state: ok, persist: ok}
  GetConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (major) — real GetConfiguration ('Retrieves the latest DEPLOYED configuration', deprecated) was actually implemented as 'return the highest-numbered HostedConfigurationVersion ever created for this profile', completely ignoring environment/deployment state — a real client would see content that was uploaded via CreateHostedConfigurationVersion but never deployed to that environment, and creating a newer hosted version would change what GetConfiguration returned even with zero deployments. Now backed by a real deployedConfigs map updated only when a deployment reaches COMPLETE (see StartDeployment/StopDeployment notes), correctly returning empty content until an actual deployment has completed and the correct version thereafter. deployedConfigs is cascade-cleaned on DeleteApplication/DeleteEnvironment/DeleteConfigurationProfile and persisted (survives Snapshot/Restore)."}
  ValidateConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateExperimentDefinition: {wire: ok, errors: ok, state: ok, persist: ok, note: "field-diffed against CreateExperimentDefinitionInput/Output in api_op_CreateExperimentDefinition.go + types.Treatment(Input)/FlagValue/AttributeValue in types.go; POST /applications/{ApplicationIdentifier}/experimentdefinitions per serializers.go. ApplicationIdentifier/EnvironmentIdentifier/ConfigurationProfileIdentifier are resolved (ID or name) against real Application/Environment/ConfigurationProfile state via the pre-existing resolveAppID/resolveEnvID/resolveProfileID helpers (configuration.go) -- not accepted as any string. Additionally validates the referenced ConfigurationProfile.Type is AWS.AppConfig.FeatureFlags when Type was explicitly set (empty Type is treated as unspecified, not wrong, so pre-existing freeform-profile test fixtures are not retroactively broken). Inline Tags are applied correctly (see tags_handling in the campaign return receipt) -- this new op does NOT repeat the bd gopherstack-lcan inline-Tags-dropped bug the six pre-existing Create* handlers still have."}
  GetExperimentDefinition: {wire: ok, errors: ok, state: ok, persist: ok, note: "resolves by ID or name within the application, matching real AWS's 'ID or name' ExperimentDefinitionIdentifier contract."}
  ListExperimentDefinitions: {wire: partial, errors: ok, state: ok, persist: ok, note: "account-wide GET /experimentdefinitions (query filters application_identifier/configuration_profile_identifier/environment_identifier/status/max_results/next_token) verified against api_op_ListExperimentDefinitions.go's httpBindings. Returns the full ExperimentDefinition shape rather than a separate ExperimentDefinitionSummary DTO -- same harmless-superset precedent as ListDeployments (extra fields are ignored by real deserializers). PARTIAL: a configuration_profile_identifier/environment_identifier filter can only be resolved by NAME when application_identifier is also supplied (to establish which application's profiles/environments to search); without an application_identifier, a name-form filter value is compared literally against the ID field only and will not match. A real client is documented as able to pass any of the three identifiers independently, so this is a genuine (narrow) gap, not a fabricated shortcut -- see gaps below."}
  UpdateExperimentDefinition: {wire: ok, errors: ok, state: ok, persist: ok, note: "nil-means-unchanged semantics verified against optional *string/*Treatment/*[]Treatment UpdateExperimentDefinitionInput members (Name/ApplicationIdentifier/ExperimentDefinitionIdentifier are the only required members). Returns ConflictException when a RUNNING run exists for the definition, matching the real doc text 'You cannot update an experiment definition while an experiment run is active.'"}
  DeleteExperimentDefinition: {wire: ok, errors: ok, state: ok, persist: ok, note: "DeleteType query param ('delete_type', ARCHIVE|DESTROY per types/enums.go) verified against api_op_DeleteExperimentDefinition.go. ARCHIVE sets Status=ARCHIVED and preserves the definition/runs; DESTROY permanently removes the definition plus every run/event/tag scoped to it (cascade, no ghost rows). ASSUMPTION (unverified against real AWS, called out explicitly): when delete_type is omitted, this backend defaults to ARCHIVE -- the SDK documents no default for either identifier."}
  StartExperimentRun: {wire: ok, errors: ok, state: ok, persist: ok, note: "field-diffed against StartExperimentRunInput/Output in api_op_StartExperimentRun.go; POST /applications/{ApplicationIdentifier}/experimentdefinitions/{ExperimentDefinitionIdentifier}/experimentruns. Only one RUNNING run per definition is allowed (ConflictException otherwise); starting a run against an ARCHIVED definition is rejected (BadRequestException). Moves the parent ExperimentDefinition.Status to ACTIVE. Inline Tags applied correctly to the run's own ARN (same lcan-avoidance as CreateExperimentDefinition). ASSUMPTION (unverified, called out explicitly): ExposurePercentage defaults to 0 when omitted -- the SDK documents no default, only that 'Set to 0 to validate the experiment before exposing production users' is a valid use, which this backend read as the safer default absent a confirmed value."}
  GetExperimentRun: {wire: ok, errors: ok, state: ok, persist: ok}
  ListExperimentRuns: {wire: ok, errors: ok, state: ok, persist: ok, note: "returns the full ExperimentRun shape (superset of ExperimentRunSummary), same precedent as ListExperimentDefinitions/ListDeployments. Status query-param filter verified against api_op_ListExperimentRuns.go."}
  UpdateExperimentRun: {wire: ok, errors: ok, state: ok, persist: ok, note: "only permitted while the run is RUNNING (BadRequestException otherwise, real AWS doc: run must be active to update). ExposurePercentage can only increase, never decrease, matching 'This value can only be increased from the current setting' -- verified by rejecting any decrease with BadRequestException. TreatmentOverrides modeled as the real single-member union (types.TreatmentOverridesMemberInline, wire key 'Inline')."}
  StopExperimentRun: {wire: ok, errors: ok, state: ok, persist: ok, note: "PATCH .../experimentruns/{Run}/stop verified against api_op_StopExperimentRun.go. Only permitted while RUNNING (BadRequestException on an already-DONE run, matching real semantics -- no re-stop). Moves Status to DONE, sets EndedAt, reverts the parent ExperimentDefinition.Status to IDLE (no other run can be RUNNING -- StartExperimentRun enforces at most one). Optional Result (ExperimentRunResult) is stored and echoed back; see results_verdict in the campaign return receipt for why this backend never computes it itself."}
  ListExperimentRunEvents: {wire: ok, errors: ok, state: ok, persist: ok, note: "returns exactly the RUN_STARTED/EXPOSURE_UPDATED/OVERRIDES_UPDATED/RUN_STOPPED events this backend actually recorded during the run's lifecycle (experiment_runs.go's appendExperimentRunEventLocked, most-recent-first -- the same ordering convention as the pre-existing DeploymentEvent family), never a synthesized timeline. GET .../experimentruns/{Run}/events verified against api_op_ListExperimentRunEvents.go."}
families:
  route_matcher: {status: ok, note: "every op's REST path prefix + HTTP method verified against aws-sdk-go-v2/service/appconfig@v1.48.0's serializers.go SplitURI calls, including the 11 new experiment routes this pass (POST/GET/PATCH/DELETE under /applications/{id}/experimentdefinitions[/...] plus the account-wide GET /experimentdefinitions)."}
  persistence: {status: ok, note: "Handler.Snapshot/Restore delegate to InMemoryBackend; new deployedConfigs map added to the snapshot (nil-guarded on restore, matching every other raw map). deploymentTimers (in-flight deployment-progression state) is deliberately NOT persisted — see its doc comment in store.go and finalizeStaleDeploymentsLocked in deployments.go, which immediately completes any deployment restored in a non-terminal state rather than leaving it stuck forever with no timer to drive it. This pass additionally added experimentDefinitions/experimentRuns (store.Table-backed, byApp/byAppName/byDef indexes rebuilt on Restore) and the raw experimentRunEvents/experimentRunCounters maps (nil-guarded, same convention as versionCounters/deploymentCounters) -- verified round-trip by TestInMemoryBackend_SnapshotRestore_FullState in persistence_test.go, extended this pass to seed and assert an experiment definition + run + its event history survives Snapshot/Restore, that byAppName/experimentRunCounters were rebuilt (not left stale), and that DeleteApplication's cascade delete now removes experiment definitions too."}
  experiment_family: {status: partial, note: "11 new ops (CreateExperimentDefinition, GetExperimentDefinition, ListExperimentDefinitions, UpdateExperimentDefinition, DeleteExperimentDefinition, StartExperimentRun, GetExperimentRun, ListExperimentRuns, UpdateExperimentRun, StopExperimentRun, ListExperimentRunEvents) added this pass -- AppConfig's A/B-testing surface, shipped since the v1.43.11 audit. Real state machine: ExperimentDefinition.Status transitions IDLE -> ACTIVE (a run starts) -> IDLE (the run stops) or -> ARCHIVED (DeleteExperimentDefinition, delete_type=ARCHIVE); ExperimentRun.Status transitions RUNNING -> DONE (StopExperimentRun only; no automatic timer-driven progression, unlike Deployment's growth curve -- a run has no duration to progress through, it just stays RUNNING until stopped). ListExperimentRunEvents returns exactly the RUN_STARTED/EXPOSURE_UPDATED/OVERRIDES_UPDATED/RUN_STOPPED events this backend actually recorded, never a fabricated timeline. References (ApplicationIdentifier/EnvironmentIdentifier/ConfigurationProfileIdentifier) are validated against real backend state via the pre-existing resolveAppID/resolveEnvID/resolveProfileID helpers, plus a ConfigurationProfile.Type==AWS.AppConfig.FeatureFlags check. marked partial (not ok) for two reasons, both listed under gaps below: (1) two wire-relevant defaults (ExposurePercentage, delete_type) are documented assumptions, not verified against real AWS; (2) FlagKey is validated for presence only, never against actual feature-flag content, since this backend has no feature-flag-content model at all."}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "Every real Create*Input in this service (CreateApplicationInput, CreateEnvironmentInput, CreateConfigurationProfileInput, CreateDeploymentStrategyInput, CreateExtensionInput, CreateExtensionAssociationInput) has an optional inline Tags map[string]string member, applied at creation time as an alternative to a separate TagResource call. None of the six corresponding handlers in this backend parse or apply it — a real client that tags a resource inline at creation gets a 200/201 with the tags silently dropped (ListTagsForResource on the new resource returns empty). This predates this pass (found while field-diffing Create* wire shapes for the deployment/extension work, not introduced by it). NOT fixed this pass: doing so correctly requires threading a tags parameter through 6 backend method signatures + the StorageBackend interface + 6 handler request structs, which touches every existing call site of those methods across this package's test suite (dozens of call sites in ~15 files) — a larger mechanical change than fit alongside the deployment-state-machine/extension-versioning/GetConfiguration work in this pass. Tracked in bd gopherstack-lcan. The two NEW Create*-shaped ops this pass adds (CreateExperimentDefinition, StartExperimentRun) do NOT repeat this bug -- their inline Tags are applied correctly, since they were written fresh against this already-known gap rather than copy-pasted from the six broken handlers."
  - "Deployment progression (StartDeployment's DEPLOYING/BAKING growth curve) runs on a fixed compressed timescale (single-digit milliseconds per step, clamped GrowthFactor) rather than being proportional to the strategy's actual configured DeploymentDurationInMinutes/FinalBakeTimeInMinutes -- e.g. a 1-minute strategy and a 1440-minute strategy complete in comparable wall-clock time. This is a deliberate, documented simplification (see deployments.go's package doc comment) matching the precedent set by services/rds and services/acm for the same reason (real AWS timings are impractical to emulate literally in a test-driven in-memory backend); not something a client can observe via any single API call, only via wall-clock timing across polls."
  - "StartExperimentRun's ExposurePercentage default (when the optional field is omitted) is UNVERIFIED against real AWS -- the SDK's ExposurePercentage doc text ('Set to 0 to validate the experiment before exposing production users') implies 0 is a meaningful value but never states it is the default for an omitted field. This backend defaults to 0 (the safer, least-surprising reading: no audience exposed without an explicit non-zero value) rather than fabricate a different unverified number. A real client that always sends ExposurePercentage explicitly is unaffected; one that omits it may observe a different default than real AWS."
  - "DeleteExperimentDefinition's delete_type default (when omitted) is UNVERIFIED against real AWS -- DeleteType's doc text describes ARCHIVE as 'hide but preserve' and DESTROY as the explicit opt-in to permanent removal, but the SDK documents no default for an omitted value. This backend defaults to ARCHIVE (the non-destructive choice) rather than assume irreversible deletion was intended. A real client that always sends delete_type explicitly is unaffected."
  - "FlagKey (the feature flag an experiment evaluates) is validated for presence only (non-empty string) plus the referenced ConfigurationProfile.Type check -- it is never checked against actual feature-flag content, because this backend has no feature-flag-content model at all (ConfigurationProfile content, even for AWS.AppConfig.FeatureFlags-typed profiles, is opaque bytes -- see HostedConfigurationVersion.Content). A real client can create an experiment definition referencing a FlagKey that does not exist in the profile's actual flag JSON; this backend accepts it. Fixing this would require this backend to first model feature-flag content structure at all, which no existing AppConfig op depends on today -- out of scope for this pass."
  - "ListExperimentDefinitions's configuration_profile_identifier/environment_identifier filters can only be resolved by NAME when application_identifier is also supplied (see the note on that op above); without an application_identifier a name-form filter value is compared literally against the ID field only and silently matches nothing. A real client is documented as able to supply any of the three identifiers independently."
  - "Treatment.Key's server-generated naming scheme ('Control' for the control treatment, 'Treatment1'..'TreatmentN' 1-indexed by creation order for the rest) is an assumption: real CreateExperimentDefinitionInput/UpdateExperimentDefinitionInput's TreatmentInput has no client-supplied Key at all, so AWS itself must assign one, but the exact scheme AWS uses is not documented in the SDK. A real client that treats Key as an opaque server-assigned identifier (which is the only documented contract) is unaffected; one that asserts an exact Key string may see a different value than real AWS."
  - "DeploymentParameters (accepted on StartExperimentRun/StopExperimentRun/UpdateExperimentRun) is parsed but intentionally discarded rather than stored or acted upon -- real GetExperimentRun/StartExperimentRun/etc. output shapes never echo it back either, so a real client observes nothing different; but this backend also does not create the underlying 'real' deployment AWS uses internally to actually serve treatment variations to production traffic, so DynamicExtensionParameters/Tags on that inner deployment have no addressable resource here to apply to."
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - "GetExtensionInput/DeleteExtensionInput document 'name, ID, or ARN' identifier resolution; this backend's resolveExtensionID only resolves by ID or name (pre-existing, unchanged this pass) -- ARN-based lookup was not added. Low risk: gopherstack conventionally addresses resources by ID/name elsewhere in this service too."
leaks: {status: clean, note: "FIXED — DeleteApplication/DeleteEnvironment/DeleteConfigurationProfile previously left ExtensionAssociation rows referencing deleted app/env/profile ARNs as ghosts (unbounded growth under repeated create/delete cycles); all three now cascade-delete associations targeting the resource being removed, plus deployedConfigs tracking entries. The new deploymentTimers map (in-flight deployment progression) and its background reconciler goroutine are self-draining/self-terminating (same ephemeral-goroutine pattern as services/rds's lifecycle reconciler): TestDeploymentTimers_DrainToZero (leak_test.go) verifies the map returns to empty once every deployment reaches a terminal state, at which point the goroutine exits on its own -- no ctx-parenting or explicit Shutdown drain is needed since nothing outlives the deployments that scheduled it. leak_test.go's pre-existing NameIndexBounded tests (Application/Extension/DeploymentStrategy) still pass under -race. This pass additionally verified (TestBackend_DeleteApplication_CascadesExperimentDefinitions, TestBackend_DeleteExperimentDefinition_DestroyCascadesRunsAndTags) that DeleteApplication and DeleteExperimentDefinition(delete_type=DESTROY) both cascade-remove every experiment run/event/tag scoped to the definition being removed -- no ghost rows survive either deletion path."}
---

## Notes

Protocol: restjson1 (REST paths + JSON bodies), like the rest of the newer AWS services.
Two response operations are httpPayload-based rather than JSON-bodied: **CreateHostedConfigurationVersion**
and **GetHostedConfigurationVersion** both return the raw configuration content as the response body, with
every other field — including the version number — bound to a response header (`Application-Id`,
`Configuration-Profile-Id`, `Content-Type`, `Description`, `Versionlabel`, `Version-Number`). See
`setHostedConfigurationVersionHeaders`'s doc comment in handler_hosted_configuration_versions.go.

**Extensions are versioned resources.** Every `UpdateExtension` call in real AWS AppConfig produces a new,
independently addressable version rather than mutating the extension in place — `GetExtension`/
`DeleteExtension` both accept an optional version number (`version_number` / `version` query params
respectively) that must resolve a *specific* historical version, defaulting to the highest when omitted.
This backend's `extensions` table is keyed by the composite `(extensionID, versionNumber)` (see
`extensionVersionKey` in store.go) rather than by ID alone; `extensionsByID` groups every version of one
extension for latest-version lookup and cascade operations, while `extensionsByName` answers name-based
identifier resolution and the create-time name-uniqueness check. Any future extension-family change must
preserve this shape — collapsing back to "one mutable record per extension" reintroduces the exact bug this
pass closed.

**Deployment state machine.** `StartDeployment` now genuinely progresses `DEPLOYING` -> (`BAKING` if the
strategy's `FinalBakeTimeInMinutes` > 0) -> `COMPLETE` for any strategy with a non-zero
`DeploymentDurationInMinutes`, via a background reconciler goroutine (`scheduleDeploymentReconcilerLocked`
in deployments.go) that advances every in-flight deployment's `PercentageComplete` per its
`GrowthType`/`GrowthFactor` on a **compressed** timescale — see the package doc comment at the top of
deployments.go for why real minute-scale durations are simulated in milliseconds, and
`effectiveGrowthFactor`'s clamp for why worst-case test runtime stays bounded regardless of a strategy's
configured `GrowthFactor`. A zero-duration, zero-bake strategy (e.g. `AppConfig.AllAtOnce`) still completes
synchronously, matching real AWS (no growth curve to run). `StopDeployment` now honors the real
`AllowRevert` header: a non-terminal deployment stops to `ROLLED_BACK`; a `COMPLETE` deployment can *only*
be stopped via `AllowRevert=true`, moving to `REVERTED` and rolling `deployedConfigs` back to the previous
`COMPLETE` deployment's version. `EventLog` is recorded most-recent-first, matching real AWS ordering.
`deploymentTimers` (the in-flight progression bookkeeping) is intentionally not persisted — see
`finalizeStaleDeploymentsLocked`'s doc comment for what happens to a deployment restored mid-flight.

**`GetConfiguration` / `CurrentDeployedConfiguration` now track real deployment state**, not "the
highest-numbered hosted version ever created." A `deployedConfigs` map (keyed by
application/environment/profile) is updated only when a deployment reaches `COMPLETE`, and rolled back on a
`StopDeployment(..., allowRevert=true)` revert. `CurrentDeployedConfiguration` (configuration.go) is a
**public read accessor with no caller inside this package** — it exists for a future
`appconfig` -> `appconfigdata` bridge (see bd `gopherstack-uiyi`: appconfigdata's config store is never
populated by a real deployment today). `cli.go` wiring to actually call it on deployment completion is out
of scope for this change; adding the accessor itself is the in-scope half of closing that cross-service gap.

**Cascade-delete**: `DeleteApplication`/`DeleteEnvironment`/`DeleteConfigurationProfile` now also remove
`ExtensionAssociation` rows targeting the ARN being deleted (previously left as ghost rows pointing at a
resource that no longer exists — an unbounded-growth leak under repeated create/delete cycles in a
long-running process) and `deployedConfigs` tracking entries for the deleted app/env/profile.

`DeleteDeploymentStrategy` alone uses `/deployementstrategies/{Id}` (missing the second "n") — a genuine AWS
typo baked into the real SDK's serializer, not a gopherstack bug; the route matcher already special-cases
this correctly (unchanged this pass).

This backend adds `CreatedAt`/`UpdatedAt` fields to `Application`/`Environment`/`DeploymentStrategy` JSON
responses that don't exist in the real AWS shapes. Harmless — real deserializers ignore unknown JSON keys —
noted here so a future auditor doesn't mistake it for parity drift (unchanged this pass).

**Experiment family (new this pass, SDK bumped v1.43.11 -> v1.48.0).** AppConfig's A/B-testing surface: an
`ExperimentDefinition` (a treatment plan attached to a feature-flag `ConfigurationProfile`) can be run
zero or more times as an `ExperimentRun`. `ApplicationIdentifier`/`EnvironmentIdentifier`/
`ConfigurationProfileIdentifier` are all resolved (ID or name) against real backend state via the
pre-existing `resolveAppID`/`resolveEnvID`/`resolveProfileID` helpers (`configuration.go`) — an
experiment can never be created against an application/environment/profile that does not exist, and the
referenced profile must be `AWS.AppConfig.FeatureFlags`-typed when its `Type` was explicitly set.
`ExperimentDefinition.Status` transitions `IDLE` -> `ACTIVE` when a run starts, back to `IDLE` when it
stops (`StartExperimentRun` allows only one `RUNNING` run per definition at a time — a second concurrent
`StartExperimentRun` is rejected with `ConflictException`), or -> `ARCHIVED` via
`DeleteExperimentDefinition(delete_type=ARCHIVE)`; `delete_type=DESTROY` instead permanently removes the
definition plus every run/event/tag scoped to it (cascade, verified leak-free —
`TestBackend_DeleteExperimentDefinition_DestroyCascadesRunsAndTags`). Unlike `Deployment`'s
compressed-timescale growth curve, `ExperimentRun` has **no automatic progression**: a run has no
duration to advance through, it simply stays `RUNNING` — at the configured `ExposurePercentage` — until a
caller calls `StopExperimentRun` (-> `DONE`) or `UpdateExperimentRun` (exposure/overrides/description,
RUNNING-only, exposure can only increase). `ListExperimentRunEvents` returns exactly the
`RUN_STARTED`/`EXPOSURE_UPDATED`/`OVERRIDES_UPDATED`/`RUN_STOPPED` events this backend actually recorded
as those calls happened (`experiment_runs.go`'s `appendExperimentRunEventLocked`, most-recent-first —
same ordering convention as `DeploymentEvent`), never a fabricated or interpolated timeline.
`DeleteApplication` now also cascade-deletes every `ExperimentDefinition` (and its runs/events/tags)
scoped to the application being removed, extending the pre-existing
`ExtensionAssociation`/`deployedConfigs` cascade-cleanup precedent.

**Experiment results are never fabricated.** `ExperimentRunResult` (`ExecutiveSummary`/
`ReasonsToLaunch`/`ReasonsNotToLaunch`) is free-text a caller supplies to `StopExperimentRun`; this
backend has no analytics engine and computes no variant metrics, confidence intervals, or statistical
significance from exposure data (there is none to compute — this backend does not simulate real user
traffic against treatments), so it only ever stores and echoes back what a caller explicitly provides,
same rationale as `AppliedExtensions`/`ActionInvocations` being left empty elsewhere in this service
(no execution engine backing them either).

**Two experiment-family defaults are documented assumptions, not verified wire facts** — flagged
explicitly rather than silently guessed: `StartExperimentRun`'s `ExposurePercentage` defaults to `0` when
omitted (the SDK documents no default), and `DeleteExperimentDefinition`'s `delete_type` defaults to
`ARCHIVE` when omitted (same: no documented default). Both reasoning chains are in `gaps` above. A real
client that always sends these fields explicitly observes no difference from real AWS either way.

**`Treatment.Key`** (`"Control"` for the control treatment, `"Treatment1"`.."TreatmentN"` for the rest,
1-indexed by creation/update order) is server-generated: real `TreatmentInput` carries no client-supplied
key, so AWS must assign one, but the exact scheme is undocumented — this backend's choice is a documented
assumption (see `Treatment`'s doc comment in `models.go`), not a verified wire fact.
