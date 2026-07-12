---
service: appconfig
sdk_module: aws-sdk-go-v2/service/appconfig@v1.43.11   # version audited against
last_audit_commit: f86ef17b                            # HEAD when this manifest was written
last_audit_date: 2026-07-12
overall: A            # real fixes found: HostedConfigurationVersion wire shape + Update* partial-update semantics
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateApplication: {wire: ok, errors: ok, state: ok, persist: ok}
  GetApplication: {wire: ok, errors: ok, state: ok, persist: ok}
  ListApplications: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — Description was unconditionally overwritten with the JSON zero value when omitted from the request, silently clearing it on any partial update (e.g. rename-only). Real UpdateApplicationInput.Name/Description are optional *string members serialized only when present; converted the handler DTO and backend signature to *string so omitted means unchanged, matching AWS partial-update semantics."}
  DeleteApplication: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateEnvironment: {wire: ok, errors: ok, state: ok, persist: ok}
  GetEnvironment: {wire: ok, errors: ok, state: ok, persist: ok}
  ListEnvironments: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateEnvironment: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — same Description-clobber bug as UpdateApplication, plus Monitors was never accepted at all (client-supplied CloudWatch alarms were silently dropped on update even though CreateEnvironment persists them). Both are now optional (*string / *[]Monitor) with omit-means-unchanged semantics."}
  DeleteEnvironment: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateConfigurationProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  GetConfigurationProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  ListConfigurationProfiles: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateConfigurationProfile: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — same Description-clobber bug, plus RetrievalRoleArn and Validators were accepted by real UpdateConfigurationProfileInput but the handler/backend only ever threaded Name/Description through (disguised no-op: a client updating RetrievalRoleArn or Validators got a 200 with the change silently discarded). Added both fields with omit-means-unchanged semantics. KmsKeyIdentifier remains unmodeled (this backend has no KMS integration) — acceptable, not wire-breaking."}
  DeleteConfigurationProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateHostedConfigurationVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (major) — real CreateHostedConfigurationVersionOutput has no JSON body at all: the httpPayload is the raw configuration content (echoed back), and every other field (ApplicationId, ConfigurationProfileId, ContentType, Description, VersionLabel, VersionNumber) is bound to a response HEADER, not JSON. This handler returned c.JSON(v) (a JSON envelope with Content omitted via json:\"-\") and set a fabricated 'Appconfig-Configuration-Version' header instead of the real 'Version-Number' header used by aws-sdk-go-v2's deserializer. A real SDK client's CreateHostedConfigurationVersionOutput.VersionNumber/ApplicationId/ConfigurationProfileId/Description/VersionLabel all came back zero-valued, and .Content held the JSON envelope bytes instead of the uploaded content. Now returns c.Blob with the raw content and sets Application-Id / Configuration-Profile-Id / Content-Type / Description / VersionLabel / Version-Number headers (verified against deserializers.go's awsRestjson1_deserializeOpHttpBindingsCreateHostedConfigurationVersionOutput)."}
  GetHostedConfigurationVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — same header bug as Create: only Content-Type and the fabricated 'Appconfig-Configuration-Version' header were set. VersionNumber (real header 'Version-Number'), ApplicationId, ConfigurationProfileId, Description, VersionLabel all came back zero-valued on a real client. Fixed via the shared setHostedConfigurationVersionHeaders helper."}
  ListHostedConfigurationVersions: {wire: ok, errors: ok, state: ok, persist: ok, note: "version_label/max_results/next_token query param names verified against ListHostedConfigurationVersionsInput's http bindings."}
  DeleteHostedConfigurationVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDeploymentStrategy: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDeploymentStrategy: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDeploymentStrategies: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDeploymentStrategy: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — same Description-clobber bug (real UpdateDeploymentStrategyInput.Description is an optional *string). Name has no counterpart in the real Input at all (a real SDK client can never send it), so the pre-existing name!=\"\" branch is unreachable via a real client and was left as-is rather than removed, to avoid an unrelated behavior change."}
  DeleteDeploymentStrategy: {wire: ok, errors: ok, state: ok, persist: ok, note: "misspelled /deployementstrategies/{Id} DELETE URI (real AWS typo, hard-coded in the SDK serializer) is matched correctly alongside the correctly-spelled path for every other op."}
  StartDeployment: {wire: ok, errors: ok, state: ok, persist: ok, note: "deployment is applied synchronously (State=COMPLETE, PercentageComplete=100 immediately) rather than progressing through BAKING/DEPLOYING/VALIDATING like real AWS. Deliberate simplification (see code comment) that avoids the 'stuck deployment, client polls forever' failure mode explicitly called out as a bug class to watch for — kept as-is."}
  GetDeployment: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDeployments: {wire: ok, errors: ok, state: ok, persist: ok}
  StopDeployment: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateExtension: {wire: ok, errors: ok, state: ok, persist: ok}
  GetExtension: {wire: partial, errors: ok, state: partial, persist: ok, note: "real GetExtension takes an optional version_number query param (extensions are versioned resources in AWS; each Update creates an independently addressable version). This backend models Extension as a single mutable record with a VersionNumber counter, so Get always returns the current version regardless of the query param. See gaps."}
  ListExtensions: {wire: ok, errors: ok, state: ok, persist: ok, note: "accepts an extension_version_number filter the real ListExtensionsInput does not have (harmless dead code — a real client never sends it; left as-is, not a wire bug)."}
  UpdateExtension: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — same Description-clobber bug (real UpdateExtensionInput.Description is an optional *string)."}
  DeleteExtension: {wire: partial, errors: ok, state: partial, persist: ok, note: "real DeleteExtension takes an optional version query param to delete a single version; this backend has no per-version storage so it always deletes the whole (single, current) extension record. See gaps."}
  CreateExtensionAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  GetExtensionAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  ListExtensionAssociations: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateExtensionAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteExtensionAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAccountSettings: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAccountSettings: {wire: ok, errors: ok, state: ok, persist: ok}
  GetConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "deprecated API; Configuration-Version response header name already correct."}
  ValidateConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  route_matcher: {status: ok, note: "every op's REST path prefix + HTTP method (incl. the misspelled /deployementstrategies/{Id} DELETE, the PATCH-based Update* ops, and the /tags/{ResourceArn} greedy-decoded-ARN path) verified against aws-sdk-go-v2/service/appconfig@v1.43.11's serializers.go SplitURI calls. All routes are exercised end to end through Handler().(c) / RouteMatcher(), not called directly, in the existing test suite."}
  persistence: {status: ok, note: "Handler.Snapshot/Restore already delegate to InMemoryBackend (persistence.go), so cli.go's setupPersistence picks this service up correctly — no silent-unregistration bug found here."}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "GetExtension/DeleteExtension ignore the real API's optional version_number/version query params (extensions are versioned resources in AWS; this backend only keeps the current version). Fixing requires storing each UpdateExtension call as a new addressable version rather than mutating in place — a larger data-model change than warranted for this pass given Extensions are a lower-traffic family. File a bd issue if multi-version extension addressing is needed."
  - "StartDeployment does not validate that ConfigurationVersion refers to an existing HostedConfigurationVersion/label before creating the deployment; real AWS returns ResourceNotFoundException for an unknown version. Low risk (deployment succeeds with an unvalidated version string) but worth a follow-up bd issue."
  - "CreateHostedConfigurationVersion ignores the optional Latest-Version-Number request header (an optimistic-concurrency check some clients send); silently accepted rather than validated. Low-traffic corner case."
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - "Extension/ExtensionAssociation family only spot-checked (wire shapes for Create/Get/List/Delete verified accurate); the versioning gap above is the one open item."
leaks: {status: clean, note: "leak_test.go's NameIndexBounded tests (Application/Extension/DeploymentStrategy) pass under -race; no unbounded goroutines or maps found."}
---

## Notes

Protocol: restjson1 (REST paths + JSON bodies), like the rest of the newer AWS services.
Two response operations are httpPayload-based rather than JSON-bodied and were the source of
this sweep's main bug: **CreateHostedConfigurationVersion** and **GetHostedConfigurationVersion**
both return the raw configuration content as the response body, with every other field —
including the version number — bound to a response header (`Application-Id`,
`Configuration-Profile-Id`, `Content-Type`, `Description`, `Versionlabel`, `Version-Number`).
This is easy to get wrong because it *looks* like every other CRUD op (which returns a JSON
envelope of the resource) but isn't. Verify header names by grepping
`awsRestjson1_deserializeOpHttpBindingsGetHostedConfigurationVersionOutput` /
`...CreateHostedConfigurationVersionOutput` in `aws-sdk-go-v2/service/appconfig/deserializers.go`
rather than trusting the request-side header names (which happen to reuse `Description` /
`Versionlabel` for the *request*, but the *response* additionally needs `Version-Number`,
`Application-Id`, `Configuration-Profile-Id` — none of which exist as request headers).

**Partial-update (PATCH) semantics**: every `Update*` operation's optional fields
(`Description`, `Monitors`, `RetrievalRoleArn`, `Validators`, ...) are `*T` in the real
`aws-sdk-go-v2` input structs, and the JSON serializer omits the key entirely when the pointer
is nil (verified via `awsRestjson1_serializeOpDocumentUpdate*Input` in serializers.go). A real
client updating only one field (e.g. renaming an Application) never sends `Description` at all.
Binding these into a plain (non-pointer) Go `string`/slice field on the server side makes
"omitted" indistinguishable from "explicitly set to the zero value", so the handler's
`updated.Description = description` (unconditional) silently wiped the existing description on
every partial update — a real, observable state-corruption bug, not just a wire-format nit. Any
future `Update*` op added to this service must use `*T` request fields and an `if x != nil`
guard, not a bare value type, or it will reintroduce this bug class.

`DeleteDeploymentStrategy` alone uses `/deployementstrategies/{Id}` (missing the second "n") —
this is a genuine AWS typo baked into the real SDK's serializer, not a gopherstack bug; every
other deployment-strategy op correctly uses the properly-spelled `/deploymentstrategies`. The
route matcher/parser here already special-cases this correctly.

`UpdateDeploymentStrategyInput` has no `Name` field in the real API (deployment strategies
cannot be renamed) — this backend's handler still accepts and applies a `Name` field, which is
harmless dead code (a real SDK client can never populate it) rather than a wire bug, so it was
left alone.

This backend adds `CreatedAt`/`UpdatedAt` fields to `Application`/`Environment`/
`DeploymentStrategy` JSON responses that don't exist in the real AWS shapes (confirmed via
`GetApplicationOutput`/`GetEnvironmentOutput`/`GetDeploymentStrategyOutput` in
`aws-sdk-go-v2/service/appconfig`). Harmless — real deserializers ignore unknown JSON keys — but
noted here so a future auditor doesn't mistake it for parity drift.
