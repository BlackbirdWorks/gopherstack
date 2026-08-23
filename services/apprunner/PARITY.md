---
service: apprunner
sdk_module: aws-sdk-go-v2/service/apprunner@v1.42.4
last_audit_commit:                                # unknown: pass ran without git access at write time, never backfilled -- gopherstack-33in
last_audit_date: 2026-08-21
overall: A            # full field-diff sweep: closed every gaps/deferred item from the 2026-07-13 audit
last_audit_date: 2026-08-19
overall: A            # wrapper-key/nested-shape sweep 2026-08-19: found and fixed one fabricated-field bug
ops:
  CreateService: {wire: fixed, errors: ok, state: ok, persist: ok, note: "immediate RUNNING (no OPERATION_IN_PROGRESS poll-forever trap); full field set now threaded: InstanceConfiguration (Cpu/Memory/InstanceRoleArn), SourceConfiguration (ImageRepository incl. ImageConfiguration, CodeRepository incl. SourceCodeVersion/CodeConfiguration, AuthenticationConfiguration, AutoDeploymentsEnabled with real default), AutoScalingConfigurationArn (resolved-or-default, HasAssociatedService bookkeeping), NetworkConfiguration (Egress/IngressConfiguration, IpAddressType, real defaults), HealthCheckConfiguration (real defaults), EncryptionConfiguration, ObservabilityConfiguration. Service response now includes the previously-missing required AutoScalingConfigurationSummary and NetworkConfiguration fields. FIXED 2026-08-21 (bd gopherstack-r80d, batch 10; fixed but NOT counted -- see Notes): validateSourceConfig checked CodeRepository.RepositoryUrl but never SourceCodeVersion (types.go:245-263, both required on CodeRepository) -- an omitted SourceCodeVersion was silently accepted and then dropped from codeRepositoryOutput entirely. Added the same required-field check already used for RepositoryUrl/ImageIdentifier. Not counted: the real aws-sdk-go-v2 client's own generated validateCodeRepository (validators.go:792-806) already rejects a nil SourceCodeVersion client-side, so no real Go SDK client can ever reach gopherstack in the buggy state -- proven instead via a raw request bypassing that client-side check, which is real for any other caller (raw HTTP, a non-Go SDK) but not provable via this campaign's real-SDK-client round-trip standard."}
  DescribeService: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateService: {wire: ok, errors: ok, state: ok, persist: ok, note: "rejects update unless status RUNNING, matches InvalidStateException; rejects switching between image/code source types (InvalidRequestException, matching the real op's documented restriction); all new CreateService fields are independently patchable (nil/empty = no change)"}
  DeleteService: {wire: ok, errors: ok, state: ok, persist: ok, note: "now cascade-cleans the service's customDomains map entry and recomputes the old AutoScalingConfiguration's HasAssociatedService (see leaks)"}
  ListServices: {wire: ok, errors: ok, state: ok, persist: ok}
  PauseService: {wire: ok, errors: ok, state: ok, persist: ok}
  ResumeService: {wire: ok, errors: ok, state: ok, persist: ok}
  StartDeployment: {wire: ok, errors: ok, state: ok, persist: ok, note: "records a real operation; completes immediately (SUCCEEDED) rather than modeling OPERATION_IN_PROGRESS"}
  ListOperations: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed -- OperationSummary now includes UpdatedAt (set equal to StartedAt/EndedAt since operations complete immediately in this backend's simplified state machine)"}
  CreateAutoScalingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAutoScalingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAutoScalingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAutoScalingConfigurations: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed -- summary now includes real HasAssociatedService, recomputed from live CreateService/UpdateService/DeleteService association state"}
  UpdateDefaultAutoScalingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  ListServicesForAutoScalingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed -- now returns real associated service ARNs; CreateService threads AutoScalingConfigurationArn (explicit, name-only-ARN, or the account's always-present seeded default) into a real association tracked on every service"}
  CreateConnection: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteConnection: {wire: ok, errors: ok, state: ok, persist: ok}
  ListConnections: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateObservabilityConfiguration: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED 2026-08-21 (bd gopherstack-r80d, batch 10; fixed but NOT counted toward the required-field tally -- TraceConfiguration itself is optional per types.go:601, only its nested Vendor is required-when-present): TracingVendor was captured from CreateObservabilityConfigurationInput and stored, but observabilityConfigurationOutput had no TraceConfiguration field at all, so it was silently dropped on every response. Added, present only when TracingVendor != \"\" (real AWS: absent means tracing isn't enabled -- not fabricating a vendor when none was configured)."}
  DescribeObservabilityConfiguration: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED 2026-08-21 (bd gopherstack-r80d, batch 10): same TraceConfiguration gap and fix as CreateObservabilityConfiguration above."}
  DeleteObservabilityConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  ListObservabilityConfigurations: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed 2026-08-19 -- summary entries were emitting fabricated Status/Latest/CreatedAt keys that have no case in the real types.ObservabilityConfigurationSummary document deserializer (deserializers.go:6215-6270); a real client would silently drop them. Now emits only ObservabilityConfigurationArn/Name/Revision, matching the narrower summary type exactly (types/types.go:613-628)"}
  CreateVpcConnector: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeVpcConnector: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteVpcConnector: {wire: ok, errors: ok, state: ok, persist: ok}
  ListVpcConnectors: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateVpcIngressConnection: {wire: ok, errors: ok, state: partial, persist: ok, note: "doesn't validate ServiceArn refers to an existing service (dangling ref allowed); matches real op's documented error set which has no ResourceNotFoundException, so not a wire bug -- see gaps"}
  DescribeVpcIngressConnection: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteVpcIngressConnection: {wire: ok, errors: ok, state: ok, persist: ok}
  ListVpcIngressConnections: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateVpcIngressConnection: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateCustomDomain: {wire: fixed, errors: ok, state: ok, persist: ok, note: "fixed to use InvalidRequestException (not ResourceNotFoundException) for unknown ServiceArn, matching this op's documented error set. FIXED 2026-08-21 (bd gopherstack-r80d, batch 10): required vpcDNSTargets (api_op_AssociateCustomDomain.go, required) had no struct field on associateCustomDomainOutput at all -- DescribeCustomDomains (identical required set) already emitted it correctly as []. Added, always []any{} (this backend doesn't model per-domain VPC ingress DNS targets, so empty is the honest value, not fabricated)."}
  DisassociateCustomDomain: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED 2026-08-21 (bd gopherstack-r80d, batch 10): same vpcDNSTargets gap and fix as AssociateCustomDomain above."}
  AssociateCustomDomain: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed to use InvalidRequestException (not ResourceNotFoundException) for unknown ServiceArn, matching this op's documented error set. 2026-08-19: also added the previously-missing VpcDNSTargets key (deserializers.go:7705-7763), emitted as an always-empty list since this backend doesn't model VPC-based custom domain DNS -- matches DescribeCustomDomains's existing convention for the same field"}
  DisassociateCustomDomain: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-08-19: added the previously-missing VpcDNSTargets key (deserializers.go:8462-8520), same empty-list convention as AssociateCustomDomain/DescribeCustomDomains"}
  DescribeCustomDomains: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  error_taxonomy: {status: ok, note: "was systemically broken across all 35 ops -- see Notes; fixed 2026-07-13"}
gaps:
  - "CreateVpcIngressConnection doesn't validate that ServiceArn refers to an existing service, allowing a dangling reference. Left as-is because CreateVpcIngressConnection's documented error set has no ResourceNotFoundException -- adding validation would need a new InvalidRequestException-mapped check, not a NotFound one, to stay wire-correct; low traffic op, deferred. Re-verified 2026-07-23: still the correct call, not a bug."
  - "2026-08-19 (Layer 3, disclosed not fixed -- member-never-emitted hunting is out of scope this sweep, only fixed incidentally-surfaced VpcDNSTargets above): ListServices's ServiceSummary items omit UpdatedAt, present on the real types.ServiceSummary (deserializers.go:6856-6963); Service/VpcConnector/VpcIngressConnection all omit DeletedAt, present on their real full types (deserializers.go:6572, 7261, 7500) -- storedVpcConnector/storedVpcIngressConnection/storedAutoScalingConfiguration already track DeletedAt internally but their wire structs never surface it; AutoScalingConfiguration additionally omits Latest (deserializers.go:4591), which isn't tracked in the domain type at all; CustomDomain omits CertificateValidationRecords (deserializers.go:4899, 5381), a genuine backend gap since no cert validation flow is modeled."
deferred: []
leaks: {status: clean, note: "no goroutines/janitors in this backend; existing leak_test.go covers handler/backend lifecycle. 2026-07-23: found and fixed one real leak -- DeleteService left its b.customDomains[serviceArn] entry behind forever (unreachable once the service is gone, since DescribeCustomDomains 404s on a deleted ServiceArn); now cascade-deleted, covered by TestDeleteService_CascadesCustomDomains. New AutoScalingConfiguration HasAssociatedService bookkeeping (CreateService/UpdateService/DeleteService) stays entirely inside the existing b.mu critical sections, no new lock paths or goroutines introduced."}
---

## Notes

**Fixed: systemic wrong exception-type names (the real bug this sweep found).** App Runner's
error model (`aws-sdk-go-v2/service/apprunner/types/errors.go`) has exactly five exception
types: `InternalServiceErrorException`, `InvalidRequestException`, `InvalidStateException`,
`ResourceNotFoundException`, `ServiceQuotaExceededException`. Before this fix, gopherstack's
`handleError` (handler.go) and the backend's sentinel wrappers (backend.go) used three
different, *wrong* type strings:

- Not-found errors (`awserr.ErrNotFound`) returned `__type: "InvalidParameterException"` --
  this exception **does not exist** anywhere in App Runner's model. Every
  Describe/Delete/Update/Pause/Resume/Tag/List op that can 404 was affected; the real SDK
  deserializer (`awsAwsjson10_deserializeOpError*` in deserializers.go) only recognizes
  `ResourceNotFoundException` for these ops, so a real client would get an untyped
  `smithy.GenericAPIError` instead of `types.ResourceNotFoundException`, breaking any
  `errors.As(err, &types.ResourceNotFoundException{})` check callers rely on.
- Already-exists conflicts (`awserr.ErrAlreadyExists`) returned
  `__type: "ServiceQuotaExceededException"`. That type IS valid for CreateService /
  CreateConnection / CreateVpcIngressConnection (verified against their deserializer
  switches), but is semantically about *quota*, not name/domain conflicts, and is **not**
  in AssociateCustomDomain's documented error set at all (only
  `InternalServiceErrorException`, `InvalidRequestException`, `InvalidStateException` --
  confirmed via the AWS docs page, not just the Go SDK). `AssociateCustomDomain` returning
  `ServiceQuotaExceededException` for a duplicate domain association would deserialize as
  untyped on a real client. Fixed to `InvalidRequestException`, which is valid for every
  App Runner operation with no exceptions.
- The catch-all/internal-fault case returned `__type: "InternalServiceError"` (missing the
  `Exception` suffix) instead of `InternalServiceErrorException`. `strings.EqualFold`
  comparisons in the SDK's error switch require an exact string match, so this also always
  fell through to a generic untyped error client-side, even though the HTTP status (500)
  was already correct.

Verified case-by-case against real per-op error sets (not just the model's global exception
list) by walking each `awsAwsjson10_deserializeOpError<Op>` function's `strings.EqualFold`
switch in `aws-sdk-go-v2/service/apprunner@v1.40.2/deserializers.go`. One op-specific
subtlety was fixed as a result: `AssociateCustomDomain`'s "service not found" branch in
`backend.go` now wraps `ErrInvalidParameter` (-> `InvalidRequestException`) instead of the
package-wide `ErrNotFound` (-> `ResourceNotFoundException`), since that op's real error set
has no `ResourceNotFoundException`. `DisassociateCustomDomain` and `DescribeCustomDomains`
*do* accept `ResourceNotFoundException` and were left using `ErrNotFound` unchanged.

HTTP status codes were already correct throughout (400 for all four client-fault exceptions,
500 for the internal-fault default) -- only the `__type` string was wrong.

**Confirmed correct (don't re-flag):**
- Protocol is `awsjson1.0` (`Content-Type: application/x-amz-json-1.0`,
  `X-Amz-Target: AppRunner.<Op>`) -- matches `handler.go`'s `apprunnerTargetPrefix`/
  `contentType` constants and the SDK's `awsAwsjson10_*` serializer/deserializer naming.
- Timestamps: `CreatedAt`/`UpdatedAt`/`StartedAt`/`EndedAt` are epoch-seconds JSON numbers
  on the wire (`smithytime.ParseEpochSeconds` in the real deserializer); gopherstack emits
  these as `int64` via `.Unix()`, which is wire-compatible (a JSON number, same as the
  SDK's `float64` epoch parse expects) even though it doesn't route through
  `pkgs/awstime.Epoch`.
- Field names for `InstanceConfiguration` (`Cpu`/`Memory`/`InstanceRoleArn`),
  `SourceConfiguration.ImageRepository` (`ImageIdentifier`/`ImageRepositoryType`), and all
  `*SummaryList`/`NextToken` list envelopes match the real serializers exactly.
- Status enums used (`RUNNING`/`PAUSED`/`DELETED` for Service; `ACTIVE`/`INACTIVE` for
  ASG/Observability/VpcConnector; `AVAILABLE`/`DELETED` for Connection/VpcIngressConnection)
  are all valid real enum members. Services/configs transition to terminal states
  immediately on create/pause/resume/delete rather than sitting in
  `OPERATION_IN_PROGRESS`/`PENDING_CREATION` -- this is a deliberate simplification (avoids
  the "client polls DescribeService forever" trap called out in parity-principles.md), not a
  disguised no-op: state actually mutates and persists correctly.
- `Handler.Snapshot`/`Restore` delegate to the backend correctly (`persistence.go`); the
  doc comment there notes this delegation itself was a previously-fixed dead-wiring bug
  (Handler had no Snapshot/Restore before Phase 3.3, so App Runner was silently never
  persisted) -- already fixed prior to this sweep, left as historical context.

## Session log

- 2026-07-13 (911ff167): Fresh audit. Fixed the three exception-type-name bugs and the
  AssociateCustomDomain not-found mapping described above (backend.go, handler.go). Added
  `invalidStateType`/`internalServiceErrorType` consts so `handleError` no longer duplicates
  wire-type strings as separate literals from backend.go's sentinel-wrapping consts (the
  literal/const divergence is exactly how the `InternalServiceError` vs
  `InternalServiceErrorException` typo happened in the first place). No existing tests
  asserted the old (wrong) `__type` strings, so no test updates were needed; full existing
  suite plus `go vet`/`go fix -diff`/`golangci-lint` all green. ~30 LOC changed.

- 2026-07-23: Closed every `gaps`/`deferred` item from the 2026-07-13 audit by field-diffing
  `CreateServiceInput`/`UpdateServiceInput`/`Service`/`OperationSummary`/
  `AutoScalingConfigurationSummary` against `aws-sdk-go-v2/service/apprunner@v1.40.2/types`
  and implementing what was missing for real (no stubs):
  - **AutoScalingConfigurationArn association** (the root cause of three separate gaps).
    `CreateService`/`UpdateService` now resolve the ARN (full ARN, name-only ARN, or bare
    name -- both formats `CreateServiceInput`'s doc comment describes) via
    `resolveASG`/`resolveOrDefaultASG` (`service_associations.go`), or fall back to the
    account's default when omitted. `ensureDefaultAutoScalingConfiguration` seeds App
    Runner's real always-present `DefaultConfiguration` revision 1 (real accounts have this
    before any `CreateAutoScalingConfiguration` call) at backend construction, `Reset`, and
    both `Restore` paths. `HasAssociatedService` is now real, recomputed by
    `recomputeASGAssociation` on every association change (create/update/delete) by scanning
    live services rather than a hand-tracked counter (simplicity over micro-perf; table sizes
    are emulator-scale). This closes: `Service.AutoScalingConfigurationSummary` (previously
    always missing, a documented-required field), `ListAutoScalingConfigurations`'s
    `HasAssociatedService` (previously hardcoded false), and
    `ListServicesForAutoScalingConfiguration` (previously always empty).
  - **`Service.NetworkConfiguration`** (previously entirely missing, also a documented-required
    field): `CreateService`/`UpdateService` accept `NetworkConfiguration` (Egress/
    IngressConfiguration, IpAddressType), validate `EgressType: VPC`'s `VpcConnectorArn`
    against the real `vpcConnectors` table (`InvalidRequestException` if unresolvable --
    `CreateService`'s error set has no `ResourceNotFoundException`, verified against
    `awsAwsjson10_deserializeOpErrorCreateService`'s switch), and apply App Runner's
    documented defaults (`DEFAULT` egress, publicly accessible, `IPV4`) when omitted.
  - **`OperationSummary.UpdatedAt`**: added to `storedOperation`/`addOperation` (set equal to
    `StartedAt`/`EndedAt` since operations complete immediately in this backend's simplified
    state machine, matching the existing `SUCCEEDED`-on-create pattern) and threaded through
    `ListOperations`'s wire output.
  - **De-deferred `HealthCheckConfiguration`/`EncryptionConfiguration`/`CodeRepository`
    sub-shapes** (previously silently accepted-and-ignored, per parity-principles.md's
    de-stub-hygiene concern about disguised no-ops): `HealthCheckConfiguration` now stores
    Protocol/Path/Interval/Timeout/HealthyThreshold/UnhealthyThreshold with App Runner's real
    defaults (`TCP`, `/`, 5s, 2s, 1, 5). `EncryptionConfiguration.KmsKey` round-trips and is
    only returned when a customer key was actually provided (App Runner omits it for the
    default managed-key case). `SourceConfiguration.CodeRepository` (RepositoryUrl,
    SourceCodeVersion, CodeConfiguration/CodeConfigurationValues) and
    `AuthenticationConfiguration` (AccessRoleArn, ConnectionArn -- validated against the real
    `connections` table when present) now round-trip field-for-field.
    `AutoDeploymentsEnabled` applies App Runner's documented default (false for an ECR Public
    image source, true otherwise) when the caller doesn't specify it.
    `InstanceConfiguration.InstanceRoleArn` now round-trips (was silently dropped).
    `ServiceObservabilityConfiguration` (ObservabilityEnabled + ObservabilityConfigurationArn,
    validated against the real `observabilityConfigs` table) now round-trips.
  - **`UpdateService`** additionally now rejects switching a service between image and code
    sources (`InvalidRequestException`), matching the real op's documented restriction ("you
    must provide the same structure member... that you originally included when you created
    the service") -- previously unenforced since `CodeRepository` didn't exist at all.
  - **Leak fix**: `DeleteService` was leaving its `b.customDomains[serviceArn]` entry behind
    forever after delete (unreachable dead state, since `DescribeCustomDomains` 404s on a
    deleted `ServiceArn`); now cascade-deleted alongside the existing tags cleanup.
  - Backend `CreateService`/`UpdateService` signatures changed from long positional-primitive
    argument lists to `CreateServiceParams`/`UpdateServiceParams` structs (internal to this
    package -- `StorageBackend` has no external implementers besides `InMemoryBackend`, and no
    caller outside this package touches backend method signatures directly, only
    `NewInMemoryBackend`/`NewHandler`/`Provider` -- verified via repo-wide grep before making
    the change).
  - No new goroutines/tickers/janitors introduced; all new bookkeeping stays inside the
    existing `b.mu` critical sections.
  - Added `service_associations.go` (resolution/validation/normalization helpers) and 15 new
    test functions across `handler_services_test.go` (13, covering every new behavior above)
    and `leak_test.go` (the customDomains cascade-delete fix), plus updated pre-existing
    `ListAutoScalingConfigurations` count assertions and `AutoScalingConfigCount` expectations
    in `handler_auto_scaling_configurations_test.go`/`persistence_test.go` for the new
    always-present `DefaultConfiguration` seed. `go build`/`go vet`/`go test -race`/
    `gofmt -l`/`golangci-lint` all green; zero `cyclop`/`gocyclo`/`gocognit`/`funlen` nolints
    before or after.

## 2026-08-21 pass (bd gopherstack-r80d, batch 10): required OUTPUT members never populated

Second service this batch, after stepfunctions. `cmd/requiredoutputfields`
puts apprunner at 44 required output fields across 32 ops-with-required (37
ops total).

**Wire shape**: not "one wrapper key around a nested domain object"
(pinpoint/bedrockagent/cleanrooms) or `map[string]any` literals (s3tables/
codecommit) -- responses are tagged structs, and most ops' own top-level
required members are flat scalars. But an AST-style walk of every
`type X struct { ... }` block in `apprunner@v1.42.4/types/types.go` (not a
grep window) found only `Service` and its nested source-config family
(`CodeConfiguration`, `CodeConfigurationValues`, `CodeRepository`,
`CustomDomain`, `EncryptionConfiguration`, `ImageRepository`,
`ServiceObservabilityConfiguration`, `SourceCodeVersion`,
`TraceConfiguration`) carry any required fields at all --
`AutoScalingConfiguration`/`Connection`/`ObservabilityConfiguration`/
`VpcConnector`/`VpcIngressConnection` and every one of their `*Summary` list
siblings declare **zero** required fields. That made this pass narrower
than stepfunctions': read all 32 ops' own required members plus every one
of those 9 nested types against their handlers.

### 1 bug found and fixed, proven via a real `aws-sdk-go-v2/service/apprunner`
### client round-trip test (`wire_output_required_r80d_test.go`)

- **`AssociateCustomDomain`/`DisassociateCustomDomain`'s `VpcDNSTargets`**
  (both ops' `Output`, required). `DescribeCustomDomains` -- the sibling op
  with the identical required set (`CustomDomain`, `DNSTarget`, `ServiceArn`,
  `VpcDNSTargets`) -- already emitted `VpcDNSTargets: []any{}` correctly, but
  `associateCustomDomainOutput`/`disassociateCustomDomainOutput` had no
  `VpcDNSTargets` field at all, so the key was entirely absent on both ops.
  Fixed by adding the field to both structs, always `[]any{}` (this backend
  doesn't model per-domain VPC-ingress DNS targets, matching
  `DescribeCustomDomains`'s existing honest-empty convention -- not
  fabricated). Proven via `Test_SDKRoundTrip_CustomDomain_VpcDNSTargets`,
  hand-reverted/confirmed-failing/restored, `md5sum`-verified byte-identical.

### 2 fixed but NOT counted

- **`CodeRepository.SourceCodeVersion`** (types.go:245-263, required
  alongside `RepositoryUrl`). `validateSourceConfig` (services.go) checked
  `RepositoryURL != ""` but never checked `SourceCodeVersionType`, so an
  omitted `SourceCodeVersion` was silently accepted and then dropped from
  `codeRepositoryOutput` (`toCodeRepositoryOutput` only sets it
  `if cs.SourceCodeVersionType != ""`). Fixed by adding the same required
  check already used for `RepositoryUrl`/`ImageIdentifier`. **Not counted**:
  the real `aws-sdk-go-v2` client's own generated `validateCodeRepository`
  (`validators.go:792-806`) already rejects a nil `SourceCodeVersion`
  client-side before any request is sent -- no real Go SDK client can ever
  reach gopherstack in the buggy state, so this campaign's real-SDK-client
  round-trip proof standard cannot apply here even though the underlying gap
  is real for any other caller (raw HTTP, a non-Go SDK, or a Go client with
  validation disabled). Proven instead via a raw request through this
  package's own `doRequest`/`newTestHandler` test helpers
  (`Test_CodeRepository_SourceCodeVersion_Required`), which bypass the Go
  SDK's client-side check the same way those other callers would; hand-
  reverted/confirmed-failing/restored, `md5sum`-verified byte-identical.
- **`ObservabilityConfiguration.TraceConfiguration`** (types.go:601,
  optional -- "If not specified, tracing isn't enabled"; not a Smithy-
  required member, so outside this cut's precise target class even though
  it is a real, provable bug). `CreateObservabilityConfiguration` captured
  `TracingVendor` from the request and stored it, but
  `observabilityConfigurationOutput` had no `TraceConfiguration` field at
  all -- a configured vendor was silently dropped on every
  Create/DescribeObservabilityConfiguration response. Fixed by adding the
  field, present only when `TracingVendor != ""` (matching the real "absent
  means not enabled" semantics, not fabricating a vendor for an
  unconfigured one). Proven via
  `Test_SDKRoundTrip_ObservabilityConfiguration_TraceConfiguration`
  (a genuine real-client round trip -- this one has no client-side
  validation blocking it, unlike `SourceCodeVersion` above), hand-reverted/
  confirmed-failing/restored, `md5sum`-verified byte-identical.

### Reviewed, not a bug / out of scope

- **`ImageRepository.ImageRepositoryType`** (required) is passed through
  unvalidated on `CreateService` (only `ImageIdentifier` is checked), but
  `imageRepositoryOutput.ImageRepositoryType` has no `omitempty` -- an
  omitted value is emitted as a present-but-empty string, not a dropped
  key. Different bug class from this cut's target (wrong/invalid content
  on a present field, not an absent required field) -- disclosed, not
  fixed.
- **`AutoScalingConfiguration`/`Connection`/`ObservabilityConfiguration`/
  `VpcConnector`/`VpcIngressConnection`** and all their `*Summary` siblings:
  confirmed via the same AST-style walk that none declare any required
  field in `types.go` -- there is nothing for this bug class to violate on
  any of them.
- **`Service`**'s own 10 required top-level fields
  (`AutoScalingConfigurationSummary`, `CreatedAt`, `InstanceConfiguration`,
  `NetworkConfiguration`, `ServiceArn`, `ServiceId`, `ServiceName`,
  `SourceConfiguration`, `Status`, `UpdatedAt`) and `CodeConfiguration`/
  `CodeConfigurationValues`'s required fields (`ConfigurationSource`/
  `Runtime`) were all already correctly guarded -- each nested object is
  only constructed (and thus its own required fields only ever set) when a
  real, non-empty upstream value exists, matching this campaign's
  "required-but-inapplicable means present-and-empty, not absent"
  principle by construction. `EncryptionConfiguration.KmsKey` similarly
  guarded (`if svc.EncryptionKmsKey != ""`).
- **`StopExecutionOutput`-style `*float64`/list-field patterns** don't
  apply here; all List ops already build their summary slices via
  `make(..., 0, len(...))` or an explicit nil-guard before assignment,
  confirmed for all 6 List ops that carry a required list field
  (`ListAutoScalingConfigurations`, `ListConnections`,
  `ListObservabilityConfigurations`, `ListServices`,
  `ListServicesForAutoScalingConfiguration`, `ListVpcConnectors`,
  `ListVpcIngressConnections`).

Total for apprunner this pass: 44 required output fields plus all 9 nested
required-bearing types read end to end across 32 ops with required output
fields, 1 counted bug, 2 fixed-but-not-counted findings, 1 disclosed
(wrong-content, not absent-field) finding.
- 2026-08-19: Wrapper-key/nested-shape wire-parity sweep (all 37 ops enumerated from the
  pinned SDK's `api_op_*.go` files and `GetSupportedOperations()`, both agree). Protocol
  reconfirmed as JSON-RPC 1.0 (`awsAwsjson10_*` in `deserializers.go`, exact-match protocol
  -- not tolerant of casing). Read every op's own `awsAwsjson10_deserializeOpDocument<Op>Output`
  and, for every nested/summary type actually emitted, its own `deserializeDocument<Type>`
  case list.
  - **Bug found and fixed**: `ListObservabilityConfigurations`'s summary entries emitted
    three fabricated keys -- `Status`, `Latest`, `CreatedAt` -- that have no case at all in
    the real `types.ObservabilityConfigurationSummary` document deserializer
    (`deserializers.go:6215-6270`; the real struct at `types/types.go:613-628` only has
    `ObservabilityConfigurationArn`/`Name`/`Revision`). A real SDK client would silently drop
    them (this is the same fabricated-summary-field bug class the sibling `fis` sweep found
    this session). Fixed in `handler_observability_configurations.go` by narrowing
    `observabilityConfigurationSummaryOutput` to the 3 real fields. New test
    `TestListObservabilityConfigurations_SummaryHasNoFabricatedFields`
    (`observability_configuration_summary_wire_test.go`) does a raw-body assertion (the real
    SDK type can't observe a leaked key, so this is the only instrument that can), plus
    `TestListObservabilityConfigurations_RealClientSeesSummaryFields` proves the 3 real
    fields still round-trip through the real client. Hand-revert reproduced the exact leaked
    keys in the raw JSON body; restored fix verified byte-identical to the pre-revert file.
  - **Incidental Layer-3 fix** (one only, per this sweep's scope -- member-never-emitted
    hunting is otherwise out of scope): `AssociateCustomDomain`/`DisassociateCustomDomain`
    were missing the `VpcDNSTargets` key present in the real deserializer
    (`deserializers.go:7705-7763`, `8462-8520`) and already emitted (as an always-empty list)
    by the sibling `DescribeCustomDomains` op in the same file. Added the same
    `VpcDNSTargets: []any{}` convention to both. New test
    `TestAssociateDisassociateCustomDomain_VpcDNSTargetsPresent`
    (`custom_domain_vpc_dns_targets_test.go`) proves via the real SDK client that
    `out.VpcDNSTargets` is now non-nil (the real
    `awsAwsjson10_deserializeDocumentVpcDNSTargetList` only runs when the JSON key is
    present, so omitting the key leaves the real client's field `nil` instead of an empty
    slice -- confirmed by hand-revert reproducing exactly that `nil` before restoring).
  - **Families verified clean** (correct wrapper key + correct nesting, summary types
    checked against their own narrower case list, no fabricated fields): CreateService/
    UpdateService/DeleteService/DescribeService/PauseService/ResumeService/StartDeployment
    (`Service`, `serviceOutput` in `handler_services.go`, all nested sub-shapes --
    SourceConfiguration/CodeRepository/ImageRepository/NetworkConfiguration/
    HealthCheckConfiguration/EncryptionConfiguration/ServiceObservabilityConfiguration --
    field-for-field against their own deserializer case lists); ListServices
    (`ServiceSummary`, correctly narrow vs. full `Service`, aside from the disclosed
    `UpdatedAt` gap below); the `AutoScalingConfigurationSummary` embedded in every `Service`
    response (correctly narrow -- 7 fields, no `MaxConcurrency`/`MaxSize`/`MinSize`, matching
    the real embedded-summary shape, not the full `AutoScalingConfiguration`);
    Create/Describe/Delete/UpdateDefaultAutoScalingConfiguration (full type, 10/12 real
    fields -- see gaps for the 2 omitted); ListAutoScalingConfigurations (`Summary`, matches
    the real 7-field narrow type exactly); Create/Delete/ListConnections (`Connection`/
    `ConnectionSummary` -- identical field sets on the real types too, so no over-emission
    risk there); ListOperations (`OperationSummary`, all 7 real fields present, including
    `UpdatedAt` which a prior sweep already fixed); Create/Describe/Delete/
    ListVpcConnectors (`VpcConnector`, no separate summary type in the real SDK, confirmed);
    Create/Describe/Delete/List/UpdateVpcIngressConnection (`VpcIngressConnection` full type,
    and `VpcIngressConnectionSummary` -- correctly narrow at just 2 fields,
    `VpcIngressConnectionArn`/`ServiceArn`, matching the real type exactly);
    ListServicesForAutoScalingConfiguration (plain `ServiceArnList`); Tag/Untag/
    ListTagsForResource (`Tag`: `Key`/`Value`, `TagResourceOutput`/`UntagResourceOutput`
    correctly empty). `DescribeCustomDomains` itself already emitted `VpcDNSTargets`
    correctly (the two sibling ops fixed above didn't).
  - Gates: `go build`, `go vet`, `go fix -diff`, `gofmt -l`, `go test -race`,
    `golangci-lint run` all clean on `./services/apprunner/...` after the fixes (see report
    for verbatim output). Zero `cyclop`/`gocyclo`/`gocognit`/`funlen` nolints introduced.
    No existing test asserted a wrong key for either bug, so none needed correcting -- only
    new tests were added.
