---
service: apprunner
sdk_module: aws-sdk-go-v2/service/apprunner@v1.40.2
last_audit_commit: pending (agent instructed not to run git; set at commit time)
last_audit_date: 2026-07-23
overall: A            # full field-diff sweep: closed every gaps/deferred item from the 2026-07-13 audit
ops:
  CreateService: {wire: ok, errors: ok, state: ok, persist: ok, note: "immediate RUNNING (no OPERATION_IN_PROGRESS poll-forever trap); full field set now threaded: InstanceConfiguration (Cpu/Memory/InstanceRoleArn), SourceConfiguration (ImageRepository incl. ImageConfiguration, CodeRepository incl. SourceCodeVersion/CodeConfiguration, AuthenticationConfiguration, AutoDeploymentsEnabled with real default), AutoScalingConfigurationArn (resolved-or-default, HasAssociatedService bookkeeping), NetworkConfiguration (Egress/IngressConfiguration, IpAddressType, real defaults), HealthCheckConfiguration (real defaults), EncryptionConfiguration, ObservabilityConfiguration. Service response now includes the previously-missing required AutoScalingConfigurationSummary and NetworkConfiguration fields"}
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
  CreateObservabilityConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeObservabilityConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteObservabilityConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  ListObservabilityConfigurations: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateVpcConnector: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeVpcConnector: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteVpcConnector: {wire: ok, errors: ok, state: ok, persist: ok}
  ListVpcConnectors: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateVpcIngressConnection: {wire: ok, errors: ok, state: partial, persist: ok, note: "doesn't validate ServiceArn refers to an existing service (dangling ref allowed); matches real op's documented error set which has no ResourceNotFoundException, so not a wire bug -- see gaps"}
  DescribeVpcIngressConnection: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteVpcIngressConnection: {wire: ok, errors: ok, state: ok, persist: ok}
  ListVpcIngressConnections: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateVpcIngressConnection: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateCustomDomain: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed to use InvalidRequestException (not ResourceNotFoundException) for unknown ServiceArn, matching this op's documented error set"}
  DisassociateCustomDomain: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeCustomDomains: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  error_taxonomy: {status: ok, note: "was systemically broken across all 35 ops -- see Notes; fixed 2026-07-13"}
gaps:
  - "CreateVpcIngressConnection doesn't validate that ServiceArn refers to an existing service, allowing a dangling reference. Left as-is because CreateVpcIngressConnection's documented error set has no ResourceNotFoundException -- adding validation would need a new InvalidRequestException-mapped check, not a NotFound one, to stay wire-correct; low traffic op, deferred. Re-verified 2026-07-23: still the correct call, not a bug."
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
