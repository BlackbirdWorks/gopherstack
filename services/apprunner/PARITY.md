---
service: apprunner
sdk_module: aws-sdk-go-v2/service/apprunner@v1.40.2
last_audit_commit: 911ff167
last_audit_date: 2026-07-13
overall: A            # systemic error-type wire bug found and fixed across nearly every op
ops:
  CreateService: {wire: ok, errors: ok, state: ok, persist: ok, note: "immediate RUNNING (no OPERATION_IN_PROGRESS poll-forever trap); CPU/Memory/ImageURI stored and reflected"}
  DescribeService: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateService: {wire: ok, errors: ok, state: ok, persist: ok, note: "rejects update unless status RUNNING, matches InvalidStateException"}
  DeleteService: {wire: ok, errors: ok, state: ok, persist: ok}
  ListServices: {wire: ok, errors: ok, state: ok, persist: ok}
  PauseService: {wire: ok, errors: ok, state: ok, persist: ok}
  ResumeService: {wire: ok, errors: ok, state: ok, persist: ok}
  StartDeployment: {wire: ok, errors: ok, state: ok, persist: ok, note: "records a real operation; completes immediately (SUCCEEDED) rather than modeling OPERATION_IN_PROGRESS"}
  ListOperations: {wire: partial, errors: ok, state: ok, persist: ok, note: "OperationSummary missing UpdatedAt field (real API has it); gap only, not wire-breaking since it's optional"}
  CreateAutoScalingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAutoScalingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAutoScalingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAutoScalingConfigurations: {wire: partial, errors: ok, state: ok, persist: ok, note: "summary omits HasAssociatedService (always false; see gaps)"}
  UpdateDefaultAutoScalingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  ListServicesForAutoScalingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "always returns empty list -- CreateService doesn't thread AutoScalingConfigurationArn, so no association ever exists; see gaps"}
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
  error_taxonomy: {status: ok, note: "was systemically broken across all 35 ops -- see Notes; fixed"}
gaps:
  - "Service response is missing the required AutoScalingConfigurationSummary and NetworkConfiguration fields (real API always populates both); CreateServiceInput doesn't accept AutoScalingConfigurationArn so no ASG association is ever tracked, and ListServicesForAutoScalingConfiguration / AutoScalingConfigurationSummary.HasAssociatedService are consequently always empty/false. Feature-sized gap, not a disguised no-op (CreateService/DescribeService work correctly for what they do model). File a bd issue for full ASG-association wiring if CreateService's AutoScalingConfigurationArn input becomes a priority."
  - "OperationSummary is missing the real API's UpdatedAt field (StartedAt/EndedAt/Id/Type/Status/TargetArn all present and correct)."
  - "ListAutoScalingConfigurations summary omits HasAssociatedService (tied to the same CreateService gap above)."
  - "CreateVpcIngressConnection doesn't validate that ServiceArn refers to an existing service, allowing a dangling reference. Left as-is because CreateVpcIngressConnection's documented error set has no ResourceNotFoundException -- adding validation would need a new InvalidRequestException-mapped check, not a NotFound one, to stay wire-correct; low traffic op, deferred."
deferred:
  - Deep field-by-field audit of HealthCheckConfiguration / EncryptionConfiguration / NetworkConfiguration / CodeRepository sub-shapes (service doesn't implement these request fields at all yet; they're silently accepted-and-ignored on input rather than rejected -- same category as the ASG-association gap above, not a wire-breaking bug).
leaks: {status: clean, note: "no goroutines/janitors in this backend; existing leak_test.go covers handler/backend lifecycle"}
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
