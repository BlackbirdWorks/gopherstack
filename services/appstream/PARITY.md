---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: appstream
sdk_module: aws-sdk-go-v2/service/appstream@v1.60.3
last_audit_commit: a7d6e20e
last_audit_date: 2026-07-12
overall: A            # genuine fixes found this pass (error codes, ARN-identifier resolution)
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  DeleteFleet: {wire: ok, errors: fixed, state: ok, persist: ok, note: "was InvalidAccountStatusException on RUNNING fleet; real DeleteFleet deserializer only recognizes ConcurrentModificationException/ResourceInUseException/ResourceNotFoundException -> now ResourceInUseException"}
  StopFleet: {wire: ok, errors: fixed, state: ok, persist: ok, note: "was erroring (InvalidAccountStatusException) on already-STOPPED fleet; real StopFleet deserializer has no state-conflict exception at all -> now idempotent no-op success"}
  StartFleet: {wire: ok, errors: ok, state: ok, persist: ok, note: "InvalidAccountStatusException on already-RUNNING fleet IS in StartFleet's real deserializer switch -- left unchanged, verified wire-compatible even though semantically about account suspension"}
  DeleteAppBlockBuilder: {wire: ok, errors: fixed, state: ok, persist: ok, note: "same DeleteFleet bug class -- now ResourceInUseException on RUNNING builder"}
  StopAppBlockBuilder: {wire: ok, errors: fixed, state: ok, persist: ok, note: "same StopFleet bug class -- now idempotent on already-STOPPED"}
  StartAppBlockBuilder: {wire: ok, errors: ok, state: ok, persist: ok, note: "InvalidAccountStatusException on already-RUNNING IS in real deserializer -- left unchanged"}
  StopImageBuilder: {wire: ok, errors: fixed, state: ok, persist: ok, note: "same StopFleet bug class -- now idempotent on already-STOPPED"}
  StartImageBuilder: {wire: ok, errors: ok, state: ok, persist: ok, note: "InvalidAccountStatusException on already-RUNNING IS in real deserializer -- left unchanged"}
  DescribeApplications: {wire: fixed, errors: ok, state: ok, persist: ok, note: "real request carries Arns (not Names); backend was doing a Name-keyed map lookup against the caller's ARN, so any real SDK client's Describe-after-Create always 404'd -- added findApplication() Name-or-Arn resolver"}
  DescribeAppBlocks: {wire: fixed, errors: ok, state: ok, persist: ok, note: "same DescribeApplications bug class; added findAppBlock() resolver"}
  DescribeImages: {wire: fixed, errors: ok, state: ok, persist: ok, note: "real request supports both Names and Arns filters; the Arns-only path was mis-resolved through the Name-keyed table -- added findImage() resolver so either identifier works"}
  AssociateApplicationFleet: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "real request carries ApplicationArn (not application Name); association was stored/looked-up under the raw ARN in a Name-keyed map -- resolved to canonical Name via findApplication() before storing"}
  DisassociateApplicationFleet: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "same AssociateApplicationFleet bug class"}
  DescribeApplicationFleetAssociations: {wire: fixed, errors: ok, state: ok, persist: ok, note: "ApplicationArn filter now resolved to canonical Name before matching map keys"}
  AssociateAppBlockBuilderAppBlock: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "same AssociateApplicationFleet bug class, real request carries AppBlockArn"}
  DisassociateAppBlockBuilderAppBlock: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "same bug class"}
  DescribeAppBlockBuilderAppBlockAssociations: {wire: fixed, errors: ok, state: ok, persist: ok, note: "AppBlockArn filter now resolved to canonical Name before matching map keys"}
  CreateUser: {wire: fixed, errors: ok, state: ok, persist: ok, note: "userARN() hand-formatted \"arn:aws:appstream:...\" bypassing pkgs/arn -- always emitted the standard partition even for GovCloud/China/ISO regions; switched to arn.Build()"}
  CreateStack: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeStacks: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateStack: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteStack: {wire: ok, errors: ok, state: ok, persist: ok, note: "correctly ResourceInUseException when an associated fleet exists (verified against real model in a prior audit)"}
  CreateFleet: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeFleets: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateFleet: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateFleet: {wire: ok, errors: ok, state: ok, persist: ok, note: "Fleet-Stack association correctly uses Names on both sides per real AssociateFleetInput -- no ARN-resolution bug here"}
  DisassociateFleet: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAssociatedFleets: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAssociatedStacks: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  AppBlock: {status: ok, note: "CRUD verified; Describe now ARN-resolved (see ops above)"}
  Application: {status: ok, note: "CRUD verified; Describe + Fleet-association ops now ARN-resolved (see ops above)"}
  Entitlement: {status: ok, note: "CreateEntitlement/DeleteEntitlement/DescribeEntitlements/UpdateEntitlement/AssociateApplicationToEntitlement/ListEntitledApplications audited -- keyed correctly by (Name+StackName) composite; ApplicationIdentifier stored opaquely with no cross-reference lookup, so no ARN-vs-Name failure mode exists there"}
  DirectoryConfig: {status: ok, note: "CRUD verified against real DirectoryConfig shape; Name-keyed, matches wire"}
  Image: {status: ok, note: "CopyImage/CreateImportedImage/CreateUpdatedImage/DeleteImage verified Name-keyed (matches real Delete/Copy inputs); Describe now Name-or-Arn resolved"}
  ImageBuilder: {status: ok, note: "CRUD + Start/Stop verified; Stop now idempotent (see ops above)"}
  ImagePermissions: {status: ok, note: "Update/Delete/DescribeImagePermissions verified against real SharedImagePermissions shape"}
  Session: {status: ok, note: "DescribeSessions/DrainSessionInstance/ExpireSession/CreateStreamingURL verified against real Session shape and DescribeSessionsInput/CreateStreamingURLInput fields"}
  Theme: {status: ok, note: "CRUD verified against real Theme shape"}
  User: {status: ok, note: "CRUD + Enable/Disable verified; ARN partition bug fixed (see CreateUser above)"}
  UserStackAssociation: {status: ok, note: "BatchAssociate/BatchDisassociate/Describe verified; correctly Name-keyed per real UserStackAssociation shape"}
  UsageReportSubscription: {status: ok, note: "single scalar record, verified against real shape"}
  ExportImageTask: {status: ok, note: "Create/Get/List verified against real shape"}
gaps: []                # no unfixed divergences found; all confirmed bugs were fixed this pass
deferred:
  - CreatedTime/StartTime wire encoding uses .Unix() (whole-second epoch int) instead of
    pkgs/awstime.Epoch() (fractional-second epoch float). Both are valid JSON numbers the
    real awsjson1.1 deserializer accepts, and the codebase is internally consistent (13
    call sites), so this was left as a style note rather than "fixed" -- revisit only if
    sub-second CreatedTime ordering becomes a test requirement.
  - DescribeAppLicenseUsage is a deliberate always-empty stub (no license-usage backend
    state exists to report on); acceptable since it returns a real (empty) shape rather
    than a fabricated one, but flagged for anyone adding real license-usage tracking.
leaks: {status: clean, note: "no goroutines/janitors in this service; all state lives in store.Table/plain maps behind the single lockmetrics.RWMutex, reset via Handler.Reset -> Backend.Reset -> registry.ResetAll + resetRawMaps"}
---

## Notes

Protocol: awsjson1.1 (single POST endpoint, `X-Amz-Target: PhotonAdminProxyService.<Op>`).
The `PhotonAdminProxyService.` target prefix is correct and verified against the real
aws-sdk-go-v2 request builder (AppStream's internal service name predates the public
"AppStream"/"WorkSpaces Applications" branding) -- do not "fix" this to `AppStream2.` or
similar, it will break every real client.

**Bug class found this pass: op-scoped error-code verification.** aws-sdk-go-v2 generates
one error-deserializer switch statement PER OPERATION (see
`deserializers.go:awsAwsjson11_deserializeOpError<Op>`), and that switch only recognizes
the exception shapes actually bound to that operation in the botocore service model. An
emulator returning an error code the operation's real deserializer doesn't recognize gets
silently demoted to a `smithy.GenericAPIError` client-side -- `errors.As(err,
&types.SomeException{})` no longer matches. This service had three fleet/builder
state-transition guards (`DeleteFleet`/`DeleteAppBlockBuilder` on RUNNING,
`StopFleet`/`StopAppBlockBuilder`/`StopImageBuilder` on already-STOPPED) all wired to a
single shared sentinel (`ErrFleetNotStopped`, `__type: InvalidAccountStatusException`)
copied from `StartFleet`'s legitimate use of that code, without checking whether the
*other* four operations' deserializers actually recognize it. `DeleteFleet`/
`DeleteAppBlockBuilder` only recognize `ResourceInUseException` for this scenario;
`StopFleet`/`StopAppBlockBuilder`/`StopImageBuilder` recognize no state-conflict
exception at all (real AWS treats stopping an already-stopped resource as an idempotent
no-op). Verify the per-operation exception binding via botocore's `service-2.json` (`pip
show botocore` -> `botocore/data/<service>/<version>/service-2.json.gz`) AND the vendored
`aws-sdk-go-v2` module's `deserializers.go` switch (both were used this pass; botocore's
`operations[Op].errors` list matches the go SDK's per-op switch contents exactly, since
both are generated from the same upstream Smithy/model source) -- do not infer the correct
code from an operation's prose documentation or from what "reads" semantically plausible.

**Bug class found this pass: ARN-vs-Name identifier confusion in association ops.**
Several AppStream ops accept an ARN for one side of a two-resource relationship while the
backend's internal maps are Name-keyed (the true identity field returned by Create*).
`DescribeApplications`/`DescribeAppBlocks` filter by `Arns` (not `Names` -- verified via
`DescribeApplicationsInput`/`DescribeAppBlocksInput`, which have no `Names` field at all).
`AssociateApplicationFleet`/`DisassociateApplicationFleet`/
`DescribeApplicationFleetAssociations` carry `ApplicationArn` (ARN) + `FleetName` (Name).
`AssociateAppBlockBuilderAppBlock`/`DisassociateAppBlockBuilderAppBlock`/
`DescribeAppBlockBuilderAppBlockAssociations` carry `AppBlockArn` (ARN) +
`AppBlockBuilderName` (Name). The pre-fix backend treated every one of these ARN
parameters as if it were the resource's Name and indexed a Name-keyed
`store.Table`/`map[string]bool` with it directly -- any real SDK client (which always
passes back the `Arn` field from a prior Create/Describe response, never invents a Name
string for these particular parameters) would get `ResourceNotFoundException` on every
Describe-by-ARN and silently-inert (no error, no effect) Associate calls, since the
association would be stored under a key nothing would ever query by name-equality either.
Fixed via `findApplication`/`findAppBlock`/`findImage` helpers that resolve either Name or
Arn, so association state is always stored under the canonical Name and both real
ARN-style traffic and this codebase's existing Name-style test fixtures work unchanged.
Fleet-Stack association (`AssociateFleet`/`DisassociateFleet`/`ListAssociatedFleets`/
`ListAssociatedStacks`) does NOT have this bug -- verified `AssociateFleetInput` uses
`FleetName`+`StackName`, both real Names, no ARN involved.

**Trap for the next auditor:** `ApplicationIdentifier` (used by
`AssociateApplicationToEntitlement`/`DisassociateApplicationFromEntitlement`/
`ListEntitledApplications`) looks like it should have the same ARN-resolution bug, but it
doesn't -- the backend never looks it up against `b.applications` at all; it's stored and
returned as an opaque string keyed only by the entitlement's own composite key. This is
correct as-is (real AWS doesn't strictly define this identifier's format either), so don't
"fix" it into another find-by-ARN-or-Name resolver without a concrete wire-shape reason.
