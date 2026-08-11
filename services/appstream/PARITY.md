---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: appstream
sdk_module: aws-sdk-go-v2/service/appstream@v1.64.5
last_audit_commit: HEAD
last_audit_date: 2026-07-26
overall: A            # CI-blocking regression fixed this pass: the eb437919a dep bump to SDK
                       # v1.64.0 switched AppStream's wire protocol from awsjson1.1 to
                       # Smithy rpc-v2-cbor; the handler only spoke the old protocol, so every
                       # real SDK request 403'd (fell through to the S3 catch-all route). See
                       # "Protocol" note below and the 2026-07-26 section at the bottom. Prior
                       # pass's grade/fixes (invented ExportImageTask shape, invented
                       # StartImageBuilder field, missing Expires on streaming URLs, missing
                       # Entitlement.LastModifiedTime, missing DirectoryConfig credential/cert
                       # fields, epoch-timestamp consistency, DescribeAppLicenseUsage field name)
                       # are unaffected by this pass -- op-level wire shapes did not change,
                       # only the transport encoding.
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
  StartImageBuilder: {wire: fixed, errors: ok, state: ok, persist: ok, note: "real StartImageBuilderOutput carries ONLY ImageBuilder -- a prior version invented a top-level StreamingURL field that no real SDK client would ever receive; removed it (and dropped the now-unused url return value from the backend method, which returns error only now)"}
  CreateStreamingURL: {wire: fixed, errors: ok, state: ok, persist: ok, note: "real CreateStreamingURLOutput carries Expires (epoch seconds) alongside StreamingURL, and CreateStreamingURLInput accepts an optional Validity (default 60s) -- both were missing; backend now accepts validitySeconds and returns the computed expiry"}
  CreateImageBuilderStreamingURL: {wire: fixed, errors: ok, state: ok, persist: ok, note: "same Expires/Validity gap as CreateStreamingURL; real default is 3600s"}
  CreateAppBlockBuilderStreamingURL: {wire: fixed, errors: ok, state: ok, persist: ok, note: "same Expires/Validity gap as CreateStreamingURL; real default is 3600s"}
  CreateExportImageTask: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "MAJOR: prior implementation invented an entirely non-existent request/response shape -- an S3Destination{S3Bucket,S3Key} request and an {ExportImageTaskId} response. Real CreateExportImageTaskInput exports a WorkSpaces Applications image to an EC2 AMI (no S3 involved at all): required ImageName+AmiName+IamRoleArn, optional AmiDescription+TagSpecifications; real output is the full ExportImageTask object (not a bare ID). Backend/domain type and handler rewritten to match; DeleteImage->ExportImageTask leak risk N/A since export tasks are independent of source image lifetime (matches real semantics: exporting doesn't pin the source image)"}
  GetExportImageTask: {wire: fixed, errors: ok, state: ok, persist: ok, note: "request field is TaskId, not the invented ExportImageTaskId; response field is TaskId (not ExportImageTaskId), ImageArn (not ImageName), CreatedDate (not CreatedTime), AmiName/AmiDescription/AmiId newly added"}
  ListExportImageTasks: {wire: fixed, errors: ok, state: ok, persist: ok, note: "real ListExportImageTasksInput has no ImageNames filter at all (that was invented) -- it takes generic Filters (opaque Name/Values, semantics undocumented, not evaluated by this emulator) plus MaxResults/NextToken pagination (default page size 50), which the prior version also lacked entirely. Rewritten using pkgs/page for real cursor pagination"}
  DescribeAppLicenseUsage: {wire: fixed, errors: ok, state: ok, persist: ok, note: "response field is AppLicenseUsages (plural) -- was emitting singular AppLicenseUsage, which a real SDK client would never populate its slice from"}
families:
  AppBlock: {status: ok, note: "CRUD verified; Describe now ARN-resolved (see ops above)"}
  AppBlockBuilder: {status: ok, note: "CRUD + Start/Stop verified; StreamingURL now carries real Expires/Validity (see ops above)"}
  Application: {status: ok, note: "CRUD verified; Describe + Fleet-association ops now ARN-resolved (see ops above)"}
  Entitlement: {status: fixed, note: "CreateEntitlement/DeleteEntitlement/DescribeEntitlements/UpdateEntitlement/AssociateApplicationToEntitlement/ListEntitledApplications audited -- keyed correctly by (Name+StackName) composite; ApplicationIdentifier stored opaquely with no cross-reference lookup, so no ARN-vs-Name failure mode exists there. FIXED: backend computed LastModifiedTime on every Create/Update but entitlementToResponse never emitted it -- real Entitlement has both CreatedTime and LastModifiedTime members; now both are on the wire"}
  DirectoryConfig: {status: fixed, note: "CRUD verified against real DirectoryConfig shape; Name-keyed, matches wire. FIXED: Create/UpdateDirectoryConfigInput both carry ServiceAccountCredentials (AccountName+AccountPassword) and CertificateBasedAuthProperties (CertificateAuthorityArn+Status) -- both were accepted by neither the request-decode struct nor the backend, so a real client's directory-join credentials were silently discarded and never returned on Describe. Now parsed, stored, and echoed back (real DirectoryConfig response shape does include AccountPassword verbatim, confirmed via botocore service-2.json -- not redacted like some other AWS services do for secrets)"}
  Image: {status: ok, note: "CopyImage/CreateImportedImage/CreateUpdatedImage/DeleteImage verified Name-keyed (matches real Delete/Copy inputs); Describe now Name-or-Arn resolved"}
  ImageBuilder: {status: fixed, note: "CRUD + Start/Stop verified; Stop now idempotent (see ops above). FIXED: StartImageBuilder response invented a StreamingURL field (see StartImageBuilder op above); StreamingURL creation now carries real Expires/Validity"}
  ImagePermissions: {status: ok, note: "Update/Delete/DescribeImagePermissions verified against real SharedImagePermissions shape"}
  Session: {status: fixed, note: "DescribeSessions/DrainSessionInstance/ExpireSession/CreateStreamingURL verified against real Session shape and DescribeSessionsInput/CreateStreamingURLInput fields. FIXED: CreateStreamingURL now honors Validity and returns Expires (see ops above)"}
  Theme: {status: ok, note: "CRUD verified against real Theme shape"}
  User: {status: ok, note: "CRUD + Enable/Disable verified; ARN partition bug fixed (see CreateUser above)"}
  UserStackAssociation: {status: ok, note: "BatchAssociate/BatchDisassociate/Describe verified; correctly Name-keyed per real UserStackAssociation shape"}
  UsageReportSubscription: {status: ok, note: "single scalar record, verified against real shape"}
  ExportImageTask: {status: fixed, note: "MAJOR rewrite this pass -- prior 'ok' verdict was wrong; the entire request/response shape was gopherstack-invented (S3-based export instead of real AMI export). See CreateExportImageTask/GetExportImageTask/ListExportImageTasks ops above for the full diff. Any real aws-sdk-go-v2 client hitting the old handler would have gotten a response with none of the fields it expects populated"}
gaps: []                # no unfixed divergences found; all confirmed bugs were fixed this pass
deferred: []              # both prior deferred items resolved this pass (see below)
resolved_this_pass:
  - CreatedTime/StartTime/CreatedDate wire encoding switched from time.Time.Unix() (whole-
    second epoch int) to pkgs/awstime.Epoch() (fractional-second epoch float) at all 16 call
    sites across handler.go/handler_appblock.go/handler_application.go/handler_image.go/
    handler_user.go. Both encodings were already wire-valid (real deserializer accepts any
    JSON number via smithytime.ParseEpochSeconds), so this was a style/consistency fix, not a
    bug fix -- closing out the prior audit's "revisit only if sub-second ordering matters"
    deferred note by just doing it, since pkgs/awstime.Epoch existed for exactly this.
  - DescribeAppLicenseUsage's always-empty response is intentional and correct (this backend
    tracks no license-usage state to report on, and real AWS returns empty for an account
    with none either) -- reclassified from "deferred stub flag" to a documented real behavior
    (see the doc comment on InMemoryBackend.DescribeAppLicenseUsage in applications.go) once
    its wire field name (AppLicenseUsages, not AppLicenseUsage) was also fixed.
leaks: {status: clean, note: "no goroutines/janitors in this service; all state lives in store.Table/plain maps behind the single lockmetrics.RWMutex, reset via Handler.Reset -> Backend.Reset -> registry.ResetAll + resetRawMaps. ExportImageTask rewrite kept the same TaskID-keyed store.Table registration (no new leak surface); ListExportImageTasks pagination reads a Snapshot-style copy of all tasks under RLock and sorts/pages it outside any lock extension, so no lock is held across the sort"}
---

## Notes

Protocol: **dual**, same as CloudWatch's dual XML/CBOR handling (see
`services/cloudwatch/PARITY.md`'s "Protocol" note for the sibling case). Two wire protocols
are served side by side from `handler.go`/`rpcv2cbor.go`, selected per-request by
`isCBORRequest`:

- **Legacy**: awsjson1.1, single POST endpoint, `X-Amz-Target: PhotonAdminProxyService.<Op>`
  header, JSON body. The `PhotonAdminProxyService.` target prefix is correct and verified
  against the real aws-sdk-go-v2 request builder (AppStream's internal service name predates
  the public "AppStream"/"WorkSpaces Applications" branding) -- do not "fix" this to
  `AppStream2.` or similar, it will break every real client. Still served deliberately:
  gopherstack's own unit tests (`doRequest` in `handler_test.go` and everything built on it)
  drive this path directly, and older pinned SDKs / the Terraform provider may still speak
  it. See the 2026-07-26 section below.
- **Current**: Smithy rpc-v2-cbor, `aws-sdk-go-v2/service/appstream >= v1.64.0`. POST to
  `/service/PhotonAdminProxyService/operation/<Op>`, `smithy-protocol: rpc-v2-cbor` header,
  CBOR body. This is what every real (unpinned) SDK client now sends.

Unlike CloudWatch, which had to hand-write ~40 CBOR-native operation handlers because its
prior protocol (query/XML) was structurally incompatible with CBOR, AppStream's rpc-v2-cbor
support is a generic bridge through the *existing* JSON operation table (`h.ops` /
`h.dispatch` in handler.go, unchanged) -- see rpcv2cbor.go's package doc comment for why that
works here and doesn't in CloudWatch, and the 2026-07-26 section below for the correctness
details (timestamp tagging, integer vs float CBOR encoding, error body shape) that bridge had
to get right.

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

## 2026-07-24 audit notes

**Bug class found this pass: gopherstack-invented request/response shapes, not just
mis-keyed lookups.** The prior two audit passes both found *identifier* bugs (wrong error
code, ARN vs Name) where the op's own shape was otherwise correct. This pass found the more
severe case: `CreateExportImageTask`/`GetExportImageTask`/`ListExportImageTasks` had a
**wholesale invented shape** with zero fields in common with the real
`CreateExportImageTaskInput`/`ExportImageTask`/`ListExportImageTasksInput` types (verified
against both the vendored `aws-sdk-go-v2/service/appstream/api_op_*.go` files and
botocore's `service-2.json` -- `pip show botocore` ->
`botocore/data/appstream/2016-12-01/service-2.json.gz`). The invented shape modeled
"export an image's metadata to an S3 JSON file"; the real operation is "export an image to
an EC2 AMI" -- a completely different feature with no S3 involvement. Any real
aws-sdk-go-v2 client calling these three ops against the old handler would unmarshal a
response into a `types.ExportImageTask{}` with every field left at its zero value (no
`TaskId`, no `ImageArn`, no `AmiName`) because none of the JSON keys the old handler wrote
(`ExportImageTaskId`, `ImageName`, `S3Bucket`, `S3Key`, `Status`, `CreatedTime`) match any
real member name. `StartImageBuilder`'s response had a narrower version of the same bug
class: a fabricated top-level `StreamingURL` field bolted onto to an otherwise-correct
`ImageBuilder` response, verified absent from the real `StartImageBuilderOutput` (which
carries only `ImageBuilder`) via the same two sources. **Lesson: op-scoped error-code
verification (2026-07-12's bug class) is necessary but not sufficient -- also diff the
full field set of every request/response struct against the real SDK type, not just the
error switch.**

**Bug class found this pass: request fields accepted by the SDK type but never wired into
the decode struct, so real client data was silently discarded.** `Entitlement` computes and
stores `LastModifiedAt` internally (used correctly by `UpdateEntitlement`) but
`entitlementToResponse` never emitted it, even though real `Entitlement` has a
`LastModifiedTime` member. `CreateDirectoryConfigInput`/`UpdateDirectoryConfigInput` both
carry `ServiceAccountCredentials` and `CertificateBasedAuthProperties` per the real model
(verified via botocore), but the request-decode structs had no fields for them at all, so
`json.Unmarshal` silently dropped any real client's directory-join credentials on the
floor. Both are now decoded, persisted on `storedDirectoryConfig`, and echoed back on
Describe (confirmed via botocore that real `DescribeDirectoryConfigs` does return
`AccountPassword` verbatim -- unlike some AWS services, AppStream does not redact it).
**Lesson: a field being present in Go's zero-value form in a response is not proof it was
considered -- check whether the *request* decode struct even has a slot for every
documented input member, especially optional ones easy to skip when only skimming the
happy-path fields.**

**Bug class found this pass: real per-op optional response fields silently omitted.**
`CreateStreamingURL`/`CreateImageBuilderStreamingURL`/`CreateAppBlockBuilderStreamingURL`
all real-world return `Expires` (an epoch timestamp) alongside `StreamingURL`, computed
from an optional `Validity` request parameter (default 60s for the user-facing one, 3600s
for the two builder ones -- verified via doc comments in the vendored SDK's
`api_op_Create*StreamingURL.go` files). None of the three accepted `Validity` or returned
`Expires` before this pass. This is exactly the "streaming URL expiry" surface the audit
brief called out to verify explicitly -- a real client relying on `Expires` to know when to
refresh a session's streaming URL would have silently gotten a URL with no stated
expiration.

## 2026-07-26 — CI-blocking regression: rpc-v2-cbor protocol migration (eb437919a)

**Root cause.** `eb437919a` bumped `aws-sdk-go-v2/service/appstream` to v1.64.0, which
switched the generated client from awsjson1.1 (`X-Amz-Target` header, JSON body) to Smithy
**rpc-v2-cbor** (`req.URL.Path = "/service/PhotonAdminProxyService/operation/<Op>"`,
`smithy-protocol: rpc-v2-cbor` header, CBOR body) -- confirmed directly in the installed
SDK's `serializers.go`/`deserializers.go`. `handler.go`'s `RouteMatcher` only matched the old
header prefix, so every real (unpinned) SDK request missed the AppStream route entirely and
fell through to the S3 catch-all, returning 403. This broke
`TestIntegration_AppStream_StackLifecycle`, `TestIntegration_AppStream_FleetLifecycle`, and
`TestTerraform_AppStreamStack` in CI. `aws-sdk-go-v2/service/cloudwatch` made the same
protocol switch earlier (v1.55+) and already has a working CBOR implementation
(`services/cloudwatch/rpcv2cbor.go`); a repo-wide scan of every SDK module in `go.mod`
confirmed `appstream` and `cloudwatch` are the *only* two services emitting `rpc-v2-cbor` --
cloudwatch was unaffected (already fixed), appstream was the sole regression.

**Fix.** `handler.go`'s `RouteMatcher`/`ExtractOperation`/`Handler()` now branch on
`isCBORRequest` (rpcv2cbor.go) to serve both protocols, mirroring CloudWatch's dual-protocol
`isCBORRequest` dispatch pattern exactly (same idea: check the CBOR path prefix first, fall
back to the legacy header). The **implementation differs from CloudWatch's**, deliberately:
CloudWatch's pre-CBOR protocol was query/XML, structurally incompatible with CBOR, so its
`rpcv2cbor_*.go` files hand-write a CBOR-native encoder/decoder per operation family (~40
operations, ~16 files). AppStream's pre-CBOR protocol was already JSON body -> generic
op-handler table (`h.ops`, unchanged, ~90 operations) that unmarshals into small per-op
structs and returns a `map[string]any` for `json.Marshal`. Because JSON and CBOR are both
just encodings of the same generic map/list/scalar tree, `rpcv2cbor.go` bridges rpc-v2-cbor
through that *existing* table instead of duplicating all ~90 operations by hand: decode CBOR
-> generic Go value -> JSON bytes -> `h.dispatch` (byte-for-byte unchanged) -> JSON bytes ->
generic Go value -> CBOR. This was verified safe against the real SDK's typed CBOR
deserializer, not assumed:

- **Timestamps.** The SDK's `smithycbor.AsTime` requires a `Tag(1, epoch-seconds)`, not a
  bare number (confirmed in `smithy-go@v1.27.3/encoding/cbor/coerce.go`). A naive generic
  bridge would emit a bare number (since the existing response builders already convert
  `time.Time` to a plain `float64` via `awstime.Epoch`/`.Unix()` for the JSON path) and the
  SDK would reject every timestamp field. Fixed by grepping
  `aws-sdk-go-v2/service/appstream/types@v1.64.0/types.go` for every `*time.Time` field,
  cross-checking which of those gopherstack's handlers actually populate, and tagging that
  exact key set (`CreatedTime`, `CreatedDate`, `LastModifiedTime`, `StartTime`, `Expires`) as
  `Tag(1, ...)` on the way out (`timestampKeys`/`numberToCBOR` in rpcv2cbor.go). Fields the
  SDK models as timestamps that gopherstack doesn't populate today
  (`SubscriptionFirstUsedDate`, `PublicBaseImageReleasedDate`, `ErrorTimestamp`, etc.) are
  out of scope for this fix and not in the set; if a future op starts emitting a new
  timestamp-shaped field under a new key, that key must be added here too.
- **Integers vs floats.** `json.Unmarshal` into `any` always produces `float64` for numbers,
  but the SDK's `AsInt32`/`AsInt64` coercers (used for `DesiredInstances`,
  `MaxUserDurationInSeconds`, etc.) only accept CBOR `Uint`/`NegInt`, not `Float32`/`Float64`
  (confirmed in `coerce.go`) -- a plain float-encoded bridge would fail every integer field
  client-side. `numberToCBOR` encodes whole numbers as `Uint`/`NegInt` and only falls back to
  `Float64` for genuinely fractional values.
- **Error shape.** The SDK's generated `getProtocolErrorInfo` (in `deserializers.go`) reads
  the exception name from an `"__type"` key **inside the CBOR body**, not from a header --
  at the time this was written, CloudWatch's `cborError` set only an `X-Amzn-Errortype`
  header and omitted `__type` from the body (both SDK-generated deserializers were read
  directly to confirm this; the header is not consumed by either protocol's generated code
  path). **FIXED 2026-07-26 (bd gopherstack-7fyf):** CloudWatch's CBOR error body now
  includes `"__type"` too, mirroring this file's shape exactly -- see the "rpc-v2-cbor errors
  were missing `__type`" note in `services/cloudwatch/PARITY.md` for the full writeup.
  AppStream's `cborError` includes `"__type"` in the body so typed exception matching
  (`errors.As(err, &types.ResourceNotFoundException{})`) works client-side.
- **Malformed bodies.** A non-map top-level CBOR value is rejected at the transport level
  (`SerializationException`) before it can reach an op handler's `json.Unmarshal` and surface
  a misleading validation error instead, matching CloudWatch's `isCBORMap` check.

**Dual-protocol decision.** Both protocols are served, permanently, not just as a migration
shim. `services/appstream/handler_test.go`'s `doRequest` helper -- and every existing test
built on it, i.e. essentially the entire pre-existing appstream unit test suite -- drives the
legacy `X-Amz-Target` header path directly; deleting it would have broken dozens of existing
tests for no reason. Older pinned SDK versions and the Terraform AWS provider may also still
negotiate the legacy protocol. `errorCodeStatus` was extracted out of the JSON path's
`handleError` (handler.go) into a shared helper so both protocols report identical
error codes/statuses for the same failure, rather than duplicating that switch.

**Operation coverage.** Because the fix routes through the same `h.ops` table used by the
legacy protocol, all ~90 operations are structurally reachable over CBOR by construction, not
just the two ops the failing integration tests happened to exercise (`CreateFleet`,
`CreateStack`). `handler_test.go`'s `TestAppStream_RPCv2CBOR/every_supported_operation_is_reachable_over_CBOR`
asserts this directly: every operation in `GetSupportedOperations()` gets a CBOR request and
must come back as well-formed, decodable CBOR with the `Smithy-Protocol` header set (a route
miss, panic, or encoding bug would instead surface as an undecodable body or a missing
header). Live-verified end to end against a real built binary:
`TestIntegration_AppStream_StackLifecycle` and `TestIntegration_AppStream_FleetLifecycle`
both pass using the real `aws-sdk-go-v2/service/appstream@v1.64.0` client.

**Reuse opportunity -- DONE 2026-07-26 (bd gopherstack-7fyf), partially.** The low-level
protocol plumbing this note originally flagged (`isCBORRequest`/`extractCBOROperation`
path-prefix helpers, `writeCBOR` response header-setting, and `cborError`'s error-body
shaping) turned out to be byte-for-byte identical between appstream and cloudwatch once
cloudwatch's `cborError` also gained `"__type"` -- these four were extracted into
`pkgs/service/rpcv2cbor.go` (`IsRPCv2CBORRequest`/`ExtractRPCv2CBOROperation`/
`WriteRPCv2CBORResponse`/`WriteRPCv2CBORError`); both services now delegate to it instead of
carrying their own copies. The other half of this note's suggestion -- sharing the
CBOR<->generic-value bridge (`cborToGo`/`goToCBOR`/`numberToCBOR`/timestamp tagging) --
was deliberately **not** done: that bridge only exists because appstream routes CBOR through
its existing generic JSON-body op handlers, whereas cloudwatch hand-writes per-operation CBOR
encoders reading directly off a decoded `cbor.Map` (`cborStr`/`cborFloat`/`cborTime`/etc.).
The two approaches operate at different levels of abstraction with no shared shape to
extract; forcing them together would contort cloudwatch's per-field helpers into something
they aren't. Left as-is.
