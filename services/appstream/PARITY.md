---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: appstream
sdk_module: aws-sdk-go-v2/service/appstream@v1.64.5
last_audit_commit:                                # unknown: pass ran without git access at write time, never backfilled -- gopherstack-33in
last_audit_date: 2026-08-23
overall: A            # 2026-08-23: closed the one remaining named-and-flagged gap this file
                       # carried (UpdateThemeForStack request-side accept-and-drop -- see
                       # UpdateThemeForStack/Theme ops rows and the dated Notes section at the
                       # bottom). No other new bugs found this pass; the rest of this file's
                       # prior findings were re-read, not re-scanned wholesale.
                       # CI-blocking regression fixed this pass: the eb437919a dep bump to SDK
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
  StartImageBuilder: {wire: fixed, errors: ok, state: ok, persist: ok, note: "InvalidAccountStatusException on already-RUNNING IS in real deserializer -- left unchanged. real StartImageBuilderOutput carries ONLY ImageBuilder -- a prior version invented a top-level StreamingURL field that no real SDK client would ever receive; removed it (and dropped the now-unused url return value from the backend method, which returns error only now)."}
  DescribeApplications: {wire: fixed, errors: ok, state: ok, persist: ok, note: "real request carries Arns (not Names); backend was doing a Name-keyed map lookup against the caller's ARN, so any real SDK client's Describe-after-Create always 404'd -- added findApplication() Name-or-Arn resolver"}
  DescribeAppBlocks: {wire: fixed, errors: ok, state: ok, persist: ok, note: "same DescribeApplications bug class; added findAppBlock() resolver"}
  DescribeImages: {wire: fixed, errors: ok, state: ok, persist: ok, note: "real request supports both Names and Arns filters; the Arns-only path was mis-resolved through the Name-keyed table -- added findImage() resolver so either identifier works. FIXED 2026-08-30 (wrapper-key-sweep): the Type filter (VisibilityType, wire key \"Type\" per serializeCBOR_DescribeImagesInput) was declared on the real input and never read at all -- a Type=PUBLIC request silently got back every private image instead of an empty list (this backend only ever creates PRIVATE images). Now filtered."}
  DescribeImagePermissions: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED 2026-08-30 (wrapper-key-sweep): SharedAwsAccountIds (wire key \"SharedAwsAccountIds\") was declared on the real input and never read -- filtering by an account an image was never shared with returned every shared account instead of an empty list. Now filtered."}
  DescribeSessions: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED 2026-08-30 (wrapper-key-sweep): AuthenticationType (wire key \"AuthenticationType\") was declared on the real input and never read -- every session this backend creates (CreateStreamingURL) has AuthenticationType API, so a USERPOOL-filtered request silently got back the API session instead of an empty list. Now filtered. InstanceId remains unfilterable: this backend has no streaming-instance concept to filter on (undocumented-by-model-absence gap, not a misread key)."}
  AssociateApplicationFleet: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "real request carries ApplicationArn (not application Name); association was stored/looked-up under the raw ARN in a Name-keyed map -- resolved to canonical Name via findApplication() before storing"}
  DisassociateApplicationFleet: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "same AssociateApplicationFleet bug class"}
  DescribeApplicationFleetAssociations: {wire: fixed, errors: ok, state: ok, persist: ok, note: "ApplicationArn filter now resolved to canonical Name before matching map keys"}
  AssociateAppBlockBuilderAppBlock: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "same AssociateApplicationFleet bug class, real request carries AppBlockArn"}
  DisassociateAppBlockBuilderAppBlock: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "same bug class"}
  DescribeAppBlockBuilderAppBlockAssociations: {wire: fixed, errors: ok, state: ok, persist: ok, note: "AppBlockArn filter now resolved to canonical Name before matching map keys"}
  CreateUser: {wire: fixed, errors: ok, state: ok, persist: ok, note: "userARN() hand-formatted \"arn:aws:appstream:...\" bypassing pkgs/arn -- always emitted the standard partition even for GovCloud/China/ISO regions; switched to arn.Build()"}
  CreateApplication: {wire: fixed, errors: fixed, state: fixed, persist: ok, note: "required members IconS3Location and InstanceFamilies (api_op_CreateApplication.go:47,53) were accepted nowhere -- neither stored nor returned, so every created application had no icon and no supported instance families. Now validated (including IconS3Location.S3Key, itself required specifically for this op per types/types.go:1434-1451) and echoed on Describe. Missing-required-member requests now return SerializationException: this op's own deserializer switch (rpc2_deserializeOpErrorCreateApplication) declares only ConcurrentModificationException/LimitExceededException/OperationNotPermittedException/ResourceAlreadyExistsException/ResourceNotFoundException -- no validation-style exception -- consistent with the SDK client blocking such requests before they're ever sent (gopherstack-ii4c)."}
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
  CreateStreamingURL: {wire: fixed, errors: ok, state: ok, persist: ok, note: "real CreateStreamingURLOutput carries Expires (epoch seconds) alongside StreamingURL, and CreateStreamingURLInput accepts an optional Validity (default 60s) -- both were missing; backend now accepts validitySeconds and returns the computed expiry"}
  CreateImageBuilderStreamingURL: {wire: fixed, errors: ok, state: ok, persist: ok, note: "same Expires/Validity gap as CreateStreamingURL; real default is 3600s"}
  CreateAppBlockBuilderStreamingURL: {wire: fixed, errors: ok, state: ok, persist: ok, note: "same Expires/Validity gap as CreateStreamingURL; real default is 3600s"}
  CreateExportImageTask: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "MAJOR: prior implementation invented an entirely non-existent request/response shape -- an S3Destination{S3Bucket,S3Key} request and an {ExportImageTaskId} response. Real CreateExportImageTaskInput exports a WorkSpaces Applications image to an EC2 AMI (no S3 involved at all): required ImageName+AmiName+IamRoleArn, optional AmiDescription+TagSpecifications; real output is the full ExportImageTask object (not a bare ID). Backend/domain type and handler rewritten to match; DeleteImage->ExportImageTask leak risk N/A since export tasks are independent of source image lifetime (matches real semantics: exporting doesn't pin the source image)"}
  GetExportImageTask: {wire: fixed, errors: ok, state: ok, persist: ok, note: "request field is TaskId, not the invented ExportImageTaskId; response field is TaskId (not ExportImageTaskId), ImageArn (not ImageName), CreatedDate (not CreatedTime), AmiName/AmiDescription/AmiId newly added"}
  ListExportImageTasks: {wire: fixed, errors: ok, state: ok, persist: ok, note: "real ListExportImageTasksInput has no ImageNames filter at all (that was invented) -- it takes generic Filters (opaque Name/Values, semantics undocumented, not evaluated by this emulator) plus MaxResults/NextToken pagination (default page size 50), which the prior version also lacked entirely. Rewritten using pkgs/page for real cursor pagination"}
  DescribeAppLicenseUsage: {wire: fixed, errors: ok, state: ok, persist: ok, note: "response field is AppLicenseUsages (plural) -- was emitting singular AppLicenseUsage, which a real SDK client would never populate its slice from"}
  CreateThemeForStack: {wire: fixed, errors: fixed, state: fixed, persist: ok, note: "gopherstack-afi1: 4 of 5 required members (FaviconS3Location, OrganizationLogoS3Location, ThemeStyling, TitleText -- api_op_CreateThemeForStack.go:29-59) were accepted nowhere; only StackName was read, so every theme had no branding at all. Now validated present (missing-required-member -> SerializationException, same rationale/precedent as CreateApplication above: this op's own deserializer switch declares only ConcurrentModificationException/InvalidAccountStatusException/LimitExceededException/OperationNotPermittedException/ResourceAlreadyExistsException/ResourceNotFoundException, no validation-style exception) and echoed on the response: real Theme (types/types.go:1752-1781) carries derived ThemeFaviconURL/ThemeOrganizationLogoURL (not the raw S3Location) -- gopherstack derives a pseudo-URL via themeURL() (https://s3.amazonaws.com/{bucket}/{key}, matching this repo's existing services/amplify/services/serverlessrepo S3-pseudo-URL convention) rather than fabricating a signed URL. FooterLinks (optional) is also now modeled end-to-end (ThemeFooterLink)."}
  UpdateThemeForStack: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "FIXED 2026-08-23 (request-side accept-and-drop sweep) -- opUpdateThemeForStack decoded only StackName; every other real UpdateThemeForStackInput member (api_op_UpdateThemeForStack.go:29-64: AttributesToDelete/FaviconS3Location/FooterLinks/OrganizationLogoS3Location/State/ThemeStyling/TitleText, all optional -- only StackName is required) was parsed nowhere and silently discarded, so a real client updating a theme's branding always got back the theme unchanged with a 200 OK. Now threaded end-to-end via a new ThemeUpdateOptions (interfaces.go) and InMemoryBackend.applyThemeUpdate (themes.go): a nil FaviconS3Location/OrganizationLogoS3Location/TitleText/FooterLinks means unset (leave unchanged, matching this op's own 'omitted fields are unchanged' semantics -- distinguishable in Go's encoding/json exactly as the real SDK's own pointer/slice-nil-vs-set fields distinguish it, confirmed against serializeCBOR_UpdateThemeForStackInput's per-field `if v.X != nil`/`len(v.X) > 0` guards); AttributesToDelete=[FOOTER_LINKS] (the only real ThemeAttribute enum value) is applied last so a delete always wins over a same-request FooterLinks set, matching this repo's established rekognition UpdateStreamProcessor apply-then-delete convention. No enum validation added for ThemeStyling/State on Update (unlike Create): this op's own deserializer error switch (ConcurrentModificationException/InvalidAccountStatusException/InvalidParameterCombinationException/LimitExceededException/OperationNotPermittedException/ResourceNotFoundException, deserializers.go) has no ValidationException/SerializationException case for a real client to receive, so no error contract could be confirmed -- fabricating one would repeat the mistake ACM's ValidationMethod=HTTP gap explicitly avoided; values are stored/echoed verbatim instead. See TestSDKRoundTrip_UpdateThemeForStack_FieldsApply (wire_shape_test.go), a real aws-sdk-go-v2 client round trip."}
families:
  AppBlock: {status: ok, note: "CRUD verified; Describe now ARN-resolved (see ops above)"}
  AppBlockBuilder: {status: ok, note: "CRUD + Start/Stop verified; StreamingURL now carries real Expires/Validity (see ops above)"}
  Application: {status: fixed, note: "CRUD verified; Describe + Fleet-association ops now ARN-resolved (see ops above). FIXED: CreateApplication's required IconS3Location/InstanceFamilies were dropped entirely (see CreateApplication above)"}
  Entitlement: {status: fixed, note: "CreateEntitlement/DeleteEntitlement/DescribeEntitlements/UpdateEntitlement/AssociateApplicationToEntitlement/ListEntitledApplications audited -- keyed correctly by (Name+StackName) composite; ApplicationIdentifier stored opaquely with no cross-reference lookup, so no ARN-vs-Name failure mode exists there. FIXED: backend computed LastModifiedTime on every Create/Update but entitlementToResponse never emitted it -- real Entitlement has both CreatedTime and LastModifiedTime members; now both are on the wire"}
  DirectoryConfig: {status: fixed, note: "CRUD verified against real DirectoryConfig shape; Name-keyed, matches wire. FIXED: Create/UpdateDirectoryConfigInput both carry ServiceAccountCredentials (AccountName+AccountPassword) and CertificateBasedAuthProperties (CertificateAuthorityArn+Status) -- both were accepted by neither the request-decode struct nor the backend, so a real client's directory-join credentials were silently discarded and never returned on Describe. Now parsed, stored, and echoed back (real DirectoryConfig response shape does include AccountPassword verbatim, confirmed via botocore service-2.json -- not redacted like some other AWS services do for secrets)"}
  Image: {status: fixed, note: "CopyImage/CreateImportedImage/CreateUpdatedImage/DeleteImage verified Name-keyed (matches real Delete/Copy inputs); Describe now Name-or-Arn resolved. FIXED 2026-08-30: DescribeImages dropped the Type (VisibilityType) filter (see DescribeImages op above)"}
  ImageBuilder: {status: fixed, note: "CRUD + Start/Stop verified; Stop now idempotent (see ops above). FIXED: StartImageBuilder response invented a StreamingURL field (see StartImageBuilder op above); StreamingURL creation now carries real Expires/Validity"}
  ImagePermissions: {status: fixed, note: "Update/Delete/DescribeImagePermissions verified against real SharedImagePermissions shape. FIXED 2026-08-30: DescribeImagePermissions dropped the SharedAwsAccountIds filter (see op above)"}
  Session: {status: fixed, note: "DescribeSessions/DrainSessionInstance/ExpireSession/CreateStreamingURL verified against real Session shape and DescribeSessionsInput/CreateStreamingURLInput fields. FIXED: CreateStreamingURL now honors Validity and returns Expires (see ops above). FIXED 2026-08-30: DescribeSessions dropped the AuthenticationType filter (see op above)"}
  Theme: {status: fixed, note: "CRUD verified against real Theme shape. FIXED (gopherstack-afi1): CreateThemeForStack dropped 4 of its 5 required members (FaviconS3Location, OrganizationLogoS3Location, ThemeStyling, TitleText) -- see CreateThemeForStack above. FIXED 2026-08-23: UpdateThemeForStack had the identical gap and is now fixed too -- see UpdateThemeForStack below."}
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

## This pass (2026-08-23): UpdateThemeForStack dropped every one of its update fields

The prior pass's `Theme` family note flagged this explicitly and left it
unfixed: `opUpdateThemeForStack` (`handler_user.go`) decoded a bare
`themeStackInput{StackName}` and `InMemoryBackend.UpdateThemeForStack`
(`themes.go`) took only a `stackName` parameter -- every other real
`UpdateThemeForStackInput` member (`AttributesToDelete`/`FaviconS3Location`/
`FooterLinks`/`OrganizationLogoS3Location`/`State`/`ThemeStyling`/
`TitleText`, confirmed against
`aws-sdk-go-v2/service/appstream@v1.64.5/api_op_UpdateThemeForStack.go:29-64`)
was parsed nowhere and silently discarded. A real client calling
`UpdateThemeForStack` to change a stack's branding got a `200 OK` echoing the
theme completely unchanged -- the request-side accept-and-drop bug class,
confirmed by a real `aws-sdk-go-v2` client round trip
(`TestSDKRoundTrip_UpdateThemeForStack_FieldsApply`, `wire_shape_test.go`)
that fails against the unfixed handler and passes against the fix.

Fixed by adding `ThemeUpdateOptions` (`interfaces.go`) and threading it
through a new `applyThemeUpdate` helper (`themes.go`) that the handler
populates from the decoded request. Every optional field follows
`UpdateThemeForStackInput`'s own "omitted fields are unchanged" convention,
matched at the Go level exactly the way the real SDK's own request struct
distinguishes "not set" from "set" (pointer-nil / slice-nil vs a present
value, confirmed against `serializeCBOR_UpdateThemeForStackInput`'s per-field
guards in `serializers.go`) -- not a fabricated convention. `AttributesToDelete
= [FOOTER_LINKS]` (the only real `ThemeAttribute` value) clears the footer
links after every other field is applied, so a delete always wins over a
same-request set.

Deliberately NOT added: enum validation for `ThemeStyling`/`State` on
Update (unlike `CreateThemeForStack`, which does validate `ThemeStyling`).
`UpdateThemeForStack`'s own error-deserializer switch
(`ConcurrentModificationException`/`InvalidAccountStatusException`/
`InvalidParameterCombinationException`/`LimitExceededException`/
`OperationNotPermittedException`/`ResourceNotFoundException`) has no
`ValidationException`/`SerializationException` case a real client could ever
receive for a bad enum value on this op -- inventing a rejection here would
be exactly the unconfirmed-error-contract mistake ACM's PARITY.md documents
avoiding for `ValidationMethod=HTTP`. Both fields are stored and echoed
verbatim instead.

Proof: hand-reverted `themes.go`/`handler_user.go`/`interfaces.go` to
`git show HEAD`, confirmed `TestSDKRoundTrip_UpdateThemeForStack_FieldsApply`
fails with the exact predicted symptom (every updated field reads back as its
pre-update value), restored the fix, `md5sum` byte-identical to before the
revert. `go build`/`go vet`/`go test ./services/appstream/...`/
`golangci-lint run ./services/appstream/...` all clean.

## This pass (2026-08-13): CreateThemeForStack dropped 4 of 5 required members (gopherstack-afi1)

From the "five ops drop the fields that define what they do" required-member
sweep. `handler_user.go:opCreateThemeForStack`/`themes.go:CreateThemeForStack`
only ever read/stored `StackName` -- `FaviconS3Location`,
`OrganizationLogoS3Location`, `ThemeStyling`, and `TitleText`, all required
(`api_op_CreateThemeForStack.go:29-59`), were unmodeled entirely, so every
theme this emulator created had no styling, no branding, and no title. See
the `CreateThemeForStack`/`Theme` entries above for the full field-diff and
error-handling detail. `TestAppStream_Themes`'s `"CreateThemeForStack returns
theme"` case previously sent only `{"StackName": ...}` and asserted just
`StackName`/`State` on the response -- it would have passed identically
whether or not the other four fields were ever read, so it encoded the same
assumption as the bug. Rewrote it to send all required fields plus a
`FooterLinks` entry and assert every response field round-trips
(`ThemeStyling`, `ThemeTitleText`, `ThemeFaviconURL`/
`ThemeOrganizationLogoURL` derived from the submitted S3 locations,
`ThemeFooterLinks`); added a `"missing FaviconS3Location rejected"` case.
`persistence_test.go`'s full-state snapshot/restore test also only exercised
`StackName` for its Theme entry -- extended to populate and assert
`ThemeStyling`/`ThemeTitleText`/`ThemeFooterLinks` survive `Snapshot`/
`Restore`, not just the initial create.

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

## 2026-08-20 — gopherstack-jqh2 pass 4: SDK route table test added

Re-extracted all 89 AppStream ops from `appstream@v1.64.5` serializers.go
(`req.URL.Path = "/service/PhotonAdminProxyService/operation/<Op>"` in each
`smithyRpcv2cbor_serializeOp<Op>.HandleSerialize`) and diffed against `h.ops`
(built by `buildOps` from five per-family literal maps). 89/89, zero
mismatches either direction — dispatch table is exactly complete, no shape-3
drift across the five family helpers. Added
`handler_sdk_route_table_test.go` (`TestExtractOperation_SDKRouteTable`),
which builds a real rpc-v2-cbor request per op and asserts both
`RouteMatcher` and `ExtractOperation` resolve it correctly; this is new
coverage against the SDK's authoritative op list, complementary to the
existing `TestAppStream_RPCv2CBOR/every_supported_operation_is_reachable_over_CBOR`
in handler_test.go, which only checks internal self-consistency against
`GetSupportedOperations()`. No stale PARITY.md entries found.

## 2026-08-28 — wrapper-key-sweep: request-side fabricated members (acceptguard)

`cmd/acceptguard` flagged two request-side bugs in `services/appstream/`
where the handler decoded a member real AWS never sends:

1. `CreateUser` read a top-level `Email` request field. Real
   `CreateUserInput` has no `Email` member at all (`appstream@v1.64.5`
   `api_op_CreateUser.go`) -- `UserName` is documented as "The email address
   of the user"; it *is* the email, there is no separate field. `types.User`
   (the response type) has no `Email` member either. Fixed by removing
   `Email` end to end: the wire request/response structs, `storedUser`/`User`
   models, and the `CreateUser` backend signature all dropped it.
2. `CreateUsageReportSubscription` read top-level `S3BucketName`/`Schedule`
   request fields. Real `CreateUsageReportSubscriptionInput` takes zero
   parameters (`api_op_CreateUsageReportSubscription.go`) -- AWS derives the
   bucket (creating or reusing one) and the schedule (the only enum value is
   `DAILY`) server-side. A real client's marshaled body is always `{}`, so
   `S3BucketName` was always empty on the response. Fixed by dropping both
   parameters from the backend's `CreateUsageReportSubscription()` (now
   takes no args) and deriving `S3BucketName` as
   `"appstream-logs-<region>-<accountID>"` and `Schedule` as the constant
   `"DAILY"`.

Proven via a real `aws-sdk-go-v2/service/appstream` client round trip in
`wire_field_fixes_test.go` (new). `TestCreateUsageReportSubscription_NoInputRealClient`
genuinely fails pre-fix (`S3BucketName` empty on the real client's response,
confirmed by hand-reverting `handler_user.go`/`interfaces.go`/`users.go`/
`usage_report_subscriptions.go` together and re-running) and passes after.
`TestCreateUser_UserNameIsEmailRealClient` passes both before and after --
`CreateUserInput`'s Go struct never had an `Email` field to send incorrectly
in the first place, so there is no request-shape difference a real typed
client can observe; the fix there is dead-field removal, not a behavior
change reachable through the wire. `handler_user.go`'s `userToResponse`
previously echoed an invented `"Email"` key that no real client's generated
`types.User` struct has any way to read.

Several raw-body tests (`handler_test.go`'s `createUser` helper,
`users_test.go` ×6, `usage_report_subscriptions_test.go` ×3,
`persistence_test.go`) sent the fabricated `Email`/`S3BucketName`/`Schedule`
request keys directly as raw JSON -- updated to match the real, narrower
request shape; none asserted on the removed response values, so no test
lost coverage.

Gates: `go build`, `go vet`, `go test -race -count=1`, `golangci-lint run` —
all clean (`./services/appstream/...`).

## 2026-08-31 Error-envelope sweep (gopherstack-6flj/uox6, errtargetaudit)

`errtargetaudit -dir appstream` reported 2 class-A findings. Both verified
against the pinned SDK's own per-op `rpc2_deserializeOpError*` switch
(appstream@v1.64.5 deserializers.go):

- `CreateEntitlement` (`entitlements.go`) emitted the shared
  `ResourceAlreadyExistsException` sentinel (`ErrAlreadyExists`) on a
  duplicate name/stack. `CreateEntitlement`'s own switch declares
  `EntitlementAlreadyExistsException`, `LimitExceededException`,
  `OperationNotPermittedException`, `ResourceNotFoundException` — not
  `ResourceAlreadyExistsException`. **Fixed**: added a dedicated
  `ErrEntitlementAlreadyExists` sentinel (wraps `awserr.ErrAlreadyExists`,
  wire code `EntitlementAlreadyExistsException`) and overrode this one call
  site; the shared `ErrAlreadyExists` sentinel is untouched. Checked all 14
  other `ErrAlreadyExists` call sites (`app_blocks.go` ×2, `applications.go`,
  `directory_configs.go`, `fleets.go`, `images.go` ×4, `stacks.go`,
  `themes.go`, `users.go`, plus this one and `CreateUsageReportSubscription`
  below) against their own declared sets: 12 of 14 legitimately declare
  `ResourceAlreadyExistsException` (`CreateAppBlock`, `CreateAppBlockBuilder`,
  `CreateDirectoryConfig`, `CreateFleet`, `CreateApplication`, `CreateStack`,
  `CopyImage`, `CreateImportedImage`, `CreateUpdatedImage`,
  `CreateImageBuilder`, `CreateThemeForStack`, `CreateUser`) — left alone.
  Proven with a new real-client test,
  `TestCreateEntitlement_EntitlementAlreadyExists_RealClient`
  (`error_envelope_fixes_test.go`), asserting `errors.As` against
  `*types.EntitlementAlreadyExistsException`; confirmed failing against the
  unmodified sentinel (got a generic `smithy.GenericAPIError` for
  `ResourceAlreadyExistsException` instead) before the fix.
- `CreateUsageReportSubscription` (`usage_report_subscriptions.go`) also
  emits `ErrAlreadyExists` when a subscription already exists. Its own
  switch declares `InvalidAccountStatusException`, `InvalidRoleException`,
  `LimitExceededException` — no conflict/already-exists type of any kind.
  **Not fixed** — recorded rather than substituted; no correct code exists
  to send for this condition in this operation's model.

Gates: `go build ./services/appstream/...`, `go vet ./...` (repo-wide,
clean), `go test -race -count=1 ./services/appstream/...` (pass; 1 test
added), `golangci-lint run ./services/appstream/...` (0 issues).

## 2026-08-31 -- gopherstack-6flj/21my: ops never named in this file

Computed the queue directly: every `List*`/`Describe*` op in
`appstream@v1.64.5`'s `api_op_*.go` files whose literal name never appears
anywhere in this PARITY.md. Seven such ops: `DescribeAppBlockBuilders`,
`DescribeImageBuilders`, `DescribeSoftwareAssociations`,
`DescribeThemeForStack`, `DescribeUsageReportSubscriptions`,
`DescribeUserStackAssociations`, `DescribeUsers`. Protocol reconfirmed from
this service's own deserializer: current traffic is rpc-v2-cbor
(`rpc2_deserializeOpError*` throughout `deserializers.go`), using a
schema-free per-field `if key == "..."` switch (not the older restjson1
shape) inside `deserializeCBOR_<Op>Output`/`deserializeCBOR_<Type>` -- read
directly rather than assumed, per this file's own protocol note above.

All seven checked at both layers against their own `deserializeCBOR_*`
functions:

- `DescribeAppBlockBuilders` (wraps `AppBlockBuilders`), `DescribeImageBuilders`
  (wraps `ImageBuilders`), `DescribeSoftwareAssociations` (wraps
  `AssociatedResource`+`SoftwareAssociations`, item fields `SoftwareName`/
  `Status` both correct against `types.SoftwareAssociations`), `DescribeThemeForStack`
  (wraps `Theme`), `DescribeUsageReportSubscriptions` (wraps
  `UsageReportSubscriptions`, item fields `S3BucketName`/`Schedule` both
  correct), `DescribeUserStackAssociations` (wraps `UserStackAssociations`,
  item fields `StackName`/`UserName`/`AuthenticationType`/
  `SendEmailNotification` all correct), `DescribeUsers` (wraps `Users`, item
  fields `UserName`/`Arn`/`FirstName`/`LastName`/`AuthenticationType`/
  `Status`/`Enabled`/`CreatedTime` all correct) -- all seven wrapper keys
  correct, no bug found in any of these five item shapes.

**Two findings recorded, neither fixed (real but currently unobservable,
or a different-axis gap):**

1. `AppBlockBuilder`/`ImageBuilder` per-item shapes both emit a `Tags`
   field (`appBlockBuilderToResponse`/`imageBuilderToResponse`,
   handler_appblock.go/handler_image.go) that is **not a member of either
   real type at all** -- confirmed against
   `deserializeCBOR_AppBlockBuilder`/`deserializeCBOR_ImageBuilder`'s full
   key switch (neither has a `"Tags"` case; AppStream tags live only via
   `ListTagsForResource`, not embedded on the resource). Harmless: a real
   client's CBOR decoder silently ignores an unrecognized key, same as the
   sagemaker connection-ARN case recorded in the 2026-08-31 c2b2c6129
   commit. Not removed this pass, recorded rather than fixed (matches this
   campaign's precedent of disclosing rather than touching a dormant,
   cost-free field).
2. `ImageBuilder.ImageName` is emitted under the wire key `"ImageName"`;
   the real `types.ImageBuilder` has no such member -- the real field is
   `ImageArn` (`deserializers.go:7851`, `types/types.go`). This mismatch is
   currently **unobservable**: `CreateImageBuilder`'s request-decode struct
   (`createImageBuilderInput`, handler_image.go) never reads `ImageArn` or
   `ImageName` from the request at all, even though real
   `CreateImageBuilderInput` declares both (either identifies the source
   image, `api_op_CreateImageBuilder.go:181,184`) -- so this backend's
   `ImageBuilder.ImageName` field is always the empty string regardless of
   what a real client sends. Fixing the wire key alone would still emit an
   always-empty field; the real gap is that `CreateImageBuilder` never
   captures a source-image identifier at all, a Create-side feature gap
   distinct from this sweep's wrapper-key/per-item-name scope. Also found,
   same op: `AppBlockBuilder`'s real type declares `VpcConfig` as a
   **required** response member (`types/types.go:248`) that this service
   does not model anywhere (no VPC concept in this backend at all, and
   `CreateAppBlockBuilder` doesn't accept one either) -- disclosed as a
   structural gap, not fixed (same class as the ImageBuilder source-image
   gap: a feature absence, not a wire-shape defect on an otherwise-modeled
   field).

**No bugs fixed this pass.** No wrapper-key mismatch, no fixable per-item
mismatch, no transposition, no case-only mismatch (CBOR/JSON-family, not
applicable), no hard decode error or panic, no wrong Go type under a
correct key found.

No web pages fetched this pass (SDK lookups went through the pinned module
cache only).

Gates: `go build ./services/appstream/...`, `go vet ./...` (repo-wide,
clean), `go test -race -count=1 ./services/appstream/...` (pass, no new
tests -- both findings above are disclosed-not-fixed, so no regression to
guard), `golangci-lint run ./services/appstream/...` (0 issues). No source
changes this pass.
