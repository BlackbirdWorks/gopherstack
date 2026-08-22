---
service: serverlessrepo
sdk_module: aws-sdk-go-v2/service/serverlessapplicationrepository@v1.33.4
last_audit_commit: e98f13133
last_audit_date: 2026-07-24
overall: A            # zero known gaps; every op field-diffed against real serializers/deserializers/model this pass
ops:
  CreateApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "201 Created via errHTTP201 sentinel; optionally creates the first version in the same call when semanticVersion + one of sourceCodeUrl/sourceCodeArchiveUrl/templateUrl are given, matching real API behavior. FIXED this pass: licenseBody/readmeBody/templateBody were silently dropped (unmarshaled into no struct field) -- see Notes"}
  GetApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "embeds current/queried Version; explicit ?semanticVersion=X 404s if missing, implicit default silently omits Version if app has none"}
  UpdateApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "PATCH; labels replaced only when the JSON key is present (nil vs [] distinguished). FIXED this pass: readmeBody was silently dropped -- see Notes"}
  DeleteApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "204 No Content; cascades to versions/templates/changesets/policy/dependencies"}
  ListApplications: {wire: ok, errors: ok, state: ok, persist: ok, note: "nextToken = exclusive cursor on last-seen application Name, matching Table's Name-ascending key order"}
  CreateApplicationVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT /applications/{id}/versions/{semanticVersion}, 201 Created; synthesizes templateUrl when only sourceCodeUrl/sourceCodeArchiveUrl given. FIXED this pass: templateBody was silently dropped -- see Notes"}
  ListApplicationVersions: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: summaries no longer include the invented non-wire 'resourcesSupported' key -- real VersionSummary shape is exactly applicationId/creationTime/semanticVersion/sourceCodeUrl, see Notes"}
  CreateCloudFormationTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "status ACTIVE->EXPIRED computed dynamically off ExpirationTime at read time, not stuck PREPARING"}
  GetCloudFormationTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateCloudFormationChangeSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: TemplateId request field was parsed but never passed to the backend, so it was accepted without ever being cross-validated against a prior CreateCloudFormationTemplate call -- now 404s (NotFoundException) on an unknown or wrong-application templateId, see Notes"}
  GetApplicationPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  PutApplicationPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "action allow-list matches the real 8 documented SAR policy actions (fixed in a prior pass)"}
  ListApplicationDependencies: {wire: ok, errors: ok, state: ok, persist: ok}
  UnshareApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "204 No Content; organizationId is validated as required but not otherwise checked against PutApplicationPolicy's PrincipalOrgIDs -- acceptable emulation simplification"}
families:
  route_matcher: {status: ok, note: "every op's HTTP method + path template cross-checked against aws-sdk-go-v2 serializers.go (POST /applications, PUT .../versions/{v}, PATCH .../{id}, DELETE .../{id}, PUT/GET .../policy, POST .../changesets, POST .../templates, GET .../templates/{id}, GET .../dependencies, POST .../unshare) -- all match; ExtractOperation dispatch table is exhaustive and correct"}
  error_shapes: {status: ok, note: "BadRequestException(400)/ConflictException(409)/NotFoundException(404)/InternalServerErrorException(500) __type strings and status codes verified against types/errors.go and api-2.json httpStatusCode traits. ForbiddenException(403)/TooManyRequestsException(429) are declared on every real operation but intentionally unimplemented: gopherstack has no IAM-authorization or rate-limiting subsystem to derive them from (no other service in this codebase synthesizes these either), so there is no state to key a 403/429 off of; this is a systemic emulator scope decision, not a service-specific gap."}
gaps: []
deferred: []
leaks: {status: clean, note: "coarse lockmetrics.RWMutex guards all backend maps; store.Table/Index used throughout (no raw sync.Mutex, no per-map locks); Snapshot/Restore round-trip all state including the 3 dirty tables (appVersions/cfTemplates/cfChangeSets) via an ephemeral DTO registry and the 2 plain maps (appPolicies/appDependencies) directly"}
---

## Notes

Protocol: **restjson1** (`aws-sdk-go-v2/service/serverlessapplicationrepository`, generated
from `models/apis/serverlessrepo/2017-09-08/api-2.json` in `aws-sdk-go@v1.55.5`). All
timestamp fields (`creationTime`, `expirationTime`) are modeled as plain `string`, not a
`timestamp` shape -- there is no epoch-vs-ISO8601 wire trap here the way there is in
JSON-1.0/1.1 services; gopherstack's `isoTimestamp` (RFC3339 UTC) is a reasonable, real-AWS-
compatible string format and does not need `pkgs/awstime.Epoch`.

### This pass (2026-07-24)

Four real field-diff bugs were found and fixed, all wire-shape-vs-real-SDK (not
self-consistency) issues per `models.go`/handler request structs vs.
`aws-sdk-go-v2/service/serverlessapplicationrepository`'s generated `serializers.go`/`types`:

1. **`CreateApplication`/`CreateApplicationVersion`/`UpdateApplication` silently dropped the
   `licenseBody`/`readmeBody`/`templateBody` wire fields.** The real
   `CreateApplicationInput`/`CreateApplicationVersionInput`/`UpdateApplicationInput` types all
   serialize these as raw JSON string fields (confirmed in `serializers.go`'s
   `awsRestjson1_serializeOpDocument*Input` functions: `object.Key("licenseBody")`,
   `object.Key("readmeBody")`, `object.Key("templateBody")`) alongside their `*Url`
   counterparts, with the AWS doc stating "You can specify only one of X and Y; otherwise, an
   error results." gopherstack's request DTOs had no field to receive them at all, so a real
   SDK client passing `LicenseBody`/`ReadmeBody`/`TemplateBody` (as opposed to a pre-uploaded
   `*Url`) had that content silently discarded by `json.Unmarshal` (unknown JSON keys are
   ignored) -- the resulting application/version would end up with an empty `licenseUrl`/
   `readmeUrl`/`templateUrl` instead of the URL real AWS generates after uploading the body
   content to S3. Fixed by adding the three `*Body` fields to `createApplicationRequest`,
   `createApplicationVersionRequest`, and `updateApplicationRequest`; added mutual-exclusivity
   validation (`ErrValidation` / 400) matching the documented constraint; and added
   `synthesizeLicenseURL`/`synthesizeReadmeURL`/`synthesizeTemplateURL` (`models.go`) to
   produce a deterministic S3-style URL when only the `*Body` form is given, mirroring the
   emulation convention gopherstack already used for deriving `templateUrl` from a bare
   `sourceCodeUrl`.

2. **`ListApplicationVersions` summaries emitted an invented `resourcesSupported` field.** The
   real `VersionSummary` shape (`types.VersionSummary`) has exactly four fields --
   `applicationId`, `creationTime`, `semanticVersion`, `sourceCodeUrl` -- and does **not**
   include `resourcesSupported` (that field exists only on the full `Version` shape returned by
   `GetApplication`/`CreateApplication`/`CreateApplicationVersion`, which gopherstack still
   emits correctly). A prior pass identified this and left it in place because 3 existing tests
   asserted its presence -- exactly the "unit tests are not parity proof" trap the project's
   `parity-principles.md` calls out. Per this task's explicit invented-field rule, removed the
   key from `handleListApplicationVersions`'s summary map and rewrote
   `TestListApplicationVersions_ResourcesSupported` as
   `TestListApplicationVersions_SummaryShape`, which now asserts the field is **absent**.

3. **`CreateCloudFormationChangeSet`'s `templateId` was parsed but never forwarded to the
   backend**, so it was accepted on the wire yet had zero effect -- not even the "accepted
   without validation" behavior the prior audit's gap note described. Added `TemplateID` to
   `CreateCloudFormationChangeSetOptions`, wired it through from
   `handleCreateCloudFormationChangeSet`, and added real cross-validation in
   `CreateCloudFormationChangeSetWithOptions`: an unknown `templateId`, or one belonging to a
   different application, now 404s (`NotFoundException`) instead of being silently accepted.

4. **`ParameterDefinition` was missing 6 of 13 real fields** (`AllowedPattern`,
   `ConstraintDescription`, `MaxLength`, `MaxValue`, `MinLength`, `MinValue` vs.
   `types.ParameterDefinition`). Added them for full field-accuracy; functionally inert today
   since gopherstack never derives non-empty parameter definitions (that requires parsing an
   AWS SAM template body, out of scope), but the shape is now correct for any caller seeding
   state directly.

Carried forward from the prior pass (2026-07-13), still verified correct:

- `handler.go`'s default error branch emits `"InternalServerErrorException"` (not the
  fabricated `"InternalServerException"`), matching `types.InternalServerErrorException` and
  the `error` trait's `httpStatusCode: 500` in `api-2.json`. Regression test:
  `TestHandler_UnexpectedError_ReturnsInternalServerErrorException` in `handler_test.go`.
- `validPolicyActionsSet()`'s 8-action allow-list for `PutApplicationPolicy` matches AWS's
  published "Application Permissions" table exactly (`GetApplication`,
  `CreateCloudFormationChangeSet`, `CreateCloudFormationTemplate`, `ListApplicationVersions`,
  `ListApplicationDependencies`, `SearchApplications`, `Deploy`, `UnshareApplication`).

"Looks-wrong-but-correct" traps for the next auditor:
- The `AppName` field on `ApplicationVersion`/`CloudFormationTemplate`/`CloudFormationChangeSet`
  is `json:"-"` and exists purely to key/index the flattened `store.Table`s (see
  `store_setup.go`'s file doc comment) -- it is intentionally absent from wire responses.
- `RouteMatcher()` gates only on SigV4 service name + `/applications` path prefix (not
  method); `ExtractOperation()` is what actually derives the operation from method + path
  depth, and uses `URL.RawPath` (falling back to `URL.Path`) specifically so ARN-form
  application IDs containing a literal `/` (percent-encoded as `%2F`) route correctly --
  this is intentional, not a routing bug.
- `GetApplicationPolicy`/`ListApplicationDependencies`/`ListApplications` etc. all
  deliberately return non-nil empty slices/maps (never `null`) to match AWS always returning
  `[]`/`{}` for empty collections.
- `ParameterDefinition` (`models.go`) is field-complete against `types.ParameterDefinition` but
  is never populated with non-empty values by any code path -- gopherstack does not parse AWS
  SAM template bodies, so `ParameterDefinitions` on every `Version`/`ApplicationVersion` is
  always `[]`. This is intentional scope, not a stub: the field is still real and always
  present (never omitted) on the wire, exactly matching what AWS returns for an application
  with no template-declared parameters.
- `synthesizeLicenseURL`/`synthesizeReadmeURL`/`synthesizeTemplateURL` (`models.go`) produce
  deterministic, not random, S3-style URLs from the caller-supplied name/semanticVersion. Real
  AWS generates opaque, unpredictable S3 keys for uploaded `*Body` content; gopherstack's
  determinism is an intentional emulation simplification (stable, greppable URLs in tests/
  snapshots) and is not meant to byte-for-byte match a real AWS-generated URL.

## gopherstack-o7gx (2026-08-22): ReadBody-failure path wrote untyped errors

`Handler()`'s `httputils.ReadBody` failure branch wrote a bare
`c.String(http.StatusInternalServerError, "internal server error")`.
serverlessrepo (SDK package name
`serverlessapplicationrepository`) is restjson1 (confirmed from
`serverlessapplicationrepository@v1.33.4` deserializers.go's
`awsRestjson1_deserializeOpError*` prefix); plain text doesn't decode
through `aws/protocol/restjson.GetErrorInfo`, so a real client got
`*json.SyntaxError`, not even `UnknownError`.

Fixed by routing the ReadBody error through this handler's own
`handleError(c, err)`: none of its typed `case`s (`awserr.ErrNotFound`,
`awserr.ErrConflict`, `awserr.ErrInvalidParameter`, `errInvalidRequest`,
`errUnknownAction`, syntax/type errors) match a `*http.MaxBytesError`/read
error, so it falls through to the pre-existing default -- already
documented in this file's own comment as matching the real
`InternalServerErrorException` `__type`
(`serverlessapplicationrepository@v1.33.4` `types/errors.go:105`).

Proven with a real `aws-sdk-go-v2/service/serverlessapplicationrepository`
client's `CreateApplication`, whose `Description` field alone exceeds
`httputils.MaxRequestBodyBytes` (16 MiB).
`TestHandler_OversizedBodySurfacesInternalServerErrorException`
(`handler_oversized_body_test.go`) asserts `apiErr.ErrorCode() ==
"InternalServerErrorException"`; confirmed it fails pre-fix with
`*json.SyntaxError` (hand-reverted, byte-identical restore after).
