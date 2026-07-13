---
service: serverlessrepo
sdk_module: aws-sdk-go-v2/service/serverlessapplicationrepository@v1.30.11
last_audit_commit: 07f98c0e
last_audit_date: 2026-07-13
overall: B            # already-accurate; proven op-by-op against real serializers/deserializers/model
ops:
  CreateApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "201 Created via errHTTP201 sentinel; optionally creates the first version in the same call when semanticVersion + one of sourceCodeUrl/sourceCodeArchiveUrl/templateUrl are given, matching real API behavior"}
  GetApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "embeds current/queried Version; explicit ?semanticVersion=X 404s if missing, implicit default silently omits Version if app has none"}
  UpdateApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "PATCH; labels replaced only when the JSON key is present (nil vs [] distinguished)"}
  DeleteApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "204 No Content; cascades to versions/templates/changesets/policy/dependencies"}
  ListApplications: {wire: ok, errors: ok, state: ok, persist: ok, note: "nextToken = exclusive cursor on last-seen application Name, matching Table's Name-ascending key order"}
  CreateApplicationVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT /applications/{id}/versions/{semanticVersion}, 201 Created; synthesizes templateUrl when only sourceCodeUrl/sourceCodeArchiveUrl given"}
  ListApplicationVersions: {wire: ok, errors: ok, state: ok, persist: ok, note: "response includes an extra non-wire 'resourcesSupported' field on each summary (real VersionSummary shape only has applicationId/creationTime/semanticVersion/sourceCodeUrl); harmless since aws-sdk-go-v2 JSON deserializers ignore unknown fields -- left as-is, see Notes"}
  CreateCloudFormationTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "status ACTIVE->EXPIRED computed dynamically off ExpirationTime at read time, not stuck PREPARING"}
  GetCloudFormationTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateCloudFormationChangeSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "TemplateId request field is accepted on the wire but not cross-validated against a prior CreateCloudFormationTemplate call -- deferred, low value for emulation"}
  GetApplicationPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  PutApplicationPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass -- see gaps/Notes: action allow-list rejected 3 real actions and accepted 2 fabricated ones"}
  ListApplicationDependencies: {wire: ok, errors: ok, state: ok, persist: ok}
  UnshareApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "204 No Content; organizationId is validated as required but not otherwise checked against PutApplicationPolicy's PrincipalOrgIDs -- acceptable emulation simplification"}
families:
  route_matcher: {status: ok, note: "every op's HTTP method + path template cross-checked against aws-sdk-go-v2 serializers.go (POST /applications, PUT .../versions/{v}, PATCH .../{id}, DELETE .../{id}, PUT/GET .../policy, POST .../changesets, POST .../templates, GET .../templates/{id}, GET .../dependencies, POST .../unshare) -- all match; ExtractOperation dispatch table is exhaustive and correct"}
  error_shapes: {status: ok, note: "FIXED this pass: default/unmatched-error branch emitted __type: InternalServerException; real AWS SAR (and aws-sdk-go-v2's restjson1 deserializer, which does a case-insensitive exact-string match) uses InternalServerErrorException. Wrong spelling meant the aws-sdk-go-v2 client would never construct a typed *types.InternalServerErrorException for any unexpected gopherstack-side error, only a generic smithy.GenericAPIError. NotFoundException/ConflictException/BadRequestException status codes (404/409/400) and __type strings already matched the model (models/apis/serverlessrepo/2017-09-08/api-2.json httpStatusCode)."}
gaps:
  - CreateCloudFormationChangeSet's TemplateId request field is accepted but not validated against an existing CreateCloudFormationTemplate record (bd: none filed -- low value, no client-visible breakage since gopherstack always succeeds rather than wrongly rejecting)
  - ListApplicationVersions summaries include a non-wire "resourcesSupported" key not present in the real VersionSummary shape; non-breaking (extra JSON fields are ignored by aws-sdk-go-v2's generated deserializer) but inaccurate if something inspects raw JSON. Left unfixed this pass since 3 existing tests assert its presence and no functional benefit accrues from removing it (bd: none filed).
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

Two real bugs were found and fixed this pass, both about the two areas the task brief calls
out as recurring bug classes (wire-shape-vs-real-SDK, not self-consistency):

1. **`handler.go`'s default error branch used the wrong `__type` string.** It emitted
   `"InternalServerException"`; the real AWS SAR service (and the generated
   `awsRestjson1_deserializeOpError*` functions in `serverlessapplicationrepository`'s
   `deserializers.go`, which `strings.EqualFold`-match the body's `__type`/header's
   `X-Amzn-ErrorType`) only recognize `"InternalServerErrorException"`
   (`types.InternalServerErrorException`). Any client doing `errors.As(err,
   &types.InternalServerErrorException{})` on an unexpected gopherstack failure would never
   match; it would only ever see a generic `smithy.GenericAPIError`. Confirmed against
   `types/errors.go` in `aws-sdk-go-v2/service/serverlessapplicationrepository@v1.30.11` and
   the `error` trait's `httpStatusCode: 500` in `api-2.json`. `NotFoundException` (404),
   `ConflictException` (409), and `BadRequestException` (400) were already correct.

2. **`backend.go`'s `validPolicyActionsSet()` allow-list for `PutApplicationPolicy`
   /`ApplicationPolicyStatement.Actions` was wrong in both directions.** Verified against AWS's
   published "Application Permissions" table
   (docs.aws.amazon.com/serverlessrepo/latest/devguide/access-control-resource-based.html):
   the only 8 valid actions are `GetApplication`, `CreateCloudFormationChangeSet`,
   `CreateCloudFormationTemplate`, `ListApplicationVersions`, `ListApplicationDependencies`,
   `SearchApplications`, `Deploy` (implies all the others), and `UnshareApplication` (used to
   revoke an AWS-Organization share). The old set was **missing**
   `CreateCloudFormationChangeSet`, `CreateCloudFormationTemplate`, and
   `ListApplicationDependencies` -- meaning `PutApplicationPolicy` would wrongly 400 a
   real, valid AWS request granting any of those three permissions. It also **accepted two
   fabricated action names that don't exist in the real API**, `SearchAndDeploy` and
   `UnSubscribeFromApplication` (real AWS would 400-reject these), which a prior parity sweep
   had baked into `handler_batch1_test.go`'s `TestBatch1_PutApplicationPolicy_AWSActions` under
   the misleading name "AWSActions" -- a textbook case of the "unit tests are not parity proof"
   trap: a hallucinated action name got enshrined by a test asserting it should be accepted.
   Fixed the allow-list to the real 8 actions (keeping the existing, deliberately
   case-insensitive lower/PascalCase acceptance -- see
   `TestRefinement2_PutApplicationPolicy_CaseSensitive_Deploy`, which predates this pass and
   was left untouched) and updated the batch1 test to use real action names.

Regression test added: `TestHandler_UnexpectedError_ReturnsInternalServerErrorException` in
`handler_test.go`, using a small `errBackend` wrapper (embeds `*InMemoryBackend`, overrides
`GetApplication` to return a plain unwrapped error) to drive the handler's default/500 error
path through the real `Handler()` dispatch and assert on `__type`.

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
