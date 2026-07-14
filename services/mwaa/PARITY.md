---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: mwaa
sdk_module: aws-sdk-go-v2/service/mwaa@v1.40.1   # version audited against (go.mod pins this)
last_audit_commit: e15f163e                      # HEAD when this manifest was written
last_audit_date: 2026-07-13
overall: A                # genuine fixes found, incl. one functionality-breaking wire bug
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateEnvironment: {wire: ok, errors: ok, state: ok, persist: ok, note: "duplicate-name conflict fixed to ValidationException/400 (was fabricated AlreadyExistsException/409)"}
  GetEnvironment: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateEnvironment: {wire: ok, errors: ok, state: ok, persist: ok, note: "NetworkConfiguration wire-shape bug fixed: AWS's update shape (UpdateNetworkConfigurationInput) has no SubnetIds member; gopherstack previously required it and always overwrote the whole NetworkConfiguration, which (a) rejected every real security-group-only update, and (b) let SubnetIds appear editable when AWS forbids it"}
  DeleteEnvironment: {wire: ok, errors: ok, state: ok, persist: ok, note: "response body no longer echoes Arn; AWS returns an empty 200 body for this op"}
  ListEnvironments: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateCliToken: {wire: ok, errors: ok, state: ok, persist: n/a, note: "token store is derived/stateless; ResourceNotFoundException for non-AVAILABLE env matches AWS's documented single error"}
  CreateWebLoginToken: {wire: partial, errors: ok, state: ok, persist: n/a, note: "AirflowIdentity/IamIdentity response fields are not populated (see gaps) -- everything else matches"}
  InvokeRestApi: {wire: partial, errors: ok, state: ok, persist: n/a, note: "Method now validated against GET/PUT/POST/PATCH/DELETE enum; response is always a synthesized 200 (see gaps)"}
  PublishMetrics: {wire: ok, errors: ok, state: ok, persist: ok}
  GetMetrics: {wire: n/a, errors: n/a, state: ok, persist: ok, note: "NOT a real AWS MWAA operation -- see Notes"}
families:
  environment_lifecycle: {status: ok, note: "CREATING/UPDATING transiently promote to AVAILABLE on next GetEnvironment observation (promoteTransientStatus); this is a deliberate mock simplification, not a stuck-forever bug"}
  errors: {status: ok, note: "audited every writeErrorResponse call site against the real MWAA exception set (AccessDeniedException, InternalServerException, ResourceNotFoundException, RestApiClientException, RestApiServerException, ServiceUnavailableException, ValidationException -- confirmed via aws-sdk-go-v2/service/mwaa@v1.40.1/types/errors.go, the union of every op's declared errors). Fabricated 'AlreadyExistsException' and 'BadRequestException' names removed; both now map to the real ValidationException/400."}
persistence: {status: ok, note: "persistence.go Snapshot/Restore predates this pass; verified it still round-trips NetworkConfiguration/UpdateNetworkConfig-affected fields correctly after the Update fix (TestAudit_NetworkConfig_UpdatePersisted, TestAudit_Snapshot_WithNetworkConfig)"}
gaps:
  - CreateWebLoginToken does not populate AirflowIdentity/IamIdentity (real AWS returns the calling IAM identity's username/ARN). No caller-identity extraction helper exists in pkgs/ or is derivable within services/mwaa alone (STS's assumed-role/session tracking lives in services/sts and is out of this audit's edit scope); populating these with fabricated values would violate the no-fabricated-data rule, so they are left absent rather than invented. Candidate follow-up: a shared pkgs/ helper that derives caller identity from the SigV4 Authorization header, usable by any service that needs it.
  - CreateEnvironment does not enforce NetworkConfiguration as a required field, nor SubnetIds-must-be-exactly-2 / SecurityGroupIds-must-be-1..5 bounds documented for AWS's CreateEnvironment+NetworkConfiguration shapes. Confirmed via docs.aws.amazon.com/mwaa/latest/API/API_CreateEnvironment.html ("NetworkConfiguration ... Required: Yes") and API_NetworkConfiguration.html (SubnetIds "Fixed number: 2", SecurityGroupIds "1-5"). Not fixed this pass: ~10+ existing tests across audit_batch1/2_test.go and backend_test.go construct create requests with 0 or 1 subnet and rely on today's lenient behavior; tightening this needs a coordinated test sweep. gopherstack is a strict superset of valid inputs here (permissive, not incorrect-output), so it was deprioritized behind the Update wire-shape bug (which actively breaks a legitimate real-client call). Tracked for a follow-up bd issue.
  - InvokeRestApi always synthesizes a 200 success with an empty RestApiResponse regardless of the fabricated Path/Method; it never returns RestApiClientException/RestApiServerException (which real AWS returns when the underlying Airflow REST call itself 4xx/5xx's). Faithfully emulating arbitrary Airflow REST API behavior per-path is out of scope for this pass; documented here rather than silently left as a "looks real" trap for the next auditor.
  - MethodNotAllowedException (405) is used for HTTP-verb mismatches on matched MWAA path prefixes (e.g. GET /clitoken/{name}). This exception name is not part of the real MWAA API model, but the code path is unreachable by any conformant aws-sdk-go-v2 client (which always sends the correct verb per operation), so it was left as-is rather than spending fix budget on a case no real client can trigger.
deferred:
  - Chaos/fault-injection interaction with the new ValidationException mappings (not re-audited this pass; ChaosOperations() surface unchanged).
leaks: {status: clean, note: "no goroutines/janitors in this service; existing leak_test.go/isolation_test.go untouched and still green"}
---

## Notes

- **Protocol**: restjson1. Route prefixes verified against aws-sdk-go-v2/service/mwaa@v1.40.1
  serializers.go for every op: `/environments` (POST-less; GET=List), `/environments/{Name}`
  (GET/PUT/DELETE/PATCH = Get/Create/Delete/Update), `/clitoken/{Name}` (POST),
  `/webtoken/{Name}` (POST -- note the real wire path is `/webtoken/`, NOT
  `/weblogintoken/` despite the operation being named CreateWebLoginToken; gopherstack's
  `pathWebTokenPrefix = "/webtoken/"` is correct, don't "fix" this on a future pass),
  `/restapi/{Name}` (POST), `/tags/{ResourceArn}` (GET/POST/DELETE =
  List/Tag/Untag), `/metrics/environments/{EnvironmentName}` (POST=PublishMetrics).
  All prefixes and HTTP methods in handler.go's RouteMatcher/ExtractOperation/ServeHTTP
  matched the real serializers exactly -- no route-matcher bugs found this pass.

- **Timestamps**: `Environment.CreatedAt` and `LastUpdate.CreatedAt` are wire-correct as
  epoch-seconds JSON numbers (`float64` via `epochSecondsNow()`), confirmed against
  `deserializers.go`'s `smithytime.ParseEpochSeconds(f64)` for both fields. This predates
  this audit pass and required no changes.

- **The standout bug this pass**: `UpdateEnvironment`'s `NetworkConfiguration` field was
  typed identically to `CreateEnvironment`'s (`*NetworkConfig`, carrying both `SubnetIds`
  and `SecurityGroupIds`), and validation required both to be non-empty. But AWS's real
  `UpdateEnvironmentInput.NetworkConfiguration` is a *different* shape
  (`UpdateNetworkConfigurationInput`) that has **no `SubnetIds` member at all** -- subnets
  are immutable after environment creation; only `SecurityGroupIds` can be changed via
  Update. Consequently, a real aws-sdk-go-v2 client calling `UpdateEnvironment` to rotate
  security groups (a common, legitimate operation) would never serialize `SubnetIds` on
  the wire, and gopherstack would deterministically reject the call with "SubnetIds must
  not be empty" -- a real client-facing functional break, not just a cosmetic wire mismatch.
  Fixed by giving Update its own `UpdateNetworkConfig` type and merging just
  `SecurityGroupIds` into the existing stored `NetworkConfiguration` rather than replacing
  it wholesale (which also fixes a second latent bug: Update could previously silently
  overwrite `SubnetIds` with attacker/caller-supplied values, which real AWS does not allow).

- **Error taxonomy**: confirmed via `aws-sdk-go-v2/service/mwaa@v1.40.1/types/errors.go`
  (the codegen'd union of every operation's declared exceptions) that MWAA's *entire*
  API model has exactly 7 exception types: `AccessDeniedException`,
  `InternalServerException`, `ResourceNotFoundException`, `RestApiClientException`,
  `RestApiServerException`, `ServiceUnavailableException`, `ValidationException`. There is
  **no `AlreadyExistsException`** anywhere in the model -- confirmed independently via the
  live API docs for `CreateEnvironment`, whose only documented errors are
  `InternalServerException`/`ServiceUnavailableException`/`ValidationException`. A real
  duplicate-name create is therefore a 400 `ValidationException`, not a 409. gopherstack
  previously fabricated both `AlreadyExistsException` (409) and `BadRequestException`
  (400, for malformed-body cases); both are now mapped to the real `ValidationException`.

- **DeleteEnvironment**'s success response is a genuinely empty body per the live API docs
  ("HTTP/1.1 200" with no response elements listed) -- gopherstack was echoing `{"Arn":
  ...}` like Create/Update do. Harmless for real clients (smithy-go's per-op deserializers
  ignore unknown JSON keys), but fixed for wire-shape honesty since the task explicitly
  asked for it. `TagResource`/`UntagResource` already correctly wrote an empty `{}` body
  and needed no changes.

- **GetMetrics** (`GET /metrics/environments/{Name}`) is **not a real AWS MWAA operation**
  -- only `PublishMetrics` (POST) exists in the real API surface (it's documented as
  "internal use only", used by the Airflow environment itself to push metrics to
  CloudWatch). gopherstack's `GetMetrics` appears to be a test-observability extension
  invented to let integration tests assert what was published. It is harmless: no real
  `aws-sdk-go-v2` client will ever issue a GET to this path, so there is no wire-parity
  risk. Left as-is; noted here so the next auditor doesn't mistake it for a stub of a real
  op (there is no real op to compare it against).
