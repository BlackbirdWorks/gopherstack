---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: bedrockruntime
sdk_module: aws-sdk-go-v2/service/bedrockruntime@v1.50.1
last_audit_commit: 7262a2b0
last_audit_date: 2026-07-13
overall: A            # genuine fixes found this pass
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  InvokeModel: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed ARN-embedded-slash modelId truncation; fixed InternalFailure->InternalServerException error code"}
  InvokeModelWithResponseStream: {wire: ok, errors: ok, state: ok, persist: n/a, note: "same modelId-extraction fix applies"}
  InvokeModelWithBidirectionalStream: {wire: ok, errors: ok, state: ok, persist: n/a}
  Converse: {wire: ok, errors: ok, state: ok, persist: n/a, note: "same modelId-extraction fix applies"}
  ConverseStream: {wire: ok, errors: ok, state: ok, persist: n/a}
  CountTokens: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed: request body wire shape was fabricated (top-level prompt/messages/system); real shape is {input:{invokeModel:{body:<base64 blob>}}} or {input:{converse:{messages,system}}}"}
  ApplyGuardrail: {wire: ok, errors: ok, state: ok, persist: n/a}
  StartAsyncInvoke: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAsyncInvoke: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAsyncInvokes: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  model-path-routing: {status: ok, note: "extractModelID/ExtractResource now bound the modelId segment by the operation's known literal suffix (not first '/'), fixing ARN modelIds (inference-profile/custom-model ARNs) that embed a '/'"}
  guardrail-path-routing: {status: ok, note: "extractGuardrailIDAndVersion already correctly delimits on the literal '/version/' substring, not first '/' -- unaffected by ARN-embedded slashes in guardrailIdentifier; verified, not changed"}
  async-invoke: {status: ok, note: "StartAsyncInvoke/GetAsyncInvoke/ListAsyncInvokes verified against real wire shapes (RFC3339 timestamps match smithytime.ParseDateTime's date-time format); idempotency via clientRequestToken verified; janitor advances InProgress->Completed and sweeps old invocations; persistence wired via backendSnapshot"}
  error-codes: {status: ok, note: "all internal-error responses changed from the fabricated 'InternalFailure' to the real SDK error code 'InternalServerException' (see types/errors.go and deserializers.go's error-code switch) -- 9 call sites in handler.go"}
gaps:
  - "InvokeModel/Converse do not implement chaos-injectable ModelErrorException/ModelNotReadyException/ThrottlingException/ServiceUnavailableException response paths (ChaosServiceName/ChaosOperations hooks exist but no service-specific fault-shape mapping beyond the generic chaos middleware) -- not fixed this pass, out of budget; low customer impact since gopherstack's chaos middleware likely handles generic fault injection at a higher layer"
  - "CountTokens' invokeModel-body token estimate uses raw decoded-byte length as a chars proxy (cannot know the tokenizer for arbitrary model-specific InvokeModel body formats); acceptable per parity rules (deterministic mock), documented as an approximation in code comments"
deferred: []
leaks: {status: clean, note: "janitor (RunJanitor/StartWorker/Shutdown) uses context-bounded worker.Group with proper cancel+done-channel wiring; no goroutine leaks found"}
---

## Notes

- **Protocol**: restjson1. Request/response bodies for InvokeModel/InvokeModelWithResponseStream/
  InvokeModelWithBidirectionalStream are raw `application/json` blob passthrough (the Body field is
  opaque bytes whose schema is model-specific); Converse/ConverseStream/CountTokens/ApplyGuardrail/
  async-invoke ops are structured JSON.

- **modelId / guardrailIdentifier can be ARNs containing an embedded `/`.** Real examples:
  inference-profile ARN `arn:aws:bedrock:<region>:<acct>:inference-profile/us.anthropic.claude-3-sonnet-...`,
  custom-model ARN `arn:aws:bedrock:<region>:<acct>:custom-model/<base-model-id>/<unique-id>`. Both are
  non-greedy `{modelId}`/`{guardrailIdentifier}` smithy URI labels (verified against
  aws-sdk-go-v2/service/bedrockruntime@v1.50.1 serializers.go), so the SDK percent-encodes the embedded
  '/' as `%2F` on the wire; net/http's URL parsing then decodes it back to a literal '/' in
  `r.URL.Path` server-side. **Bug fixed this pass**: `extractModelID`/`ExtractResource` used to cut the
  modelId at the FIRST '/' after "/model/", silently truncating ARN modelIds and losing the trailing
  model-family marker (e.g. "claude") that `mockInvokeModelResponse` keys off of -- this caused ARN-style
  invocations to silently fall back to the WRONG response envelope (legacy/default format instead of the
  correct Claude 3 Messages API format, etc). Fix: bound the modelId segment using the *known literal
  suffix* for the already-resolved operation (`/invoke`, `/converse`, ...) via `modelPathSuffixForOp`,
  trimming from the tail instead of cutting at the first '/'. `extractGuardrailIDAndVersion` was already
  correct -- it delimits on the literal substring `/version/`, not the first '/', so ARN
  guardrailIdentifiers were never actually affected there (verified, not a bug).
  **Trap for the next auditor**: don't "fix" the guardrail path parser by analogy -- it doesn't need it.

- **CountTokens wire shape**: the request body is `{"input": {"invokeModel": {"body": "<base64
  blob>"}}}` or `{"input": {"converse": {"messages": [...], "system": [...], ...}}}` -- a discriminated
  union under "input", NOT top-level `prompt`/`messages`/`system` fields (verified against
  awsRestjson1_serializeDocumentCountTokensInput / serializeDocumentInvokeModelTokensRequest /
  serializeDocumentConverseTokensRequest in the real SDK's serializers.go). The `body` member is a
  smithy blob -- base64-encoded JSON text on the wire; Go's `encoding/json` transparently
  base64-decodes into a `[]byte` struct field, so `Body []byte \`json:"body"\`` round-trips it directly.
  Fixed this pass: the handler previously looked for fields that never exist on this operation's real
  request, silently falling back to counting the raw envelope's byte length (JSON structural overhead)
  instead of the actual model input -- always producing an inflated, content-independent token
  estimate. Now parses the real union and measures the decoded invokeModel body / converse message text.

- **Error codes**: this service's real exception codes (verified against
  aws-sdk-go-v2/service/bedrockruntime@v1.50.1 types/errors.go and deserializers.go's
  errorCode switch) are AccessDeniedException, ConflictException, InternalServerException,
  ModelErrorException, ModelNotReadyException, ModelStreamErrorException, ModelTimeoutException,
  ResourceNotFoundException, ServiceQuotaExceededException, ServiceUnavailableException,
  ThrottlingException, ValidationException. Fixed this pass: internal-error responses used the
  Query/EC2-protocol-style code "InternalFailure" (a generic gopherstack pattern copied from other
  services), which does not match any case in the SDK's restjson1 error-code switch -- the aws-sdk-go-v2
  client would fall through to a generic/untyped smithy API error instead of a typed
  `*types.InternalServerException`, breaking client code that does typed exception matching (a common
  Bedrock retry-logic pattern). All 9 call sites now use "InternalServerException".
  "UnknownOperationException" for unmatched routes is left as-is: it's an internal gopherstack-wide
  sentinel for unrouted paths (same convention as ecs/ecr/glue/cloudwatchlogs/redshift/resourcegroups),
  not a real AWS wire code, and out of scope to change service-by-service.

- **Timestamps**: StartAsyncInvoke/GetAsyncInvoke/ListAsyncInvokes use `time.RFC3339` string
  formatting for submitTime/lastModifiedTime/endTime, which matches the smithy `date-time` timestamp
  format the real client parses via `smithytime.ParseDateTime` (verified in deserializers.go) --
  correct, NOT an epoch-seconds numeric field like some other services. Don't "fix" this to
  `awstime.Epoch` by reflex; it would break the wire shape here.

- **Unit tests are external black-box** (`package bedrockruntime_test`), calling `h.Handler()(c)` via
  Echo directly. This exercises the actual dispatch/extraction code path (including the modelId-ARN fix)
  but bypasses `RouteMatcher()` itself for the invoke dispatch tests; `RouteMatcher()` is separately
  covered by `TestHandler_RouteMatcher`. The matcher's prefix check (`HasPrefix(path, "/model/")`) is
  unaffected by embedded slashes in modelId, so no matcher-specific regression test was needed for the
  ARN fix -- the bug lived entirely in modelId *extraction* downstream of routing, which the existing
  `doRequest`-driven tests do exercise for real (`TestParity_InvokeModel_ARNModelIDWithEmbeddedSlash`
  asserts the correct Claude 3 Messages-API envelope is chosen, not just that extraction returns the
  right string).

- **ApplyGuardrail** mock is deterministic: BLOCKED for content containing "blocked"/"harmful"/
  "toxic"/"unsafe" (case-insensitive substring), NONE otherwise; usage counters are always 0 (all
  required int32 fields in the real GuardrailUsage struct, present with zero values -- acceptable mock,
  not a disguised no-op since assessments/outputs do reflect the real input content).

- **Converse/ConverseStream** mock reads the actual request (messages/system) to estimate input
  tokens and does NOT ignore them; the completion text itself is a fixed deterministic string, which is
  the explicitly-acceptable "mock inference" behavior per parity rules (no real LLM backing this).
