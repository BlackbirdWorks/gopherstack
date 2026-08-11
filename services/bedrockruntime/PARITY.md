---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: bedrockruntime
sdk_module: aws-sdk-go-v2/service/bedrockruntime@v1.57.1   # bumped from v1.56.0 pin; auth.go/errors.go re-verified this pass
last_audit_commit: 8c56f4eb9                                # HEAD when the 2026-08-07 pass (gopherstack-ayfw) was written
last_audit_date: 2026-08-07
overall: A            # 2026-08-07 (gopherstack-ayfw): fixed ChaosServiceName ("bedrockruntime" -> "bedrock", matching
                      # real SigV4 signing name) -- see chaos-fault-injection family below. Closes the gap the
                      # 2026-07-25 pass below left open; grade held at A.
                      # 2026-07-25: genuine fixes found this pass
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  InvokeModel: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed this pass: guardrailIdentifier-without-guardrailVersion now ValidationException (matches documented InvokeModelInput precondition); PerformanceConfigLatency request header now echoed onto the response header (was silently dropped, always empty to real SDK callers)"}
  InvokeModelWithResponseStream: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed this pass (CRITICAL): the 'chunk' event payload was the raw mock-response JSON; the real client (types.PayloadPart via awsRestjson1_deserializeDocumentPayloadPart) requires the payload to be a JSON document {\"bytes\":\"<base64>\"} -- previously every real SDK client's InvokeModelWithResponseStream call against gopherstack decoded to an EMPTY body. Also added: X-Amzn-Bedrock-Content-Type response header (was never set --  bound to a *different* header than plain InvokeModel's Content-Type), same guardrail-header and PerformanceConfigLatency fixes as InvokeModel, chunk event :content-type fixed from the wrong 'application/octet-stream' to 'application/json'"}
  InvokeModelWithBidirectionalStream: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed this pass: same chunk-payload {\"bytes\":<base64>} wrapping bug as InvokeModelWithResponseStream (types.BidirectionalOutputPayloadPart has the identical Bytes-wrapped shape). No guardrail/PerformanceConfigLatency headers exist on this op's real Input struct (verified against api_op_InvokeModelWithBidirectionalStream.go: ModelId is its only member), so those fixes do not apply here"}
  Converse: {wire: ok, errors: ok, state: ok, persist: n/a, note: "field-diffed against ConverseInput/ConverseOutput -- messages/system/inferenceConfig/toolConfig/guardrailConfig accepted (not fabricated-away), output.message/stopReason/usage{input,output,totalTokens}/metrics{latencyMs} all match required members"}
  ConverseStream: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed this pass: contentBlockStart event no longer sends a fabricated 'start':{'text':''} field -- types.ContentBlockStart's union has only image/toolResult/toolUse variants (verified against deserializeDocumentContentBlockStart in the real SDK), no 'text' member exists, so a plain-text content block must omit 'start' entirely rather than emit a non-existent union tag. Event names (messageStart/contentBlockStart/contentBlockDelta/contentBlockStop/messageStop/metadata) and their field shapes (contentBlockIndex/delta.text/stopReason/usage/metrics.latencyMs) verified against awsRestjson1_deserializeEventStreamConverseStreamOutput -- all correct, unchanged"}
  CountTokens: {wire: ok, errors: ok, state: ok, persist: n/a, note: "unchanged this pass -- previously fixed request body wire shape ({input:{invokeModel:{body}}} / {input:{converse:{...}}}) re-verified still correct"}
  ApplyGuardrail: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed this pass: assessments was ALWAYS an empty array, including when action=BLOCKED -- a disguised no-op (PARITY.md previously and incorrectly claimed 'assessments... reflect the real input content', which was false: only outputs did). Now a BLOCKED action reports a types.GuardrailWordPolicyAssessment-shaped wordPolicy.customWords entry naming the matched keyword, matching the real GuardrailAssessment union's required member shapes"}
  StartAsyncInvoke: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAsyncInvoke: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAsyncInvokes: {wire: ok, errors: ok, state: ok, persist: ok}
  InvokeGuardrailChecks: {wire: partial, errors: ok, state: partial, persist: n/a, note: "NEW this pass (POST /guardrail-checks/invoke, confirmed from serializers.go's awsRestjson1_serializeOpInvokeGuardrailChecks path literal; field-diffed contentFilter/promptAttack/sensitiveInformation config+result shapes against types.go/deserializers.go). Does NOT reference a stored 'bedrock'-service guardrail resource -- Checks is supplied inline in the request, so there is no guardrailIdentifier and nothing in the separate bedrock control-plane backend to consult (confirmed unreachable/inapplicable here, not silently ignored). contentFilter/promptAttack: no real ML classifier exists, so each requested group is present (matches whether the caller asked for it) but its 'results' array is honestly EMPTY rather than carrying a fabricated severityScore per category -- this is a genuine wire-completeness gap (real AWS always returns one entry per requested category) traded deliberately against the alternative of inventing classifier output; wire: partial reflects this. sensitiveInformation: genuinely evaluated via literal, deterministic pattern/format detectors (see handler_guardrail_checks.go's piiDetectors) for EMAIL/PHONE/IP_ADDRESS/URL/AWS_ACCESS_KEY/MAC_ADDRESS/US_SOCIAL_SECURITY_NUMBER/CREDIT_DEBIT_CARD_NUMBER (the last Luhn-checksum validated); every other GuardrailChecksSensitiveInformationEntityType value requires free-text NER or a jurisdiction-specific checksum not implemented, and is honestly never matched -- not fabricated. confidenceScore is always exactly 1.0 for a detected entity (a deterministic pattern hit, not an invented probability). usage.*.textUnits is always 0, matching the same not-metered precedent ApplyGuardrail already established for its usage block."}
families:
  model-path-routing: {status: ok, note: "unchanged this pass; extractModelID/ExtractResource ARN-embedded-slash fix from the prior audit re-verified still correct"}
  guardrail-path-routing: {status: ok, note: "unchanged this pass"}
  async-invoke: {status: ok, note: "unchanged this pass; StartAsyncInvoke/GetAsyncInvoke/ListAsyncInvokes wire shapes, idempotency, janitor advance/sweep, and persistence re-verified against StartAsyncInvokeInput/GetAsyncInvokeOutput/AsyncInvokeSummary -- all required members present, no fabricated fields"}
  error-codes: {status: ok, note: "unchanged this pass; resolveErrorType/fallback confirmed to map to the REAL modeled 'InternalServerException' (not a fabricated 'InternalFailure') at all 9 handler.go/handler_*.go call sites -- re-verified, not a regression"}
  event-stream-chunk-payload: {status: ok, note: "NEW this pass (see InvokeModelWithResponseStream/InvokeModelWithBidirectionalStream op notes): the 'chunk' event's payload must be the smithy PayloadPart/BidirectionalOutputPayloadPart document shape {\"bytes\":\"<base64>\"}, not the raw response JSON. This was the highest-impact bug found this audit -- it broke response-body delivery for every real aws-sdk-go-v2 client streaming call against gopherstack, silently (no error, just an empty Body on the client side)."}
  chaos-fault-injection: {status: ok, note: "2026-08-07 (gopherstack-ayfw): ChaosServiceName was \"bedrockruntime\", but real Bedrock Runtime signs every request with SigV4 service name \"bedrock\" (verified: aws-sdk-go-v2/service/bedrockruntime@v1.57.1 auth.go's serviceAuthOptions, unconditional for every operation) -- the same signing name the sibling services/bedrock control-plane handler already declares. pkgs/chaos's Middleware extracts the fault-matching service string straight from the real Authorization header's SigV4 credential scope, so the old value could never match real client traffic; a fault rule created from the chaos dashboard's own GET /targets discovery (which surfaced \"bedrockruntime\") would silently never fire. Fixed to \"bedrock\" -- getTargets already merges entries sharing one signing name across handlers (its own doc comment cites S3/S3 Control as precedent), so this needed no pkgs/chaos change. New chaos_test.go proves both the fix (a \"bedrock\"-targeted rule now intercepts a real InvokeModel call before the handler runs) and the regression it fixes (a \"bedrockruntime\"-targeted rule does not). This resolves the bd issue's premise -- once the service name matches, the existing generic mechanism already supports injecting ModelError/ModelNotReady/Throttling/ServiceUnavailable (or any other) error code/status for InvokeModel/Converse/any op; see gaps for the one remaining, out-of-scope refinement (ModelErrorException's extra OriginalStatusCode/ResourceName members)."}
gaps:
  - "The generic pkgs/chaos FaultError shape ({code, statusCode} -> a plain {__type, message} JSON body) can inject any error code/status for InvokeModel/Converse/etc (verified: real ModelErrorException/ModelNotReadyException/ThrottlingException/ServiceUnavailableException are all restjson1 GetErrorInfo-resolvable from a body __type field, no X-Amzn-ErrorType header required), but cannot reproduce ModelErrorException's two extra members (OriginalStatusCode, ResourceName) since chaos.FaultError has no per-service extension point for them. Buildable (add optional extra-fields support to chaos.FaultError) but out of this pass's scope: it is shared pkgs/chaos infrastructure, not bedrockruntime-local, and touching it has blast radius across all 137 chaos-registered services. (bd: gopherstack-ayfw)"
  - "CountTokens' invokeModel-body token estimate uses raw decoded-byte length as a chars proxy (cannot know the tokenizer for arbitrary model-specific InvokeModel body formats); acceptable per parity rules (deterministic mock), documented as an approximation in code comments"
  - "Converse's guardrailConfig body field (GuardrailIdentifier/GuardrailVersion) is accepted opaquely (json.RawMessage, unparsed) but not validated for the identifier-requires-version precondition that InvokeModel's equivalent HEADER fields now enforce -- both fields are optional/unrequired on types.GuardrailConfiguration (no smithy 'required' trait, verified), so the real SDK client does not enforce this combination client-side either; low-value/out-of-budget this pass since Converse's mock inference doesn't depend on guardrail semantics to produce a valid response"
  - "StartAsyncInvoke does not validate the real, client-side-required 'modelInput' body member is present -- deliberately not added: the real aws-sdk-go-v2 client enforces this required struct field before ever constructing the HTTP request (addOpStartAsyncInvokeValidationMiddleware), so no real SDK-driven caller can produce a request that omits it; adding server-side validation for it would only add risk (touches ~8 existing test bodies) for a scenario no real client can trigger"
  - "InvokeGuardrailChecks' contentFilter (VIOLENCE/HATE/SEXUAL/MISCONDUCT/INSULTS) and promptAttack (JAILBREAK/PROMPT_INJECTION/PROMPT_LEAKAGE) checks always return an empty results list for a requested group instead of one severityScore entry per requested category: gopherstack has no real ML content/prompt-injection classifier, and a per-category score would be pure fabrication. Documented, not hidden -- see the op note above."
  - "InvokeGuardrailChecks' sensitiveInformation check only genuinely detects EMAIL/PHONE/IP_ADDRESS/URL/AWS_ACCESS_KEY/MAC_ADDRESS/US_SOCIAL_SECURITY_NUMBER/CREDIT_DEBIT_CARD_NUMBER (literal, deterministic formats). Every other GuardrailChecksSensitiveInformationEntityType (NAME, ADDRESS, AGE, PASSWORD, DRIVER_ID, LICENSE_PLATE, AWS_SECRET_KEY, and the various bank/tax/passport/health-ID entity types) requires free-text NER or a jurisdiction-specific checksum this backend does not implement, so those types are honestly never matched rather than fabricated."
deferred: []
leaks: {status: clean, note: "unchanged this pass; janitor (RunJanitor/StartWorker/Shutdown) uses context-bounded worker.Group with proper cancel+done-channel wiring; no goroutine leaks found. No new goroutines/locks introduced by this pass's fixes (all pure request/response shape changes)."}
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
  required int32 fields in the real GuardrailUsage struct, present with zero values -- acceptable mock).
  **Corrected this pass**: the previous version of this note claimed "assessments/outputs do reflect
  the real input content" -- that was only ever true of `outputs`. `assessments` was unconditionally
  `[]` regardless of action, including BLOCKED, which IS a disguised no-op (the one thing a caller most
  wants explained -- *why* was I blocked -- was always empty). Fixed: BLOCKED now returns one
  `types.GuardrailAssessment`-shaped entry with a `wordPolicy.customWords` array containing the matched
  keyword, `action: "BLOCKED"`, `detected: true` (see `types.GuardrailWordPolicyAssessment`/
  `types.GuardrailCustomWord`'s required members in the real SDK's types.go). NONE still returns `[]`,
  which is correct (no policy violation to report) -- verified this is not a second disguised no-op by
  re-reading `buildGuardrailAssessments`'s NONE branch.

- **Converse/ConverseStream** mock reads the actual request (messages/system) to estimate input
  tokens and does NOT ignore them; the completion text itself is a fixed deterministic string, which is
  the explicitly-acceptable "mock inference" behavior per parity rules (no real LLM backing this).
  **Fixed this pass**: ConverseStream's `contentBlockStart` event sent a fabricated
  `"start":{"text":""}` field. The real `types.ContentBlockStart` union (verified against
  `awsRestjson1_deserializeDocumentContentBlockStart` in deserializers.go) has exactly three variants --
  image, toolResult, toolUse -- and no "text" variant, because plain-text content blocks carry no
  meaningful start-of-block payload in the real API. gopherstack's mock only ever emits plain-text
  content, so `contentBlockStart` now omits `start` entirely (real clients tolerate an unset/nil
  `ContentBlockStartEvent.Start`; sending an unrecognized union tag instead produces a `types.UnknownUnionMember`
  substitution client-side, which is the wrong outcome for a field the mock never needed to send).

- **InvokeModelWithResponseStream / InvokeModelWithBidirectionalStream "chunk" event payload** (CRITICAL
  bug fixed this pass): the event message's payload bytes were the raw mock model-response JSON
  (`{"completion":"...", ...}`) written directly. The real aws-sdk-go-v2 client deserializes a "chunk"
  event's payload as `types.PayloadPart` / `types.BidirectionalOutputPayloadPart`
  (`awsRestjson1_deserializeDocumentPayloadPart` in deserializers.go), which looks for exactly one JSON
  key, `"bytes"`, holding the base64-encoded actual response bytes -- any other shape leaves
  `PayloadPart.Bytes` **nil**. This means every real SDK client streaming call
  (`InvokeModelWithResponseStream`/`InvokeModelWithBidirectionalStream`) against gopherstack silently
  received an EMPTY response body -- no error, just nothing, because the raw JSON's top-level keys
  ("completion", "id", "role", ...) never matched the "bytes" key the deserializer looks for. Fixed via
  `modelResponsePayloadPart()`, which now wraps the mock response as
  `{"bytes":"<base64 of the response JSON>"}` before framing it into the chunk event. Also fixed in the
  same pass: the chunk event's `:content-type` message header was the wrong `"application/octet-stream"`
  (now `"application/json"`, matching a structured-document payload), and
  `InvokeModelWithResponseStream`'s HTTP-level `X-Amzn-Bedrock-Content-Type` response header (bound to a
  *different* wire location than plain InvokeModel's `Content-Type` -- verified against
  `awsRestjson1_deserializeOpHttpBindingsInvokeModelWithResponseStreamOutput`) was never set at all.

- **InvokeModel / InvokeModelWithResponseStream guardrail headers**: `GuardrailIdentifier`
  (`X-Amzn-Bedrock-Guardrailidentifier`) and `GuardrailVersion` (`X-Amzn-Bedrock-Guardrailversion`) are
  documented as jointly required in `InvokeModelInput.GuardrailIdentifier`'s doc comment ("An error will
  be thrown ... You provide a guardrail identifier, but guardrailVersion isn't specified"). Fixed this
  pass: gopherstack previously accepted any combination silently. `validateGuardrailHeaders` now returns
  `ValidationException` when an identifier is set without a version, or when a guardrail identifier is
  combined with a non-`application/json` content type (also documented). `InvokeModelWithBidirectionalStream`
  does NOT get this check -- its real `Input` struct has only `ModelId` (verified against
  `api_op_InvokeModelWithBidirectionalStream.go`), no guardrail headers exist on that operation.

- **InvokeGuardrailChecks routing**: `POST /guardrail-checks/invoke` is a fixed, standalone endpoint --
  it is NOT under `guardrailPathPrefix` (`/guardrail/`) despite the name similarity to ApplyGuardrail's
  `/guardrail/{id}/version/{ver}/apply`. `"/guardrail-checks/invoke"` does not share that prefix (the
  character after `/guardrail` is `-`, not `/`), so it is matched/dispatched as its own case in
  `RouteMatcher`/`Handler()`/`asyncOrGuardrailOperation`, not folded into `handleGuardrailPath`. It also
  takes its check configuration inline in the request body rather than a path-embedded
  guardrailIdentifier/guardrailVersion -- there is no guardrail resource lookup at all for this
  operation, in gopherstack or in real AWS.

- **PerformanceConfigLatency echo**: `InvokeModel`/`InvokeModelWithResponseStream`'s
  `X-Amzn-Bedrock-Performanceconfig-Latency` request header now echoes onto the response (real output
  struct's `PerformanceConfigLatency` member, read back from the same header name -- verified against
  `awsRestjson1_deserializeOpHttpBindingsInvokeModelOutput`). Previously always empty to callers who set
  it. gopherstack has no real latency-optimized inference tier, so it reflects the caller's request value
  instead of fabricating one; omitted entirely (not defaulted to "standard") when the caller didn't send
  it, to avoid inventing a value with no backing semantics.
