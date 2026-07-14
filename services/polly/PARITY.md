---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: polly
sdk_module: aws-sdk-go-v2/service/polly@v1.57.5   # version audited against
last_audit_commit: b0d0cfe0                       # HEAD when this manifest was written
last_audit_date: 2026-07-13
overall: A            # real fixes found (error taxonomy, HTTP status, OutputFormat coverage)
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  SynthesizeSpeech: {wire: ok, errors: ok, state: ok, persist: n/a, note: "stateless op; fixed OutputFormat gap (ogg_opus/mulaw/alaw), TextLengthExceededException/InvalidSampleRateException/EngineNotSupportedException/LanguageNotSupportedException/MarksNotSupportedForFormatException/SsmlMarksNotSupportedForTextTypeException now returned instead of a single generic InvalidParameterValueException"}
  StartSpeechSynthesisStream: {wire: ok, errors: partial, state: n/a, persist: n/a, note: "eventstream framing verified; error taxonomy left generic (deferred, see gaps)"}
  StartSpeechSynthesisTask: {wire: ok, errors: ok, state: ok, persist: ok, note: "same OutputFormat/error-taxonomy fixes as SynthesizeSpeech; OutputURI extension mapping extended for ogg_opus"}
  GetSpeechSynthesisTask: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED: SynthesisTaskNotFoundException now returns HTTP 400 per the real service model (was 404)"}
  ListSpeechSynthesisTasks: {wire: ok, errors: ok, state: ok, persist: ok, note: "NextToken decode failure now InvalidNextTokenException (was generic InvalidParameterValueException); MaxResults out-of-range left generic (unlisted in the real service model, see gaps)"}
  PutLexicon: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED: malformed PLS Content now InvalidLexiconException (was generic InvalidParameterValueException); name-format violation stays generic (unlisted for PutLexicon in the real model)"}
  GetLexicon: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteLexicon: {wire: ok, errors: ok, state: ok, persist: ok}
  ListLexicons: {wire: ok, errors: ok, state: ok, persist: ok, note: "NextToken decode failure now InvalidNextTokenException"}
  DescribeVoices: {wire: ok, errors: ok, state: ok, persist: n/a, note: "static catalogue; no MaxResults param in real API, single-page response is compliant. Catalogue covers ~90 of the ~112 real VoiceId enum values -- see gaps"}
  TagResource: {wire: n/a, errors: n/a, state: ok, persist: ok, note: "NOT a real Polly API surface -- see gaps"}
  UntagResource: {wire: n/a, errors: n/a, state: ok, persist: ok, note: "NOT a real Polly API surface -- see gaps"}
  ListTagsForResource: {wire: n/a, errors: n/a, state: ok, persist: ok, note: "NOT a real Polly API surface -- see gaps"}
families:
  lexicon: {status: ok, note: "Put/Get/List/Delete verified against restjson1 paths and PutLexicon/GetLexicon shapes; persistence round-trips (store.Table)"}
  synthesisTask: {status: ok, note: "Start/Get/List verified; lifecycle advance-on-poll (scheduled->inProgress->completed/failed) unchanged and correct; persist round-trips including TaskStatus"}
  synthesizeSpeech: {status: ok, note: "REST payload response verified: Content-Type set from OutputFormat, X-Amzn-RequestCharacters header present; speech-mark json-stream verified"}
  voices: {status: ok, note: "filter logic (Engine/Gender/LanguageCode/IncludeAdditionalLanguageCodes) verified against real DescribeVoicesInput/Voice shape"}
gaps:
  - "TagResource/UntagResource/ListTagsForResource + the /v1/tags/{arn} routes are NOT part of the real Amazon Polly API (confirmed: aws-sdk-go-v2/service/polly has no such api_op_*.go files, and service-2.json's operation list omits them entirely). This is gopherstack-invented functionality, not a wire-shape bug -- no genuine SDK client can reach these routes, so it's harmless, but it is not real AWS surface. Left in place (fully functional, not a stub) rather than removed, since removal is a larger behavioral change outside a parity-bugfix pass. (bd: file follow-up to confirm intentional and document, or remove)"
  - "Built-in voice catalogue (backend.go builtInVoices) covers ~90 of the real ~112 VoiceId enum values from aws-sdk-go-v2/service/polly/types. Missing IDs include Geraint, Zeina, Hiujin, Tomoko, Zayd, Danielle, Gregory, Jitka, Sabrina, Jasmine, Jihye, Ambre, Beatrice, Florian, Lennart, Lorenzo, Tiffany, Andres, and Arabic (arb) support. Not fabricated data -- a real but incomplete subset. Completing the catalogue is a larger, lower-risk-tolerance change deferred from this pass."
  - "PutLexicon does not implement LexiconSizeExceededException, MaxLexemeLengthExceededException, MaxLexiconsNumberExceededException, UnsupportedPlsAlphabetException, or UnsupportedPlsLanguageException -- these require new quota/PLS-schema validation logic (not just error-type remapping of existing checks) and exact AWS quota numbers that were not available to verify confidently in this pass."
  - "StartSpeechSynthesisTask does not validate OutputS3BucketName/OutputS3KeyPrefix format (InvalidS3BucketException/InvalidS3KeyException) or SnsTopicArn format (InvalidSnsTopicArnException); any string is accepted. New validation, deferred."
  - "SynthesizeSpeech/StartSpeechSynthesisTask do not validate SSML well-formedness (InvalidSsmlException) when TextType=ssml; any string is accepted as SSML. New validation, deferred."
  - "ListSpeechSynthesisTasks MaxResults out-of-range still returns generic InvalidParameterValueException; the real service-2.json model lists no MaxResults-specific exception for this op at all, so correct AWS behavior here (clamp vs error) is unconfirmed -- left as-is rather than guessed."
  - "DescribeVoices ignores any NextToken query parameter (always returns the full catalogue in one page, never emits NextToken). The real API does support NextToken/pagination per paginators-1.json, but returning everything in a single page is valid AWS behavior (paginated APIs may return all results in the first page) -- not changed."
  - "StartSpeechSynthesisStream's error taxonomy stays generic InvalidParameterValueException; the real op's documented errors are ValidationException/ServiceQuotaExceededException/ThrottlingException, none of which map cleanly onto today's validation without additional behavioral changes (e.g., real throttling simulation). Deferred."
deferred:
  - StartSpeechSynthesisStream error taxonomy
  - Lexicon quota/PLS-schema validation (LexiconSizeExceeded, MaxLexemeLength, MaxLexiconsNumber, UnsupportedPlsAlphabet/Language)
  - S3 bucket/key and SNS topic ARN format validation on StartSpeechSynthesisTask
  - SSML well-formedness validation
  - Full VoiceId catalogue completion
leaks: {status: clean, note: "no goroutines/timers; task lifecycle advances synchronously on each Get/List poll (b.mu-guarded), no background janitor to leak"}
---

## Notes

Freeform: AWS-behavior specifics worth remembering, and any "looks-wrong-but-correct" traps
so the next auditor doesn't re-flag them.

- **Protocol**: restjson1. Response bodies for the management ops (Lexicon*, SpeechSynthesisTask*)
  are `{"...": ...}` JSON envelopes; `SynthesizeSpeech`/`StartSpeechSynthesisStream` are raw
  bodies (audio bytes / eventstream frames), not JSON envelopes -- this is correct AWS behavior,
  not a missing envelope bug.

- **SynthesisTaskNotFoundException is HTTP 400, not 404.** Confirmed directly from
  `botocore/data/polly/2016-06-10/service-2.json`: `'error': {'httpStatusCode': 400}`. This is
  unusual for a "NotFound"-named exception (compare `LexiconNotFoundException`, which genuinely is
  404) but is the real, documented AWS behavior -- do not "fix" it back to 404 in a future pass.

- **OutputFormat has 7 real values**: `json, mp3, ogg_opus, ogg_vorbis, pcm, mulaw, alaw`. This
  service previously only accepted 4 (`mp3, ogg_vorbis, pcm, json`), rejecting valid real-SDK
  requests for `ogg_opus`/`mulaw`/`alaw` with `InvalidParameterValueException`. Now fixed with
  correct Content-Type (`ogg_opus`→`audio/ogg`, `mulaw`→`audio/mulaw`, `alaw`→`audio/alaw`),
  SampleRate constraints (`ogg_opus`: only `"48000"`; `mulaw`/`alaw`: only `"8000"`), and
  synthetic (headerless, non-WAV) silence bytes distinct from the PCM path's WAV container.

- **PCM's synthetic bytes are wrapped in a RIFF/WAV header even though AWS's real `pcm` output is
  headerless raw signed-16-bit little-endian samples** (Content-Type `audio/pcm`, not
  `audio/wav`). This is a pre-existing minor inconsistency in the mock audio byte content, not
  the wire shape (Content-Type/RequestCharacters headers are correct). Per this audit's scope
  ("mock audio bytes are acceptable"), left unchanged -- do not "fix" without re-confirming scope,
  since every existing PCM test (`bodyMagic: []byte("RIFF")`, WAV-header sample-rate byte offsets)
  depends on the WAV wrapper.

- **Error taxonomy**: AWS Polly has ~15 named exceptions across its ops (see
  `service-2.json`'s per-operation `errors` list). Only a subset maps cleanly onto gopherstack's
  *existing* validation checks without inventing new business rules or guessing undocumented
  quota numbers -- that subset (TextLengthExceededException, InvalidSampleRateException,
  EngineNotSupportedException, LanguageNotSupportedException, MarksNotSupportedForFormatException,
  SsmlMarksNotSupportedForTextTypeException, InvalidNextTokenException, InvalidLexiconException)
  is now wired through `writeBackendError`. Checks with no confident 1:1 AWS exception mapping
  (invalid Engine/OutputFormat/TextType enum values, LexiconNames-count-exceeded, unknown VoiceId,
  MaxResults out of range) intentionally stay on the generic `ErrValidation` →
  `InvalidParameterValueException` fallback -- this is very likely still correct AWS behavior for
  most of them (unlisted/unmodeled validation errors commonly surface as a generic 400 across AWS
  REST APIs), but wasn't independently confirmable from the service model, so treat it as the
  known baseline rather than a bug to "complete" blindly.

- **`checkVoiceSupport` (backend.go)** replaced the old boolean `voiceSupports`: it now
  distinguishes "voice ID doesn't exist" (generic `ErrValidation`) from "voice exists but doesn't
  support this engine" (`EngineNotSupportedException`) from "voice exists, engine ok, but doesn't
  speak this language" (`LanguageNotSupportedException`). All three are real, separately-named AWS
  exceptions with distinct meanings per the service model's documentation strings.

- **RouteMatcher / dispatch use the same `parseRoute` helper** — `Handler()`'s dispatcher and
  `RouteMatcher()` both call `parseRoute(method, path)`; there is no separate/duplicate routing
  table, so unit tests calling `h.Handler()(c)` directly do NOT bypass real routing logic here
  (unlike the bug class seen in other services). `TestHandlerMetadataAndRouting` additionally
  exercises `RouteMatcher()` explicitly. No routing bug found this pass; paths/methods verified
  against `aws-sdk-go-v2/service/polly/serializers.go` (`/v1/lexicons/{Name}` PUT/GET/DELETE,
  `/v1/lexicons` GET, `/v1/voices` GET, `/v1/synthesisTasks` POST/GET,
  `/v1/synthesisTasks/{TaskId}` GET, `/v1/synthesisStream` POST, `/v1/speech` POST).

- **Tagging is fabricated** (see gaps): Amazon Polly's real API has zero tagging operations —
  confirmed by the complete absence of `api_op_TagResource.go` / `api_op_UntagResource.go` /
  `api_op_ListTagsForResource.go` in `aws-sdk-go-v2/service/polly`, and their absence from
  `service-2.json`'s operation list. `sdk_completeness_test.go`'s `sdkcheck.CheckCompleteness`
  passes anyway because that check only verifies gopherstack doesn't have *fewer* ops than the
  SDK client surface, not that it doesn't have *more*. No real SDK client can invoke these routes,
  so they're inert with respect to AWS parity, but a future cleanup could remove them for
  surface-area hygiene.
