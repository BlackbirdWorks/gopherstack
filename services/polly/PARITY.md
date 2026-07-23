---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: polly
sdk_module: aws-sdk-go-v2/service/polly@v1.57.5   # version audited against
last_audit_commit: b0d0cfe0                       # HEAD when this manifest was written
last_audit_date: 2026-07-23
overall: A            # zero gaps remaining: all 8 gaps and 5 deferred items from the prior pass fixed
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  SynthesizeSpeech: {wire: ok, errors: ok, state: ok, persist: n/a, note: "OutputFormat coverage complete (ogg_opus/mulaw/alaw); full op-specific error taxonomy; SSML well-formedness now validated (InvalidSsmlException)"}
  StartSpeechSynthesisStream: {wire: ok, errors: ok, state: n/a, persist: n/a, note: "FIXED: error taxonomy now the real op-specific set -- every client validation failure remaps to the generic ValidationException (never SynthesizeSpeech's op-specific exception names), matching the real deserializer's error switch (ServiceFailureException/ServiceQuotaExceededException/ThrottlingException/ValidationException). ServiceQuotaExceededException/ThrottlingException remain unimplemented -- see items_still_open equivalent in Notes."}
  StartSpeechSynthesisTask: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED: removed fabricated SnsRoleArn request/response field (not a real Polly API field -- see Notes); added real OutputS3KeyPrefix request field wired into OutputUri; added S3 bucket/key and SNS topic ARN format validation; SSML-vs-plain-text length limit now correctly differentiated (100000 billed / 200000 total, was flat 100000 for both)"}
  GetSpeechSynthesisTask: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED: added InvalidTaskIdException for syntactically invalid (non-UUID) TaskId, distinct from SynthesisTaskNotFoundException for a well-formed-but-unknown one -- both are real, separately-modeled exceptions for this op"}
  ListSpeechSynthesisTasks: {wire: ok, errors: ok, state: ok, persist: ok, note: "MaxResults out-of-range left generic (unlisted in the real service model -- confirmed via deserializer's error switch, which lists only InvalidNextTokenException/ServiceFailureException)"}
  PutLexicon: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED: implemented LexiconSizeExceededException (>40000 chars), MaxLexemeLengthExceededException (>100 char <phoneme>/<alias> replacement), MaxLexiconsNumberExceededException (>100 lexicons/account), UnsupportedPlsAlphabetException (alphabet not ipa/x-sampa), UnsupportedPlsLanguageException (xml:lang outside the 42-value LanguageCode enum) -- all quota numbers sourced from docs.aws.amazon.com/polly/latest/dg/limits.html#limits-lexicons"}
  GetLexicon: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteLexicon: {wire: ok, errors: ok, state: ok, persist: ok}
  ListLexicons: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeVoices: {wire: ok, errors: ok, state: ok, persist: n/a, note: "FIXED: built-in voice catalogue now covers all 106 VoiceId enum values in the pinned SDK (was ~87/106); every existing entry's SupportedEngines re-verified against docs.aws.amazon.com/polly/latest/dg/voicelist.html and corrected where wrong (many voices were missing their generative-engine support, a few had extra/missing standard or neural support -- see Notes). Still no MaxResults/NextToken pagination -- confirmed correct AWS behavior (single-page response is valid), not changed."}
families:
  lexicon: {status: ok, note: "Put/Get/List/Delete verified against restjson1 paths and PutLexicon/GetLexicon shapes; quota/PLS-schema validation field-diffed against limits.html and the real UnsupportedPlsAlphabetException doc string; persistence round-trips (store.Table)"}
  synthesisTask: {status: ok, note: "Start/Get/List verified; SnsRoleArn (fabricated) removed, OutputS3KeyPrefix (real, previously missing) added and wired into OutputUri; S3/SNS format validation added; lifecycle advance-on-poll unchanged and correct; persist round-trips including the new OutputS3KeyPrefix field"}
  synthesizeSpeech: {status: ok, note: "REST payload response verified: Content-Type set from OutputFormat, X-Amzn-RequestCharacters header present; speech-mark json-stream verified; SSML well-formedness (must be valid XML wrapped in <speak>) now enforced for both SynthesizeSpeech and StartSpeechSynthesisTask via one shared validateSSML, and for StartSpeechSynthesisStream via SynthesizeSpeech's shared validateOptions"}
  voices: {status: ok, note: "filter logic (Engine/Gender/LanguageCode/IncludeAdditionalLanguageCodes) verified against real DescribeVoicesInput/Voice shape; full 106-voice catalogue field-diffed against the AWS voicelist.html table and the pinned SDK's VoiceId enum -- every voice's LanguageCode/Gender/SupportedEngines cross-checked"}
gaps: []
deferred: []
leaks: {status: clean, note: "no goroutines/timers; task lifecycle advances synchronously on each Get/List poll (b.mu-guarded), no background janitor to leak. Tag* removal deleted the last map keyed independently of store.Table (b.tags) with no replacement -- one fewer thing that could ghost-row after delete."}
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

- **OutputFormat has 7 real values**: `json, mp3, ogg_opus, ogg_vorbis, pcm, mulaw, alaw`, all
  covered with correct Content-Type and SampleRate constraints.

- **PCM's synthetic bytes are wrapped in a RIFF/WAV header even though AWS's real `pcm` output is
  headerless raw signed-16-bit little-endian samples** (Content-Type `audio/pcm`, not
  `audio/wav`). This is a pre-existing minor inconsistency in the mock audio byte content, not
  the wire shape (Content-Type/RequestCharacters headers are correct). Per prior audit scope
  ("mock audio bytes are acceptable"), left unchanged -- do not "fix" without re-confirming scope,
  since every existing PCM test (`bodyMagic: []byte("RIFF")`, WAV-header sample-rate byte offsets)
  depends on the WAV wrapper.

- **Error taxonomy is now complete per-op**, verified directly against each operation's
  `awsRestjson1_deserializeOpError<Op>` switch in `aws-sdk-go-v2/service/polly/deserializers.go`
  (the modeled error list, not guessed): `SynthesizeSpeech`/`StartSpeechSynthesisTask` share
  {EngineNotSupported, InvalidSampleRate, InvalidSsml, LanguageNotSupported, LexiconNotFound,
  MarksNotSupportedForFormat, ServiceFailure, SsmlMarksNotSupportedForTextType,
  TextLengthExceeded} (+S3Bucket/S3Key/SnsTopicArn for the task op only); `PutLexicon` has
  {InvalidLexicon, LexiconSizeExceeded, MaxLexemeLengthExceeded, MaxLexiconsNumberExceeded,
  ServiceFailure, UnsupportedPlsAlphabet, UnsupportedPlsLanguage}; `GetSpeechSynthesisTask` has
  {InvalidTaskId, ServiceFailure, SynthesisTaskNotFound}; `List*`/`DescribeVoices` have only
  {InvalidNextToken, ServiceFailure}; `StartSpeechSynthesisStream` has only {ServiceFailure,
  ServiceQuotaExceeded, Throttling, Validation} -- notably NOT the op-specific names above, so
  every validation failure on that op remaps to the generic `ValidationException` (see
  `ErrStreamValidation`'s doc comment in errors.go). Checks with no modeled exception at all
  (invalid Engine/OutputFormat/TextType enum values, LexiconNames-count-exceeded, unknown VoiceId
  for SynthesizeSpeech/StartSpeechSynthesisTask, MaxResults out of range, lexicon
  name-format violation) intentionally stay on the generic `ErrValidation` →
  `InvalidParameterValueException` fallback for those two ops -- this is standard AWS behavior
  for unlisted/unmodeled validation errors (returned via the SDK's generic
  `smithy.GenericAPIError` path), not a gap.

- **`checkVoiceSupport` (speech.go)** distinguishes "voice ID doesn't exist" (generic
  `ErrValidation`) from "voice exists but doesn't support this engine"
  (`EngineNotSupportedException`) from "voice exists, engine ok, but doesn't speak this language"
  (`LanguageNotSupportedException`). All three are real, separately-named AWS exceptions with
  distinct meanings per the service model's documentation strings.

- **RouteMatcher / dispatch use the same `parseRoute` helper** — `Handler()`'s dispatcher and
  `RouteMatcher()` both call `parseRoute(method, path)`; there is no separate/duplicate routing
  table, so unit tests calling `h.Handler()(c)` directly do NOT bypass real routing logic here.
  Paths/methods verified against `aws-sdk-go-v2/service/polly/serializers.go`
  (`/v1/lexicons/{Name}` PUT/GET/DELETE, `/v1/lexicons` GET, `/v1/voices` GET,
  `/v1/synthesisTasks` POST/GET, `/v1/synthesisTasks/{TaskId}` GET, `/v1/synthesisStream` POST,
  `/v1/speech` POST) -- this list is now exhaustive; there is no `/v1/tags/{arn}` route.

- **Tagging surface removed.** `TagResource`/`UntagResource`/`ListTagsForResource` and the
  `/v1/tags/{arn}` routes were gopherstack-invented functionality with zero basis in the real
  Amazon Polly API (confirmed: no `api_op_TagResource.go` et al. in `aws-sdk-go-v2/service/polly`,
  and `service-2.json`'s operation list omits them entirely). The prior audit flagged this as a
  gap needing a decision ("confirm intentional and document, or remove") but left it in place;
  this pass removed it for true parity -- `tags.go`/`tags_test.go` deleted, the `b.tags` map and
  `TaskARN`/`taskARN` helpers deleted, the three ops dropped from
  `GetSupportedOperations`/routing/dispatch, `Tag` struct removed from models.go, and
  `backendSnapshot.Tags` dropped (snapshot version bumped 1→2, discarding any snapshot with tag
  data on restore -- acceptable since the feature no longer exists). `sdk_completeness_test.go`
  still passes: `sdkcheck.CheckCompleteness` only verifies gopherstack doesn't have *fewer* ops
  than the SDK client surface, never more, so this was never a completeness-test dependency.

- **`SnsRoleArn` was a fabricated field, now removed.** Real Polly's
  `StartSpeechSynthesisTaskInput`/`SynthesisTask` (request and response) have no `SnsRoleArn`
  field at all -- confirmed directly in `aws-sdk-go-v2/service/polly/api_op_StartSpeechSynthesisTask.go`.
  Only `SnsTopicArn` is real. The field was silently invented (possibly confused with a different
  AWS service that does have an SNS role ARN parameter) and has been deleted from
  `startTaskInput`/`taskOutput`/`SpeechSynthesisTask`/the backend method signature. Real
  `OutputS3KeyPrefix`, which existed on the real request type but was never read by gopherstack's
  handler (silently dropped), is now wired through end-to-end and woven into the constructed
  `OutputUri`.

- **Voice catalogue is exhaustively field-diffed.** All 106 `VoiceId` enum values from the pinned
  SDK (`aws-sdk-go-v2/service/polly/types`) are present. Three voices AWS's live documentation page
  lists (Patrick, Alba, Raúl) are intentionally excluded: they are not part of the pinned SDK's
  `VoiceId` enum (a newer AWS addition unreleased at pin time), so accepting them would let this
  backend respond to a `VoiceId` no real client built against `v1.57.5` could ever send. Every
  voice's `LanguageCode`/`Gender`/`SupportedEngines` was cross-checked against
  `docs.aws.amazon.com/polly/latest/dg/voicelist.html`'s table (fetched live during this pass, not
  reconstructed from memory) -- this caught and fixed several pre-existing `SupportedEngines`
  errors beyond the 19 missing voices, most commonly a voice missing `generative` engine support it
  actually has (e.g. Lisa, Laura, Olivia, Kajal, Niamh, Aria, Ayanda, Remi, Isabelle, Gabrielle,
  Liam, Vicki, Daniel, Hannah, Ola, Camila, Lucia, Sergio, Mia, Lupe, Pedro, Seoyeon) plus a few
  outright-wrong entries (Justin had `standard` it doesn't support; Kevin was missing `standard` it
  does support; Joanna/Matthew/Ruth/Stephen had the wrong long-form/generative mix; Lotte had
  `neural` it doesn't support). Bilingual voices (Aditi/Kajal: en-IN+hi-IN; Hala/Zayd: ar-AE+arb)
  use `AdditionalLanguageCodes` per `docs.aws.amazon.com/polly/latest/dg/bilingual-voices.html`,
  which confirms Hala/Zayd are Amazon Polly's only other fully bilingual voices besides Aditi/Kajal.

- **SSML well-formedness validation (`validateSSML` in speech.go)** requires TextType=ssml input
  to be well-formed XML with exactly one root element named `speak` (checked via `encoding/xml`
  tokenization, not a regex) -- unwrapped plain text or malformed markup now returns
  `InvalidSsmlException`. This is shared by `SynthesizeSpeech`, `StartSpeechSynthesisTask` (both
  call the common `validateOptions`), and `StartSpeechSynthesisStream` (calls `SynthesizeSpeech`
  internally, then remaps any resulting error including this one to `ValidationException` per the
  stream op's real error taxonomy above).

- **Lexicon quotas** (`docs.aws.amazon.com/polly/latest/dg/limits.html#limits-lexicons`, fetched
  live): lexicon content ≤40,000 characters, ≤100 lexicons per account, ≤100 characters per
  `<phoneme>`/`<alias>` replacement, lexicon name ≤20 alphanumeric characters (already correct
  pre-pass). `MaxLexiconsNumberExceededException` only fires for a genuinely new lexicon name;
  overwriting an existing one never counts against the quota (matches real `PutLexicon` semantics:
  "If a lexicon with the same name already exists ... it is overwritten").

- **StartSpeechSynthesisTask text limits corrected to differentiate TextType**:
  100,000 billed characters (plain text) vs 200,000 total characters (SSML, markup not billed),
  per `docs.aws.amazon.com/polly/latest/dg/limits.html#limits-long`. Was previously a flat 100,000
  regardless of TextType, incorrectly rejecting valid SSML requests between 100,001 and 200,000
  characters.

- **`GetSpeechSynthesisTask` now validates TaskId format.** A syntactically invalid (non-UUID)
  TaskId returns `InvalidTaskIdException`; a well-formed UUID that doesn't match any task returns
  `SynthesisTaskNotFoundException`. Confirmed reachable server-side behavior (not merely client-side
  SDK validation): `aws-sdk-go-v2/service/polly/validators.go` only checks `TaskId` is non-nil, not
  its format, so a real HTTP client (not just the Go SDK) can trigger this server-side. Task IDs are
  UUIDs (see `uuid.NewString()` in `StartSpeechSynthesisTask`).

- **Not implemented, and not claimed as fixed**: `ServiceQuotaExceededException`/
  `ThrottlingException` for `StartSpeechSynthesisStream` require genuine request-rate/quota
  simulation (a different kind of feature entirely, unrelated to input-validation taxonomy) and
  were out of scope for this pass; the `ValidationException` remapping for actual client input
  errors is complete and correct on its own.
