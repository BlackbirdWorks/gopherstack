---
service: sts
sdk_module: aws-sdk-go-v2/service/sts@v1.44.0   # version audited against (pinned in go.mod)
last_audit_commit: eb94f3c3                     # HEAD before this pass's changes
last_audit_date: 2026-07-24
overall: B                # three real wire-shape bugs found and fixed this pass (see ops
                           # below) — this sweep independently field-diffed every op's
                           # Input/Output struct against the pinned SDK's api_op_*.go source
                           # rather than trusting the prior "verified correct" notes, and the
                           # trust turned out to be misplaced for three operations.
ops:
  AssumeRole: {wire: ok, errors: ok, state: ok, persist: ok, note: "trust-policy Principal/Condition/Effect evaluation, ExternalId, MFA absent (n/a for this op), role-chaining 1h cap, transitive tags, PackedPolicySize — re-verified field-for-field against AssumeRoleInput/Output this pass, no changes needed"}
  AssumeRoleWithSAML: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "FIXED this pass: the real AssumeRoleWithSAMLInput (aws-sdk-go-v2/service/sts's api_op_AssumeRoleWithSAML.go) has ONLY PrincipalArn/RoleArn/SAMLAssertion/DurationSeconds/Policy/PolicyArns — RoleSessionName, SourceIdentity, and Tags were gopherstack-invented top-level wire parameters accepted by the handler (not real SDK request members). AWS instead derives these, plus Subject/SubjectType/Issuer/Audience/NameQualifier's issuer component, from the SAMLAssertion's own <NameID>/<Issuer>/<SubjectConfirmationData>/<Attribute> elements. Added saml_attributes.go's extractSAMLAssertionData to parse the assertion for the RoleSessionName/SourceIdentity/PrincipalTag:*/TransitiveTagKeys attributes and NameID/Issuer/Recipient elements per AWS's documented derivations; removed the three invented fields from AssumeRoleWithSAMLInput and stopped parsing them from the request form (handler_saml.go); buildSAMLResponse now sources Subject/SubjectType/Issuer/Audience from the assertion with the previous hardcoded/PrincipalArn-derived values retained only as fallbacks for minimal test assertions carrying none of these elements"}
  AssumeRoleWithWebIdentity: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "FIXED this pass: the real AssumeRoleWithWebIdentityInput has no SourceIdentity or Tags request member either (only RoleArn/RoleSessionName/WebIdentityToken/DurationSeconds/Policy/PolicyArns/ProviderId) — AWS's doc comment for AssumeRoleWithWebIdentityOutput.SourceIdentity says explicitly \"You do this by adding a claim to the JSON web token.\" Removed both invented fields from AssumeRoleWithWebIdentityInput; added extractWebIdentitySourceIdentity/extractWebIdentityTags (web_identity.go) which read jwtClaimSourceIdentity (\"https://aws.amazon.com/source_identity\") and jwtClaimTags (\"https://aws.amazon.com/tags\", already used elsewhere in this package by GetWebIdentityToken for the same purpose) custom claims from the WebIdentityToken instead of top-level request params"}
  AssumeRoot: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "approved TaskPolicyArn allowlist, fixed 900s duration, arn:aws:sts::ACCT:assumed-root ARN shape re-verified correct. FIXED this pass: AssumeRootOutput.SourceIdentity (\"the source identity specified by the principal that is calling the AssumeRoot operation\" which \"persists across chained role sessions\") was always empty — AssumeRoot has no SourceIdentity input parameter, so it must inherit from the caller's own STS session; added AssumeRootInput.CallerSession (populated by handler_assume_root.go from the SigV4 Authorization header, mirroring the existing AssumeRole role-chaining pattern) and propagated its SourceIdentity into both the new session and the response"}
  GetCallerIdentity: {wire: ok, errors: ok, state: ok, persist: ok, note: "session-token mismatch -> InvalidClientTokenId (not AccessDenied), expired session -> ExpiredTokenException — no input parameters in the real API either; re-verified correct"}
  GetDelegatedAccessToken: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "FIXED this pass: the real GetDelegatedAccessTokenInput has ONLY TradeInToken — no DurationSeconds member (confirmed against both api_op_GetDelegatedAccessToken.go's struct and its awsAwsquery serializer, neither of which reference DurationSeconds for this op). gopherstack had invented a DurationSeconds wire parameter (accepted by handler_delegated_access.go, validated against the AssumeRole 900-43200s range) that does not exist on the real operation. Removed the field from GetDelegatedAccessTokenInput and the handler's form parsing; the backend now always issues DefaultDurationSeconds (3600s) credentials since the caller has no way to influence the lifetime. (Prior-pass note retained: TradeInToken's JWT-shaped exp claim is still checked, returning ExpiredTradeInTokenException for an already-expired token.)"}
  GetFederationToken: {wire: ok, errors: ok, state: ok, persist: ok, note: "federated-user ARN/ID shape, tag/policy-arn/packed-policy validation — re-verified field-for-field against GetFederationTokenInput/Output this pass, no changes needed"}
  GetSessionToken: {wire: ok, errors: ok, state: ok, persist: ok, note: "MFA serial+code pairing and format validation, 900-129600s range — re-verified field-for-field against GetSessionTokenInput/Output this pass, no changes needed"}
  GetWebIdentityToken: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "SigningAlgorithm RS256/ES384 narrowing and SDK-completeness listing from a prior pass re-verified correct. FIXED this pass: implemented SessionDurationEscalationException (a real error type dispatched specifically for this op's error branch in deserializers.go) — a caller using temporary STS credentials can no longer request a DurationSeconds whose resulting JWT expiration exceeds the caller's own session expiration; added GetWebIdentityTokenInput.CallerSession (populated by handler_web_identity.go from the SigV4 Authorization header) and the check in the backend. JWTPayloadSizeExceededException and OutboundWebIdentityFederationDisabledException remain unmodeled — see gaps below."}
  GetAccessKeyInfo: {wire: ok, errors: ok, state: ok, persist: ok, note: "session lookup then well-formed-prefix fallback to backend account ID — re-verified correct"}
  DecodeAuthorizationMessage: {wire: ok, errors: ok, state: ok, persist: n/a, note: "HMAC-signed self-issued messages verified; foreign base64 blobs decoded permissively for emulator usability — re-verified correct"}
families:
  trust-policy-evaluation: {status: ok, note: "Principal (AWS/Federated/Service/wildcard), Action (incl. wildcard glob), Effect Allow/Deny, Condition (StringEquals/StringLike/NotEquals/NotLike + IfExists, case-insensitive keys) all implemented in trustpolicy.go and independently verified against the statements in AssumeRole/WithSAML/WithWebIdentity — no changes needed"}
  session-tag-validation: {status: ok, note: "key/value length, charset, aws: reserved prefix, case-insensitive dup detection, MaxTagCount=50, transitive-tag merge on role chaining — verified correct; AssumeRoleWithSAML's TransitiveTagKeys (assertion-derived, previously never wired to the session at all) is now also propagated, closing a related chaining gap"}
  locking: {status: ok, note: "InMemoryBackend.mu is *lockmetrics.RWMutex (New(\"sts\")) per pkgs-catalog.md; every new lock path added this pass (GetWebIdentityToken/AssumeRoot CallerSession lookups) reuses the existing LookupSession/RLock accessors — no new raw sync.Mutex, no lock ordering changes"}
gaps:
  - "JWTPayloadSizeExceededException, OutboundWebIdentityFederationDisabledException are real error types in aws-sdk-go-v2/service/sts/types (both dispatched specifically on GetWebIdentityToken's error branch) but the SDK ships no doc comment giving a byte-size threshold (JWTPayloadSizeExceededException) or an account-settings model (OutboundWebIdentityFederationDisabledException — this would need a cross-service account-settings flag gopherstack does not currently model, analogous to IAM account settings, with no other API in this codebase to toggle it). Not fixed this pass — no reliable spec to implement a non-arbitrary threshold/flag against. SessionDurationEscalationException (the third previously-unmodeled error in this same dispatch group) WAS implemented this pass — see GetWebIdentityToken above. (bd: gopherstack-p05, follow-up)"
deferred:
  - "SESSION-POLICY EVALUATION: session Policy/PolicyArns are validated for shape/size (MalformedPolicyDocument, PackedPolicyTooLarge) and PackedPolicySize is computed, but the policy document's *content* is not enforced against subsequent API calls (no IAM policy-evaluation engine wired to session credentials). This mirrors the rest of the emulator's authz model and is out of scope for a service-local sts audit."
  - "GetWebIdentityToken Tags payload-size cap (JWTPayloadSizeExceededException) — see gaps above."
leaks: {status: clean, note: "Sessions map is bounded by (a) the background Janitor (sweepExpiredSessions, default 30s tick) when WithJanitor is configured, and (b) an opportunistic sweep on every storeSession once the map reaches sessionEvictThreshold=256, so unbounded growth cannot occur even with the janitor disabled. Janitor.Run selects on ctx.Done() and its worker.Group is Stop()'d on cancellation — no goroutine leak. No unbounded slices/maps found elsewhere in the package. New CallerSession lookups (AssumeRoot, GetWebIdentityToken) reuse the existing LookupSession path and add no new state."}
---

## Notes

Freeform findings and traps for the next auditor.

### Wire-format / protocol
STS is the AWS **query (XML)** protocol (`Version=2011-06-15`), not JSON. Every
response envelope in `models.go` is `XMLName` + `xmlns,attr` + `<Op>Result` +
`ResponseMetadata` in that field order — **this order is load-bearing for XML
marshaling** (Go's `encoding/xml` serialises struct fields in declaration order,
and `<Op>Result` must precede `<ResponseMetadata>` on the wire to match AWS).
**Trap for future `fieldalignment`/govet-driven cleanups**: running
`fieldalignment -fix` (or any tool that reorders struct fields for memory
packing) across the whole package will silently swap `<Op>Result` and
`ResponseMetadata` in every response struct in `models.go`, breaking wire
compatibility while still compiling and passing `go vet`/`golangci-lint`
(fieldalignment has no concept of XML field order). This was caught during
this sweep (five `TestBatch2_*_ResultBeforeMetadata` tests failed after a
package-wide `fieldalignment -fix` run) and reverted; **never run a
whole-package `fieldalignment -fix` here — scope it to a single non-wire file
(e.g. `backend.go`) or hand-edit the target struct instead.**

### Credentials / ID shapes (verified correct, no changes)
- Access key IDs: `ASIA` + 16 upper-alnum chars (`generateAccessKeyID`).
- Assumed-role ARN: `arn:aws:sts::ACCOUNT:assumed-role/ROLE_NAME/SESSION` — note
  that an IAM path is **stripped**: `role/team/dev/MyRole` → `assumed-role/MyRole/SESSION`,
  only the final path segment survives (`buildAssumedRoleArn`/`roleNameFromResource`).
  This is a common trap: naively keeping the full path produces a wire-invalid ARN.
- AssumedRoleId: `AROA` + 16-char derived suffix + `:` + session name
  (`deriveRoleID`) — deterministic from the role ARN so repeated `AssumeRole`
  calls for the same role produce a stable role-ID prefix.
- `Expiration` fields are RFC3339 strings (`time.RFC3339`), which is correct for
  the query/XML protocol (unlike JSON-protocol services, which need
  `pkgs/awstime.Epoch()` — STS does **not** want epoch numbers here).

### GetWebIdentityToken / GetDelegatedAccessToken are real ops, not gopherstack extensions
The pinned `aws-sdk-go-v2/service/sts@v1.43.5` ships
`api_op_GetWebIdentityToken.go` and `api_op_GetDelegatedAccessToken.go` — both
are genuine, documented AWS STS actions (apparently added to the API after an
earlier audit pass assumed otherwise). This sweep found and corrected three
places that encoded the stale "GetWebIdentityToken is not real" belief:
`Handler.GetSupportedOperations()`'s doc comment + missing list entry,
`sdk_completeness_test.go`'s `notImplemented` acknowledgement list, and
`TestParity_GetWebIdentityToken_NotInSupportedOps` (inverted to
`TestParity_GetWebIdentityToken_InSupportedOps`). **If a future SDK bump adds
more ops, re-run `TestSDKCompleteness` first** — it is the authoritative
sentinel for this class of drift (it diffs `GetSupportedOperations()` against
the real SDK client's method set via reflection).

### GetWebIdentityToken SigningAlgorithm — only two values are valid
`aws-sdk-go-v2/service/sts@v1.43.5`'s doc comment for `SigningAlgorithm` is
explicit: *"Valid values are RS256 (RSA with SHA-256) and ES384 (ECDSA using
P-384 curve with SHA-384)."* The emulator previously accepted a 9-value JOSE
allowlist (RS256/RS384/RS512/ES256/ES384/ES512/PS256/PS384/PS512) inherited
from generic JWT-library conventions rather than the STS API's actual
constraint — narrowed to `{RS256, ES384}` this pass. A test
(`TestRefinement2_GetWebIdentityTokenValidSigningAlgorithms`, renamed to
`TestRefinement2_GetWebIdentityTokenSigningAlgorithms`) had asserted the wrong
(permissive) behaviour as correct; it is now a table of accept/reject cases.

### GetDelegatedAccessToken TradeInToken — was an unvalidated pass-through
`types.ExpiredTradeInTokenException` exists in the SDK ("The trade-in token
provided in the request has expired and can no longer be exchanged for
credentials") and the input doc comment says the token "must be valid and
unexpired at the time of the request" — yet the handler accepted **any**
non-empty string forever. Fixed by adding `validateTradeInTokenExpiry`
(`tokenvalidation.go`), which — mirroring the existing JWT-exp handling already
used for `WebIdentityToken` — checks the `exp` claim only when the token is
JWT-shaped (three non-empty dot-separated segments); opaque test-fixture tokens
remain accepted unchanged (no external issuer keys are available to verify a
signature either way, matching the emulator's existing stance on
WebIdentityToken/SAMLAssertion). New sentinel `ErrExpiredTradeInToken` maps to
`ExpiredTradeInTokenException` / HTTP 400 in `mapErrorToCode`.

### Locking
`InMemoryBackend.mu` was a raw `sync.Mutex` (violates
`.claude/memories/pkgs-catalog.md`'s "never scatter raw sync.Mutex — use
lockmetrics.RWMutex" rule); every other audited service backend already uses
`lockmetrics.RWMutex`. Converted with per-call-site operation labels; read-only
accessors use `RLock`/`RUnlock`. No behavioural change — `Lock`/`Unlock` stayed
write-exclusive everywhere a map mutation (session store/delete) occurs.

### Not touched (shared files / out of scope)
`cli.go`'s `stsBk.SetRoleLookup(...)` / `stsBk.SetOIDCLookup(...)` wiring was
read but not modified — the `RoleLookup`/`OIDCLookup` interfaces in
`backend.go` are unchanged (additive-safe; no signature changes were needed).

### Re-audit 2026-07-11 (HEAD eb94f3c3, no changes made)
Ran the standard re-audit protocol before touching any code:
- `git diff ce30166a..eb94f3c3 -- services/sts/` (the commit that actually
  authored/committed this ledger, since `0407b38d` predates the sweep-3 squash
  merge and is not an ancestor of any commit on this branch) — **empty**, no
  local drift in `services/sts/` since the last audit.
- `go.mod` bumped `aws-sdk-go-v2/service/sts` v1.43.5 → v1.44.0 in the interim
  (dependency-upgrade commit `e51c0de9`, unrelated to sts specifically). Diffed
  the two module versions on disk
  (`go/pkg/mod/github.com/aws/aws-sdk-go-v2/service/sts@{v1.43.5,v1.44.0}`):
  zero `api_op_*.go` or `types/` differences — the only changes upstream were
  added `serde_snapshot`/`serde_snapshot_test.go` test-infrastructure files.
  No new/changed operations, no new error types to model.
- No `TODO`/`FIXME`/`XXX`/`HACK` markers in non-test source.
- All five gates re-verified green with zero changes:
  `go build`, `go vet`, `go test -race` (ok, 2.956s), `go fix -diff` (empty),
  `golangci-lint run` (0 issues).

Conclusion: nothing to fix this pass. All `ops`/`families` rows above are
carried forward unchanged from the 2026-07-05 audit; `gaps`/`deferred` items
remain open (still no reliable spec for the three unimplemented exception
types; session-policy-content enforcement still correctly out of scope for a
service-local audit).

### Re-audit 2026-07-24: AssumeRoleWithSAML/AssumeRoleWithWebIdentity had invented wire params
The 2026-07-11 (and earlier) audits repeatedly marked `AssumeRoleWithSAML` and
`AssumeRoleWithWebIdentity` `wire: ok` on the strength of "no stub" behavioral
checks — real state mutation, correct-looking response shapes — without
actually diffing `AssumeRoleWithSAMLInput`/`AssumeRoleWithWebIdentityInput`
field-for-field against the pinned SDK's `api_op_*.go` struct definitions.
Doing that diff this pass found both were accepting **top-level request
parameters that do not exist in the real API**:

- `AssumeRoleWithSAMLInput` (real fields: `PrincipalArn`, `RoleArn`,
  `SAMLAssertion`, `DurationSeconds`, `Policy`, `PolicyArns` — confirmed
  against `api_op_AssumeRoleWithSAML.go`) — gopherstack additionally accepted
  `RoleSessionName`, `SourceIdentity`, and `Tags` as if they were separate
  wire parameters, parsed straight off `r.FormValue(...)` /
  `Tags.member.N.*` in `handler_saml.go`.
- `AssumeRoleWithWebIdentityInput` (real fields: `RoleArn`,
  `RoleSessionName`, `WebIdentityToken`, `DurationSeconds`, `Policy`,
  `PolicyArns`, `ProviderId`) — gopherstack additionally accepted
  `SourceIdentity` and `Tags` the same way.

This is not a cosmetic gap: a real `aws-sdk-go-v2` client calling either
operation has **no way to set these fields on the wire** — the generated
`AssumeRoleWithSAMLInput`/`AssumeRoleWithWebIdentityInput` Go structs the SDK
serializes simply don't have the fields, so the emulator's prior behavior
(accepting them as if AWS did) could never be exercised by a real SDK client
and would only work with hand-rolled form-encoded requests bypassing the SDK
entirely — the opposite of parity.

The real mechanism, per both operations' `Output` doc comments, is that AWS
derives these values **server-side** from the credential material itself:
`AssumeRoleWithSAMLOutput.SourceIdentity` / `.Subject` / `.SubjectType` /
`.Issuer` / `.Audience` come from the SAML assertion's `<Attribute>` /
`<NameID>` / `<Issuer>` / `<SubjectConfirmationData>` elements (see
`saml_attributes.go`'s `extractSAMLAssertionData`), and
`AssumeRoleWithWebIdentityOutput.SourceIdentity` (and, by the same convention
this package already used for `GetWebIdentityToken`, session tags) come from
custom claims in the `WebIdentityToken` JWT (see `web_identity.go`'s
`extractWebIdentitySourceIdentity`/`extractWebIdentityTags` and
`jwtClaimSourceIdentity`/`jwtClaimTags` in `token_validation.go`).

**Lesson for future auditors**: "no stub" (real state mutation, plausible
response shape) is necessary but not sufficient for `wire: ok`. An op can look
completely real — correct XML envelope, correct error codes, a session
actually stored — while still accepting invented request parameters a real
SDK client could never send. The only way to catch this class of bug is a
literal field list diff against the SDK's generated `Input`/`Output` structs
(or, better, its `serializers.go`/`deserializers.go`, which are authoritative
for what actually goes on the wire).

### Re-audit 2026-07-24: GetDelegatedAccessToken had an invented DurationSeconds parameter
Same root cause as above, smaller blast radius: `GetDelegatedAccessTokenInput`
has exactly one field in the real SDK (`TradeInToken`) — confirmed against
both the struct definition in `api_op_GetDelegatedAccessToken.go` and its
`awsAwsquery_serializeOpDocumentGetDelegatedAccessTokenInput` serializer
(neither references `DurationSeconds`). gopherstack had invented a
`DurationSeconds` wire parameter, accepted by `handler_delegated_access.go`
and validated against the same 900-43200s range as `AssumeRole`. Removed;
the backend now always issues `DefaultDurationSeconds` (3600s) credentials.

### Re-audit 2026-07-24: SessionDurationEscalationException and AssumeRoot SourceIdentity
Two smaller closable gaps found by reading the SDK's per-operation error
dispatch tables (`deserializers.go`'s `awsAwsquery_deserializeOpError<Op>`
functions) and `Output` doc comments rather than just the `types/errors.go`
doc comments in isolation:

- `SessionDurationEscalationException` is dispatched specifically in
  `GetWebIdentityToken`'s error branch (alongside
  `JWTPayloadSizeExceededException` and
  `OutboundWebIdentityFederationDisabledException`, which remain unmodeled —
  see `gaps`). Its doc comment ("you cannot use this operation to extend the
  lifetime of a session beyond what was granted when the session was
  originally created") maps cleanly onto a caller using temporary STS
  credentials to request a `GetWebIdentityToken` JWT whose `DurationSeconds`
  would outlive their own session — implemented via a new
  `GetWebIdentityTokenInput.CallerSession` populated from the SigV4
  Authorization header, mirroring the existing `AssumeRole` role-chaining
  `CallerSession` pattern.
- `AssumeRootOutput.SourceIdentity` was always empty — `AssumeRootInput` has
  no `SourceIdentity` parameter, and the doc comment says source identity
  "persists across chained role sessions", so it must be inherited from the
  calling principal's own session. Added the same `CallerSession` pattern to
  `AssumeRoot`.
