---
service: sts
sdk_module: aws-sdk-go-v2/service/sts@v1.44.0   # version audited against (pinned in go.mod)
last_audit_commit: eb94f3c3                     # HEAD when this manifest was written
last_audit_date: 2026-07-11
overall: B                # already-accurate op-by-op; no local drift and no SDK surface
                           # changes since the previous audit (see "Re-audit 2026-07-11" note
                           # below) — ok rows below carried forward unchanged.
ops:
  AssumeRole: {wire: ok, errors: ok, state: ok, persist: ok, note: "trust-policy Principal/Condition/Effect evaluation, ExternalId, MFA absent (n/a for this op), role-chaining 1h cap, transitive tags, PackedPolicySize — all verified correct pre-existing"}
  AssumeRoleWithSAML: {wire: ok, errors: ok, state: ok, persist: ok, note: "base64+temporal Conditions window, NameQualifier BASE64(SHA1(issuer;acct;idp)) per AWS spec, PrincipalArn shape — verified correct"}
  AssumeRoleWithWebIdentity: {wire: ok, errors: ok, state: ok, persist: ok, note: "JWT exp/nbf/iat self-consistency checks, OIDC lookup, trust-policy federated evaluation — verified correct"}
  AssumeRoot: {wire: ok, errors: ok, state: ok, persist: ok, note: "approved TaskPolicyArn allowlist, fixed 900s duration, arn:aws:sts::ACCT:assumed-root — verified correct"}
  GetCallerIdentity: {wire: ok, errors: ok, state: ok, persist: ok, note: "session-token mismatch -> InvalidClientTokenId (not AccessDenied), expired session -> ExpiredTokenException — verified correct"}
  GetDelegatedAccessToken: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: TradeInToken was accepted forever with no expiry check (disguised stub); now validates a JWT-shaped token's exp claim and returns ExpiredTradeInTokenException, matching the real exception AWS documents for this op"}
  GetFederationToken: {wire: ok, errors: ok, state: ok, persist: ok, note: "federated-user ARN/ID shape, tag/policy-arn/packed-policy validation — verified correct"}
  GetSessionToken: {wire: ok, errors: ok, state: ok, persist: ok, note: "MFA serial+code pairing and format validation, 900-129600s range — verified correct"}
  GetWebIdentityToken: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: (1) SigningAlgorithm accepted 9 values (RS256/RS384/RS512/ES256/ES384/ES512/PS256/PS384/PS512) but the real API only documents RS256 and ES384 as valid — narrowed to match; (2) op was wrongly excluded from GetSupportedOperations/SDK-completeness list on the (now-stale) belief it was a gopherstack-only extension — it is a real op in the pinned SDK (api_op_GetWebIdentityToken.go) and is now listed + counted"}
  GetAccessKeyInfo: {wire: ok, errors: ok, state: ok, persist: ok, note: "session lookup then well-formed-prefix fallback to backend account ID — verified correct"}
  DecodeAuthorizationMessage: {wire: ok, errors: ok, state: ok, persist: n/a, note: "HMAC-signed self-issued messages verified; foreign base64 blobs decoded permissively for emulator usability — verified correct"}
families:
  trust-policy-evaluation: {status: ok, note: "Principal (AWS/Federated/Service/wildcard), Action (incl. wildcard glob), Effect Allow/Deny, Condition (StringEquals/StringLike/NotEquals/NotLike + IfExists, case-insensitive keys) all implemented in trustpolicy.go and independently verified against the statements in AssumeRole/WithSAML/WithWebIdentity — no changes needed"}
  session-tag-validation: {status: ok, note: "key/value length, charset, aws: reserved prefix, case-insensitive dup detection, MaxTagCount=50, transitive-tag merge on role chaining — verified correct"}
  locking: {status: fixed, note: "InMemoryBackend used a raw sync.Mutex, violating pkgs-catalog.md's rule to use lockmetrics.RWMutex for backend maps. Converted mu to *lockmetrics.RWMutex (New(\"sts\")), split read-only accessors (AccountID, getEffectiveMaxDuration, roleDerivedMaxDuration, lookupRoleMeta, validateOIDCProvider, SessionCounts, Snapshot, GetAccessKeyInfo lookup) to RLock, kept map-mutating paths (storeSession, evictExpiredSessionsLocked callers, LookupSession, GetCallerIdentity, ValidateSessionCredential, Reset, Restore, janitor sweep) on Lock, each with an operation-name label for the gopherstack_lock_* metrics."}
gaps:
  - "JWTPayloadSizeExceededException, OutboundWebIdentityFederationDisabledException, SessionDurationEscalationException are real error types in aws-sdk-go-v2/service/sts/types but the SDK ships no doc comment describing their trigger thresholds/semantics beyond the type name; the emulator does not currently model any of the three (no JWT-tag-payload size cap, no account-level web-identity-federation-disabled flag, no explicit escalation-attempt detection beyond the existing silent 1h role-chain clamp). Not fixed this pass — no reliable spec to implement against without further doc research. (bd: gopherstack-p05, follow-up)"
deferred:
  - "SESSION-POLICY EVALUATION: session Policy/PolicyArns are validated for shape/size (MalformedPolicyDocument, PackedPolicyTooLarge) and PackedPolicySize is computed, but the policy document's *content* is not enforced against subsequent API calls (no IAM policy-evaluation engine wired to session credentials). This mirrors the rest of the emulator's authz model and is out of scope for a service-local sts audit."
  - "GetWebIdentityToken Tags payload-size cap (JWTPayloadSizeExceededException) — see gaps above."
leaks: {status: clean, note: "Sessions map is bounded by (a) the background Janitor (sweepExpiredSessions, default 30s tick) when WithJanitor is configured, and (b) an opportunistic sweep on every storeSession once the map reaches sessionEvictThreshold=256, so unbounded growth cannot occur even with the janitor disabled. Janitor.Run selects on ctx.Done() and its worker.Group is Stop()'d on cancellation — no goroutine leak. No unbounded slices/maps found elsewhere in the package."}
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
