---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: acmpca
sdk_module: aws-sdk-go-v2/service/acmpca@v1.50.0   # version audited against
last_audit_commit: 3cec3729                          # HEAD when this manifest was written
last_audit_date: 2026-08-20
overall: A            # wrapper-key/nested-shape re-audit this pass: zero new wire bugs found (see Notes)
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateCertificateAuthority: {wire: ok, errors: ok, state: ok, persist: ok, note: "ROOT auto-signs+activates; SUBORDINATE -> PENDING_CERTIFICATE. FIXED THIS PASS: IdempotencyToken now deduplicated (5-min window); KeyStorageSecurityStandard/UsageMode/RevocationConfiguration now accepted, validated, stored, and echoed (previously entirely absent from the model -- a gap not listed in the prior manifest, found via full field-diff)."}
  DescribeCertificateAuthority: {wire: ok, errors: ok, state: ok, persist: ok, note: "reports RestorableUntil, LastStateChangeAt (new field, fixed this pass), KeyStorageSecurityStandard, UsageMode, RevocationConfiguration (omitted entirely when unconfigured, matching a nil *types.RevocationConfiguration). A CA past its RestorableUntil deadline now correctly returns ResourceNotFoundException (fixed this pass -- see gaps)."}
  ListCertificateAuthorities: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED THIS PASS: ResourceOwner now validated and enforced -- SELF/empty lists this account's CAs, OTHER_ACCOUNTS returns an empty page (no cross-account sharing modeled), anything else is InvalidParameterException. Also now filters out CAs past their RestorableUntil deadline."}
  DeleteCertificateAuthority: {wire: ok, errors: ok, state: ok, persist: ok, note: "tracks RestorableUntil (default 30d) and sets LastStateChangeAt (new field, fixed this pass)."}
  UpdateCertificateAuthority: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED THIS PASS: now accepts RevocationConfiguration (omitting the field leaves the CA's existing configuration unchanged, matching the real API's documented semantics -- distinguished from an explicit null via a custom UnmarshalJSON tracking which wire keys were present); sets LastStateChangeAt on status change."}
  RestoreCertificateAuthority: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED THIS PASS: clears RestorableUntil and now correctly rejects a restore attempted after the RestorableUntil deadline (ResourceNotFoundException, matching real AWS permanently removing the CA once its restoration window ends) -- see caGet/casInRegion in store.go, the single choke point every CA read/write goes through."}
  GetCertificateAuthorityCsr: {wire: ok, errors: ok, state: ok, persist: ok}
  ImportCertificateAuthorityCertificate: {wire: ok, errors: ok, state: ok, persist: ok, note: "sets LastStateChangeAt (fixed this pass)."}
  GetCertificateAuthorityCertificate: {wire: ok, errors: ok, state: ok, persist: ok}
  IssueCertificate: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED THIS PASS (severe wire bug, found via field-diff): the certificate ARN's final path segment must be the certificate's own serial number in decimal (see IssueCertificateOutput's doc example) -- gopherstack instead appended an unrelated crypto/rand ID, meaning every issued cert ARN was wrong-shaped. Also FIXED: IdempotencyToken deduplication (5-min window); TemplateArn now gates ApiPassthrough per the real API's documented 'ignored unless an APIPassthrough/APICSRPassthrough template variant is selected' rule; ApiPassthrough now really applies Subject/KeyUsage/ExtendedKeyUsage/SubjectAlternativeNames(DNS+IP+email)/CustomExtensions overrides to the issued cert (previously silently ignored entirely). UsageMode=SHORT_LIVED_CERTIFICATE now enforces the real API's 7-day validity cap. Still not implemented: ApiPassthrough.Extensions.CertificatePolicies, the ASN1Subject RDN types beyond CommonName/Country/Organization/OrganizationalUnit/State/Locality/SerialNumber, and the GeneralName variants beyond DnsName/IpAddress/Rfc822Name -- all explicitly REJECTED (InvalidParameterException) rather than silently dropped when a caller sets them; TemplateArn's per-template default extension profile (e.g. SubordinateCACertificate_PathLenN's path-length constraint) is not modeled beyond the APIPassthrough-gating behavior. END_DATE validity type is still treated as epoch seconds like ABSOLUTE rather than true UTCTime/GeneralizedTime -- pre-existing intentional simplification, unchanged this pass (see Traps)."}
  GetCertificate: {wire: ok, errors: ok, state: ok, persist: ok}
  RevokeCertificate: {wire: ok, errors: ok, state: ok, persist: ok, note: "CORRECTED THIS PASS: the prior manifest's gap note ('does not require CRL/OCSP to be enabled before revoking') was a misdiagnosis -- re-checked against the real SDK's RevokeCertificate doc comment, which describes CRL/OCSP as purely optional side-effects of revocation, not a precondition for it. No such requirement exists in the real API; this was never actually a gap and no fix was needed or made."}
  ListPermissions: {wire: ok, errors: ok, state: ok, persist: ok}
  CreatePermission: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED THIS PASS: Principal now validated to be exactly 'acm.amazonaws.com', the only value the real API accepts per CreatePermissionInput.Principal's doc comment ('At this time, the only valid principal is acm.amazonaws.com')."}
  DeletePermission: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateCertificateAuthorityAuditReport: {wire: ok, errors: ok, state: ok, persist: ok, note: "synchronous SUCCESS; real AWS is async (CREATING->SUCCESS/FAILED) but this is a reasonable simplification for an emulator."}
  DescribeCertificateAuthorityAuditReport: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  PutPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  TagCertificateAuthority: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED THIS PASS: enforces the real API's 50-tag-per-CA limit, returning TooManyTagsException (mapped to the real exception's ErrorCode) when exceeded."}
  UntagCertificateAuthority: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTags: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED THIS PASS: MaxResults/NextToken now paginate for real (via pkgs/page, same pattern as every other list op in this service) instead of always returning the full tag set in one page. The invented 'ListTagsForCertificateAuthority' op alias was DELETED (see Notes) -- it does not exist anywhere in aws-sdk-go-v2; the real op is ListTags only."}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "NEW (found this pass): CertificateAuthority.FailureReason (types.FailureReason: REQUEST_TIMED_OUT/UNSUPPORTED_ALGORITHM/OTHER) and CertificateAuthorityStatus's FAILED/EXPIRED enum values are entirely unmodeled -- CreateCertificateAuthority is synchronous and always succeeds or returns an immediate validation error, so no CA ever reaches FAILED, and no expiry-driven ACTIVE->EXPIRED transition is simulated. FailureReason is correctly never emitted (matching the real API omitting it whenever Status != FAILED), so this is a state-machine depth gap, not a wire-shape bug -- disclosed, not fixed (would need a new terminal status + expiry sweep, out of scope for a wrapper-key/nesting sweep)."
  - "NEW (found this pass): CertificateAuthorityConfiguration.CsrExtensions (nested CsrExtensions{KeyUsage, SubjectInformationAccess->AccessDescription{AccessMethod,GeneralName}}) is accepted by neither CreateCertificateAuthority's input decoding (caConfigInput has no CsrExtensions field) nor echoed by Describe/List -- silently dropped on the request side rather than rejected. Real AWS would echo a caller-supplied CsrExtensions back on every subsequent Describe/List; gopherstack never stores it, so a caller setting it gets no error but also never sees it round-trip. Disclosed, not fixed -- same class of gap as the already-documented ASN1Subject exotic RDN types, but this one lacks the explicit-rejection treatment those get in decodeASN1Subject/decodeExtensions (handler_certificates.go); a caller has no signal the field was ignored."
  - ApiPassthrough.Extensions.CertificatePolicies is rejected (InvalidParameterException) rather than implemented -- would require arbitrary OID/PolicyQualifier ASN.1 encoding beyond a simple pkix.Extension passthrough
  - ApiPassthrough.Subject's exotic RDN types (DistinguishedNameQualifier, GenerationQualifier, Initials, Pseudonym, Surname, Title, CustomAttributes) are rejected rather than implemented -- crypto/x509's pkix.Name has no direct fields for most of these
  - ApiPassthrough.Extensions.SubjectAlternativeNames' exotic GeneralName variants (OtherName, DirectoryName, EdiPartyName, UniformResourceIdentifier, RegisteredId) are rejected rather than implemented -- only DnsName/IpAddress/Rfc822Name (the three Terraform's aws_acmpca_certificate resource actually exposes) are modeled
  - TemplateArn's per-template default X.509 extension profile (e.g. SubordinateCACertificate_PathLenN's CA path-length constraint, OCSPSigningCertificate/CodeSigningCertificate's preset KeyUsage/ExtendedKeyUsage) is not modeled; only the documented APIPassthrough/APICSRPassthrough-gating behavior (whether ApiPassthrough is honored at all) is implemented -- every issued cert uses the same flat extension baseline (optionally overridden by ApiPassthrough) regardless of TemplateArn's specific value
  - RevocationConfiguration.CrlConfiguration/OcspConfiguration's CNAME fields (CustomCname, OcspCustomCname) and S3BucketName are accepted as any non-empty string; the real API's RFC2396/S3-bucket-naming-rule validation is not enforced
  - IssueCertificate's END_DATE validity type is still treated as Unix epoch seconds (same as ABSOLUTE) rather than true UTCTime/GeneralizedTime -- pre-existing intentional simplification, not touched this pass (see Traps)
  - DELETED CAs past their RestorableUntil deadline are hidden from every read path (Describe/List/Get/Issue/etc. all treat them as not-found, matching real AWS's user-visible behavior) and RestoreCertificateAuthority correctly rejects them, but the row is not physically freed from the in-memory store.Table until the next process Reset() -- consistent with how every other terminal-state resource in this backend (revoked certs, etc.) is retained rather than garbage-collected; not a new leak, just not a true memory-reclaiming sweep
deferred: []              # both prior deferred items (ApiPassthrough, TemplateArn) now substantially implemented -- remaining edges tracked under gaps above
leaks: {status: clean, note: "no goroutines/janitors in this service; all state lives in store.Table/store.Index behind the coarse b.mu lockmetrics.RWMutex, matching pkgs-catalog guidance. The RestorableUntil-deadline enforcement added this pass is a lazy read-time filter (caGet/casInRegion in store.go), not a background sweep -- no new goroutine, no new lock, no new leak surface."}
---

## Notes

Protocol: awsjson1.1 (single POST, `X-Amz-Target: ACMPrivateCA.<Op>`; RouteMatcher prefix
`"ACMPrivateCA."` confirmed against the SDK's `ServiceID`/operation names — correct).

### 2026-08-20 re-audit: wrapper-key / nested-shape sweep (zero new wire bugs)

Scope: this pass targeted the wrapper-key/nesting-level/JSON-type/enum-value bug class
specifically (not a full re-audit of state/errors/persist, which the 2026-07-23 pass
already covered in depth). All 23 ops in `GetSupportedOperations()` were enumerated and
cross-checked 1:1 against `ls $(go env GOMODCACHE)/.../acmpca@v1.50.0/api_op_*.go` (23
files, exact match, no drift since the last pass).

**Protocol reconfirmed independently**: `grep -c '^func awsAwsjson11_deserializeOp'
deserializers.go` → 35 (defined and called; JSON-RPC, not the restjson flat-body false-
positive trap the brief warned about — that trap does not apply here, confirmed rather
than assumed).

**Every op's Input/Output struct** (`api_op_*.go`) was read directly and diffed field-by-
field against gopherstack's wire structs in `handler_certificate_authorities.go`,
`handler_certificates.go`, `handler_audit_reports.go`, `handler_ca_policy.go`,
`handler_permissions.go`, `handler_tags.go`. Every wrapper key, nesting level, and JSON
type matched exactly. Every emitted enum value (`CertificateAuthorityStatus`,
`CertificateAuthorityType`, `KeyStorageSecurityStandard`, `CertificateAuthorityUsageMode`,
`CrlType`, `S3ObjectAcl`, `ResourceOwner`, `RevocationReason`, `AuditReportStatus`,
`AuditReportResponseFormat`, `ActionType`) was grepped in `models.go`/`permissions.go` and
compared byte-for-byte against `types/enums.go` — all exact matches, no invented values,
no case mismatches.

**GeneralName** (the 8-member mutually-exclusive union: `DnsName`, `IpAddress`,
`Rfc822Name`, `DirectoryName`, `EdiPartyName`, `OtherName`, `UniformResourceIdentifier`,
`RegisteredId` — confirmed 8, not 9, against `types.go:594-628`) never appears on any
response shape in the real SDK; it exists only inside
`IssueCertificateInput.ApiPassthrough.Extensions.SubjectAlternativeNames`, which
gopherstack never echoes back anywhere. So the "request-only field leaking into a
response" bug class does not apply to `GeneralName` here — verified by grep, not assumed.
On the request side (`generalNameWire`/`decodeGeneralName`, `handler_certificates.go`),
all 8 variants are represented in the wire struct; the 3 Terraform actually uses
(`DnsName`/`IpAddress`/`Rfc822Name`) are implemented, the other 5 are explicitly rejected
with `InvalidParameterException` rather than silently dropped — correct treatment, no
change needed.

**Request-only-field-in-response check**: `ApiPassthrough` (the other main
request/response-shared-shape risk named in the brief) is `IssueCertificateInput`-only —
`IssueCertificateOutput` has just `CertificateArn`, confirmed by reading the real
`api_op_IssueCertificate.go` struct directly — and gopherstack's `issueCertificateOutput`
correctly has no `ApiPassthrough`-derived fields. Clean.

**Two new (small, pre-existing) gaps found and disclosed, not fixed** — see `gaps` above:
`FailureReason`/`CertificateAuthorityStatus`'s `FAILED`/`EXPIRED` values (state-machine
depth, not a wire bug — the field is correctly never emitted since no CA ever reaches
those states), and `CertificateAuthorityConfiguration.CsrExtensions` (silently dropped on
input rather than explicitly rejected like the sibling exotic-field gaps get). Both are
Layer 3 in nature (missing feature depth), surfaced incidentally while diffing
`CertificateAuthority`'s and `CertificateAuthorityConfiguration`'s full field lists against
`types/types.go:160-266`, not from a dedicated Layer-3 hunt.

**Existing tests**: every wire-key assertion in `handler_certificate_authorities_test.go`,
`handler_certificates_test.go`, `handler_tags_test.go`, `handler_permissions_test.go`,
`handler_audit_reports_test.go`, `handler_ca_policy_test.go`, `handler_sdk_route_table_test.go`,
and `api_passthrough_test.go` was spot-checked against the real SDK structs; none assert a
wrong key/nesting/type/value. No test correction was needed this pass (contrast with
redshiftdata's three wrong-key tests the same session).

**Added**: `wire_sdk_roundtrip_test.go` —
`TestDescribeCertificateAuthority_SDKRoundTrip`, this service's first *typed* real-SDK-
client round-trip test (prior coverage was raw-JSON-map assertions via
`handler_*_test.go`'s `doACMPCARequest`, which can't detect a case-sensitive key miss the
way a typed client's deserializer can). It creates a CA with a full
`CertificateAuthorityConfiguration.Subject` and `RevocationConfiguration`
(`CrlConfiguration`+`OcspConfiguration`), describes it back through the real
`acmpcasdk.Client`, and asserts every nested field survived. **Proven meaningful by hand-
revert**: changed `certAuthorityOutput.RevocationConfiguration`'s JSON tag from
`"RevocationConfiguration"` to `"revocationConfiguration"` (lowercase r) — the test failed
exactly as predicted (`ca.RevocationConfiguration` came back `nil` through the real
client, since `awsAwsjson11_deserializeOpDocumentDescribeCertificateAuthorityOutput`'s case
switch is case-sensitive and only matches the capitalized key); reverted, confirmed
`git diff` byte-identical and the test green again.

**last_audit_commit provenance re-checked**: the prior manifest's `last_audit_commit:
1c4ee34e` is dated `Sun Jul 19 14:07:07 2026` (`git show -s --format=%ad`), 4 days before
`last_audit_date: 2026-07-23` — not the days-to-weeks-stale pattern the re-audit protocol
warns about; `git diff 1c4ee34e..<pre-this-pass-HEAD> --stat -- services/acmpca/` showed
~2,658 insertions across 28 files, matching the prior manifest's own extensive "FIXED THIS
PASS" narrative line-for-line. Provenance is genuine, not a copy-paste sha. No prose/header
SDK-version mismatch found (`sdk_module` header says `v1.50.0`; `go.mod` pins the identical
`v1.50.0`; no other version number appears anywhere in this file's prose). Every prior
"FIXED THIS PASS" claim in `ops:` was spot-verified still true by reading the current code
(`RevocationConfiguration` round-trips, `KeyStorageSecurityStandard`/`UsageMode` echo
correctly, `LastStateChangeAt` present, issued-cert ARN uses the decimal serial, etc.) — no
regressions, no stale claims.

### Bugs fixed this pass (real, high-impact)

1. **Issued certificate ARNs embedded the wrong ID.** aws-sdk-go-v2's
   `IssueCertificateOutput` doc comment gives a concrete example ARN ending in
   `.../certificate/286535153982981100925020015808220737245` — that trailing
   number is the certificate's own serial number in decimal. gopherstack instead
   generated an unrelated 16-byte `crypto/rand` ID for that path segment, so
   every issued certificate's ARN was wrong-shaped relative to real AWS (a
   client parsing the ARN to recover the serial, or comparing it against a
   value obtained elsewhere, would get a mismatch). Fixed in
   `certificates.go` (`signAndStoreCertificateLocked`): the ARN is now built
   from `big.Int` decimal formatting of the same hex serial already stored on
   `IssuedCertificate.Serial`.

2. **`ListTagsForCertificateAuthority` was an invented operation** — it does
   not exist anywhere in `aws-sdk-go-v2/service/acmpca` (no
   `api_op_ListTagsForCertificateAuthority.go`; only `ListTags` is real).
   Deleted from `GetSupportedOperations()` and the dispatch switch in
   `handler.go`; a request naming it now correctly gets `InvalidAction` like
   any other unrecognized op, instead of silently succeeding as an alias for
   `ListTags`.

3. **`CertificateAuthority` was missing three real SDK fields entirely**
   (`KeyStorageSecurityStandard`, `UsageMode`, `LastStateChangeAt`) — none of
   the create/describe/list wire shapes carried them at all, which is a gap
   beyond what the prior manifest tracked (that pass's field-diff didn't reach
   these). Added: `KeyStorageSecurityStandard` (validated against the 3-value
   enum, default `FIPS_140_2_LEVEL_3_OR_HIGHER`), `UsageMode` (validated
   against the 2-value enum, default `GENERAL_PURPOSE`, and now enforces the
   real API's 7-day certificate-validity cap for
   `SHORT_LIVED_CERTIFICATE`-mode CAs on `IssueCertificate`), and
   `LastStateChangeAt` (set on every status/certificate-material transition:
   Create, self-sign-activate, Import, Update, Delete, Restore).

4. **`RevocationConfiguration` (CRL/OCSP) was entirely unmodeled** — the
   biggest of the 8 pre-existing gaps. `CreateCertificateAuthority` and
   `UpdateCertificateAuthority` now accept a real `RevocationConfiguration`
   input (validated per the documented constraints: a disabled
   `CrlConfiguration`/`OcspConfiguration` must set only `Enabled=false`; an
   *enabled* `CrlConfiguration` must specify `S3BucketName`; `CrlType` and
   `S3ObjectAcl` are validated against their enums), and
   `DescribeCertificateAuthority`/`ListCertificateAuthorities` now report it
   back — omitting the field entirely when unconfigured, matching how the real
   SDK omits a nil `*types.RevocationConfiguration` rather than emitting an
   empty object. `UpdateCertificateAuthority`'s "omit the field to leave
   existing config unchanged" semantics required a custom `UnmarshalJSON` on
   the wire input type to distinguish "key absent" from "key present with a
   null/zero value", since both unmarshal a Go pointer field to `nil`.

5. **`ApiPassthrough`/`TemplateArn` were both silently ignored** (the two
   prior deferred items). Both are now substantially implemented:
   `TemplateArn` gates `ApiPassthrough` exactly as the real API's doc comment
   describes ("An APIPassthrough or APICSRPassthrough template variant must be
   selected, or else this parameter is ignored"); when gated-in,
   `ApiPassthrough.Subject` (the 6 common X.500 fields + `SerialNumber`) and
   `ApiPassthrough.Extensions` (`KeyUsage`'s 9 bits, `ExtendedKeyUsage` —
   standard types via `crypto/x509` constants where they exist,
   Microsoft/CT-EKU OIDs via `UnknownExtKeyUsage` where they don't, plus
   arbitrary custom OIDs; `SubjectAlternativeNames` for DNS/IP/email;
   `CustomExtensions` via raw `pkix.Extension` passthrough) now really alter
   the signed certificate (see `crypto.go`'s `applyAPIPassthrough`). The
   sub-fields not implemented (`CertificatePolicies`, exotic `ASN1Subject` RDN
   types, exotic `GeneralName` variants) are explicitly **rejected** with
   `InvalidParameterException` when a caller sets them, rather than silently
   dropped — see `handler_certificates.go`'s `decodeASN1Subject`/
   `decodeExtensions`/`decodeGeneralName`, and parity-principles.md's
   no-silent-gaps rule.

6. **`RestorableUntil` was tracked but never enforced.** A DELETED CA now
   becomes invisible (`ResourceNotFoundException`) to every read path once its
   restoration window passes, and `RestoreCertificateAuthority` correctly
   rejects a restore attempted after the deadline — both via a single choke
   point, `caGet`/`casInRegion` in `store.go`, rather than scattered checks
   across every CA-touching function. This is a lazy read-time filter, not a
   background sweep (see leaks note above): no new goroutine was introduced.

7. **`CreateCertificateAuthority`/`IssueCertificate` `IdempotencyToken` was
   accepted but never deduplicated.** Both now recognize repeated calls
   bearing the same token within a 5-minute window (matching the real API's
   documented behavior) and return the original resource's ARN instead of
   creating a duplicate, via a small `(region, op, token) -> (resourceARN,
   expiresAt)` cache on the backend (`store.go`'s `idempotentResourceARN`/
   `rememberIdempotency`) — deliberately not persisted through Snapshot/Restore,
   since it's a short-lived dedup cache, not durable resource state.

8. **`ListCertificateAuthorities`'s `ResourceOwner` was accepted but
   ignored.** Now validated against the real 2-value enum: `SELF`/empty lists
   this account's CAs (unchanged behavior), `OTHER_ACCOUNTS` returns an empty
   page (no cross-account CA sharing is modeled, so no CA is ever owned by
   another account), and any other value is `InvalidParameterException`.

9. **`TagCertificateAuthority` never enforced the 50-tag-per-CA limit.** Now
   returns `TooManyTagsException` when tagging would exceed it (checked
   without mutating state first, via `tagCountAfterMerge` in
   `handler_tags.go`).

10. **`ListTags` never paginated.** `MaxResults`/`NextToken` now behave like
    every other list op in this service (`pkgs/page`), instead of always
    returning the full tag set in one page.

11. **`CreatePermission` accepted any `Principal` string.** Now validated to
    be exactly `"acm.amazonaws.com"`, the only value
    `CreatePermissionInput.Principal`'s doc comment says the real API accepts.

12. **CA/audit-report resource IDs used a flat 32-char hex string with no
    dashes.** `newRandomID` now formats the same entropy as a dashed UUID
    (8-4-4-4-12), matching the shape of real ACM PCA resource IDs (see
    `CreateCertificateAuthorityOutput`'s doc comment example).

### Traps for the next auditor (looks-wrong-but-intentional)

- `IssueCertificate`'s `Validity.Type == "END_DATE"` is handled identically to
  `"ABSOLUTE"` (both treated as Unix epoch seconds via `time.Unix`). Per AWS docs,
  END_DATE is technically UTCTime/GeneralizedTime (`YYMMDDHHMMSS`/`YYYYMMDDHHMMSS`
  as a decimal integer), a different encoding from ABSOLUTE's Unix epoch. This is a
  deliberate prior-sweep simplification (see `TestACMPCA_IssueCertificate_ValidityTypeAliases`),
  left as-is again this pass — implementing true UTCTime parsing for a rarely-used
  validity type wasn't judged worth the risk of breaking existing intentional-design
  tests without a dedicated pass. Flagged here (and in gaps) instead of silently
  left off the manifest.
- `RevokeCertificate` does **not** check whether CRL/OCSP is enabled on the CA
  before revoking — this was flagged as a gap in the prior manifest under the
  theory that real AWS requires it, but re-reading the real SDK's doc comment
  this pass found no such precondition: CRL/OCSP are described purely as
  optional side-effects of revocation (the CRL gets updated, OCSP responses
  change), never as a requirement for the `RevokeCertificate` call to succeed.
  That prior gap note was a misdiagnosis; no fix was needed.
- `RestorableUntil`-past-deadline CAs are hidden by `caGet`/`casInRegion`
  filtering, not physically deleted from `b.cas`/`b.casByRegion` — see the
  `leaks` note above for why this is an accepted tradeoff, not a regression.
- Outbound blob fields the SDK models as `*string` (e.g.
  `GetCertificate`/`GetCertificateAuthorityCertificate`/`GetCertificateAuthorityCsr`'s
  `Certificate`/`CertificateChain`/`Csr` **output** fields) are intentionally
  plain PEM strings, not base64 — this asymmetry (blob on input, string on
  output) is a real AWS API quirk, confirmed against `deserializers.go`, not a
  gopherstack bug. (Carried over from the prior pass's notes; still accurate.)

### Follow-ups for bd (not fixed — out of scope / lower value this pass)

- `ApiPassthrough.Extensions.CertificatePolicies` (OID/PolicyQualifier ASN.1 encoding).
- `ApiPassthrough.Subject`'s exotic RDN types and `SubjectAlternativeNames`'s exotic
  `GeneralName` variants (both explicitly rejected rather than silently dropped).
- `TemplateArn`'s per-template default X.509 extension profiles (path-length
  constraints, OCSP/code-signing presets) beyond the APIPassthrough-gating behavior.
- `RevocationConfiguration`'s CNAME/S3-bucket-name format validation (RFC2396,
  S3 bucket naming rules) — currently any non-empty string is accepted.
- True UTCTime/GeneralizedTime parsing for `Validity.Type == "END_DATE"`.
- NEW (2026-08-20 pass): `CertificateAuthorityStatus`'s `FAILED`/`EXPIRED` values and
  `CertificateAuthority.FailureReason` are entirely unmodeled (no CA ever fails creation
  or expires).
- NEW (2026-08-20 pass): `CertificateAuthorityConfiguration.CsrExtensions` is silently
  dropped on `CreateCertificateAuthority` input rather than stored/echoed or explicitly
  rejected like its `ASN1Subject`/`Extensions` siblings.
