---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: acmpca
sdk_module: aws-sdk-go-v2/service/acmpca@v1.46.10   # version audited against
last_audit_commit: 1c4ee34e                          # HEAD when this manifest was written
last_audit_date: 2026-07-23
overall: A            # all 8 gaps + both deferred families closed (fully or partially, see notes); 2 new wire bugs found+fixed
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
