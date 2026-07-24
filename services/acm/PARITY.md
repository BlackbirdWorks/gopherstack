---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: acm
sdk_module: aws-sdk-go-v2/service/acm@v1.37.21   # version audited against
last_audit_commit: HEAD                           # see git log for this pass's commit
last_audit_date: 2026-07-23
overall: A            # A = genuine fix found (wire-shape bug); B = already-accurate, proven op-by-op
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  RequestCertificate: {wire: ok, errors: ok, state: ok, persist: ok, note: "field-diffed this pass against RequestCertificateInput/CertificateOptions: added DomainValidationOptions input (validated + applied, InvalidDomainValidationOptionsException wired), Options.Export input (stored, echoed on Describe/List, see gaps for enforcement scope), SAN-count-exceeded now LimitExceededException (was ValidationException); RSA_1024 weak-key rejection now correctly wrapped as ValidationException instead of escaping to a 500 InternalFailure"}
  DescribeCertificate: {wire: ok, errors: ok, state: ok, persist: ok, note: "RenewalSummary now includes UpdatedAt (required/always-present on real wire, was missing entirely) and RenewalStatusReason; Options.Export added; InvalidArnException wired for malformed CertificateArn"}
  ListCertificates: {wire: ok, errors: ok, state: ok, persist: ok, note: "CertificateSummary previously omitted CreatedAt entirely (always-present real field) -- fixed. Also added RevokedAt/InUse/KeyUsages/ExtendedKeyUsages/ExportOption/Exported(PRIVATE-only)/HasAdditionalSubjectAlternativeNames(always false, correct given our SAN cap), closing the prior gap row. ManagedBy intentionally still omitted (see gaps)."}
  DeleteCertificate: {wire: ok, errors: ok, state: ok, persist: ok, note: "InvalidArnException wired for malformed CertificateArn"}
  ImportCertificate: {wire: ok, errors: ok, state: ok, persist: ok, note: "re-import (CertificateArn set) updates in place; matches AWS. InvalidArnException wired when CertificateArn is supplied and malformed"}
  GetCertificate: {wire: ok, errors: ok, state: ok, persist: ok, note: "correctly rejects PENDING_VALIDATION/FAILED/VALIDATION_TIMED_OUT with RequestInProgressException-style error; InvalidArnException wired"}
  ExportCertificate: {wire: ok, errors: ok, state: ok, persist: ok, note: "restricted to IMPORTED/PRIVATE; fake chain synthesized when none stored, matching AWS always-return-chain behavior; Exported flag now tracked and surfaced on CertificateSummary for PRIVATE certs; InvalidArnException wired. AMAZON_ISSUED still unconditionally rejected -- see gaps for the 2025 exportable-public-certificates feature, deliberately NOT enforced this pass"}
  AddTagsToCertificate: {wire: ok, errors: ok, state: ok, persist: ok, note: "TooManyTagsException (was ValidationException) for >50 tags; InvalidTagException added for empty key or reserved aws: prefix (key or value); InvalidArnException wired"}
  RemoveTagsFromCertificate: {wire: ok, errors: ok, state: ok, persist: ok, note: "InvalidArnException wired"}
  ListTagsForCertificate: {wire: ok, errors: ok, state: ok, persist: ok, note: "InvalidArnException wired"}
  RenewCertificate: {wire: ok, errors: ok, state: ok, persist: ok, note: "IMPORTED/PRIVATE(caArn set) rejected with RequestInProgressException-mapped ErrNotEligible, matching AWS restriction to AMAZON_ISSUED. RenewalSummary.UpdatedAt now set/refreshed on renewal start and on auto-validation completion. InvalidArnException wired"}
  RevokeCertificate: {wire: ok, errors: ok, state: ok, persist: ok, note: "InvalidArnException wired"}
  UpdateCertificateOptions: {wire: ok, errors: ok, state: ok, persist: ok, note: "Export field on the shared CertificateOptions input type is intentionally ignored here (AWS: Export is immutable after creation); InvalidArnException wired"}
  ResendValidationEmail: {wire: ok, errors: ok, state: ok, persist: ok, note: "InvalidArnException wired"}
  GetAccountConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  PutAccountConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "idempotency-token conflict correctly returns ConflictException on mismatched settings"}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - ExportCertificate still unconditionally rejects AMAZON_ISSUED (public) certificates with RequestInProgressException, matching pre-2025 ACM behavior. Real AWS added "exportable public certificates" (public certs created after 2025-06-17 are exportable when Options.Export=ENABLED); Options.Export is now stored/validated/echoed correctly on the wire (RequestCertificate input, DescribeCertificate/ListCertificates output) but ExportCertificate does NOT yet gate AMAZON_ISSUED export on it. Not fixed this pass: the exact error code/condition AWS returns when a public cert lacks Export=ENABLED could not be confirmed from available documentation (RequestInProgressException's documented meaning is specifically "still pending validation", which would misrepresent this condition), and changing this risks fabricating an unverified error contract. Existing test TestACMHandler_ExportCertificate_AmazonIssued_Returns_RequestInProgressException locks in the current (conservative, pre-2025-parity) behavior.
  - CertificateDetail/CertificateSummary omit ManagedBy (AWS: which service, e.g. CLOUDFRONT, manages the cert) — no backend concept of CloudFront-managed certs exists; RequestCertificate's ManagedBy input field is also not accepted. Feature gap, not audited further this pass (field is optional on the real wire; omission is correct-by-absence for certs gopherstack never marks as managed).
  - ValidationMethod=HTTP (DomainValidation.HttpRedirect) is accepted as an input value but not given HTTP-specific handling -- buildInitialDVOList falls through to DNS-style ResourceRecord generation for any non-DNS/non-EMAIL method. Real AWS's HTTP validation method is documented as CloudFront-internal (HttpRedirect "exists only when the certificate type is AMAZON_ISSUED and the validation method is HTTP", set when CloudFront requests certs on a customer's behalf) rather than a method end users normally invoke directly; low value/high uncertainty, left unimplemented.
  - InvalidArgsException and TagPolicyException (both present in the real SDK's types/errors.go) are not wired to any code path -- no tag-policy engine or "invalid args" condition distinct from the other mapped errors exists in gopherstack to trigger them from.
  - RequestCertificate does not accept the ManagedBy input field (CLOUDFRONT); see ManagedBy gap above.
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - AMAZON_ISSUED export gating via Options.Export=ENABLED (2025 exportable-public-certificates feature) — see gaps
  - ManagedBy (CloudFront-managed certificates) end-to-end
  - HTTP validation method / HttpRedirect
leaks: {status: clean, note: "isolation_test.go / leak_test.go already cover timer goroutine lifecycle (Shutdown stops auto-validate timers); Reset()/Close() explicitly stop all pending time.AfterFunc timers; janitor sweeps orphaned timers whose cert was deleted. This pass added no new goroutines/timers -- ExportCertificate's RLock->Lock change (to persist the new Exported flag) and the two new backend methods (ApplyDomainValidationOverrides, SetExportPreference) all use the existing b.mu lock with clean defer-release, verified via -race across the full suite."}
---

## Notes (2026-07-23 pass)

- **Error-code corrections found this pass** (field-diffed against
  `aws-sdk-go-v2/service/acm@v1.37.21/types/errors.go` and the live AWS API
  reference docs for RequestCertificate/ExportCertificate, which enumerate
  each operation's actual `Errors` section):
  - Malformed (non-empty, wrong-shape) `CertificateArn` now returns
    `InvalidArnException` (400) instead of falling through to
    `ResourceNotFoundException`, on every op that takes a `CertificateArn`.
    A new `validateCertArn`/`certArnPattern` in certificate_validation.go
    checks the real ACM ARN shape
    (`arn:<partition>:acm:<region>:<account>:certificate/<id>`); empty ARNs
    are deliberately left alone (existing per-op "required" checks and
    not-found fallbacks already handle those correctly).
  - RequestCertificate's domain-count-exceeded case now returns
    `LimitExceededException` (an ACM *quota* error per AWS docs) instead of
    `ValidationException`.
  - Tag-count-exceeded (`AddTagsToCertificate`/`RequestCertificate` Tags)
    now returns `TooManyTagsException` instead of `ValidationException`.
  - A new `InvalidTagException` covers empty tag keys and the AWS-reserved
    `aws:` key/value prefix (previously unvalidated).
  - `RequestCertificate` with `KeyAlgorithm: RSA_1024` (a real but
    import-only-supported enum value) previously escaped
    `handleOpError`'s known-error `errors.Is` switch entirely (the
    `errWeakKey` sentinel was never wrapped with `ErrInvalidParameter`) and
    was reported as a 500 `InternalFailure` with no client-facing signal of
    what went wrong. Now correctly wrapped and reported as 400
    `ValidationException`.

- **Wire-shape gaps closed this pass**:
  - `RenewalSummary` (nested in `DescribeCertificate`'s `CertificateDetail`)
    was missing `UpdatedAt` -- a `This member is required` field on the real
    SDK type, meaning it is *always* present on the real wire. Added, set on
    renewal start and refreshed when the renewal's own domain validation
    completes.
  - `RenewalSummary.RenewalStatusReason` (optional `FailureReason`) added.
  - `CertificateSummary` (the `ListCertificates` response shape) was missing
    `CreatedAt` entirely -- an always-present field on the real wire, not an
    optional one gopherstack could legitimately omit. Fixed. Also added
    `RevokedAt`, `InUse` (derived from the existing `InUseBy` tracking),
    `KeyUsages`/`ExtendedKeyUsages` (same data already computed for
    `DescribeCertificate`, just not projected into the summary),
    `HasAdditionalSubjectAlternativeNames` (always `false`, which is correct
    given gopherstack's SAN count never approaches the real 100-name cap
    this field guards), `Exported` (PRIVATE-type only, matching the real
    field's documented scope) and `ExportOption`. This closes the gap row
    from the prior pass ("CertificateSummary omits optional AWS fields
    ... entirely"). `ManagedBy` remains out of scope (see gaps).
  - `CertificateOptions.Export` (both the `RequestCertificate` input and the
    `DescribeCertificate`/nested `Options` output) added end-to-end: stored
    on the certificate via a new `SetExportPreference` backend call (mirrors
    the existing post-creation-tags pattern rather than growing
    `RequestCertificate`'s already-large positional signature, which 57+
    existing call sites depend on), and echoed correctly on read. Real AWS
    docs confirm `Export` is immutable after creation, so
    `UpdateCertificateOptions` intentionally never touches it even though it
    shares the same wire input type.
  - `RequestCertificate.DomainValidationOptions` (the input array letting a
    caller pick a custom EMAIL `ValidationDomain` per requested domain,
    previously accepted nowhere) is now parsed, validated (`DomainName` must
    be one of the requested domains; `ValidationDomain` must be the domain
    itself or a superdomain -- both real AWS constraints), and applied via a
    new `ApplyDomainValidationOverrides` backend call, updating both
    `DomainValidationOption.ValidationDomain` and (for EMAIL) the derived
    well-known validation email addresses. Violations return
    `InvalidDomainValidationOptionsException` and, verified by test, do not
    leave a certificate behind.

- Protocol: awsjson1.1 (single POST endpoint, `X-Amz-Target: CertificateManager.<Op>`).
  Verified the exact target prefix `CertificateManager.` against
  `aws-sdk-go-v2/service/acm@v1.37.21/serializers.go` (every op's
  `SetHeader("X-Amz-Target").String("CertificateManager.<Op>")`) — matches
  `acmTargetPrefix` in handler.go exactly. 16 ops enumerated in the SDK's
  `api_op_*.go` files (including `RevokeCertificate`, confirmed to be a real
  ACM op, not an acmpca-only one) match gopherstack's dispatch table exactly
  -- no fabricated ops found, none missing.

- **Bug fixed prior pass** (kept for history): `certificateDetail.KeyUsage` and
  `.ExtendedKeyUsage` in handler.go were tagged `json:"KeyUsage"` /
  `json:"ExtendedKeyUsage"` (singular) instead of the real AWS wire names
  `KeyUsages`/`ExtendedKeyUsages` (plural).

- **Looks-wrong-but-correct trap** (kept for history): `listCertificatesIncludes`
  uses PascalCase JSON tags but the real wire sends lowerCamelCase for this
  one shape; `encoding/json`'s case-insensitive fallback makes this
  correct as-is. Do not "fix" this.

- RequestCertificate always returns/persists a full `DomainValidationOptions`
  list (with `ResourceRecord{Name,Type,Value}` for DNS validation, or
  `ValidationEmails` for EMAIL validation, now honoring caller-supplied
  `ValidationDomain` overrides where provided) — required for Terraform's
  `aws_acm_certificate` + `aws_route53_record` validation-record workflow to
  function.

- Timestamps: all `CreatedAt`/`IssuedAt`/`ImportedAt`/`NotBefore`/`NotAfter`/`RevokedAt`/
  the new `RenewalSummary.UpdatedAt`/`CertificateSummary.CreatedAt`/`RevokedAt`
  are emitted as epoch-second integers (`.Unix()` on wire, matching
  `smithytime.ParseEpochSeconds` in the real deserializer) — audited the new
  fields against this same rule this pass; no ISO8601-string bug introduced.

- Error-code mapping (`handleOpError` in handler.go) now covers: `ValidationException`,
  `ResourceNotFoundException`, `RequestInProgressException`, `InvalidStateException`,
  `ResourceInUseException`, `ConflictException`, `InvalidArnException`,
  `LimitExceededException`, `TooManyTagsException`, `InvalidTagException`,
  `InvalidDomainValidationOptionsException` — all field-diffed against the real
  SDK's `types/errors.go` this pass; no fabricated error codes found.

- Persistence: `InMemoryBackend.Snapshot`/`Restore` and `Handler.Snapshot`/`Restore`
  both exist and round-trip correctly (handler wraps backend snapshot + its own
  tag-store DTO). `certs` is a "dirty" store.Table (hidden `region` field) with
  its own DTO-registry round-trip in persistence.go, not registered directly on
  `b.registry` — documented and correct per store_setup.go's own comments. The
  new `Certificate.ExportPref`/`Exported` and `RenewalSummary.UpdatedAt`/
  `RenewalStatusReason` fields are plain JSON-tagged struct fields on types
  already round-tripped by this mechanism -- no persistence-layer changes were
  needed, verified by the full existing persistence_test.go suite passing
  unmodified.
