---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: acm
sdk_module: aws-sdk-go-v2/service/acm@v1.37.21   # version audited against
last_audit_commit: 024e43bf                       # HEAD when this manifest was written
last_audit_date: 2026-07-13
overall: A            # A = genuine fix found (wire-shape bug); B = already-accurate, proven op-by-op
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  RequestCertificate: {wire: ok, errors: ok, state: ok, persist: ok, note: "DomainValidationOptions incl. ResourceRecord for DNS validation always populated; idempotency-token dedupe verified"}
  DescribeCertificate: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed KeyUsages/ExtendedKeyUsages field-name bug this pass (see gaps/notes)"}
  ListCertificates: {wire: ok, errors: ok, state: ok, persist: ok, note: "Includes filter field names (extendedKeyUsage/keyTypes/keyUsage) are lowercase on the real wire; gopherstack's PascalCase tags still decode them via encoding/json's case-insensitive matching — verified not a bug"}
  DeleteCertificate: {wire: ok, errors: ok, state: ok, persist: ok}
  ImportCertificate: {wire: ok, errors: ok, state: ok, persist: ok, note: "re-import (CertificateArn set) updates in place; matches AWS"}
  GetCertificate: {wire: ok, errors: ok, state: ok, persist: ok, note: "correctly rejects PENDING_VALIDATION/FAILED/VALIDATION_TIMED_OUT with RequestInProgressException-style error"}
  ExportCertificate: {wire: ok, errors: ok, state: ok, persist: ok, note: "restricted to IMPORTED/PRIVATE; fake chain synthesized when none stored, matching AWS always-return-chain behavior"}
  AddTagsToCertificate: {wire: ok, errors: ok, state: ok, persist: ok}
  RemoveTagsFromCertificate: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForCertificate: {wire: ok, errors: ok, state: ok, persist: ok}
  RenewCertificate: {wire: ok, errors: ok, state: ok, persist: ok, note: "IMPORTED/PRIVATE(caArn set) rejected with RequestInProgressException-mapped ErrNotEligible, matching AWS restriction to AMAZON_ISSUED"}
  RevokeCertificate: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateCertificateOptions: {wire: ok, errors: ok, state: ok, persist: ok}
  ResendValidationEmail: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAccountConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  PutAccountConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "idempotency-token conflict correctly returns ConflictException on mismatched settings"}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - CertificateSummary (ListCertificates) omits optional AWS fields Exported/ExportOption/InUse/ManagedBy/KeyUsages/ExtendedKeyUsages/HasAdditionalSubjectAlternativeNames entirely; not wired to any backend tracking. Deferred as a feature gap, not a wire bug (fields are optional on the real wire too).
  - CertificateDetail omits ManagedBy (AWS: which service, e.g. CLOUDFRONT, manages the cert) — no backend concept of managed-by exists. Feature gap, not audited further this pass.
  - Malformed/garbage ARNs on read ops (Describe/Get/Export/Renew/etc.) return ResourceNotFoundException; real AWS returns InvalidArnException for arns that fail ARN-shape validation before the not-found check. Not fixed this pass (low traffic path, needs ARN-shape validator).
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - CertificateSummary optional-field parity (Exported/ManagedBy/etc., see gaps)
  - InvalidArnException vs ResourceNotFoundException distinction on malformed ARNs
leaks: {status: clean, note: "isolation_test.go / leak_test.go already cover timer goroutine lifecycle (Shutdown stops auto-validate timers); Reset()/Close() explicitly stop all pending time.AfterFunc timers; janitor sweeps orphaned timers whose cert was deleted"}
---

## Notes

- Protocol: awsjson1.1 (single POST endpoint, `X-Amz-Target: CertificateManager.<Op>`).
  Verified the exact target prefix `CertificateManager.` against
  `aws-sdk-go-v2/service/acm@v1.37.21/serializers.go` (every op's
  `SetHeader("X-Amz-Target").String("CertificateManager.<Op>")`) — matches
  `acmTargetPrefix` in handler.go exactly.

- **Bug fixed this pass**: `certificateDetail.KeyUsage` and `.ExtendedKeyUsage` in
  handler.go were tagged `json:"KeyUsage"` / `json:"ExtendedKeyUsage"` (singular).
  The real AWS wire field names (confirmed against
  `awsAwsjson11_deserializeDocumentCertificateDetail` in the SDK's
  `deserializers.go`) are `KeyUsages` / `ExtendedKeyUsages` (plural). Since these
  differ by more than case, Go's `encoding/json` case-insensitive fallback does NOT
  save this — a real aws-sdk-go-v2 client parsing gopherstack's DescribeCertificate
  response would always see empty `KeyUsages`/`ExtendedKeyUsages` slices, even
  though gopherstack's backend correctly computed and populated the underlying
  key-usage data. Existing unit test `TestACMHandler_DescribeCertificate_KeyUsageAndInUseBy`
  in handler_test.go was itself asserting against the wrong (self-referential)
  field names and had to be updated — a textbook case of parity-principles.md rule
  #3 ("unit tests are not parity proof" / testing against your own output rather
  than the real SDK).

- **Looks-wrong-but-correct trap**: `listCertificatesIncludes` (the `Includes`
  filter on ListCertificates input) uses PascalCase JSON tags (`KeyTypes`,
  `ExtendedKeyUsage`, `KeyUsage`) but the real wire (per
  `awsAwsjson11_serializeDocumentFilters` in the SDK) sends **lowerCamelCase**
  keys (`keyTypes`, `extendedKeyUsage`, `keyUsage`) for this one shape (an ACM
  smithy-model quirk — most other ACM fields are PascalCase). This does NOT need
  fixing: `encoding/json.Unmarshal` matches JSON object keys to struct tags
  case-insensitively when there's no exact match, so `"keyTypes"` on the wire
  still binds correctly to the `json:"KeyTypes"` tag. Verified by existing test
  `TestACMHandler_ListCertificates_IncludesFilters` in handler_test.go which
  already sends lowercase keys and passes. Do not "fix" this to PascalCase or
  add duplicate lowercase tags — it is already correct.

- RequestCertificate always returns/persists a full `DomainValidationOptions`
  list (with `ResourceRecord{Name,Type,Value}` for DNS validation, or
  `ValidationEmails` for EMAIL validation) — required for Terraform's
  `aws_acm_certificate` + `aws_route53_record` validation-record workflow to
  function; confirmed this is not a disguised no-op (real per-domain synthetic
  CNAME tokens generated via `buildDomainValidationOptions`).

- Timestamps: all `CreatedAt`/`IssuedAt`/`ImportedAt`/`NotBefore`/`NotAfter`/`RevokedAt`
  are emitted as epoch-second integers (`.Unix()` on wire, matching
  `smithytime.ParseEpochSeconds` in the real deserializer) — no ISO8601 string bug
  found here (unlike other services flagged in prior parity sweeps).

- Error-code mapping (`handleOpError` in handler.go) uses only error type names
  that actually exist in the real SDK's `types/errors.go`
  (ValidationException, ResourceNotFoundException, RequestInProgressException,
  InvalidStateException, ResourceInUseException, ConflictException) — no
  fabricated error codes found.

- Persistence: `InMemoryBackend.Snapshot`/`Restore` and `Handler.Snapshot`/`Restore`
  both exist and round-trip correctly (handler wraps backend snapshot + its own
  tag-store DTO). `certs` is a "dirty" store.Table (hidden `region` field) with
  its own DTO-registry round-trip in persistence.go, not registered directly on
  `b.registry` — documented and correct per store_setup.go's own comments.
