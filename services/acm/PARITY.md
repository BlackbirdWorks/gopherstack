---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: acm
sdk_module: aws-sdk-go-v2/service/acm@v1.43.4   # version audited against
last_audit_commit: HEAD                           # see git log for this pass's commit
last_audit_date: 2026-08-10
overall: A            # A = genuine fix found (wire-shape bug); B = already-accurate, proven op-by-op
# 2026-07-25 pass: implemented 23 ops added between v1.37.21 and v1.43.0 (the
# ACME family: endpoints, external account bindings, accounts, domain
# validations; plus SearchCertificates and generic resource tagging). No
# wire-shape bug was found in the PREVIOUSLY-implemented 16 ops this pass
# (not re-audited beyond confirming TestSDKCompleteness and the existing
# suite still pass) -- see gaps below for the two deliberately-scoped-down
# spots in the NEW surface (AcmeAccount is never populated; AcmeDomainValidation
# never leaves VALIDATING). B, not A, because the grade reflects genuine but
# incomplete coverage of new surface, not a bug fix on audited-and-confirmed-
# accurate old surface.
#
# 2026-07-30 pass (re-audit, gopherstack B-grade sweep): confirmed the 2026-07-25
# rationale was still current -- re-read every `gaps`/`deferred` bullet against the
# installed SDK and found one, and only one, genuinely mechanical field-diff gap
# (not a design/scope gap): RequestCertificate.ManagedBy / CertificateDetail.ManagedBy
# / CertificateSummary.ManagedBy / AcmCertificateMetadata(Filter).ManagedBy -- all real
# fields on the installed aws-sdk-go-v2/service/acm@v1.43.0 types (confirmed by reading
# types.go/api_op_RequestCertificate.go directly: CertificateManagedBy is a real,
# currently single-valued enum, "CLOUDFRONT") that gopherstack accepted nowhere and
# echoed nowhere. FIXED this pass end-to-end (input validation, storage, echo on
# Describe/List/Search, real SearchCertificates filter+sort matching) -- see Notes and
# the ManagedBy gap bullets removed below. Still held at B, not raised to A: every
# other `gaps`/`deferred` item is a genuine, still-open, non-mechanical scope boundary
# (a full RFC 8555 ACME protocol front-end for AcmeAccount, DNS-record verification
# for AcmeDomainValidation.Status, the 2025 exportable-public-certificates gating
# whose exact error contract could not be confirmed from available docs, HTTP
# validation method, structured-DN Subject filtering) that this pass did not close and
# should not be asserted closed. See gaps/deferred below for the itemized remainder.
#
# 2026-07-30 pass #2 (parity-5, RAISED TO A): re-opened each of the five reasons this
# service was held at B/A- and re-checked them individually against primary sources
# (live AWS API reference pages, fetched this pass, not just the bundled Go SDK) instead
# of accepting the prior framing on faith. Two turned out to be genuinely closeable;
# closed both, with tests. The other three were re-confirmed genuinely out of reach for
# a single pass, with more evidence than before, not just re-asserted:
#   1. RFC 8555 ACME protocol front-end (AcmeAccount population) -- unchanged, still a
#      real cryptographic protocol server this rollout has never been asked to build.
#   2. Route 53 DNS-record verification for AcmeDomainValidation.Status -- investigated
#      the wiring cost concretely rather than dismissing it: gopherstack DOES have an
#      established cross-service backend-sharing pattern (services/cloudformation's
#      ServiceBackends struct, injected in cli.go after core handlers are constructed,
#      already gives CloudFormation direct in-process access to route53's Handler to
#      create/read real hosted zones and record sets). Importing route53 into acm is not
#      blocked by an import cycle (route53 does not import acm). But wiring ACM the same
#      way is a materially larger change than the two closed this pass: it requires new
#      cli.go initialization-order wiring (ACM would need to be constructed after/paired
#      with a Route53 Handler instance, the same way CloudFormation is special-cased
#      today, not through the generic service.Provider path ACM currently registers
#      through), a provider-signature change, and reasoning about how a regional ACM
#      backend pairs with Route 53 (a global service in real AWS) -- cross-cutting scope
#      the same size class as route53resolver's deferred Route 53 Profile DELEGATE
#      gap. Flagged rather than half-wired; see gaps.
#   3. 2025 exportable-public-certificates gating -- CLOSED this pass. Fetched
#      docs.aws.amazon.com/acm/latest/APIReference/API_ExportCertificate.html and
#      API_CertificateOptions.html directly (the "unconfirmed error contract" blocker):
#      ExportCertificate's documented Errors section has exactly five entries
#      (InvalidArnException/RequestInProgressException/ResourceNotFoundException/
#      ThrottlingException/ValidationException) and RequestInProgressException's
#      documented meaning is specifically "the certificate ... has not yet been issued"
#      -- confirming the previous unconditional RequestInProgressException for every
#      AMAZON_ISSUED export attempt was actually WRONG for an already-issued,
#      not-opted-in certificate (ValidationException fits; RequestInProgressException
#      does not). CertificateOptions.Export's doc ("You can opt in to allow the export of
#      your certificates by specifying ENABLED") gave the real eligibility rule. The
#      pre-2025-06-17 date carve-out doesn't need separate modeling: every
#      gopherstack-created certificate is timestamped with the real current time, always
#      after that cutoff. Implemented in validateCertExportable (certificates.go); see
#      ops below.
#   4. HTTP validation method -- investigated further, found genuine new nuance rather
#      than closing it: RequestCertificate's live API reference lists ValidationMethod's
#      "Valid Values" as `EMAIL | DNS | HTTP` (HTTP IS a syntactically accepted enum
#      value), which is MORE permissive than the Go SDK's doc comment alone suggested
#      ("You can validate with DNS or validate with email") -- but the page does not
#      document what actually happens server-side for a direct customer call with
#      ValidationMethod=HTTP (accepted-then-fails, immediately rejected, or something
#      else), and DomainValidationOption (the per-domain input shape) still has no
#      ValidationMethod member at all, only DomainName/ValidationDomain (EMAIL-only).
#      Building either a fabricated acceptance path or a fabricated rejection error here
#      would violate the same no-invented-error-contract rule that made #3 worth
#      confirming before touching. Left as an honestly-deepened gap, not built -- see
#      gaps.
#   5. SearchCertificates X509AttributeFilter.Subject (structured Distinguished Name
#      filtering) -- CLOSED this pass, and turned out narrower than the prior framing
#      assumed. Read types.go directly: the real SubjectFilter union (the actual FILTER
#      input, distinct from the DistinguishedName OUTPUT type the old gap description
#      was reasoning from) defines exactly one member, CommonName
#      (SubjectFilterMemberCommonName) -- despite DistinguishedName having 15 RDN
#      components (Organization/Country/etc.), AWS has not exposed filtering on any of
#      them except CommonName. So "full DN filtering" was never actually in scope to
#      begin with; CommonName filtering is the complete real feature. Implementing it
#      surfaced a genuine, independent wire-shape bug while here: crypto.go was
#      collapsing pkix.Name into a single flattened display string (Certificate.Subject/
#      Issuer, e.g. "CN=example.com,OU=Server CA 1B,O=Amazon,C=US") and SearchCertificates
#      then fed that whole flattened string into X509Attributes.Subject.CommonName /
#      .Issuer.CommonName -- which per types.go should hold JUST the CN
#      ("example.com"). A real SDK client reading that field after this pass's parent
#      fixes would have received a garbled value. Fixed by capturing
#      pkix.Name.CommonName / cert.Subject.CommonName separately at certificate
#      creation/import time (new Certificate.SubjectCommonName/IssuerCommonName fields)
#      instead of re-deriving it from the flattened string. SortBy=COMMON_NAME (a real
#      SearchCertificatesSortBy enum value, previously falling into the
#      no-tracked-data ARN-order fallback) now sorts on real data too.
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  RequestCertificate: {wire: ok, errors: partial, state: ok, persist: ok, note: "field-diffed this pass against RequestCertificateInput/CertificateOptions: added DomainValidationOptions input (validated + applied, InvalidDomainValidationOptionsException wired), Options.Export input (stored, echoed on Describe/List, see gaps for enforcement scope), SAN-count-exceeded now LimitExceededException (was ValidationException); RSA_1024 weak-key rejection now correctly wrapped as ValidationException instead of escaping to a 500 InternalFailure. 2026-07-30: ManagedBy input added (real CertificateManagedBy enum, single value CLOUDFRONT; verified against types.go/api_op_RequestCertificate.go), validated before certificate creation (so an unknown value never leaves an orphaned cert behind, same reasoning as DomainValidationOptions) and stored via a new SetManagedBy backend call, mirroring the existing SetExportPreference immutable-after-creation pattern. 2026-08-10: ValidationMethod=HTTP now starts the certificate PENDING_VALIDATION (was previously swallowed by buildInitialDVOList's default branch, immediately issuing the cert with a mislabeled DNS ResourceRecord) and populates a synthetic HttpRedirect (DomainValidationOption.HTTPRedirect/RedirectFrom/RedirectTo) instead -- see DescribeCertificate note and gaps for the still-unconfirmed accept/reject contract this does NOT claim to resolve. errors: partial because RequestCertificate's own deserializer (deserializers.go:3346-3400+, v1.43.4) recognizes InvalidArnException/InvalidDomainValidationOptionsException/InvalidParameterException/InvalidTagException/LimitExceededException/TagPolicyException/TooManyTagsException -- NOT ValidationException -- but validateRequestCertInput's DomainName-required/domain-shape checks, validateManagedBy, and the RSA_1024 weak-key check (crypto.go) all still return ValidationException (ErrInvalidParameter) here; see gaps, not fixed this pass because validateDomainName and the weak-key check are shared with RenewCertificate (whose real error set DOES include ValidationException, confirmed deserializers.go:3272) and CreateAcmeDomainValidation (a third, different error set), so a correct fix needs per-caller error codes, not a global rename."}
  DescribeCertificate: {wire: ok, errors: ok, state: ok, persist: ok, note: "RenewalSummary now includes UpdatedAt (required/always-present on real wire, was missing entirely) and RenewalStatusReason; Options.Export added; InvalidArnException wired for malformed CertificateArn. 2026-07-30: ManagedBy echoed (see RequestCertificate note); jsonDescribeCertificate decomposed into buildDomainValidationOptionList/buildRenewalSummaryDetail helpers to stay under funlen after the addition. 2026-08-10: DomainValidationOptions[].HttpRedirect wired (types.DomainValidation.HttpRedirect, types.go:1053-1056, v1.43.4: 'exists only when ... the validation method is HTTP'); mutually exclusive with ResourceRecord per real wire semantics, see RequestCertificate note."}
  ListCertificates: {wire: ok, errors: ok, state: ok, persist: ok, note: "CertificateSummary previously omitted CreatedAt entirely (always-present real field) -- fixed. Also added RevokedAt/InUse/KeyUsages/ExtendedKeyUsages/ExportOption/Exported/HasAdditionalSubjectAlternativeNames(always false, correct given our SAN cap), closing the prior gap row. 2026-07-30: ManagedBy added (was previously intentionally omitted, see prior gaps -- now real, see RequestCertificate note). FIXED THIS PASS (parity-5): Exported was gated to PRIVATE-type certificates only, mirroring a doc-comment restriction ('This value exists only when the certificate type is PRIVATE') that was real in aws-sdk-go-v2/service/acm@v1.37.21 but is GONE from the currently-installed v1.43.0's types.go -- AWS dropped it when exportable public certificates shipped in 2025. Now that AMAZON_ISSUED certificates can genuinely be exported too (see ExportCertificate), gating Exported to PRIVATE was stale; set unconditionally, matching SearchCertificates' AcmCertificateMetadata.Exported (handler_search_certificates.go), which was already correctly unconditional. 2026-08-10: ListCertificates' deserializer (deserializers.go:2698-2747, v1.43.4) recognizes exactly InvalidArgsException/ValidationException -- unlike every other op in this package -- and previously nothing validated CertificateStatuses/Includes.KeyTypes/Includes.KeyUsage/Includes.ExtendedKeyUsage/SortBy/SortOrder against their real enums, so an unrecognized value (typo or otherwise) silently matched zero certificates and returned 200 instead of 400 -- the more-permissive-than-AWS direction. validateListCertificatesParams (certificate_validation.go) now rejects any value outside the real CertificateStatus/KeyAlgorithm/KeyUsageName/ExtendedKeyUsageName/SortBy(CREATED_AT only)/SortOrder enums with InvalidArgsException (new ErrInvalidArgs sentinel)."}
  DeleteCertificate: {wire: ok, errors: ok, state: ok, persist: ok, note: "InvalidArnException wired for malformed CertificateArn"}
  ImportCertificate: {wire: ok, errors: ok, state: ok, persist: ok, note: "re-import (CertificateArn set) updates in place; matches AWS. InvalidArnException wired when CertificateArn is supplied and malformed"}
  GetCertificate: {wire: ok, errors: ok, state: ok, persist: ok, note: "correctly rejects PENDING_VALIDATION/FAILED/VALIDATION_TIMED_OUT with RequestInProgressException-style error; InvalidArnException wired"}
  ExportCertificate: {wire: ok, errors: ok, state: ok, persist: ok, note: "restricted to IMPORTED/PRIVATE unconditionally, plus AMAZON_ISSUED when opted in; fake chain synthesized when none stored, matching AWS always-return-chain behavior; Exported flag now tracked and surfaced on CertificateSummary for every certificate type (was PRIVATE-only, see ListCertificates note); InvalidArnException wired. FIXED THIS PASS (parity-5): implements the 2025 exportable-public-certificates feature end-to-end -- validateCertExportable (certificates.go) allows an AMAZON_ISSUED export when Options.Export=ENABLED (confirmed against API_CertificateOptions.html's Export doc), and returns the correct error per certificate state: RequestInProgressException only while genuinely PENDING_VALIDATION/VALIDATION_TIMED_OUT/FAILED (matching that error's own documented meaning, confirmed against API_ExportCertificate.html's Errors section), ValidationException for an issued-but-not-opted-in certificate (previously, incorrectly, also RequestInProgressException). See TestACMHandler_ExportCertificate_AmazonIssued (handler_certificate_status_errors_test.go) and TestACMBackend_ExportCertificate's amazon_issued cases (certificates_test.go). 2026-08-10: RE-VERIFIED independently against the primary source (not the doc page) -- awsAwsjson11_deserializeOpErrorExportCertificate's switch (deserializers.go:1630-1652, v1.43.4) recognizes exactly InvalidArnException/RequestInProgressException/ResourceNotFoundException/ThrottlingException/ValidationException, confirming both of gopherstack's codes (RequestInProgressException for genuinely-pending, ValidationException for issued-but-not-opted-in) are in the operation's own recognized error set. No divergence found in either direction: IMPORTED/PRIVATE stay unconditionally exportable (matches AWS, which never withheld a caller-supplied key) and AMAZON_ISSUED stays gated on Options.Export=ENABLED (matches API_CertificateOptions.html); gopherstack does not accept an export the real service would refuse, nor refuse one it would allow, for any state this backend can produce."}
  AddTagsToCertificate: {wire: ok, errors: ok, state: ok, persist: ok, note: "TooManyTagsException (was ValidationException) for >50 tags; InvalidTagException added for empty key or reserved aws: prefix (key or value); InvalidArnException wired"}
  RemoveTagsFromCertificate: {wire: ok, errors: ok, state: ok, persist: ok, note: "InvalidArnException wired"}
  ListTagsForCertificate: {wire: ok, errors: ok, state: ok, persist: ok, note: "InvalidArnException wired"}
  RenewCertificate: {wire: ok, errors: ok, state: ok, persist: ok, note: "IMPORTED/PRIVATE(caArn set) rejected with RequestInProgressException-mapped ErrNotEligible, matching AWS restriction to AMAZON_ISSUED. RenewalSummary.UpdatedAt now set/refreshed on renewal start and on auto-validation completion. InvalidArnException wired"}
  RevokeCertificate: {wire: ok, errors: ok, state: ok, persist: ok, note: "InvalidArnException wired"}
  UpdateCertificateOptions: {wire: ok, errors: ok, state: ok, persist: ok, note: "Export field on the shared CertificateOptions input type is intentionally ignored here (AWS: Export is immutable after creation); InvalidArnException wired"}
  ResendValidationEmail: {wire: ok, errors: ok, state: ok, persist: ok, note: "InvalidArnException wired"}
  GetAccountConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  PutAccountConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "idempotency-token conflict correctly returns ConflictException on mismatched settings"}
  SearchCertificates: {wire: ok, errors: ok, state: ok, persist: ok, note: "field-diffed against SearchCertificatesInput/Output and the CertificateFilterStatement/CertificateFilter/AcmCertificateMetadataFilter/X509AttributeFilter union wire shapes in serializers.go/deserializers.go (union members serialize as single-key wrapper objects, e.g. {\"Filter\":{\"CertificateArn\":...}}). Supports the full And/Or/Not/Filter recursive tree; AcmCertificateMetadataFilter members Status/Type/ValidationMethod/RenewalStatus/Exported/InUse/ExportOption map to real Certificate fields; AcmeAccountId/AcmeEndpointArn/CertificateKeyPairOrigin filters honestly never match (no such data tracked, see gaps). X509AttributeFilter supports KeyAlgorithm/KeyUsage/ExtendedKeyUsage/SerialNumber/SubjectAlternativeName.DnsName(EQUALS/CONTAINS)/NotAfter/NotBefore. SortBy supports all real fields with data (falls back to stable ARN ordering for untracked fields, matching ListCertificates' own fallback). 2026-07-30: ManagedBy filter member and MANAGED_BY sort now match/sort for real (Certificate.ManagedBy is now tracked data, see RequestCertificate note) -- new TestACMHandler_SearchCertificates/AcmCertificateMetadataFilter_ManagedBy case locks this in. FIXED THIS PASS (parity-5): X509AttributeFilter.Subject (CommonName) now implemented -- read types.go directly and found the real SubjectFilter union defines only ONE member, CommonName (SubjectFilterMemberCommonName); the 'full Distinguished Name filtering' the prior gap description assumed was missing scope was never actually offered by the real API to begin with. Also fixed a genuine wire-shape bug found while implementing this: X509Attributes.Subject.CommonName/.Issuer.CommonName were fed the fully-flattened pkix.Name.String() rendering (e.g. \"CN=example.com,OU=Server CA 1B,O=Amazon,C=US\") instead of just the CN (\"example.com\") the real DistinguishedName.CommonName field holds -- fixed by capturing Certificate.SubjectCommonName/IssuerCommonName separately at cert creation/import time (crypto.go). SortBy=COMMON_NAME now sorts on this real data too (was previously in the no-tracked-data ARN-order fallback bucket). See TestACMHandler_SearchCertificates/X509AttributeFilter_SubjectCommonName."}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "routes by ARN shape (certificate/acme-endpoint/acme-external-account-binding/acme-domain-validation, most-specific-first) via resolveTaggableResourceArn (handler_resource_tags.go); a CertificateArn resolves to the SAME h.tags-backed store ListTagsForCertificate/AddTagsToCertificate use -- see tagging_verdict. Malformed ResourceArn -> ValidationException (not InvalidArnException; the real op's documented Errors section lists only ResourceNotFoundException/ValidationException, unlike CertificateArn ops)."}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "same ARN-type routing as ListTagsForResource; shares h.tags with AddTagsToCertificate for certificate ARNs."}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "TagKeys (not Tags) input, field-diffed against UntagResourceInput; same ARN-type routing."}
  CreateAcmeEndpoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "field-diffed against CreateAcmeEndpointInput/Output and AcmeEndpoint. CertificateAuthority union (only PublicCertificateAuthority member exists in the real SDK) required and validated; AuthorizationBehavior must be PRE_APPROVED (the only real enum value); IdempotencyToken dedupes via a fingerprint of AuthorizationBehavior+Contact+AllowedKeyAlgorithms, ConflictException on mismatch (mirrors RequestCertificate/PutAccountConfiguration's existing idempotency pattern). Endpoints go ACTIVE synchronously -- gopherstack has no async provisioning pipeline and a synchronous result claims no real ACME client interaction happened. EndpointUrl is a deterministic synthetic URL."}
  DescribeAcmeEndpoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "ARN shape validated against the real documented pattern (arn:aws[a-z-]*:acm:<region>:<account>:acme-endpoint/<id>, narrowed to gopherstack's permissive partition class like certArnPattern)."}
  ListAcmeEndpoints: {wire: ok, errors: ok, state: ok, persist: ok, note: "paginated via pkgs/page, sorted by ARN; AcmeEndpointSummary is field-identical to AcmeEndpoint on the real wire so the same wire struct is reused."}
  UpdateAcmeEndpoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "every field optional on the real wire; empty/absent means unchanged (an empty string is indistinguishable from absent in this op's non-pointer wire fields, matching AWS's own non-pointer AuthorizationBehavior/Contact enum types)."}
  DeleteAcmeEndpoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascade-deletes every owned AcmeExternalAccountBinding/AcmeDomainValidation/AcmeAccount -- see ownership_and_cascade. Also cleans up the endpoint's own tag entry (h.cleanupTags)."}
  CreateAcmeExternalAccountBinding: {wire: ok, errors: ok, state: ok, persist: ok, note: "field-diffed against CreateAcmeExternalAccountBindingInput/Output and AcmeExternalAccountBinding. RoleArn validated against the real documented IAM role ARN pattern (arn:aws[a-z-]*:iam::<account>:role/.+); Expiration{Type,Value} (MINUTES/HOURS/DAYS) computes ExpiresAt; KeyId/MacKey are synthetic per-EAB random credentials (not cryptographically meaningful -- no real ACME protocol server exists to validate them against, see CLAUDE.md parity principles). Owned by AcmeEndpointArn (ResourceNotFoundException if the endpoint does not exist); IdempotencyToken dedupes via AcmeEndpointArn+RoleArn+Expiration fingerprint."}
  DescribeAcmeExternalAccountBinding: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAcmeExternalAccountBindings: {wire: ok, errors: ok, state: ok, persist: ok, note: "paginated per-endpoint via the shared listOwnedByEndpoint generic helper (acme_models.go), sorted by ARN."}
  GetAcmeExternalAccountBindingCredentials: {wire: ok, errors: ok, state: ok, persist: ok, note: "InvalidStateException for a revoked EAB -- credentials are never issued for a binding gopherstack knows to be revoked."}
  RevokeAcmeExternalAccountBinding: {wire: ok, errors: ok, state: ok, persist: ok, note: "double-revoke returns InvalidStateException, matching RevokeCertificate's already-revoked handling elsewhere in this package."}
  DescribeAcmeAccount: {wire: ok, errors: ok, state: ok, persist: ok, note: "see gaps -- AcmeAccount is honestly always empty (no ACME protocol server); this op validates the AcmeEndpointArn FK for real and returns ResourceNotFoundException for the (always-absent) account."}
  ListAcmeAccounts: {wire: ok, errors: ok, state: ok, persist: ok, note: "same honest-emptiness as DescribeAcmeAccount; validates AcmeEndpointArn, returns an empty AcmeAccounts array."}
  RevokeAcmeAccount: {wire: ok, errors: ok, state: ok, persist: ok, note: "same honest-emptiness; ResourceNotFoundException, never a fabricated success."}
  CreateAcmeDomainValidation: {wire: ok, errors: ok, state: ok, persist: ok, note: "field-diffed against CreateAcmeDomainValidationInput/Output and AcmeDomainValidation/PrevalidationOptions/PrevalidationDetails. PrevalidationOptions.DnsPrevalidation (the only union member the real SDK defines) required; synthesizes a CNAME ResourceRecord using the same random-token construction certificate DNS validation uses (distinct well-known suffix so the two never look identical on the wire). Status is always VALIDATING -- see gaps, this is the domain-validation-success fabrication the task explicitly warned against avoiding. Owned by AcmeEndpointArn, cascade-deleted with it."}
  DescribeAcmeDomainValidation: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAcmeDomainValidations: {wire: ok, errors: ok, state: ok, persist: ok, note: "paginated per-endpoint via the same listOwnedByEndpoint helper as ListAcmeExternalAccountBindings."}
  UpdateAcmeDomainValidation: {wire: ok, errors: ok, state: ok, persist: ok, note: "only PrevalidationOptions is updatable on the real wire; regenerates the DNS ResourceRecord when supplied. Status remains VALIDATING (never fabricated as re-verified)."}
  DeleteAcmeDomainValidation: {wire: ok, errors: ok, state: ok, persist: ok}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "ValidationMethod=HTTP: gopherstack now starts the certificate PENDING_VALIDATION and returns a synthetic DomainValidation.HttpRedirect for a direct RequestCertificate call with ValidationMethod=HTTP (fixed 2026-08-10, see RequestCertificate/DescribeCertificate ops notes), the correct wire shape per types.DomainValidation.HttpRedirect's own doc comment. What remains genuinely unconfirmed: DomainValidation.HttpRedirect's doc text describes HTTP validation as being for 'certificates requested through Amazon CloudFront' specifically, and RequestCertificate's own doc prose only mentions DNS/email ('You can validate with DNS or validate with email') even though ValidationMethod's Valid Values list (API_RequestCertificate.html) syntactically includes HTTP -- neither page documents whether a direct (non-CloudFront) customer RequestCertificate call with ValidationMethod=HTTP is accepted, immediately rejected, or something else. gopherstack now accepts it (the more-permissive direction); building a rejection path would require fabricating an unconfirmed error contract, the same risk the 2025 export-gating gap was stuck on before its contract was confirmed from the operation's own Errors section -- HTTP has no equivalent confirmation available. RedirectFrom/RedirectTo are freeform strings with no documented format (API_HttpRedirect.html: both 'Required: No', no schema given), so the synthetic values gopherstack generates are placeholders in the correct shape, not a claimed-real URL convention."
  - TagPolicyException (present in RequestCertificate/AddTagsToCertificate's real error sets, types/errors.go) is not wired to any code path -- no tag-policy engine (AWS Organizations tag policies) exists in gopherstack to trigger it from; this is correct-by-absence, not a stub, since gopherstack has no cross-account policy state to evaluate. InvalidArgsException (ListCertificates' own error, distinct from every other op's ValidationException) IS now wired -- fixed 2026-08-10, see ListCertificates ops note.
  - "RequestCertificate's own recognized error set (deserializers.go:3346-3400+, v1.43.4) does NOT include ValidationException, only InvalidParameterException -- but validateRequestCertInput (empty/malformed DomainName, SAN shape), validateManagedBy, and the RSA_1024 weak-key rejection (crypto.go) all still return ValidationException on this op, an error code a real SDK client's typed error handling for RequestCertificate would never recognize. Not fixed this pass: validateDomainName and the weak-key check are shared with RenewCertificate (whose real error set correctly includes ValidationException, deserializers.go:3272) and validateDomainName is also shared with CreateAcmeDomainValidation (a third, ACME-family error set) -- a correct fix needs the shared validators to return a caller-specific error code rather than a single global rename, which risks breaking RenewCertificate's already-correct behavior if rushed. Needs its own pass auditing every RequestCertificate-reachable validation error against deserializers.go:3346-3400+ specifically."
  - AcmeAccount is never populated (DescribeAcmeAccount/ListAcmeAccounts/RevokeAcmeAccount always operate on an empty account set). Real ACME accounts are created by an ACME client's own RFC 8555 "newAccount" protocol call against the endpoint's EndpointUrl -- a real ACME protocol front-end (parsing/serving actual ACME JSON, JWS-signed requests, nonce challenges, etc.) is out of scope for this rollout per the task's explicit instruction that real cryptographic ACME protocol work is not required. The three ops are wired against real (honestly empty) backend state and validate their AcmeEndpointArn FK for real -- this is a deliberate scope boundary, not an unwired stub. Deferred: an actual ACME protocol server that populates this table.
  - "AcmeDomainValidation.Status never leaves VALIDATING (real values also include VALID/INVALID/DELETING). RE-INVESTIGATED THIS PASS (parity-5): the task's reframe -- 'DNS validation is checkable against the emulator's own Route 53 state if that is wired' -- is architecturally real, not a dead end: services/cloudformation already establishes a cross-service backend-sharing pattern (its ServiceBackends struct, injected in cli.go after core handlers are constructed, gives CloudFormation direct in-process access to route53's Handler); importing route53 into acm is not blocked by an import cycle (route53 does not import acm). But wiring acm the same way requires cli.go initialization-order changes (constructing/pairing an ACM Handler with a Route53 Handler instance the way CloudFormation is special-cased today, not through the generic service.Provider path acm currently registers through), an ACM provider-signature change, and resolving how a regional ACM backend pairs with Route 53 (a global service in real AWS) -- a materially larger, cross-cutting change than either fix landed this pass, comparable in scope to route53resolver's own deferred Route 53 Profile DELEGATE gap. Not wired this pass; flagged with a concrete path instead of dismissed. FailureDetails is consequently still always absent too (nothing to report a failure for without real verification)."
  - AcmCertificateMetadataFilter's AcmeAccountId/AcmeEndpointArn/CertificateKeyPairOrigin members (and the matching SearchCertificates SortBy values) never match/sort meaningfully: Certificate carries no such fields (CertificateDetail.AcmeAccountId/AcmeEndpointArn are real-SDK fields no code path populates, since no ACME-issued-certificate flow exists in gopherstack to derive them from -- see the AcmeAccount gap above). Correct-by-absence, not fabricated. (ManagedBy, previously grouped with this bullet, is now real end-to-end -- fixed 2026-07-30, see ops above.)
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - RequestCertificate's ValidationException-vs-InvalidParameterException error-code mismatch (see gaps) — needs a per-operation validator-error audit, not a global rename
  - A real ACME protocol front-end (RFC 8555 server) that would let AcmeAccount, and CertificateDetail's new AcmeAccountId/AcmeEndpointArn fields, actually get populated
  - AcmeDomainValidation real DNS-record verification (VALID/INVALID transitions) — a concrete cross-service wiring path now exists (see gaps), next pass could attempt the cli.go/provider wiring rather than the DNS-check logic itself, which is the smaller half of this gap
leaks: {status: clean, note: "isolation_test.go / leak_test.go already cover timer goroutine lifecycle (Shutdown stops auto-validate timers); Reset()/Close() explicitly stop all pending time.AfterFunc timers; janitor sweeps orphaned timers whose cert was deleted. This pass added no new goroutines/timers -- ExportCertificate's RLock->Lock change (to persist the new Exported flag) and the two new backend methods (ApplyDomainValidationOverrides, SetExportPreference) all use the existing b.mu lock with clean defer-release, verified via -race across the full suite. The new ACME resource family (endpoints/EABs/domain-validations/accounts) introduces no timers or other goroutines -- Create* ops are fully synchronous; DeleteAcmeEndpoint's cascade delete is a plain in-lock loop over store.Index.Get results, verified via -race across the full suite including the new SDK round-trip tests."}
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
    Follow-up fix: `CertificateSummary.KeyUsages`/`ExtendedKeyUsages` were
    initially projected using the *same* `[]{"Name": "..."}` object-wrapped
    shape as `CertificateDetail.KeyUsages` ([]types.KeyUsage) -- but real
    AWS's `CertificateSummary.KeyUsages`/`ExtendedKeyUsages` (the shapes
    `ListCertificates` actually returns) are plain string arrays
    (`[]types.KeyUsageName`/`[]types.ExtendedKeyUsageName`), an asymmetry
    between `CertificateDetail` and `CertificateSummary` in the real API, not
    a gopherstack invention to normalize away. The object-wrapped shape broke
    every real SDK client's `ListCertificates` deserializer ("expected
    KeyUsageName to be of type string, got map[string]interface{} instead"),
    caught by `TestTerraform_ACM`. Fixed: `certificateSummary.KeyUsages`/
    `ExtendedKeyUsages` are now `[]string`.
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

## Notes (2026-07-25 pass — parity-4 campaign, ACME family + SearchCertificates + generic tagging)

- **Scope**: the Go SDK modules were bumped (`aws-sdk-go-v2/service/acm`
  v1.37.21 → v1.43.0), adding 23 operations gopherstack had neither
  implemented nor listed in `sdk_completeness_test.go`'s `notImplemented`
  slice. All 23 are now real: `TestSDKCompleteness` passes with an empty
  `notImplemented` argument (verified `git diff services/acm/sdk_completeness_test.go`
  is empty this pass).

- **Two op groups**:
  1. A new ACME (RFC 8555) resource family: `AcmeEndpoint` (root),
     `AcmeExternalAccountBinding` and `AcmeAccount` (children of an
     endpoint), `AcmeDomainValidation` (child of an endpoint). New files per
     family, matching this service's existing split-by-concern layout:
     `acme_endpoints.go`/`handler_acme_endpoints.go`,
     `acme_eab.go`/`handler_acme_eab.go`,
     `acme_accounts.go`/`handler_acme_accounts.go`,
     `acme_domain_validations.go`/`handler_acme_domain_validations.go`,
     plus `acme_models.go` for shared constants/ARN patterns/idempotency
     helpers used across all four.
  2. Four non-ACME ops: `SearchCertificates` (`search_certificates.go`/
     `handler_search_certificates.go`) and the generic
     `ListTagsForResource`/`TagResource`/`UntagResource` triad
     (`handler_resource_tags.go`).

- **Ownership/cascade** (the task's explicit instruction not to treat 19 ops
  as independent CRUD): `AcmeEndpoint` owns `AcmeExternalAccountBinding`,
  `AcmeAccount`, and `AcmeDomainValidation` via an `AcmeEndpointArn` FK,
  matching the real API's own ARN nesting
  (`acme-endpoint/<epID>/acme-external-account-binding/<id>` and
  `acme-endpoint/<epID>/acme-domain-validation/<id>`, confirmed against the
  live AWS API reference for `CreateAcmeExternalAccountBinding`/
  `GetAcmeExternalAccountBindingCredentials`/`CreateAcmeDomainValidation`).
  Every Create* in the family validates the parent endpoint exists first
  (`ResourceNotFoundException` otherwise); `DeleteAcmeEndpoint`
  cascade-deletes every EAB/domain-validation/account it owns (via
  `store.Index.Get(epARN)` on `eabsByEndpoint`/`domainValidationsByEndpoint`/
  `acmeAccountsByEndpoint`) rather than leaving orphans or blocking the
  delete.

- **Generic resource tagging routes by ARN type, not certificate assumption**
  (the task's explicit ask): `resolveTaggableResourceArn`
  (`handler_resource_tags.go`) matches `ResourceArn` against each known ARN
  shape (EAB/domain-validation/endpoint/certificate, most-specific-first
  since the EAB/domain-validation shapes are supersets of the endpoint
  shape's prefix) and checks real existence via the matching backend's
  `*Exists` method. A `CertificateArn` passed to `TagResource` reads/writes
  the exact SAME `h.tags` store entry `AddTagsToCertificate`/
  `ListTagsForCertificate` use (verified end-to-end by
  `TestACMHandler_GenericResourceTags_RouteByArnType/CertificateArn_SharesStoreWithAddTagsToCertificate`)
  — real AWS tags are one underlying set per resource regardless of which
  API reads/writes them; the real docs' "use the certificate-specific op for
  certificates" note is a convenience recommendation, not evidence of a
  second disjoint tag store. Malformed `ResourceArn` on these three ops
  returns `ValidationException` (not `InvalidArnException`) since that is
  what the real ops' documented `Errors` sections actually list — confirmed
  by fetching the live AWS API reference pages for
  `ListTagsForResource`/`TagResource`, which enumerate only
  `ResourceNotFoundException`/`ValidationException`
  (`ServiceQuotaExceededException` additionally for `TagResource`, not
  modeled since gopherstack has no quota-exhaustion condition to trigger it
  from).

- **No fabricated verified state** (the task's explicit warning): two spots
  in the new surface deliberately never claim something was verified when it
  was not — see the `gaps` entries for `AcmeAccount` (never populated; real
  accounts come from an actual ACME protocol client hitting the endpoint's
  `EndpointUrl`, which gopherstack does not implement) and
  `AcmeDomainValidation.Status` (always `VALIDATING`; gopherstack has no DNS
  resolver to check the synthesized prevalidation record against, so it
  never claims `VALID`).

- **ARN shapes field-diffed against the live AWS API reference** (not just
  the Go SDK, since ARN patterns are documented but not enforced client-side
  in the generated code): `acme-endpoint/<id>`,
  `acme-endpoint/<id>/acme-external-account-binding/<id>`,
  `acme-endpoint/<id>/acme-domain-validation/<id>` — confirmed against
  `API_CreateAcmeEndpoint`/`API_CreateAcmeExternalAccountBinding`/
  `API_CreateAcmeDomainValidation`/`API_GetAcmeExternalAccountBindingCredentials`'s
  documented `Pattern:` constraints. `iamRoleArnPattern` similarly mirrors
  `CreateAcmeExternalAccountBindingInput.RoleArn`'s documented pattern
  (`arn:aws[a-z-]*:iam::[0-9]{12}:role/.+`).

- **Union wire shapes verified against `serializers.go`/`deserializers.go`**,
  not guessed from the Go type names: every ACM union (`CertificateAuthority`,
  `PrevalidationOptions`/`PrevalidationDetails`, `CertificateFilterStatement`,
  `CertificateFilter`, `AcmCertificateMetadataFilter`, `X509AttributeFilter`,
  `SubjectAlternativeNameFilter`, `CertificateMetadata`, `GeneralName`)
  serializes/deserializes as a single-key wrapper object keyed by the Go
  member-type's suffix (e.g. `CertificateAuthorityMemberPublicCertificateAuthority`
  → `{"PublicCertificateAuthority": {...}}`), confirmed by reading the
  `awsAwsjson11_serializeDocument*`/`awsAwsjson11_deserializeDocument*`
  functions directly rather than inferring the wrapper key from the type
  name (which would have been correct here, but was verified rather than
  assumed).

- **Idempotency**: `CreateAcmeEndpoint`/`CreateAcmeExternalAccountBinding`/
  `CreateAcmeDomainValidation` each accept an optional `IdempotencyToken`,
  implemented via `checkAcmeIdempotency` (`acme_models.go`) — a token reused
  with an identical request (checked via a per-family field fingerprint, not
  full struct equality, matching the existing `RequestCertificate`/
  `PutAccountConfiguration` idempotency pattern in this package) returns the
  original resource; a token reused with different fields returns
  `ConflictException`, which all three ops' documented `Errors` sections
  list.

- **Persistence**: the ACME resource family's four tables
  (`acmeEndpoints`/`acmeExternalAccountBindings`/`acmeDomainValidations`/
  `acmeAccounts`) register directly on `b.registry` via `store.Register` —
  unlike `certs`, none of the new structs need Certificate's "dirty table"
  DTO indirection, since `Region` is a plain exported JSON-tagged field on
  each (the wire-shape structs living in the `handler_acme_*.go` files never
  reference it, so there is no risk of it leaking onto the wire). The three
  new idempotency-token maps persist as plain fields on `backendSnapshot`,
  same pattern as the pre-existing `idempotencyMap`/`accountIdempotency`.
  Round-tripped by the full existing `persistence_test.go` suite (which
  passes unmodified) plus every new table's implicit coverage via
  `TestInMemoryBackend_SnapshotRestore`'s existing `b.registry`-wide
  assertions.

- **Lint decomposition**: adding a 15th case to `handleOpError`'s switch and
  a comparable growth to a prospective `SearchCertificates` sort-field switch
  both would have pushed `cyclop` over its threshold; both were converted to
  table/map lookups instead (`acmErrorCodeTable`, `searchSortComparators`),
  which is a net simplification, not new code weight, and generalizes better
  than adding another `//nolint:cyclop` (banned in this campaign) ever would.
  `CreateAcmeExternalAccountBinding`/`CreateAcmeDomainValidation` similarly
  had their input-validation blocks factored into standalone
  `validateCreateAcme*Params` functions for the same reason.
  `persistence.go`'s `Restore` was split into `resetToEmpty`/
  `applyRestoredMaps` helpers to stay under `funlen`'s statement cap once the
  three new idempotency-map fields were added to it.

## Notes (2026-07-30 pass — B-grade re-audit)

- **Scope**: this service's `overall: B` line explicitly attributes the grade to
  "genuine but incomplete coverage of new surface, not a bug fix on
  audited-and-confirmed-accurate old surface." This pass re-read every `gaps`/
  `deferred` bullet against the currently-installed
  `aws-sdk-go-v2/service/acm@v1.43.0` to check whether that reasoning was still
  current, or whether any bullet was actually a stale/closeable mechanical gap
  rather than a genuine design-scope boundary.

- **ManagedBy: found genuinely closeable, fixed end-to-end.** Read
  `types.go`/`api_op_RequestCertificate.go` directly: `RequestCertificateInput.ManagedBy`,
  `CertificateDetail.ManagedBy`, `CertificateSummary.ManagedBy`, and
  `AcmCertificateMetadata(Filter).ManagedBy` are all real fields sharing one enum
  (`types.CertificateManagedBy`, currently single-valued: `CLOUDFRONT`). Unlike the
  ACME-account/DNS-verification gaps (which require simulating a protocol gopherstack
  correctly declines to fake), echoing a caller-supplied enum value is exactly the
  same class of fix as the prior pass's `Options.Export` field -- no protocol or
  cryptography to fake, just a missing field. Implemented:
  - `RequestCertificate.ManagedBy` accepted, validated against the single real enum
    value (`validateManagedBy`, `certificate_validation.go`) *before* the certificate
    is created (so a bad value never orphans a certificate, same reasoning as
    `DomainValidationOptions`), stored via a new `SetManagedBy` backend call mirroring
    `SetExportPreference`'s immutable-after-creation pattern.
  - Echoed on `DescribeCertificate` (`CertificateDetail.ManagedBy`), `ListCertificates`
    (`CertificateSummary.ManagedBy`), and `SearchCertificates`
    (`AcmCertificateMetadata.ManagedBy`).
  - `SearchCertificates`' `AcmCertificateMetadataFilter.ManagedBy` filter member and
    `MANAGED_BY` `SortBy` value now match/sort against real data instead of falling
    into the "no tracked data, never matches" default case.
  - New table-test coverage (per this campaign's testing standards -- cases added to
    existing tables, no new test files/standalone funcs): a
    `RequestCertificate.ManagedBy` case table in `wire_field_additions_test.go`
    (`TestACMHandler_RequestCertificate_ManagedBy` — accept+echo, absent-stays-absent,
    reject-unknown-value) and an `AcmCertificateMetadataFilter_ManagedBy` case added to
    the existing `TestACMHandler_SearchCertificates` table in
    `handler_certificates_list_test.go`.

- **Held at B, not raised to A.** Fixing ManagedBy closes the one mechanical
  field-diff gap this pass found, but it does not touch the reasons the rest of
  `gaps`/`deferred` remain open, all of which were re-confirmed as genuine, not
  stale: a real RFC 8555 ACME protocol front-end (`AcmeAccount` population) and
  real DNS-record verification (`AcmeDomainValidation.Status` leaving
  `VALIDATING`) are both explicitly out of scope per this rollout's own prior
  instruction against faking protocol/cryptographic verification that didn't
  happen; the 2025 exportable-public-certificates gating remains unfixed because
  the exact error contract could not be confirmed from available documentation
  (guessing it would trade a coverage gap for a wire-shape bug, the more severe
  class per `parity-principles.md`); `X509AttributeFilter.Subject` structured-DN
  filtering has no backing structured data to filter without a Subject-parsing
  project of its own. None of these are mechanical field-diff gaps like
  ManagedBy was -- they are genuine, still-open scope/verification boundaries.

## Notes (2026-07-30 pass #2 — parity-5, raised to A)

- **Method**: re-opened all five reasons this service was held at B, one at a time,
  each starting from "is the stated reason still true" rather than taken on faith
  (per this campaign's explicit instruction to challenge "structural" labels). Two
  fetched primary AWS documentation this pass (`WebFetch` against
  `docs.aws.amazon.com/acm/latest/APIReference/...`), not just the bundled Go SDK's
  doc comments, which is what actually resolved the previously-"unconfirmed error
  contract" blocker on the 2025 export-gating item.

- **2025 exportable-public-certificates gating, closed**: `API_ExportCertificate.html`'s
  `Errors` section has exactly five entries and `RequestInProgressException`'s own
  documented meaning ("the certificate ... has not yet been issued") ruled it out for
  an already-issued-but-ineligible certificate; `ValidationException` ("supplied input
  failed to satisfy constraints") is the only plausible fit among the remaining four.
  `API_CertificateOptions.html`'s `Export` doc gave the actual eligibility rule
  ("opt in ... by specifying `ENABLED`"). New `validateCertExportable`
  (`certificates.go`) implements: IMPORTED/PRIVATE unconditionally exportable (real
  AWS never withheld their keys); AMAZON_ISSUED gated on `ExportPref ==
  "ENABLED"`; a still-`PENDING_VALIDATION`/`VALIDATION_TIMED_OUT`/`FAILED`
  AMAZON_ISSUED certificate keeps `RequestInProgressException`. The
  pre-2025-06-17 date carve-out isn't separately modeled -- every certificate this
  backend creates is timestamped with the real current time, always after that
  cutoff, so the condition can never fire for a gopherstack-issued certificate.
  Discovered and fixed a second, related bug while here: `CertificateSummary.Exported`
  was gated to `Type == PRIVATE`, mirroring a doc-comment restriction that was real in
  `aws-sdk-go-v2/service/acm@v1.37.21` ("This value exists only when the certificate
  type is PRIVATE") but has been silently dropped from the currently-installed
  v1.43.0's `types.go` -- AWS removed the restriction when it shipped exportable
  public certificates. Fixed to set unconditionally (`buildCertificateSummary`,
  `handler_certificates.go`), matching `SearchCertificates`' own `Exported` field,
  which was already correctly unconditional. Tests:
  `TestACMHandler_ExportCertificate_AmazonIssued` (rewritten as a 3-case table --
  still-pending/issued-without-Export/issued-with-Export, replacing a test whose old
  name asserted behavior this pass proved wrong) and
  `TestACMBackend_ExportCertificate`'s two new cases.

- **SearchCertificates `X509AttributeFilter.Subject`, closed, and narrower than
  assumed**: the real `SubjectFilter` union (`types.go`) defines exactly one member,
  `CommonName` -- the "full Distinguished Name filtering" the old gap description
  described as missing was never actually offered by the real API to begin with, only
  `CommonName` was. Implementing it surfaced an independent, genuine wire-shape bug:
  `crypto.go` collapsed `pkix.Name`/`x509.Certificate.Subject` into a single flattened
  display string immediately at cert creation/import time (`Certificate.Subject`,
  e.g. `"CN=example.com,OU=Server CA 1B,O=Amazon,C=US"`), and `certToSearchResult`
  (`handler_search_certificates.go`) fed that whole string into
  `X509Attributes.Subject.CommonName`, which per `types.go`'s `DistinguishedName`
  should hold just the CN (`"example.com"`). Fixed by capturing
  `pkix.Name.CommonName`/`cert.Subject.CommonName` (and the `Issuer` equivalents)
  separately at the point both `generateSelfSignedCert` and `extractCertMetadataFull`
  already parse/construct the structured name (`crypto.go`), storing them as new
  `Certificate.SubjectCommonName`/`IssuerCommonName` fields, and wiring the `Subject`
  filter member + `SortBy=COMMON_NAME` (a real enum value, previously in the
  no-tracked-data ARN-order fallback) on top of that real data. Test:
  `TestACMHandler_SearchCertificates/X509AttributeFilter_SubjectCommonName`, which
  also locks in the wire-shape fix by asserting the response's `CommonName` is the
  bare domain, not the flattened DN string.

- **The other three reasons re-confirmed, not re-asserted**: RFC 8555 ACME protocol
  front-end and HTTP validation method's server-side contract remain unconfirmable
  without either building real cryptographic protocol handling or fabricating an
  error contract respectively (HTTP gained one genuine new data point this pass —
  `ValidationMethod`'s `Valid Values` technically include `HTTP` — but not enough to
  safely act on, see gaps). Route 53 DNS-record verification for
  `AcmeDomainValidation.Status` got the deepest re-investigation: confirmed
  gopherstack already has a working cross-service backend-injection precedent
  (`services/cloudformation`'s `ServiceBackends`, wired in `cli.go`) that `acm` could
  in principle reuse, and confirmed no import cycle blocks `acm` importing `route53`
  -- but actually wiring it is cli.go-initialization-order-changing, cross-cutting
  work on the same scale as a full family, not a same-file fix, so it was flagged
  with a concrete next-pass path rather than half-wired or dismissed.

- **Held at B in the prior pass, raised to A this pass**: two closed items each
  involved a genuine wire-shape/error-contract fix (the `Exported`-gating staleness
  and the `X509Attributes.Subject.CommonName` bug), which is this file's own stated
  bar for A ("genuine fix found (wire-shape bug)"), on top of a real, previously-open
  feature-completeness gap (2025 export gating) closed with primary-source evidence
  instead of guesswork. The three remaining open items were re-verified as genuinely
  out of reach for a single pass, not re-asserted from the prior pass's notes without
  re-checking.

## Notes (2026-08-10 pass — bd gopherstack-vuf6)

- **sdk_module pin was stale**: this file's header cited v1.43.0 but go.mod pins
  v1.43.4. Diffed both module-cache trees (`diff -rq`) -- only CHANGELOG.md/go.mod/
  go.sum/go_module_metadata.go differ; every generated `.go` source file (types,
  serializers, deserializers) is byte-identical between the two versions. No
  existing claim in this file rested on a version-specific difference that doesn't
  exist; header corrected, no re-verification of prior op rows was needed.
- **ExportCertificate AMAZON_ISSUED gating (item 1)**: re-verified from the
  operation's own deserializer switch (`awsAwsjson11_deserializeOpErrorExportCertificate`,
  deserializers.go:1630-1652, v1.43.4) rather than trusting the prior pass's doc-page
  citation alone -- confirms the exact same five-error set
  (InvalidArnException/RequestInProgressException/ResourceNotFoundException/
  ThrottlingException/ValidationException) the prior pass found via docs.aws.amazon.com,
  and confirms gopherstack's current codes (RequestInProgressException while pending,
  ValidationException for issued-but-not-opted-in) are both in that set. Checked both
  directions explicitly: no state this backend can produce lets it accept an export the
  real service would refuse, or refuse one the real service would allow. No code change
  needed; this item was already correctly closed by the parity-5 pass.
- **ManagedBy=CLOUDFRONT (item 2)**: confirmed by reading the code, not just the
  PARITY.md notes -- validateManagedBy validates it, SetManagedBy stores it, and it is
  echoed on DescribeCertificate/ListCertificates/SearchCertificates and filterable via
  AcmCertificateMetadataFilter.ManagedBy. This is genuinely stored-and-echoed data, not
  a field accepted-and-dropped. Real AWS's own docs describe no ACM-side behavioral
  restriction tied to ManagedBy (CloudFront enforces its own restrictions on its side of
  the relationship, not ACM's), so echo-only is the complete real scope for this field,
  not an under-implementation.
- **ValidationMethod HTTP/HttpRedirect (item 3), FIXED**: fetched
  API_HttpRedirect.html and API_RequestCertificate.html directly this pass.
  `types.DomainValidation.HttpRedirect`'s doc comment ("exists only when ... the
  validation method is HTTP") establishes the real wire shape is mutually exclusive
  with ResourceRecord/ValidationEmails. Found and fixed a genuine bug independent of
  what real AWS does for a direct HTTP request: `buildInitialDVOList`
  (certificates.go) funneled ValidationMethod=HTTP into the SAME default branch used
  for PRIVATE certificates (no validation needed) -- issuing the certificate
  immediately (status=ISSUED) and stamping a DNS-shaped ResourceRecord onto a
  DomainValidationOption whose ValidationMethod field said "HTTP". Fixed by adding an
  HTTP case throughout (buildInitialDVOList, buildDomainValidationOptions, the wire
  structs) so HTTP now starts PENDING_VALIDATION and returns a synthetic HttpRedirect
  instead. What is NOT claimed: whether real AWS's public RequestCertificate API
  actually accepts a direct (non-CloudFront) customer call with ValidationMethod=HTTP
  at all -- neither fetched page confirms accept-vs-reject for that path, so no
  rejection error was fabricated; see gaps. New test:
  `Test_SDKRoundTrip_RequestCertificate_ValidationMethod` (wire_field_additions_test.go)
  round-trips all three methods through the real SDK client and asserts the right
  union member is populated.
- **InvalidArgsException / tag policy (item 4)**: read the SDK source, not the bd
  issue's truncated title, to find the real claim. `InvalidArgsException` belongs
  exclusively to `ListCertificates` (deserializers.go:2698-2747, the ONLY op in this
  package whose deserializer recognizes it, paired only with ValidationException --
  every other op uses a different error family). `TagPolicyException` is real and
  belongs to RequestCertificate/AddTagsToCertificate (AWS Organizations tag policies);
  gopherstack correctly never triggers it, having no tag-policy engine to evaluate
  against -- left absent, not fabricated. FIXED the ListCertificates gap: previously
  no code validated CertificateStatuses/Includes.KeyTypes/KeyUsage/ExtendedKeyUsage/
  SortBy/SortOrder against their real enums (CertificateStatus enums.go:199-224,
  KeyAlgorithm enums.go:416-442, KeyUsageName enums.go:445-476, ExtendedKeyUsageName
  enums.go:328-363, SortBy enums.go:692-697 [CREATED_AT only], SortOrder
  enums.go:709-725), so an invalid value (e.g. `CertificateStatuses: ["BOGUS"]`)
  silently matched nothing and returned 200 -- the more-permissive-than-AWS direction
  this campaign is specifically watching for. `validateListCertificatesParams`
  (certificate_validation.go) now rejects these with the new `ErrInvalidArgs`
  sentinel. New test: `TestACMHandler_ListCertificates_InvalidArgsException`.
- **New gap surfaced, not fixed**: RequestCertificate's own deserializer
  (deserializers.go:3346-3400+) does not recognize ValidationException at all, only
  InvalidParameterException -- yet several of gopherstack's RequestCertificate-path
  validation failures (empty/malformed DomainName, invalid ManagedBy, RSA_1024
  weak-key rejection) still return ValidationException. Not fixed this pass because
  the underlying validators (`validateDomainName`, the weak-key check in crypto.go)
  are shared with RenewCertificate (whose real error set correctly includes
  ValidationException) and CreateAcmeDomainValidation (a third error set); a correct
  fix needs per-caller error codes, which is a larger, separately-scoped change. See
  gaps/deferred.
- **Adjacent bug fixed**: `copyCert`'s `DomainValidationOptions` deep-copy (and the
  identical shallow-copy in `RenewalSummary.DomainValidationOptions`, both
  models.go) copied the `ResourceRecord`/`ValidationEmails` pointer/slice fields
  correctly but would have shared the new `HttpRedirect` pointer between the
  original and the copy had it been added without fixing this -- extracted a shared
  `copyDomainValidationOptions` helper (also fixes `funlen`/`gocognit` growth from the
  new field) and used it in both places, so `RenewalSummary`'s copy is now
  correctly isolated too, closing a pre-existing (if narrower) aliasing gap that
  predates this pass.
- **Verification**: pre-fix failures for both fixed items were captured via a
  `git worktree` at HEAD (not `git stash`) with only the new/modified test files
  copied in over the pre-fix production code -- `Test_SDKRoundTrip_RequestCertificate_ValidationMethod/http`
  failed asserting `PENDING_VALIDATION` (got `ISSUED`) and a nil `HttpRedirect`;
  `TestACMHandler_ListCertificates_InvalidArgsException`'s four cases each failed
  asserting HTTP 400 (got 200); the persistence round-trip case failed to compile
  (`HTTPRedirect` field did not exist). All three now pass against the fix.
