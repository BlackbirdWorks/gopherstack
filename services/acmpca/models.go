package acmpca

import (
	"crypto/ecdsa"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

const (
	caStatusCreating           = "CREATING"
	caStatusActive             = "ACTIVE"
	caStatusDisabled           = "DISABLED"
	caStatusDeleted            = "DELETED"
	caStatusPendingCertificate = "PENDING_CERTIFICATE"
	caTypePRoot                = "ROOT"
	caTypeSubordinate          = "SUBORDINATE"
	defaultMaxItems            = 100
	certStatusActive           = "ACTIVE"
	certStatusRevoked          = "REVOKED"
	defaultKeyAlgorithm        = "EC_prime256v1"
	defaultSignAlgorithm       = "SHA256WITHECDSA"
	caResourceIDPrefix         = "certificate-authority/"
	certResourceIDPrefix       = "certificate/"
	reportResourcePrefix       = "audit-report/"
	auditReportStatus          = "SUCCESS"
	auditReportFormatCSV       = "CSV"
	auditReportFormatJSON      = "JSON"
	actionGetCertificate       = "GetCertificate"
	actionIssueCertificate     = "IssueCertificate"
	actionListPermissions      = "ListPermissions"
	// acmServicePrincipal is the only valid CreatePermission Principal per
	// aws-sdk-go-v2's CreatePermissionInput.Principal doc comment.
	acmServicePrincipal          = "acm.amazonaws.com"
	permanentDeletionMinDays     = int32(7)
	permanentDeletionMaxDays     = int32(30)
	defaultPermanentDeletionDays = int32(30)

	// serialBitLen is the number of bits for a random serial number.
	serialBitLen = 128
)

const (
	revocationReasonUnspecified   = "UNSPECIFIED"
	revocationReasonKeyCompromise = "KEY_COMPROMISE"
	revocationReasonCACompromise  = "CERTIFICATE_AUTHORITY_COMPROMISE"
	revocationReasonAffiliation   = "AFFILIATION_CHANGED"
	revocationReasonSuperseded    = "SUPERSEDED"
	revocationReasonCessation     = "CESSATION_OF_OPERATION"
	revocationReasonPrivWithdrawn = "PRIVILEGE_WITHDRAWN"
	revocationReasonAACompromise  = "A_A_COMPROMISE"
)

// KeyStorageSecurityStandard values, mirroring types.KeyStorageSecurityStandard.
const (
	keyStorageStandardFips2 = "FIPS_140_2_LEVEL_2_OR_HIGHER"
	keyStorageStandardFips3 = "FIPS_140_2_LEVEL_3_OR_HIGHER"
	keyStorageStandardCCPC1 = "CCPC_LEVEL_1_OR_HIGHER"
)

// CertificateAuthorityUsageMode values, mirroring types.CertificateAuthorityUsageMode.
const (
	usageModeGeneralPurpose        = "GENERAL_PURPOSE"
	usageModeShortLivedCertificate = "SHORT_LIVED_CERTIFICATE"
	// shortLivedCertMaxValidityDays is the real API's documented validity cap
	// for certificates issued by a SHORT_LIVED_CERTIFICATE-usage-mode CA.
	shortLivedCertMaxValidityDays = 7
)

// CrlType values, mirroring types.CrlType.
const (
	crlTypeComplete    = "COMPLETE"
	crlTypePartitioned = "PARTITIONED"
)

// S3ObjectACL values, mirroring types.S3ObjectAcl.
const (
	s3ObjectACLPublicRead  = "PUBLIC_READ"
	s3ObjectACLBucketOwner = "BUCKET_OWNER_FULL_CONTROL"
)

// ResourceOwner values, mirroring types.ResourceOwner.
const (
	resourceOwnerSelf          = "SELF"
	resourceOwnerOtherAccounts = "OTHER_ACCOUNTS"
)

// CertificateAuthoritySubject holds the subject fields for a Certificate Authority.
type CertificateAuthoritySubject struct {
	CommonName         string `json:"CommonName,omitempty"`
	Country            string `json:"Country,omitempty"`
	Organization       string `json:"Organization,omitempty"`
	OrganizationalUnit string `json:"OrganizationalUnit,omitempty"`
	State              string `json:"State,omitempty"`
	Locality           string `json:"Locality,omitempty"`
}

// CertificateAuthorityConfiguration holds the configuration for a Certificate Authority.
type CertificateAuthorityConfiguration struct {
	Subject          CertificateAuthoritySubject `json:"Subject"`
	KeyAlgorithm     string                      `json:"KeyAlgorithm"`
	SigningAlgorithm string                      `json:"SigningAlgorithm"`
}

// CrlConfiguration mirrors aws-sdk-go-v2 types.CrlConfiguration: certificate
// revocation list settings for a CA. CrlDistributionPointExtensionConfiguration
// is flattened into OmitExtension (its only field).
type CrlConfiguration struct {
	CustomCname      string `json:"customCname,omitempty"`
	CustomPath       string `json:"customPath,omitempty"`
	S3BucketName     string `json:"s3BucketName,omitempty"`
	S3ObjectACL      string `json:"s3ObjectAcl,omitempty"`
	CrlType          string `json:"crlType,omitempty"`
	ExpirationInDays int32  `json:"expirationInDays,omitempty"`
	Enabled          bool   `json:"enabled"`
	OmitExtension    bool   `json:"omitExtension,omitempty"`
}

// OcspConfiguration mirrors aws-sdk-go-v2 types.OcspConfiguration: Online
// Certificate Status Protocol settings for a CA.
type OcspConfiguration struct {
	OcspCustomCname string `json:"ocspCustomCname,omitempty"`
	Enabled         bool   `json:"enabled"`
}

// RevocationConfiguration mirrors aws-sdk-go-v2 types.RevocationConfiguration:
// the combined CRL/OCSP configuration reported by DescribeCertificateAuthority
// and accepted by CreateCertificateAuthority/UpdateCertificateAuthority.
type RevocationConfiguration struct {
	CrlConfiguration  *CrlConfiguration  `json:"crlConfiguration,omitempty"`
	OcspConfiguration *OcspConfiguration `json:"ocspConfiguration,omitempty"`
}

// APIPassthroughSubject overrides the CSR-derived certificate subject with
// explicit X.500 attributes, mirroring the commonly-used fields of
// aws-sdk-go-v2 types.ASN1Subject. The exotic RDN types (DistinguishedNameQualifier,
// GenerationQualifier, Initials, Pseudonym, Surname, Title, CustomAttributes) are
// intentionally not modeled -- see decodeASN1Subject in handler_certificates.go,
// which rejects them explicitly rather than silently dropping them.
type APIPassthroughSubject struct {
	CommonName         string
	Country            string
	Organization       string
	OrganizationalUnit string
	State              string
	Locality           string
	SerialNumber       string
}

// APIPassthroughKeyUsage mirrors aws-sdk-go-v2 types.KeyUsage.
type APIPassthroughKeyUsage struct {
	DigitalSignature bool
	NonRepudiation   bool
	KeyEncipherment  bool
	DataEncipherment bool
	KeyAgreement     bool
	KeyCertSign      bool
	CRLSign          bool
	EncipherOnly     bool
	DecipherOnly     bool
}

// APIPassthroughExtendedKeyUsage mirrors aws-sdk-go-v2 types.ExtendedKeyUsage:
// exactly one of Type (a standard ExtendedKeyUsageType) or ObjectIdentifier
// (a custom OID) is set.
type APIPassthroughExtendedKeyUsage struct {
	Type             string
	ObjectIdentifier string
}

// APIPassthroughSAN mirrors the DnsName/IpAddress/Rfc822Name variants of
// aws-sdk-go-v2 types.GeneralName -- the three SubjectAlternativeNames variants
// Terraform's aws_acmpca_certificate resource exposes. The remaining GeneralName
// variants (OtherName, DirectoryName, EdiPartyName, UniformResourceIdentifier,
// RegisteredId) are intentionally not modeled -- see decodeGeneralName in
// handler_certificates.go, which rejects them explicitly.
type APIPassthroughSAN struct {
	DNSName      string
	IPAddress    string
	EmailAddress string
}

// APIPassthroughCustomExtension mirrors aws-sdk-go-v2 types.CustomExtension: an
// arbitrary X.509 extension identified by OID, carrying an already-DER-encoded
// value the caller supplies verbatim (base64 on the wire).
type APIPassthroughCustomExtension struct {
	ObjectIdentifier string
	ValueBase64      string
	Critical         bool
}

// APIPassthroughExtensions mirrors aws-sdk-go-v2 types.Extensions.
// CertificatePolicies is intentionally not modeled -- see decodeExtensions in
// handler_certificates.go, which rejects it explicitly (OID/PolicyQualifier
// ASN.1 encoding not implemented; PARITY.md tracks this as still-open).
type APIPassthroughExtensions struct {
	KeyUsage                *APIPassthroughKeyUsage
	ExtendedKeyUsage        []APIPassthroughExtendedKeyUsage
	SubjectAlternativeNames []APIPassthroughSAN
	CustomExtensions        []APIPassthroughCustomExtension
}

// APIPassthrough mirrors aws-sdk-go-v2 types.APIPassthrough: the subject and
// X.509 extension overrides IssueCertificate applies when the request's
// TemplateArn selects an APIPassthrough/APICSRPassthrough template variant
// (see decodeAPIPassthrough in handler_certificates.go for that gating).
type APIPassthrough struct {
	Subject    *APIPassthroughSubject
	Extensions *APIPassthroughExtensions
}

// idempotencyRecord is a cached (resourceARN, expiry) pair for a single
// idempotency token, scoped by region+operation+token (see idempotencyCacheKey).
// Real AWS recognizes repeated CreateCertificateAuthority/IssueCertificate calls
// bearing the same IdempotencyToken within a 5-minute window as one logical
// request and returns the original resource's ARN instead of creating a
// duplicate.
type idempotencyRecord struct {
	expiresAt   time.Time
	resourceARN string
}

// CertificateAuthority represents an ACM PCA Certificate Authority.
type CertificateAuthority struct {
	CreatedAt time.Time `json:"createdAt"`
	NotBefore time.Time `json:"notBefore"`
	NotAfter  time.Time `json:"notAfter"`
	// RestorableUntil is the end of the restoration window while the CA is
	// DELETED (see DeleteCertificateAuthority); zero once the CA is not DELETED.
	RestorableUntil time.Time `json:"restorableUntil"`
	// LastStateChangeAt is updated on every operation that changes Status or
	// the CA's certificate material (Create, Import, self-sign-activate,
	// Update, Delete, Restore), mirroring types.CertificateAuthority's
	// LastStateChangeAt field.
	LastStateChangeAt                 time.Time `json:"lastStateChangeAt"`
	privKey                           *ecdsa.PrivateKey
	CertificateAuthorityConfiguration CertificateAuthorityConfiguration `json:"certificateAuthorityConfiguration"`
	// RevocationConfiguration holds the CRL/OCSP settings accepted by
	// CreateCertificateAuthority/UpdateCertificateAuthority; nil means "not
	// configured" (DescribeCertificateAuthority omits the field entirely, as
	// the real SDK does for a nil *types.RevocationConfiguration).
	RevocationConfiguration *RevocationConfiguration `json:"revocationConfiguration,omitempty"`
	ARN                     string                   `json:"arn"`
	OwnerAccount            string                   `json:"ownerAccount"`
	Type                    string                   `json:"type"`
	Status                  string                   `json:"status"`
	// KeyStorageSecurityStandard mirrors types.KeyStorageSecurityStandard;
	// defaults to FIPS_140_2_LEVEL_3_OR_HIGHER, matching the real API's default.
	KeyStorageSecurityStandard string `json:"keyStorageSecurityStandard,omitempty"`
	// UsageMode mirrors types.CertificateAuthorityUsageMode; defaults to
	// GENERAL_PURPOSE. When SHORT_LIVED_CERTIFICATE, IssueCertificate enforces
	// the real API's 7-day validity cap for certificates issued by this CA.
	UsageMode        string `json:"usageMode,omitempty"`
	Serial           string `json:"serial,omitempty"`
	CertificateBody  string `json:"certificateBody,omitempty"`
	CertificateChain string `json:"certificateChain,omitempty"`
	CSR              string `json:"csr,omitempty"`
	// region is the store.Table composite-key qualifier (see regionKey); it is
	// unexported so it is never marshaled by a plain json.Marshal(CertificateAuthority)
	// and is instead carried through persistence via caDTO (see persistence.go).
	region string
}

// IssuedCertificate represents a certificate issued by an ACM PCA Certificate Authority.
type IssuedCertificate struct {
	IssuedAt         time.Time  `json:"issuedAt"`
	NotBefore        time.Time  `json:"notBefore"`
	NotAfter         time.Time  `json:"notAfter"`
	RevokedAt        *time.Time `json:"revokedAt,omitempty"`
	ARN              string     `json:"arn"`
	CAARN            string     `json:"caArn"`
	Status           string     `json:"status"`
	Serial           string     `json:"serial"`
	CertBody         string     `json:"certBody"`
	RevocationReason string     `json:"revocationReason,omitempty"`
	// region is the store.Table composite-key qualifier (see regionKey); carried
	// through persistence via regionalDTO (see persistence.go).
	region string
}

// Permission represents an ACM PCA permission granted on a certificate authority.
type Permission struct {
	CreatedAt               time.Time `json:"createdAt"`
	CertificateAuthorityArn string    `json:"certificateAuthorityArn"`
	Policy                  string    `json:"policy,omitempty"`
	Principal               string    `json:"principal"`
	SourceAccount           string    `json:"sourceAccount,omitempty"`
	// region is the store.Table composite-key qualifier (see regionKey); carried
	// through persistence via regionalDTO (see persistence.go).
	region  string
	Actions []string `json:"actions"`
}

// AuditReport represents an ACM PCA audit report generated for a certificate authority.
type AuditReport struct {
	CreatedAt               time.Time `json:"createdAt"`
	AuditReportID           string    `json:"auditReportId"`
	CertificateAuthorityArn string    `json:"certificateAuthorityArn"`
	S3BucketName            string    `json:"s3BucketName"`
	S3Key                   string    `json:"s3Key"`
	Status                  string    `json:"status"`
	// region is the store.Table composite-key qualifier (see regionKey); carried
	// through persistence via regionalDTO (see persistence.go).
	region string
}

// InMemoryBackend is the in-memory store for ACM PCA resources.
//
// The CA/certificate/permission/audit-report collections below were
// previously nested by region (outer key = region, e.g.
// map[string]map[string]*CertificateAuthority) so that same-ARN-shaped
// resources in different regions were fully isolated. Phase 3.3 of the
// datalayer refactor replaces each of those with a flat *store.Table, keyed
// by the composite "region|id" string (see regionKey), with a companion
// *store.Index grouping certs/permissions by (region, CA ARN) for the
// CA-scoped list operations -- the same region-qualified-table pattern
// services/emr/services/neptune/services/mwaa use. certsByCASerial (a
// derived, rebuilt-on-Restore lookup of bare strings with no identity of
// their own) and policies (a bare string value with no identity struct) are
// deliberately NOT converted to store.Table: store.Table requires a *V value
// with its own identity, which neither has; they remain plain region-nested
// maps.
type InMemoryBackend struct {
	cas             *store.Table[CertificateAuthority]
	casByRegion     *store.Index[CertificateAuthority]
	certs           *store.Table[IssuedCertificate]
	certsByCA       *store.Index[IssuedCertificate]
	certsByCASerial map[string]map[string]string // region → caARN+"#"+serial → certARN
	permissions     *store.Table[Permission]
	permissionsByCA *store.Index[Permission]
	auditReports    *store.Table[AuditReport]
	policies        map[string]map[string]string
	// idempotency caches CreateCertificateAuthority/IssueCertificate
	// IdempotencyToken -> resourceARN for a 5-minute window (see
	// idempotencyRecord). Deliberately NOT persisted through Snapshot/Restore:
	// it is a short-lived request-dedup cache, not durable resource state, and
	// a restored backend starting with an empty cache is indistinguishable
	// from one where every outstanding token has already expired.
	idempotency map[string]idempotencyRecord
	registry    *store.Registry
	mu          *lockmetrics.RWMutex
	accountID   string
	region      string
}
