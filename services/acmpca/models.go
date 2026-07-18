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
	caStatusCreating             = "CREATING"
	caStatusActive               = "ACTIVE"
	caStatusDisabled             = "DISABLED"
	caStatusDeleted              = "DELETED"
	caStatusPendingCertificate   = "PENDING_CERTIFICATE"
	caTypePRoot                  = "ROOT"
	caTypeSubordinate            = "SUBORDINATE"
	defaultMaxItems              = 100
	certStatusActive             = "ACTIVE"
	certStatusRevoked            = "REVOKED"
	defaultKeyAlgorithm          = "EC_prime256v1"
	defaultSignAlgorithm         = "SHA256WITHECDSA"
	caResourceIDPrefix           = "certificate-authority/"
	certResourceIDPrefix         = "certificate/"
	reportResourcePrefix         = "audit-report/"
	auditReportStatus            = "SUCCESS"
	auditReportFormatCSV         = "CSV"
	auditReportFormatJSON        = "JSON"
	actionGetCertificate         = "GetCertificate"
	actionIssueCertificate       = "IssueCertificate"
	actionListPermissions        = "ListPermissions"
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

// CertificateAuthority represents an ACM PCA Certificate Authority.
type CertificateAuthority struct {
	CreatedAt time.Time `json:"createdAt"`
	NotBefore time.Time `json:"notBefore"`
	NotAfter  time.Time `json:"notAfter"`
	// RestorableUntil is the end of the restoration window while the CA is
	// DELETED (see DeleteCertificateAuthority); zero once the CA is not DELETED.
	RestorableUntil                   time.Time `json:"restorableUntil"`
	privKey                           *ecdsa.PrivateKey
	CertificateAuthorityConfiguration CertificateAuthorityConfiguration `json:"certificateAuthorityConfiguration"`
	ARN                               string                            `json:"arn"`
	OwnerAccount                      string                            `json:"ownerAccount"`
	Type                              string                            `json:"type"`
	Status                            string                            `json:"status"`
	Serial                            string                            `json:"serial,omitempty"`
	CertificateBody                   string                            `json:"certificateBody,omitempty"`
	CertificateChain                  string                            `json:"certificateChain,omitempty"`
	CSR                               string                            `json:"csr,omitempty"`
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
	registry        *store.Registry
	mu              *lockmetrics.RWMutex
	accountID       string
	region          string
}
