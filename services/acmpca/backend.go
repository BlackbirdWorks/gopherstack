package acmpca

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	cryptorand "crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

var (
	// ErrCANotFound is returned when a Certificate Authority is not found.
	ErrCANotFound = errors.New("ResourceNotFoundException")
	// ErrCertNotFound is returned when an issued certificate is not found.
	ErrCertNotFound = errors.New("ResourceNotFoundException")
	// ErrInvalidParameter is returned when an invalid parameter is provided.
	ErrInvalidParameter = errors.New("InvalidParameterException")
	// ErrInvalidState is returned when the CA is in an invalid state for the operation.
	ErrInvalidState = errors.New("InvalidStateException")
	// ErrPermissionNotFound is returned when a CA permission is not found.
	ErrPermissionNotFound = errors.New("ResourceNotFoundException")
	// ErrPolicyNotFound is returned when a CA policy is not found.
	ErrPolicyNotFound = errors.New("ResourceNotFoundException")
	// ErrAuditReportNotFound is returned when a CA audit report is not found.
	ErrAuditReportNotFound = errors.New("ResourceNotFoundException")

	errCAPrivKeyNil    = errors.New("CA private key is nil")
	errDecodeCSRPEM    = errors.New("failed to decode CSR PEM")
	errDecodeCACertPEM = errors.New("failed to decode CA certificate PEM")
)

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
	permanentDeletionMinDays   = int32(7)
	permanentDeletionMaxDays   = int32(30)

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
	CreatedAt                         time.Time `json:"createdAt"`
	NotBefore                         time.Time `json:"notBefore"`
	NotAfter                          time.Time `json:"notAfter"`
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
}

// Permission represents an ACM PCA permission granted on a certificate authority.
type Permission struct {
	CreatedAt               time.Time `json:"createdAt"`
	CertificateAuthorityArn string    `json:"certificateAuthorityArn"`
	Policy                  string    `json:"policy,omitempty"`
	Principal               string    `json:"principal"`
	SourceAccount           string    `json:"sourceAccount,omitempty"`
	Actions                 []string  `json:"actions"`
}

// AuditReport represents an ACM PCA audit report generated for a certificate authority.
type AuditReport struct {
	CreatedAt               time.Time `json:"createdAt"`
	AuditReportID           string    `json:"auditReportId"`
	CertificateAuthorityArn string    `json:"certificateAuthorityArn"`
	S3BucketName            string    `json:"s3BucketName"`
	S3Key                   string    `json:"s3Key"`
	Status                  string    `json:"status"`
}

// InMemoryBackend is the in-memory store for ACM PCA resources.
// InMemoryBackend stores ACM PCA state. All resource maps are nested by region
// (outer key = region) so that resources are isolated per region.
type InMemoryBackend struct {
	cas             map[string]map[string]*CertificateAuthority
	certs           map[string]map[string]*IssuedCertificate
	certsByCASerial map[string]map[string]string // region → caARN+"#"+serial → certARN
	permissions     map[string]map[string]*Permission
	auditReports    map[string]map[string]*AuditReport
	policies        map[string]map[string]string
	mu              *lockmetrics.RWMutex
	accountID       string
	region          string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		cas:             make(map[string]map[string]*CertificateAuthority),
		certs:           make(map[string]map[string]*IssuedCertificate),
		certsByCASerial: make(map[string]map[string]string),
		permissions:     make(map[string]map[string]*Permission),
		auditReports:    make(map[string]map[string]*AuditReport),
		policies:        make(map[string]map[string]string),
		accountID:       accountID,
		region:          region,
		mu:              lockmetrics.New("acmpca"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// The *Store helpers return the per-region inner map, lazily creating it.
// Callers must hold b.mu.

func (b *InMemoryBackend) casStore(region string) map[string]*CertificateAuthority {
	if b.cas[region] == nil {
		b.cas[region] = make(map[string]*CertificateAuthority)
	}

	return b.cas[region]
}

func (b *InMemoryBackend) certsStore(region string) map[string]*IssuedCertificate {
	if b.certs[region] == nil {
		b.certs[region] = make(map[string]*IssuedCertificate)
	}

	return b.certs[region]
}

func (b *InMemoryBackend) certsByCASerialStore(region string) map[string]string {
	if b.certsByCASerial[region] == nil {
		b.certsByCASerial[region] = make(map[string]string)
	}

	return b.certsByCASerial[region]
}

func (b *InMemoryBackend) permissionsStore(region string) map[string]*Permission {
	if b.permissions[region] == nil {
		b.permissions[region] = make(map[string]*Permission)
	}

	return b.permissions[region]
}

func (b *InMemoryBackend) auditReportsStore(region string) map[string]*AuditReport {
	if b.auditReports[region] == nil {
		b.auditReports[region] = make(map[string]*AuditReport)
	}

	return b.auditReports[region]
}

func (b *InMemoryBackend) policiesStore(region string) map[string]string {
	if b.policies[region] == nil {
		b.policies[region] = make(map[string]string)
	}

	return b.policies[region]
}

// CreateCertificateAuthority creates a new Certificate Authority.
func (b *InMemoryBackend) CreateCertificateAuthority(
	ctx context.Context,
	caType string,
	cfg CertificateAuthorityConfiguration,
) (*CertificateAuthority, error) {
	if caType == "" {
		caType = caTypePRoot
	}

	if caType != caTypePRoot && caType != caTypeSubordinate {
		return nil, fmt.Errorf("%w: CertificateAuthorityType must be ROOT or SUBORDINATE", ErrInvalidParameter)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateCertificateAuthority")
	defer b.mu.Unlock()

	id, err := newRandomID()
	if err != nil {
		return nil, err
	}

	caARN := arn.Build("acm-pca", region, b.accountID, caResourceIDPrefix+id)

	if cfg.KeyAlgorithm == "" {
		cfg.KeyAlgorithm = defaultKeyAlgorithm
	}

	if cfg.SigningAlgorithm == "" {
		cfg.SigningAlgorithm = defaultSignAlgorithm
	}

	privKey, err := ecdsa.GenerateKey(elliptic.P256(), cryptorand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate CA key: %w", err)
	}

	csrPEM, err := generateCSR(privKey, cfg.Subject)
	if err != nil {
		return nil, fmt.Errorf("generate CSR: %w", err)
	}

	now := time.Now().UTC()
	ca := &CertificateAuthority{
		ARN:                               caARN,
		OwnerAccount:                      b.accountID,
		Type:                              caType,
		Status:                            caStatusCreating,
		CertificateAuthorityConfiguration: cfg,
		CSR:                               csrPEM,
		CreatedAt:                         now,
		privKey:                           privKey,
	}

	b.casStore(region)[caARN] = ca

	// For ROOT CAs we auto-sign and activate to make Terraform apply succeed without
	// requiring a multi-step workflow.
	if caType == caTypePRoot {
		if activateErr := b.selfSignAndActivate(ca, now); activateErr != nil {
			return nil, activateErr
		}
	} else {
		// SUBORDINATE CAs immediately transition to PENDING_CERTIFICATE, matching
		// AWS behaviour (CREATING is only a transient internal state).
		ca.Status = caStatusPendingCertificate
	}

	cp := copyCA(ca)

	return &cp, nil
}

// selfSignAndActivate generates a self-signed certificate for the CA and sets it to ACTIVE.
// Must be called with the write lock held.
func (b *InMemoryBackend) selfSignAndActivate(ca *CertificateAuthority, now time.Time) error {
	certPEM, serial, err := selfSignCA(ca, now)
	if err != nil {
		return fmt.Errorf("self-sign CA: %w", err)
	}

	ca.CertificateBody = certPEM
	ca.Serial = serial
	ca.Status = caStatusActive
	ca.NotBefore = now
	ca.NotAfter = now.Add(10 * 365 * 24 * time.Hour)

	return nil
}

// verifyCertificateAuthorityActive checks that the CA exists and is not DELETED.
func (b *InMemoryBackend) verifyCertificateAuthorityActive(ctx context.Context, caARN string) error {
	region := getRegion(ctx, b.region)

	b.mu.RLock("verifyCertificateAuthorityActive")
	defer b.mu.RUnlock()

	ca, ok := b.casStore(region)[caARN]
	if !ok {
		return fmt.Errorf("%w: CA %s not found", ErrCANotFound, caARN)
	}

	if ca.Status == caStatusDeleted {
		return fmt.Errorf("%w: CA is DELETED", ErrInvalidState)
	}

	return nil
}

// DescribeCertificateAuthority returns the CA with the given ARN.
func (b *InMemoryBackend) DescribeCertificateAuthority(
	ctx context.Context, caARN string,
) (*CertificateAuthority, error) {
	if err := validateRequiredParameter(caARN, "CertificateAuthorityArn"); err != nil {
		return nil, err
	}

	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeCertificateAuthority")
	defer b.mu.RUnlock()

	ca, ok := b.casStore(region)[caARN]
	if !ok {
		return nil, fmt.Errorf("%w: CA %s not found", ErrCANotFound, caARN)
	}

	cp := copyCA(ca)

	return &cp, nil
}

// ListCertificateAuthorities returns a paginated list of CAs sorted by ARN.
func (b *InMemoryBackend) ListCertificateAuthorities(
	ctx context.Context, nextToken string, maxItems int,
) page.Page[CertificateAuthority] {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListCertificateAuthorities")

	casMap := b.casStore(region)
	cas := make([]CertificateAuthority, 0, len(casMap))
	for _, ca := range casMap {
		cas = append(cas, copyCA(ca))
	}
	b.mu.RUnlock()

	sort.Slice(cas, func(i, j int) bool { return cas[i].ARN < cas[j].ARN })

	return page.New(cas, nextToken, maxItems, defaultMaxItems)
}

// DeleteCertificateAuthority marks the CA as DELETED.
func (b *InMemoryBackend) DeleteCertificateAuthority(
	ctx context.Context, caARN string, permanentDeletionDays int32,
) error {
	if permanentDeletionDays != 0 &&
		(permanentDeletionDays < permanentDeletionMinDays || permanentDeletionDays > permanentDeletionMaxDays) {
		return fmt.Errorf(
			"%w: PermanentDeletionTimeInDays must be between %d and %d",
			ErrInvalidParameter,
			permanentDeletionMinDays,
			permanentDeletionMaxDays,
		)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteCertificateAuthority")
	defer b.mu.Unlock()

	ca, ok := b.casStore(region)[caARN]
	if !ok {
		return fmt.Errorf("%w: CA %s not found", ErrCANotFound, caARN)
	}

	switch ca.Status {
	case caStatusDisabled, caStatusCreating, caStatusPendingCertificate:
		// allowed
	default:
		return fmt.Errorf(
			"%w: CA must be in DISABLED, CREATING, or PENDING_CERTIFICATE state (current: %s)",
			ErrInvalidState,
			ca.Status,
		)
	}

	ca.Status = caStatusDeleted

	return nil
}

// UpdateCertificateAuthority updates the CA status.
func (b *InMemoryBackend) UpdateCertificateAuthority(ctx context.Context, caARN, status string) error {
	if err := validateRequiredParameter(caARN, "CertificateAuthorityArn"); err != nil {
		return err
	}

	if status != "" && status != caStatusActive && status != caStatusDisabled {
		return fmt.Errorf("%w: status must be ACTIVE or DISABLED", ErrInvalidParameter)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdateCertificateAuthority")
	defer b.mu.Unlock()

	ca, ok := b.casStore(region)[caARN]
	if !ok {
		return fmt.Errorf("%w: CA %s not found", ErrCANotFound, caARN)
	}

	if status != "" {
		ca.Status = status
	}

	return nil
}

// GetCertificateAuthorityCsr returns the CSR PEM for the given CA.
func (b *InMemoryBackend) GetCertificateAuthorityCsr(ctx context.Context, caARN string) (string, error) {
	if err := validateRequiredParameter(caARN, "CertificateAuthorityArn"); err != nil {
		return "", err
	}

	region := getRegion(ctx, b.region)

	b.mu.RLock("GetCertificateAuthorityCsr")
	defer b.mu.RUnlock()

	ca, ok := b.casStore(region)[caARN]
	if !ok {
		return "", fmt.Errorf("%w: CA %s not found", ErrCANotFound, caARN)
	}

	return ca.CSR, nil
}

// ImportCertificateAuthorityCertificate imports a signed certificate for the CA, activating it.
// It parses the certificate to extract NotBefore/NotAfter and stores the optional chain.
func (b *InMemoryBackend) ImportCertificateAuthorityCertificate(
	ctx context.Context, caARN, certPEM, chainPEM string,
) error {
	if err := validateRequiredParameter(caARN, "CertificateAuthorityArn"); err != nil {
		return err
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("ImportCertificateAuthorityCertificate")
	defer b.mu.Unlock()

	ca, ok := b.casStore(region)[caARN]
	if !ok {
		return fmt.Errorf("%w: CA %s not found", ErrCANotFound, caARN)
	}

	block, _ := pem.Decode([]byte(certPEM))
	if block == nil {
		return fmt.Errorf("%w: failed to decode certificate PEM for CA %s", ErrInvalidParameter, caARN)
	}

	parsedCert, parseErr := x509.ParseCertificate(block.Bytes)
	if parseErr != nil {
		return fmt.Errorf("%w: failed to parse certificate for CA %s: %w", ErrInvalidParameter, caARN, parseErr)
	}

	ca.NotBefore = parsedCert.NotBefore
	ca.NotAfter = parsedCert.NotAfter
	ca.Serial = hex.EncodeToString(parsedCert.SerialNumber.Bytes())

	ca.CertificateBody = certPEM
	ca.CertificateChain = chainPEM
	ca.Status = caStatusActive

	return nil
}

// GetCertificateAuthorityCertificate returns the certificate body and chain PEM for the given CA.
func (b *InMemoryBackend) GetCertificateAuthorityCertificate(
	ctx context.Context, caARN string,
) (string, string, error) {
	if err := validateRequiredParameter(caARN, "CertificateAuthorityArn"); err != nil {
		return "", "", err
	}

	region := getRegion(ctx, b.region)

	b.mu.RLock("GetCertificateAuthorityCertificate")
	defer b.mu.RUnlock()

	ca, ok := b.casStore(region)[caARN]
	if !ok {
		return "", "", fmt.Errorf("%w: CA %s not found", ErrCANotFound, caARN)
	}

	if ca.CertificateBody == "" {
		return "", "", fmt.Errorf("%w: CA %s has no certificate imported", ErrCANotFound, caARN)
	}

	return ca.CertificateBody, ca.CertificateChain, nil
}

// IssueCertificate issues a new certificate signed by the given CA.
func (b *InMemoryBackend) IssueCertificate(
	ctx context.Context, caARN, csrPEM string, validityDays int,
) (*IssuedCertificate, error) {
	if err := validateRequiredParameter(caARN, "CertificateAuthorityArn"); err != nil {
		return nil, err
	}

	if err := validateRequiredParameter(csrPEM, "Csr"); err != nil {
		return nil, err
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("IssueCertificate")
	defer b.mu.Unlock()

	ca, ok := b.casStore(region)[caARN]
	if !ok {
		return nil, fmt.Errorf("%w: CA %s not found", ErrCANotFound, caARN)
	}

	if ca.Status != caStatusActive {
		return nil, fmt.Errorf("%w: CA %s is not ACTIVE", ErrInvalidState, caARN)
	}

	if validityDays <= 0 {
		validityDays = 365
	}

	certPEM, serial, err := signCSR(ca, csrPEM, validityDays)
	if err != nil {
		return nil, fmt.Errorf("sign CSR: %w", err)
	}

	id, err := newRandomID()
	if err != nil {
		return nil, err
	}

	certARN := arn.Build("acm-pca", region, b.accountID,
		caResourceIDPrefix+extractCAID(caARN)+"/"+certResourceIDPrefix+id)

	now := time.Now().UTC()
	cert := &IssuedCertificate{
		ARN:       certARN,
		CAARN:     caARN,
		Status:    certStatusActive,
		Serial:    serial,
		CertBody:  certPEM,
		IssuedAt:  now,
		NotBefore: now,
		NotAfter:  now.Add(time.Duration(validityDays) * 24 * time.Hour),
	}

	b.certsStore(region)[certARN] = cert
	b.certsByCASerialStore(region)[caARN+"#"+serial] = certARN

	cp := *cert

	return &cp, nil
}

// GetCertificate returns the certificate for the given CA and certificate ARN.
// It validates that the certificate belongs to the specified CA.
func (b *InMemoryBackend) GetCertificate(ctx context.Context, caARN, certARN string) (*IssuedCertificate, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("GetCertificate")
	defer b.mu.RUnlock()

	cert, ok := b.certsStore(region)[certARN]
	if !ok {
		return nil, fmt.Errorf("%w: certificate %s not found", ErrCertNotFound, certARN)
	}

	if cert.CAARN != caARN {
		return nil, fmt.Errorf("%w: certificate %s does not belong to CA %s", ErrCANotFound, certARN, caARN)
	}

	cp := *cert

	return &cp, nil
}

// RevokeCertificate revokes the given certificate using the O(1) serial index.
func (b *InMemoryBackend) RevokeCertificate(ctx context.Context, caARN, serial, revocationReason string) error {
	if revocationReason != "" {
		switch revocationReason {
		case revocationReasonUnspecified, revocationReasonKeyCompromise, revocationReasonCACompromise,
			revocationReasonAffiliation, revocationReasonSuperseded, revocationReasonCessation,
			revocationReasonPrivWithdrawn, revocationReasonAACompromise:
			// valid
		default:
			return fmt.Errorf("%w: invalid RevocationReason %q", ErrInvalidParameter, revocationReason)
		}
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("RevokeCertificate")
	defer b.mu.Unlock()

	cas := b.casStore(region)
	ca, ok := cas[caARN]
	if !ok {
		return fmt.Errorf("%w: CA %s not found", ErrCANotFound, caARN)
	}

	if ca.Status == caStatusDeleted {
		return fmt.Errorf("%w: CA %s is DELETED", ErrInvalidState, caARN)
	}

	certARN, ok := b.certsByCASerialStore(region)[caARN+"#"+serial]
	if !ok {
		return fmt.Errorf("%w: certificate with serial %s not found", ErrCertNotFound, serial)
	}

	certs := b.certsStore(region)
	certs[certARN].Status = certStatusRevoked
	now := time.Now().UTC()
	certs[certARN].RevokedAt = &now
	certs[certARN].RevocationReason = revocationReason

	return nil
}

// ListCertificates returns a paginated list of certificates issued by the given CA.
func (b *InMemoryBackend) ListCertificates(
	ctx context.Context,
	caARN string,
	nextToken string,
	maxItems int,
) page.Page[IssuedCertificate] {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListCertificates")
	defer b.mu.RUnlock()

	var certs []IssuedCertificate
	for _, c := range b.certsStore(region) {
		if c.CAARN == caARN {
			certs = append(certs, *c)
		}
	}

	sort.Slice(certs, func(i, j int) bool { return certs[i].ARN < certs[j].ARN })

	return page.New(certs, nextToken, maxItems, defaultMaxItems)
}

// CreateCertificateAuthorityAuditReport creates a new audit report for the given CA.
func (b *InMemoryBackend) CreateCertificateAuthorityAuditReport(
	ctx context.Context,
	caARN string,
	s3BucketName string,
	responseFormat string,
) (*AuditReport, error) {
	if err := validateRequiredParameter(caARN, "CertificateAuthorityArn"); err != nil {
		return nil, err
	}

	if s3BucketName == "" {
		return nil, fmt.Errorf("%w: S3BucketName is required", ErrInvalidParameter)
	}

	format := strings.ToUpper(responseFormat)
	if format != auditReportFormatJSON && format != auditReportFormatCSV {
		return nil, fmt.Errorf("%w: AuditReportResponseFormat must be JSON or CSV", ErrInvalidParameter)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateCertificateAuthorityAuditReport")
	defer b.mu.Unlock()

	auditCA, ok := b.casStore(region)[caARN]
	if !ok {
		return nil, fmt.Errorf("%w: CA %s not found", ErrCANotFound, caARN)
	}

	if auditCA.Status != caStatusActive {
		return nil, fmt.Errorf("%w: CA %s must be ACTIVE to create an audit report", ErrInvalidState, caARN)
	}

	id, err := newRandomID()
	if err != nil {
		return nil, err
	}

	report := &AuditReport{
		CreatedAt:               time.Now().UTC(),
		AuditReportID:           id,
		CertificateAuthorityArn: caARN,
		S3BucketName:            s3BucketName,
		S3Key:                   fmt.Sprintf("%s%s.%s", reportResourcePrefix, id, strings.ToLower(format)),
		Status:                  auditReportStatus,
	}
	b.auditReportsStore(region)[id] = report

	cp := copyAuditReport(report)

	return &cp, nil
}

// DescribeCertificateAuthorityAuditReport returns the audit report for the given CA.
func (b *InMemoryBackend) DescribeCertificateAuthorityAuditReport(
	ctx context.Context,
	caARN string,
	auditReportID string,
) (*AuditReport, error) {
	if err := validateRequiredParameter(caARN, "CertificateAuthorityArn"); err != nil {
		return nil, err
	}

	if err := validateRequiredParameter(auditReportID, "AuditReportId"); err != nil {
		return nil, err
	}

	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeCertificateAuthorityAuditReport")
	defer b.mu.RUnlock()

	if _, ok := b.casStore(region)[caARN]; !ok {
		return nil, fmt.Errorf("%w: CA %s not found", ErrCANotFound, caARN)
	}

	report, ok := b.auditReportsStore(region)[auditReportID]
	if !ok || report.CertificateAuthorityArn != caARN {
		return nil, fmt.Errorf("%w: audit report %s not found", ErrAuditReportNotFound, auditReportID)
	}

	cp := copyAuditReport(report)

	return &cp, nil
}

// CreatePermission creates a permission on the given CA.
func (b *InMemoryBackend) CreatePermission(
	ctx context.Context,
	caARN string,
	principal string,
	sourceAccount string,
	actions []string,
) (*Permission, error) {
	if err := validateRequiredParameter(caARN, "CertificateAuthorityArn"); err != nil {
		return nil, err
	}

	if principal == "" {
		return nil, fmt.Errorf("%w: Principal is required", ErrInvalidParameter)
	}

	if len(actions) == 0 {
		return nil, fmt.Errorf("%w: Actions is required", ErrInvalidParameter)
	}

	for _, action := range actions {
		switch action {
		case actionIssueCertificate, actionGetCertificate, actionListPermissions:
		default:
			return nil, fmt.Errorf("%w: unsupported action %s", ErrInvalidParameter, action)
		}
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("CreatePermission")
	defer b.mu.Unlock()

	if _, ok := b.casStore(region)[caARN]; !ok {
		return nil, fmt.Errorf("%w: CA %s not found", ErrCANotFound, caARN)
	}

	key := permissionKey(caARN, principal, sourceAccount)
	permission := &Permission{
		CreatedAt:               time.Now().UTC(),
		Actions:                 append([]string(nil), actions...),
		CertificateAuthorityArn: caARN,
		Principal:               principal,
		SourceAccount:           sourceAccount,
	}
	b.permissionsStore(region)[key] = permission

	cp := copyPermission(permission)

	return &cp, nil
}

// DeletePermission deletes a permission on the given CA.
func (b *InMemoryBackend) DeletePermission(ctx context.Context, caARN, principal, sourceAccount string) error {
	if err := validateRequiredParameter(caARN, "CertificateAuthorityArn"); err != nil {
		return err
	}

	if err := validateRequiredParameter(principal, "Principal"); err != nil {
		return err
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("DeletePermission")
	defer b.mu.Unlock()

	if _, ok := b.casStore(region)[caARN]; !ok {
		return fmt.Errorf("%w: CA %s not found", ErrCANotFound, caARN)
	}

	key := permissionKey(caARN, principal, sourceAccount)
	permissions := b.permissionsStore(region)
	if _, ok := permissions[key]; !ok {
		return fmt.Errorf("%w: permission for principal %s not found", ErrPermissionNotFound, principal)
	}

	delete(permissions, key)

	return nil
}

// ListPermissions lists permissions on the given CA.
func (b *InMemoryBackend) ListPermissions(
	ctx context.Context, caARN, nextToken string, maxItems int,
) (page.Page[Permission], error) {
	if err := validateRequiredParameter(caARN, "CertificateAuthorityArn"); err != nil {
		return page.Page[Permission]{}, err
	}

	region := getRegion(ctx, b.region)

	b.mu.RLock("ListPermissions")
	defer b.mu.RUnlock()

	if _, ok := b.casStore(region)[caARN]; !ok {
		return page.Page[Permission]{}, fmt.Errorf("%w: CA %s not found", ErrCANotFound, caARN)
	}

	permissions := b.permissionsStore(region)
	perms := make([]Permission, 0, len(permissions))
	for _, perm := range permissions {
		if perm.CertificateAuthorityArn == caARN {
			perms = append(perms, copyPermission(perm))
		}
	}

	sort.Slice(perms, func(i, j int) bool {
		if perms[i].Principal == perms[j].Principal {
			return perms[i].SourceAccount < perms[j].SourceAccount
		}

		return perms[i].Principal < perms[j].Principal
	})

	return page.New(perms, nextToken, maxItems, defaultMaxItems), nil
}

// PutPolicy stores a resource policy on the given CA.
func (b *InMemoryBackend) PutPolicy(ctx context.Context, caARN, policy string) error {
	if err := validateRequiredParameter(caARN, "ResourceArn"); err != nil {
		return err
	}

	if policy == "" {
		return fmt.Errorf("%w: Policy is required", ErrInvalidParameter)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("PutPolicy")
	defer b.mu.Unlock()

	if _, ok := b.casStore(region)[caARN]; !ok {
		return fmt.Errorf("%w: CA %s not found", ErrCANotFound, caARN)
	}

	b.policiesStore(region)[caARN] = policy

	return nil
}

// GetPolicy returns the resource policy for the given CA.
func (b *InMemoryBackend) GetPolicy(ctx context.Context, caARN string) (string, error) {
	if err := validateRequiredParameter(caARN, "ResourceArn"); err != nil {
		return "", err
	}

	region := getRegion(ctx, b.region)

	b.mu.RLock("GetPolicy")
	defer b.mu.RUnlock()

	if _, ok := b.casStore(region)[caARN]; !ok {
		return "", fmt.Errorf("%w: CA %s not found", ErrCANotFound, caARN)
	}

	policy, ok := b.policiesStore(region)[caARN]
	if !ok {
		return "", fmt.Errorf("%w: policy for CA %s not found", ErrPolicyNotFound, caARN)
	}

	return policy, nil
}

// DeletePolicy deletes the resource policy for the given CA.
func (b *InMemoryBackend) DeletePolicy(ctx context.Context, caARN string) error {
	if err := validateRequiredParameter(caARN, "ResourceArn"); err != nil {
		return err
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("DeletePolicy")
	defer b.mu.Unlock()

	if _, ok := b.casStore(region)[caARN]; !ok {
		return fmt.Errorf("%w: CA %s not found", ErrCANotFound, caARN)
	}

	policies := b.policiesStore(region)
	if _, ok := policies[caARN]; !ok {
		return fmt.Errorf("%w: policy for CA %s not found", ErrPolicyNotFound, caARN)
	}

	delete(policies, caARN)

	return nil
}

// RestoreCertificateAuthority restores a deleted CA into the DISABLED state.
func (b *InMemoryBackend) RestoreCertificateAuthority(ctx context.Context, caARN string) error {
	if err := validateRequiredParameter(caARN, "CertificateAuthorityArn"); err != nil {
		return err
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("RestoreCertificateAuthority")
	defer b.mu.Unlock()

	ca, ok := b.casStore(region)[caARN]
	if !ok {
		return fmt.Errorf("%w: CA %s not found", ErrCANotFound, caARN)
	}

	if ca.Status != caStatusDeleted {
		return fmt.Errorf("%w: CA %s is not DELETED", ErrInvalidState, caARN)
	}

	ca.Status = caStatusDisabled

	return nil
}

// copyCA returns a shallow copy of the CertificateAuthority, excluding the private key.
func copyCA(ca *CertificateAuthority) CertificateAuthority {
	cp := *ca
	cp.privKey = nil

	return cp
}

func copyPermission(permission *Permission) Permission {
	cp := *permission
	cp.Actions = append([]string(nil), permission.Actions...)

	return cp
}

func copyAuditReport(report *AuditReport) AuditReport {
	return *report
}

func permissionKey(caARN, principal, sourceAccount string) string {
	// url.QueryEscape does not emit pipe characters, so "|" remains a safe separator.
	return strings.Join([]string{
		url.QueryEscape(caARN),
		url.QueryEscape(principal),
		url.QueryEscape(sourceAccount),
	}, "|")
}

// validateRequiredParameter returns ErrInvalidParameter when a required field is empty.
func validateRequiredParameter(value, fieldName string) error {
	if value == "" {
		return fmt.Errorf("%w: %s is required", ErrInvalidParameter, fieldName)
	}

	return nil
}

// generateCSR generates a PEM-encoded CSR from the given private key and subject.
func generateCSR(privKey *ecdsa.PrivateKey, subject CertificateAuthoritySubject) (string, error) {
	cn := subject.CommonName
	if cn == "" {
		cn = "Gopherstack Root CA"
	}

	tmpl := &x509.CertificateRequest{
		Subject: pkix.Name{
			CommonName:         cn,
			Country:            nonEmptySlice(subject.Country),
			Organization:       nonEmptySlice(subject.Organization),
			OrganizationalUnit: nonEmptySlice(subject.OrganizationalUnit),
			Province:           nonEmptySlice(subject.State),
			Locality:           nonEmptySlice(subject.Locality),
		},
	}

	csrDER, err := x509.CreateCertificateRequest(cryptorand.Reader, tmpl, privKey)
	if err != nil {
		return "", fmt.Errorf("create CSR: %w", err)
	}

	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER})), nil
}

// selfSignCA generates a self-signed certificate for the given CA and returns the PEM and serial hex.
func selfSignCA(ca *CertificateAuthority, now time.Time) (string, string, error) {
	if ca.privKey == nil {
		return "", "", errCAPrivKeyNil
	}

	serial, err := cryptorand.Int(
		cryptorand.Reader,
		new(big.Int).Lsh(big.NewInt(1), serialBitLen),
	)
	if err != nil {
		return "", "", fmt.Errorf("generate serial: %w", err)
	}

	cn := ca.CertificateAuthorityConfiguration.Subject.CommonName
	if cn == "" {
		cn = "Gopherstack Root CA"
	}

	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:         cn,
			Organization:       nonEmptySlice(ca.CertificateAuthorityConfiguration.Subject.Organization),
			Country:            nonEmptySlice(ca.CertificateAuthorityConfiguration.Subject.Country),
			OrganizationalUnit: nonEmptySlice(ca.CertificateAuthorityConfiguration.Subject.OrganizationalUnit),
			Province:           nonEmptySlice(ca.CertificateAuthorityConfiguration.Subject.State),
			Locality:           nonEmptySlice(ca.CertificateAuthorityConfiguration.Subject.Locality),
		},
		NotBefore:             now,
		NotAfter:              now.Add(10 * 365 * 24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(cryptorand.Reader, tmpl, tmpl, &ca.privKey.PublicKey, ca.privKey)
	if err != nil {
		return "", "", fmt.Errorf("create certificate: %w", err)
	}

	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})),
		hex.EncodeToString(serial.Bytes()),
		nil
}

// signCSR signs a CSR using the CA's private key and returns the PEM certificate and serial.
func signCSR(ca *CertificateAuthority, csrPEM string, validityDays int) (string, string, error) {
	if ca.privKey == nil {
		return "", "", errCAPrivKeyNil
	}

	block, _ := pem.Decode([]byte(csrPEM))
	if block == nil {
		return "", "", errDecodeCSRPEM
	}

	csr, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil {
		return "", "", fmt.Errorf("parse CSR: %w", err)
	}

	// Parse CA certificate to get the issuer details.
	caBlock, _ := pem.Decode([]byte(ca.CertificateBody))
	if caBlock == nil {
		return "", "", errDecodeCACertPEM
	}

	caCert, err := x509.ParseCertificate(caBlock.Bytes)
	if err != nil {
		return "", "", fmt.Errorf("parse CA certificate: %w", err)
	}

	serial, err := cryptorand.Int(
		cryptorand.Reader,
		new(big.Int).Lsh(big.NewInt(1), serialBitLen),
	)
	if err != nil {
		return "", "", fmt.Errorf("generate serial: %w", err)
	}

	now := time.Now().UTC()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      csr.Subject,
		NotBefore:    now,
		NotAfter:     now.Add(time.Duration(validityDays) * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     csr.DNSNames,
	}

	certDER, err := x509.CreateCertificate(cryptorand.Reader, tmpl, caCert, csr.PublicKey, ca.privKey)
	if err != nil {
		return "", "", fmt.Errorf("create certificate: %w", err)
	}

	serialHex := hex.EncodeToString(serial.Bytes())

	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})), serialHex, nil
}

// nonEmptySlice returns a slice containing the string if it is non-empty, or nil otherwise.
func nonEmptySlice(s string) []string {
	if s == "" {
		return nil
	}

	return []string{s}
}

// extractCAID extracts the CA ID from a CA ARN.
// e.g. arn:aws:acm-pca:us-east-1:000000000000:certificate-authority/abc123 → abc123.
func extractCAID(caARN string) string {
	parts := splitARN(caARN)
	if len(parts) == 0 {
		return caARN
	}

	last := parts[len(parts)-1]
	// strip "certificate-authority/" prefix
	const prefix = "certificate-authority/"
	if len(last) > len(prefix) {
		return last[len(prefix):]
	}

	return last
}

// newRandomID generates a 16-byte cryptographically-random hex ID for ARN resource segments.
func newRandomID() (string, error) {
	buf := make([]byte, 16) //nolint:mnd // 16-byte random ID
	if _, err := io.ReadFull(cryptorand.Reader, buf); err != nil {
		return "", fmt.Errorf("generate random ID: %w", err)
	}

	return hex.EncodeToString(buf), nil
}

func splitARN(a string) []string {
	// ARN format: arn:partition:service:region:account:resource
	// We want the resource part after the 5th colon.
	count := 0
	for i, c := range a {
		if c == ':' {
			count++
			if count == 5 { //nolint:mnd // 5th colon separates account from resource
				return []string{a[i+1:]}
			}
		}
	}

	return nil
}

// Reset clears all backend state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.cas = make(map[string]map[string]*CertificateAuthority)
	b.certs = make(map[string]map[string]*IssuedCertificate)
	b.certsByCASerial = make(map[string]map[string]string)
	b.permissions = make(map[string]map[string]*Permission)
	b.auditReports = make(map[string]map[string]*AuditReport)
	b.policies = make(map[string]map[string]string)
}
