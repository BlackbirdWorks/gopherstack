package acm

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	cryptorand "crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

var (
	ErrCertNotFound      = errors.New("ResourceNotFoundException")
	ErrInvalidParameter  = errors.New("ValidationException")
	ErrNotEligible       = errors.New("RequestInProgressException")
	ErrRequestInProgress = errors.New("RequestInProgressException")
	ErrAlreadyRevoked    = errors.New("InvalidStateException")
	ErrInvalidState      = errors.New("InvalidStateException")
	ErrResourceInUse     = errors.New("ResourceInUseException")
	ErrConflict          = errors.New("ConflictException")
	errInvalidPEM        = errors.New("failed to decode PEM block")
)

const (
	validationMethodDNS      = "DNS"
	validationMethodEMAIL    = "EMAIL"
	statusPendingValidation  = "PENDING_VALIDATION"
	statusIssued             = "ISSUED"
	statusRevoked            = "REVOKED"
	statusInactive           = "INACTIVE"
	statusExpired            = "EXPIRED"
	statusValidationTimedOut = "VALIDATION_TIMED_OUT"
	statusFailed             = "FAILED"
	validationStatusSuccess  = "SUCCESS"
	validationTokenLen       = 8
	autoValidateDelayMS      = 100
	randByteDivisor          = 2
	certTypeImported         = "IMPORTED"
	certValidityDuration     = 365 * 24 * time.Hour

	defaultDaysBeforeExpiry = int32(45)

	transparencyLoggingEnabled  = "ENABLED"
	transparencyLoggingDisabled = "DISABLED"

	// keyAlgorithmEC is the AWS ACM key algorithm name for ECDSA P-256 keys.
	keyAlgorithmEC = "EC_prime256v1"
	// signatureAlgorithmECDSA is the signature algorithm string for ECDSA with SHA-256.

	// hexBase is the numeric base used when formatting [big.Int] serial numbers as hex strings.
	hexBase = 16

	// maxDomainLength is the maximum length of a domain name per RFC 1035 / AWS ACM constraints.
	maxDomainLength = 253
	// maxDomainLabelLength is the maximum length of a single DNS label (component between dots).
	maxDomainLabelLength = 63

	// keyUsageDigitalSignature is the AWS string for the X.509 digitalSignature key usage.
	keyUsageDigitalSignature = "DIGITAL_SIGNATURE"
	// extKeyUsageServerAuth is the AWS string for the X.509 serverAuth extended key usage.
	extKeyUsageServerAuth = "TLS_WEB_SERVER_AUTHENTICATION"

	// listCertSortByCreatedAt is the sort-by field name for creation timestamp.
	listCertSortByCreatedAt = "CREATED_AT"
)

// ResourceRecord holds the CNAME record used for DNS certificate validation.
type ResourceRecord struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	Value string `json:"value"`
}

// DomainValidationOption holds the validation details for a single domain.
type DomainValidationOption struct {
	ResourceRecord   *ResourceRecord `json:"resourceRecord,omitempty"`
	DomainName       string          `json:"domainName"`
	ValidationDomain string          `json:"validationDomain"`
	ValidationStatus string          `json:"validationStatus"`
	ValidationMethod string          `json:"validationMethod"`
	// ValidationEmails is populated when ValidationMethod is EMAIL.
	ValidationEmails []string `json:"validationEmails,omitempty"`
}

// Certificate represents an ACM certificate.
type Certificate struct {
	RevokedAt                          *time.Time `json:"revokedAt,omitempty"`
	IssuedAt                           *time.Time `json:"issuedAt,omitempty"`
	ImportedAt                         *time.Time `json:"importedAt,omitempty"`
	CreatedAt                          time.Time  `json:"createdAt"`
	NotBefore                          time.Time  `json:"notBefore"`
	NotAfter                           time.Time  `json:"notAfter"`
	ARN                                string     `json:"arn"`
	DomainName                         string     `json:"domainName"`
	Serial                             string     `json:"serial,omitempty"`
	Subject                            string     `json:"subject,omitempty"`
	Issuer                             string     `json:"issuer,omitempty"`
	KeyAlgorithm                       string     `json:"keyAlgorithm,omitempty"`
	SignatureAlgorithm                 string     `json:"signatureAlgorithm,omitempty"`
	Status                             string     `json:"status"`
	Type                               string     `json:"type"`
	RevocationReason                   string     `json:"revocationReason,omitempty"`
	RenewalEligibility                 string     `json:"renewalEligibility,omitempty"`
	ValidationMethod                   string     `json:"validationMethod,omitempty"`
	CertificateBody                    string     `json:"certificateBody,omitempty"`
	CertificateChain                   string     `json:"certificateChain,omitempty"`
	PrivateKey                         string     `json:"privateKey,omitempty"`
	CertificateTransparencyLoggingPref string     `json:"certTransparencyLoggingPref,omitempty"`
	IdempotencyToken                   string     `json:"idempotencyToken,omitempty"`
	CertificateAuthorityArn            string     `json:"certificateAuthorityArn,omitempty"`
	KeyID                              string     `json:"keyId,omitempty"`
	// FailureReason is set when the certificate enters FAILED status.
	FailureReason           string                   `json:"failureReason,omitempty"`
	SubjectAlternativeNames []string                 `json:"subjectAlternativeNames,omitempty"`
	DomainValidationOptions []DomainValidationOption `json:"domainValidationOptions,omitempty"`
	// RenewalSummary describes the state of the most recent managed renewal attempt.
	// It is set when RenewCertificate is called on an AMAZON_ISSUED certificate.
	RenewalSummary *RenewalSummary `json:"renewalSummary,omitempty"`
	// InUseBy holds the ARNs of AWS resources that use this certificate.
	InUseBy []string `json:"inUseBy,omitempty"`
	// KeyUsage lists the allowed key usages parsed from the X.509 certificate.
	KeyUsage []string `json:"keyUsage,omitempty"`
	// ExtendedKeyUsage lists the extended key usages parsed from the X.509 certificate.
	ExtendedKeyUsage []string `json:"extendedKeyUsage,omitempty"`
}

// AccountConfig holds account-level ACM configuration.
type AccountConfig struct {
	DaysBeforeExpiry int32 `json:"daysBeforeExpiry"`
}

// accountIdempotencyEntry stores the settings associated with a PutAccountConfiguration idempotency token.
type accountIdempotencyEntry struct {
	CreatedAt        time.Time `json:"createdAt"`
	DaysBeforeExpiry int32     `json:"daysBeforeExpiry"`
}

// certIdempotencyEntry stores the ARN associated with a RequestCertificate idempotency token.
type certIdempotencyEntry struct {
	CreatedAt time.Time `json:"createdAt"`
	ARN       string    `json:"arn"`
}

const (
	renewalEligibilityEligible   = "ELIGIBLE"
	renewalEligibilityIneligible = "INELIGIBLE"

	// renewalStatusPendingValidation is the AWS RenewalStatus when a renewal is awaiting validation.
	renewalStatusPendingValidation = "PENDING_VALIDATION"
)

// RenewalSummary describes the state of an ACM managed renewal for a certificate.
type RenewalSummary struct {
	// RenewalStatus is the status of the renewal (e.g. PENDING_VALIDATION, SUCCESS).
	RenewalStatus string `json:"RenewalStatus"`
	// DomainValidationOptions contains per-domain validation details for the renewal.
	DomainValidationOptions []DomainValidationOption `json:"DomainValidationOptions,omitempty"`
}

// InMemoryBackend is the in-memory store for ACM certificates.
type InMemoryBackend struct {
	timers map[string]*time.Timer
	certs  map[string]*Certificate
	// idempotencyMap maps RequestCertificate idempotency tokens to cert info.
	idempotencyMap map[string]certIdempotencyEntry
	// accountIdempotency maps PutAccountConfiguration tokens to their applied settings.
	accountIdempotency map[string]accountIdempotencyEntry
	mu                 *lockmetrics.RWMutex
	accountID          string
	region             string
	accountConfig      AccountConfig
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		certs:              make(map[string]*Certificate),
		timers:             make(map[string]*time.Timer),
		idempotencyMap:     make(map[string]certIdempotencyEntry),
		accountIdempotency: make(map[string]accountIdempotencyEntry),
		accountID:          accountID,
		region:             region,
		mu:                 lockmetrics.New("acm"),
		accountConfig:      AccountConfig{DaysBeforeExpiry: defaultDaysBeforeExpiry},
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// RequestCertificate creates a new certificate for the given domain.
// When validationMethod is "DNS" or "EMAIL" the certificate starts in
// PENDING_VALIDATION and automatically transitions to ISSUED after a short delay.
// idempotencyToken, if non-empty, deduplicates the request — repeated calls with
// the same token return the previously created certificate ARN.
func (b *InMemoryBackend) RequestCertificate(
	domainName, certType, validationMethod, idempotencyToken, keyAlgorithm, caArn, optionsPref string,
	sans []string,
) (*Certificate, error) {
	if err := validateRequestCertInput(domainName, sans); err != nil {
		return nil, err
	}

	certBody, privateKey, certMeta, notBefore, notAfter, err := generateSelfSignedCert(domainName, sans, keyAlgorithm)
	if err != nil {
		return nil, fmt.Errorf("failed to generate certificate: %w", err)
	}

	if keyAlgorithm == "" {
		keyAlgorithm = keyAlgorithmEC
	}

	b.mu.Lock("RequestCertificate")
	defer b.mu.Unlock()

	// Idempotency: return existing cert if same token was already used.
	existing, found, checkErr := b.checkIdempotency(idempotencyToken, domainName, validationMethod, keyAlgorithm, sans)
	if checkErr != nil {
		return nil, checkErr
	} else if found {
		return existing, nil
	}

	id := fmt.Sprintf("%x", time.Now().UnixNano())
	certARN := arn.Build("acm", b.region, b.accountID, "certificate/"+id)

	if certType == "" {
		certType = "AMAZON_ISSUED"
	}

	renewalEligibility := renewalEligibilityEligible
	if certType == certTypeImported {
		renewalEligibility = renewalEligibilityIneligible
	}

	status, dvoList, err := buildInitialDVOList(domainName, sans, validationMethod)
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	var issuedAt *time.Time

	if status == statusIssued {
		issuedAt = &now
	}
	if optionsPref == "" {
		optionsPref = transparencyLoggingEnabled
	}

	cert := &Certificate{
		ARN:                                certARN,
		DomainName:                         domainName,
		Serial:                             certMeta.serial,
		Subject:                            certMeta.subject,
		Issuer:                             certMeta.issuer,
		KeyAlgorithm:                       keyAlgorithm,
		SignatureAlgorithm:                 certMeta.signatureAlgorithm,
		Status:                             status,
		Type:                               certType,
		RenewalEligibility:                 renewalEligibility,
		ValidationMethod:                   validationMethod,
		IdempotencyToken:                   idempotencyToken,
		SubjectAlternativeNames:            sans,
		DomainValidationOptions:            dvoList,
		CertificateBody:                    certBody,
		PrivateKey:                         privateKey,
		CreatedAt:                          now,
		IssuedAt:                           issuedAt,
		NotBefore:                          notBefore,
		NotAfter:                           notAfter,
		KeyUsage:                           []string{keyUsageDigitalSignature},
		ExtendedKeyUsage:                   []string{extKeyUsageServerAuth},
		CertificateTransparencyLoggingPref: optionsPref,
		CertificateAuthorityArn:            caArn,
	}
	b.certs[certARN] = cert

	if idempotencyToken != "" {
		b.idempotencyMap[idempotencyToken] = certIdempotencyEntry{
			ARN:       certARN,
			CreatedAt: now,
		}
	}

	if status == statusPendingValidation {
		t := time.AfterFunc(autoValidateDelayMS*time.Millisecond, func() { b.autoValidate(certARN) })
		b.timers[certARN] = t
	}

	cp := copyCert(cert)

	return &cp, nil
}

func (b *InMemoryBackend) checkIdempotency(
	idempotencyToken, domainName, validationMethod, keyAlgorithm string,
	sans []string,
) (*Certificate, bool, error) {
	if idempotencyToken == "" {
		return nil, false, nil
	}

	entry, ok := b.idempotencyMap[idempotencyToken]
	if !ok {
		return nil, false, nil
	}

	c, exists := b.certs[entry.ARN]
	if !exists {
		return nil, false, nil
	}

	if c.DomainName != domainName || c.ValidationMethod != validationMethod ||
		c.KeyAlgorithm != keyAlgorithm ||
		!slices.Equal(c.SubjectAlternativeNames, sans) {
		return nil, false, fmt.Errorf(
			"%w: idempotency token already used with different parameters",
			ErrInvalidParameter,
		)
	}

	cp := copyCert(c)

	return &cp, true, nil
}

// validateRequestCertInput validates the DomainName and all SANs for a RequestCertificate call.
func validateRequestCertInput(domainName string, sans []string) error {
	if domainName == "" {
		return fmt.Errorf("%w: DomainName is required", ErrInvalidParameter)
	}

	const maxDomainsPerCertificate = 10
	if len(sans)+1 > maxDomainsPerCertificate {
		return fmt.Errorf(
			"%w: maximum of 10 domain names (1 primary + 9 SANs) allowed per certificate",
			ErrInvalidParameter,
		)
	}

	if err := validateDomainName(domainName); err != nil {
		return err
	}

	for _, san := range sans {
		if err := validateDomainName(san); err != nil {
			return fmt.Errorf("%w: invalid SAN %q: %w", ErrInvalidParameter, san, err)
		}
	}

	return nil
}

// validateDomainName checks that the given domain name satisfies AWS ACM constraints.
// AWS rejects domain names longer than 253 characters, empty labels, labels exceeding 63
// characters, and labels that are purely numeric (which would be IP addresses).
func validateDomainName(name string) error {
	if len(name) > maxDomainLength {
		return fmt.Errorf("%w: domain %q exceeds maximum length of %d", ErrInvalidParameter, name, maxDomainLength)
	}

	// Strip leading wildcard component (*.example.com → example.com for label checks)
	checkName := strings.TrimPrefix(name, "*.")

	for label := range strings.SplitSeq(checkName, ".") {
		if label == "" {
			return fmt.Errorf("%w: domain %q contains an empty label", ErrInvalidParameter, name)
		}

		if len(label) > maxDomainLabelLength {
			return fmt.Errorf("%w: domain label %q in %q exceeds %d characters",
				ErrInvalidParameter, label, name, maxDomainLabelLength)
		}
	}

	return nil
}

// buildInitialDVOList constructs the initial DomainValidationOptions list and determines
// the certificate's initial status based on the validation method.
func buildInitialDVOList(
	domainName string,
	sans []string,
	validationMethod string,
) (string, []DomainValidationOption, error) {
	allDomains := append([]string{domainName}, sans...)
	status := statusIssued

	var (
		dvoList []DomainValidationOption
		err     error
	)

	switch validationMethod {
	case validationMethodDNS, validationMethodEMAIL:
		status = statusPendingValidation
		dvoList, err = buildDomainValidationOptions(allDomains, validationMethod)
	default:
		dvoList, err = buildDomainValidationOptions(allDomains, validationMethodDNS)
	}

	if err != nil {
		return "", nil, err
	}

	if status == statusIssued {
		for i := range dvoList {
			dvoList[i].ValidationStatus = validationStatusSuccess
		}
	}

	return status, dvoList, nil
}

// copyCert returns a deep copy of a Certificate, ensuring all pointer fields
// and the DomainValidationOptions slice are not shared with the original.
func copyCert(c *Certificate) Certificate {
	cp := *c

	if c.RevokedAt != nil {
		t := *c.RevokedAt
		cp.RevokedAt = &t
	}

	if c.IssuedAt != nil {
		t := *c.IssuedAt
		cp.IssuedAt = &t
	}

	if c.ImportedAt != nil {
		t := *c.ImportedAt
		cp.ImportedAt = &t
	}

	if len(c.SubjectAlternativeNames) > 0 {
		cp.SubjectAlternativeNames = append([]string(nil), c.SubjectAlternativeNames...)
	}

	if len(c.InUseBy) > 0 {
		cp.InUseBy = append([]string(nil), c.InUseBy...)
	}

	if len(c.KeyUsage) > 0 {
		cp.KeyUsage = append([]string(nil), c.KeyUsage...)
	}

	if len(c.ExtendedKeyUsage) > 0 {
		cp.ExtendedKeyUsage = append([]string(nil), c.ExtendedKeyUsage...)
	}

	if len(c.DomainValidationOptions) > 0 {
		cp.DomainValidationOptions = make([]DomainValidationOption, len(c.DomainValidationOptions))
		copy(cp.DomainValidationOptions, c.DomainValidationOptions)

		for i, dvo := range c.DomainValidationOptions {
			if dvo.ResourceRecord != nil {
				rr := *dvo.ResourceRecord
				cp.DomainValidationOptions[i].ResourceRecord = &rr
			}

			if len(dvo.ValidationEmails) > 0 {
				cp.DomainValidationOptions[i].ValidationEmails = append([]string(nil), dvo.ValidationEmails...)
			}
		}
	}

	if c.RenewalSummary != nil {
		rs := *c.RenewalSummary
		if len(c.RenewalSummary.DomainValidationOptions) > 0 {
			rs.DomainValidationOptions = append(
				[]DomainValidationOption(nil),
				c.RenewalSummary.DomainValidationOptions...,
			)
		}

		cp.RenewalSummary = &rs
	}

	return cp
}

// autoValidate transitions a certificate from PENDING_VALIDATION to ISSUED after a
// short delay, simulating the DNS/email validation workflow.
func (b *InMemoryBackend) autoValidate(certARN string) {
	b.mu.Lock("autoValidate")
	defer b.mu.Unlock()

	delete(b.timers, certARN)

	c, ok := b.certs[certARN]
	if !ok || c.Status != statusPendingValidation {
		return
	}

	now := time.Now().UTC()
	c.Status = statusIssued
	c.IssuedAt = &now

	for i := range c.DomainValidationOptions {
		c.DomainValidationOptions[i].ValidationStatus = validationStatusSuccess
	}
}

// autoValidateRenewal transitions a certificate's RenewalSummary from PENDING_VALIDATION to SUCCESS after a
// short delay, simulating the DNS/email validation workflow for managed renewals.
func (b *InMemoryBackend) autoValidateRenewal(certARN string) {
	b.mu.Lock("autoValidateRenewal")
	defer b.mu.Unlock()

	delete(b.timers, certARN)

	c, ok := b.certs[certARN]
	if !ok || c.RenewalSummary == nil || c.RenewalSummary.RenewalStatus != renewalStatusPendingValidation {
		return
	}

	c.RenewalSummary.RenewalStatus = validationStatusSuccess
	for i := range c.RenewalSummary.DomainValidationOptions {
		c.RenewalSummary.DomainValidationOptions[i].ValidationStatus = validationStatusSuccess
	}
}

// ImportCertificate stores a PEM-encoded certificate, private key, and optional
// certificate chain, returning the ARN of the newly created or updated entry.
// When certARNToUpdate is non-empty, the existing certificate is updated in-place
// (re-import), matching AWS behavior where CertificateArn may be passed to replace
// an existing imported certificate.
func (b *InMemoryBackend) ImportCertificate(
	certBody, privateKey, certChain, certARNToUpdate string,
) (*Certificate, error) {
	if certBody == "" {
		return nil, fmt.Errorf("%w: Certificate is required", ErrInvalidParameter)
	}

	if privateKey == "" {
		return nil, fmt.Errorf("%w: PrivateKey is required", ErrInvalidParameter)
	}

	domainName, meta, notBefore, notAfter, err := extractCertMetadataFull(certBody)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid certificate body: %w", ErrInvalidParameter, err)
	}

	now := time.Now().UTC()

	b.mu.Lock("ImportCertificate")
	defer b.mu.Unlock()

	// Re-import: update existing certificate in-place.
	if certARNToUpdate != "" {
		existing, ok := b.certs[certARNToUpdate]
		if !ok {
			return nil, fmt.Errorf("%w: certificate %s not found", ErrCertNotFound, certARNToUpdate)
		}

		existing.CertificateBody = certBody
		existing.CertificateChain = certChain
		existing.PrivateKey = privateKey
		existing.DomainName = domainName
		existing.NotBefore = notBefore
		existing.NotAfter = notAfter
		existing.Serial = meta.serial
		existing.Subject = meta.subject
		existing.Issuer = meta.issuer
		existing.SignatureAlgorithm = meta.signatureAlgorithm
		existing.ImportedAt = &now
		existing.Status = statusIssued
		existing.KeyUsage = meta.keyUsage
		existing.ExtendedKeyUsage = meta.extKeyUsage

		cp := copyCert(existing)

		return &cp, nil
	}

	id := fmt.Sprintf("%x", time.Now().UnixNano())
	certARN := arn.Build("acm", b.region, b.accountID, "certificate/"+id)

	cert := &Certificate{
		ARN:                                certARN,
		DomainName:                         domainName,
		Serial:                             meta.serial,
		Subject:                            meta.subject,
		Issuer:                             meta.issuer,
		KeyAlgorithm:                       keyAlgorithmEC,
		SignatureAlgorithm:                 meta.signatureAlgorithm,
		Status:                             statusIssued,
		Type:                               certTypeImported,
		RenewalEligibility:                 renewalEligibilityIneligible,
		CertificateBody:                    certBody,
		CertificateChain:                   certChain,
		PrivateKey:                         privateKey,
		CreatedAt:                          now,
		ImportedAt:                         &now,
		NotBefore:                          notBefore,
		NotAfter:                           notAfter,
		KeyUsage:                           meta.keyUsage,
		ExtendedKeyUsage:                   meta.extKeyUsage,
		CertificateTransparencyLoggingPref: transparencyLoggingEnabled,
	}
	b.certs[certARN] = cert

	cp := copyCert(cert)

	return &cp, nil
}

// RenewCertificate regenerates the certificate material for an AMAZON_ISSUED certificate,
// extending its validity by one year. Returns ErrNotEligible for IMPORTED certificates,
// as AWS ACM does not support renewing imported certificates.
func (b *InMemoryBackend) RenewCertificate(certARN string) error {
	b.mu.Lock("RenewCertificate")
	defer b.mu.Unlock()

	c, exists := b.certs[certARN]
	if !exists {
		return fmt.Errorf("%w: certificate %s not found", ErrCertNotFound, certARN)
	}

	if c.Type == certTypeImported {
		return fmt.Errorf("%w: only AMAZON_ISSUED certificates can be renewed", ErrNotEligible)
	}

	if c.CertificateAuthorityArn != "" {
		return fmt.Errorf("%w: PRIVATE certificates cannot be renewed through this API", ErrNotEligible)
	}

	domainName := c.DomainName
	sans := c.SubjectAlternativeNames
	validationMethod := c.ValidationMethod

	certBody, privateKey, meta, notBefore, notAfter, err := generateSelfSignedCert(domainName, sans, c.KeyAlgorithm)
	if err != nil {
		return fmt.Errorf("failed to generate self-signed certificate: %w", err)
	}

	status, dvoList, err := buildInitialDVOList(domainName, sans, validationMethod)
	if err != nil {
		return fmt.Errorf("failed to build domain validation options: %w", err)
	}

	c.CertificateBody = certBody
	c.PrivateKey = privateKey
	c.Serial = meta.serial
	c.Subject = meta.subject
	c.Issuer = meta.issuer
	c.SignatureAlgorithm = meta.signatureAlgorithm
	c.NotBefore = notBefore
	c.NotAfter = notAfter

	// Mark the certificate as eligible for renewal and set the renewal summary.
	c.RenewalEligibility = renewalEligibilityEligible
	c.RenewalSummary = &RenewalSummary{
		RenewalStatus:           status,
		DomainValidationOptions: dvoList,
	}

	if status == statusPendingValidation {
		t := time.AfterFunc(autoValidateDelayMS*time.Millisecond, func() { b.autoValidateRenewal(certARN) })
		// We can share the timer map, because normal validation is done
		// if a renewal is happening (a cert must be issued to be renewed).
		// Wait, if there's an existing timer, stop it first.
		if oldT, ok := b.timers[certARN]; ok {
			oldT.Stop()
		}
		b.timers[certARN] = t
	}

	return nil
}

// fakeCertChain is a fake PEM certificate chain (intermediate + root) returned by
// ExportCertificate when the stored certificate has no associated chain.
// This simulates the AWS ACM behavior of always returning a chain for exported certs.
const fakeCertChain = "-----BEGIN CERTIFICATE-----\n" +
	"MIIBpDCCAUqgAwIBAgIUFakeIntermediateCA0001AgAgAAgICAgICAgICIwCgYIKoZIzj0E\n" +
	"AwIwETEPMA0GA1UEAxMGZmFrZUNBMB4XDTIwMDEwMTAwMDAwMFoXDTMwMDEwMTAw\n" +
	"MDAwMFowETEPMA0GA1UEAxMGZmFrZUNBMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcD\n" +
	"QgAEfakeIntermediatePublicKeyDataHereForTestingPurposesOnlyNotRealCA\n" +
	"o0IwQDAdBgNVHQ4EFgQUFakeIntermediateCAKeyId001234MA8GA1UdEwEB/wQFMAMB\n" +
	"Af8wDgYDVR0PAQH/BAQDAgGGMAoGCCqGSM49BAMCA0gAMEUCIFakeIntermediateSig\n" +
	"nature001ForTestPurposesAgICAgICAgICAgICAgAgICAgICAgICAgICAgIA\n" +
	"-----END CERTIFICATE-----\n" +
	"-----BEGIN CERTIFICATE-----\n" +
	"MIIBmDCCAT6gAwIBAgIUFakeRootCA0001AgAgAAgICAgICAgICIwCgYIKoZIzj0E\n" +
	"AwIwDzENMAsGA1UEAxMEcm9vdDAeFw0yMDAxMDEwMDAwMDBaFw0zMDAxMDEwMDAw\n" +
	"MDBaMA8xDTALBgNVBAMTBHJvb3QwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAARf\n" +
	"akeRootPublicKeyDataHereForTestingPurposesOnlyNotARealRootCertificate\n" +
	"o0IwQDAdBgNVHQ4EFgQUFakeRootCAKeyId00123456789012345678901234567890\n" +
	"MA8GA1UdEwEB/wQFMAMBAf8wDgYDVR0PAQH/BAQDAgGGMAoGCCqGSM49BAMCA0AA\n" +
	"MEYCIQCFakeRootSignature001ForTestingPurposesNotARealSignatureAtAllXX\n" +
	"AiEAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICA\n" +
	"-----END CERTIFICATE-----\n"

// ExportCertificate returns the PEM certificate body, chain, and private key for
// an IMPORTED or PRIVATE certificate. Returns ErrNotEligible for AMAZON_ISSUED certificates.
// When the stored certificate has no associated chain, a fake chain (intermediate + root)
// is returned in PEM format to simulate AWS ACM behaviour.
// If passphrase is non-nil and non-empty, the private key is returned encrypted using AES-256.
func (b *InMemoryBackend) ExportCertificate(certARN string, passphrase []byte) (*Certificate, error) {
	b.mu.RLock("ExportCertificate")
	defer b.mu.RUnlock()

	cert, ok := b.certs[certARN]
	if !ok {
		return nil, fmt.Errorf("%w: certificate %s not found", ErrCertNotFound, certARN)
	}

	if cert.Type != certTypeImported && cert.Type != "PRIVATE" {
		return nil, fmt.Errorf("%w: only IMPORTED or PRIVATE certificates can be exported", ErrNotEligible)
	}

	cp := copyCert(cert)

	// Always return a certificate chain; use a fake chain when none was supplied.
	if cp.CertificateChain == "" {
		cp.CertificateChain = fakeCertChain
	}

	if len(passphrase) > 0 {
		encKey, encErr := encryptPrivateKeyPEM(cp.PrivateKey, passphrase)
		if encErr != nil {
			return nil, fmt.Errorf("export: %w", encErr)
		}

		cp.PrivateKey = encKey
	}

	return &cp, nil
}

// GetCertificate returns the PEM certificate body and chain for any certificate.
func (b *InMemoryBackend) GetCertificate(certARN string) (string, string, error) {
	b.mu.RLock("GetCertificate")
	defer b.mu.RUnlock()

	cert, ok := b.certs[certARN]
	if !ok {
		return "", "", fmt.Errorf("%w: certificate %s not found", ErrCertNotFound, certARN)
	}

	if cert.Status == statusPendingValidation || cert.Status == statusValidationTimedOut ||
		cert.Status == statusFailed {
		return "", "", fmt.Errorf("%w: certificate %s is in state %s", ErrRequestInProgress, certARN, cert.Status)
	}

	return cert.CertificateBody, cert.CertificateChain, nil
}

// DescribeCertificate returns the certificate with the given ARN.
func (b *InMemoryBackend) DescribeCertificate(arn string) (*Certificate, error) {
	b.mu.RLock("DescribeCertificate")
	defer b.mu.RUnlock()

	cert, exists := b.certs[arn]
	if !exists {
		return nil, fmt.Errorf("%w: certificate %s not found", ErrCertNotFound, arn)
	}

	cp := copyCert(cert)

	return &cp, nil
}

// ListCertificatesParams holds all filter and sorting options for ListCertificates.
type ListCertificatesParams struct {
	NextToken        string
	SortBy           string
	SortOrder        string
	StatusFilter     []string
	KeyTypes         []string
	KeyUsage         []string
	ExtendedKeyUsage []string
	MaxItems         int
}

// listCertFilters holds compiled filter sets for ListCertificates.
type listCertFilters struct {
	statusSet      map[string]struct{}
	keyTypeSet     map[string]struct{}
	keyUsageSet    map[string]struct{}
	extKeyUsageSet map[string]struct{}
}

// buildListCertFilters compiles the filter sets from ListCertificatesParams.
func buildListCertFilters(p ListCertificatesParams) listCertFilters {
	f := listCertFilters{
		statusSet:      make(map[string]struct{}, len(p.StatusFilter)),
		keyTypeSet:     make(map[string]struct{}, len(p.KeyTypes)),
		keyUsageSet:    make(map[string]struct{}, len(p.KeyUsage)),
		extKeyUsageSet: make(map[string]struct{}, len(p.ExtendedKeyUsage)),
	}

	for _, s := range p.StatusFilter {
		f.statusSet[s] = struct{}{}
	}

	for _, k := range p.KeyTypes {
		f.keyTypeSet[k] = struct{}{}
	}

	for _, ku := range p.KeyUsage {
		f.keyUsageSet[ku] = struct{}{}
	}

	for _, eku := range p.ExtendedKeyUsage {
		f.extKeyUsageSet[eku] = struct{}{}
	}

	return f
}

// matches returns true if the certificate satisfies all filters.
func (f listCertFilters) matches(c *Certificate) bool {
	if len(f.statusSet) > 0 {
		if _, ok := f.statusSet[c.Status]; !ok {
			return false
		}
	}

	if len(f.keyTypeSet) > 0 {
		if _, ok := f.keyTypeSet[c.KeyAlgorithm]; !ok {
			return false
		}
	}

	if len(f.keyUsageSet) > 0 && !matchesAny(c.KeyUsage, f.keyUsageSet) {
		return false
	}

	if len(f.extKeyUsageSet) > 0 && !matchesAny(c.ExtendedKeyUsage, f.extKeyUsageSet) {
		return false
	}

	return true
}

// ListCertificates returns a paginated list of certificates, with optional
// filtering and sorting.
func (b *InMemoryBackend) ListCertificates(p ListCertificatesParams) (page.Page[Certificate], error) {
	if err := page.ValidateToken(p.NextToken); err != nil {
		return page.Page[Certificate]{}, fmt.Errorf("%w: invalid NextToken", ErrInvalidParameter)
	}

	b.mu.RLock("ListCertificates")
	defer b.mu.RUnlock()

	filters := buildListCertFilters(p)
	certs := make([]Certificate, 0, len(b.certs))

	for _, c := range b.certs {
		if filters.matches(c) {
			certs = append(certs, copyCert(c))
		}
	}

	descending := strings.EqualFold(p.SortOrder, "DESCENDING")

	switch strings.ToUpper(p.SortBy) {
	case listCertSortByCreatedAt:
		sort.Slice(certs, func(i, j int) bool {
			if descending {
				return certs[i].CreatedAt.After(certs[j].CreatedAt)
			}

			return certs[i].CreatedAt.Before(certs[j].CreatedAt)
		})
	default:
		// Default: sort by ARN for stable, deterministic ordering.
		sort.Slice(certs, func(i, j int) bool {
			if descending {
				return certs[i].ARN > certs[j].ARN
			}

			return certs[i].ARN < certs[j].ARN
		})
	}

	return page.New(certs, p.NextToken, p.MaxItems, acmDefaultMaxItems), nil
}

const acmDefaultMaxItems = 100

// matchesAny returns true if any element of values is in the set.
func matchesAny(values []string, set map[string]struct{}) bool {
	for _, v := range values {
		if _, ok := set[v]; ok {
			return true
		}
	}

	return false
}

// CertExists reports whether a certificate with the given ARN exists in the backend.
// This is used by the handler to validate tag operations.
func (b *InMemoryBackend) CertExists(certARN string) bool {
	b.mu.RLock("CertExists")
	defer b.mu.RUnlock()

	_, ok := b.certs[certARN]

	return ok
}

// AddInUseBy records that a resource ARN is using the certificate. It is a no-op
// if the certificate does not exist or the ARN is already present.
func (b *InMemoryBackend) AddInUseBy(certARN, resourceARN string) {
	b.mu.Lock("AddInUseBy")
	defer b.mu.Unlock()

	cert, ok := b.certs[certARN]
	if !ok {
		return
	}

	if slices.Contains(cert.InUseBy, resourceARN) {
		return
	}

	cert.InUseBy = append(cert.InUseBy, resourceARN)
}

// RemoveInUseBy removes a resource ARN from the certificate's InUseBy list. It is a no-op
// if the certificate does not exist or the ARN is not present.
func (b *InMemoryBackend) RemoveInUseBy(certARN, resourceARN string) {
	b.mu.Lock("RemoveInUseBy")
	defer b.mu.Unlock()

	cert, ok := b.certs[certARN]
	if !ok {
		return
	}

	filtered := cert.InUseBy[:0]

	for _, existing := range cert.InUseBy {
		if existing != resourceARN {
			filtered = append(filtered, existing)
		}
	}

	cert.InUseBy = filtered
}

// DeleteCertificate removes the certificate with the given ARN.
func (b *InMemoryBackend) DeleteCertificate(certARN string) error {
	b.mu.Lock("DeleteCertificate")
	defer b.mu.Unlock()

	cert, exists := b.certs[certARN]
	if !exists {
		return fmt.Errorf("%w: certificate %s not found", ErrCertNotFound, certARN)
	}

	if len(cert.InUseBy) > 0 {
		return fmt.Errorf("%w: certificate %s is in use", ErrResourceInUse, certARN)
	}

	if t, ok := b.timers[certARN]; ok {
		t.Stop()
		delete(b.timers, certARN)
	}

	delete(b.certs, certARN)

	// Drop any idempotency-token entries that pointed at this cert so the
	// map cannot grow unbounded for long-running backends.
	for tok, entry := range b.idempotencyMap {
		if entry.ARN == certARN {
			delete(b.idempotencyMap, tok)
		}
	}

	return nil
}

// buildDomainValidationOptions creates DomainValidationOption entries with
// synthetic CNAME records for DNS validation, or synthetic email addresses for EMAIL validation.
func buildDomainValidationOptions(domains []string, validationMethod string) ([]DomainValidationOption, error) {
	opts := make([]DomainValidationOption, 0, len(domains))
	seen := make(map[string]bool, len(domains))

	for _, d := range domains {
		if seen[d] {
			continue
		}
		seen[d] = true

		status := validationStatusSuccess
		if validationMethod == validationMethodDNS || validationMethod == validationMethodEMAIL {
			status = statusPendingValidation
		}

		opt := DomainValidationOption{
			DomainName:       d,
			ValidationDomain: d,
			ValidationStatus: status,
			ValidationMethod: validationMethod,
		}

		switch validationMethod {
		case validationMethodDNS:
			nameToken, err := randHex(validationTokenLen)
			if err != nil {
				return nil, err
			}

			valueToken, err := randHex(validationTokenLen)
			if err != nil {
				return nil, err
			}

			opt.ResourceRecord = &ResourceRecord{
				Name:  "_" + nameToken + "." + d + ".",
				Type:  "CNAME",
				Value: "_" + valueToken + ".acm-validations.aws.",
			}

		case validationMethodEMAIL:
			// AWS sends validation emails to well-known addresses at the domain root.
			rootDomain := d
			if strings.HasPrefix(d, "*.") {
				rootDomain = d[2:]
			}

			opt.ValidationEmails = []string{
				"admin@" + rootDomain,
				"administrator@" + rootDomain,
				"hostmaster@" + rootDomain,
				"postmaster@" + rootDomain,
				"webmaster@" + rootDomain,
			}
		}

		opts = append(opts, opt)
	}

	return opts, nil
}

// randHex returns a random lowercase hex string of length n characters.
func randHex(n int) (string, error) {
	b := make([]byte, (n+randByteDivisor-1)/randByteDivisor)
	if _, err := cryptorand.Read(b); err != nil {
		return "", fmt.Errorf("crypto/rand read failed: %w", err)
	}

	return hex.EncodeToString(b)[:n], nil
}

// certMetadata holds extracted metadata from a parsed X.509 certificate.
type certMetadata struct {
	serial             string
	subject            string
	issuer             string
	signatureAlgorithm string
	keyUsage           []string
	extKeyUsage        []string
}

// generateSelfSignedCert generates a self-signed ECDSA P-256 certificate for
// the given domain (and optional SANs) and returns PEM-encoded certificate,
// private key, extracted metadata, and the certificate's NotBefore/NotAfter validity times.
func generateSelfSignedCert(
	domainName string,
	sans []string,
	keyAlgorithm string,
) (string, string, certMetadata, time.Time, time.Time, error) {
	priv, pub, sigAlgo, err := generateKey(keyAlgorithm)
	if err != nil {
		return "", "", certMetadata{}, time.Time{}, time.Time{}, fmt.Errorf("generate key: %w", err)
	}

	serialBytes := make([]byte, 16) //nolint:mnd // 128-bit random serial
	if _, err = cryptorand.Read(serialBytes); err != nil {
		return "", "", certMetadata{}, time.Time{}, time.Time{}, fmt.Errorf("generate serial: %w", err)
	}

	serial := new(big.Int).SetBytes(serialBytes)

	dnsNames := append([]string{domainName}, sans...)

	notBefore := time.Now().UTC().Truncate(time.Second)
	notAfter := notBefore.Add(certValidityDuration)

	subjectName := pkix.Name{
		Organization:       []string{"Amazon"},
		OrganizationalUnit: []string{"Server CA 1B"},
		Country:            []string{"US"},
		CommonName:         domainName,
	}

	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      subjectName,
		DNSNames:     dnsNames,
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	certDER, err := x509.CreateCertificate(cryptorand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		return "", "", certMetadata{}, time.Time{}, time.Time{}, fmt.Errorf("create certificate: %w", err)
	}

	certPEM := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}))

	keyDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return "", "", certMetadata{}, time.Time{}, time.Time{}, fmt.Errorf("marshal key: %w", err)
	}

	var keyType string
	switch priv.(type) {
	case *rsa.PrivateKey:
		keyType = "RSA PRIVATE KEY"
	case *ecdsa.PrivateKey:
		keyType = "EC PRIVATE KEY"
	}

	keyPEM := string(pem.EncodeToMemory(&pem.Block{Type: keyType, Bytes: keyDER}))

	meta := certMetadata{
		serial:             serial.Text(hexBase),
		subject:            subjectName.String(),
		issuer:             subjectName.String(), // self-signed: issuer == subject
		signatureAlgorithm: sigAlgo,
	}

	return certPEM, keyPEM, meta, notBefore, notAfter, nil
}

// extractCertMetadataFull parses a PEM-encoded certificate and returns the primary
// domain name (first SAN or CN), extracted metadata, NotBefore, and NotAfter.
func extractCertMetadataFull(certPEM string) (string, certMetadata, time.Time, time.Time, error) {
	block, _ := pem.Decode([]byte(certPEM))
	if block == nil {
		return "", certMetadata{}, time.Time{}, time.Time{}, errInvalidPEM
	}

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return "", certMetadata{}, time.Time{}, time.Time{}, fmt.Errorf("parse certificate: %w", err)
	}

	domainName := cert.Subject.CommonName
	if len(cert.DNSNames) > 0 {
		domainName = cert.DNSNames[0]
	}

	meta := certMetadata{
		serial:             cert.SerialNumber.Text(hexBase),
		subject:            cert.Subject.String(),
		issuer:             cert.Issuer.String(),
		signatureAlgorithm: cert.SignatureAlgorithm.String(),
		keyUsage:           x509KeyUsageToAWS(cert.KeyUsage),
		extKeyUsage:        x509ExtKeyUsageToAWS(cert.ExtKeyUsage),
	}

	return domainName, meta, cert.NotBefore.UTC(), cert.NotAfter.UTC(), nil
}

// x509KeyUsageToAWS converts x509.KeyUsage bitmask to a slice of AWS key usage strings.
func x509KeyUsageToAWS(ku x509.KeyUsage) []string {
	mapping := []struct {
		name string
		bit  x509.KeyUsage
	}{
		{"DIGITAL_SIGNATURE", x509.KeyUsageDigitalSignature},
		{"NON_REPUDIATION", x509.KeyUsageContentCommitment},
		{"KEY_ENCIPHERMENT", x509.KeyUsageKeyEncipherment},
		{"DATA_ENCIPHERMENT", x509.KeyUsageDataEncipherment},
		{"KEY_AGREEMENT", x509.KeyUsageKeyAgreement},
		{"CERTIFICATE_SIGNING", x509.KeyUsageCertSign},
		{"CRL_SIGNING", x509.KeyUsageCRLSign},
		{"ENCIPHER_ONLY", x509.KeyUsageEncipherOnly},
		{"DECIPHER_ONLY", x509.KeyUsageDecipherOnly},
	}

	var result []string
	for _, m := range mapping {
		if ku&m.bit != 0 {
			result = append(result, m.name)
		}
	}

	return result
}

// x509ExtKeyUsageToAWS converts a slice of x509.ExtKeyUsage to AWS extended key usage strings.
func x509ExtKeyUsageToAWS(ekus []x509.ExtKeyUsage) []string {
	var result []string

	for _, eku := range ekus {
		switch eku {
		case x509.ExtKeyUsageAny:
			result = append(result, "ANY")
		case x509.ExtKeyUsageServerAuth:
			result = append(result, "TLS_WEB_SERVER_AUTHENTICATION")
		case x509.ExtKeyUsageClientAuth:
			result = append(result, "TLS_WEB_CLIENT_AUTHENTICATION")
		case x509.ExtKeyUsageCodeSigning:
			result = append(result, "CODE_SIGNING")
		case x509.ExtKeyUsageEmailProtection:
			result = append(result, "EMAIL_PROTECTION")
		case x509.ExtKeyUsageIPSECEndSystem:
			result = append(result, "IPSEC_END_SYSTEM")
		case x509.ExtKeyUsageIPSECTunnel:
			result = append(result, "IPSEC_TUNNEL")
		case x509.ExtKeyUsageIPSECUser:
			result = append(result, "IPSEC_USER")
		case x509.ExtKeyUsageTimeStamping:
			result = append(result, "TIME_STAMPING")
		case x509.ExtKeyUsageOCSPSigning:
			result = append(result, "OCSP_SIGNING")
		case x509.ExtKeyUsageMicrosoftServerGatedCrypto,
			x509.ExtKeyUsageNetscapeServerGatedCrypto,
			x509.ExtKeyUsageMicrosoftCommercialCodeSigning,
			x509.ExtKeyUsageMicrosoftKernelCodeSigning:
			result = append(result, "CUSTOM")
		}
	}

	return result
}

// encryptPrivateKeyPEM encrypts a PEM-encoded private key with the given passphrase,
// returning a PEM block with type "ENCRYPTED PRIVATE KEY".
func encryptPrivateKeyPEM(privateKeyPEM string, passphrase []byte) (string, error) {
	block, _ := pem.Decode([]byte(privateKeyPEM))
	if block == nil {
		return "", errInvalidPEM
	}

	//nolint:staticcheck // EncryptPEMBlock is deprecated but functional for ACM passphrase simulation
	encBlock, err := x509.EncryptPEMBlock(
		cryptorand.Reader,
		"ENCRYPTED PRIVATE KEY",
		block.Bytes,
		passphrase,
		x509.PEMCipherAES256,
	)
	if err != nil {
		return "", fmt.Errorf("encrypt private key: %w", err)
	}

	return string(pem.EncodeToMemory(encBlock)), nil
}

// Reset clears all certificate state and stops any pending auto-validate timers.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, t := range b.timers {
		t.Stop()
	}

	b.certs = make(map[string]*Certificate)
	b.timers = make(map[string]*time.Timer)
	b.idempotencyMap = make(map[string]certIdempotencyEntry)
	b.accountIdempotency = make(map[string]accountIdempotencyEntry)
	b.accountConfig = AccountConfig{DaysBeforeExpiry: defaultDaysBeforeExpiry}
}

// GetAccountConfiguration returns the account-level ACM configuration.
func (b *InMemoryBackend) GetAccountConfiguration() AccountConfig {
	b.mu.RLock("GetAccountConfiguration")
	defer b.mu.RUnlock()

	return b.accountConfig
}

// PutAccountConfiguration stores the account-level ACM configuration.
// idempotencyToken must be non-empty; repeated calls with the same token are
// silently accepted only when the configuration is identical (AWS behavior).
// A conflicting call with the same token but different settings returns ErrConflict.
func (b *InMemoryBackend) PutAccountConfiguration(idempotencyToken string, daysBeforeExpiry *int32) error {
	if idempotencyToken == "" {
		return fmt.Errorf("%w: IdempotencyToken is required", ErrInvalidParameter)
	}

	wantDays := defaultDaysBeforeExpiry
	if daysBeforeExpiry != nil {
		if *daysBeforeExpiry < 0 {
			return fmt.Errorf("%w: DaysBeforeExpiry must be non-negative", ErrInvalidParameter)
		}

		wantDays = *daysBeforeExpiry
	}

	b.mu.Lock("PutAccountConfiguration")
	defer b.mu.Unlock()

	if prev, seen := b.accountIdempotency[idempotencyToken]; seen {
		if prev.DaysBeforeExpiry != wantDays {
			return fmt.Errorf(
				"%w: IdempotencyToken %q was already used with different settings",
				ErrConflict, idempotencyToken,
			)
		}
		// Same token + same settings → idempotent success.
		return nil
	}

	b.accountIdempotency[idempotencyToken] = accountIdempotencyEntry{
		DaysBeforeExpiry: wantDays,
		CreatedAt:        time.Now().UTC(),
	}
	b.accountConfig.DaysBeforeExpiry = wantDays

	return nil
}

// ResendValidationEmail re-triggers the EMAIL validation flow for a certificate
// that is still in PENDING_VALIDATION status with EMAIL validation method.
func (b *InMemoryBackend) ResendValidationEmail(certARN, domain, validationDomain string) error {
	if certARN == "" {
		return fmt.Errorf("%w: CertificateArn is required", ErrInvalidParameter)
	}

	if domain == "" {
		return fmt.Errorf("%w: Domain is required", ErrInvalidParameter)
	}

	if validationDomain == "" {
		return fmt.Errorf("%w: ValidationDomain is required", ErrInvalidParameter)
	}

	b.mu.Lock("ResendValidationEmail")
	defer b.mu.Unlock()

	cert, ok := b.certs[certARN]
	if !ok {
		return fmt.Errorf("%w: certificate %s not found", ErrCertNotFound, certARN)
	}

	if cert.Status != statusPendingValidation {
		return fmt.Errorf("%w: certificate is not in PENDING_VALIDATION status", ErrInvalidParameter)
	}

	if cert.ValidationMethod != validationMethodEMAIL {
		return fmt.Errorf("%w: certificate was not requested with EMAIL validation", ErrInvalidParameter)
	}

	found := false
	for i, dvo := range cert.DomainValidationOptions {
		if dvo.DomainName == domain {
			cert.DomainValidationOptions[i].ValidationStatus = statusPendingValidation
			found = true
		}
	}

	if !found {
		return fmt.Errorf("%w: domain %s not found in certificate", ErrInvalidParameter, domain)
	}

	// Reset the auto-validate timer to simulate email resend triggering re-validation.
	if t, exists := b.timers[certARN]; exists {
		t.Stop()
		delete(b.timers, certARN)
	}

	t := time.AfterFunc(autoValidateDelayMS*time.Millisecond, func() { b.autoValidate(certARN) })
	b.timers[certARN] = t

	return nil
}

// validRevocationReasons reports whether a given RevocationReason string is valid.
func validRevocationReason(r string) bool {
	switch r {
	case "UNSPECIFIED", "KEY_COMPROMISE", "CA_COMPROMISE", "AFFILIATION_CHANGED",
		"SUPERSEDED", "CESSATION_OF_OPERATION", "CERTIFICATE_HOLD",
		"REMOVE_FROM_CRL", "PRIVILEGE_WITHDRAWN", "A_A_COMPROMISE":
		return true
	default:
		return false
	}
}

// RevokeCertificate marks the certificate as REVOKED with the given reason.
// Returns ErrAlreadyRevoked if the certificate is already revoked.
// Only ISSUED certificates can be revoked; PENDING_VALIDATION certs return ErrInvalidParameter.
func (b *InMemoryBackend) RevokeCertificate(certARN, revocationReason string) error {
	if certARN == "" {
		return fmt.Errorf("%w: CertificateArn is required", ErrInvalidParameter)
	}

	if revocationReason == "" {
		return fmt.Errorf("%w: RevocationReason is required", ErrInvalidParameter)
	}

	if !validRevocationReason(revocationReason) {
		return fmt.Errorf("%w: invalid RevocationReason %q", ErrInvalidParameter, revocationReason)
	}

	b.mu.Lock("RevokeCertificate")
	defer b.mu.Unlock()

	cert, ok := b.certs[certARN]
	if !ok {
		return fmt.Errorf("%w: certificate %s not found", ErrCertNotFound, certARN)
	}

	if cert.Status == statusRevoked {
		return fmt.Errorf("%w: certificate %s is already revoked", ErrAlreadyRevoked, certARN)
	}

	if cert.Status == statusPendingValidation {
		return fmt.Errorf(
			"%w: certificate %s is in PENDING_VALIDATION and cannot be revoked",
			ErrInvalidState, certARN,
		)
	}

	now := time.Now().UTC()
	cert.Status = statusRevoked
	cert.RevocationReason = revocationReason
	cert.RevokedAt = &now

	// Stop any pending auto-validate timer.
	if t, exists := b.timers[certARN]; exists {
		t.Stop()
		delete(b.timers, certARN)
	}

	return nil
}

// validTransparencyPreference reports whether a given CertificateTransparencyLoggingPreference is valid.
func validTransparencyPreference(p string) bool {
	return p == transparencyLoggingEnabled || p == transparencyLoggingDisabled
}

// UpdateCertificateOptions sets the CertificateTransparencyLoggingPreference for
// a certificate. Only ISSUED certificates may be updated.
func (b *InMemoryBackend) UpdateCertificateOptions(certARN, transparencyLoggingPref string) error {
	if certARN == "" {
		return fmt.Errorf("%w: CertificateArn is required", ErrInvalidParameter)
	}

	if transparencyLoggingPref == "" {
		return fmt.Errorf("%w: Options.CertificateTransparencyLoggingPreference is required", ErrInvalidParameter)
	}

	if !validTransparencyPreference(transparencyLoggingPref) {
		return fmt.Errorf(
			"%w: invalid CertificateTransparencyLoggingPreference %q",
			ErrInvalidParameter,
			transparencyLoggingPref,
		)
	}

	b.mu.Lock("UpdateCertificateOptions")
	defer b.mu.Unlock()

	cert, ok := b.certs[certARN]
	if !ok {
		return fmt.Errorf("%w: certificate %s not found", ErrCertNotFound, certARN)
	}

	if cert.Status != statusIssued {
		return fmt.Errorf("%w: only ISSUED certificates may have options updated", ErrInvalidState)
	}

	cert.CertificateTransparencyLoggingPref = transparencyLoggingPref

	return nil
}

// ExpireCertificate transitions an ISSUED certificate to EXPIRED status.
// Returns ErrCertNotFound if no such certificate exists, ErrInvalidParameter if the
// certificate is not in ISSUED status.
func (b *InMemoryBackend) ExpireCertificate(certARN string) error {
	b.mu.Lock("ExpireCertificate")
	defer b.mu.Unlock()

	cert, ok := b.certs[certARN]
	if !ok {
		return fmt.Errorf("%w: certificate %s not found", ErrCertNotFound, certARN)
	}

	if cert.Status != statusIssued {
		return fmt.Errorf("%w: only ISSUED certificates can be expired, got %s", ErrInvalidParameter, cert.Status)
	}

	cert.Status = statusExpired

	return nil
}

// InactivateCertificate transitions an ISSUED certificate to INACTIVE status.
// Returns ErrCertNotFound if no such certificate exists, ErrInvalidParameter if the
// certificate is not in ISSUED status.
func (b *InMemoryBackend) InactivateCertificate(certARN string) error {
	b.mu.Lock("InactivateCertificate")
	defer b.mu.Unlock()

	cert, ok := b.certs[certARN]
	if !ok {
		return fmt.Errorf("%w: certificate %s not found", ErrCertNotFound, certARN)
	}

	if cert.Status != statusIssued {
		return fmt.Errorf("%w: only ISSUED certificates can be inactivated, got %s", ErrInvalidParameter, cert.Status)
	}

	cert.Status = statusInactive

	return nil
}

// TimeoutPendingValidation transitions a PENDING_VALIDATION certificate to VALIDATION_TIMED_OUT.
// Returns ErrCertNotFound if no such certificate exists, ErrInvalidParameter if the
// certificate is not in PENDING_VALIDATION status.
func (b *InMemoryBackend) TimeoutPendingValidation(certARN string) error {
	b.mu.Lock("TimeoutPendingValidation")
	defer b.mu.Unlock()

	cert, ok := b.certs[certARN]
	if !ok {
		return fmt.Errorf("%w: certificate %s not found", ErrCertNotFound, certARN)
	}

	if cert.Status != statusPendingValidation {
		return fmt.Errorf(
			"%w: only PENDING_VALIDATION certificates can time out, got %s",
			ErrInvalidParameter, cert.Status,
		)
	}

	// Stop any pending auto-validate timer.
	if t, exists := b.timers[certARN]; exists {
		t.Stop()
		delete(b.timers, certARN)
	}

	cert.Status = statusValidationTimedOut

	return nil
}

// FailCertificate transitions a PENDING_VALIDATION certificate to FAILED status with
// the given failure reason.
// Returns ErrCertNotFound if no such certificate exists, ErrInvalidParameter if the
// certificate is not in PENDING_VALIDATION status.
func (b *InMemoryBackend) FailCertificate(certARN, reason string) error {
	b.mu.Lock("FailCertificate")
	defer b.mu.Unlock()

	cert, ok := b.certs[certARN]
	if !ok {
		return fmt.Errorf("%w: certificate %s not found", ErrCertNotFound, certARN)
	}

	if cert.Status != statusPendingValidation {
		return fmt.Errorf(
			"%w: only PENDING_VALIDATION certificates can be failed, got %s",
			ErrInvalidParameter, cert.Status,
		)
	}

	// Stop any pending auto-validate timer.
	if t, exists := b.timers[certARN]; exists {
		t.Stop()
		delete(b.timers, certARN)
	}

	cert.Status = statusFailed
	cert.FailureReason = reason

	return nil
}

func generateKey(keyAlgorithm string) (any, any, string, error) {
	const sigAlgoSHA256WithRSA = "SHA256WITHRSA"
	const rsa1024 = 1024
	const rsa2048 = 2048
	const rsa3072 = 3072
	const rsa4096 = 4096

	switch keyAlgorithm {
	case "RSA_1024":
		privRSA, rsaErr := rsa.GenerateKey(cryptorand.Reader, rsa1024)
		if rsaErr != nil {
			return nil, nil, "", rsaErr
		}

		return privRSA, &privRSA.PublicKey, sigAlgoSHA256WithRSA, nil
	case "RSA_2048":
		privRSA, rsaErr := rsa.GenerateKey(cryptorand.Reader, rsa2048)
		if rsaErr != nil {
			return nil, nil, "", rsaErr
		}

		return privRSA, &privRSA.PublicKey, sigAlgoSHA256WithRSA, nil
	case "RSA_3072":
		privRSA, rsaErr := rsa.GenerateKey(cryptorand.Reader, rsa3072)
		if rsaErr != nil {
			return nil, nil, "", rsaErr
		}

		return privRSA, &privRSA.PublicKey, sigAlgoSHA256WithRSA, nil
	case "RSA_4096":
		privRSA, rsaErr := rsa.GenerateKey(cryptorand.Reader, rsa4096)
		if rsaErr != nil {
			return nil, nil, "", rsaErr
		}

		return privRSA, &privRSA.PublicKey, sigAlgoSHA256WithRSA, nil
	case "EC_secp384r1":
		privEC, ecErr := ecdsa.GenerateKey(elliptic.P384(), cryptorand.Reader)
		if ecErr != nil {
			return nil, nil, "", ecErr
		}

		return privEC, &privEC.PublicKey, "SHA384WITHECDSA", nil
	case "EC_prime256v1", "":
		privEC, ecErr := ecdsa.GenerateKey(elliptic.P256(), cryptorand.Reader)
		if ecErr != nil {
			return nil, nil, "", ecErr
		}

		return privEC, &privEC.PublicKey, "SHA256WITHECDSA", nil
	default:
		return nil, nil, "", fmt.Errorf("%w: unsupported key algorithm %s", ErrInvalidParameter, keyAlgorithm)
	}
}
