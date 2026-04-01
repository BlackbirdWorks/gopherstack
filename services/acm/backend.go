package acm

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	cryptorand "crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

var (
	ErrCertNotFound     = errors.New("ResourceNotFoundException")
	ErrInvalidParameter = errors.New("ValidationException")
	ErrNotEligible      = errors.New("RequestError")
	ErrAlreadyRevoked   = errors.New("InvalidStateException")
	errInvalidPEM       = errors.New("failed to decode PEM block")
)

const (
	validationMethodDNS     = "DNS"
	validationMethodEMAIL   = "EMAIL"
	statusPendingValidation = "PENDING_VALIDATION"
	statusIssued            = "ISSUED"
	statusRevoked           = "REVOKED"
	validationStatusSuccess = "SUCCESS"
	validationTokenLen      = 8
	autoValidateDelayMS     = 100
	randByteDivisor         = 2
	certTypeImported        = "IMPORTED"
	certValidityDuration    = 365 * 24 * time.Hour

	defaultDaysBeforeExpiry = int32(45)

	transparencyLoggingEnabled  = "ENABLED"
	transparencyLoggingDisabled = "DISABLED"
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
}

// Certificate represents an ACM certificate.
type Certificate struct {
	RevokedAt                          *time.Time               `json:"revokedAt,omitempty"`
	CreatedAt                          time.Time                `json:"createdAt"`
	NotBefore                          time.Time                `json:"notBefore"`
	NotAfter                           time.Time                `json:"notAfter"`
	ARN                                string                   `json:"arn"`
	DomainName                         string                   `json:"domainName"`
	Status                             string                   `json:"status"`
	Type                               string                   `json:"type"`
	RevocationReason                   string                   `json:"revocationReason,omitempty"`
	RenewalEligibility                 string                   `json:"renewalEligibility,omitempty"`
	ValidationMethod                   string                   `json:"validationMethod,omitempty"`
	CertificateBody                    string                   `json:"certificateBody,omitempty"`
	CertificateChain                   string                   `json:"certificateChain,omitempty"`
	PrivateKey                         string                   `json:"privateKey,omitempty"`
	CertificateTransparencyLoggingPref string                   `json:"certTransparencyLoggingPref,omitempty"`
	SubjectAlternativeNames            []string                 `json:"subjectAlternativeNames,omitempty"`
	DomainValidationOptions            []DomainValidationOption `json:"domainValidationOptions,omitempty"`
}

// AccountConfig holds account-level ACM configuration.
type AccountConfig struct {
	DaysBeforeExpiry int32 `json:"daysBeforeExpiry"`
}

const (
	renewalEligibilityEligible   = "ELIGIBLE"
	renewalEligibilityIneligible = "INELIGIBLE"
)

// InMemoryBackend is the in-memory store for ACM certificates.
type InMemoryBackend struct {
	timers         map[string]*time.Timer
	certs          map[string]*Certificate
	idempotencyMap map[string]struct{}
	mu             *lockmetrics.RWMutex
	accountID      string
	region         string
	accountConfig  AccountConfig
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		certs:          make(map[string]*Certificate),
		timers:         make(map[string]*time.Timer),
		idempotencyMap: make(map[string]struct{}),
		accountID:      accountID,
		region:         region,
		mu:             lockmetrics.New("acm"),
		accountConfig:  AccountConfig{DaysBeforeExpiry: defaultDaysBeforeExpiry},
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// RequestCertificate creates a new certificate for the given domain.
// When validationMethod is "DNS" or "EMAIL" the certificate starts in
// PENDING_VALIDATION and automatically transitions to ISSUED after a short delay.
func (b *InMemoryBackend) RequestCertificate(
	domainName, certType, validationMethod string,
	sans []string,
) (*Certificate, error) {
	if domainName == "" {
		return nil, fmt.Errorf("%w: DomainName is required", ErrInvalidParameter)
	}

	certBody, privateKey, notBefore, notAfter, err := generateSelfSignedCert(domainName, sans)
	if err != nil {
		return nil, fmt.Errorf("failed to generate certificate: %w", err)
	}

	b.mu.Lock("RequestCertificate")
	defer b.mu.Unlock()

	id := fmt.Sprintf("%x", time.Now().UnixNano())
	certARN := arn.Build("acm", b.region, b.accountID, "certificate/"+id)

	if certType == "" {
		certType = "AMAZON_ISSUED"
	}

	renewalEligibility := renewalEligibilityEligible
	if certType == certTypeImported {
		renewalEligibility = renewalEligibilityIneligible
	}

	status := statusIssued
	var dvoList []DomainValidationOption

	allDomains := append([]string{domainName}, sans...)

	switch validationMethod {
	case validationMethodDNS, validationMethodEMAIL:
		status = statusPendingValidation
		dvoList, err = buildDomainValidationOptions(allDomains, validationMethod)
	default:
		dvoList, err = buildDomainValidationOptions(allDomains, validationMethodDNS)
	}

	if err != nil {
		return nil, err
	}

	// When the certificate is issued immediately, mark all DVOs as validated.
	if status == statusIssued {
		for i := range dvoList {
			dvoList[i].ValidationStatus = validationStatusSuccess
		}
	}

	cert := &Certificate{
		ARN:                     certARN,
		DomainName:              domainName,
		Status:                  status,
		Type:                    certType,
		RenewalEligibility:      renewalEligibility,
		ValidationMethod:        validationMethod,
		SubjectAlternativeNames: sans,
		DomainValidationOptions: dvoList,
		CertificateBody:         certBody,
		PrivateKey:              privateKey,
		CreatedAt:               time.Now().UTC(),
		NotBefore:               notBefore,
		NotAfter:                notAfter,
	}
	b.certs[certARN] = cert

	if status == statusPendingValidation {
		t := time.AfterFunc(autoValidateDelayMS*time.Millisecond, func() { b.autoValidate(certARN) })
		b.timers[certARN] = t
	}

	cp := copyCert(cert)

	return &cp, nil
}

// copyCert returns a deep copy of a Certificate, ensuring the DomainValidationOptions
// slice and its ResourceRecord pointers are not shared with the original.
func copyCert(c *Certificate) Certificate {
	cp := *c

	if len(c.DomainValidationOptions) > 0 {
		cp.DomainValidationOptions = make([]DomainValidationOption, len(c.DomainValidationOptions))
		copy(cp.DomainValidationOptions, c.DomainValidationOptions)

		for i, dvo := range c.DomainValidationOptions {
			if dvo.ResourceRecord != nil {
				rr := *dvo.ResourceRecord
				cp.DomainValidationOptions[i].ResourceRecord = &rr
			}
		}
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

	c.Status = statusIssued

	for i := range c.DomainValidationOptions {
		c.DomainValidationOptions[i].ValidationStatus = "SUCCESS"
	}
}

// ImportCertificate stores a PEM-encoded certificate, private key, and optional
// certificate chain, returning the ARN of the newly created entry.
func (b *InMemoryBackend) ImportCertificate(certBody, privateKey, certChain string) (*Certificate, error) {
	if certBody == "" {
		return nil, fmt.Errorf("%w: Certificate is required", ErrInvalidParameter)
	}

	if privateKey == "" {
		return nil, fmt.Errorf("%w: PrivateKey is required", ErrInvalidParameter)
	}

	domainName, notBefore, notAfter, err := extractCertMetadata(certBody)
	if err != nil {
		domainName = "imported"
	}

	b.mu.Lock("ImportCertificate")
	defer b.mu.Unlock()

	id := fmt.Sprintf("%x", time.Now().UnixNano())
	certARN := arn.Build("acm", b.region, b.accountID, "certificate/"+id)

	cert := &Certificate{
		ARN:                certARN,
		DomainName:         domainName,
		Status:             "ISSUED",
		Type:               certTypeImported,
		RenewalEligibility: renewalEligibilityIneligible,
		CertificateBody:    certBody,
		CertificateChain:   certChain,
		PrivateKey:         privateKey,
		CreatedAt:          time.Now().UTC(),
		NotBefore:          notBefore,
		NotAfter:           notAfter,
	}
	b.certs[certARN] = cert

	cp := copyCert(cert)

	return &cp, nil
}

// RenewCertificate regenerates the certificate material for an AMAZON_ISSUED certificate,
// extending its validity by one year. Returns ErrNotEligible for IMPORTED certificates,
// as AWS ACM does not support renewing imported certificates.
func (b *InMemoryBackend) RenewCertificate(certARN string) error {
	b.mu.RLock("RenewCertificate")
	cert, ok := b.certs[certARN]
	var certType, domainName string
	var sans []string
	if ok {
		certType = cert.Type
		domainName = cert.DomainName
		// Copy slice to avoid holding a reference to the internal backing array
		// outside the lock.
		sans = append([]string(nil), cert.SubjectAlternativeNames...)
	}
	b.mu.RUnlock()

	if !ok {
		return fmt.Errorf("%w: certificate %s not found", ErrCertNotFound, certARN)
	}

	if certType == certTypeImported {
		return fmt.Errorf("%w: only AMAZON_ISSUED certificates can be renewed", ErrNotEligible)
	}

	certBody, privateKey, notBefore, notAfter, err := generateSelfSignedCert(domainName, sans)
	if err != nil {
		return fmt.Errorf("failed to regenerate certificate: %w", err)
	}

	b.mu.Lock("RenewCertificate")
	defer b.mu.Unlock()

	c, exists := b.certs[certARN]
	if !exists {
		return fmt.Errorf("%w: certificate %s not found", ErrCertNotFound, certARN)
	}

	c.CertificateBody = certBody
	c.PrivateKey = privateKey
	c.NotBefore = notBefore
	c.NotAfter = notAfter

	return nil
}

// ExportCertificate returns the PEM certificate body, chain, and private key for
// an IMPORTED certificate. Returns ErrNotEligible for AMAZON_ISSUED certificates.
func (b *InMemoryBackend) ExportCertificate(certARN string) (*Certificate, error) {
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

// ListCertificates returns a paginated list of certificates sorted by ARN.
func (b *InMemoryBackend) ListCertificates(nextToken string, maxItems int) page.Page[Certificate] {
	b.mu.RLock("ListCertificates")
	defer b.mu.RUnlock()

	certs := make([]Certificate, 0, len(b.certs))
	for _, c := range b.certs {
		certs = append(certs, copyCert(c))
	}

	sort.Slice(certs, func(i, j int) bool { return certs[i].ARN < certs[j].ARN })

	return page.New(certs, nextToken, maxItems, acmDefaultMaxItems)
}

const acmDefaultMaxItems = 100

// DeleteCertificate removes the certificate with the given ARN.
func (b *InMemoryBackend) DeleteCertificate(arn string) error {
	b.mu.Lock("DeleteCertificate")
	defer b.mu.Unlock()

	if _, exists := b.certs[arn]; !exists {
		return fmt.Errorf("%w: certificate %s not found", ErrCertNotFound, arn)
	}

	if t, ok := b.timers[arn]; ok {
		t.Stop()
		delete(b.timers, arn)
	}

	delete(b.certs, arn)

	return nil
}

// buildDomainValidationOptions creates DomainValidationOption entries with
// synthetic CNAME records for each domain in the list.
func buildDomainValidationOptions(domains []string, validationMethod string) ([]DomainValidationOption, error) {
	opts := make([]DomainValidationOption, 0, len(domains))

	for _, d := range domains {
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

		if validationMethod == validationMethodDNS {
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

// generateSelfSignedCert generates a self-signed ECDSA P-256 certificate for
// the given domain (and optional SANs) and returns PEM-encoded certificate,
// private key, and the certificate's NotBefore/NotAfter validity times.
func generateSelfSignedCert(
	domainName string,
	sans []string,
) (string, string, time.Time, time.Time, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), cryptorand.Reader)
	if err != nil {
		return "", "", time.Time{}, time.Time{}, fmt.Errorf("generate key: %w", err)
	}

	serialBytes := make([]byte, 16) //nolint:mnd // 128-bit random serial
	if _, err = cryptorand.Read(serialBytes); err != nil {
		return "", "", time.Time{}, time.Time{}, fmt.Errorf("generate serial: %w", err)
	}

	serial := new(big.Int).SetBytes(serialBytes)

	dnsNames := append([]string{domainName}, sans...)

	notBefore := time.Now().UTC().Truncate(time.Second)
	notAfter := notBefore.Add(certValidityDuration)

	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: domainName},
		DNSNames:     dnsNames,
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	certDER, err := x509.CreateCertificate(cryptorand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		return "", "", time.Time{}, time.Time{}, fmt.Errorf("create certificate: %w", err)
	}

	certPEM := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}))

	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return "", "", time.Time{}, time.Time{}, fmt.Errorf("marshal key: %w", err)
	}

	keyPEM := string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))

	return certPEM, keyPEM, notBefore, notAfter, nil
}

// extractCertMetadata parses a PEM-encoded certificate and returns the primary
// domain name (first SAN or CN), NotBefore, and NotAfter.
func extractCertMetadata(certPEM string) (string, time.Time, time.Time, error) {
	block, _ := pem.Decode([]byte(certPEM))
	if block == nil {
		return "", time.Time{}, time.Time{}, errInvalidPEM
	}

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return "", time.Time{}, time.Time{}, fmt.Errorf("parse certificate: %w", err)
	}

	domainName := cert.Subject.CommonName
	if len(cert.DNSNames) > 0 {
		domainName = cert.DNSNames[0]
	}

	return domainName, cert.NotBefore.UTC(), cert.NotAfter.UTC(), nil
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
	b.idempotencyMap = make(map[string]struct{})
	b.accountConfig = AccountConfig{DaysBeforeExpiry: defaultDaysBeforeExpiry}
}

// GetAccountConfiguration returns the account-level ACM configuration.
func (b *InMemoryBackend) GetAccountConfiguration() AccountConfig {
	b.mu.RLock("GetAccountConfiguration")
	defer b.mu.RUnlock()

	return b.accountConfig
}

// PutAccountConfiguration stores the account-level ACM configuration.
// idempotencyToken must be non-empty; repeated calls with the same token within
// the same backend lifetime are silently accepted (idempotent).
func (b *InMemoryBackend) PutAccountConfiguration(idempotencyToken string, daysBeforeExpiry *int32) error {
	if idempotencyToken == "" {
		return fmt.Errorf("%w: IdempotencyToken is required", ErrInvalidParameter)
	}

	if daysBeforeExpiry != nil && *daysBeforeExpiry < 0 {
		return fmt.Errorf("%w: DaysBeforeExpiry must be non-negative", ErrInvalidParameter)
	}

	b.mu.Lock("PutAccountConfiguration")
	defer b.mu.Unlock()

	if _, seen := b.idempotencyMap[idempotencyToken]; seen {
		return nil
	}

	b.idempotencyMap[idempotencyToken] = struct{}{}

	if daysBeforeExpiry != nil {
		b.accountConfig.DaysBeforeExpiry = *daysBeforeExpiry
	} else {
		b.accountConfig.DaysBeforeExpiry = defaultDaysBeforeExpiry
	}

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

	// Reset the auto-validate timer to simulate email resend triggering re-validation.
	if t, exists := b.timers[certARN]; exists {
		t.Stop()
	}

	t := time.AfterFunc(autoValidateDelayMS*time.Millisecond, func() { b.autoValidate(certARN) })
	b.timers[certARN] = t

	return nil
}

// validRevocationReasons reports whether a given RevocationReason string is valid.
func validRevocationReason(r string) bool {
	switch r {
	case "UNSPECIFIED", "KEY_COMPROMISE", "CA_COMPROMISE", "AFFILIATION_CHANGED",
		"SUPERCEDED", "SUPERSEDED", "CESSATION_OF_OPERATION", "CERTIFICATE_HOLD",
		"REMOVE_FROM_CRL", "PRIVILEGE_WITHDRAWN", "A_A_COMPROMISE":
		return true
	default:
		return false
	}
}

// RevokeCertificate marks the certificate as REVOKED with the given reason.
// Returns ErrAlreadyRevoked if the certificate is already revoked.
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
		return fmt.Errorf("%w: only ISSUED certificates may have options updated", ErrInvalidParameter)
	}

	cert.CertificateTransparencyLoggingPref = transparencyLoggingPref

	return nil
}
