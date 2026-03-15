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
	errInvalidPEM       = errors.New("failed to decode PEM block")
)

const (
	validationMethodDNS     = "DNS"
	validationMethodEMAIL   = "EMAIL"
	statusPendingValidation = "PENDING_VALIDATION"
	statusIssued            = "ISSUED"
	validationStatusSuccess = "SUCCESS"
	validationTokenLen      = 8
	autoValidateDelayMS     = 100
	randByteDivisor         = 2
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
	CreatedAt               time.Time                `json:"createdAt"`
	ARN                     string                   `json:"arn"`
	DomainName              string                   `json:"domainName"`
	Status                  string                   `json:"status"`
	Type                    string                   `json:"type"`
	ValidationMethod        string                   `json:"validationMethod,omitempty"`
	CertificateBody         string                   `json:"certificateBody,omitempty"`
	CertificateChain        string                   `json:"certificateChain,omitempty"`
	PrivateKey              string                   `json:"privateKey,omitempty"`
	SubjectAlternativeNames []string                 `json:"subjectAlternativeNames,omitempty"`
	DomainValidationOptions []DomainValidationOption `json:"domainValidationOptions,omitempty"`
}

// InMemoryBackend is the in-memory store for ACM certificates.
type InMemoryBackend struct {
	certs     map[string]*Certificate
	mu        *lockmetrics.RWMutex
	accountID string
	region    string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		certs:     make(map[string]*Certificate),
		accountID: accountID,
		region:    region,
		mu:        lockmetrics.New("acm"),
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

	certBody, privateKey, err := generateSelfSignedCert(domainName, sans)
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
		ValidationMethod:        validationMethod,
		SubjectAlternativeNames: sans,
		DomainValidationOptions: dvoList,
		CertificateBody:         certBody,
		PrivateKey:              privateKey,
		CreatedAt:               time.Now().UTC(),
	}
	b.certs[certARN] = cert

	if status == statusPendingValidation {
		go b.autoValidate(certARN)
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
	time.Sleep(autoValidateDelayMS * time.Millisecond)

	b.mu.Lock("autoValidate")
	defer b.mu.Unlock()

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

	domainName, err := extractCNFromPEM(certBody)
	if err != nil {
		domainName = "imported"
	}

	b.mu.Lock("ImportCertificate")
	defer b.mu.Unlock()

	id := fmt.Sprintf("%x", time.Now().UnixNano())
	certARN := arn.Build("acm", b.region, b.accountID, "certificate/"+id)

	cert := &Certificate{
		ARN:              certARN,
		DomainName:       domainName,
		Status:           "ISSUED",
		Type:             "IMPORTED",
		CertificateBody:  certBody,
		CertificateChain: certChain,
		PrivateKey:       privateKey,
		CreatedAt:        time.Now().UTC(),
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

	cert, ok := b.certs[certARN]
	if !ok {
		return fmt.Errorf("%w: certificate %s not found", ErrCertNotFound, certARN)
	}

	if cert.Type == "IMPORTED" {
		return fmt.Errorf("%w: only AMAZON_ISSUED certificates can be renewed", ErrNotEligible)
	}

	certBody, privateKey, err := generateSelfSignedCert(cert.DomainName, cert.SubjectAlternativeNames)
	if err != nil {
		return fmt.Errorf("failed to regenerate certificate: %w", err)
	}

	cert.CertificateBody = certBody
	cert.PrivateKey = privateKey

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

	if cert.Type != "IMPORTED" && cert.Type != "PRIVATE" {
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
// the given domain (and optional SANs) and returns PEM-encoded certificate and private key.
func generateSelfSignedCert(domainName string, sans []string) (string, string, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), cryptorand.Reader)
	if err != nil {
		return "", "", fmt.Errorf("generate key: %w", err)
	}

	serialBytes := make([]byte, 16) //nolint:mnd // 128-bit random serial
	if _, err = cryptorand.Read(serialBytes); err != nil {
		return "", "", fmt.Errorf("generate serial: %w", err)
	}

	serial := new(big.Int).SetBytes(serialBytes)

	dnsNames := append([]string{domainName}, sans...)

	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: domainName},
		DNSNames:     dnsNames,
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	certDER, err := x509.CreateCertificate(cryptorand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		return "", "", fmt.Errorf("create certificate: %w", err)
	}

	certPEM := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}))

	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return "", "", fmt.Errorf("marshal key: %w", err)
	}

	keyPEM := string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))

	return certPEM, keyPEM, nil
}

// extractCNFromPEM parses a PEM-encoded certificate and returns the CommonName.
func extractCNFromPEM(certPEM string) (string, error) {
	block, _ := pem.Decode([]byte(certPEM))
	if block == nil {
		return "", errInvalidPEM
	}

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return "", fmt.Errorf("parse certificate: %w", err)
	}

	if len(cert.DNSNames) > 0 {
		return cert.DNSNames[0], nil
	}

	return cert.Subject.CommonName, nil
}
