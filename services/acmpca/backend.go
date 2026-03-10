package acmpca

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
	// ErrCANotFound is returned when a Certificate Authority is not found.
	ErrCANotFound = errors.New("ResourceNotFoundException")
	// ErrCertNotFound is returned when an issued certificate is not found.
	ErrCertNotFound = errors.New("ResourceNotFoundException")
	// ErrInvalidParameter is returned when an invalid parameter is provided.
	ErrInvalidParameter = errors.New("InvalidParameterException")
	// ErrInvalidState is returned when the CA is in an invalid state for the operation.
	ErrInvalidState = errors.New("InvalidStateException")

	errCAPrivKeyNil    = errors.New("CA private key is nil")
	errDecodeCSRPEM    = errors.New("failed to decode CSR PEM")
	errDecodeCACertPEM = errors.New("failed to decode CA certificate PEM")
)

const (
	caStatusCreating     = "CREATING"
	caStatusActive       = "ACTIVE"
	caStatusDeleted      = "DELETED"
	caTypePRoot          = "ROOT"
	caTypeSubordinate    = "SUBORDINATE"
	defaultMaxItems      = 100
	certStatusActive     = "ACTIVE"
	certStatusRevoked    = "REVOKED"
	defaultKeyAlgorithm  = "EC_prime256v1"
	defaultSignAlgorithm = "SHA256WITHRSA"
	caResourceIDPrefix   = "certificate-authority/"
	certResourceIDPrefix = "certificate/"

	// serialBitLen is the number of bits for a random serial number.
	serialBitLen = 128
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
	Type                              string                            `json:"type"`
	Status                            string                            `json:"status"`
	CertificateBody                   string                            `json:"certificateBody,omitempty"`
	CSR                               string                            `json:"csr,omitempty"`
}

// IssuedCertificate represents a certificate issued by an ACM PCA Certificate Authority.
type IssuedCertificate struct {
	IssuedAt  time.Time `json:"issuedAt"`
	NotBefore time.Time `json:"notBefore"`
	NotAfter  time.Time `json:"notAfter"`
	ARN       string    `json:"arn"`
	CAARN     string    `json:"caArn"`
	Status    string    `json:"status"`
	Serial    string    `json:"serial"`
	CertBody  string    `json:"certBody"`
}

// InMemoryBackend is the in-memory store for ACM PCA resources.
type InMemoryBackend struct {
	cas       map[string]*CertificateAuthority
	certs     map[string]*IssuedCertificate
	mu        *lockmetrics.RWMutex
	accountID string
	region    string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		cas:       make(map[string]*CertificateAuthority),
		certs:     make(map[string]*IssuedCertificate),
		accountID: accountID,
		region:    region,
		mu:        lockmetrics.New("acmpca"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// CreateCertificateAuthority creates a new Certificate Authority.
func (b *InMemoryBackend) CreateCertificateAuthority(
	caType string,
	cfg CertificateAuthorityConfiguration,
) (*CertificateAuthority, error) {
	if caType == "" {
		caType = caTypePRoot
	}

	if caType != caTypePRoot && caType != caTypeSubordinate {
		return nil, fmt.Errorf("%w: CertificateAuthorityType must be ROOT or SUBORDINATE", ErrInvalidParameter)
	}

	b.mu.Lock("CreateCertificateAuthority")
	defer b.mu.Unlock()

	id := fmt.Sprintf("%x", time.Now().UnixNano())
	caARN := arn.Build("acm-pca", b.region, b.accountID, caResourceIDPrefix+id)

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
		Type:                              caType,
		Status:                            caStatusCreating,
		CertificateAuthorityConfiguration: cfg,
		CSR:                               csrPEM,
		CreatedAt:                         now,
		privKey:                           privKey,
	}

	b.cas[caARN] = ca

	// For ROOT CAs we auto-sign and activate to make Terraform apply succeed without
	// requiring a multi-step workflow.
	if caType == caTypePRoot {
		if activateErr := b.selfSignAndActivate(ca, now); activateErr != nil {
			return nil, activateErr
		}
	}

	cp := copyCA(ca)

	return &cp, nil
}

// selfSignAndActivate generates a self-signed certificate for the CA and sets it to ACTIVE.
// Must be called with the write lock held.
func (b *InMemoryBackend) selfSignAndActivate(ca *CertificateAuthority, now time.Time) error {
	certPEM, err := selfSignCA(ca, now)
	if err != nil {
		return fmt.Errorf("self-sign CA: %w", err)
	}

	ca.CertificateBody = certPEM
	ca.Status = caStatusActive
	ca.NotBefore = now
	ca.NotAfter = now.Add(10 * 365 * 24 * time.Hour)

	return nil
}

// DescribeCertificateAuthority returns the CA with the given ARN.
func (b *InMemoryBackend) DescribeCertificateAuthority(caARN string) (*CertificateAuthority, error) {
	b.mu.RLock("DescribeCertificateAuthority")
	defer b.mu.RUnlock()

	ca, ok := b.cas[caARN]
	if !ok {
		return nil, fmt.Errorf("%w: CA %s not found", ErrCANotFound, caARN)
	}

	cp := copyCA(ca)

	return &cp, nil
}

// ListCertificateAuthorities returns a paginated list of CAs sorted by ARN.
func (b *InMemoryBackend) ListCertificateAuthorities(nextToken string, maxItems int) page.Page[CertificateAuthority] {
	b.mu.RLock("ListCertificateAuthorities")
	defer b.mu.RUnlock()

	cas := make([]CertificateAuthority, 0, len(b.cas))
	for _, ca := range b.cas {
		cas = append(cas, copyCA(ca))
	}

	sort.Slice(cas, func(i, j int) bool { return cas[i].ARN < cas[j].ARN })

	return page.New(cas, nextToken, maxItems, defaultMaxItems)
}

// DeleteCertificateAuthority marks the CA as DELETED.
func (b *InMemoryBackend) DeleteCertificateAuthority(caARN string) error {
	b.mu.Lock("DeleteCertificateAuthority")
	defer b.mu.Unlock()

	ca, ok := b.cas[caARN]
	if !ok {
		return fmt.Errorf("%w: CA %s not found", ErrCANotFound, caARN)
	}

	ca.Status = caStatusDeleted

	return nil
}

// UpdateCertificateAuthority updates the CA status.
func (b *InMemoryBackend) UpdateCertificateAuthority(caARN, status string) error {
	b.mu.Lock("UpdateCertificateAuthority")
	defer b.mu.Unlock()

	ca, ok := b.cas[caARN]
	if !ok {
		return fmt.Errorf("%w: CA %s not found", ErrCANotFound, caARN)
	}

	if status != "" {
		ca.Status = status
	}

	return nil
}

// GetCertificateAuthorityCsr returns the CSR PEM for the given CA.
func (b *InMemoryBackend) GetCertificateAuthorityCsr(caARN string) (string, error) {
	b.mu.RLock("GetCertificateAuthorityCsr")
	defer b.mu.RUnlock()

	ca, ok := b.cas[caARN]
	if !ok {
		return "", fmt.Errorf("%w: CA %s not found", ErrCANotFound, caARN)
	}

	return ca.CSR, nil
}

// ImportCertificateAuthorityCertificate imports a signed certificate for the CA, activating it.
func (b *InMemoryBackend) ImportCertificateAuthorityCertificate(caARN, certPEM string) error {
	b.mu.Lock("ImportCertificateAuthorityCertificate")
	defer b.mu.Unlock()

	ca, ok := b.cas[caARN]
	if !ok {
		return fmt.Errorf("%w: CA %s not found", ErrCANotFound, caARN)
	}

	ca.CertificateBody = certPEM
	ca.Status = caStatusActive

	return nil
}

// GetCertificateAuthorityCertificate returns the certificate PEM for the given CA.
func (b *InMemoryBackend) GetCertificateAuthorityCertificate(caARN string) (string, error) {
	b.mu.RLock("GetCertificateAuthorityCertificate")
	defer b.mu.RUnlock()

	ca, ok := b.cas[caARN]
	if !ok {
		return "", fmt.Errorf("%w: CA %s not found", ErrCANotFound, caARN)
	}

	return ca.CertificateBody, nil
}

// IssueCertificate issues a new certificate signed by the given CA.
func (b *InMemoryBackend) IssueCertificate(caARN, csrPEM string, validityDays int) (*IssuedCertificate, error) {
	b.mu.Lock("IssueCertificate")
	defer b.mu.Unlock()

	ca, ok := b.cas[caARN]
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

	id := fmt.Sprintf("%x", time.Now().UnixNano())
	certARN := arn.Build("acm-pca", b.region, b.accountID,
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

	b.certs[certARN] = cert

	cp := *cert

	return &cp, nil
}

// GetCertificate returns the certificate PEM for the given certificate ARN.
func (b *InMemoryBackend) GetCertificate(certARN string) (*IssuedCertificate, error) {
	b.mu.RLock("GetCertificate")
	defer b.mu.RUnlock()

	cert, ok := b.certs[certARN]
	if !ok {
		return nil, fmt.Errorf("%w: certificate %s not found", ErrCertNotFound, certARN)
	}

	cp := *cert

	return &cp, nil
}

// RevokeCertificate revokes the given certificate.
func (b *InMemoryBackend) RevokeCertificate(caARN, serial, _ string) error {
	b.mu.Lock("RevokeCertificate")
	defer b.mu.Unlock()

	if _, ok := b.cas[caARN]; !ok {
		return fmt.Errorf("%w: CA %s not found", ErrCANotFound, caARN)
	}

	for _, cert := range b.certs {
		if cert.CAARN == caARN && cert.Serial == serial {
			cert.Status = certStatusRevoked

			return nil
		}
	}

	return fmt.Errorf("%w: certificate with serial %s not found", ErrCertNotFound, serial)
}

// ListCertificates returns a paginated list of certificates issued by the given CA.
func (b *InMemoryBackend) ListCertificates(
	caARN string,
	nextToken string,
	maxItems int,
) page.Page[IssuedCertificate] {
	b.mu.RLock("ListCertificates")
	defer b.mu.RUnlock()

	var certs []IssuedCertificate
	for _, c := range b.certs {
		if c.CAARN == caARN {
			certs = append(certs, *c)
		}
	}

	sort.Slice(certs, func(i, j int) bool { return certs[i].ARN < certs[j].ARN })

	return page.New(certs, nextToken, maxItems, defaultMaxItems)
}

// copyCA returns a shallow copy of the CertificateAuthority, excluding the private key.
func copyCA(ca *CertificateAuthority) CertificateAuthority {
	cp := *ca
	cp.privKey = nil

	return cp
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

// selfSignCA generates a self-signed certificate for the given CA.
func selfSignCA(ca *CertificateAuthority, now time.Time) (string, error) {
	if ca.privKey == nil {
		return "", errCAPrivKeyNil
	}

	serial, err := cryptorand.Int(
		cryptorand.Reader,
		new(big.Int).Lsh(big.NewInt(1), serialBitLen),
	)
	if err != nil {
		return "", fmt.Errorf("generate serial: %w", err)
	}

	cn := ca.CertificateAuthorityConfiguration.Subject.CommonName
	if cn == "" {
		cn = "Gopherstack Root CA"
	}

	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             now,
		NotAfter:              now.Add(10 * 365 * 24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(cryptorand.Reader, tmpl, tmpl, &ca.privKey.PublicKey, ca.privKey)
	if err != nil {
		return "", fmt.Errorf("create certificate: %w", err)
	}

	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})), nil
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
