package iot

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/google/uuid"
)

// fakePEM is a minimal fake PEM certificate returned by CreateCertificateFromCsr and RegisterCertificate.
const fakePEM = `-----BEGIN CERTIFICATE-----
MIICpDCCAYwCCQDU+pQ4pHgSpDANBgkqhkiG9w0BAQsFADAUMRIwEAYDVQQDDAls
b2NhbGhvc3QwHhcNMjMwMTAxMDAwMDAwWhcNMjQwMTAxMDAwMDAwWjAUMRIwEAYD
VQQDDAlsb2NhbGhvc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQC7
o4qne60TB3wolFl6qADvFVMZUDCwJJlFBMDkajIxpQFNbBgxDuAQFV8AAAAAAA==
-----END CERTIFICATE-----`

// certIDHexLen is the number of bytes (half the hex char count) for a certificate ID.
const certIDHexLen = 32 // produces a 64-char hex string

// certStatusActive is the AWS IoT certificate ACTIVE status value.
const certStatusActive = "ACTIVE"

// certStatusInactive is the AWS IoT certificate INACTIVE status value.
const certStatusInactive = "INACTIVE"

// certStatusPendingTransfer is the AWS IoT certificate PENDING_TRANSFER status value.
const certStatusPendingTransfer = "PENDING_TRANSFER"

// certStatusRevoked is the AWS IoT certificate REVOKED status value.
const certStatusRevoked = "REVOKED"

// certStatusPendingActivation is the AWS IoT certificate PENDING_ACTIVATION status value.
const certStatusPendingActivation = "PENDING_ACTIVATION"

// randomHex generates a cryptographically random hex string of n bytes (2n characters).
func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic("iot: randomHex: crypto/rand failed: " + err.Error())
	}

	return hex.EncodeToString(b)
}

// newCertificate creates a new Certificate with a random 64-hex-char ID.
func (b *InMemoryBackend) newCertificate(pem, status string) *Certificate {
	certID := randomHex(certIDHexLen)
	arn := arn.Build("iot", b.region, b.accountID, fmt.Sprintf("cert/%s", certID))
	now := time.Now()

	return &Certificate{
		CertificateID:  certID,
		ARN:            arn,
		Status:         status,
		PEM:            pem,
		CreatedAt:      now,
		LastModifiedAt: now,
	}
}

// CreateCertificateFromCsr creates a new certificate from a CSR.
func (b *InMemoryBackend) CreateCertificateFromCsr(input *CreateCertificateFromCsrInput) (*Certificate, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	status := certStatusInactive
	if input.SetAsActive {
		status = certStatusActive
	}

	cert := b.newCertificate(fakePEM, status)
	b.certificates.Put(cert)

	return cert, nil
}

// RegisterCertificate registers a certificate.
func (b *InMemoryBackend) RegisterCertificate(input *RegisterCertificateInput) (*Certificate, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	status := input.Status
	if status == "" {
		status = certStatusInactive
	}

	pem := input.CertificatePem
	if pem == "" {
		pem = fakePEM
	}

	cert := b.newCertificate(pem, status)
	b.certificates.Put(cert)

	return cert, nil
}

// RegisterCertificateWithoutCA registers a certificate without a CA.
func (b *InMemoryBackend) RegisterCertificateWithoutCA(input *RegisterCertificateInput) (*Certificate, error) {
	return b.RegisterCertificate(input)
}

// DescribeCertificate returns a Certificate by ID.
func (b *InMemoryBackend) DescribeCertificate(certificateID string) (*Certificate, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	cert, ok := b.certificates.Get(certificateID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrCertificateNotFound, certificateID)
	}

	cp := *cert

	return &cp, nil
}

// ListCertificates returns all certificates sorted by ID.
func (b *InMemoryBackend) ListCertificates() []*Certificate {
	b.mu.RLock()
	defer b.mu.RUnlock()

	items := b.certificates.Snapshot()
	out := make([]*Certificate, 0, len(items))

	for _, v := range items {
		cp := *v
		out = append(out, &cp)
	}

	return out
}

// isValidCertStatus reports whether s is a legal AWS IoT certificate status.
func isValidCertStatus(s string) bool {
	switch s {
	case certStatusActive, certStatusInactive, certStatusRevoked,
		certStatusPendingTransfer, certStatusPendingActivation:
		return true
	}

	return false
}

// UpdateCertificate updates the status of a certificate.
func (b *InMemoryBackend) UpdateCertificate(input *UpdateCertificateInput) error {
	switch input.NewStatus {
	case certStatusPendingTransfer, certStatusPendingActivation:
		return fmt.Errorf("%w: status %q cannot be set via UpdateCertificate", ErrValidation, input.NewStatus)
	}

	if input.NewStatus != "" && !isValidCertStatus(input.NewStatus) {
		return fmt.Errorf("%w: invalid certificate status %q", ErrValidation, input.NewStatus)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	cert, ok := b.certificates.Get(input.CertificateID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrCertificateNotFound, input.CertificateID)
	}

	cert.Status = input.NewStatus
	cert.LastModifiedAt = time.Now()

	return nil
}

// DeleteCertificate deletes a certificate by ID.
func (b *InMemoryBackend) DeleteCertificate(certificateID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	cert, ok := b.certificates.Get(certificateID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrCertificateNotFound, certificateID)
	}

	if cert.Status == certStatusActive {
		return fmt.Errorf("%w: certificate %q must be deactivated before deletion", ErrDeleteConflict, certificateID)
	}

	b.certificates.Delete(certificateID)

	return nil
}

// CreateCertificateProvider creates a new certificate provider.
func (b *InMemoryBackend) CreateCertificateProvider(
	input *CreateCertificateProviderInput,
) (*CertificateProvider, error) {
	if input.CertificateProviderName == "" {
		return nil, fmt.Errorf("%w: CertificateProviderName is required", ErrValidation)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.certificateProviders.Has(input.CertificateProviderName) {
		return nil, fmt.Errorf("%w: certificate provider %q already exists",
			ErrAlreadyExists, input.CertificateProviderName)
	}

	arn := arn.Build("iot", b.region, b.accountID,
		fmt.Sprintf("certificateprovider/%s", input.CertificateProviderName))

	ops := make([]string, len(input.AccountDefaultForOperations))
	copy(ops, input.AccountDefaultForOperations)

	cp := &CertificateProvider{
		CertificateProviderName:     input.CertificateProviderName,
		ARN:                         arn,
		LambdaFunctionARN:           input.LambdaFunctionARN,
		AccountDefaultForOperations: ops,
		CreatedAt:                   time.Now(),
		LastModifiedAt:              time.Now(),
	}

	b.certificateProviders.Put(cp)

	return cp, nil
}

// DescribeCertificateProvider returns a certificate provider by name.
func (b *InMemoryBackend) DescribeCertificateProvider(name string) (*CertificateProvider, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	cp, ok := b.certificateProviders.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrCertificateProviderNotFound, name)
	}

	result := *cp
	result.AccountDefaultForOperations = make([]string, len(cp.AccountDefaultForOperations))
	copy(result.AccountDefaultForOperations, cp.AccountDefaultForOperations)

	return &result, nil
}

// ListCertificateProviders returns all certificate providers sorted by name.
func (b *InMemoryBackend) ListCertificateProviders() []*CertificateProvider {
	b.mu.RLock()
	defer b.mu.RUnlock()

	items := b.certificateProviders.Snapshot()
	out := make([]*CertificateProvider, 0, len(items))

	for _, v := range items {
		cp := *v
		cp.AccountDefaultForOperations = make([]string, len(v.AccountDefaultForOperations))
		copy(cp.AccountDefaultForOperations, v.AccountDefaultForOperations)
		out = append(out, &cp)
	}

	return out
}

// UpdateCertificateProvider updates an existing certificate provider.
func (b *InMemoryBackend) UpdateCertificateProvider(input *UpdateCertificateProviderInput) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	cp, ok := b.certificateProviders.Get(input.CertificateProviderName)
	if !ok {
		return fmt.Errorf("%w: %s", ErrCertificateProviderNotFound, input.CertificateProviderName)
	}

	if input.LambdaFunctionARN != "" {
		cp.LambdaFunctionARN = input.LambdaFunctionARN
	}

	if input.AccountDefaultForOperations != nil {
		ops := make([]string, len(input.AccountDefaultForOperations))
		copy(ops, input.AccountDefaultForOperations)
		cp.AccountDefaultForOperations = ops
	}

	cp.LastModifiedAt = time.Now()

	return nil
}

// DeleteCertificateProvider deletes a certificate provider by name.
func (b *InMemoryBackend) DeleteCertificateProvider(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.certificateProviders.Has(name) {
		return fmt.Errorf("%w: %s", ErrCertificateProviderNotFound, name)
	}

	b.certificateProviders.Delete(name)

	return nil
}

// CACertificate represents an IoT CA certificate.
type CACertificate struct {
	CertificateARN   string  `json:"certificateArn"`
	CertificateID    string  `json:"certificateId"`
	Status           string  `json:"status"`
	CertificatePem   string  `json:"certificatePem,omitempty"`
	OwnedBy          string  `json:"ownedBy,omitempty"`
	AutoRegistration string  `json:"autoRegistrationStatus,omitempty"`
	CreationDate     float64 `json:"creationDate,omitempty"`
	LastModifiedDate float64 `json:"lastModifiedDate,omitempty"`
}

func cloneCACert(c *CACertificate) *CACertificate {
	cp := *c

	return &cp
}

func (b *InMemoryBackend) caCertARN(id string) string {
	return arn.Build("iot", b.region, b.accountID, fmt.Sprintf("cacert/%s", id))
}

func (b *InMemoryBackend) RegisterCACertificate(pem, status string) (*CACertificate, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := uuid.NewString()[:12]
	now := float64(time.Now().Unix())
	ca := &CACertificate{
		CertificateID:    id,
		CertificateARN:   b.caCertARN(id),
		Status:           status,
		CertificatePem:   pem,
		CreationDate:     now,
		LastModifiedDate: now,
		OwnedBy:          b.accountID,
		AutoRegistration: "DISABLE",
	}
	if ca.Status == "" {
		ca.Status = "ACTIVE"
	}
	b.caCertificates.Put(ca)

	return cloneCACert(ca), nil
}

func (b *InMemoryBackend) DescribeCACertificate(id string) (*CACertificate, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	ca, ok := b.caCertificates.Get(id)
	if !ok {
		return nil, fmt.Errorf("CA certificate %q not found: %w", id, ErrResourceNotFound)
	}

	return cloneCACert(ca), nil
}

func (b *InMemoryBackend) ListCACertificates() []*CACertificate {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]*CACertificate, 0, b.caCertificates.Len())
	for _, v := range b.caCertificates.Snapshot() {
		out = append(out, cloneCACert(v))
	}

	return out
}

func (b *InMemoryBackend) UpdateCACertificate(id, status string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	ca, ok := b.caCertificates.Get(id)
	if !ok {
		return fmt.Errorf("CA certificate %q not found: %w", id, ErrResourceNotFound)
	}
	if status != "" {
		ca.Status = status
	}
	ca.LastModifiedDate = float64(time.Now().Unix())

	return nil
}

func (b *InMemoryBackend) DeleteCACertificate(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.caCertificates.Has(id) {
		return fmt.Errorf("CA certificate %q not found: %w", id, ErrResourceNotFound)
	}
	b.caCertificates.Delete(id)

	return nil
}

func (b *InMemoryBackend) ListCertificatesByCA(caID string) []*Certificate {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var out []*Certificate
	for _, v := range b.certificates.Snapshot() {
		cert := v
		if cert.CACertificateID == caID {
			cp := *cert
			out = append(out, &cp)
		}
	}

	return out
}

func (b *InMemoryBackend) CancelCertificateTransfer(certID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.certificateTransfers, certID)

	return nil
}

// CreateProvisioningClaim returns a fake cert/key pair for the given template.
func (b *InMemoryBackend) CreateProvisioningClaim(templateName string) (string, string, string, error) {
	b.mu.RLock()
	_, ok := b.provTemplates.Get(templateName)
	b.mu.RUnlock()

	if !ok {
		return "", "", "", fmt.Errorf("provisioning template %q not found: %w", templateName, ErrResourceNotFound)
	}

	return fakePEM, fakePublicKey, fakePrivateKey, nil
}

// CreateKeysAndCertificate creates a new certificate with generated fake keys.
func (b *InMemoryBackend) CreateKeysAndCertificate(setAsActive bool) (*Certificate, string, string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	status := certStatusInactive
	if setAsActive {
		status = certStatusActive
	}
	cert := b.newCertificate(fakePEM, status)
	b.certificates.Put(cert)
	cp := *cert

	return &cp, fakePublicKey, fakePrivateKey, nil
}

// TransferCertificate initiates a certificate transfer to another account.
func (b *InMemoryBackend) TransferCertificate(certID, targetAccount string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	cert, ok := b.certificates.Get(certID)
	if !ok {
		return fmt.Errorf("certificate %q not found: %w", certID, ErrCertificateNotFound)
	}
	cert.Status = certStatusPendingTransfer
	cert.LastModifiedAt = time.Now()
	b.certificateTransfers[certID] = targetAccount

	return nil
}

// RejectCertificateTransfer rejects a pending certificate transfer.
func (b *InMemoryBackend) RejectCertificateTransfer(certID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	cert, ok := b.certificates.Get(certID)
	if !ok {
		return fmt.Errorf("certificate %q not found: %w", certID, ErrCertificateNotFound)
	}
	cert.Status = certStatusInactive
	cert.LastModifiedAt = time.Now()
	delete(b.certificateTransfers, certID)

	return nil
}

const fakePublicKey = `-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA2a2rwplBQLzHPZe5TNJG
pECOsDSZBBFKpbD+mBBpMBWMdQwCgYIKoZIzj0EAwIDSQADRgIhANXBFB+AAAAA
-----END PUBLIC KEY-----`

//nolint:gosec // fakePrivateKey is a test-only placeholder, not a real credential
const fakePrivateKey = `-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEA2a2rwplBQLzHPZe5TNJGpECOsDSZBBFKpbD+mBBpMBWMdQwC
gYIKoZIzj0EAwIDSQADRgIhANXBFB+AAAAA
-----END RSA PRIVATE KEY-----`

// OutgoingCertificate describes a certificate pending transfer to another
// AWS account.
type OutgoingCertificate struct {
	CertificateARN  string  `json:"certificateArn"`
	CertificateID   string  `json:"certificateId"`
	TransferredTo   string  `json:"transferredTo,omitempty"`
	TransferMessage string  `json:"transferMessage,omitempty"`
	CreationDate    float64 `json:"creationDate,omitempty"`
	TransferDate    float64 `json:"transferDate,omitempty"`
}

// ListOutgoingCertificates lists certificates whose status is
// PENDING_TRANSFER, derived from real, stored certificate state.
func (b *InMemoryBackend) ListOutgoingCertificates() []*OutgoingCertificate {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]*OutgoingCertificate, 0)

	for _, v := range b.certificates.Snapshot() {
		cert := v
		if cert.Status != certStatusPendingTransfer {
			continue
		}

		out = append(out, &OutgoingCertificate{
			CertificateARN: cert.ARN,
			CertificateID:  cert.CertificateID,
			CreationDate:   float64(cert.CreatedAt.Unix()),
			TransferDate:   float64(cert.LastModifiedAt.Unix()),
		})
	}

	return out
}
