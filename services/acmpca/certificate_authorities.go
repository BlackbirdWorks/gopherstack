package acmpca

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	cryptorand "crypto/rand"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

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
		region:                            region,
	}

	b.caPut(ca)

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

	ca, ok := b.caGet(region, caARN)
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

	ca, ok := b.caGet(region, caARN)
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

	var cas []CertificateAuthority
	func() {
		b.mu.RLock("ListCertificateAuthorities")
		defer b.mu.RUnlock()

		casInRegion := b.casInRegion(region)
		cas = make([]CertificateAuthority, 0, len(casInRegion))
		for _, ca := range casInRegion {
			cas = append(cas, copyCA(ca))
		}
	}()

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

	ca, ok := b.caGet(region, caARN)
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

	// AWS defaults PermanentDeletionTimeInDays to 30 when unset; DescribeCertificateAuthority
	// reports the remaining restoration window via RestorableUntil.
	days := permanentDeletionDays
	if days == 0 {
		days = defaultPermanentDeletionDays
	}

	ca.RestorableUntil = time.Now().UTC().AddDate(0, 0, int(days))
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

	ca, ok := b.caGet(region, caARN)
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

	ca, ok := b.caGet(region, caARN)
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

	ca, ok := b.caGet(region, caARN)
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

	ca, ok := b.caGet(region, caARN)
	if !ok {
		return "", "", fmt.Errorf("%w: CA %s not found", ErrCANotFound, caARN)
	}

	if ca.CertificateBody == "" {
		return "", "", fmt.Errorf("%w: CA %s has no certificate imported", ErrCANotFound, caARN)
	}

	return ca.CertificateBody, ca.CertificateChain, nil
}

// RestoreCertificateAuthority restores a deleted CA into the DISABLED state.
func (b *InMemoryBackend) RestoreCertificateAuthority(ctx context.Context, caARN string) error {
	if err := validateRequiredParameter(caARN, "CertificateAuthorityArn"); err != nil {
		return err
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("RestoreCertificateAuthority")
	defer b.mu.Unlock()

	ca, ok := b.caGet(region, caARN)
	if !ok {
		return fmt.Errorf("%w: CA %s not found", ErrCANotFound, caARN)
	}

	if ca.Status != caStatusDeleted {
		return fmt.Errorf("%w: CA %s is not DELETED", ErrInvalidState, caARN)
	}

	ca.Status = caStatusDisabled
	ca.RestorableUntil = time.Time{}

	return nil
}

// copyCA returns a shallow copy of the CertificateAuthority, excluding the private key.
func copyCA(ca *CertificateAuthority) CertificateAuthority {
	cp := *ca
	cp.privKey = nil

	return cp
}
