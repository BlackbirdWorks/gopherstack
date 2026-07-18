package acmpca

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

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

	ca, ok := b.caGet(region, caARN)
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
		region:    region,
	}

	b.certPut(cert)
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

	cert, ok := b.certGet(region, certARN)
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

	ca, ok := b.caGet(region, caARN)
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

	cert, ok := b.certGet(region, certARN)
	if !ok {
		return fmt.Errorf("%w: certificate with serial %s not found", ErrCertNotFound, serial)
	}

	cert.Status = certStatusRevoked
	now := time.Now().UTC()
	cert.RevokedAt = &now
	cert.RevocationReason = revocationReason

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

	certsForCA := b.certsForCA(region, caARN)
	certs := make([]IssuedCertificate, 0, len(certsForCA))
	for _, c := range certsForCA {
		certs = append(certs, *c)
	}

	sort.Slice(certs, func(i, j int) bool { return certs[i].ARN < certs[j].ARN })

	return page.New(certs, nextToken, maxItems, defaultMaxItems)
}
