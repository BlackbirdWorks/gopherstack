package transfer

import (
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"
)

// DeleteCertificate removes a certificate by ID.
func (b *InMemoryBackend) DeleteCertificate(certificateID string) error {
	b.mu.Lock("DeleteCertificate")
	defer b.mu.Unlock()

	if !b.certificates.Has(certificateID) {
		return fmt.Errorf("%w: certificate %s not found", ErrCertificateNotFound, certificateID)
	}

	b.certificates.Delete(certificateID)

	return nil
}

// ImportCertificate imports a certificate.
// notBefore and notAfter are optional; zero values use defaults (now and +1 year).
// If body is a valid PEM certificate, NotBefore/NotAfter are extracted from it.
func (b *InMemoryBackend) ImportCertificate(
	usage, body, description string,
	notBefore, notAfter time.Time,
	tags map[string]string,
) (*Certificate, error) {
	b.mu.Lock("ImportCertificate")
	defer b.mu.Unlock()

	// Try to parse PEM if provided.
	if body != "" {
		block, _ := pem.Decode([]byte(body))
		if block == nil {
			return nil, fmt.Errorf(
				"%w: certificate body is not a valid PEM block",
				ErrValidation,
			)
		}

		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf(
				"%w: failed to parse certificate: %w",
				ErrValidation,
				err,
			)
		}

		// Override notBefore/notAfter from certificate.
		notBefore = cert.NotBefore
		notAfter = cert.NotAfter
	}

	certID := "cert-" + uuid.NewString()[:20]

	now := time.Now()
	if notBefore.IsZero() {
		notBefore = now
	}

	if notAfter.IsZero() {
		notAfter = now.AddDate(1, 0, 0)
	}

	merged := make(map[string]string, len(tags))
	maps.Copy(merged, tags)

	c := &Certificate{
		CertificateID: certID,
		Usage:         usage,
		Body:          body,
		Description:   description,
		Status:        agreementStatusActive,
		NotBeforeDate: notBefore,
		NotAfterDate:  notAfter,
		CreatedAt:     now,
		Tags:          merged,
		AccountID:     b.accountID,
		Region:        b.region,
	}
	b.certificates.Put(c)
	b.initTagsStore(certificateARN(b.accountID, b.region, certID), merged)

	cp := *c
	cp.Tags = make(map[string]string, len(merged))
	maps.Copy(cp.Tags, merged)

	return &cp, nil
}

// DescribeCertificate returns a certificate by ID.
func (b *InMemoryBackend) DescribeCertificate(certificateID string) (*Certificate, error) {
	b.mu.RLock("DescribeCertificate")
	defer b.mu.RUnlock()

	c, ok := b.certificates.Get(certificateID)
	if !ok {
		return nil, fmt.Errorf(
			"%w: certificate %s not found",
			ErrCertificateNotFound,
			certificateID,
		)
	}

	cp := *c
	cp.Tags = make(map[string]string, len(c.Tags))
	maps.Copy(cp.Tags, c.Tags)

	return &cp, nil
}

// ListCertificates returns all certificates sorted by certificateID.
func (b *InMemoryBackend) ListCertificates() []*Certificate {
	b.mu.RLock("ListCertificates")
	defer b.mu.RUnlock()

	all := b.certificates.All()
	out := make([]*Certificate, 0, len(all))

	for _, c := range all {
		cp := *c
		cp.Tags = make(map[string]string, len(c.Tags))
		maps.Copy(cp.Tags, c.Tags)
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].CertificateID < out[j].CertificateID
	})

	return out
}

// UpdateCertificate updates mutable fields on a certificate.
func (b *InMemoryBackend) UpdateCertificate(
	certificateID, description string,
) (*Certificate, error) {
	b.mu.Lock("UpdateCertificate")
	defer b.mu.Unlock()

	c, ok := b.certificates.Get(certificateID)
	if !ok {
		return nil, fmt.Errorf(
			"%w: certificate %s not found",
			ErrCertificateNotFound,
			certificateID,
		)
	}

	if description != "" {
		c.Description = description
	}

	cp := *c
	cp.Tags = make(map[string]string, len(c.Tags))
	maps.Copy(cp.Tags, c.Tags)

	return &cp, nil
}

// AddCertificateInternal seeds a certificate for testing purposes.
func (b *InMemoryBackend) AddCertificateInternal(certID string) {
	b.mu.Lock("AddCertificateInternal")
	defer b.mu.Unlock()

	b.certificates.Put(&Certificate{
		CertificateID: certID,
		CreatedAt:     time.Now(),
		Tags:          make(map[string]string),
		AccountID:     b.accountID,
		Region:        b.region,
	})
}
