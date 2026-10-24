package sesv2

import (
	"fmt"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const identityCertificateStatusActive = "ACTIVE"

// IdentityCertificate is an S/MIME certificate association on an email identity.
type IdentityCertificate struct {
	CertificateARN string `json:"certificateArn"`
	FromAddress    string `json:"fromAddress"`
	Status         string `json:"status"`
}

func validateCertificateARN(certARN string) error {
	parts := strings.SplitN(certARN, ":", 6) //nolint:mnd // arn:partition:service:region:account:resource
	if len(parts) != 6 || parts[0] != "arn" || parts[2] != "acm" || !strings.HasPrefix(parts[5], "certificate/") ||
		len(parts[5]) == len("certificate/") {
		return fmt.Errorf("%w: CertificateArn must be an ACM certificate ARN", ErrInvalidInput)
	}

	return nil
}

func validateCertificateFromAddress(addr string) error {
	local, domain, ok := strings.Cut(addr, "@")
	if !ok || local == "" || !validDomainName(domain) {
		return fmt.Errorf("%w: FromAddress %q is not a valid email address", ErrInvalidInput, addr)
	}

	return nil
}

func certificateIndex(certs []IdentityCertificate, fromAddress string) int {
	return slices.IndexFunc(certs, func(c IdentityCertificate) bool {
		return strs.Equal(c.FromAddress, fromAddress)
	})
}

// AssociateEmailIdentityCertificate links an ACM certificate to a sending address of an identity.
func (b *InMemoryBackend) AssociateEmailIdentityCertificate(identity, fromAddress, certARN string) error {
	if err := validateCertificateARN(certARN); err != nil {
		return err
	}

	if err := validateCertificateFromAddress(fromAddress); err != nil {
		return err
	}

	b.mu.Lock("AssociateEmailIdentityCertificate")
	defer b.mu.Unlock()

	ei, ok := b.identities.Get(identity)
	if !ok {
		return identityMissing(identity)
	}

	cert := IdentityCertificate{
		CertificateARN: certARN,
		FromAddress:    fromAddress,
		Status:         identityCertificateStatusActive,
	}

	if i := certificateIndex(ei.Certificates, fromAddress); i >= 0 {
		ei.Certificates[i] = cert

		return nil
	}

	ei.Certificates = append(ei.Certificates, cert)

	return nil
}

// DisassociateEmailIdentityCertificate removes the certificate association for a sending address.
func (b *InMemoryBackend) DisassociateEmailIdentityCertificate(identity, fromAddress string) error {
	b.mu.Lock("DisassociateEmailIdentityCertificate")
	defer b.mu.Unlock()

	ei, ok := b.identities.Get(identity)
	if !ok {
		return identityMissing(identity)
	}

	i := certificateIndex(ei.Certificates, fromAddress)
	if i < 0 {
		return fmt.Errorf("%w: no certificate is associated with %s", ErrNotFound, fromAddress)
	}

	ei.Certificates = slices.Delete(ei.Certificates, i, i+1)

	return nil
}

// ListEmailIdentityCertificates returns a page of certificate associations, ordered by FromAddress.
func (b *InMemoryBackend) ListEmailIdentityCertificates(
	identity, nextToken string,
	pageSize int,
) (page.Page[IdentityCertificate], error) {
	b.mu.RLock("ListEmailIdentityCertificates")
	defer b.mu.RUnlock()

	ei, ok := b.identities.Get(identity)
	if !ok {
		return page.Page[IdentityCertificate]{}, identityMissing(identity)
	}

	certs := slices.Clone(ei.Certificates)
	slices.SortFunc(certs, func(a, c IdentityCertificate) int {
		return strings.Compare(a.FromAddress, c.FromAddress)
	})

	return page.New(certs, nextToken, pageSize, sesv2DefaultMaxItems), nil
}
