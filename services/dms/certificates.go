package dms

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// ImportCertificate creates a certificate record.
func (b *InMemoryBackend) ImportCertificate(
	ctx context.Context, identifier, certPem string, kv map[string]string,
) (*Certificate, error) {
	b.mu.Lock("ImportCertificate")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if b.certificates.Has(regionKey(region, identifier)) {
		return nil, fmt.Errorf("%w: certificate %s already exists", ErrAlreadyExists, identifier)
	}

	certARN := arn.Build("dms", region, b.accountID, "certificate:"+identifier)
	t := tags.New("dms.certificate." + identifier + ".tags")

	if len(kv) > 0 {
		t.Merge(kv)
	}

	cert := &Certificate{
		CertificateIdentifier: identifier,
		CertificateArn:        certARN,
		CertificatePem:        certPem,
		AccountID:             b.accountID,
		Region:                region,
		Tags:                  t,
	}
	b.certificates.Put(cert)
	cp := *cert

	return &cp, nil
}

// DeleteCertificate deletes a certificate by identifier or ARN.
func (b *InMemoryBackend) DeleteCertificate(ctx context.Context, identifierOrArn string) (*Certificate, error) {
	b.mu.Lock("DeleteCertificate")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if cert, ok := b.certificates.Get(regionKey(region, identifierOrArn)); ok {
		cp := *cert
		cert.Tags.Close()
		b.certificates.Delete(regionKey(region, identifierOrArn))

		return &cp, nil
	}

	if cert, ok := lookupUnique(b.certificatesByARN, regionKey(region, identifierOrArn)); ok {
		cp := *cert
		cert.Tags.Close()
		b.certificates.Delete(regionKey(region, cert.CertificateIdentifier))

		return &cp, nil
	}

	return nil, fmt.Errorf("%w: certificate %s not found", ErrNotFound, identifierOrArn)
}

// DescribeCertificates returns all certificates.
func (b *InMemoryBackend) DescribeCertificates(ctx context.Context) ([]*Certificate, error) {
	b.mu.RLock("DescribeCertificates")
	defer b.mu.RUnlock()

	items := b.certificatesByRegion.Get(getRegion(ctx, b.region))
	list := make([]*Certificate, 0, len(items))
	for _, cert := range items {
		cp := *cert
		list = append(list, &cp)
	}

	return list, nil
}
