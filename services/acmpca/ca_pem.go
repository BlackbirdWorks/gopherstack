package acmpca

import (
	"context"
	"strings"
)

const arnRegionIndex = 3

// CertificatePEMForARN returns the PEM certificate of a CA followed by its chain, resolving the
// region from the ARN so in-process consumers need no request context.
func (b *InMemoryBackend) CertificatePEMForARN(caARN string) ([]byte, error) {
	ctx := context.Background()

	if parts := strings.Split(caARN, ":"); len(parts) > arnRegionIndex && parts[arnRegionIndex] != "" {
		ctx = context.WithValue(ctx, regionContextKey{}, parts[arnRegionIndex])
	}

	body, chain, err := b.GetCertificateAuthorityCertificate(ctx, caARN)
	if err != nil {
		return nil, err
	}

	return []byte(strings.TrimSpace(body) + "\n" + chain), nil
}
