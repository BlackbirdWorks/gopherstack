package mediaconvert

import (
	"fmt"
	"strings"
)

// AssociateCertificate registers an ACM certificate ARN with this backend.
func (b *InMemoryBackend) AssociateCertificate(certARN string) error {
	b.mu.Lock("AssociateCertificate")
	defer b.mu.Unlock()

	if certARN == "" {
		return fmt.Errorf("%w: arn is required", ErrValidation)
	}

	if !strings.HasPrefix(certARN, "arn:") {
		return fmt.Errorf("%w: arn must start with 'arn:'", ErrValidation)
	}

	if _, ok := b.certificates[certARN]; ok {
		return fmt.Errorf("%w: certificate %s already associated", ErrAlreadyExists, certARN)
	}
	b.certificates[certARN] = struct{}{}

	return nil
}

// DisassociateCertificate removes an ACM certificate ARN association.
func (b *InMemoryBackend) DisassociateCertificate(certARN string) error {
	b.mu.Lock("DisassociateCertificate")
	defer b.mu.Unlock()

	if certARN == "" {
		return fmt.Errorf("%w: arn is required", ErrValidation)
	}

	if !strings.HasPrefix(certARN, "arn:") {
		return fmt.Errorf("%w: arn must start with 'arn:'", ErrValidation)
	}

	if _, ok := b.certificates[certARN]; !ok {
		return fmt.Errorf("%w: certificate %s not found", ErrNotFound, certARN)
	}
	delete(b.certificates, certARN)

	return nil
}
