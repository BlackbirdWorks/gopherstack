package acm

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrCertNotFound     = errors.New("ResourceNotFoundException")
	ErrInvalidParameter = errors.New("ValidationException")
)

// Certificate represents an ACM certificate.
type Certificate struct {
	ARN        string
	DomainName string
	Status     string
	Type       string
	CreatedAt  time.Time
}

// InMemoryBackend is the in-memory store for ACM certificates.
type InMemoryBackend struct {
	certs     map[string]*Certificate
	accountID string
	region    string
	mu        sync.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		certs:     make(map[string]*Certificate),
		accountID: accountID,
		region:    region,
	}
}

// RequestCertificate creates a new certificate for the given domain.
func (b *InMemoryBackend) RequestCertificate(domainName, certType string) (*Certificate, error) {
	if domainName == "" {
		return nil, fmt.Errorf("%w: DomainName is required", ErrInvalidParameter)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	id := fmt.Sprintf("%x", time.Now().UnixNano())
	arn := fmt.Sprintf("arn:aws:acm:%s:%s:certificate/%s", b.region, b.accountID, id)

	if certType == "" {
		certType = "AMAZON_ISSUED"
	}

	cert := &Certificate{
		ARN:        arn,
		DomainName: domainName,
		Status:     "ISSUED",
		Type:       certType,
		CreatedAt:  time.Now().UTC(),
	}
	b.certs[arn] = cert

	cp := *cert

	return &cp, nil
}

// DescribeCertificate returns the certificate with the given ARN.
func (b *InMemoryBackend) DescribeCertificate(arn string) (*Certificate, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	cert, exists := b.certs[arn]
	if !exists {
		return nil, fmt.Errorf("%w: certificate %s not found", ErrCertNotFound, arn)
	}

	cp := *cert

	return &cp, nil
}

// ListCertificates returns all certificates.
func (b *InMemoryBackend) ListCertificates() []Certificate {
	b.mu.RLock()
	defer b.mu.RUnlock()

	certs := make([]Certificate, 0, len(b.certs))
	for _, c := range b.certs {
		certs = append(certs, *c)
	}

	return certs
}

// DeleteCertificate removes the certificate with the given ARN.
func (b *InMemoryBackend) DeleteCertificate(arn string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.certs[arn]; !exists {
		return fmt.Errorf("%w: certificate %s not found", ErrCertNotFound, arn)
	}

	delete(b.certs, arn)

	return nil
}
