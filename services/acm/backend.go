package acm

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

var (
	ErrCertNotFound     = errors.New("ResourceNotFoundException")
	ErrInvalidParameter = errors.New("ValidationException")
)

// Certificate represents an ACM certificate.
type Certificate struct {
	CreatedAt  time.Time `json:"createdAt"`
	ARN        string    `json:"arn"`
	DomainName string    `json:"domainName"`
	Status     string    `json:"status"`
	Type       string    `json:"type"`
}

// InMemoryBackend is the in-memory store for ACM certificates.
type InMemoryBackend struct {
	certs     map[string]*Certificate
	mu        *lockmetrics.RWMutex
	accountID string
	region    string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		certs:     make(map[string]*Certificate),
		accountID: accountID,
		region:    region,
		mu:        lockmetrics.New("acm"),
	}
}

// RequestCertificate creates a new certificate for the given domain.
func (b *InMemoryBackend) RequestCertificate(domainName, certType string) (*Certificate, error) {
	if domainName == "" {
		return nil, fmt.Errorf("%w: DomainName is required", ErrInvalidParameter)
	}

	b.mu.Lock("RequestCertificate")
	defer b.mu.Unlock()

	id := fmt.Sprintf("%x", time.Now().UnixNano())
	certARN := arn.Build("acm", b.region, b.accountID, "certificate/"+id)

	if certType == "" {
		certType = "AMAZON_ISSUED"
	}

	cert := &Certificate{
		ARN:        certARN,
		DomainName: domainName,
		Status:     "ISSUED",
		Type:       certType,
		CreatedAt:  time.Now().UTC(),
	}
	b.certs[certARN] = cert

	cp := *cert

	return &cp, nil
}

// DescribeCertificate returns the certificate with the given ARN.
func (b *InMemoryBackend) DescribeCertificate(arn string) (*Certificate, error) {
	b.mu.RLock("DescribeCertificate")
	defer b.mu.RUnlock()

	cert, exists := b.certs[arn]
	if !exists {
		return nil, fmt.Errorf("%w: certificate %s not found", ErrCertNotFound, arn)
	}

	cp := *cert

	return &cp, nil
}

// ListCertificates returns a paginated list of certificates sorted by ARN.
func (b *InMemoryBackend) ListCertificates(nextToken string, maxItems int) page.Page[Certificate] {
	b.mu.RLock("ListCertificates")
	defer b.mu.RUnlock()

	certs := make([]Certificate, 0, len(b.certs))
	for _, c := range b.certs {
		certs = append(certs, *c)
	}

	sort.Slice(certs, func(i, j int) bool { return certs[i].ARN < certs[j].ARN })

	return page.New(certs, nextToken, maxItems, acmDefaultMaxItems)
}

const acmDefaultMaxItems = 100

// DeleteCertificate removes the certificate with the given ARN.
func (b *InMemoryBackend) DeleteCertificate(arn string) error {
	b.mu.Lock("DeleteCertificate")
	defer b.mu.Unlock()

	if _, exists := b.certs[arn]; !exists {
		return fmt.Errorf("%w: certificate %s not found", ErrCertNotFound, arn)
	}

	delete(b.certs, arn)

	return nil
}
