package sagemaker

import (
	"context"
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrDomainNotFound is returned when a domain does not exist.
	ErrDomainNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrDomainAlreadyExists is returned when a domain already exists.
	ErrDomainAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
)

// Domain represents a SageMaker Studio domain.
type Domain struct {
	CreationTime     time.Time         `json:"CreationTime"`
	LastModifiedTime time.Time         `json:"LastModifiedTime"`
	Tags             map[string]string `json:"Tags,omitempty"`
	DomainID         string            `json:"DomainId"`
	DomainArn        string            `json:"DomainArn"`
	DomainName       string            `json:"DomainName"`
	Status           string            `json:"Status"`
	URL              string            `json:"Url,omitempty"`
	AuthMode         string            `json:"AuthMode,omitempty"`
}

func cloneDomain(d *Domain) *Domain {
	cp := *d
	cp.Tags = maps.Clone(d.Tags)

	return &cp
}

// CreateDomain creates a new SageMaker Studio domain.
func (b *InMemoryBackend) CreateDomain(
	ctx context.Context,
	name, authMode string,
	tags map[string]string,
) (*Domain, error) {
	b.mu.Lock("CreateDomain")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	for _, d := range b.domainsStore(region).All() {
		if d.DomainName == name {
			return nil, fmt.Errorf("%w: domain %s already exists", ErrDomainAlreadyExists, name)
		}
	}

	id := fmt.Sprintf("d-%s", generateID())
	domainArn := arn.Build("sagemaker", region, b.accountID, "domain/"+id)
	now := time.Now()

	d := &Domain{
		DomainID:         id,
		DomainArn:        domainArn,
		DomainName:       name,
		AuthMode:         authMode,
		Status:           statusInService,
		URL:              fmt.Sprintf("https://%s.studio.%s.sagemaker.aws", id, region),
		CreationTime:     now,
		LastModifiedTime: now,
		Tags:             mergeTags(nil, tags),
	}
	b.domainsStore(region).Put(d)

	return cloneDomain(d), nil
}

// DescribeDomain returns a domain by ID or name.
func (b *InMemoryBackend) DescribeDomain(ctx context.Context, idOrName string) (*Domain, error) {
	b.mu.RLock("DescribeDomain")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	if d, ok := b.domainsStoreRO(region).Get(idOrName); ok {
		return cloneDomain(d), nil
	}

	for _, d := range b.domainsStoreRO(region).All() {
		if d.DomainName == idOrName {
			return cloneDomain(d), nil
		}
	}

	return nil, fmt.Errorf("%w: domain %q not found", ErrDomainNotFound, idOrName)
}

// ListDomains returns all domains sorted by name.
func (b *InMemoryBackend) ListDomains(ctx context.Context, nextToken string) ([]*Domain, string) {
	b.mu.RLock("ListDomains")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListPaged(b.domainsStoreRO(region), nextToken, cloneDomain,
		func(a, b *Domain) bool { return a.DomainName < b.DomainName })
}

// DeleteDomain deletes a domain by ID or name.
func (b *InMemoryBackend) DeleteDomain(ctx context.Context, idOrName string) error {
	b.mu.Lock("DeleteDomain")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.domainsStore(region)

	for _, d := range store.All() {
		if d.DomainID == idOrName || d.DomainName == idOrName {
			store.Delete(d.DomainID)

			return nil
		}
	}

	return fmt.Errorf("%w: domain %q not found", ErrDomainNotFound, idOrName)
}

// UpdateDomain updates a domain's status.
func (b *InMemoryBackend) UpdateDomain(ctx context.Context, idOrName string) (*Domain, error) {
	b.mu.Lock("UpdateDomain")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	for _, d := range b.domainsStore(region).All() {
		if d.DomainID == idOrName || d.DomainName == idOrName {
			d.LastModifiedTime = time.Now()

			return cloneDomain(d), nil
		}
	}

	return nil, fmt.Errorf("%w: domain %q not found", ErrDomainNotFound, idOrName)
}
