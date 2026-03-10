package cloudfront

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"strings"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("NoSuchDistribution", awserr.ErrNotFound)
	// ErrOAINotFound is returned when a requested OAI does not exist.
	ErrOAINotFound = awserr.New("NoSuchCloudFrontOriginAccessIdentity", awserr.ErrNotFound)
)

const (
	// idChars are the uppercase alphanumeric characters used for CloudFront IDs.
	idChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	// idLen is the length of generated CloudFront IDs.
	idLen = 14
)

// generateID generates a random uppercase alphanumeric ID of length 14.
func generateID() string {
	b := make([]byte, idLen)
	for i := range b {
		b[i] = idChars[rand.IntN(len(idChars))] //nolint:gosec // mock service, not security-sensitive
	}

	return string(b)
}

// Distribution represents a CloudFront distribution.
type Distribution struct {
	Tags            map[string]string
	ID              string
	ARN             string
	DomainName      string
	Status          string
	ETag            string
	CallerReference string
	Comment         string
	RawConfig       []byte // raw DistributionConfig XML from request
	Enabled         bool
}

// OriginAccessIdentity represents a CloudFront Origin Access Identity.
type OriginAccessIdentity struct {
	ID                string
	ARN               string
	S3CanonicalUserID string
	ETag              string
	CallerReference   string
	Comment           string
}

// InMemoryBackend stores CloudFront resources in memory.
type InMemoryBackend struct {
	distributions map[string]*Distribution
	oais          map[string]*OriginAccessIdentity
	mu            *lockmetrics.RWMutex
	accountID     string
	region        string
}

// NewInMemoryBackend creates a new in-memory CloudFront backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		distributions: make(map[string]*Distribution),
		oais:          make(map[string]*OriginAccessIdentity),
		mu:            lockmetrics.New("cloudfront"),
		accountID:     accountID,
		region:        region,
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// distributionARN builds an ARN for a CloudFront distribution.
// CloudFront ARNs have no region component.
func (b *InMemoryBackend) distributionARN(id string) string {
	return fmt.Sprintf("arn:aws:cloudfront::%s:distribution/%s", b.accountID, id)
}

// oaiARN builds an ARN for an Origin Access Identity.
func (b *InMemoryBackend) oaiARN(id string) string {
	return fmt.Sprintf("arn:aws:cloudfront::%s:origin-access-identity/cloudfront/%s", b.accountID, id)
}

// CreateDistribution creates a new CloudFront distribution.
func (b *InMemoryBackend) CreateDistribution(
	callerRef, comment string,
	enabled bool,
	rawConfig []byte,
) (*Distribution, error) {
	b.mu.Lock("CreateDistribution")
	defer b.mu.Unlock()

	id := generateID()
	d := &Distribution{
		ID:              id,
		ARN:             b.distributionARN(id),
		DomainName:      strings.ToLower(id) + ".cloudfront.net",
		Status:          "Deployed",
		ETag:            uuid.NewString(),
		CallerReference: callerRef,
		Comment:         comment,
		Enabled:         enabled,
		RawConfig:       rawConfig,
		Tags:            make(map[string]string),
	}
	b.distributions[id] = d
	cp := b.copyDistribution(d)

	return cp, nil
}

// GetDistribution returns a distribution by ID.
func (b *InMemoryBackend) GetDistribution(id string) (*Distribution, error) {
	b.mu.RLock("GetDistribution")
	defer b.mu.RUnlock()

	d, ok := b.distributions[id]
	if !ok {
		return nil, fmt.Errorf("%w: distribution %s not found", ErrNotFound, id)
	}

	return b.copyDistribution(d), nil
}

// UpdateDistribution updates an existing distribution's config.
func (b *InMemoryBackend) UpdateDistribution(
	id, comment string,
	enabled bool,
	rawConfig []byte,
) (*Distribution, error) {
	b.mu.Lock("UpdateDistribution")
	defer b.mu.Unlock()

	d, ok := b.distributions[id]
	if !ok {
		return nil, fmt.Errorf("%w: distribution %s not found", ErrNotFound, id)
	}

	d.Comment = comment
	d.Enabled = enabled
	d.RawConfig = rawConfig
	d.ETag = uuid.NewString()
	cp := b.copyDistribution(d)

	return cp, nil
}

// DeleteDistribution deletes a distribution by ID.
func (b *InMemoryBackend) DeleteDistribution(id string) error {
	b.mu.Lock("DeleteDistribution")
	defer b.mu.Unlock()

	if _, ok := b.distributions[id]; !ok {
		return fmt.Errorf("%w: distribution %s not found", ErrNotFound, id)
	}
	delete(b.distributions, id)

	return nil
}

// ListDistributions returns all distributions.
func (b *InMemoryBackend) ListDistributions() []*Distribution {
	b.mu.RLock("ListDistributions")
	defer b.mu.RUnlock()

	list := make([]*Distribution, 0, len(b.distributions))
	for _, d := range b.distributions {
		list = append(list, b.copyDistribution(d))
	}

	return list
}

// CreateOAI creates a new Origin Access Identity.
func (b *InMemoryBackend) CreateOAI(callerRef, comment string) (*OriginAccessIdentity, error) {
	b.mu.Lock("CreateOAI")
	defer b.mu.Unlock()

	id := generateID()
	oai := &OriginAccessIdentity{
		ID:                id,
		ARN:               b.oaiARN(id),
		S3CanonicalUserID: uuid.NewString(),
		ETag:              uuid.NewString(),
		CallerReference:   callerRef,
		Comment:           comment,
	}
	b.oais[id] = oai
	cp := *oai

	return &cp, nil
}

// GetOAI returns an OAI by ID.
func (b *InMemoryBackend) GetOAI(id string) (*OriginAccessIdentity, error) {
	b.mu.RLock("GetOAI")
	defer b.mu.RUnlock()

	oai, ok := b.oais[id]
	if !ok {
		return nil, fmt.Errorf("%w: OAI %s not found", ErrOAINotFound, id)
	}
	cp := *oai

	return &cp, nil
}

// DeleteOAI deletes an OAI by ID.
func (b *InMemoryBackend) DeleteOAI(id string) error {
	b.mu.Lock("DeleteOAI")
	defer b.mu.Unlock()

	if _, ok := b.oais[id]; !ok {
		return fmt.Errorf("%w: OAI %s not found", ErrOAINotFound, id)
	}
	delete(b.oais, id)

	return nil
}

// ListOAIs returns all OAIs.
func (b *InMemoryBackend) ListOAIs() []*OriginAccessIdentity {
	b.mu.RLock("ListOAIs")
	defer b.mu.RUnlock()

	list := make([]*OriginAccessIdentity, 0, len(b.oais))
	for _, oai := range b.oais {
		cp := *oai
		list = append(list, &cp)
	}

	return list
}

// TagResource adds or updates tags on a resource by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, kv map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	for _, d := range b.distributions {
		if d.ARN == resourceARN {
			maps.Copy(d.Tags, kv)

			return nil
		}
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

// UntagResource removes tags from a resource by ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	for _, d := range b.distributions {
		if d.ARN == resourceARN {
			for _, k := range keys {
				delete(d.Tags, k)
			}

			return nil
		}
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

// ListTags returns the tags for a resource by ARN.
func (b *InMemoryBackend) ListTags(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTags")
	defer b.mu.RUnlock()

	for _, d := range b.distributions {
		if d.ARN == resourceARN {
			cp := make(map[string]string, len(d.Tags))
			maps.Copy(cp, d.Tags)

			return cp, nil
		}
	}

	return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

func (b *InMemoryBackend) copyDistribution(d *Distribution) *Distribution {
	cp := *d
	rawCopy := make([]byte, len(d.RawConfig))
	copy(rawCopy, d.RawConfig)
	cp.RawConfig = rawCopy

	tagsCopy := make(map[string]string, len(d.Tags))
	maps.Copy(tagsCopy, d.Tags)
	cp.Tags = tagsCopy

	return &cp
}
