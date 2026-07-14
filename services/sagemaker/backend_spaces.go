package sagemaker

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ErrSpaceNotFound is returned when a space does not exist.
var ErrSpaceNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)

// ---------------------------------------------------------------------------
// Space
// ---------------------------------------------------------------------------

// Space represents a SageMaker Studio space.
type Space struct {
	CreationTime     time.Time         `json:"CreationTime"`
	LastModifiedTime time.Time         `json:"LastModifiedTime"`
	Tags             map[string]string `json:"Tags,omitempty"`
	SpaceName        string            `json:"SpaceName"`
	SpaceArn         string            `json:"SpaceArn"`
	DomainID         string            `json:"DomainId"`
	SpaceStatus      string            `json:"SpaceStatus"`
}

func cloneSpace(s *Space) *Space {
	cp := *s
	cp.Tags = maps.Clone(s.Tags)

	return &cp
}

func spaceKey(domainID, spaceName string) string {
	return domainID + "/" + spaceName
}

// CreateSpace creates a SageMaker Studio space.
func (b *InMemoryBackend) CreateSpace(
	ctx context.Context,
	domainID, spaceName string,
	tags map[string]string,
) (*Space, error) {
	b.mu.Lock("CreateSpace")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if domainID == "" {
		return nil, fmt.Errorf("%w: DomainID is required", ErrValidation)
	}

	if spaceName == "" {
		return nil, fmt.Errorf("%w: SpaceName is required", ErrValidation)
	}

	key := spaceKey(domainID, spaceName)

	if _, ok := b.spacesStore(region).Get(key); ok {
		return nil, fmt.Errorf("%w: space %q already exists in domain %q", ErrValidation, spaceName, domainID)
	}

	spaceARN := arn.Build("sagemaker", region, b.accountID, "space/"+domainID+"/"+spaceName)
	now := time.Now()

	s := &Space{
		SpaceName:        spaceName,
		SpaceArn:         spaceARN,
		DomainID:         domainID,
		SpaceStatus:      "InService",
		Tags:             mergeTags(nil, tags),
		CreationTime:     now,
		LastModifiedTime: now,
	}
	b.spacesStore(region).Put(s)

	return cloneSpace(s), nil
}

// DescribeSpace returns a space by domain ID and space name.
func (b *InMemoryBackend) DescribeSpace(ctx context.Context, domainID, spaceName string) (*Space, error) {
	b.mu.RLock("DescribeSpace")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	s, ok := b.spacesStoreRO(region).Get(spaceKey(domainID, spaceName))
	if !ok {
		return nil, fmt.Errorf("%w: space %q not found in domain %q", ErrSpaceNotFound, spaceName, domainID)
	}

	return cloneSpace(s), nil
}

// DeleteSpace removes a space.
func (b *InMemoryBackend) DeleteSpace(ctx context.Context, domainID, spaceName string) error {
	b.mu.Lock("DeleteSpace")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	key := spaceKey(domainID, spaceName)

	if _, ok := b.spacesStore(region).Get(key); !ok {
		return fmt.Errorf("%w: space %q not found in domain %q", ErrSpaceNotFound, spaceName, domainID)
	}

	store := b.spacesStore(region)
	store.Delete(key)

	return nil
}

// ListSpaces returns all spaces optionally filtered by domain ID.
func (b *InMemoryBackend) ListSpaces(ctx context.Context, domainID, nextToken string) ([]*Space, string) {
	b.mu.RLock("ListSpaces")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	var keys []string
	for _, s := range b.spacesStoreRO(region).All() {
		if domainID == "" || s.DomainID == domainID {
			keys = append(keys, spaceKey(s.DomainID, s.SpaceName))
		}
	}

	sort.Strings(keys)

	start := 0
	if nextToken != "" {
		for i, k := range keys {
			if k == nextToken {
				start = i

				break
			}
		}
	}

	end := min(start+sagemakerDefaultPageSize, len(keys))

	out := make([]*Space, 0, end-start)
	for _, k := range keys[start:end] {
		out = append(out, cloneSpace(tableGet(b.spacesStoreRO(region), k)))
	}

	next := ""
	if end < len(keys) {
		next = keys[end]
	}

	return out, next
}

// UpdateSpace updates a space in a domain. Returns the updated space.
func (b *InMemoryBackend) UpdateSpace(ctx context.Context, domainID, spaceName string) (*Space, error) {
	b.mu.Lock("UpdateSpace")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	key := spaceKey(domainID, spaceName)

	s, ok := b.spacesStore(region).Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: space %q not found in domain %q", ErrSpaceNotFound, spaceName, domainID)
	}

	s.LastModifiedTime = time.Now()

	return cloneSpace(s), nil
}
