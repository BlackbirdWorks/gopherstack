package ram

import (
	"fmt"
	"maps"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	// statusActive is the active status for a resource share.
	statusActive = "ACTIVE"
	// statusDeleted is the deleted status for a resource share.
	statusDeleted = "DELETED"
	// associationStatusAssociated is the associated status for an association.
	associationStatusAssociated = "ASSOCIATED"
	// associationStatusDisassociated is the disassociated status for an association.
	associationStatusDisassociated = "DISASSOCIATED"
	// associationTypePrincipal is the principal association type.
	associationTypePrincipal = "PRINCIPAL"
	// associationTypeResource is the resource association type.
	associationTypeResource = "RESOURCE"
)

var (
	// ErrNotFound is returned when a resource share does not exist.
	ErrNotFound = awserr.New("UnknownResourceException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource share already exists.
	ErrAlreadyExists = awserr.New("ResourceShareAlreadyExistsException", awserr.ErrConflict)
)

// ResourceShare represents an AWS RAM resource share.
type ResourceShare struct {
	LastUpdatedTime         time.Time         `json:"lastUpdatedTime"`
	CreationTime            time.Time         `json:"creationTime"`
	Tags                    map[string]string `json:"tags,omitempty"`
	Name                    string            `json:"name"`
	ARN                     string            `json:"arn"`
	OwningAccountID         string            `json:"owningAccountId"`
	Status                  string            `json:"status"`
	StatusMessage           string            `json:"statusMessage,omitempty"`
	AllowExternalPrincipals bool              `json:"allowExternalPrincipals"`
}

// ResourceShareAssociation represents a principal or resource associated with a resource share.
type ResourceShareAssociation struct {
	LastUpdatedTime   time.Time `json:"lastUpdatedTime"`
	CreationTime      time.Time `json:"creationTime"`
	ResourceShareARN  string    `json:"resourceShareArn"`
	ResourceShareName string    `json:"resourceShareName"`
	AssociatedEntity  string    `json:"associatedEntity"`
	AssociationType   string    `json:"associationType"`
	Status            string    `json:"status"`
	External          bool      `json:"external"`
}

// cloneResourceShare returns a deep copy of rs with the Tags map cloned.
func cloneResourceShare(rs *ResourceShare) *ResourceShare {
	cp := *rs
	cp.Tags = maps.Clone(rs.Tags)

	return &cp
}

// cloneAssociation returns a deep copy of an association.
func cloneAssociation(a *ResourceShareAssociation) *ResourceShareAssociation {
	cp := *a

	return &cp
}

// InMemoryBackend is an in-memory store for AWS RAM resources.
type InMemoryBackend struct {
	resourceShares map[string]*ResourceShare
	mu             *lockmetrics.RWMutex
	accountID      string
	region         string
	associations   []*ResourceShareAssociation
}

// NewInMemoryBackend creates a new in-memory RAM backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		resourceShares: make(map[string]*ResourceShare),
		associations:   make([]*ResourceShareAssociation, 0),
		accountID:      accountID,
		region:         region,
		mu:             lockmetrics.New("ram"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// CreateResourceShare creates a new resource share.
func (b *InMemoryBackend) CreateResourceShare(
	name string,
	allowExternalPrincipals bool,
	tags map[string]string,
	principals, resourceARNs []string,
) (*ResourceShare, error) {
	b.mu.Lock("CreateResourceShare")
	defer b.mu.Unlock()

	// Check for name collision.
	for _, rs := range b.resourceShares {
		if rs.Name == name && rs.Status != statusDeleted {
			return nil, fmt.Errorf("%w: resource share %s already exists", ErrAlreadyExists, name)
		}
	}

	now := time.Now()
	// Use a stable UUID-based resource share ID so the ARN remains valid
	// even if the share is later renamed via UpdateResourceShare.
	shareID := uuid.NewString()
	shareARN := arn.Build("ram", b.region, b.accountID, "resource-share/"+shareID)

	rs := &ResourceShare{
		Name:                    name,
		ARN:                     shareARN,
		OwningAccountID:         b.accountID,
		Status:                  statusActive,
		AllowExternalPrincipals: allowExternalPrincipals,
		CreationTime:            now,
		LastUpdatedTime:         now,
		Tags:                    mergeTags(nil, tags),
	}
	b.resourceShares[shareARN] = rs

	// Add principal associations.
	for _, p := range principals {
		b.associations = append(b.associations, &ResourceShareAssociation{
			ResourceShareARN:  shareARN,
			ResourceShareName: name,
			AssociatedEntity:  p,
			AssociationType:   associationTypePrincipal,
			Status:            associationStatusAssociated,
			CreationTime:      now,
			LastUpdatedTime:   now,
		})
	}

	// Add resource associations.
	for _, r := range resourceARNs {
		b.associations = append(b.associations, &ResourceShareAssociation{
			ResourceShareARN:  shareARN,
			ResourceShareName: name,
			AssociatedEntity:  r,
			AssociationType:   associationTypeResource,
			Status:            associationStatusAssociated,
			CreationTime:      now,
			LastUpdatedTime:   now,
		})
	}

	return cloneResourceShare(rs), nil
}

// GetResourceShare returns a resource share by ARN.
func (b *InMemoryBackend) GetResourceShare(shareARN string) (*ResourceShare, error) {
	b.mu.RLock("GetResourceShare")
	defer b.mu.RUnlock()

	rs, ok := b.resourceShares[shareARN]
	if !ok || rs.Status == statusDeleted {
		return nil, fmt.Errorf("%w: resource share %s not found", ErrNotFound, shareARN)
	}

	return cloneResourceShare(rs), nil
}

// ListResourceShares returns resource shares matching the given owner filter.
// resourceOwner should be "SELF" or "OTHER-ACCOUNTS". For the mock, "SELF" returns all owned shares.
func (b *InMemoryBackend) ListResourceShares(resourceOwner string) []*ResourceShare {
	b.mu.RLock("ListResourceShares")
	defer b.mu.RUnlock()

	list := make([]*ResourceShare, 0, len(b.resourceShares))

	for _, rs := range b.resourceShares {
		if rs.Status == statusDeleted {
			continue
		}

		if resourceOwner == "SELF" || resourceOwner == "" {
			list = append(list, cloneResourceShare(rs))
		}
	}

	return list
}

// UpdateResourceShare updates an existing resource share.
// If name is changed, all matching associations are updated to reflect the new name.
func (b *InMemoryBackend) UpdateResourceShare(
	shareARN, name string,
	allowExternalPrincipals *bool,
) (*ResourceShare, error) {
	b.mu.Lock("UpdateResourceShare")
	defer b.mu.Unlock()

	rs, ok := b.resourceShares[shareARN]
	if !ok || rs.Status == statusDeleted {
		return nil, fmt.Errorf("%w: resource share %s not found", ErrNotFound, shareARN)
	}

	if name != "" {
		rs.Name = name
		// Keep association ResourceShareName in sync.
		for _, a := range b.associations {
			if a.ResourceShareARN == shareARN {
				a.ResourceShareName = name
			}
		}
	}

	if allowExternalPrincipals != nil {
		rs.AllowExternalPrincipals = *allowExternalPrincipals
	}

	rs.LastUpdatedTime = time.Now()

	return cloneResourceShare(rs), nil
}

// DeleteResourceShare deletes a resource share.
func (b *InMemoryBackend) DeleteResourceShare(shareARN string) error {
	b.mu.Lock("DeleteResourceShare")
	defer b.mu.Unlock()

	rs, ok := b.resourceShares[shareARN]
	if !ok || rs.Status == statusDeleted {
		return fmt.Errorf("%w: resource share %s not found", ErrNotFound, shareARN)
	}

	delete(b.resourceShares, shareARN)

	// Remove all associations that belonged to this share.
	kept := b.associations[:0]
	for _, a := range b.associations {
		if a.ResourceShareARN != shareARN {
			kept = append(kept, a)
		}
	}
	b.associations = kept

	return nil
}

// AssociateResourceShare associates principals or resource ARNs with a resource share.
// Returns deep copies of the new associations so callers cannot mutate backend state.
func (b *InMemoryBackend) AssociateResourceShare(
	shareARN string,
	principals, resourceARNs []string,
) ([]*ResourceShareAssociation, error) {
	b.mu.Lock("AssociateResourceShare")
	defer b.mu.Unlock()

	rs, ok := b.resourceShares[shareARN]
	if !ok || rs.Status == statusDeleted {
		return nil, fmt.Errorf("%w: resource share %s not found", ErrNotFound, shareARN)
	}

	now := time.Now()
	added := make([]*ResourceShareAssociation, 0, len(principals)+len(resourceARNs))

	for _, p := range principals {
		assoc := &ResourceShareAssociation{
			ResourceShareARN:  shareARN,
			ResourceShareName: rs.Name,
			AssociatedEntity:  p,
			AssociationType:   associationTypePrincipal,
			Status:            associationStatusAssociated,
			CreationTime:      now,
			LastUpdatedTime:   now,
		}
		b.associations = append(b.associations, assoc)
		added = append(added, cloneAssociation(assoc))
	}

	for _, r := range resourceARNs {
		assoc := &ResourceShareAssociation{
			ResourceShareARN:  shareARN,
			ResourceShareName: rs.Name,
			AssociatedEntity:  r,
			AssociationType:   associationTypeResource,
			Status:            associationStatusAssociated,
			CreationTime:      now,
			LastUpdatedTime:   now,
		}
		b.associations = append(b.associations, assoc)
		added = append(added, cloneAssociation(assoc))
	}

	return added, nil
}

// DisassociateResourceShare removes principals or resource ARNs from a resource share.
func (b *InMemoryBackend) DisassociateResourceShare(
	shareARN string,
	principals, resourceARNs []string,
) ([]*ResourceShareAssociation, error) {
	b.mu.Lock("DisassociateResourceShare")
	defer b.mu.Unlock()

	rs, ok := b.resourceShares[shareARN]
	if !ok || rs.Status == statusDeleted {
		return nil, fmt.Errorf("%w: resource share %s not found", ErrNotFound, shareARN)
	}

	toRemove := make(map[string]struct{}, len(principals)+len(resourceARNs))

	for _, p := range principals {
		toRemove[p] = struct{}{}
	}

	for _, r := range resourceARNs {
		toRemove[r] = struct{}{}
	}

	var updated []*ResourceShareAssociation

	kept := b.associations[:0]

	for _, a := range b.associations {
		if a.ResourceShareARN == shareARN {
			if _, found := toRemove[a.AssociatedEntity]; found {
				cp := cloneAssociation(a)
				cp.Status = associationStatusDisassociated
				cp.LastUpdatedTime = time.Now()
				updated = append(updated, cp)

				continue
			}
		}

		kept = append(kept, a)
	}

	b.associations = kept

	_ = rs // share lookup above ensures we return NotFound for deleted shares

	return updated, nil
}

// GetResourceShareAssociations returns associations for the given resource share ARNs and type.
func (b *InMemoryBackend) GetResourceShareAssociations(
	associationType string,
	shareARNs []string,
) []*ResourceShareAssociation {
	b.mu.RLock("GetResourceShareAssociations")
	defer b.mu.RUnlock()

	arnSet := make(map[string]struct{}, len(shareARNs))

	for _, a := range shareARNs {
		arnSet[a] = struct{}{}
	}

	result := make([]*ResourceShareAssociation, 0)

	for _, a := range b.associations {
		if a.Status == associationStatusDisassociated {
			continue
		}

		if associationType != "" && a.AssociationType != associationType {
			continue
		}

		if len(arnSet) > 0 {
			if _, ok := arnSet[a.ResourceShareARN]; !ok {
				continue
			}
		}

		result = append(result, cloneAssociation(a))
	}

	return result
}

// TagResource adds or updates tags on a resource share identified by ARN.
func (b *InMemoryBackend) TagResource(shareARN string, kv map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	rs, ok := b.resourceShares[shareARN]
	if !ok || rs.Status == statusDeleted {
		return fmt.Errorf("%w: resource share %s not found", ErrNotFound, shareARN)
	}

	rs.Tags = mergeTags(rs.Tags, kv)

	return nil
}

// UntagResource removes specified tag keys from a resource share.
func (b *InMemoryBackend) UntagResource(shareARN string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	rs, ok := b.resourceShares[shareARN]
	if !ok || rs.Status == statusDeleted {
		return fmt.Errorf("%w: resource share %s not found", ErrNotFound, shareARN)
	}

	for _, k := range keys {
		delete(rs.Tags, k)
	}

	return nil
}

// ListTagsForResource returns tags for a resource share identified by ARN.
func (b *InMemoryBackend) ListTagsForResource(shareARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	rs, ok := b.resourceShares[shareARN]
	if !ok || rs.Status == statusDeleted {
		return nil, fmt.Errorf("%w: resource share %s not found", ErrNotFound, shareARN)
	}

	result := make(map[string]string, len(rs.Tags))
	maps.Copy(result, rs.Tags)

	return result, nil
}

// mergeTags merges new tags into existing ones, returning a new map.
func mergeTags(existing, incoming map[string]string) map[string]string {
	result := make(map[string]string, len(existing)+len(incoming))
	maps.Copy(result, existing)
	maps.Copy(result, incoming)

	return result
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.resourceShares = make(map[string]*ResourceShare)
	b.associations = make([]*ResourceShareAssociation, 0)
}
