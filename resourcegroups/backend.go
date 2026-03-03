package resourcegroups

import (
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

var (
	// ErrNotFound is returned when a resource group is not found.
	ErrNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource group already exists.
	ErrAlreadyExists = awserr.New("BadRequestException", awserr.ErrAlreadyExists)
)

// ResourceQuery represents a tag-based resource query for a group.
type ResourceQuery struct {
	Type  string `json:"Type"`
	Query string `json:"Query"`
}

// Group represents a Resource Group.
// Field names use PascalCase JSON tags to match what the AWS SDK expects in responses.
type Group struct {
	Tags          *tags.Tags     `json:"Tags,omitempty"`
	ResourceQuery *ResourceQuery `json:"ResourceQuery,omitempty"`
	Name          string         `json:"Name"`
	ARN           string         `json:"GroupArn"`
	Description   string         `json:"Description"`
}

// InMemoryBackend is the in-memory store for Resource Groups.
type InMemoryBackend struct {
	groups    map[string]*Group
	mu        *lockmetrics.RWMutex
	accountID string
	region    string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		groups:    make(map[string]*Group),
		accountID: accountID,
		region:    region,
		mu:        lockmetrics.New("resourcegroups"),
	}
}

// CreateGroup creates a new resource group.
// The Tags field in the returned Group points to the backend-owned Tags
// collection; callers should treat it as read-only.
func (b *InMemoryBackend) CreateGroup(
	name, description string,
	resourceQuery *ResourceQuery,
	inputTags *tags.Tags,
) (*Group, error) {
	b.mu.Lock("CreateGroup")
	defer b.mu.Unlock()

	if _, ok := b.groups[name]; ok {
		return nil, fmt.Errorf("%w: group %s already exists", ErrAlreadyExists, name)
	}

	groupARN := arn.Build("resource-groups", b.region, b.accountID, "group/"+name)

	// Clone caller-provided tags into a backend-owned collection so that the
	// caller cannot mutate backend state by keeping a reference to inputTags.
	var backendTags *tags.Tags
	if inputTags == nil {
		backendTags = tags.New("rg." + name + ".tags")
	} else {
		backendTags = tags.FromMap("rg."+name+".tags", inputTags.Clone())
	}

	g := &Group{Name: name, ARN: groupARN, Description: description, Tags: backendTags, ResourceQuery: resourceQuery}
	b.groups[name] = g

	cp := *g

	return &cp, nil
}

// DeleteGroup deletes a resource group by name or ARN.
func (b *InMemoryBackend) DeleteGroup(nameOrARN string) error {
	b.mu.Lock("DeleteGroup")
	defer b.mu.Unlock()

	name := nameOrARN
	if idx := strings.LastIndex(nameOrARN, "group/"); idx >= 0 {
		name = nameOrARN[idx+len("group/"):]
	}

	if _, ok := b.groups[name]; !ok {
		return fmt.Errorf("%w: group %s not found", ErrNotFound, name)
	}

	delete(b.groups, name)

	return nil
}

// ListGroups returns all resource groups.
// The Tags field in each returned Group points to the backend-owned Tags
// collection; callers should treat it as read-only.
func (b *InMemoryBackend) ListGroups() []Group {
	b.mu.RLock("ListGroups")
	defer b.mu.RUnlock()

	out := make([]Group, 0, len(b.groups))
	for _, g := range b.groups {
		out = append(out, *g)
	}

	return out
}

// GetTagsByARN returns the tags for the resource group identified by ARN.
func (b *InMemoryBackend) GetTagsByARN(resourceARN string) (map[string]string, error) {
	b.mu.RLock("GetTagsByARN")
	defer b.mu.RUnlock()

	g := b.findByARN(resourceARN)
	if g == nil {
		return nil, fmt.Errorf("%w: group with ARN %s not found", ErrNotFound, resourceARN)
	}

	return g.Tags.Clone(), nil
}

// AddTagsByARN merges newTags into the resource group identified by ARN and
// returns the resulting tag set.
func (b *InMemoryBackend) AddTagsByARN(resourceARN string, newTags map[string]string) (map[string]string, error) {
	b.mu.Lock("AddTagsByARN")
	defer b.mu.Unlock()

	g := b.findByARN(resourceARN)
	if g == nil {
		return nil, fmt.Errorf("%w: group with ARN %s not found", ErrNotFound, resourceARN)
	}

	g.Tags.Merge(newTags)

	return g.Tags.Clone(), nil
}

// RemoveTagsByARN removes the specified tag keys from the resource group
// identified by ARN.
func (b *InMemoryBackend) RemoveTagsByARN(resourceARN string, keys []string) error {
	b.mu.Lock("RemoveTagsByARN")
	defer b.mu.Unlock()

	g := b.findByARN(resourceARN)
	if g == nil {
		return fmt.Errorf("%w: group with ARN %s not found", ErrNotFound, resourceARN)
	}

	g.Tags.DeleteKeys(keys)

	return nil
}

// findByARN looks up a group by its ARN (must be called under a lock).
func (b *InMemoryBackend) findByARN(resourceARN string) *Group {
	for _, g := range b.groups {
		if g.ARN == resourceARN {
			return g
		}
	}

	return nil
}

// GetGroup returns a resource group by name or ARN.
// The Tags field in the returned Group points to the backend-owned Tags
// collection; callers should treat it as read-only.
func (b *InMemoryBackend) GetGroup(nameOrARN string) (*Group, error) {
	b.mu.RLock("GetGroup")
	defer b.mu.RUnlock()

	// Support ARN-based lookup: extract the group name from the ARN suffix.
	// e.g. "arn:aws:resource-groups:us-east-1:123:group/my-group" → "my-group"
	name := nameOrARN
	if idx := strings.LastIndex(nameOrARN, "group/"); idx >= 0 {
		name = nameOrARN[idx+len("group/"):]
	}

	g, ok := b.groups[name]
	if !ok {
		return nil, fmt.Errorf("%w: group %s not found", ErrNotFound, name)
	}

	cp := *g

	return &cp, nil
}
