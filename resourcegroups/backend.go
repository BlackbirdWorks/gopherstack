package resourcegroups

import (
	"fmt"

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
	Type  string `json:"type"`
	Query string `json:"query"`
}

// Group represents a Resource Group.
type Group struct {
	Tags          *tags.Tags
	ResourceQuery *ResourceQuery
	Name          string
	ARN           string
	Description   string
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

// DeleteGroup deletes a resource group by name.
func (b *InMemoryBackend) DeleteGroup(name string) error {
	b.mu.Lock("DeleteGroup")
	defer b.mu.Unlock()

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

// GetGroup returns a resource group by name.
// The Tags field in the returned Group points to the backend-owned Tags
// collection; callers should treat it as read-only.
func (b *InMemoryBackend) GetGroup(name string) (*Group, error) {
	b.mu.RLock("GetGroup")
	defer b.mu.RUnlock()

	g, ok := b.groups[name]
	if !ok {
		return nil, fmt.Errorf("%w: group %s not found", ErrNotFound, name)
	}

	cp := *g

	return &cp, nil
}
