package resourcegroups

import (
	"errors"
	"fmt"
	"maps"
	"sync"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

var (
	// ErrNotFound is returned when a resource group is not found.
	ErrNotFound = errors.New("NotFoundException")
	// ErrAlreadyExists is returned when a resource group already exists.
	ErrAlreadyExists = errors.New("BadRequestException")
)

// Group represents a Resource Group.
type Group struct {
	Tags        map[string]string
	Name        string
	ARN         string
	Description string
}

// InMemoryBackend is the in-memory store for Resource Groups.
type InMemoryBackend struct {
	groups    map[string]*Group
	accountID string
	region    string
	mu        sync.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		groups:    make(map[string]*Group),
		accountID: accountID,
		region:    region,
	}
}

// CreateGroup creates a new resource group.
func (b *InMemoryBackend) CreateGroup(name, description string, tags map[string]string) (*Group, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.groups[name]; ok {
		return nil, fmt.Errorf("%w: group %s already exists", ErrAlreadyExists, name)
	}

	groupARN := arn.Build("resource-groups", b.region, b.accountID, "group/"+name)
	t := make(map[string]string)
	maps.Copy(t, tags)

	g := &Group{Name: name, ARN: groupARN, Description: description, Tags: t}
	b.groups[name] = g

	cp := *g
	cp.Tags = make(map[string]string, len(g.Tags))
	maps.Copy(cp.Tags, g.Tags)

	return &cp, nil
}

// DeleteGroup deletes a resource group by name.
func (b *InMemoryBackend) DeleteGroup(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.groups[name]; !ok {
		return fmt.Errorf("%w: group %s not found", ErrNotFound, name)
	}

	delete(b.groups, name)

	return nil
}

// ListGroups returns all resource groups.
func (b *InMemoryBackend) ListGroups() []Group {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]Group, 0, len(b.groups))
	for _, g := range b.groups {
		cp := *g
		cp.Tags = make(map[string]string, len(g.Tags))
		maps.Copy(cp.Tags, g.Tags)
		out = append(out, cp)
	}

	return out
}

// GetGroup returns a resource group by name.
func (b *InMemoryBackend) GetGroup(name string) (*Group, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	g, ok := b.groups[name]
	if !ok {
		return nil, fmt.Errorf("%w: group %s not found", ErrNotFound, name)
	}

	cp := *g
	cp.Tags = make(map[string]string, len(g.Tags))
	maps.Copy(cp.Tags, g.Tags)

	return &cp, nil
}
