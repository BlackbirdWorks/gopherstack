package wafv2

import (
	"fmt"
	"maps"
	"sort"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrWebACLNotFound is returned when a WebACL does not exist.
	ErrWebACLNotFound = awserr.New("WAFNonexistentItemException", awserr.ErrNotFound)
	// ErrWebACLAlreadyExists is returned when a WebACL with the same name already exists.
	ErrWebACLAlreadyExists = awserr.New("WAFDuplicateItemException", awserr.ErrConflict)
	// ErrIPSetNotFound is returned when an IPSet does not exist.
	ErrIPSetNotFound = awserr.New("WAFNonexistentItemException", awserr.ErrNotFound)
	// ErrIPSetAlreadyExists is returned when an IPSet with the same name already exists.
	ErrIPSetAlreadyExists = awserr.New("WAFDuplicateItemException", awserr.ErrConflict)
)

// WebACL represents an AWS WAFv2 Web ACL.
type WebACL struct {
	Tags          map[string]string
	ID            string
	Name          string
	Scope         string
	Description   string
	DefaultAction string
	LockToken     string
}

// IPSet represents an AWS WAFv2 IP Set.
type IPSet struct {
	Tags             map[string]string
	ID               string
	Name             string
	Scope            string
	Description      string
	IPAddressVersion string
	LockToken        string
	Addresses        []string
}

// InMemoryBackend is an in-memory store for WAFv2 resources.
type InMemoryBackend struct {
	webACLs   map[string]*WebACL
	ipSets    map[string]*IPSet
	mu        *lockmetrics.RWMutex
	accountID string
	region    string
}

// NewInMemoryBackend creates a new in-memory WAFv2 backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		webACLs:   make(map[string]*WebACL),
		ipSets:    make(map[string]*IPSet),
		accountID: accountID,
		region:    region,
		mu:        lockmetrics.New("wafv2"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// WebACLARN builds an ARN for a WebACL.
func (b *InMemoryBackend) WebACLARN(name, id, scope string) string {
	prefix := scopePrefix(scope)

	return arn.Build("wafv2", b.region, b.accountID, prefix+"/webacl/"+name+"/"+id)
}

// IPSetARN builds a public ARN for an IPSet.
func (b *InMemoryBackend) IPSetARN(name, id, scope string) string {
	prefix := scopePrefix(scope)

	return arn.Build("wafv2", b.region, b.accountID, prefix+"/ipset/"+name+"/"+id)
}

func scopePrefix(scope string) string {
	if scope == "CLOUDFRONT" {
		return "global"
	}

	return "regional"
}

// CreateWebACL creates a new WebACL.
func (b *InMemoryBackend) CreateWebACL(
	name, scope, description, defaultAction string,
	tags map[string]string,
) (*WebACL, error) {
	b.mu.Lock("CreateWebACL")
	defer b.mu.Unlock()

	for _, w := range b.webACLs {
		if w.Name == name && w.Scope == scope {
			return nil, fmt.Errorf("%w: web ACL %q already exists in scope %s", ErrWebACLAlreadyExists, name, scope)
		}
	}

	id := uuid.NewString()
	w := &WebACL{
		ID:            id,
		Name:          name,
		Scope:         scope,
		Description:   description,
		DefaultAction: defaultAction,
		LockToken:     uuid.NewString(),
		Tags:          cloneTags(tags),
	}
	b.webACLs[id] = w

	return cloneWebACL(w), nil
}

// GetWebACL returns a WebACL by ID.
func (b *InMemoryBackend) GetWebACL(id string) (*WebACL, error) {
	b.mu.RLock("GetWebACL")
	defer b.mu.RUnlock()

	w, ok := b.webACLs[id]
	if !ok {
		return nil, fmt.Errorf("%w: web ACL %q not found", ErrWebACLNotFound, id)
	}

	return cloneWebACL(w), nil
}

// UpdateWebACL updates a WebACL by ID.
func (b *InMemoryBackend) UpdateWebACL(id, description, defaultAction string) (*WebACL, error) {
	b.mu.Lock("UpdateWebACL")
	defer b.mu.Unlock()

	w, ok := b.webACLs[id]
	if !ok {
		return nil, fmt.Errorf("%w: web ACL %q not found", ErrWebACLNotFound, id)
	}

	if description != "" {
		w.Description = description
	}

	if defaultAction != "" {
		w.DefaultAction = defaultAction
	}

	w.LockToken = uuid.NewString()

	return cloneWebACL(w), nil
}

// DeleteWebACL deletes a WebACL by ID.
func (b *InMemoryBackend) DeleteWebACL(id string) error {
	b.mu.Lock("DeleteWebACL")
	defer b.mu.Unlock()

	if _, ok := b.webACLs[id]; !ok {
		return fmt.Errorf("%w: web ACL %q not found", ErrWebACLNotFound, id)
	}

	delete(b.webACLs, id)

	return nil
}

// ListWebACLs returns all WebACLs sorted by name.
func (b *InMemoryBackend) ListWebACLs() []*WebACL {
	b.mu.RLock("ListWebACLs")
	defer b.mu.RUnlock()

	list := make([]*WebACL, 0, len(b.webACLs))

	for _, w := range b.webACLs {
		list = append(list, cloneWebACL(w))
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].Name < list[j].Name
	})

	return list
}

// CreateIPSet creates a new IPSet.
func (b *InMemoryBackend) CreateIPSet(
	name, scope, description, ipAddressVersion string,
	addresses []string,
	tags map[string]string,
) (*IPSet, error) {
	b.mu.Lock("CreateIPSet")
	defer b.mu.Unlock()

	for _, s := range b.ipSets {
		if s.Name == name && s.Scope == scope {
			return nil, fmt.Errorf("%w: IP set %q already exists in scope %s", ErrIPSetAlreadyExists, name, scope)
		}
	}

	id := uuid.NewString()
	s := &IPSet{
		ID:               id,
		Name:             name,
		Scope:            scope,
		Description:      description,
		IPAddressVersion: ipAddressVersion,
		Addresses:        cloneAddresses(addresses),
		LockToken:        uuid.NewString(),
		Tags:             cloneTags(tags),
	}
	b.ipSets[id] = s

	return cloneIPSet(s), nil
}

// GetIPSet returns an IPSet by ID.
func (b *InMemoryBackend) GetIPSet(id string) (*IPSet, error) {
	b.mu.RLock("GetIPSet")
	defer b.mu.RUnlock()

	s, ok := b.ipSets[id]
	if !ok {
		return nil, fmt.Errorf("%w: IP set %q not found", ErrIPSetNotFound, id)
	}

	return cloneIPSet(s), nil
}

// UpdateIPSet updates an IPSet by ID.
func (b *InMemoryBackend) UpdateIPSet(id, description string, addresses []string) (*IPSet, error) {
	b.mu.Lock("UpdateIPSet")
	defer b.mu.Unlock()

	s, ok := b.ipSets[id]
	if !ok {
		return nil, fmt.Errorf("%w: IP set %q not found", ErrIPSetNotFound, id)
	}

	if description != "" {
		s.Description = description
	}

	if addresses != nil {
		s.Addresses = cloneAddresses(addresses)
	}

	s.LockToken = uuid.NewString()

	return cloneIPSet(s), nil
}

// DeleteIPSet deletes an IPSet by ID.
func (b *InMemoryBackend) DeleteIPSet(id string) error {
	b.mu.Lock("DeleteIPSet")
	defer b.mu.Unlock()

	if _, ok := b.ipSets[id]; !ok {
		return fmt.Errorf("%w: IP set %q not found", ErrIPSetNotFound, id)
	}

	delete(b.ipSets, id)

	return nil
}

// ListIPSets returns all IPSets sorted by name.
func (b *InMemoryBackend) ListIPSets() []*IPSet {
	b.mu.RLock("ListIPSets")
	defer b.mu.RUnlock()

	list := make([]*IPSet, 0, len(b.ipSets))

	for _, s := range b.ipSets {
		list = append(list, cloneIPSet(s))
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].Name < list[j].Name
	})

	return list
}

// TagResource adds tags to a WAFv2 resource identified by its ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	for _, w := range b.webACLs {
		if b.WebACLARN(w.Name, w.ID, w.Scope) == resourceARN {
			if w.Tags == nil {
				w.Tags = make(map[string]string)
			}

			maps.Copy(w.Tags, tags)

			return nil
		}
	}

	for _, s := range b.ipSets {
		if b.IPSetARN(s.Name, s.ID, s.Scope) == resourceARN {
			if s.Tags == nil {
				s.Tags = make(map[string]string)
			}

			maps.Copy(s.Tags, tags)

			return nil
		}
	}

	return fmt.Errorf("%w: resource %q not found", ErrWebACLNotFound, resourceARN)
}

// ListTagsForResource returns the tags for a WAFv2 resource identified by its ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	for _, w := range b.webACLs {
		if b.WebACLARN(w.Name, w.ID, w.Scope) == resourceARN {
			return maps.Clone(w.Tags), nil
		}
	}

	for _, s := range b.ipSets {
		if b.IPSetARN(s.Name, s.ID, s.Scope) == resourceARN {
			return maps.Clone(s.Tags), nil
		}
	}

	return nil, fmt.Errorf("%w: resource %q not found", ErrWebACLNotFound, resourceARN)
}

// UntagResource removes tags from a WAFv2 resource identified by its ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	for _, w := range b.webACLs {
		if b.WebACLARN(w.Name, w.ID, w.Scope) == resourceARN {
			for _, k := range tagKeys {
				delete(w.Tags, k)
			}

			return nil
		}
	}

	for _, s := range b.ipSets {
		if b.IPSetARN(s.Name, s.ID, s.Scope) == resourceARN {
			for _, k := range tagKeys {
				delete(s.Tags, k)
			}

			return nil
		}
	}

	return fmt.Errorf("%w: resource %q not found", ErrWebACLNotFound, resourceARN)
}

func cloneWebACL(w *WebACL) *WebACL {
	cp := *w
	cp.Tags = maps.Clone(w.Tags)

	return &cp
}

func cloneIPSet(s *IPSet) *IPSet {
	cp := *s
	cp.Tags = maps.Clone(s.Tags)
	cp.Addresses = cloneAddresses(s.Addresses)

	return &cp
}

func cloneAddresses(addresses []string) []string {
	if addresses == nil {
		return []string{}
	}

	out := make([]string, len(addresses))
	copy(out, addresses)

	return out
}

func cloneTags(tags map[string]string) map[string]string {
	if tags == nil {
		return make(map[string]string)
	}

	return maps.Clone(tags)
}
