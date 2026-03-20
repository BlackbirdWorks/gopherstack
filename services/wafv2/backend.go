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
	Tags             map[string]string `json:"tags,omitempty"`
	ID               string            `json:"id"`
	Name             string            `json:"name"`
	Scope            string            `json:"scope"`
	Description      string            `json:"description"`
	DefaultAction    string            `json:"defaultAction"`
	VisibilityConfig string            `json:"visibilityConfig"`
	LockToken        string            `json:"lockToken"`
}

// IPSet represents an AWS WAFv2 IP Set.
type IPSet struct {
	Tags             map[string]string `json:"tags,omitempty"`
	ID               string            `json:"id"`
	Name             string            `json:"name"`
	Scope            string            `json:"scope"`
	Description      string            `json:"description"`
	IPAddressVersion string            `json:"ipAddressVersion"`
	LockToken        string            `json:"lockToken"`
	Addresses        []string          `json:"addresses,omitempty"`
}

// InMemoryBackend is an in-memory store for WAFv2 resources.
type InMemoryBackend struct {
	webACLs           map[string]*WebACL
	ipSets            map[string]*IPSet
	webACLByARN       map[string]string // ARN → webACL ID
	ipSetByARN        map[string]string // ARN → ipSet ID
	webACLByNameScope map[string]string // "name:scope" → webACL ID (O(1) duplicate check)
	ipSetByNameScope  map[string]string // "name:scope" → ipSet ID (O(1) duplicate check)
	mu                *lockmetrics.RWMutex
	accountID         string
	region            string
}

// NewInMemoryBackend creates a new in-memory WAFv2 backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		webACLs:           make(map[string]*WebACL),
		ipSets:            make(map[string]*IPSet),
		webACLByARN:       make(map[string]string),
		ipSetByARN:        make(map[string]string),
		webACLByNameScope: make(map[string]string),
		ipSetByNameScope:  make(map[string]string),
		accountID:         accountID,
		region:            region,
		mu:                lockmetrics.New("wafv2"),
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

func nameScope(name, scope string) string {
	return name + ":" + scope
}

// CreateWebACL creates a new WebACL.
func (b *InMemoryBackend) CreateWebACL(
	name, scope, description, defaultAction, visibilityConfig string,
	tags map[string]string,
) (*WebACL, error) {
	b.mu.Lock("CreateWebACL")
	defer b.mu.Unlock()

	if _, exists := b.webACLByNameScope[nameScope(name, scope)]; exists {
		return nil, fmt.Errorf("%w: web ACL %q already exists in scope %s", ErrWebACLAlreadyExists, name, scope)
	}

	id := uuid.NewString()
	w := &WebACL{
		ID:               id,
		Name:             name,
		Scope:            scope,
		Description:      description,
		DefaultAction:    defaultAction,
		VisibilityConfig: visibilityConfig,
		LockToken:        uuid.NewString(),
		Tags:             cloneTags(tags),
	}
	b.webACLs[id] = w
	b.webACLByARN[b.WebACLARN(name, id, scope)] = id
	b.webACLByNameScope[nameScope(name, scope)] = id

	return cloneWebACL(w), nil
}
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
func (b *InMemoryBackend) UpdateWebACL(id, description, defaultAction, visibilityConfig string) (*WebACL, error) {
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

	if visibilityConfig != "" {
		w.VisibilityConfig = visibilityConfig
	}

	w.LockToken = uuid.NewString()

	return cloneWebACL(w), nil
}

// DeleteWebACL deletes a WebACL by ID.
func (b *InMemoryBackend) DeleteWebACL(id string) error {
	b.mu.Lock("DeleteWebACL")
	defer b.mu.Unlock()

	if w, ok := b.webACLs[id]; ok {
		delete(b.webACLByARN, b.WebACLARN(w.Name, w.ID, w.Scope))
		delete(b.webACLByNameScope, nameScope(w.Name, w.Scope))
	} else {
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

	if _, exists := b.ipSetByNameScope[nameScope(name, scope)]; exists {
		return nil, fmt.Errorf("%w: IP set %q already exists in scope %s", ErrIPSetAlreadyExists, name, scope)
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
	b.ipSetByARN[b.IPSetARN(name, id, scope)] = id
	b.ipSetByNameScope[nameScope(name, scope)] = id

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

	if s, ok := b.ipSets[id]; ok {
		delete(b.ipSetByARN, b.IPSetARN(s.Name, s.ID, s.Scope))
		delete(b.ipSetByNameScope, nameScope(s.Name, s.Scope))
	} else {
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

	if id, ok := b.webACLByARN[resourceARN]; ok {
		w := b.webACLs[id]
		if w.Tags == nil {
			w.Tags = make(map[string]string)
		}

		maps.Copy(w.Tags, tags)

		return nil
	}

	if id, ok := b.ipSetByARN[resourceARN]; ok {
		s := b.ipSets[id]
		if s.Tags == nil {
			s.Tags = make(map[string]string)
		}

		maps.Copy(s.Tags, tags)

		return nil
	}

	return fmt.Errorf("%w: resource %q not found", ErrWebACLNotFound, resourceARN)
}

// ListTagsForResource returns the tags for a WAFv2 resource identified by its ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if id, ok := b.webACLByARN[resourceARN]; ok {
		return maps.Clone(b.webACLs[id].Tags), nil
	}

	if id, ok := b.ipSetByARN[resourceARN]; ok {
		return maps.Clone(b.ipSets[id].Tags), nil
	}

	return nil, fmt.Errorf("%w: resource %q not found", ErrWebACLNotFound, resourceARN)
}

// UntagResource removes tags from a WAFv2 resource identified by its ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if id, ok := b.webACLByARN[resourceARN]; ok {
		w := b.webACLs[id]
		for _, k := range tagKeys {
			delete(w.Tags, k)
		}

		return nil
	}

	if id, ok := b.ipSetByARN[resourceARN]; ok {
		s := b.ipSets[id]
		for _, k := range tagKeys {
			delete(s.Tags, k)
		}

		return nil
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

// Reset clears all WAFv2 WebACL and IPSet state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.webACLs = make(map[string]*WebACL)
	b.ipSets = make(map[string]*IPSet)
	b.webACLByARN = make(map[string]string)
	b.ipSetByARN = make(map[string]string)
	b.webACLByNameScope = make(map[string]string)
	b.ipSetByNameScope = make(map[string]string)
}
