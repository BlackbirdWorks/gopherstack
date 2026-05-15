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
	// ErrAssociationNotFound is returned when a WebACL association does not exist.
	ErrAssociationNotFound = awserr.New("WAFNonexistentItemException", awserr.ErrNotFound)
	// ErrRegexPatternSetNotFound is returned when a RegexPatternSet does not exist.
	ErrRegexPatternSetNotFound = awserr.New("WAFNonexistentItemException", awserr.ErrNotFound)
	// ErrRegexPatternSetAlreadyExists is returned when a RegexPatternSet with the same name already exists.
	ErrRegexPatternSetAlreadyExists = awserr.New("WAFDuplicateItemException", awserr.ErrConflict)
	// ErrRuleGroupNotFound is returned when a RuleGroup does not exist.
	ErrRuleGroupNotFound = awserr.New("WAFNonexistentItemException", awserr.ErrNotFound)
	// ErrRuleGroupAlreadyExists is returned when a RuleGroup with the same name already exists.
	ErrRuleGroupAlreadyExists = awserr.New("WAFDuplicateItemException", awserr.ErrConflict)
	// ErrAPIKeyNotFound is returned when an API key does not exist.
	ErrAPIKeyNotFound = awserr.New("WAFNonexistentItemException", awserr.ErrNotFound)
	// ErrLoggingConfigNotFound is returned when a logging configuration does not exist.
	ErrLoggingConfigNotFound = awserr.New("WAFNonexistentItemException", awserr.ErrNotFound)
	// ErrPermissionPolicyNotFound is returned when a permission policy does not exist.
	ErrPermissionPolicyNotFound = awserr.New("WAFNonexistentItemException", awserr.ErrNotFound)
)

const (
	// ScopeRegional is the REGIONAL WAFv2 scope.
	ScopeRegional = "REGIONAL"
	// ScopeCloudFront is the CLOUDFRONT WAFv2 scope.
	ScopeCloudFront = "CLOUDFRONT"
	// IPVersionIPv4 is the IPV4 address version.
	IPVersionIPv4 = "IPV4"
	// IPVersionIPv6 is the IPV6 address version.
	IPVersionIPv6 = "IPV6"
	wcuPerRule    = int64(1)
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

// RegexPatternSet represents an AWS WAFv2 Regex Pattern Set.
type RegexPatternSet struct {
	Tags                  map[string]string `json:"tags,omitempty"`
	ID                    string            `json:"id"`
	Name                  string            `json:"name"`
	Scope                 string            `json:"scope"`
	Description           string            `json:"description"`
	LockToken             string            `json:"lockToken"`
	RegularExpressionList []string          `json:"regularExpressionList,omitempty"`
}

// RuleGroup represents an AWS WAFv2 Rule Group.
type RuleGroup struct {
	Tags             map[string]string `json:"tags,omitempty"`
	ID               string            `json:"id"`
	Name             string            `json:"name"`
	Scope            string            `json:"scope"`
	Description      string            `json:"description"`
	VisibilityConfig string            `json:"visibilityConfig"`
	LockToken        string            `json:"lockToken"`
	Rules            []map[string]any  `json:"rules,omitempty"`
	Capacity         int64             `json:"capacity"`
}

// APIKey represents an AWS WAFv2 API key.
type APIKey struct {
	APIKeyValue  string   `json:"apiKey"`
	Scope        string   `json:"scope"`
	TokenDomains []string `json:"tokenDomains,omitempty"`
}

// InMemoryBackend is an in-memory store for WAFv2 resources.
type InMemoryBackend struct {
	webACLs                map[string]*WebACL
	ipSets                 map[string]*IPSet
	regexPatternSets       map[string]*RegexPatternSet
	ruleGroups             map[string]*RuleGroup
	apiKeys                map[string]*APIKey // key: scope+":"+apiKeyValue
	loggingConfigs         map[string]bool    // resourceARN → configured
	permissionPolicies     map[string]string  // resourceARN → policy JSON
	webACLByARN            map[string]string  // ARN → webACL ID
	ipSetByARN             map[string]string  // ARN → ipSet ID
	regexPatternSetByARN   map[string]string  // ARN → regexPatternSet ID
	ruleGroupByARN         map[string]string  // ARN → ruleGroup ID
	webACLByNameScope      map[string]string  // "name:scope" → webACL ID (O(1) duplicate check)
	ipSetByNameScope       map[string]string  // "name:scope" → ipSet ID (O(1) duplicate check)
	regexPatternSetByScope map[string]string  // "name:scope" → regexPatternSet ID
	ruleGroupByNameScope   map[string]string  // "name:scope" → ruleGroup ID
	associations           map[string]string  // resourceARN → webACL ID (AssociateWebACL)
	mu                     *lockmetrics.RWMutex
	accountID              string
	region                 string
}

// NewInMemoryBackend creates a new in-memory WAFv2 backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		webACLs:                make(map[string]*WebACL),
		ipSets:                 make(map[string]*IPSet),
		regexPatternSets:       make(map[string]*RegexPatternSet),
		ruleGroups:             make(map[string]*RuleGroup),
		apiKeys:                make(map[string]*APIKey),
		loggingConfigs:         make(map[string]bool),
		permissionPolicies:     make(map[string]string),
		webACLByARN:            make(map[string]string),
		ipSetByARN:             make(map[string]string),
		regexPatternSetByARN:   make(map[string]string),
		ruleGroupByARN:         make(map[string]string),
		webACLByNameScope:      make(map[string]string),
		ipSetByNameScope:       make(map[string]string),
		regexPatternSetByScope: make(map[string]string),
		ruleGroupByNameScope:   make(map[string]string),
		associations:           make(map[string]string),
		accountID:              accountID,
		region:                 region,
		mu:                     lockmetrics.New("wafv2"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the AWS account ID this backend is configured for.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// validScope reports whether scope is a recognised WAFv2 scope.
func validScope(scope string) bool {
	return scope == ScopeRegional || scope == ScopeCloudFront
}

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

// RegexPatternSetARN builds an ARN for a RegexPatternSet.
func (b *InMemoryBackend) RegexPatternSetARN(name, id, scope string) string {
	prefix := scopePrefix(scope)

	return arn.Build("wafv2", b.region, b.accountID, prefix+"/regexpatternset/"+name+"/"+id)
}

// RuleGroupARN builds an ARN for a RuleGroup.
func (b *InMemoryBackend) RuleGroupARN(name, id, scope string) string {
	prefix := scopePrefix(scope)

	return arn.Build("wafv2", b.region, b.accountID, prefix+"/rulegroup/"+name+"/"+id)
}

func apiKeyMapKey(scope, apiKey string) string {
	return scope + ":" + apiKey
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

	w, ok := b.webACLs[id]
	if !ok {
		return fmt.Errorf("%w: web ACL %q not found", ErrWebACLNotFound, id)
	}

	delete(b.webACLByARN, b.WebACLARN(w.Name, w.ID, w.Scope))
	delete(b.webACLByNameScope, nameScope(w.Name, w.Scope))
	delete(b.webACLs, id)

	// Cascade: remove all resource associations for this WebACL.
	for resourceARN, assocID := range b.associations {
		if assocID == id {
			delete(b.associations, resourceARN)
		}
	}

	// Cascade: remove the WebACL's own logging config and permission policy.
	webACLArnStr := b.WebACLARN(w.Name, w.ID, w.Scope)
	delete(b.loggingConfigs, webACLArnStr)
	delete(b.permissionPolicies, webACLArnStr)

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

	if id, ok := b.regexPatternSetByARN[resourceARN]; ok {
		r := b.regexPatternSets[id]
		if r.Tags == nil {
			r.Tags = make(map[string]string)
		}

		maps.Copy(r.Tags, tags)

		return nil
	}

	if id, ok := b.ruleGroupByARN[resourceARN]; ok {
		rg := b.ruleGroups[id]
		if rg.Tags == nil {
			rg.Tags = make(map[string]string)
		}

		maps.Copy(rg.Tags, tags)

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

	if id, ok := b.regexPatternSetByARN[resourceARN]; ok {
		return maps.Clone(b.regexPatternSets[id].Tags), nil
	}

	if id, ok := b.ruleGroupByARN[resourceARN]; ok {
		return maps.Clone(b.ruleGroups[id].Tags), nil
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

	if id, ok := b.regexPatternSetByARN[resourceARN]; ok {
		r := b.regexPatternSets[id]
		for _, k := range tagKeys {
			delete(r.Tags, k)
		}

		return nil
	}

	if id, ok := b.ruleGroupByARN[resourceARN]; ok {
		rg := b.ruleGroups[id]
		for _, k := range tagKeys {
			delete(rg.Tags, k)
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
	b.regexPatternSets = make(map[string]*RegexPatternSet)
	b.ruleGroups = make(map[string]*RuleGroup)
	b.apiKeys = make(map[string]*APIKey)
	b.loggingConfigs = make(map[string]bool)
	b.permissionPolicies = make(map[string]string)
	b.webACLByARN = make(map[string]string)
	b.ipSetByARN = make(map[string]string)
	b.regexPatternSetByARN = make(map[string]string)
	b.ruleGroupByARN = make(map[string]string)
	b.webACLByNameScope = make(map[string]string)
	b.ipSetByNameScope = make(map[string]string)
	b.regexPatternSetByScope = make(map[string]string)
	b.ruleGroupByNameScope = make(map[string]string)
	b.associations = make(map[string]string)
}

// AssociateWebACL associates a WebACL with a resource ARN.
func (b *InMemoryBackend) AssociateWebACL(webACLARN, resourceARN string) error {
	b.mu.Lock("AssociateWebACL")
	defer b.mu.Unlock()

	webACLID, ok := b.webACLByARN[webACLARN]
	if !ok {
		return fmt.Errorf("%w: web ACL with ARN %q not found", ErrWebACLNotFound, webACLARN)
	}

	b.associations[resourceARN] = webACLID

	return nil
}

// DisassociateWebACL removes the WebACL association from a resource ARN.
func (b *InMemoryBackend) DisassociateWebACL(resourceARN string) error {
	b.mu.Lock("DisassociateWebACL")
	defer b.mu.Unlock()

	if _, ok := b.associations[resourceARN]; !ok {
		return fmt.Errorf("%w: no web ACL association found for resource %q", ErrAssociationNotFound, resourceARN)
	}

	delete(b.associations, resourceARN)

	return nil
}

// GetWebACLForResource returns the WebACL associated with the given resource ARN.
func (b *InMemoryBackend) GetWebACLForResource(resourceARN string) (*WebACL, error) {
	b.mu.RLock("GetWebACLForResource")
	defer b.mu.RUnlock()

	webACLID, ok := b.associations[resourceARN]
	if !ok {
		return nil, fmt.Errorf("%w: no web ACL association found for resource %q", ErrAssociationNotFound, resourceARN)
	}

	w, ok := b.webACLs[webACLID]
	if !ok {
		return nil, fmt.Errorf("%w: web ACL %q not found", ErrWebACLNotFound, webACLID)
	}

	return cloneWebACL(w), nil
}

// CheckCapacity returns the capacity consumed by the provided rules.
// Each rule costs wcuPerRule WCUs in this in-memory implementation.
func (b *InMemoryBackend) CheckCapacity(_ string, rules []map[string]any) (int64, error) {
	return int64(len(rules)) * wcuPerRule, nil
}

// CreateAPIKey creates a new API key for the given scope and token domains.
func (b *InMemoryBackend) CreateAPIKey(scope string, tokenDomains []string) (*APIKey, error) {
	b.mu.Lock("CreateAPIKey")
	defer b.mu.Unlock()

	key := uuid.NewString()
	a := &APIKey{
		APIKeyValue:  key,
		Scope:        scope,
		TokenDomains: cloneAddresses(tokenDomains),
	}
	b.apiKeys[apiKeyMapKey(scope, key)] = a

	return &APIKey{
		APIKeyValue:  a.APIKeyValue,
		Scope:        a.Scope,
		TokenDomains: cloneAddresses(a.TokenDomains),
	}, nil
}

// DeleteAPIKey deletes the API key identified by scope and key value.
func (b *InMemoryBackend) DeleteAPIKey(scope, apiKey string) error {
	b.mu.Lock("DeleteAPIKey")
	defer b.mu.Unlock()

	k := apiKeyMapKey(scope, apiKey)
	if _, ok := b.apiKeys[k]; !ok {
		return fmt.Errorf("%w: API key not found", ErrAPIKeyNotFound)
	}

	delete(b.apiKeys, k)

	return nil
}

// CreateRegexPatternSet creates a new RegexPatternSet.
func (b *InMemoryBackend) CreateRegexPatternSet(
	name, scope, description string,
	regularExpressionList []string,
	tags map[string]string,
) (*RegexPatternSet, error) {
	b.mu.Lock("CreateRegexPatternSet")
	defer b.mu.Unlock()

	if _, exists := b.regexPatternSetByScope[nameScope(name, scope)]; exists {
		return nil, fmt.Errorf(
			"%w: regex pattern set %q already exists in scope %s",
			ErrRegexPatternSetAlreadyExists,
			name,
			scope,
		)
	}

	id := uuid.NewString()
	rps := &RegexPatternSet{
		ID:                    id,
		Name:                  name,
		Scope:                 scope,
		Description:           description,
		RegularExpressionList: cloneAddresses(regularExpressionList),
		LockToken:             uuid.NewString(),
		Tags:                  cloneTags(tags),
	}
	b.regexPatternSets[id] = rps
	arnStr := b.RegexPatternSetARN(name, id, scope)
	b.regexPatternSetByARN[arnStr] = id
	b.regexPatternSetByScope[nameScope(name, scope)] = id

	return cloneRegexPatternSet(rps), nil
}

// DeleteRegexPatternSet deletes a RegexPatternSet by ID.
func (b *InMemoryBackend) DeleteRegexPatternSet(id string) error {
	b.mu.Lock("DeleteRegexPatternSet")
	defer b.mu.Unlock()

	rps, ok := b.regexPatternSets[id]
	if !ok {
		return fmt.Errorf("%w: regex pattern set %q not found", ErrRegexPatternSetNotFound, id)
	}

	delete(b.regexPatternSetByARN, b.RegexPatternSetARN(rps.Name, rps.ID, rps.Scope))
	delete(b.regexPatternSetByScope, nameScope(rps.Name, rps.Scope))
	delete(b.regexPatternSets, id)

	return nil
}

// CreateRuleGroup creates a new RuleGroup.
func (b *InMemoryBackend) CreateRuleGroup(
	name, scope, description, visibilityConfig string,
	capacity int64,
	rules []map[string]any,
	tags map[string]string,
) (*RuleGroup, error) {
	b.mu.Lock("CreateRuleGroup")
	defer b.mu.Unlock()

	if _, exists := b.ruleGroupByNameScope[nameScope(name, scope)]; exists {
		return nil, fmt.Errorf("%w: rule group %q already exists in scope %s", ErrRuleGroupAlreadyExists, name, scope)
	}

	id := uuid.NewString()
	rg := &RuleGroup{
		ID:               id,
		Name:             name,
		Scope:            scope,
		Description:      description,
		VisibilityConfig: visibilityConfig,
		Capacity:         capacity,
		Rules:            cloneRules(rules),
		LockToken:        uuid.NewString(),
		Tags:             cloneTags(tags),
	}
	b.ruleGroups[id] = rg
	arnStr := b.RuleGroupARN(name, id, scope)
	b.ruleGroupByARN[arnStr] = id
	b.ruleGroupByNameScope[nameScope(name, scope)] = id

	return cloneRuleGroup(rg), nil
}

// DeleteFirewallManagerRuleGroups removes all Firewall Manager rule group
// associations from the WebACL identified by webACLARN, then returns a fresh
// copy of the updated WebACL.
func (b *InMemoryBackend) DeleteFirewallManagerRuleGroups(webACLARN string) (*WebACL, error) {
	b.mu.Lock("DeleteFirewallManagerRuleGroups")
	defer b.mu.Unlock()

	webACLID, ok := b.webACLByARN[webACLARN]
	if !ok {
		return nil, fmt.Errorf("%w: web ACL with ARN %q not found", ErrWebACLNotFound, webACLARN)
	}

	w, ok := b.webACLs[webACLID]
	if !ok {
		return nil, fmt.Errorf("%w: web ACL %q not found", ErrWebACLNotFound, webACLID)
	}

	w.LockToken = uuid.NewString()

	return cloneWebACL(w), nil
}

// PutLoggingConfiguration stores a logging configuration for the given resource ARN.
func (b *InMemoryBackend) PutLoggingConfiguration(resourceARN string) error {
	b.mu.Lock("PutLoggingConfiguration")
	defer b.mu.Unlock()

	b.loggingConfigs[resourceARN] = true

	return nil
}

// DeleteLoggingConfiguration removes the logging configuration for the given resource ARN.
func (b *InMemoryBackend) DeleteLoggingConfiguration(resourceARN string) error {
	b.mu.Lock("DeleteLoggingConfiguration")
	defer b.mu.Unlock()

	if !b.loggingConfigs[resourceARN] {
		return fmt.Errorf("%w: no logging configuration found for resource %q", ErrLoggingConfigNotFound, resourceARN)
	}

	delete(b.loggingConfigs, resourceARN)

	return nil
}

// PutPermissionPolicy stores a permission policy for the given resource ARN.
func (b *InMemoryBackend) PutPermissionPolicy(resourceARN, policy string) error {
	b.mu.Lock("PutPermissionPolicy")
	defer b.mu.Unlock()

	b.permissionPolicies[resourceARN] = policy

	return nil
}

// DeletePermissionPolicy removes the permission policy for the given resource ARN.
func (b *InMemoryBackend) DeletePermissionPolicy(resourceARN string) error {
	b.mu.Lock("DeletePermissionPolicy")
	defer b.mu.Unlock()

	if _, ok := b.permissionPolicies[resourceARN]; !ok {
		return fmt.Errorf("%w: no permission policy found for resource %q", ErrPermissionPolicyNotFound, resourceARN)
	}

	delete(b.permissionPolicies, resourceARN)

	return nil
}

// GetRegexPatternSet returns a RegexPatternSet by ID.
func (b *InMemoryBackend) GetRegexPatternSet(id string) (*RegexPatternSet, error) {
	b.mu.RLock("GetRegexPatternSet")
	defer b.mu.RUnlock()

	r, ok := b.regexPatternSets[id]
	if !ok {
		return nil, fmt.Errorf("%w: regex pattern set %q not found", ErrRegexPatternSetNotFound, id)
	}

	return cloneRegexPatternSet(r), nil
}

// ListRegexPatternSets returns all RegexPatternSets sorted by name.
func (b *InMemoryBackend) ListRegexPatternSets() []*RegexPatternSet {
	b.mu.RLock("ListRegexPatternSets")
	defer b.mu.RUnlock()

	list := make([]*RegexPatternSet, 0, len(b.regexPatternSets))

	for _, r := range b.regexPatternSets {
		list = append(list, cloneRegexPatternSet(r))
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// UpdateRegexPatternSet updates a RegexPatternSet by ID.
func (b *InMemoryBackend) UpdateRegexPatternSet(
	id, description string,
	regularExpressionList []string,
) (*RegexPatternSet, error) {
	b.mu.Lock("UpdateRegexPatternSet")
	defer b.mu.Unlock()

	r, ok := b.regexPatternSets[id]
	if !ok {
		return nil, fmt.Errorf("%w: regex pattern set %q not found", ErrRegexPatternSetNotFound, id)
	}

	if description != "" {
		r.Description = description
	}

	if regularExpressionList != nil {
		r.RegularExpressionList = cloneAddresses(regularExpressionList)
	}

	r.LockToken = uuid.NewString()

	return cloneRegexPatternSet(r), nil
}

// GetRuleGroup returns a RuleGroup by ID.
func (b *InMemoryBackend) GetRuleGroup(id string) (*RuleGroup, error) {
	b.mu.RLock("GetRuleGroup")
	defer b.mu.RUnlock()

	rg, ok := b.ruleGroups[id]
	if !ok {
		return nil, fmt.Errorf("%w: rule group %q not found", ErrRuleGroupNotFound, id)
	}

	return cloneRuleGroup(rg), nil
}

// ListRuleGroups returns all RuleGroups sorted by name.
func (b *InMemoryBackend) ListRuleGroups() []*RuleGroup {
	b.mu.RLock("ListRuleGroups")
	defer b.mu.RUnlock()

	list := make([]*RuleGroup, 0, len(b.ruleGroups))

	for _, rg := range b.ruleGroups {
		list = append(list, cloneRuleGroup(rg))
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// UpdateRuleGroup updates a RuleGroup by ID.
func (b *InMemoryBackend) UpdateRuleGroup(
	id, description, visibilityConfig string,
	rules []map[string]any,
) (*RuleGroup, error) {
	b.mu.Lock("UpdateRuleGroup")
	defer b.mu.Unlock()

	rg, ok := b.ruleGroups[id]
	if !ok {
		return nil, fmt.Errorf("%w: rule group %q not found", ErrRuleGroupNotFound, id)
	}

	if description != "" {
		rg.Description = description
	}

	if visibilityConfig != "" {
		rg.VisibilityConfig = visibilityConfig
	}

	if rules != nil {
		rg.Rules = cloneRules(rules)
	}

	rg.LockToken = uuid.NewString()

	return cloneRuleGroup(rg), nil
}

// ListAPIKeys returns all API keys, optionally filtered by scope.
func (b *InMemoryBackend) ListAPIKeys(scope string) []*APIKey {
	b.mu.RLock("ListAPIKeys")
	defer b.mu.RUnlock()

	list := make([]*APIKey, 0, len(b.apiKeys))

	for _, a := range b.apiKeys {
		if scope == "" || a.Scope == scope {
			list = append(list, &APIKey{
				APIKeyValue:  a.APIKeyValue,
				Scope:        a.Scope,
				TokenDomains: cloneAddresses(a.TokenDomains),
			})
		}
	}

	sort.Slice(list, func(i, j int) bool { return list[i].APIKeyValue < list[j].APIKeyValue })

	return list
}

// GetDecryptedAPIKey returns the API key identified by scope and key value.
func (b *InMemoryBackend) GetDecryptedAPIKey(scope, apiKey string) (*APIKey, error) {
	b.mu.RLock("GetDecryptedAPIKey")
	defer b.mu.RUnlock()

	a, ok := b.apiKeys[apiKeyMapKey(scope, apiKey)]
	if !ok {
		return nil, fmt.Errorf("%w: API key not found", ErrAPIKeyNotFound)
	}

	return &APIKey{
		APIKeyValue:  a.APIKeyValue,
		Scope:        a.Scope,
		TokenDomains: cloneAddresses(a.TokenDomains),
	}, nil
}

// GetLoggingConfiguration returns whether a logging configuration exists for the given resource ARN.
func (b *InMemoryBackend) GetLoggingConfiguration(resourceARN string) (bool, error) {
	b.mu.RLock("GetLoggingConfiguration")
	defer b.mu.RUnlock()

	if !b.loggingConfigs[resourceARN] {
		return false, fmt.Errorf(
			"%w: no logging configuration found for resource %q",
			ErrLoggingConfigNotFound,
			resourceARN,
		)
	}

	return true, nil
}

// GetPermissionPolicy returns the permission policy for the given resource ARN.
func (b *InMemoryBackend) GetPermissionPolicy(resourceARN string) (string, error) {
	b.mu.RLock("GetPermissionPolicy")
	defer b.mu.RUnlock()

	policy, ok := b.permissionPolicies[resourceARN]
	if !ok {
		return "", fmt.Errorf(
			"%w: no permission policy found for resource %q",
			ErrPermissionPolicyNotFound,
			resourceARN,
		)
	}

	return policy, nil
}

// ListResourcesForWebACL returns all resource ARNs associated with the given WebACL ARN.
func (b *InMemoryBackend) ListResourcesForWebACL(webACLARN string) ([]string, error) {
	b.mu.RLock("ListResourcesForWebACL")
	defer b.mu.RUnlock()

	if _, ok := b.webACLByARN[webACLARN]; !ok {
		return nil, fmt.Errorf("%w: web ACL with ARN %q not found", ErrWebACLNotFound, webACLARN)
	}

	webACLID := b.webACLByARN[webACLARN]
	result := make([]string, 0, len(b.associations))

	for resourceARN, wID := range b.associations {
		if wID == webACLID {
			result = append(result, resourceARN)
		}
	}

	sort.Strings(result)

	return result, nil
}

func cloneRegexPatternSet(r *RegexPatternSet) *RegexPatternSet {
	cp := *r
	cp.Tags = maps.Clone(r.Tags)
	cp.RegularExpressionList = cloneAddresses(r.RegularExpressionList)

	return &cp
}

func cloneRuleGroup(rg *RuleGroup) *RuleGroup {
	cp := *rg
	cp.Tags = maps.Clone(rg.Tags)
	cp.Rules = cloneRules(rg.Rules)

	return &cp
}

func cloneRules(rules []map[string]any) []map[string]any {
	if rules == nil {
		return []map[string]any{}
	}

	out := make([]map[string]any, len(rules))
	for i, r := range rules {
		rm := make(map[string]any, len(r))
		maps.Copy(rm, r)

		out[i] = rm
	}

	return out
}
