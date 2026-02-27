package route53resolver

import (
	"errors"
	"fmt"
	"sync"

	"github.com/google/uuid"
)

var (
	ErrNotFound      = errors.New("ResourceNotFoundException")
	ErrAlreadyExists = errors.New("ResourceExistsException")
)

type IPAddress struct {
	SubnetId string
	IP       string
}

type ResolverEndpoint struct {
	ID          string
	ARN         string
	Direction   string
	Name        string
	Status      string
	VpcId       string
	IPAddresses []IPAddress
	AccountID   string
	Region      string
}

type ResolverRule struct {
	ID                 string
	ARN                string
	Name               string
	DomainName         string
	RuleType           string
	Status             string
	ResolverEndpointId string
	AccountID          string
	Region             string
}

type InMemoryBackend struct {
	endpoints map[string]*ResolverEndpoint
	rules     map[string]*ResolverRule
	accountID string
	region    string
	mu        sync.RWMutex
}

func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		endpoints: make(map[string]*ResolverEndpoint),
		rules:     make(map[string]*ResolverRule),
		accountID: accountID,
		region:    region,
	}
}

func (b *InMemoryBackend) CreateResolverEndpoint(name, direction, vpcId string, ips []IPAddress) (*ResolverEndpoint, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	dirPrefix := direction
	if len(dirPrefix) > 2 {
		dirPrefix = dirPrefix[:2]
	}
	id := "rslvr-" + dirPrefix + "-" + uuid.New().String()[:8]
	arn := fmt.Sprintf("arn:aws:route53resolver:%s:%s:resolver-endpoint/%s", b.region, b.accountID, id)
	ep := &ResolverEndpoint{
		ID:          id,
		ARN:         arn,
		Name:        name,
		Direction:   direction,
		Status:      "OPERATIONAL",
		VpcId:       vpcId,
		IPAddresses: ips,
		AccountID:   b.accountID,
		Region:      b.region,
	}
	b.endpoints[id] = ep
	cp := *ep
	cp.IPAddresses = make([]IPAddress, len(ep.IPAddresses))
	copy(cp.IPAddresses, ep.IPAddresses)
	return &cp, nil
}

func (b *InMemoryBackend) GetResolverEndpoint(id string) (*ResolverEndpoint, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	ep, ok := b.endpoints[id]
	if !ok {
		return nil, fmt.Errorf("%w: resolver endpoint %s not found", ErrNotFound, id)
	}
	cp := *ep
	cp.IPAddresses = make([]IPAddress, len(ep.IPAddresses))
	copy(cp.IPAddresses, ep.IPAddresses)
	return &cp, nil
}

func (b *InMemoryBackend) ListResolverEndpoints() []*ResolverEndpoint {
	b.mu.RLock()
	defer b.mu.RUnlock()

	list := make([]*ResolverEndpoint, 0, len(b.endpoints))
	for _, ep := range b.endpoints {
		cp := *ep
		list = append(list, &cp)
	}
	return list
}

func (b *InMemoryBackend) DeleteResolverEndpoint(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.endpoints[id]; !ok {
		return fmt.Errorf("%w: resolver endpoint %s not found", ErrNotFound, id)
	}
	delete(b.endpoints, id)
	return nil
}

func (b *InMemoryBackend) CreateResolverRule(name, domainName, ruleType, endpointId string) (*ResolverRule, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := "rslvr-rr-" + uuid.New().String()[:8]
	arn := fmt.Sprintf("arn:aws:route53resolver:%s:%s:resolver-rule/%s", b.region, b.accountID, id)
	r := &ResolverRule{
		ID:                 id,
		ARN:                arn,
		Name:               name,
		DomainName:         domainName,
		RuleType:           ruleType,
		Status:             "COMPLETE",
		ResolverEndpointId: endpointId,
		AccountID:          b.accountID,
		Region:             b.region,
	}
	b.rules[id] = r
	cp := *r
	return &cp, nil
}

func (b *InMemoryBackend) GetResolverRule(id string) (*ResolverRule, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	r, ok := b.rules[id]
	if !ok {
		return nil, fmt.Errorf("%w: resolver rule %s not found", ErrNotFound, id)
	}
	cp := *r
	return &cp, nil
}

func (b *InMemoryBackend) ListResolverRules() []*ResolverRule {
	b.mu.RLock()
	defer b.mu.RUnlock()

	list := make([]*ResolverRule, 0, len(b.rules))
	for _, r := range b.rules {
		cp := *r
		list = append(list, &cp)
	}
	return list
}

func (b *InMemoryBackend) DeleteResolverRule(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.rules[id]; !ok {
		return fmt.Errorf("%w: resolver rule %s not found", ErrNotFound, id)
	}
	delete(b.rules, id)
	return nil
}
