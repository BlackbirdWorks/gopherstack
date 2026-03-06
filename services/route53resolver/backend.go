package route53resolver

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	ErrNotFound      = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	ErrAlreadyExists = awserr.New("ResourceExistsException", awserr.ErrAlreadyExists)
)

type IPAddress struct {
	SubnetID string `json:"subnetID"`
	IP       string `json:"ip"`
}

type ResolverEndpoint struct {
	ID          string      `json:"id"`
	ARN         string      `json:"arn"`
	Direction   string      `json:"direction"`
	Name        string      `json:"name"`
	Status      string      `json:"status"`
	VpcID       string      `json:"vpcID"`
	AccountID   string      `json:"accountID"`
	Region      string      `json:"region"`
	IPAddresses []IPAddress `json:"ipAddresses"`
}

type ResolverRule struct {
	ID                 string `json:"id"`
	ARN                string `json:"arn"`
	Name               string `json:"name"`
	DomainName         string `json:"domainName"`
	RuleType           string `json:"ruleType"`
	Status             string `json:"status"`
	ResolverEndpointID string `json:"resolverEndpointID"`
	AccountID          string `json:"accountID"`
	Region             string `json:"region"`
}

type InMemoryBackend struct {
	endpoints map[string]*ResolverEndpoint
	rules     map[string]*ResolverRule
	mu        *lockmetrics.RWMutex
	accountID string
	region    string
}

func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		endpoints: make(map[string]*ResolverEndpoint),
		rules:     make(map[string]*ResolverRule),
		accountID: accountID,
		region:    region,
		mu:        lockmetrics.New("route53resolver"),
	}
}

const dirPrefixLen = 2

func (b *InMemoryBackend) CreateResolverEndpoint(
	name, direction, vpcID string,
	ips []IPAddress,
) (*ResolverEndpoint, error) {
	b.mu.Lock("CreateResolverEndpoint")
	defer b.mu.Unlock()

	dirPrefix := direction
	if len(dirPrefix) > dirPrefixLen {
		dirPrefix = dirPrefix[:dirPrefixLen]
	}
	id := "rslvr-" + dirPrefix + "-" + uuid.New().String()[:8]
	epARN := arn.Build("route53resolver", b.region, b.accountID, "resolver-endpoint/"+id)
	ep := &ResolverEndpoint{
		ID:          id,
		ARN:         epARN,
		Name:        name,
		Direction:   direction,
		Status:      "OPERATIONAL",
		VpcID:       vpcID,
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
	b.mu.RLock("GetResolverEndpoint")
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
	b.mu.RLock("ListResolverEndpoints")
	defer b.mu.RUnlock()

	list := make([]*ResolverEndpoint, 0, len(b.endpoints))
	for _, ep := range b.endpoints {
		cp := *ep
		cp.IPAddresses = make([]IPAddress, len(ep.IPAddresses))
		copy(cp.IPAddresses, ep.IPAddresses)
		list = append(list, &cp)
	}

	return list
}

func (b *InMemoryBackend) DeleteResolverEndpoint(id string) error {
	b.mu.Lock("DeleteResolverEndpoint")
	defer b.mu.Unlock()

	if _, ok := b.endpoints[id]; !ok {
		return fmt.Errorf("%w: resolver endpoint %s not found", ErrNotFound, id)
	}
	delete(b.endpoints, id)

	return nil
}

func (b *InMemoryBackend) CreateResolverRule(name, domainName, ruleType, endpointID string) (*ResolverRule, error) {
	b.mu.Lock("CreateResolverRule")
	defer b.mu.Unlock()

	id := "rslvr-rr-" + uuid.New().String()[:8]
	ruleARN := arn.Build("route53resolver", b.region, b.accountID, "resolver-rule/"+id)
	r := &ResolverRule{
		ID:                 id,
		ARN:                ruleARN,
		Name:               name,
		DomainName:         domainName,
		RuleType:           ruleType,
		Status:             "COMPLETE",
		ResolverEndpointID: endpointID,
		AccountID:          b.accountID,
		Region:             b.region,
	}
	b.rules[id] = r
	cp := *r

	return &cp, nil
}

func (b *InMemoryBackend) GetResolverRule(id string) (*ResolverRule, error) {
	b.mu.RLock("GetResolverRule")
	defer b.mu.RUnlock()

	r, ok := b.rules[id]
	if !ok {
		return nil, fmt.Errorf("%w: resolver rule %s not found", ErrNotFound, id)
	}
	cp := *r

	return &cp, nil
}

func (b *InMemoryBackend) ListResolverRules() []*ResolverRule {
	b.mu.RLock("ListResolverRules")
	defer b.mu.RUnlock()

	list := make([]*ResolverRule, 0, len(b.rules))
	for _, r := range b.rules {
		cp := *r
		list = append(list, &cp)
	}

	return list
}

func (b *InMemoryBackend) DeleteResolverRule(id string) error {
	b.mu.Lock("DeleteResolverRule")
	defer b.mu.Unlock()

	if _, ok := b.rules[id]; !ok {
		return fmt.Errorf("%w: resolver rule %s not found", ErrNotFound, id)
	}
	delete(b.rules, id)

	return nil
}
