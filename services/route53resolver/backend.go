package route53resolver

import (
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

var (
	ErrNotFound      = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	ErrAlreadyExists = awserr.New("ResourceExistsException", awserr.ErrAlreadyExists)
	ErrValidation    = awserr.New("InvalidRequestException", awserr.ErrInvalidParameter)
)

type IPAddress struct {
	IPID     string `json:"ipID"`
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

// FirewallRuleGroup represents a DNS Firewall rule group.
type FirewallRuleGroup struct {
	ID               string `json:"id"`
	ARN              string `json:"arn"`
	Name             string `json:"name"`
	CreatorRequestID string `json:"creatorRequestId"`
	Status           string `json:"status"`
	OwnerID          string `json:"ownerId"`
	RuleCount        int32  `json:"ruleCount"`
}

// FirewallRuleGroupAssociation represents an association between a rule group and a VPC.
type FirewallRuleGroupAssociation struct {
	ID                  string `json:"id"`
	ARN                 string `json:"arn"`
	Name                string `json:"name"`
	FirewallRuleGroupID string `json:"firewallRuleGroupId"`
	VpcID               string `json:"vpcId"`
	Status              string `json:"status"`
	Priority            int32  `json:"priority"`
}

// FirewallDomainList represents a DNS Firewall domain list.
type FirewallDomainList struct {
	ID               string `json:"id"`
	ARN              string `json:"arn"`
	Name             string `json:"name"`
	CreatorRequestID string `json:"creatorRequestId"`
	Status           string `json:"status"`
	DomainCount      int32  `json:"domainCount"`
}

// FirewallRule represents a single rule within a DNS Firewall rule group.
type FirewallRule struct {
	ID                   string `json:"id"`
	Name                 string `json:"name"`
	FirewallRuleGroupID  string `json:"firewallRuleGroupId"`
	FirewallDomainListID string `json:"firewallDomainListId"`
	Action               string `json:"action"`
	BlockResponse        string `json:"blockResponse"`
	Priority             int32  `json:"priority"`
}

// OutpostResolver represents a Resolver on an Outpost.
type OutpostResolver struct {
	ID                    string `json:"id"`
	ARN                   string `json:"arn"`
	Name                  string `json:"name"`
	CreatorRequestID      string `json:"creatorRequestId"`
	OutpostARN            string `json:"outpostArn"`
	PreferredInstanceType string `json:"preferredInstanceType"`
	Status                string `json:"status"`
	InstanceCount         int32  `json:"instanceCount"`
}

// ResolverQueryLogConfig represents a query logging configuration.
type ResolverQueryLogConfig struct {
	ID               string `json:"id"`
	ARN              string `json:"arn"`
	Name             string `json:"name"`
	CreatorRequestID string `json:"creatorRequestId"`
	DestinationARN   string `json:"destinationArn"`
	Status           string `json:"status"`
	OwnerID          string `json:"ownerId"`
}

// ResolverQueryLogConfigAssociation represents an association between a VPC and a query log config.
type ResolverQueryLogConfigAssociation struct {
	ID                       string `json:"id"`
	ResolverQueryLogConfigID string `json:"resolverQueryLogConfigId"`
	ResourceID               string `json:"resourceId"`
	Status                   string `json:"status"`
}

// ResolverRuleAssociation represents an association between a Resolver rule and a VPC.
type ResolverRuleAssociation struct {
	ID             string `json:"id"`
	Name           string `json:"name"`
	ResolverRuleID string `json:"resolverRuleId"`
	VPCID          string `json:"vpcId"`
	Status         string `json:"status"`
}

type InMemoryBackend struct {
	endpoints                     map[string]*ResolverEndpoint
	rules                         map[string]*ResolverRule
	tags                          map[string][]svcTags.KV
	firewallRuleGroups            map[string]*FirewallRuleGroup
	firewallRuleGroupAssociations map[string]*FirewallRuleGroupAssociation
	firewallDomainLists           map[string]*FirewallDomainList
	firewallRules                 map[string]*FirewallRule
	outpostResolvers              map[string]*OutpostResolver
	queryLogConfigs               map[string]*ResolverQueryLogConfig
	queryLogConfigAssociations    map[string]*ResolverQueryLogConfigAssociation
	ruleAssociations              map[string]*ResolverRuleAssociation
	mu                            *lockmetrics.RWMutex
	accountID                     string
	region                        string
}

func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		endpoints:                     make(map[string]*ResolverEndpoint),
		rules:                         make(map[string]*ResolverRule),
		tags:                          make(map[string][]svcTags.KV),
		firewallRuleGroups:            make(map[string]*FirewallRuleGroup),
		firewallRuleGroupAssociations: make(map[string]*FirewallRuleGroupAssociation),
		firewallDomainLists:           make(map[string]*FirewallDomainList),
		firewallRules:                 make(map[string]*FirewallRule),
		outpostResolvers:              make(map[string]*OutpostResolver),
		queryLogConfigs:               make(map[string]*ResolverQueryLogConfig),
		queryLogConfigAssociations:    make(map[string]*ResolverQueryLogConfigAssociation),
		ruleAssociations:              make(map[string]*ResolverRuleAssociation),
		accountID:                     accountID,
		region:                        region,
		mu:                            lockmetrics.New("route53resolver"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

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

	ipsCopy := make([]IPAddress, len(ips))
	for i, ip := range ips {
		ipsCopy[i] = ip
		if ipsCopy[i].IPID == "" {
			ipsCopy[i].IPID = "rni-" + uuid.New().String()[:8]
		}
	}

	ep := &ResolverEndpoint{
		ID:          id,
		ARN:         epARN,
		Name:        name,
		Direction:   direction,
		Status:      "OPERATIONAL",
		VpcID:       vpcID,
		IPAddresses: ipsCopy,
		AccountID:   b.accountID,
		Region:      b.region,
	}
	b.endpoints[id] = ep
	cp := *ep
	cp.IPAddresses = make([]IPAddress, len(ep.IPAddresses))
	copy(cp.IPAddresses, ep.IPAddresses)

	return &cp, nil
}

// ListResolverEndpointIPAddresses returns the IP addresses associated with a resolver endpoint.
func (b *InMemoryBackend) ListResolverEndpointIPAddresses(endpointID string) ([]IPAddress, error) {
	b.mu.RLock("ListResolverEndpointIpAddresses")
	defer b.mu.RUnlock()

	ep, ok := b.endpoints[endpointID]
	if !ok {
		return nil, fmt.Errorf("%w: resolver endpoint %s not found", ErrNotFound, endpointID)
	}
	cp := make([]IPAddress, len(ep.IPAddresses))
	copy(cp, ep.IPAddresses)

	return cp, nil
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

	toDelete := make([]string, 0)
	for ruleID, r := range b.rules {
		if r.ResolverEndpointID == id {
			toDelete = append(toDelete, ruleID)
		}
	}
	for _, ruleID := range toDelete {
		delete(b.rules, ruleID)
	}

	delete(b.endpoints, id)

	return nil
}

func (b *InMemoryBackend) CreateResolverRule(name, domainName, ruleType, endpointID string) (*ResolverRule, error) {
	b.mu.Lock("CreateResolverRule")
	defer b.mu.Unlock()

	if endpointID != "" {
		if _, ok := b.endpoints[endpointID]; !ok {
			return nil, fmt.Errorf("%w: resolver endpoint %s not found", ErrNotFound, endpointID)
		}
	}

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

// TagResource adds or updates tags on a resource identified by its ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, kvs []svcTags.KV) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	existing := b.tags[resourceARN]
	keyIdx := make(map[string]int, len(existing))
	for i, kv := range existing {
		keyIdx[kv.Key] = i
	}
	for _, kv := range kvs {
		if i, ok := keyIdx[kv.Key]; ok {
			existing[i].Value = kv.Value
		} else {
			existing = append(existing, kv)
			keyIdx[kv.Key] = len(existing) - 1
		}
	}
	sort.Slice(existing, func(i, j int) bool { return existing[i].Key < existing[j].Key })
	b.tags[resourceARN] = existing

	return nil
}

// UntagResource removes tags from a resource identified by its ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	existing := b.tags[resourceARN]
	keySet := make(map[string]bool, len(keys))
	for _, k := range keys {
		keySet[k] = true
	}
	remaining := make([]svcTags.KV, 0, len(existing))
	for _, kv := range existing {
		if !keySet[kv.Key] {
			remaining = append(remaining, kv)
		}
	}
	b.tags[resourceARN] = remaining

	return nil
}

// ListTagsForResource returns the tags for a resource identified by its ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) []svcTags.KV {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	kvs := b.tags[resourceARN]
	if len(kvs) == 0 {
		return []svcTags.KV{}
	}
	cp := make([]svcTags.KV, len(kvs))
	copy(cp, kvs)

	return cp
}

// CreateFirewallRuleGroup creates a new DNS Firewall rule group.
func (b *InMemoryBackend) CreateFirewallRuleGroup(name, creatorRequestID string) (*FirewallRuleGroup, error) {
	b.mu.Lock("CreateFirewallRuleGroup")
	defer b.mu.Unlock()

	id := "rslvr-frg-" + uuid.New().String()[:8]
	groupARN := arn.Build("route53resolver", b.region, b.accountID, "firewall-rule-group/"+id)
	g := &FirewallRuleGroup{
		ID:               id,
		ARN:              groupARN,
		Name:             name,
		CreatorRequestID: creatorRequestID,
		Status:           "COMPLETE",
		OwnerID:          b.accountID,
	}
	b.firewallRuleGroups[id] = g
	cp := *g

	return &cp, nil
}

// AssociateFirewallRuleGroup associates a FirewallRuleGroup with a VPC.
func (b *InMemoryBackend) AssociateFirewallRuleGroup(
	firewallRuleGroupID, vpcID, name, _ string,
	priority int32,
) (*FirewallRuleGroupAssociation, error) {
	b.mu.Lock("AssociateFirewallRuleGroup")
	defer b.mu.Unlock()

	if _, ok := b.firewallRuleGroups[firewallRuleGroupID]; !ok {
		return nil, fmt.Errorf("%w: firewall rule group %s not found", ErrNotFound, firewallRuleGroupID)
	}

	id := "rslvr-frgassoc-" + uuid.New().String()[:8]
	assocARN := arn.Build("route53resolver", b.region, b.accountID, "firewall-rule-group-association/"+id)
	assoc := &FirewallRuleGroupAssociation{
		ID:                  id,
		ARN:                 assocARN,
		Name:                name,
		FirewallRuleGroupID: firewallRuleGroupID,
		VpcID:               vpcID,
		Priority:            priority,
		Status:              "COMPLETE",
	}
	b.firewallRuleGroupAssociations[id] = assoc
	cp := *assoc

	return &cp, nil
}

// AssociateResolverEndpointIPAddress adds an IP address to a resolver endpoint.
func (b *InMemoryBackend) AssociateResolverEndpointIPAddress(
	endpointID, subnetID, ip string,
) (*ResolverEndpoint, error) {
	b.mu.Lock("AssociateResolverEndpointIPAddress")
	defer b.mu.Unlock()

	ep, ok := b.endpoints[endpointID]
	if !ok {
		return nil, fmt.Errorf("%w: resolver endpoint %s not found", ErrNotFound, endpointID)
	}

	newIP := IPAddress{
		IPID:     "rni-" + uuid.New().String()[:8],
		SubnetID: subnetID,
		IP:       ip,
	}
	ep.IPAddresses = append(ep.IPAddresses, newIP)

	cp := *ep
	cp.IPAddresses = make([]IPAddress, len(ep.IPAddresses))
	copy(cp.IPAddresses, ep.IPAddresses)

	return &cp, nil
}

// CreateResolverQueryLogConfig creates a new query logging configuration.
func (b *InMemoryBackend) CreateResolverQueryLogConfig(
	name, creatorRequestID, destinationARN string,
) (*ResolverQueryLogConfig, error) {
	b.mu.Lock("CreateResolverQueryLogConfig")
	defer b.mu.Unlock()

	id := "rqlc-" + uuid.New().String()[:8]
	configARN := arn.Build("route53resolver", b.region, b.accountID, "resolver-query-log-config/"+id)
	cfg := &ResolverQueryLogConfig{
		ID:               id,
		ARN:              configARN,
		Name:             name,
		CreatorRequestID: creatorRequestID,
		DestinationARN:   destinationARN,
		Status:           "CREATED",
		OwnerID:          b.accountID,
	}
	b.queryLogConfigs[id] = cfg
	cp := *cfg

	return &cp, nil
}

// AssociateResolverQueryLogConfig associates a VPC with a query log config.
func (b *InMemoryBackend) AssociateResolverQueryLogConfig(
	queryLogConfigID, resourceID string,
) (*ResolverQueryLogConfigAssociation, error) {
	b.mu.Lock("AssociateResolverQueryLogConfig")
	defer b.mu.Unlock()

	if _, ok := b.queryLogConfigs[queryLogConfigID]; !ok {
		return nil, fmt.Errorf("%w: resolver query log config %s not found", ErrNotFound, queryLogConfigID)
	}

	id := "rqlca-" + uuid.New().String()[:8]
	assoc := &ResolverQueryLogConfigAssociation{
		ID:                       id,
		ResolverQueryLogConfigID: queryLogConfigID,
		ResourceID:               resourceID,
		Status:                   "ACTIVE",
	}
	b.queryLogConfigAssociations[id] = assoc
	cp := *assoc

	return &cp, nil
}

// AssociateResolverRule associates a resolver rule with a VPC.
func (b *InMemoryBackend) AssociateResolverRule(resolverRuleID, vpcID, name string) (*ResolverRuleAssociation, error) {
	b.mu.Lock("AssociateResolverRule")
	defer b.mu.Unlock()

	if _, ok := b.rules[resolverRuleID]; !ok {
		return nil, fmt.Errorf("%w: resolver rule %s not found", ErrNotFound, resolverRuleID)
	}

	id := "rslvr-rrassoc-" + uuid.New().String()[:8]
	assoc := &ResolverRuleAssociation{
		ID:             id,
		Name:           name,
		ResolverRuleID: resolverRuleID,
		VPCID:          vpcID,
		Status:         "COMPLETE",
	}
	b.ruleAssociations[id] = assoc
	cp := *assoc

	return &cp, nil
}

// CreateFirewallDomainList creates a new DNS Firewall domain list.
func (b *InMemoryBackend) CreateFirewallDomainList(name, creatorRequestID string) (*FirewallDomainList, error) {
	b.mu.Lock("CreateFirewallDomainList")
	defer b.mu.Unlock()

	id := "rslvr-fdl-" + uuid.New().String()[:8]
	listARN := arn.Build("route53resolver", b.region, b.accountID, "firewall-domain-list/"+id)
	dl := &FirewallDomainList{
		ID:               id,
		ARN:              listARN,
		Name:             name,
		CreatorRequestID: creatorRequestID,
		Status:           "COMPLETE",
	}
	b.firewallDomainLists[id] = dl
	cp := *dl

	return &cp, nil
}

// DeleteFirewallDomainList deletes a DNS Firewall domain list.
func (b *InMemoryBackend) DeleteFirewallDomainList(id string) (*FirewallDomainList, error) {
	b.mu.Lock("DeleteFirewallDomainList")
	defer b.mu.Unlock()

	dl, ok := b.firewallDomainLists[id]
	if !ok {
		return nil, fmt.Errorf("%w: firewall domain list %s not found", ErrNotFound, id)
	}
	cp := *dl
	delete(b.firewallDomainLists, id)

	return &cp, nil
}

// CreateFirewallRule creates a new rule in a DNS Firewall rule group.
func (b *InMemoryBackend) CreateFirewallRule(
	firewallRuleGroupID, name, action, _ string,
	priority int32,
	firewallDomainListID string,
) (*FirewallRule, error) {
	b.mu.Lock("CreateFirewallRule")
	defer b.mu.Unlock()

	if _, ok := b.firewallRuleGroups[firewallRuleGroupID]; !ok {
		return nil, fmt.Errorf("%w: firewall rule group %s not found", ErrNotFound, firewallRuleGroupID)
	}

	id := "rslvr-frr-" + uuid.New().String()[:8]
	rule := &FirewallRule{
		ID:                   id,
		Name:                 name,
		FirewallRuleGroupID:  firewallRuleGroupID,
		FirewallDomainListID: firewallDomainListID,
		Action:               action,
		Priority:             priority,
	}
	b.firewallRules[id] = rule

	// Increment rule count on the group.
	b.firewallRuleGroups[firewallRuleGroupID].RuleCount++

	cp := *rule

	return &cp, nil
}

// CreateOutpostResolver creates a new Resolver on an Outpost.
func (b *InMemoryBackend) CreateOutpostResolver(
	name, creatorRequestID, outpostARN, preferredInstanceType string,
	instanceCount int32,
) (*OutpostResolver, error) {
	b.mu.Lock("CreateOutpostResolver")
	defer b.mu.Unlock()

	if instanceCount <= 0 {
		instanceCount = defaultOutpostResolverInstanceCount
	}

	id := "rslvr-op-" + uuid.New().String()[:8]
	resolverARN := arn.Build("route53resolver", b.region, b.accountID, "outpost-resolver/"+id)
	r := &OutpostResolver{
		ID:                    id,
		ARN:                   resolverARN,
		Name:                  name,
		CreatorRequestID:      creatorRequestID,
		OutpostARN:            outpostARN,
		PreferredInstanceType: preferredInstanceType,
		InstanceCount:         instanceCount,
		Status:                "OPERATIONAL",
	}
	b.outpostResolvers[id] = r
	cp := *r

	return &cp, nil
}

const defaultOutpostResolverInstanceCount int32 = 4
