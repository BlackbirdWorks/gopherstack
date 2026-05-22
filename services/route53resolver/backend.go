package route53resolver

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

func currentTime() string {
	return time.Now().UTC().Format(time.RFC3339)
}

const (
	shareStatusNotShared = "NOT_SHARED"
)

var (
	ErrNotFound      = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	ErrAlreadyExists = awserr.New("ResourceExistsException", awserr.ErrAlreadyExists)
	ErrValidation    = awserr.New("InvalidRequestException", awserr.ErrInvalidParameter)
)

// Status and classification constants.
const (
	statusOperational = "OPERATIONAL"
	statusComplete    = "COMPLETE"
	statusCreated     = "CREATED"
	statusActive      = "ACTIVE"
	statusDeleting    = "DELETING"
	statusUpdating    = "UPDATING"

	directionInbound  = "INBOUND"
	directionOutbound = "OUTBOUND"

	ruleTypeForward   = "FORWARD"
	ruleTypeSystem    = "SYSTEM"
	ruleTypeRecursive = "RECURSIVE"

	firewallActionAllow = "ALLOW"
	firewallActionBlock = "BLOCK"
	firewallActionAlert = "ALERT"

	defaultOutpostResolverInstanceCount int32 = 4

	// firewallPriorityAutoIncrement is the step used when auto-assigning a priority.
	firewallPriorityAutoIncrement int32 = 100

	domainUpdateOpReplace = "REPLACE"
	domainUpdateOpAdd     = "ADD"
	domainUpdateOpRemove  = "REMOVE"

	firewallFailOpenEnabled  = "ENABLED"
	firewallFailOpenDisabled = "DISABLED"

	autodefinedReverseEnabled  = "ENABLE"
	autodefinedReverseDisabled = "DISABLE"

	dnssecValidationEnable  = "ENABLE"
	dnssecValidationDisable = "DISABLE"

	validationStatusEnabled  = "ENABLED"
	validationStatusEnabling = "ENABLING"
	validationStatusDisabled = "DISABLED"
	validationStatusDisabling = "DISABLING"

	mutationProtectionEnabled  = "ENABLED"
	mutationProtectionDisabled = "DISABLED"

	endpointTypeIPV4     = "IPV4"
	endpointTypeIPV6     = "IPV6"
	endpointTypeDualStack = "DUALSTACK"

	blockResponseNODATA  = "NODATA"
	blockResponseNXDOMAIN = "NXDOMAIN"
	blockResponseOVERRIDE = "OVERRIDE"
)

// Exported constants for use in demo seeding.
const (
	DirectionInbound        = directionInbound
	DirectionOutbound       = directionOutbound
	RuleTypeForward         = ruleTypeForward
	FirewallPriorityDefault = firewallPriorityAutoIncrement
)

type IPAddress struct {
	IPID     string `json:"ipID"`
	SubnetID string `json:"subnetID"`
	IP       string `json:"ip"`
	Ipv6     string `json:"ipv6,omitempty"`
}

type ResolverEndpoint struct {
	ID                    string       `json:"id"`
	ARN                   string       `json:"arn"`
	Direction             string       `json:"direction"`
	Name                  string       `json:"name"`
	Status                string       `json:"status"`
	StatusMessage         string       `json:"statusMessage,omitempty"`
	VpcID                 string       `json:"vpcID"`
	HostVPCID             string       `json:"hostVpcId"`
	AccountID             string       `json:"accountID"`
	Region                string       `json:"region"`
	ResolverEndpointType  string       `json:"resolverEndpointType"`
	SecurityGroupIDs      []string     `json:"securityGroupIds"`
	IPAddresses           []IPAddress  `json:"ipAddresses"`
	Tags                  []svcTags.KV `json:"tags,omitempty"`
	Protocols             []string     `json:"protocols,omitempty"`
	OutpostArn            string       `json:"outpostArn,omitempty"`
	PreferredInstanceType string       `json:"preferredInstanceType,omitempty"`
	CreatorRequestID      string       `json:"creatorRequestId,omitempty"`
	CreationTime          string       `json:"creationTime,omitempty"`
	ModificationTime      string       `json:"modificationTime,omitempty"`
}

type ResolverRule struct {
	ID                 string     `json:"id"`
	ARN                string     `json:"arn"`
	Name               string     `json:"name"`
	DomainName         string     `json:"domainName"`
	RuleType           string     `json:"ruleType"`
	Status             string     `json:"status"`
	StatusMessage      string     `json:"statusMessage,omitempty"`
	ShareStatus        string     `json:"shareStatus"`
	ResolverEndpointID string     `json:"resolverEndpointID"`
	AccountID          string     `json:"accountID"`
	Region             string     `json:"region"`
	TargetIps          []TargetIP `json:"targetIps,omitempty"`
	CreatorRequestID   string     `json:"creatorRequestId,omitempty"`
	OwnerId            string     `json:"ownerId,omitempty"`
	CreationTime       string     `json:"creationTime,omitempty"`
	ModificationTime   string     `json:"modificationTime,omitempty"`
}

// TargetIP represents a forwarding target IP for a resolver rule.
type TargetIP struct {
	IP       string `json:"ip"`
	Port     int32  `json:"port"`
	Ipv6     string `json:"ipv6,omitempty"`
	Protocol string `json:"protocol,omitempty"`
}

// FirewallRuleGroup represents a DNS Firewall rule group.
type FirewallRuleGroup struct {
	ID               string       `json:"id"`
	ARN              string       `json:"arn"`
	Name             string       `json:"name"`
	CreatorRequestID string       `json:"creatorRequestId"`
	Status           string       `json:"status"`
	StatusMessage    string       `json:"statusMessage,omitempty"`
	OwnerID          string       `json:"ownerId"`
	ShareStatus      string       `json:"shareStatus"`
	Tags             []svcTags.KV `json:"tags,omitempty"`
	RuleCount        int32        `json:"ruleCount"`
	CreationTime     string       `json:"creationTime,omitempty"`
	ModificationTime string       `json:"modificationTime,omitempty"`
}

// FirewallRuleGroupAssociation represents an association between a rule group and a VPC.
type FirewallRuleGroupAssociation struct {
	ID                  string `json:"id"`
	ARN                 string `json:"arn"`
	Name                string `json:"name"`
	FirewallRuleGroupID string `json:"firewallRuleGroupId"`
	VpcID               string `json:"vpcId"`
	Status              string `json:"status"`
	StatusMessage       string `json:"statusMessage,omitempty"`
	Priority            int32  `json:"priority"`
	MutationProtection  string `json:"mutationProtection"`
	ManagedOwnerName    string `json:"managedOwnerName,omitempty"`
	CreatorRequestID    string `json:"creatorRequestId,omitempty"`
	CreationTime        string `json:"creationTime,omitempty"`
	ModificationTime    string `json:"modificationTime,omitempty"`
}

// FirewallDomainList represents a DNS Firewall domain list.
type FirewallDomainList struct {
	ID               string       `json:"id"`
	ARN              string       `json:"arn"`
	Name             string       `json:"name"`
	CreatorRequestID string       `json:"creatorRequestId"`
	Status           string       `json:"status"`
	Tags             []svcTags.KV `json:"tags,omitempty"`
	Domains          []string     `json:"domains,omitempty"`
	DomainCount      int32        `json:"domainCount"`
	ManagedOwnerName string       `json:"managedOwnerName,omitempty"`
}

// FirewallRule represents a single rule within a DNS Firewall rule group.
type FirewallRule struct {
	ID                         string `json:"id"`
	ARN                        string `json:"arn"`
	Name                       string `json:"name"`
	FirewallRuleGroupID        string `json:"firewallRuleGroupId"`
	FirewallDomainListID       string `json:"firewallDomainListId"`
	Action                     string `json:"action"`
	BlockResponse              string `json:"blockResponse,omitempty"`
	BlockOverrideDomain        string `json:"blockOverrideDomain,omitempty"`
	BlockOverrideDnsType       string `json:"blockOverrideDnsType,omitempty"`
	BlockOverrideTtl           int32  `json:"blockOverrideTtl,omitempty"`
	Qtype                      string `json:"qtype,omitempty"`
	ConfidenceThreshold        string `json:"confidenceThreshold,omitempty"`
	CreatorRequestID           string `json:"creatorRequestId,omitempty"`
	CreationTime               string `json:"creationTime,omitempty"`
	ModificationTime           string `json:"modificationTime,omitempty"`
	Priority                   int32  `json:"priority"`
}

// OutpostResolver represents a Resolver on an Outpost.
type OutpostResolver struct {
	ID                    string       `json:"id"`
	ARN                   string       `json:"arn"`
	Name                  string       `json:"name"`
	CreatorRequestID      string       `json:"creatorRequestId"`
	OutpostARN            string       `json:"outpostArn"`
	PreferredInstanceType string       `json:"preferredInstanceType"`
	Status                string       `json:"status"`
	Tags                  []svcTags.KV `json:"tags,omitempty"`
	InstanceCount         int32        `json:"instanceCount"`
}

// ResolverQueryLogConfig represents a query logging configuration.
type ResolverQueryLogConfig struct {
	ID               string       `json:"id"`
	ARN              string       `json:"arn"`
	Name             string       `json:"name"`
	CreatorRequestID string       `json:"creatorRequestId"`
	DestinationARN   string       `json:"destinationArn"`
	Status           string       `json:"status"`
	OwnerID          string       `json:"ownerId"`
	Tags             []svcTags.KV `json:"tags,omitempty"`
	AssociationCount int32        `json:"associationCount"`
	ShareStatus      string       `json:"shareStatus"`
	CreationTime     string       `json:"creationTime,omitempty"`
}

// ResolverQueryLogConfigAssociation represents an association between a VPC and a query log config.
type ResolverQueryLogConfigAssociation struct {
	ID                       string `json:"id"`
	ResolverQueryLogConfigID string `json:"resolverQueryLogConfigId"`
	ResourceID               string `json:"resourceId"`
	Status                   string `json:"status"`
	Error                    string `json:"error,omitempty"`
	ErrorMessage             string `json:"errorMessage,omitempty"`
	CreationTime             string `json:"creationTime,omitempty"`
}

// ResolverRuleAssociation represents an association between a Resolver rule and a VPC.
type ResolverRuleAssociation struct {
	ID             string `json:"id"`
	Name           string `json:"name"`
	ResolverRuleID string `json:"resolverRuleId"`
	VPCID          string `json:"vpcId"`
	Status         string `json:"status"`
}

// FirewallConfig represents the DNS Firewall configuration for a VPC.
type FirewallConfig struct {
	ID               string `json:"id"`
	OwnerID          string `json:"ownerId"`
	ResourceID       string `json:"resourceId"`
	FirewallFailOpen string `json:"firewallFailOpen"`
}

// ResolverConfig represents the Resolver configuration for a VPC.
type ResolverConfig struct {
	ID                 string `json:"id"`
	ARN                string `json:"arn"`
	OwnerID            string `json:"ownerId"`
	ResourceID         string `json:"resourceId"`
	AutodefinedReverse string `json:"autodefinedReverse"`
}

// ResolverDnssecConfig represents the DNSSEC configuration for a VPC.
type ResolverDnssecConfig struct {
	ID               string `json:"id"`
	OwnerID          string `json:"ownerId"`
	ResourceID       string `json:"resourceId"`
	ValidationStatus string `json:"validationStatus"`
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
	firewallConfigs               map[string]*FirewallConfig
	resolverConfigs               map[string]*ResolverConfig
	resolverDnssecConfigs         map[string]*ResolverDnssecConfig
	firewallRuleGroupPolicies     map[string]string
	queryLogConfigPolicies        map[string]string
	resolverRulePolicies          map[string]string
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
		firewallConfigs:               make(map[string]*FirewallConfig),
		resolverConfigs:               make(map[string]*ResolverConfig),
		resolverDnssecConfigs:         make(map[string]*ResolverDnssecConfig),
		firewallRuleGroupPolicies:     make(map[string]string),
		queryLogConfigPolicies:        make(map[string]string),
		resolverRulePolicies:          make(map[string]string),
		accountID:                     accountID,
		region:                        region,
		mu:                            lockmetrics.New("route53resolver"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the AWS account ID this backend is configured for.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Reset clears all backend state, returning it to an empty initial state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.endpoints = make(map[string]*ResolverEndpoint)
	b.rules = make(map[string]*ResolverRule)
	b.tags = make(map[string][]svcTags.KV)
	b.firewallRuleGroups = make(map[string]*FirewallRuleGroup)
	b.firewallRuleGroupAssociations = make(map[string]*FirewallRuleGroupAssociation)
	b.firewallDomainLists = make(map[string]*FirewallDomainList)
	b.firewallRules = make(map[string]*FirewallRule)
	b.outpostResolvers = make(map[string]*OutpostResolver)
	b.queryLogConfigs = make(map[string]*ResolverQueryLogConfig)
	b.queryLogConfigAssociations = make(map[string]*ResolverQueryLogConfigAssociation)
	b.ruleAssociations = make(map[string]*ResolverRuleAssociation)
	b.firewallConfigs = make(map[string]*FirewallConfig)
	b.resolverConfigs = make(map[string]*ResolverConfig)
	b.resolverDnssecConfigs = make(map[string]*ResolverDnssecConfig)
	b.firewallRuleGroupPolicies = make(map[string]string)
	b.queryLogConfigPolicies = make(map[string]string)
	b.resolverRulePolicies = make(map[string]string)
}

const dirPrefixLen = 2

func (b *InMemoryBackend) CreateResolverEndpoint(
	name, direction, vpcID string,
	ips []IPAddress,
	securityGroupIDs []string,
	resolverEndpointType string,
	protocols []string,
	outpostArn, preferredInstanceType, creatorRequestID string,
) (*ResolverEndpoint, error) {
	b.mu.Lock("CreateResolverEndpoint")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	if direction != directionInbound && direction != directionOutbound {
		return nil, fmt.Errorf("%w: Direction must be %s or %s", ErrValidation, directionInbound, directionOutbound)
	}

	if resolverEndpointType == "" {
		resolverEndpointType = endpointTypeIPV4
	}
	switch resolverEndpointType {
	case endpointTypeIPV4, endpointTypeIPV6, endpointTypeDualStack:
		// valid
	default:
		return nil, fmt.Errorf("%w: ResolverEndpointType must be IPV4, IPV6, or DUALSTACK", ErrValidation)
	}

	if len(protocols) == 0 {
		protocols = []string{"Do53"}
	}

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

	sgCopy := make([]string, len(securityGroupIDs))
	copy(sgCopy, securityGroupIDs)

	protocolsCopy := make([]string, len(protocols))
	copy(protocolsCopy, protocols)

	now := currentTime()
	ep := &ResolverEndpoint{
		ID:                    id,
		ARN:                   epARN,
		Name:                  name,
		Direction:             direction,
		Status:                statusOperational,
		VpcID:                 vpcID,
		HostVPCID:             vpcID,
		IPAddresses:           ipsCopy,
		SecurityGroupIDs:      sgCopy,
		ResolverEndpointType:  resolverEndpointType,
		AccountID:             b.accountID,
		Region:                b.region,
		Protocols:             protocolsCopy,
		OutpostArn:            outpostArn,
		PreferredInstanceType: preferredInstanceType,
		CreatorRequestID:      creatorRequestID,
		CreationTime:          now,
		ModificationTime:      now,
	}
	b.endpoints[id] = ep

	return cloneEndpoint(ep), nil
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

	return cloneEndpoint(ep), nil
}

func (b *InMemoryBackend) ListResolverEndpoints() []*ResolverEndpoint {
	b.mu.RLock("ListResolverEndpoints")
	defer b.mu.RUnlock()

	list := make([]*ResolverEndpoint, 0, len(b.endpoints))
	for _, ep := range b.endpoints {
		list = append(list, cloneEndpoint(ep))
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

func (b *InMemoryBackend) DeleteResolverEndpoint(id string) error {
	b.mu.Lock("DeleteResolverEndpoint")
	defer b.mu.Unlock()

	ep, ok := b.endpoints[id]
	if !ok {
		return fmt.Errorf("%w: resolver endpoint %s not found", ErrNotFound, id)
	}

	// Clean up tags.
	delete(b.tags, ep.ARN)

	toDelete := make([]string, 0, len(b.rules))
	for ruleID, r := range b.rules {
		if r.ResolverEndpointID == id {
			toDelete = append(toDelete, ruleID)
		}
	}
	for _, ruleID := range toDelete {
		// Cascade: delete tags and all rule associations referencing this rule.
		if rule, exists := b.rules[ruleID]; exists {
			delete(b.tags, rule.ARN)
		}
		for assocID, assoc := range b.ruleAssociations {
			if assoc.ResolverRuleID == ruleID {
				delete(b.ruleAssociations, assocID)
			}
		}
		delete(b.rules, ruleID)
	}

	delete(b.endpoints, id)

	return nil
}

func (b *InMemoryBackend) CreateResolverRule(
	name, domainName, ruleType, endpointID, creatorRequestID string,
	targetIps []TargetIP,
) (*ResolverRule, error) {
	b.mu.Lock("CreateResolverRule")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	if domainName == "" {
		return nil, fmt.Errorf("%w: DomainName is required", ErrValidation)
	}

	switch ruleType {
	case ruleTypeForward, ruleTypeSystem, ruleTypeRecursive:
		// valid
	default:
		return nil, fmt.Errorf(
			"%w: RuleType must be %s, %s, or %s",
			ErrValidation,
			ruleTypeForward,
			ruleTypeSystem,
			ruleTypeRecursive,
		)
	}

	// SYSTEM and RECURSIVE rules must not have TargetIps or ResolverEndpointId.
	if ruleType == ruleTypeSystem || ruleType == ruleTypeRecursive {
		if endpointID != "" {
			return nil, fmt.Errorf("%w: SYSTEM/RECURSIVE rules must not have a ResolverEndpointId", ErrValidation)
		}
		if len(targetIps) > 0 {
			return nil, fmt.Errorf("%w: SYSTEM/RECURSIVE rules must not have TargetIps", ErrValidation)
		}
	}

	if endpointID != "" {
		if _, ok := b.endpoints[endpointID]; !ok {
			return nil, fmt.Errorf("%w: resolver endpoint %s not found", ErrNotFound, endpointID)
		}
	}

	var tipsCopy []TargetIP
	if len(targetIps) > 0 {
		tipsCopy = make([]TargetIP, len(targetIps))
		copy(tipsCopy, targetIps)
	}

	now := currentTime()
	id := "rslvr-rr-" + uuid.New().String()[:8]
	ruleARN := arn.Build("route53resolver", b.region, b.accountID, "resolver-rule/"+id)
	r := &ResolverRule{
		ID:                 id,
		ARN:                ruleARN,
		Name:               name,
		DomainName:         domainName,
		RuleType:           ruleType,
		Status:             statusComplete,
		ShareStatus:        shareStatusNotShared,
		ResolverEndpointID: endpointID,
		AccountID:          b.accountID,
		Region:             b.region,
		TargetIps:          tipsCopy,
		CreatorRequestID:   creatorRequestID,
		OwnerId:            b.accountID,
		CreationTime:       now,
		ModificationTime:   now,
	}
	b.rules[id] = r
	cp := cloneRule(r)

	return cp, nil
}

func (b *InMemoryBackend) GetResolverRule(id string) (*ResolverRule, error) {
	b.mu.RLock("GetResolverRule")
	defer b.mu.RUnlock()

	r, ok := b.rules[id]
	if !ok {
		return nil, fmt.Errorf("%w: resolver rule %s not found", ErrNotFound, id)
	}

	return cloneRule(r), nil
}

func (b *InMemoryBackend) ListResolverRules() []*ResolverRule {
	b.mu.RLock("ListResolverRules")
	defer b.mu.RUnlock()

	list := make([]*ResolverRule, 0, len(b.rules))
	for _, r := range b.rules {
		list = append(list, cloneRule(r))
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

func (b *InMemoryBackend) DeleteResolverRule(id string) error {
	b.mu.Lock("DeleteResolverRule")
	defer b.mu.Unlock()

	r, ok := b.rules[id]
	if !ok {
		return fmt.Errorf("%w: resolver rule %s not found", ErrNotFound, id)
	}

	// Clean up tags.
	delete(b.tags, r.ARN)

	// Cascade: delete all associations referencing this rule.
	for assocID, assoc := range b.ruleAssociations {
		if assoc.ResolverRuleID == id {
			delete(b.ruleAssociations, assocID)
		}
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

	now := currentTime()
	id := "rslvr-frg-" + uuid.New().String()[:8]
	groupARN := arn.Build("route53resolver", b.region, b.accountID, "firewall-rule-group/"+id)
	g := &FirewallRuleGroup{
		ID:               id,
		ARN:              groupARN,
		Name:             name,
		CreatorRequestID: creatorRequestID,
		Status:           statusComplete,
		OwnerID:          b.accountID,
		ShareStatus:      shareStatusNotShared,
		CreationTime:     now,
		ModificationTime: now,
	}
	b.firewallRuleGroups[id] = g
	cp := *g

	return &cp, nil
}

// AssociateFirewallRuleGroup associates a FirewallRuleGroup with a VPC.
func (b *InMemoryBackend) AssociateFirewallRuleGroup(
	firewallRuleGroupID, vpcID, name, creatorRequestID, mutationProtection string,
	priority int32,
) (*FirewallRuleGroupAssociation, error) {
	b.mu.Lock("AssociateFirewallRuleGroup")
	defer b.mu.Unlock()

	if _, ok := b.firewallRuleGroups[firewallRuleGroupID]; !ok {
		return nil, fmt.Errorf("%w: firewall rule group %s not found", ErrNotFound, firewallRuleGroupID)
	}

	if mutationProtection == "" {
		mutationProtection = mutationProtectionDisabled
	}

	now := currentTime()
	id := "rslvr-frgassoc-" + uuid.New().String()[:8]
	assocARN := arn.Build("route53resolver", b.region, b.accountID, "firewall-rule-group-association/"+id)
	assoc := &FirewallRuleGroupAssociation{
		ID:                  id,
		ARN:                 assocARN,
		Name:                name,
		FirewallRuleGroupID: firewallRuleGroupID,
		VpcID:               vpcID,
		Priority:            priority,
		Status:              statusComplete,
		MutationProtection:  mutationProtection,
		CreatorRequestID:    creatorRequestID,
		CreationTime:        now,
		ModificationTime:    now,
	}
	b.firewallRuleGroupAssociations[id] = assoc
	cp := *assoc

	return &cp, nil
}

// AssociateResolverEndpointIPAddress adds an IP address to a resolver endpoint.
func (b *InMemoryBackend) AssociateResolverEndpointIPAddress(
	endpointID, subnetID, ip, ipv6 string,
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
		Ipv6:     ipv6,
	}
	ep.IPAddresses = append(ep.IPAddresses, newIP)
	ep.ModificationTime = currentTime()

	return cloneEndpoint(ep), nil
}

// validQueryLogDestinationPrefixes lists accepted ARN prefixes for query log destinations.
var validQueryLogDestinationPrefixes = []string{
	"arn:aws:s3:::",
	"arn:aws:logs:",
	"arn:aws:firehose:",
}

// CreateResolverQueryLogConfig creates a new query logging configuration.
func (b *InMemoryBackend) CreateResolverQueryLogConfig(
	name, creatorRequestID, destinationARN string,
) (*ResolverQueryLogConfig, error) {
	b.mu.Lock("CreateResolverQueryLogConfig")
	defer b.mu.Unlock()

	if !isValidQueryLogDestination(destinationARN) {
		return nil, fmt.Errorf(
			"%w: DestinationArn must be an S3 bucket, CloudWatch Logs log group, or Kinesis Firehose stream ARN",
			ErrValidation,
		)
	}

	now := currentTime()
	id := "rqlc-" + uuid.New().String()[:8]
	configARN := arn.Build("route53resolver", b.region, b.accountID, "resolver-query-log-config/"+id)
	cfg := &ResolverQueryLogConfig{
		ID:               id,
		ARN:              configARN,
		Name:             name,
		CreatorRequestID: creatorRequestID,
		DestinationARN:   destinationARN,
		Status:           statusCreated,
		OwnerID:          b.accountID,
		ShareStatus:      shareStatusNotShared,
		CreationTime:     now,
	}
	b.queryLogConfigs[id] = cfg
	cp := *cfg

	return &cp, nil
}

func isValidQueryLogDestination(destinationARN string) bool {
	for _, prefix := range validQueryLogDestinationPrefixes {
		if strings.HasPrefix(destinationARN, prefix) {
			return true
		}
	}

	return false
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

	now := currentTime()
	id := "rqlca-" + uuid.New().String()[:8]
	assoc := &ResolverQueryLogConfigAssociation{
		ID:                       id,
		ResolverQueryLogConfigID: queryLogConfigID,
		ResourceID:               resourceID,
		Status:                   statusActive,
		CreationTime:             now,
	}
	b.queryLogConfigAssociations[id] = assoc

	// Increment AssociationCount on the config.
	if cfg, ok := b.queryLogConfigs[queryLogConfigID]; ok {
		cfg.AssociationCount++
	}

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
		Status:         statusComplete,
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
		Status:           statusComplete,
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
	cp := cloneFirewallDomainList(dl)
	delete(b.tags, dl.ARN)
	delete(b.firewallDomainLists, id)

	return cp, nil
}

// CreateFirewallRuleParams holds all parameters for creating a firewall rule.
type CreateFirewallRuleParams struct {
	FirewallRuleGroupID  string
	Name                 string
	Action               string
	BlockResponse        string
	BlockOverrideDomain  string
	BlockOverrideDnsType string
	BlockOverrideTtl     int32
	Qtype                string
	ConfidenceThreshold  string
	CreatorRequestID     string
	FirewallDomainListID string
	Priority             int32
}

// CreateFirewallRule creates a new rule in a DNS Firewall rule group.
func (b *InMemoryBackend) CreateFirewallRule(p CreateFirewallRuleParams) (*FirewallRule, error) {
	b.mu.Lock("CreateFirewallRule")
	defer b.mu.Unlock()

	if _, ok := b.firewallRuleGroups[p.FirewallRuleGroupID]; !ok {
		return nil, fmt.Errorf("%w: firewall rule group %s not found", ErrNotFound, p.FirewallRuleGroupID)
	}

	// Validate BLOCK+OVERRIDE requires BlockOverrideDomain and BlockOverrideDnsType.
	if p.Action == firewallActionBlock && p.BlockResponse == blockResponseOVERRIDE {
		if p.BlockOverrideDomain == "" {
			return nil, fmt.Errorf("%w: BlockOverrideDomain is required when BlockResponse is OVERRIDE", ErrValidation)
		}
		if p.BlockOverrideDnsType == "" {
			return nil, fmt.Errorf("%w: BlockOverrideDnsType is required when BlockResponse is OVERRIDE", ErrValidation)
		}
	}

	// Auto-assign priority if not provided.
	if p.Priority == 0 {
		maxPriority := int32(0)
		for _, existing := range b.firewallRules {
			if existing.FirewallRuleGroupID == p.FirewallRuleGroupID && existing.Priority > maxPriority {
				maxPriority = existing.Priority
			}
		}
		p.Priority = maxPriority + firewallPriorityAutoIncrement
	}

	// Validate priority uniqueness within the rule group.
	for _, existing := range b.firewallRules {
		if existing.FirewallRuleGroupID == p.FirewallRuleGroupID && existing.Priority == p.Priority {
			return nil, fmt.Errorf("%w: a firewall rule with priority %d already exists in group %s",
				ErrValidation, p.Priority, p.FirewallRuleGroupID)
		}
	}

	now := currentTime()
	id := "rslvr-frr-" + uuid.New().String()[:8]
	ruleARN := arn.Build("route53resolver", b.region, b.accountID, "firewall-rule/"+id)
	rule := &FirewallRule{
		ID:                   id,
		ARN:                  ruleARN,
		Name:                 p.Name,
		FirewallRuleGroupID:  p.FirewallRuleGroupID,
		FirewallDomainListID: p.FirewallDomainListID,
		Action:               p.Action,
		BlockResponse:        p.BlockResponse,
		BlockOverrideDomain:  p.BlockOverrideDomain,
		BlockOverrideDnsType: p.BlockOverrideDnsType,
		BlockOverrideTtl:     p.BlockOverrideTtl,
		Qtype:                p.Qtype,
		ConfidenceThreshold:  p.ConfidenceThreshold,
		CreatorRequestID:     p.CreatorRequestID,
		CreationTime:         now,
		ModificationTime:     now,
		Priority:             p.Priority,
	}
	b.firewallRules[id] = rule

	// Increment rule count on the group.
	b.firewallRuleGroups[p.FirewallRuleGroupID].RuleCount++

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
		Status:                statusOperational,
	}
	b.outpostResolvers[id] = r
	cp := *r

	return &cp, nil
}

// cloneEndpoint returns a deep copy of a ResolverEndpoint.
func cloneEndpoint(ep *ResolverEndpoint) *ResolverEndpoint {
	cp := *ep
	cp.IPAddresses = make([]IPAddress, len(ep.IPAddresses))
	copy(cp.IPAddresses, ep.IPAddresses)

	if ep.SecurityGroupIDs != nil {
		cp.SecurityGroupIDs = make([]string, len(ep.SecurityGroupIDs))
		copy(cp.SecurityGroupIDs, ep.SecurityGroupIDs)
	} else {
		cp.SecurityGroupIDs = []string{}
	}

	if ep.Protocols != nil {
		cp.Protocols = make([]string, len(ep.Protocols))
		copy(cp.Protocols, ep.Protocols)
	}

	return &cp
}

// cloneRule returns a deep copy of a ResolverRule.
func cloneRule(r *ResolverRule) *ResolverRule {
	cp := *r
	if r.TargetIps != nil {
		cp.TargetIps = make([]TargetIP, len(r.TargetIps))
		copy(cp.TargetIps, r.TargetIps)
	}

	return &cp
}

// AddEndpointInternal adds a resolver endpoint directly to the backend (test seed helper).
func (b *InMemoryBackend) AddEndpointInternal(name, direction string) *ResolverEndpoint {
	b.mu.Lock("AddEndpointInternal")
	defer b.mu.Unlock()

	id := "rslvr-in-" + uuid.New().String()[:8]
	epARN := arn.Build("route53resolver", b.region, b.accountID, "resolver-endpoint/"+id)
	ep := &ResolverEndpoint{
		ID:               id,
		ARN:              epARN,
		Name:             name,
		Direction:        direction,
		Status:           statusOperational,
		IPAddresses:      []IPAddress{},
		SecurityGroupIDs: []string{},
		AccountID:        b.accountID,
		Region:           b.region,
	}
	b.endpoints[id] = ep

	return cloneEndpoint(ep)
}

// AddRuleInternal adds a resolver rule directly to the backend (test seed helper).
func (b *InMemoryBackend) AddRuleInternal(name, domainName, ruleType string) *ResolverRule {
	b.mu.Lock("AddRuleInternal")
	defer b.mu.Unlock()

	id := "rslvr-rr-" + uuid.New().String()[:8]
	ruleARN := arn.Build("route53resolver", b.region, b.accountID, "resolver-rule/"+id)
	r := &ResolverRule{
		ID:          id,
		ARN:         ruleARN,
		Name:        name,
		DomainName:  domainName,
		RuleType:    ruleType,
		Status:      statusComplete,
		ShareStatus: shareStatusNotShared,
		AccountID:   b.accountID,
		Region:      b.region,
	}
	b.rules[id] = r

	return cloneRule(r)
}

// AddFirewallRuleGroupInternal adds a firewall rule group directly to the backend (test seed helper).
func (b *InMemoryBackend) AddFirewallRuleGroupInternal(name string) *FirewallRuleGroup {
	b.mu.Lock("AddFirewallRuleGroupInternal")
	defer b.mu.Unlock()

	id := "rslvr-frg-" + uuid.New().String()[:8]
	groupARN := arn.Build("route53resolver", b.region, b.accountID, "firewall-rule-group/"+id)
	g := &FirewallRuleGroup{
		ID:      id,
		ARN:     groupARN,
		Name:    name,
		Status:  statusComplete,
		OwnerID: b.accountID,
	}
	b.firewallRuleGroups[id] = g
	cp := *g

	return &cp
}

// AddFirewallDomainListInternal adds a firewall domain list directly to the backend (test seed helper).
func (b *InMemoryBackend) AddFirewallDomainListInternal(name string) *FirewallDomainList {
	b.mu.Lock("AddFirewallDomainListInternal")
	defer b.mu.Unlock()

	id := "rslvr-fdl-" + uuid.New().String()[:8]
	listARN := arn.Build("route53resolver", b.region, b.accountID, "firewall-domain-list/"+id)
	dl := &FirewallDomainList{
		ID:     id,
		ARN:    listARN,
		Name:   name,
		Status: statusComplete,
	}
	b.firewallDomainLists[id] = dl
	cp := *dl

	return &cp
}

// AddOutpostResolverInternal adds an outpost resolver directly to the backend (test seed helper).
func (b *InMemoryBackend) AddOutpostResolverInternal(name, outpostARN string) *OutpostResolver {
	b.mu.Lock("AddOutpostResolverInternal")
	defer b.mu.Unlock()

	id := "rslvr-op-" + uuid.New().String()[:8]
	resolverARN := arn.Build("route53resolver", b.region, b.accountID, "outpost-resolver/"+id)
	r := &OutpostResolver{
		ID:            id,
		ARN:           resolverARN,
		Name:          name,
		OutpostARN:    outpostARN,
		InstanceCount: defaultOutpostResolverInstanceCount,
		Status:        statusOperational,
	}
	b.outpostResolvers[id] = r
	cp := *r

	return &cp
}

// AddQueryLogConfigInternal adds a query log config directly to the backend (test seed helper).
func (b *InMemoryBackend) AddQueryLogConfigInternal(name, destinationARN string) *ResolverQueryLogConfig {
	b.mu.Lock("AddQueryLogConfigInternal")
	defer b.mu.Unlock()

	id := "rqlc-" + uuid.New().String()[:8]
	configARN := arn.Build("route53resolver", b.region, b.accountID, "resolver-query-log-config/"+id)
	cfg := &ResolverQueryLogConfig{
		ID:             id,
		ARN:            configARN,
		Name:           name,
		DestinationARN: destinationARN,
		Status:         statusCreated,
		OwnerID:        b.accountID,
	}
	b.queryLogConfigs[id] = cfg
	cp := *cfg

	return &cp
}

// AddRuleInternalWithEndpoint adds a resolver rule with an endpoint ID directly to the backend (demo seed helper).
func (b *InMemoryBackend) AddRuleInternalWithEndpoint(name, domainName, ruleType, endpointID string) *ResolverRule {
	b.mu.Lock("AddRuleInternalWithEndpoint")
	defer b.mu.Unlock()

	id := "rslvr-rr-" + uuid.New().String()[:8]
	ruleARN := arn.Build("route53resolver", b.region, b.accountID, "resolver-rule/"+id)
	r := &ResolverRule{
		ID:                 id,
		ARN:                ruleARN,
		Name:               name,
		DomainName:         domainName,
		RuleType:           ruleType,
		Status:             statusComplete,
		ShareStatus:        shareStatusNotShared,
		ResolverEndpointID: endpointID,
		AccountID:          b.accountID,
		Region:             b.region,
	}
	b.rules[id] = r

	return cloneRule(r)
}

// AddFirewallRuleInternal adds a firewall rule directly to the backend (demo seed helper).
func (b *InMemoryBackend) AddFirewallRuleInternal(
	groupID, name, action, domainListID string,
	priority int32,
) *FirewallRule {
	b.mu.Lock("AddFirewallRuleInternal")
	defer b.mu.Unlock()

	grp, ok := b.firewallRuleGroups[groupID]
	if !ok {
		return nil
	}

	now := currentTime()
	id := "rslvr-frr-" + uuid.New().String()[:8]
	ruleARN := arn.Build("route53resolver", b.region, b.accountID, "firewall-rule/"+id)
	rule := &FirewallRule{
		ID:                   id,
		ARN:                  ruleARN,
		Name:                 name,
		FirewallRuleGroupID:  groupID,
		FirewallDomainListID: domainListID,
		Action:               action,
		Priority:             priority,
		CreationTime:         now,
		ModificationTime:     now,
	}
	b.firewallRules[id] = rule
	grp.RuleCount++
	cp := *rule

	return &cp
}

// --- Firewall Rule operations ---

// DeleteFirewallRule deletes a firewall rule by ID and decrements the group rule count.
func (b *InMemoryBackend) DeleteFirewallRule(id string) (*FirewallRule, error) {
	b.mu.Lock("DeleteFirewallRule")
	defer b.mu.Unlock()

	rule, ok := b.firewallRules[id]
	if !ok {
		return nil, fmt.Errorf("%w: firewall rule %s not found", ErrNotFound, id)
	}
	cp := *rule
	if grp, exists := b.firewallRuleGroups[rule.FirewallRuleGroupID]; exists && grp.RuleCount > 0 {
		grp.RuleCount--
	}
	delete(b.firewallRules, id)

	return &cp, nil
}

// UpdateFirewallRuleParams holds all updatable fields for a firewall rule.
type UpdateFirewallRuleParams struct {
	ID                   string
	Name                 string
	Action               string
	BlockResponse        string
	BlockOverrideDomain  string
	BlockOverrideDnsType string
	BlockOverrideTtl     int32
	Qtype                string
	ConfidenceThreshold  string
	FirewallDomainListID string
	Priority             int32
}

// UpdateFirewallRule updates an existing firewall rule.
func (b *InMemoryBackend) UpdateFirewallRule(p UpdateFirewallRuleParams) (*FirewallRule, error) {
	b.mu.Lock("UpdateFirewallRule")
	defer b.mu.Unlock()

	rule, ok := b.firewallRules[p.ID]
	if !ok {
		return nil, fmt.Errorf("%w: firewall rule %s not found", ErrNotFound, p.ID)
	}
	if p.Name != "" {
		rule.Name = p.Name
	}
	if p.Action != "" {
		rule.Action = p.Action
	}
	if p.BlockResponse != "" {
		rule.BlockResponse = p.BlockResponse
	}
	if p.BlockOverrideDomain != "" {
		rule.BlockOverrideDomain = p.BlockOverrideDomain
	}
	if p.BlockOverrideDnsType != "" {
		rule.BlockOverrideDnsType = p.BlockOverrideDnsType
	}
	if p.BlockOverrideTtl != 0 {
		rule.BlockOverrideTtl = p.BlockOverrideTtl
	}
	if p.Qtype != "" {
		rule.Qtype = p.Qtype
	}
	if p.ConfidenceThreshold != "" {
		rule.ConfidenceThreshold = p.ConfidenceThreshold
	}
	if p.FirewallDomainListID != "" {
		rule.FirewallDomainListID = p.FirewallDomainListID
	}
	if p.Priority != 0 {
		rule.Priority = p.Priority
	}
	rule.ModificationTime = currentTime()
	cp := *rule

	return &cp, nil
}

// ListFirewallRules lists firewall rules, optionally filtered by rule group ID.
func (b *InMemoryBackend) ListFirewallRules(firewallRuleGroupID string) []*FirewallRule {
	b.mu.RLock("ListFirewallRules")
	defer b.mu.RUnlock()

	list := make([]*FirewallRule, 0, len(b.firewallRules))
	for _, r := range b.firewallRules {
		if firewallRuleGroupID != "" && r.FirewallRuleGroupID != firewallRuleGroupID {
			continue
		}
		cp := *r
		list = append(list, &cp)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Priority < list[j].Priority })

	return list
}

// --- Firewall Rule Group operations ---

// DeleteFirewallRuleGroup deletes a firewall rule group and cascades to its rules and associations.
func (b *InMemoryBackend) DeleteFirewallRuleGroup(id string) (*FirewallRuleGroup, error) {
	b.mu.Lock("DeleteFirewallRuleGroup")
	defer b.mu.Unlock()

	grp, ok := b.firewallRuleGroups[id]
	if !ok {
		return nil, fmt.Errorf("%w: firewall rule group %s not found", ErrNotFound, id)
	}
	cp := *grp

	// Clean up tags.
	delete(b.tags, grp.ARN)

	// Cascade: delete rules belonging to this group.
	for ruleID, rule := range b.firewallRules {
		if rule.FirewallRuleGroupID == id {
			delete(b.firewallRules, ruleID)
		}
	}
	// Cascade: delete associations for this group.
	for assocID, assoc := range b.firewallRuleGroupAssociations {
		if assoc.FirewallRuleGroupID == id {
			delete(b.firewallRuleGroupAssociations, assocID)
		}
	}
	delete(b.firewallRuleGroups, id)

	return &cp, nil
}

// GetFirewallRuleGroup retrieves a firewall rule group by ID.
func (b *InMemoryBackend) GetFirewallRuleGroup(id string) (*FirewallRuleGroup, error) {
	b.mu.RLock("GetFirewallRuleGroup")
	defer b.mu.RUnlock()

	grp, ok := b.firewallRuleGroups[id]
	if !ok {
		return nil, fmt.Errorf("%w: firewall rule group %s not found", ErrNotFound, id)
	}
	cp := *grp

	return &cp, nil
}

// ListFirewallRuleGroups lists all firewall rule groups.
func (b *InMemoryBackend) ListFirewallRuleGroups() []*FirewallRuleGroup {
	b.mu.RLock("ListFirewallRuleGroups")
	defer b.mu.RUnlock()

	list := make([]*FirewallRuleGroup, 0, len(b.firewallRuleGroups))
	for _, g := range b.firewallRuleGroups {
		cp := *g
		list = append(list, &cp)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// GetFirewallRuleGroupPolicy retrieves the resource policy for a firewall rule group ARN.
func (b *InMemoryBackend) GetFirewallRuleGroupPolicy(arn string) string {
	b.mu.RLock("GetFirewallRuleGroupPolicy")
	defer b.mu.RUnlock()

	return b.firewallRuleGroupPolicies[arn]
}

// PutFirewallRuleGroupPolicy stores a resource policy for a firewall rule group ARN.
func (b *InMemoryBackend) PutFirewallRuleGroupPolicy(arn, policy string) error {
	b.mu.Lock("PutFirewallRuleGroupPolicy")
	defer b.mu.Unlock()

	b.firewallRuleGroupPolicies[arn] = policy

	return nil
}

// --- Firewall Rule Group Association operations ---

// GetFirewallRuleGroupAssociation retrieves an association by ID.
func (b *InMemoryBackend) GetFirewallRuleGroupAssociation(id string) (*FirewallRuleGroupAssociation, error) {
	b.mu.RLock("GetFirewallRuleGroupAssociation")
	defer b.mu.RUnlock()

	assoc, ok := b.firewallRuleGroupAssociations[id]
	if !ok {
		return nil, fmt.Errorf("%w: firewall rule group association %s not found", ErrNotFound, id)
	}
	cp := *assoc

	return &cp, nil
}

// ListFirewallRuleGroupAssociations lists associations, optionally filtered by VPC or group.
func (b *InMemoryBackend) ListFirewallRuleGroupAssociations(
	vpcID, firewallRuleGroupID string,
) []*FirewallRuleGroupAssociation {
	b.mu.RLock("ListFirewallRuleGroupAssociations")
	defer b.mu.RUnlock()

	list := make([]*FirewallRuleGroupAssociation, 0, len(b.firewallRuleGroupAssociations))
	for _, a := range b.firewallRuleGroupAssociations {
		if vpcID != "" && a.VpcID != vpcID {
			continue
		}
		if firewallRuleGroupID != "" && a.FirewallRuleGroupID != firewallRuleGroupID {
			continue
		}
		cp := *a
		list = append(list, &cp)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Priority < list[j].Priority })

	return list
}

// DisassociateFirewallRuleGroup removes a firewall rule group association.
func (b *InMemoryBackend) DisassociateFirewallRuleGroup(id string) (*FirewallRuleGroupAssociation, error) {
	b.mu.Lock("DisassociateFirewallRuleGroup")
	defer b.mu.Unlock()

	assoc, ok := b.firewallRuleGroupAssociations[id]
	if !ok {
		return nil, fmt.Errorf("%w: firewall rule group association %s not found", ErrNotFound, id)
	}

	if assoc.MutationProtection == mutationProtectionEnabled {
		return nil, fmt.Errorf("%w: cannot disassociate: MutationProtection is ENABLED", ErrValidation)
	}

	cp := *assoc
	delete(b.firewallRuleGroupAssociations, id)

	return &cp, nil
}

// UpdateFirewallRuleGroupAssociation updates name, priority, or mutation protection of an association.
func (b *InMemoryBackend) UpdateFirewallRuleGroupAssociation(
	id, name, mutationProtection string,
	priority int32,
) (*FirewallRuleGroupAssociation, error) {
	b.mu.Lock("UpdateFirewallRuleGroupAssociation")
	defer b.mu.Unlock()

	assoc, ok := b.firewallRuleGroupAssociations[id]
	if !ok {
		return nil, fmt.Errorf("%w: firewall rule group association %s not found", ErrNotFound, id)
	}
	if name != "" {
		assoc.Name = name
	}
	if priority != 0 {
		assoc.Priority = priority
	}
	if mutationProtection != "" {
		if mutationProtection != mutationProtectionEnabled && mutationProtection != mutationProtectionDisabled {
			return nil, fmt.Errorf("%w: MutationProtection must be ENABLED or DISABLED", ErrValidation)
		}
		assoc.MutationProtection = mutationProtection
	}
	assoc.ModificationTime = currentTime()
	cp := *assoc

	return &cp, nil
}

// --- Firewall Domain List operations ---

// GetFirewallDomainList retrieves a domain list by ID.
func (b *InMemoryBackend) GetFirewallDomainList(id string) (*FirewallDomainList, error) {
	b.mu.RLock("GetFirewallDomainList")
	defer b.mu.RUnlock()

	dl, ok := b.firewallDomainLists[id]
	if !ok {
		return nil, fmt.Errorf("%w: firewall domain list %s not found", ErrNotFound, id)
	}
	cp := cloneFirewallDomainList(dl)

	return cp, nil
}

// ListFirewallDomainLists lists all firewall domain lists.
func (b *InMemoryBackend) ListFirewallDomainLists() []*FirewallDomainList {
	b.mu.RLock("ListFirewallDomainLists")
	defer b.mu.RUnlock()

	list := make([]*FirewallDomainList, 0, len(b.firewallDomainLists))
	for _, dl := range b.firewallDomainLists {
		list = append(list, cloneFirewallDomainList(dl))
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// ListFirewallDomains returns the domains stored in a domain list.
func (b *InMemoryBackend) ListFirewallDomains(id string) ([]string, error) {
	b.mu.RLock("ListFirewallDomains")
	defer b.mu.RUnlock()

	dl, ok := b.firewallDomainLists[id]
	if !ok {
		return nil, fmt.Errorf("%w: firewall domain list %s not found", ErrNotFound, id)
	}
	cp := make([]string, len(dl.Domains))
	copy(cp, dl.Domains)

	return cp, nil
}

// UpdateFirewallDomains replaces, adds, or removes domains in a domain list.
func (b *InMemoryBackend) UpdateFirewallDomains(id, operation string, domains []string) (*FirewallDomainList, error) {
	b.mu.Lock("UpdateFirewallDomains")
	defer b.mu.Unlock()

	dl, ok := b.firewallDomainLists[id]
	if !ok {
		return nil, fmt.Errorf("%w: firewall domain list %s not found", ErrNotFound, id)
	}

	switch operation {
	case domainUpdateOpReplace:
		dl.Domains = make([]string, len(domains))
		copy(dl.Domains, domains)
	case domainUpdateOpAdd:
		existing := make(map[string]bool, len(dl.Domains))
		for _, d := range dl.Domains {
			existing[d] = true
		}
		for _, d := range domains {
			if !existing[d] {
				dl.Domains = append(dl.Domains, d)
			}
		}
	case domainUpdateOpRemove:
		toRemove := make(map[string]bool, len(domains))
		for _, d := range domains {
			toRemove[d] = true
		}
		remaining := make([]string, 0, len(dl.Domains))
		for _, d := range dl.Domains {
			if !toRemove[d] {
				remaining = append(remaining, d)
			}
		}
		dl.Domains = remaining
	default:
		return nil, fmt.Errorf(
			"%w: Operation must be %s, %s, or %s",
			ErrValidation,
			domainUpdateOpReplace,
			domainUpdateOpAdd,
			domainUpdateOpRemove,
		)
	}
	dl.DomainCount = domainCount(dl.Domains)
	cp := cloneFirewallDomainList(dl)

	return cp, nil
}

// ImportFirewallDomains simulates importing domains from a URL into a domain list.
func (b *InMemoryBackend) ImportFirewallDomains(id, operation, domainFileURL string) (*FirewallDomainList, error) {
	b.mu.Lock("ImportFirewallDomains")
	defer b.mu.Unlock()

	dl, ok := b.firewallDomainLists[id]
	if !ok {
		return nil, fmt.Errorf("%w: firewall domain list %s not found", ErrNotFound, id)
	}

	// Simulate: clear domains on REPLACE, leave intact for ADD/REMOVE (no HTTP in mock).
	if operation == domainUpdateOpReplace {
		dl.Domains = []string{}
		dl.DomainCount = 0
	}
	dl.Status = statusComplete
	_ = domainFileURL
	cp := cloneFirewallDomainList(dl)

	return cp, nil
}

// cloneFirewallDomainList returns a deep copy of a FirewallDomainList.
func cloneFirewallDomainList(dl *FirewallDomainList) *FirewallDomainList {
	cp := *dl
	if dl.Domains != nil {
		cp.Domains = make([]string, len(dl.Domains))
		copy(cp.Domains, dl.Domains)
	}

	return &cp
}

// domainCount returns the number of domains as int32, capping at MaxInt32.
func domainCount(domains []string) int32 {
	const maxInt32 = 1<<31 - 1
	if len(domains) > maxInt32 {
		return maxInt32
	}

	return int32(len(domains)) //nolint:gosec // guarded above
}

// --- Firewall Config operations ---

// GetFirewallConfig returns or lazily creates the firewall config for a resource (VPC).
func (b *InMemoryBackend) GetFirewallConfig(resourceID string) *FirewallConfig {
	b.mu.Lock("GetFirewallConfig")
	defer b.mu.Unlock()

	if cfg, ok := b.firewallConfigs[resourceID]; ok {
		cp := *cfg

		return &cp
	}
	id := "fwc-" + uuid.New().String()[:8]
	cfg := &FirewallConfig{
		ID:               id,
		OwnerID:          b.accountID,
		ResourceID:       resourceID,
		FirewallFailOpen: firewallFailOpenDisabled,
	}
	b.firewallConfigs[resourceID] = cfg
	cp := *cfg

	return &cp
}

// UpdateFirewallConfig updates the firewall fail-open setting for a resource.
func (b *InMemoryBackend) UpdateFirewallConfig(resourceID, firewallFailOpen string) (*FirewallConfig, error) {
	b.mu.Lock("UpdateFirewallConfig")
	defer b.mu.Unlock()

	if firewallFailOpen != firewallFailOpenEnabled && firewallFailOpen != firewallFailOpenDisabled {
		return nil, fmt.Errorf(
			"%w: FirewallFailOpen must be %s or %s",
			ErrValidation,
			firewallFailOpenEnabled,
			firewallFailOpenDisabled,
		)
	}

	cfg, ok := b.firewallConfigs[resourceID]
	if !ok {
		id := "fwc-" + uuid.New().String()[:8]
		cfg = &FirewallConfig{
			ID:         id,
			OwnerID:    b.accountID,
			ResourceID: resourceID,
		}
		b.firewallConfigs[resourceID] = cfg
	}
	cfg.FirewallFailOpen = firewallFailOpen
	cp := *cfg

	return &cp, nil
}

// ListFirewallConfigs lists all firewall configs.
func (b *InMemoryBackend) ListFirewallConfigs() []*FirewallConfig {
	b.mu.RLock("ListFirewallConfigs")
	defer b.mu.RUnlock()

	list := make([]*FirewallConfig, 0, len(b.firewallConfigs))
	for _, cfg := range b.firewallConfigs {
		cp := *cfg
		list = append(list, &cp)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].ResourceID < list[j].ResourceID })

	return list
}

// --- Resolver Config operations ---

// GetResolverConfig returns or lazily creates the resolver config for a resource (VPC).
func (b *InMemoryBackend) GetResolverConfig(resourceID string) *ResolverConfig {
	b.mu.Lock("GetResolverConfig")
	defer b.mu.Unlock()

	if cfg, ok := b.resolverConfigs[resourceID]; ok {
		cp := *cfg

		return &cp
	}
	id := "rslvr-rc-" + uuid.New().String()[:8]
	cfgARN := arn.Build("route53resolver", b.region, b.accountID, "resolver-config/"+id)
	cfg := &ResolverConfig{
		ID:                 id,
		ARN:                cfgARN,
		OwnerID:            b.accountID,
		ResourceID:         resourceID,
		AutodefinedReverse: "DISABLED",
	}
	b.resolverConfigs[resourceID] = cfg
	cp := *cfg

	return &cp
}

// UpdateResolverConfig updates the AutodefinedReverse setting for a resource.
func (b *InMemoryBackend) UpdateResolverConfig(resourceID, autodefinedReverse string) (*ResolverConfig, error) {
	b.mu.Lock("UpdateResolverConfig")
	defer b.mu.Unlock()

	if autodefinedReverse != autodefinedReverseEnabled && autodefinedReverse != autodefinedReverseDisabled {
		return nil, fmt.Errorf(
			"%w: AutodefinedReverse must be %s or %s",
			ErrValidation,
			autodefinedReverseEnabled,
			autodefinedReverseDisabled,
		)
	}

	cfg, ok := b.resolverConfigs[resourceID]
	if !ok {
		id := "rslvr-rc-" + uuid.New().String()[:8]
		cfgARN := arn.Build("route53resolver", b.region, b.accountID, "resolver-config/"+id)
		cfg = &ResolverConfig{
			ID:         id,
			ARN:        cfgARN,
			OwnerID:    b.accountID,
			ResourceID: resourceID,
		}
		b.resolverConfigs[resourceID] = cfg
	}
	if autodefinedReverse == autodefinedReverseEnabled {
		cfg.AutodefinedReverse = "ENABLED"
	} else {
		cfg.AutodefinedReverse = "DISABLED"
	}
	cp := *cfg

	return &cp, nil
}

// ListResolverConfigs lists all resolver configs.
func (b *InMemoryBackend) ListResolverConfigs() []*ResolverConfig {
	b.mu.RLock("ListResolverConfigs")
	defer b.mu.RUnlock()

	list := make([]*ResolverConfig, 0, len(b.resolverConfigs))
	for _, cfg := range b.resolverConfigs {
		cp := *cfg
		list = append(list, &cp)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].ResourceID < list[j].ResourceID })

	return list
}

// --- Resolver DNSSEC Config operations ---

// GetResolverDnssecConfig returns or lazily creates the DNSSEC config for a resource.
func (b *InMemoryBackend) GetResolverDnssecConfig(resourceID string) *ResolverDnssecConfig {
	b.mu.Lock("GetResolverDnssecConfig")
	defer b.mu.Unlock()

	if cfg, ok := b.resolverDnssecConfigs[resourceID]; ok {
		cp := *cfg

		return &cp
	}
	id := "rslvr-dnssec-" + uuid.New().String()[:8]
	cfg := &ResolverDnssecConfig{
		ID:               id,
		OwnerID:          b.accountID,
		ResourceID:       resourceID,
		ValidationStatus: validationStatusDisabled,
	}
	b.resolverDnssecConfigs[resourceID] = cfg
	cp := *cfg

	return &cp
}

// UpdateResolverDnssecConfig updates DNSSEC validation for a resource.
func (b *InMemoryBackend) UpdateResolverDnssecConfig(resourceID, validation string) (*ResolverDnssecConfig, error) {
	b.mu.Lock("UpdateResolverDnssecConfig")
	defer b.mu.Unlock()

	if validation != dnssecValidationEnable && validation != dnssecValidationDisable {
		return nil, fmt.Errorf(
			"%w: Validation must be %s or %s",
			ErrValidation,
			dnssecValidationEnable,
			dnssecValidationDisable,
		)
	}

	cfg, ok := b.resolverDnssecConfigs[resourceID]
	if !ok {
		id := "rslvr-dnssec-" + uuid.New().String()[:8]
		cfg = &ResolverDnssecConfig{
			ID:         id,
			OwnerID:    b.accountID,
			ResourceID: resourceID,
		}
		b.resolverDnssecConfigs[resourceID] = cfg
	}
	if validation == dnssecValidationEnable {
		cfg.ValidationStatus = validationStatusEnabling
	} else {
		cfg.ValidationStatus = validationStatusDisabling
	}
	cp := *cfg

	return &cp, nil
}

// ListResolverDnssecConfigs lists all DNSSEC configs.
func (b *InMemoryBackend) ListResolverDnssecConfigs() []*ResolverDnssecConfig {
	b.mu.RLock("ListResolverDnssecConfigs")
	defer b.mu.RUnlock()

	list := make([]*ResolverDnssecConfig, 0, len(b.resolverDnssecConfigs))
	for _, cfg := range b.resolverDnssecConfigs {
		cp := *cfg
		list = append(list, &cp)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].ResourceID < list[j].ResourceID })

	return list
}

// --- Outpost Resolver operations ---

// DeleteOutpostResolver deletes an outpost resolver.
func (b *InMemoryBackend) DeleteOutpostResolver(id string) (*OutpostResolver, error) {
	b.mu.Lock("DeleteOutpostResolver")
	defer b.mu.Unlock()

	r, ok := b.outpostResolvers[id]
	if !ok {
		return nil, fmt.Errorf("%w: outpost resolver %s not found", ErrNotFound, id)
	}
	cp := *r
	delete(b.outpostResolvers, id)

	return &cp, nil
}

// GetOutpostResolver retrieves an outpost resolver by ID.
func (b *InMemoryBackend) GetOutpostResolver(id string) (*OutpostResolver, error) {
	b.mu.RLock("GetOutpostResolver")
	defer b.mu.RUnlock()

	r, ok := b.outpostResolvers[id]
	if !ok {
		return nil, fmt.Errorf("%w: outpost resolver %s not found", ErrNotFound, id)
	}
	cp := *r

	return &cp, nil
}

// ListOutpostResolvers lists all outpost resolvers.
func (b *InMemoryBackend) ListOutpostResolvers() []*OutpostResolver {
	b.mu.RLock("ListOutpostResolvers")
	defer b.mu.RUnlock()

	list := make([]*OutpostResolver, 0, len(b.outpostResolvers))
	for _, r := range b.outpostResolvers {
		cp := *r
		list = append(list, &cp)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// UpdateOutpostResolver updates name, preferred instance type, or instance count.
func (b *InMemoryBackend) UpdateOutpostResolver(
	id, name, preferredInstanceType string,
	instanceCount int32,
) (*OutpostResolver, error) {
	b.mu.Lock("UpdateOutpostResolver")
	defer b.mu.Unlock()

	r, ok := b.outpostResolvers[id]
	if !ok {
		return nil, fmt.Errorf("%w: outpost resolver %s not found", ErrNotFound, id)
	}
	if name != "" {
		r.Name = name
	}
	if preferredInstanceType != "" {
		r.PreferredInstanceType = preferredInstanceType
	}
	if instanceCount > 0 {
		r.InstanceCount = instanceCount
	}
	cp := *r

	return &cp, nil
}

// --- Query Log Config operations ---

// DeleteResolverQueryLogConfig deletes a query log config and its associations.
func (b *InMemoryBackend) DeleteResolverQueryLogConfig(id string) (*ResolverQueryLogConfig, error) {
	b.mu.Lock("DeleteResolverQueryLogConfig")
	defer b.mu.Unlock()

	cfg, ok := b.queryLogConfigs[id]
	if !ok {
		return nil, fmt.Errorf("%w: resolver query log config %s not found", ErrNotFound, id)
	}
	cp := *cfg

	// Clean up tags.
	delete(b.tags, cfg.ARN)

	// Cascade: remove all associations referencing this config.
	for assocID, assoc := range b.queryLogConfigAssociations {
		if assoc.ResolverQueryLogConfigID == id {
			delete(b.queryLogConfigAssociations, assocID)
		}
	}
	delete(b.queryLogConfigs, id)

	return &cp, nil
}

// GetResolverQueryLogConfig retrieves a query log config by ID.
func (b *InMemoryBackend) GetResolverQueryLogConfig(id string) (*ResolverQueryLogConfig, error) {
	b.mu.RLock("GetResolverQueryLogConfig")
	defer b.mu.RUnlock()

	cfg, ok := b.queryLogConfigs[id]
	if !ok {
		return nil, fmt.Errorf("%w: resolver query log config %s not found", ErrNotFound, id)
	}
	cp := *cfg

	return &cp, nil
}

// ListResolverQueryLogConfigs lists all query log configs.
func (b *InMemoryBackend) ListResolverQueryLogConfigs() []*ResolverQueryLogConfig {
	b.mu.RLock("ListResolverQueryLogConfigs")
	defer b.mu.RUnlock()

	list := make([]*ResolverQueryLogConfig, 0, len(b.queryLogConfigs))
	for _, cfg := range b.queryLogConfigs {
		cp := *cfg
		list = append(list, &cp)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// GetResolverQueryLogConfigAssociation retrieves an association by ID.
func (b *InMemoryBackend) GetResolverQueryLogConfigAssociation(id string) (*ResolverQueryLogConfigAssociation, error) {
	b.mu.RLock("GetResolverQueryLogConfigAssociation")
	defer b.mu.RUnlock()

	assoc, ok := b.queryLogConfigAssociations[id]
	if !ok {
		return nil, fmt.Errorf("%w: resolver query log config association %s not found", ErrNotFound, id)
	}
	cp := *assoc

	return &cp, nil
}

// DisassociateResolverQueryLogConfig removes a query log config association.
func (b *InMemoryBackend) DisassociateResolverQueryLogConfig(id string) (*ResolverQueryLogConfigAssociation, error) {
	b.mu.Lock("DisassociateResolverQueryLogConfig")
	defer b.mu.Unlock()

	assoc, ok := b.queryLogConfigAssociations[id]
	if !ok {
		return nil, fmt.Errorf("%w: resolver query log config association %s not found", ErrNotFound, id)
	}
	cp := *assoc
	delete(b.queryLogConfigAssociations, id)

	// Decrement AssociationCount on the config.
	if cfg, ok := b.queryLogConfigs[assoc.ResolverQueryLogConfigID]; ok && cfg.AssociationCount > 0 {
		cfg.AssociationCount--
	}

	return &cp, nil
}

// ListResolverQueryLogConfigAssociations lists all query log config associations.
func (b *InMemoryBackend) ListResolverQueryLogConfigAssociations() []*ResolverQueryLogConfigAssociation {
	b.mu.RLock("ListResolverQueryLogConfigAssociations")
	defer b.mu.RUnlock()

	list := make([]*ResolverQueryLogConfigAssociation, 0, len(b.queryLogConfigAssociations))
	for _, a := range b.queryLogConfigAssociations {
		cp := *a
		list = append(list, &cp)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })

	return list
}

// GetResolverQueryLogConfigPolicy retrieves a resource policy for a query log config ARN.
func (b *InMemoryBackend) GetResolverQueryLogConfigPolicy(arn string) string {
	b.mu.RLock("GetResolverQueryLogConfigPolicy")
	defer b.mu.RUnlock()

	return b.queryLogConfigPolicies[arn]
}

// PutResolverQueryLogConfigPolicy stores a resource policy for a query log config ARN.
func (b *InMemoryBackend) PutResolverQueryLogConfigPolicy(arn, policy string) error {
	b.mu.Lock("PutResolverQueryLogConfigPolicy")
	defer b.mu.Unlock()

	b.queryLogConfigPolicies[arn] = policy

	return nil
}

// --- Resolver Rule Association operations ---

// GetResolverRuleAssociation retrieves a rule association by ID.
func (b *InMemoryBackend) GetResolverRuleAssociation(id string) (*ResolverRuleAssociation, error) {
	b.mu.RLock("GetResolverRuleAssociation")
	defer b.mu.RUnlock()

	assoc, ok := b.ruleAssociations[id]
	if !ok {
		return nil, fmt.Errorf("%w: resolver rule association %s not found", ErrNotFound, id)
	}
	cp := *assoc

	return &cp, nil
}

// DisassociateResolverRule removes a resolver rule association.
func (b *InMemoryBackend) DisassociateResolverRule(id string) (*ResolverRuleAssociation, error) {
	b.mu.Lock("DisassociateResolverRule")
	defer b.mu.Unlock()

	assoc, ok := b.ruleAssociations[id]
	if !ok {
		return nil, fmt.Errorf("%w: resolver rule association %s not found", ErrNotFound, id)
	}
	cp := *assoc
	delete(b.ruleAssociations, id)

	return &cp, nil
}

// ListResolverRuleAssociations lists all resolver rule associations.
func (b *InMemoryBackend) ListResolverRuleAssociations() []*ResolverRuleAssociation {
	b.mu.RLock("ListResolverRuleAssociations")
	defer b.mu.RUnlock()

	list := make([]*ResolverRuleAssociation, 0, len(b.ruleAssociations))
	for _, a := range b.ruleAssociations {
		cp := *a
		list = append(list, &cp)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })

	return list
}

// GetResolverRulePolicy retrieves a resource policy for a resolver rule ARN.
func (b *InMemoryBackend) GetResolverRulePolicy(arn string) string {
	b.mu.RLock("GetResolverRulePolicy")
	defer b.mu.RUnlock()

	return b.resolverRulePolicies[arn]
}

// PutResolverRulePolicy stores a resource policy for a resolver rule ARN.
func (b *InMemoryBackend) PutResolverRulePolicy(arn, policy string) error {
	b.mu.Lock("PutResolverRulePolicy")
	defer b.mu.Unlock()

	b.resolverRulePolicies[arn] = policy

	return nil
}

// --- Resolver Endpoint Update ---

// UpdateResolverEndpoint updates name, endpoint type, and/or protocols of a resolver endpoint.
func (b *InMemoryBackend) UpdateResolverEndpoint(
	id, name, resolverEndpointType string,
	protocols []string,
) (*ResolverEndpoint, error) {
	b.mu.Lock("UpdateResolverEndpoint")
	defer b.mu.Unlock()

	ep, ok := b.endpoints[id]
	if !ok {
		return nil, fmt.Errorf("%w: resolver endpoint %s not found", ErrNotFound, id)
	}
	if name != "" {
		ep.Name = name
	}
	if resolverEndpointType != "" {
		switch resolverEndpointType {
		case endpointTypeIPV4, endpointTypeIPV6, endpointTypeDualStack:
			ep.ResolverEndpointType = resolverEndpointType
		default:
			return nil, fmt.Errorf("%w: ResolverEndpointType must be IPV4, IPV6, or DUALSTACK", ErrValidation)
		}
	}
	if len(protocols) > 0 {
		protocolsCopy := make([]string, len(protocols))
		copy(protocolsCopy, protocols)
		ep.Protocols = protocolsCopy
	}
	ep.ModificationTime = currentTime()

	return cloneEndpoint(ep), nil
}

// DisassociateResolverEndpointIPAddress removes an IP address from a resolver endpoint.
func (b *InMemoryBackend) DisassociateResolverEndpointIPAddress(endpointID, ipID string) (*ResolverEndpoint, error) {
	b.mu.Lock("DisassociateResolverEndpointIPAddress")
	defer b.mu.Unlock()

	ep, ok := b.endpoints[endpointID]
	if !ok {
		return nil, fmt.Errorf("%w: resolver endpoint %s not found", ErrNotFound, endpointID)
	}

	newIPs := make([]IPAddress, 0, len(ep.IPAddresses))
	found := false
	for _, ip := range ep.IPAddresses {
		if ip.IPID == ipID {
			found = true

			continue
		}
		newIPs = append(newIPs, ip)
	}
	if !found {
		return nil, fmt.Errorf("%w: IP address %s not found on endpoint %s", ErrNotFound, ipID, endpointID)
	}
	ep.IPAddresses = newIPs

	return cloneEndpoint(ep), nil
}

// --- Resolver Rule Update ---

// UpdateResolverRule updates fields of a resolver rule.
func (b *InMemoryBackend) UpdateResolverRule(
	id, name, resolverEndpointID string,
	targetIps []TargetIP,
) (*ResolverRule, error) {
	b.mu.Lock("UpdateResolverRule")
	defer b.mu.Unlock()

	r, ok := b.rules[id]
	if !ok {
		return nil, fmt.Errorf("%w: resolver rule %s not found", ErrNotFound, id)
	}
	if name != "" {
		r.Name = name
	}
	if resolverEndpointID != "" {
		r.ResolverEndpointID = resolverEndpointID
	}
	if targetIps != nil {
		tipsCopy := make([]TargetIP, len(targetIps))
		copy(tipsCopy, targetIps)
		r.TargetIps = tipsCopy
	}

	return cloneRule(r), nil
}
