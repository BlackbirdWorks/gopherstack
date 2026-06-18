package route53resolver

import (
	"context"
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
	ErrNotFound         = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	ErrAlreadyExists    = awserr.New("ResourceExistsException", awserr.ErrAlreadyExists)
	ErrValidation       = awserr.New("InvalidRequestException", awserr.ErrInvalidParameter)
	ErrInvalidParameter = awserr.New("InvalidParameterException", awserr.ErrInvalidParameter)
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

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

	validationStatusEnabled   = "ENABLED"
	validationStatusEnabling  = "ENABLING"
	validationStatusDisabled  = "DISABLED"
	validationStatusDisabling = "DISABLING"

	mutationProtectionEnabled  = "ENABLED"
	mutationProtectionDisabled = "DISABLED"

	endpointTypeIPV4      = "IPV4"
	endpointTypeIPV6      = "IPV6"
	endpointTypeDualStack = "DUALSTACK"

	blockResponseNODATA   = "NODATA"
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
	OutpostArn            string       `json:"outpostArn,omitempty"`
	PreferredInstanceType string       `json:"preferredInstanceType,omitempty"`
	CreatorRequestID      string       `json:"creatorRequestId,omitempty"`
	CreationTime          string       `json:"creationTime,omitempty"`
	ModificationTime      string       `json:"modificationTime,omitempty"`
	SecurityGroupIDs      []string     `json:"securityGroupIds"`
	IPAddresses           []IPAddress  `json:"ipAddresses"`
	Tags                  []svcTags.KV `json:"tags,omitempty"`
	Protocols             []string     `json:"protocols,omitempty"`
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
	CreatorRequestID   string     `json:"creatorRequestId,omitempty"`
	OwnerID            string     `json:"ownerId,omitempty"`
	CreationTime       string     `json:"creationTime,omitempty"`
	ModificationTime   string     `json:"modificationTime,omitempty"`
	TargetIps          []TargetIP `json:"targetIps,omitempty"`
}

// TargetIP represents a forwarding target IP for a resolver rule.
type TargetIP struct {
	IP       string `json:"ip"`
	Ipv6     string `json:"ipv6,omitempty"`
	Protocol string `json:"protocol,omitempty"`
	Port     int32  `json:"port"`
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
	CreationTime     string       `json:"creationTime,omitempty"`
	ModificationTime string       `json:"modificationTime,omitempty"`
	Tags             []svcTags.KV `json:"tags,omitempty"`
	RuleCount        int32        `json:"ruleCount"`
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
	MutationProtection  string `json:"mutationProtection"`
	ManagedOwnerName    string `json:"managedOwnerName,omitempty"`
	CreatorRequestID    string `json:"creatorRequestId,omitempty"`
	CreationTime        string `json:"creationTime,omitempty"`
	ModificationTime    string `json:"modificationTime,omitempty"`
	Priority            int32  `json:"priority"`
}

// FirewallDomainList represents a DNS Firewall domain list.
type FirewallDomainList struct {
	ID               string       `json:"id"`
	ARN              string       `json:"arn"`
	Name             string       `json:"name"`
	CreatorRequestID string       `json:"creatorRequestId"`
	Status           string       `json:"status"`
	ManagedOwnerName string       `json:"managedOwnerName,omitempty"`
	Tags             []svcTags.KV `json:"tags,omitempty"`
	Domains          []string     `json:"domains,omitempty"`
	DomainCount      int32        `json:"domainCount"`
}

// FirewallRule represents a single rule within a DNS Firewall rule group.
type FirewallRule struct {
	ID                   string `json:"id"`
	ARN                  string `json:"arn"`
	Name                 string `json:"name"`
	FirewallRuleGroupID  string `json:"firewallRuleGroupId"`
	FirewallDomainListID string `json:"firewallDomainListId"`
	Action               string `json:"action"`
	BlockResponse        string `json:"blockResponse,omitempty"`
	BlockOverrideDomain  string `json:"blockOverrideDomain,omitempty"`
	BlockOverrideDNSType string `json:"blockOverrideDnsType,omitempty"`
	Qtype                string `json:"qtype,omitempty"`
	ConfidenceThreshold  string `json:"confidenceThreshold,omitempty"`
	CreatorRequestID     string `json:"creatorRequestId,omitempty"`
	CreationTime         string `json:"creationTime,omitempty"`
	ModificationTime     string `json:"modificationTime,omitempty"`
	BlockOverrideTTL     int32  `json:"blockOverrideTtl,omitempty"`
	Priority             int32  `json:"priority"`
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
	ShareStatus      string       `json:"shareStatus"`
	CreationTime     string       `json:"creationTime,omitempty"`
	Tags             []svcTags.KV `json:"tags,omitempty"`
	AssociationCount int32        `json:"associationCount"`
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
	endpoints                     map[string]map[string]*ResolverEndpoint
	rules                         map[string]map[string]*ResolverRule
	tags                          map[string]map[string][]svcTags.KV
	firewallRuleGroups            map[string]map[string]*FirewallRuleGroup
	firewallRuleGroupAssociations map[string]map[string]*FirewallRuleGroupAssociation
	firewallDomainLists           map[string]map[string]*FirewallDomainList
	firewallRules                 map[string]map[string]*FirewallRule
	outpostResolvers              map[string]map[string]*OutpostResolver
	queryLogConfigs               map[string]map[string]*ResolverQueryLogConfig
	queryLogConfigAssociations    map[string]map[string]*ResolverQueryLogConfigAssociation
	ruleAssociations              map[string]map[string]*ResolverRuleAssociation
	firewallConfigs               map[string]map[string]*FirewallConfig
	resolverConfigs               map[string]map[string]*ResolverConfig
	resolverDnssecConfigs         map[string]map[string]*ResolverDnssecConfig
	firewallRuleGroupPolicies     map[string]map[string]string
	queryLogConfigPolicies        map[string]map[string]string
	resolverRulePolicies          map[string]map[string]string
	mu                            *lockmetrics.RWMutex
	accountID                     string
	region                        string
}

func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		endpoints:                     make(map[string]map[string]*ResolverEndpoint),
		rules:                         make(map[string]map[string]*ResolverRule),
		tags:                          make(map[string]map[string][]svcTags.KV),
		firewallRuleGroups:            make(map[string]map[string]*FirewallRuleGroup),
		firewallRuleGroupAssociations: make(map[string]map[string]*FirewallRuleGroupAssociation),
		firewallDomainLists:           make(map[string]map[string]*FirewallDomainList),
		firewallRules:                 make(map[string]map[string]*FirewallRule),
		outpostResolvers:              make(map[string]map[string]*OutpostResolver),
		queryLogConfigs:               make(map[string]map[string]*ResolverQueryLogConfig),
		queryLogConfigAssociations:    make(map[string]map[string]*ResolverQueryLogConfigAssociation),
		ruleAssociations:              make(map[string]map[string]*ResolverRuleAssociation),
		firewallConfigs:               make(map[string]map[string]*FirewallConfig),
		resolverConfigs:               make(map[string]map[string]*ResolverConfig),
		resolverDnssecConfigs:         make(map[string]map[string]*ResolverDnssecConfig),
		firewallRuleGroupPolicies:     make(map[string]map[string]string),
		queryLogConfigPolicies:        make(map[string]map[string]string),
		resolverRulePolicies:          make(map[string]map[string]string),
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

	b.endpoints = make(map[string]map[string]*ResolverEndpoint)
	b.rules = make(map[string]map[string]*ResolverRule)
	b.tags = make(map[string]map[string][]svcTags.KV)
	b.firewallRuleGroups = make(map[string]map[string]*FirewallRuleGroup)
	b.firewallRuleGroupAssociations = make(map[string]map[string]*FirewallRuleGroupAssociation)
	b.firewallDomainLists = make(map[string]map[string]*FirewallDomainList)
	b.firewallRules = make(map[string]map[string]*FirewallRule)
	b.outpostResolvers = make(map[string]map[string]*OutpostResolver)
	b.queryLogConfigs = make(map[string]map[string]*ResolverQueryLogConfig)
	b.queryLogConfigAssociations = make(map[string]map[string]*ResolverQueryLogConfigAssociation)
	b.ruleAssociations = make(map[string]map[string]*ResolverRuleAssociation)
	b.firewallConfigs = make(map[string]map[string]*FirewallConfig)
	b.resolverConfigs = make(map[string]map[string]*ResolverConfig)
	b.resolverDnssecConfigs = make(map[string]map[string]*ResolverDnssecConfig)
	b.firewallRuleGroupPolicies = make(map[string]map[string]string)
	b.queryLogConfigPolicies = make(map[string]map[string]string)
	b.resolverRulePolicies = make(map[string]map[string]string)
}

// Per-region lazy store helpers.

func (b *InMemoryBackend) endpointsStore(region string) map[string]*ResolverEndpoint {
	if b.endpoints[region] == nil {
		b.endpoints[region] = make(map[string]*ResolverEndpoint)
	}

	return b.endpoints[region]
}

func (b *InMemoryBackend) rulesStore(region string) map[string]*ResolverRule {
	if b.rules[region] == nil {
		b.rules[region] = make(map[string]*ResolverRule)
	}

	return b.rules[region]
}

func (b *InMemoryBackend) tagsStore(region string) map[string][]svcTags.KV {
	if b.tags[region] == nil {
		b.tags[region] = make(map[string][]svcTags.KV)
	}

	return b.tags[region]
}

func (b *InMemoryBackend) firewallRuleGroupsStore(region string) map[string]*FirewallRuleGroup {
	if b.firewallRuleGroups[region] == nil {
		b.firewallRuleGroups[region] = make(map[string]*FirewallRuleGroup)
	}

	return b.firewallRuleGroups[region]
}

func (b *InMemoryBackend) firewallRuleGroupAssociationsStore(region string) map[string]*FirewallRuleGroupAssociation {
	if b.firewallRuleGroupAssociations[region] == nil {
		b.firewallRuleGroupAssociations[region] = make(map[string]*FirewallRuleGroupAssociation)
	}

	return b.firewallRuleGroupAssociations[region]
}

func (b *InMemoryBackend) firewallDomainListsStore(region string) map[string]*FirewallDomainList {
	if b.firewallDomainLists[region] == nil {
		b.firewallDomainLists[region] = make(map[string]*FirewallDomainList)
	}

	return b.firewallDomainLists[region]
}

func (b *InMemoryBackend) firewallRulesStore(region string) map[string]*FirewallRule {
	if b.firewallRules[region] == nil {
		b.firewallRules[region] = make(map[string]*FirewallRule)
	}

	return b.firewallRules[region]
}

func (b *InMemoryBackend) outpostResolversStore(region string) map[string]*OutpostResolver {
	if b.outpostResolvers[region] == nil {
		b.outpostResolvers[region] = make(map[string]*OutpostResolver)
	}

	return b.outpostResolvers[region]
}

func (b *InMemoryBackend) queryLogConfigsStore(region string) map[string]*ResolverQueryLogConfig {
	if b.queryLogConfigs[region] == nil {
		b.queryLogConfigs[region] = make(map[string]*ResolverQueryLogConfig)
	}

	return b.queryLogConfigs[region]
}

func (b *InMemoryBackend) queryLogConfigAssociationsStore(region string) map[string]*ResolverQueryLogConfigAssociation {
	if b.queryLogConfigAssociations[region] == nil {
		b.queryLogConfigAssociations[region] = make(map[string]*ResolverQueryLogConfigAssociation)
	}

	return b.queryLogConfigAssociations[region]
}

func (b *InMemoryBackend) ruleAssociationsStore(region string) map[string]*ResolverRuleAssociation {
	if b.ruleAssociations[region] == nil {
		b.ruleAssociations[region] = make(map[string]*ResolverRuleAssociation)
	}

	return b.ruleAssociations[region]
}

func (b *InMemoryBackend) firewallConfigsStore(region string) map[string]*FirewallConfig {
	if b.firewallConfigs[region] == nil {
		b.firewallConfigs[region] = make(map[string]*FirewallConfig)
	}

	return b.firewallConfigs[region]
}

func (b *InMemoryBackend) resolverConfigsStore(region string) map[string]*ResolverConfig {
	if b.resolverConfigs[region] == nil {
		b.resolverConfigs[region] = make(map[string]*ResolverConfig)
	}

	return b.resolverConfigs[region]
}

func (b *InMemoryBackend) resolverDnssecConfigsStore(region string) map[string]*ResolverDnssecConfig {
	if b.resolverDnssecConfigs[region] == nil {
		b.resolverDnssecConfigs[region] = make(map[string]*ResolverDnssecConfig)
	}

	return b.resolverDnssecConfigs[region]
}

func (b *InMemoryBackend) firewallRuleGroupPoliciesStore(region string) map[string]string {
	if b.firewallRuleGroupPolicies[region] == nil {
		b.firewallRuleGroupPolicies[region] = make(map[string]string)
	}

	return b.firewallRuleGroupPolicies[region]
}

func (b *InMemoryBackend) queryLogConfigPoliciesStore(region string) map[string]string {
	if b.queryLogConfigPolicies[region] == nil {
		b.queryLogConfigPolicies[region] = make(map[string]string)
	}

	return b.queryLogConfigPolicies[region]
}

func (b *InMemoryBackend) resolverRulePoliciesStore(region string) map[string]string {
	if b.resolverRulePolicies[region] == nil {
		b.resolverRulePolicies[region] = make(map[string]string)
	}

	return b.resolverRulePolicies[region]
}

const dirPrefixLen = 2

func (b *InMemoryBackend) CreateResolverEndpoint(
	ctx context.Context,
	name, direction, vpcID string,
	ips []IPAddress,
	securityGroupIDs []string,
	resolverEndpointType string,
	protocols []string,
	outpostArn, preferredInstanceType, creatorRequestID string,
) (*ResolverEndpoint, error) {
	b.mu.Lock("CreateResolverEndpoint")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	if direction != directionInbound && direction != directionOutbound {
		return nil, fmt.Errorf(
			"%w: Direction must be %s or %s",
			ErrValidation,
			directionInbound,
			directionOutbound,
		)
	}

	if resolverEndpointType == "" {
		resolverEndpointType = endpointTypeIPV4
	}
	switch resolverEndpointType {
	case endpointTypeIPV4, endpointTypeIPV6, endpointTypeDualStack:
		// valid
	default:
		return nil, fmt.Errorf(
			"%w: ResolverEndpointType must be IPV4, IPV6, or DUALSTACK",
			ErrValidation,
		)
	}

	if len(protocols) == 0 {
		protocols = []string{"Do53"}
	}

	dirPrefix := direction
	if len(dirPrefix) > dirPrefixLen {
		dirPrefix = dirPrefix[:dirPrefixLen]
	}
	id := "rslvr-" + dirPrefix + "-" + uuid.New().String()[:8]
	epARN := arn.Build("route53resolver", region, b.accountID, "resolver-endpoint/"+id)

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
		Region:                region,
		Protocols:             protocolsCopy,
		OutpostArn:            outpostArn,
		PreferredInstanceType: preferredInstanceType,
		CreatorRequestID:      creatorRequestID,
		CreationTime:          now,
		ModificationTime:      now,
	}
	b.endpointsStore(region)[id] = ep

	return cloneEndpoint(ep), nil
}

// ListResolverEndpointIPAddresses returns the IP addresses associated with a resolver endpoint.
func (b *InMemoryBackend) ListResolverEndpointIPAddresses(ctx context.Context, endpointID string) ([]IPAddress, error) {
	b.mu.RLock("ListResolverEndpointIpAddresses")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	ep, ok := b.endpointsStore(region)[endpointID]
	if !ok {
		return nil, fmt.Errorf("%w: resolver endpoint %s not found", ErrNotFound, endpointID)
	}
	cp := make([]IPAddress, len(ep.IPAddresses))
	copy(cp, ep.IPAddresses)

	return cp, nil
}

func (b *InMemoryBackend) GetResolverEndpoint(ctx context.Context, id string) (*ResolverEndpoint, error) {
	b.mu.RLock("GetResolverEndpoint")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	ep, ok := b.endpointsStore(region)[id]
	if !ok {
		return nil, fmt.Errorf("%w: resolver endpoint %s not found", ErrNotFound, id)
	}

	return cloneEndpoint(ep), nil
}

func (b *InMemoryBackend) ListResolverEndpoints(ctx context.Context) []*ResolverEndpoint {
	b.mu.RLock("ListResolverEndpoints")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.endpointsStore(region)
	list := make([]*ResolverEndpoint, 0, len(store))
	for _, ep := range store {
		list = append(list, cloneEndpoint(ep))
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

func (b *InMemoryBackend) DeleteResolverEndpoint(ctx context.Context, id string) error {
	b.mu.Lock("DeleteResolverEndpoint")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	eps := b.endpointsStore(region)
	ep, ok := eps[id]
	if !ok {
		return fmt.Errorf("%w: resolver endpoint %s not found", ErrNotFound, id)
	}

	tags := b.tagsStore(region)
	rules := b.rulesStore(region)
	ruleAssocs := b.ruleAssociationsStore(region)

	// Clean up tags.
	delete(tags, ep.ARN)

	toDelete := make([]string, 0, len(rules))
	for ruleID, r := range rules {
		if r.ResolverEndpointID == id {
			toDelete = append(toDelete, ruleID)
		}
	}
	for _, ruleID := range toDelete {
		// Cascade: delete tags and all rule associations referencing this rule.
		if rule, exists := rules[ruleID]; exists {
			delete(tags, rule.ARN)
		}
		for assocID, assoc := range ruleAssocs {
			if assoc.ResolverRuleID == ruleID {
				delete(ruleAssocs, assocID)
			}
		}
		delete(rules, ruleID)
	}

	delete(eps, id)

	return nil
}

func (b *InMemoryBackend) CreateResolverRule(
	ctx context.Context,
	name, domainName, ruleType, endpointID, creatorRequestID string,
	targetIps []TargetIP,
) (*ResolverRule, error) {
	b.mu.Lock("CreateResolverRule")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

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
			return nil, fmt.Errorf(
				"%w: SYSTEM/RECURSIVE rules must not have a ResolverEndpointId",
				ErrValidation,
			)
		}
		if len(targetIps) > 0 {
			return nil, fmt.Errorf(
				"%w: SYSTEM/RECURSIVE rules must not have TargetIps",
				ErrValidation,
			)
		}
	}

	if endpointID != "" {
		if _, ok := b.endpointsStore(region)[endpointID]; !ok {
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
	ruleARN := arn.Build("route53resolver", region, b.accountID, "resolver-rule/"+id)
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
		Region:             region,
		TargetIps:          tipsCopy,
		CreatorRequestID:   creatorRequestID,
		OwnerID:            b.accountID,
		CreationTime:       now,
		ModificationTime:   now,
	}
	b.rulesStore(region)[id] = r
	cp := cloneRule(r)

	return cp, nil
}

func (b *InMemoryBackend) GetResolverRule(ctx context.Context, id string) (*ResolverRule, error) {
	b.mu.RLock("GetResolverRule")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	r, ok := b.rulesStore(region)[id]
	if !ok {
		return nil, fmt.Errorf("%w: resolver rule %s not found", ErrNotFound, id)
	}

	return cloneRule(r), nil
}

func (b *InMemoryBackend) ListResolverRules(ctx context.Context) []*ResolverRule {
	b.mu.RLock("ListResolverRules")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.rulesStore(region)
	list := make([]*ResolverRule, 0, len(store))
	for _, r := range store {
		list = append(list, cloneRule(r))
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

func (b *InMemoryBackend) DeleteResolverRule(ctx context.Context, id string) error {
	b.mu.Lock("DeleteResolverRule")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	rules := b.rulesStore(region)
	r, ok := rules[id]
	if !ok {
		return fmt.Errorf("%w: resolver rule %s not found", ErrNotFound, id)
	}

	tags := b.tagsStore(region)
	ruleAssocs := b.ruleAssociationsStore(region)

	// Clean up tags.
	delete(tags, r.ARN)

	// Cascade: delete all associations referencing this rule.
	for assocID, assoc := range ruleAssocs {
		if assoc.ResolverRuleID == id {
			delete(ruleAssocs, assocID)
		}
	}

	delete(rules, id)

	return nil
}

// TagResource adds or updates tags on a resource identified by its ARN.
func (b *InMemoryBackend) TagResource(ctx context.Context, resourceARN string, kvs []svcTags.KV) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	tags := b.tagsStore(region)
	existing := tags[resourceARN]
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
	tags[resourceARN] = existing

	return nil
}

// UntagResource removes tags from a resource identified by its ARN.
func (b *InMemoryBackend) UntagResource(ctx context.Context, resourceARN string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	tags := b.tagsStore(region)
	existing := tags[resourceARN]
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
	tags[resourceARN] = remaining

	return nil
}

// ListTagsForResource returns the tags for a resource identified by its ARN.
func (b *InMemoryBackend) ListTagsForResource(ctx context.Context, resourceARN string) []svcTags.KV {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	kvs := b.tagsStore(region)[resourceARN]
	if len(kvs) == 0 {
		return []svcTags.KV{}
	}
	cp := make([]svcTags.KV, len(kvs))
	copy(cp, kvs)

	return cp
}

// CreateFirewallRuleGroup creates a new DNS Firewall rule group.
func (b *InMemoryBackend) CreateFirewallRuleGroup(
	ctx context.Context,
	name, creatorRequestID string,
) (*FirewallRuleGroup, error) {
	b.mu.Lock("CreateFirewallRuleGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	now := currentTime()
	id := "rslvr-frg-" + uuid.New().String()[:8]
	groupARN := arn.Build("route53resolver", region, b.accountID, "firewall-rule-group/"+id)
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
	b.firewallRuleGroupsStore(region)[id] = g
	cp := *g

	return &cp, nil
}

// AssociateFirewallRuleGroup associates a FirewallRuleGroup with a VPC.
func (b *InMemoryBackend) AssociateFirewallRuleGroup(
	ctx context.Context,
	firewallRuleGroupID, vpcID, name, creatorRequestID, mutationProtection string,
	priority int32,
) (*FirewallRuleGroupAssociation, error) {
	b.mu.Lock("AssociateFirewallRuleGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	groups := b.firewallRuleGroupsStore(region)

	if _, ok := groups[firewallRuleGroupID]; !ok {
		return nil, fmt.Errorf(
			"%w: firewall rule group %s not found",
			ErrNotFound,
			firewallRuleGroupID,
		)
	}

	if mutationProtection == "" {
		mutationProtection = mutationProtectionDisabled
	}

	now := currentTime()
	id := "rslvr-frgassoc-" + uuid.New().String()[:8]
	assocARN := arn.Build(
		"route53resolver",
		region,
		b.accountID,
		"firewall-rule-group-association/"+id,
	)
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
	b.firewallRuleGroupAssociationsStore(region)[id] = assoc
	cp := *assoc

	return &cp, nil
}

// AssociateResolverEndpointIPAddress adds an IP address to a resolver endpoint.
func (b *InMemoryBackend) AssociateResolverEndpointIPAddress(
	ctx context.Context,
	endpointID, subnetID, ip, ipv6 string,
) (*ResolverEndpoint, error) {
	b.mu.Lock("AssociateResolverEndpointIPAddress")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	ep, ok := b.endpointsStore(region)[endpointID]
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

// CreateResolverQueryLogConfig creates a new query logging configuration.
func (b *InMemoryBackend) CreateResolverQueryLogConfig(
	ctx context.Context,
	name, creatorRequestID, destinationARN string,
) (*ResolverQueryLogConfig, error) {
	b.mu.Lock("CreateResolverQueryLogConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if !isValidQueryLogDestination(destinationARN) {
		return nil, fmt.Errorf(
			"%w: DestinationArn must be an S3 bucket, CloudWatch Logs log group, or Kinesis Firehose stream ARN",
			ErrValidation,
		)
	}

	now := currentTime()
	id := "rqlc-" + uuid.New().String()[:8]
	configARN := arn.Build(
		"route53resolver",
		region,
		b.accountID,
		"resolver-query-log-config/"+id,
	)
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
	b.queryLogConfigsStore(region)[id] = cfg
	cp := *cfg

	return &cp, nil
}

func isValidQueryLogDestination(destinationARN string) bool {
	for _, prefix := range []string{"arn:aws:s3:::", "arn:aws:logs:", "arn:aws:firehose:"} {
		if strings.HasPrefix(destinationARN, prefix) {
			return true
		}
	}

	return false
}

// AssociateResolverQueryLogConfig associates a VPC with a query log config.
func (b *InMemoryBackend) AssociateResolverQueryLogConfig(
	ctx context.Context,
	queryLogConfigID, resourceID string,
) (*ResolverQueryLogConfigAssociation, error) {
	b.mu.Lock("AssociateResolverQueryLogConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	configs := b.queryLogConfigsStore(region)

	if _, ok := configs[queryLogConfigID]; !ok {
		return nil, fmt.Errorf(
			"%w: resolver query log config %s not found",
			ErrNotFound,
			queryLogConfigID,
		)
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
	b.queryLogConfigAssociationsStore(region)[id] = assoc

	// Increment AssociationCount on the config.
	if cfg, ok := configs[queryLogConfigID]; ok {
		cfg.AssociationCount++
	}

	cp := *assoc

	return &cp, nil
}

// AssociateResolverRule associates a resolver rule with a VPC.
func (b *InMemoryBackend) AssociateResolverRule(
	ctx context.Context,
	resolverRuleID, vpcID, name string,
) (*ResolverRuleAssociation, error) {
	b.mu.Lock("AssociateResolverRule")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.rulesStore(region)[resolverRuleID]; !ok {
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
	b.ruleAssociationsStore(region)[id] = assoc
	cp := *assoc

	return &cp, nil
}

// CreateFirewallDomainList creates a new DNS Firewall domain list.
func (b *InMemoryBackend) CreateFirewallDomainList(
	ctx context.Context,
	name, creatorRequestID string,
) (*FirewallDomainList, error) {
	b.mu.Lock("CreateFirewallDomainList")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	id := "rslvr-fdl-" + uuid.New().String()[:8]
	listARN := arn.Build("route53resolver", region, b.accountID, "firewall-domain-list/"+id)
	dl := &FirewallDomainList{
		ID:               id,
		ARN:              listARN,
		Name:             name,
		CreatorRequestID: creatorRequestID,
		Status:           statusComplete,
	}
	b.firewallDomainListsStore(region)[id] = dl
	cp := *dl

	return &cp, nil
}

// DeleteFirewallDomainList deletes a DNS Firewall domain list.
func (b *InMemoryBackend) DeleteFirewallDomainList(ctx context.Context, id string) (*FirewallDomainList, error) {
	b.mu.Lock("DeleteFirewallDomainList")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	lists := b.firewallDomainListsStore(region)
	dl, ok := lists[id]
	if !ok {
		return nil, fmt.Errorf("%w: firewall domain list %s not found", ErrNotFound, id)
	}
	cp := cloneFirewallDomainList(dl)
	delete(b.tagsStore(region), dl.ARN)
	delete(lists, id)

	return cp, nil
}

// CreateFirewallRuleParams holds all parameters for creating a firewall rule.
type CreateFirewallRuleParams struct {
	FirewallRuleGroupID  string
	Name                 string
	Action               string
	BlockResponse        string
	BlockOverrideDomain  string
	BlockOverrideDNSType string
	Qtype                string
	ConfidenceThreshold  string
	CreatorRequestID     string
	FirewallDomainListID string
	BlockOverrideTTL     int32
	Priority             int32
}

// CreateFirewallRule creates a new rule in a DNS Firewall rule group.
func (b *InMemoryBackend) CreateFirewallRule(ctx context.Context, p CreateFirewallRuleParams) (*FirewallRule, error) {
	b.mu.Lock("CreateFirewallRule")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	groups := b.firewallRuleGroupsStore(region)
	rules := b.firewallRulesStore(region)

	if _, ok := groups[p.FirewallRuleGroupID]; !ok {
		return nil, fmt.Errorf(
			"%w: firewall rule group %s not found",
			ErrNotFound,
			p.FirewallRuleGroupID,
		)
	}

	// Validate BLOCK+OVERRIDE requires BlockOverrideDomain and BlockOverrideDNSType.
	if p.Action == firewallActionBlock && p.BlockResponse == blockResponseOVERRIDE {
		if p.BlockOverrideDomain == "" {
			return nil, fmt.Errorf(
				"%w: BlockOverrideDomain is required when BlockResponse is OVERRIDE",
				ErrValidation,
			)
		}
		if p.BlockOverrideDNSType == "" {
			return nil, fmt.Errorf(
				"%w: BlockOverrideDNSType is required when BlockResponse is OVERRIDE",
				ErrValidation,
			)
		}
	}

	// Auto-assign priority if not provided.
	if p.Priority == 0 {
		maxPriority := int32(0)
		for _, existing := range rules {
			if existing.FirewallRuleGroupID == p.FirewallRuleGroupID &&
				existing.Priority > maxPriority {
				maxPriority = existing.Priority
			}
		}
		p.Priority = maxPriority + firewallPriorityAutoIncrement
	}

	// Validate priority uniqueness within the rule group.
	for _, existing := range rules {
		if existing.FirewallRuleGroupID == p.FirewallRuleGroupID &&
			existing.Priority == p.Priority {
			return nil, fmt.Errorf(
				"%w: a firewall rule with priority %d already exists in group %s",
				ErrValidation,
				p.Priority,
				p.FirewallRuleGroupID,
			)
		}
	}

	now := currentTime()
	id := "rslvr-frr-" + uuid.New().String()[:8]
	ruleARN := arn.Build("route53resolver", region, b.accountID, "firewall-rule/"+id)
	rule := &FirewallRule{
		ID:                   id,
		ARN:                  ruleARN,
		Name:                 p.Name,
		FirewallRuleGroupID:  p.FirewallRuleGroupID,
		FirewallDomainListID: p.FirewallDomainListID,
		Action:               p.Action,
		BlockResponse:        p.BlockResponse,
		BlockOverrideDomain:  p.BlockOverrideDomain,
		BlockOverrideDNSType: p.BlockOverrideDNSType,
		BlockOverrideTTL:     p.BlockOverrideTTL,
		Qtype:                p.Qtype,
		ConfidenceThreshold:  p.ConfidenceThreshold,
		CreatorRequestID:     p.CreatorRequestID,
		CreationTime:         now,
		ModificationTime:     now,
		Priority:             p.Priority,
	}
	rules[id] = rule

	// Increment rule count on the group.
	groups[p.FirewallRuleGroupID].RuleCount++

	cp := *rule

	return &cp, nil
}

// CreateOutpostResolver creates a new Resolver on an Outpost.
func (b *InMemoryBackend) CreateOutpostResolver(
	ctx context.Context,
	name, creatorRequestID, outpostARN, preferredInstanceType string,
	instanceCount int32,
) (*OutpostResolver, error) {
	b.mu.Lock("CreateOutpostResolver")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if instanceCount <= 0 {
		instanceCount = defaultOutpostResolverInstanceCount
	}

	id := "rslvr-op-" + uuid.New().String()[:8]
	resolverARN := arn.Build("route53resolver", region, b.accountID, "outpost-resolver/"+id)
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
	b.outpostResolversStore(region)[id] = r
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
	b.endpointsStore(b.region)[id] = ep

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
	b.rulesStore(b.region)[id] = r

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
	b.firewallRuleGroupsStore(b.region)[id] = g
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
	b.firewallDomainListsStore(b.region)[id] = dl
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
	b.outpostResolversStore(b.region)[id] = r
	cp := *r

	return &cp
}

// AddQueryLogConfigInternal adds a query log config directly to the backend (test seed helper).
func (b *InMemoryBackend) AddQueryLogConfigInternal(
	name, destinationARN string,
) *ResolverQueryLogConfig {
	b.mu.Lock("AddQueryLogConfigInternal")
	defer b.mu.Unlock()

	id := "rqlc-" + uuid.New().String()[:8]
	configARN := arn.Build(
		"route53resolver",
		b.region,
		b.accountID,
		"resolver-query-log-config/"+id,
	)
	cfg := &ResolverQueryLogConfig{
		ID:             id,
		ARN:            configARN,
		Name:           name,
		DestinationARN: destinationARN,
		Status:         statusCreated,
		OwnerID:        b.accountID,
	}
	b.queryLogConfigsStore(b.region)[id] = cfg
	cp := *cfg

	return &cp
}

// AddRuleInternalWithEndpoint adds a resolver rule with an endpoint ID directly to the backend (demo seed helper).
func (b *InMemoryBackend) AddRuleInternalWithEndpoint(
	name, domainName, ruleType, endpointID string,
) *ResolverRule {
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
	b.rulesStore(b.region)[id] = r

	return cloneRule(r)
}

// AddFirewallRuleInternal adds a firewall rule directly to the backend (demo seed helper).
func (b *InMemoryBackend) AddFirewallRuleInternal(
	groupID, name, action, domainListID string,
	priority int32,
) *FirewallRule {
	b.mu.Lock("AddFirewallRuleInternal")
	defer b.mu.Unlock()

	groups := b.firewallRuleGroupsStore(b.region)
	grp, ok := groups[groupID]
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
	b.firewallRulesStore(b.region)[id] = rule
	grp.RuleCount++
	cp := *rule

	return &cp
}

// --- Firewall Rule operations ---

// DeleteFirewallRule deletes a firewall rule by ID and decrements the group rule count.
func (b *InMemoryBackend) DeleteFirewallRule(ctx context.Context, id string) (*FirewallRule, error) {
	b.mu.Lock("DeleteFirewallRule")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	rules := b.firewallRulesStore(region)
	rule, ok := rules[id]
	if !ok {
		return nil, fmt.Errorf("%w: firewall rule %s not found", ErrNotFound, id)
	}
	cp := *rule
	groups := b.firewallRuleGroupsStore(region)
	if grp, exists := groups[rule.FirewallRuleGroupID]; exists && grp.RuleCount > 0 {
		grp.RuleCount--
	}
	delete(rules, id)

	return &cp, nil
}

// UpdateFirewallRuleParams holds all updatable fields for a firewall rule.
type UpdateFirewallRuleParams struct {
	ID                   string
	Name                 string
	Action               string
	BlockResponse        string
	BlockOverrideDomain  string
	BlockOverrideDNSType string
	Qtype                string
	ConfidenceThreshold  string
	FirewallDomainListID string
	BlockOverrideTTL     int32
	Priority             int32
}

// UpdateFirewallRule updates an existing firewall rule.
func (b *InMemoryBackend) UpdateFirewallRule(ctx context.Context, p UpdateFirewallRuleParams) (*FirewallRule, error) {
	b.mu.Lock("UpdateFirewallRule")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	rules := b.firewallRulesStore(region)
	rule, ok := rules[p.ID]
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
	if p.BlockOverrideDNSType != "" {
		rule.BlockOverrideDNSType = p.BlockOverrideDNSType
	}
	if p.BlockOverrideTTL != 0 {
		rule.BlockOverrideTTL = p.BlockOverrideTTL
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
func (b *InMemoryBackend) ListFirewallRules(ctx context.Context, firewallRuleGroupID string) []*FirewallRule {
	b.mu.RLock("ListFirewallRules")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.firewallRulesStore(region)
	list := make([]*FirewallRule, 0, len(store))
	for _, r := range store {
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
func (b *InMemoryBackend) DeleteFirewallRuleGroup(ctx context.Context, id string) (*FirewallRuleGroup, error) {
	b.mu.Lock("DeleteFirewallRuleGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	groups := b.firewallRuleGroupsStore(region)
	grp, ok := groups[id]
	if !ok {
		return nil, fmt.Errorf("%w: firewall rule group %s not found", ErrNotFound, id)
	}
	cp := *grp

	// Clean up tags.
	delete(b.tagsStore(region), grp.ARN)

	// Cascade: delete rules belonging to this group.
	rules := b.firewallRulesStore(region)
	for ruleID, rule := range rules {
		if rule.FirewallRuleGroupID == id {
			delete(rules, ruleID)
		}
	}
	// Cascade: delete associations for this group.
	assocs := b.firewallRuleGroupAssociationsStore(region)
	for assocID, assoc := range assocs {
		if assoc.FirewallRuleGroupID == id {
			delete(assocs, assocID)
		}
	}
	delete(groups, id)

	return &cp, nil
}

// GetFirewallRuleGroup retrieves a firewall rule group by ID.
func (b *InMemoryBackend) GetFirewallRuleGroup(ctx context.Context, id string) (*FirewallRuleGroup, error) {
	b.mu.RLock("GetFirewallRuleGroup")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	grp, ok := b.firewallRuleGroupsStore(region)[id]
	if !ok {
		return nil, fmt.Errorf("%w: firewall rule group %s not found", ErrNotFound, id)
	}
	cp := *grp

	return &cp, nil
}

// ListFirewallRuleGroups lists all firewall rule groups.
func (b *InMemoryBackend) ListFirewallRuleGroups(ctx context.Context) []*FirewallRuleGroup {
	b.mu.RLock("ListFirewallRuleGroups")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.firewallRuleGroupsStore(region)
	list := make([]*FirewallRuleGroup, 0, len(store))
	for _, g := range store {
		cp := *g
		list = append(list, &cp)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// GetFirewallRuleGroupPolicy retrieves the resource policy for a firewall rule group ARN.
func (b *InMemoryBackend) GetFirewallRuleGroupPolicy(ctx context.Context, arnStr string) string {
	b.mu.RLock("GetFirewallRuleGroupPolicy")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return b.firewallRuleGroupPoliciesStore(region)[arnStr]
}

// PutFirewallRuleGroupPolicy stores a resource policy for a firewall rule group ARN.
func (b *InMemoryBackend) PutFirewallRuleGroupPolicy(ctx context.Context, arnStr, policy string) error {
	b.mu.Lock("PutFirewallRuleGroupPolicy")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	b.firewallRuleGroupPoliciesStore(region)[arnStr] = policy

	return nil
}

// --- Firewall Rule Group Association operations ---

// GetFirewallRuleGroupAssociation retrieves an association by ID.
func (b *InMemoryBackend) GetFirewallRuleGroupAssociation(
	ctx context.Context,
	id string,
) (*FirewallRuleGroupAssociation, error) {
	b.mu.RLock("GetFirewallRuleGroupAssociation")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	assoc, ok := b.firewallRuleGroupAssociationsStore(region)[id]
	if !ok {
		return nil, fmt.Errorf("%w: firewall rule group association %s not found", ErrNotFound, id)
	}
	cp := *assoc

	return &cp, nil
}

// ListFirewallRuleGroupAssociations lists associations, optionally filtered by VPC or group.
func (b *InMemoryBackend) ListFirewallRuleGroupAssociations(
	ctx context.Context,
	vpcID, firewallRuleGroupID string,
) []*FirewallRuleGroupAssociation {
	b.mu.RLock("ListFirewallRuleGroupAssociations")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.firewallRuleGroupAssociationsStore(region)
	list := make([]*FirewallRuleGroupAssociation, 0, len(store))
	for _, a := range store {
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
func (b *InMemoryBackend) DisassociateFirewallRuleGroup(
	ctx context.Context,
	id string,
) (*FirewallRuleGroupAssociation, error) {
	b.mu.Lock("DisassociateFirewallRuleGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	assocs := b.firewallRuleGroupAssociationsStore(region)
	assoc, ok := assocs[id]
	if !ok {
		return nil, fmt.Errorf("%w: firewall rule group association %s not found", ErrNotFound, id)
	}

	if assoc.MutationProtection == mutationProtectionEnabled {
		return nil, fmt.Errorf(
			"%w: cannot disassociate: MutationProtection is ENABLED",
			ErrValidation,
		)
	}

	cp := *assoc
	delete(assocs, id)

	return &cp, nil
}

// UpdateFirewallRuleGroupAssociation updates name, priority, or mutation protection of an association.
func (b *InMemoryBackend) UpdateFirewallRuleGroupAssociation(
	ctx context.Context,
	id, name, mutationProtection string,
	priority int32,
) (*FirewallRuleGroupAssociation, error) {
	b.mu.Lock("UpdateFirewallRuleGroupAssociation")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	assocs := b.firewallRuleGroupAssociationsStore(region)
	assoc, ok := assocs[id]
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
		if mutationProtection != mutationProtectionEnabled &&
			mutationProtection != mutationProtectionDisabled {
			return nil, fmt.Errorf(
				"%w: MutationProtection must be ENABLED or DISABLED",
				ErrValidation,
			)
		}
		assoc.MutationProtection = mutationProtection
	}
	assoc.ModificationTime = currentTime()
	cp := *assoc

	return &cp, nil
}

// --- Firewall Domain List operations ---

// GetFirewallDomainList retrieves a domain list by ID.
func (b *InMemoryBackend) GetFirewallDomainList(ctx context.Context, id string) (*FirewallDomainList, error) {
	b.mu.RLock("GetFirewallDomainList")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	dl, ok := b.firewallDomainListsStore(region)[id]
	if !ok {
		return nil, fmt.Errorf("%w: firewall domain list %s not found", ErrNotFound, id)
	}
	cp := cloneFirewallDomainList(dl)

	return cp, nil
}

// ListFirewallDomainLists lists all firewall domain lists.
func (b *InMemoryBackend) ListFirewallDomainLists(ctx context.Context) []*FirewallDomainList {
	b.mu.RLock("ListFirewallDomainLists")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.firewallDomainListsStore(region)
	list := make([]*FirewallDomainList, 0, len(store))
	for _, dl := range store {
		list = append(list, cloneFirewallDomainList(dl))
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// ListFirewallDomains returns the domains stored in a domain list.
func (b *InMemoryBackend) ListFirewallDomains(ctx context.Context, id string) ([]string, error) {
	b.mu.RLock("ListFirewallDomains")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	dl, ok := b.firewallDomainListsStore(region)[id]
	if !ok {
		return nil, fmt.Errorf("%w: firewall domain list %s not found", ErrNotFound, id)
	}
	cp := make([]string, len(dl.Domains))
	copy(cp, dl.Domains)

	return cp, nil
}

// UpdateFirewallDomains replaces, adds, or removes domains in a domain list.
func (b *InMemoryBackend) UpdateFirewallDomains(
	ctx context.Context,
	id, operation string,
	domains []string,
) (*FirewallDomainList, error) {
	b.mu.Lock("UpdateFirewallDomains")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	dl, ok := b.firewallDomainListsStore(region)[id]
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
func (b *InMemoryBackend) ImportFirewallDomains(
	ctx context.Context,
	id, operation, domainFileURL string,
) (*FirewallDomainList, error) {
	b.mu.Lock("ImportFirewallDomains")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	dl, ok := b.firewallDomainListsStore(region)[id]
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
func (b *InMemoryBackend) GetFirewallConfig(ctx context.Context, resourceID string) *FirewallConfig {
	b.mu.Lock("GetFirewallConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.firewallConfigsStore(region)
	if cfg, ok := store[resourceID]; ok {
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
	store[resourceID] = cfg
	cp := *cfg

	return &cp
}

// UpdateFirewallConfig updates the firewall fail-open setting for a resource.
func (b *InMemoryBackend) UpdateFirewallConfig(
	ctx context.Context,
	resourceID, firewallFailOpen string,
) (*FirewallConfig, error) {
	b.mu.Lock("UpdateFirewallConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if firewallFailOpen != firewallFailOpenEnabled && firewallFailOpen != firewallFailOpenDisabled {
		return nil, fmt.Errorf(
			"%w: FirewallFailOpen must be %s or %s",
			ErrValidation,
			firewallFailOpenEnabled,
			firewallFailOpenDisabled,
		)
	}

	store := b.firewallConfigsStore(region)
	cfg, ok := store[resourceID]
	if !ok {
		id := "fwc-" + uuid.New().String()[:8]
		cfg = &FirewallConfig{
			ID:         id,
			OwnerID:    b.accountID,
			ResourceID: resourceID,
		}
		store[resourceID] = cfg
	}
	cfg.FirewallFailOpen = firewallFailOpen
	cp := *cfg

	return &cp, nil
}

// ListFirewallConfigs lists all firewall configs.
func (b *InMemoryBackend) ListFirewallConfigs(ctx context.Context) []*FirewallConfig {
	b.mu.RLock("ListFirewallConfigs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.firewallConfigsStore(region)
	list := make([]*FirewallConfig, 0, len(store))
	for _, cfg := range store {
		cp := *cfg
		list = append(list, &cp)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].ResourceID < list[j].ResourceID })

	return list
}

// --- Resolver Config operations ---

// GetResolverConfig returns or lazily creates the resolver config for a resource (VPC).
func (b *InMemoryBackend) GetResolverConfig(ctx context.Context, resourceID string) *ResolverConfig {
	b.mu.Lock("GetResolverConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.resolverConfigsStore(region)
	if cfg, ok := store[resourceID]; ok {
		cp := *cfg

		return &cp
	}
	id := "rslvr-rc-" + uuid.New().String()[:8]
	cfgARN := arn.Build("route53resolver", region, b.accountID, "resolver-config/"+id)
	cfg := &ResolverConfig{
		ID:                 id,
		ARN:                cfgARN,
		OwnerID:            b.accountID,
		ResourceID:         resourceID,
		AutodefinedReverse: "DISABLED",
	}
	store[resourceID] = cfg
	cp := *cfg

	return &cp
}

// UpdateResolverConfig updates the AutodefinedReverse setting for a resource.
func (b *InMemoryBackend) UpdateResolverConfig(
	ctx context.Context,
	resourceID, autodefinedReverse string,
) (*ResolverConfig, error) {
	b.mu.Lock("UpdateResolverConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if autodefinedReverse != autodefinedReverseEnabled &&
		autodefinedReverse != autodefinedReverseDisabled {
		return nil, fmt.Errorf(
			"%w: AutodefinedReverse must be %s or %s",
			ErrValidation,
			autodefinedReverseEnabled,
			autodefinedReverseDisabled,
		)
	}

	store := b.resolverConfigsStore(region)
	cfg, ok := store[resourceID]
	if !ok {
		id := "rslvr-rc-" + uuid.New().String()[:8]
		cfgARN := arn.Build("route53resolver", region, b.accountID, "resolver-config/"+id)
		cfg = &ResolverConfig{
			ID:         id,
			ARN:        cfgARN,
			OwnerID:    b.accountID,
			ResourceID: resourceID,
		}
		store[resourceID] = cfg
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
func (b *InMemoryBackend) ListResolverConfigs(ctx context.Context) []*ResolverConfig {
	b.mu.RLock("ListResolverConfigs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.resolverConfigsStore(region)
	list := make([]*ResolverConfig, 0, len(store))
	for _, cfg := range store {
		cp := *cfg
		list = append(list, &cp)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].ResourceID < list[j].ResourceID })

	return list
}

// --- Resolver DNSSEC Config operations ---

// GetResolverDnssecConfig returns or lazily creates the DNSSEC config for a resource.
func (b *InMemoryBackend) GetResolverDnssecConfig(ctx context.Context, resourceID string) *ResolverDnssecConfig {
	b.mu.Lock("GetResolverDnssecConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.resolverDnssecConfigsStore(region)
	if cfg, ok := store[resourceID]; ok {
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
	store[resourceID] = cfg
	cp := *cfg

	return &cp
}

// UpdateResolverDnssecConfig updates DNSSEC validation for a resource.
func (b *InMemoryBackend) UpdateResolverDnssecConfig(
	ctx context.Context,
	resourceID, validation string,
) (*ResolverDnssecConfig, error) {
	b.mu.Lock("UpdateResolverDnssecConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if validation != dnssecValidationEnable && validation != dnssecValidationDisable {
		return nil, fmt.Errorf(
			"%w: Validation must be %s or %s",
			ErrValidation,
			dnssecValidationEnable,
			dnssecValidationDisable,
		)
	}

	store := b.resolverDnssecConfigsStore(region)
	cfg, ok := store[resourceID]
	if !ok {
		id := "rslvr-dnssec-" + uuid.New().String()[:8]
		cfg = &ResolverDnssecConfig{
			ID:         id,
			OwnerID:    b.accountID,
			ResourceID: resourceID,
		}
		store[resourceID] = cfg
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
func (b *InMemoryBackend) ListResolverDnssecConfigs(ctx context.Context) []*ResolverDnssecConfig {
	b.mu.RLock("ListResolverDnssecConfigs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.resolverDnssecConfigsStore(region)
	list := make([]*ResolverDnssecConfig, 0, len(store))
	for _, cfg := range store {
		cp := *cfg
		list = append(list, &cp)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].ResourceID < list[j].ResourceID })

	return list
}

// --- Outpost Resolver operations ---

// DeleteOutpostResolver deletes an outpost resolver.
func (b *InMemoryBackend) DeleteOutpostResolver(ctx context.Context, id string) (*OutpostResolver, error) {
	b.mu.Lock("DeleteOutpostResolver")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.outpostResolversStore(region)
	r, ok := store[id]
	if !ok {
		return nil, fmt.Errorf("%w: outpost resolver %s not found", ErrNotFound, id)
	}
	cp := *r
	delete(store, id)

	return &cp, nil
}

// GetOutpostResolver retrieves an outpost resolver by ID.
func (b *InMemoryBackend) GetOutpostResolver(ctx context.Context, id string) (*OutpostResolver, error) {
	b.mu.RLock("GetOutpostResolver")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	r, ok := b.outpostResolversStore(region)[id]
	if !ok {
		return nil, fmt.Errorf("%w: outpost resolver %s not found", ErrNotFound, id)
	}
	cp := *r

	return &cp, nil
}

// ListOutpostResolvers lists all outpost resolvers.
func (b *InMemoryBackend) ListOutpostResolvers(ctx context.Context) []*OutpostResolver {
	b.mu.RLock("ListOutpostResolvers")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.outpostResolversStore(region)
	list := make([]*OutpostResolver, 0, len(store))
	for _, r := range store {
		cp := *r
		list = append(list, &cp)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// UpdateOutpostResolver updates name, preferred instance type, or instance count.
func (b *InMemoryBackend) UpdateOutpostResolver(
	ctx context.Context,
	id, name, preferredInstanceType string,
	instanceCount int32,
) (*OutpostResolver, error) {
	b.mu.Lock("UpdateOutpostResolver")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	r, ok := b.outpostResolversStore(region)[id]
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
func (b *InMemoryBackend) DeleteResolverQueryLogConfig(
	ctx context.Context,
	id string,
) (*ResolverQueryLogConfig, error) {
	b.mu.Lock("DeleteResolverQueryLogConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	configs := b.queryLogConfigsStore(region)
	cfg, ok := configs[id]
	if !ok {
		return nil, fmt.Errorf("%w: resolver query log config %s not found", ErrNotFound, id)
	}
	cp := *cfg

	// Clean up tags.
	delete(b.tagsStore(region), cfg.ARN)

	// Cascade: remove all associations referencing this config.
	assocs := b.queryLogConfigAssociationsStore(region)
	for assocID, assoc := range assocs {
		if assoc.ResolverQueryLogConfigID == id {
			delete(assocs, assocID)
		}
	}
	delete(configs, id)

	return &cp, nil
}

// GetResolverQueryLogConfig retrieves a query log config by ID.
func (b *InMemoryBackend) GetResolverQueryLogConfig(ctx context.Context, id string) (*ResolverQueryLogConfig, error) {
	b.mu.RLock("GetResolverQueryLogConfig")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	cfg, ok := b.queryLogConfigsStore(region)[id]
	if !ok {
		return nil, fmt.Errorf("%w: resolver query log config %s not found", ErrNotFound, id)
	}
	cp := *cfg

	return &cp, nil
}

// ListResolverQueryLogConfigs lists all query log configs.
func (b *InMemoryBackend) ListResolverQueryLogConfigs(ctx context.Context) []*ResolverQueryLogConfig {
	b.mu.RLock("ListResolverQueryLogConfigs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.queryLogConfigsStore(region)
	list := make([]*ResolverQueryLogConfig, 0, len(store))
	for _, cfg := range store {
		cp := *cfg
		list = append(list, &cp)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// GetResolverQueryLogConfigAssociation retrieves an association by ID.
func (b *InMemoryBackend) GetResolverQueryLogConfigAssociation(
	ctx context.Context,
	id string,
) (*ResolverQueryLogConfigAssociation, error) {
	b.mu.RLock("GetResolverQueryLogConfigAssociation")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	assoc, ok := b.queryLogConfigAssociationsStore(region)[id]
	if !ok {
		return nil, fmt.Errorf(
			"%w: resolver query log config association %s not found",
			ErrNotFound,
			id,
		)
	}
	cp := *assoc

	return &cp, nil
}

// DisassociateResolverQueryLogConfig removes a query log config association.
func (b *InMemoryBackend) DisassociateResolverQueryLogConfig(
	ctx context.Context,
	id string,
) (*ResolverQueryLogConfigAssociation, error) {
	b.mu.Lock("DisassociateResolverQueryLogConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	assocs := b.queryLogConfigAssociationsStore(region)
	assoc, ok := assocs[id]
	if !ok {
		return nil, fmt.Errorf(
			"%w: resolver query log config association %s not found",
			ErrNotFound,
			id,
		)
	}
	cp := *assoc
	delete(assocs, id)

	// Decrement AssociationCount on the config.
	configs := b.queryLogConfigsStore(region)
	if cfg := configs[assoc.ResolverQueryLogConfigID]; cfg != nil && cfg.AssociationCount > 0 {
		cfg.AssociationCount--
	}

	return &cp, nil
}

// ListResolverQueryLogConfigAssociations lists all query log config associations.
func (b *InMemoryBackend) ListResolverQueryLogConfigAssociations(
	ctx context.Context,
) []*ResolverQueryLogConfigAssociation {
	b.mu.RLock("ListResolverQueryLogConfigAssociations")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.queryLogConfigAssociationsStore(region)
	list := make([]*ResolverQueryLogConfigAssociation, 0, len(store))
	for _, a := range store {
		cp := *a
		list = append(list, &cp)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })

	return list
}

// GetResolverQueryLogConfigPolicy retrieves a resource policy for a query log config ARN.
func (b *InMemoryBackend) GetResolverQueryLogConfigPolicy(ctx context.Context, arnStr string) string {
	b.mu.RLock("GetResolverQueryLogConfigPolicy")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return b.queryLogConfigPoliciesStore(region)[arnStr]
}

// PutResolverQueryLogConfigPolicy stores a resource policy for a query log config ARN.
func (b *InMemoryBackend) PutResolverQueryLogConfigPolicy(ctx context.Context, arnStr, policy string) error {
	b.mu.Lock("PutResolverQueryLogConfigPolicy")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	b.queryLogConfigPoliciesStore(region)[arnStr] = policy

	return nil
}

// --- Resolver Rule Association operations ---

// GetResolverRuleAssociation retrieves a rule association by ID.
func (b *InMemoryBackend) GetResolverRuleAssociation(ctx context.Context, id string) (*ResolverRuleAssociation, error) {
	b.mu.RLock("GetResolverRuleAssociation")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	assoc, ok := b.ruleAssociationsStore(region)[id]
	if !ok {
		return nil, fmt.Errorf("%w: resolver rule association %s not found", ErrNotFound, id)
	}
	cp := *assoc

	return &cp, nil
}

// DisassociateResolverRule removes a resolver rule association.
func (b *InMemoryBackend) DisassociateResolverRule(ctx context.Context, id string) (*ResolverRuleAssociation, error) {
	b.mu.Lock("DisassociateResolverRule")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	assocs := b.ruleAssociationsStore(region)
	assoc, ok := assocs[id]
	if !ok {
		return nil, fmt.Errorf("%w: resolver rule association %s not found", ErrNotFound, id)
	}
	cp := *assoc
	delete(assocs, id)

	return &cp, nil
}

// ListResolverRuleAssociations lists all resolver rule associations.
func (b *InMemoryBackend) ListResolverRuleAssociations(ctx context.Context) []*ResolverRuleAssociation {
	b.mu.RLock("ListResolverRuleAssociations")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.ruleAssociationsStore(region)
	list := make([]*ResolverRuleAssociation, 0, len(store))
	for _, a := range store {
		cp := *a
		list = append(list, &cp)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })

	return list
}

// GetResolverRulePolicy retrieves a resource policy for a resolver rule ARN.
func (b *InMemoryBackend) GetResolverRulePolicy(ctx context.Context, arnStr string) string {
	b.mu.RLock("GetResolverRulePolicy")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return b.resolverRulePoliciesStore(region)[arnStr]
}

// PutResolverRulePolicy stores a resource policy for a resolver rule ARN.
func (b *InMemoryBackend) PutResolverRulePolicy(ctx context.Context, arnStr, policy string) error {
	b.mu.Lock("PutResolverRulePolicy")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	b.resolverRulePoliciesStore(region)[arnStr] = policy

	return nil
}

// --- Resolver Endpoint Update ---

// UpdateResolverEndpoint updates name, endpoint type, and/or protocols of a resolver endpoint.
func (b *InMemoryBackend) UpdateResolverEndpoint(
	ctx context.Context,
	id, name, resolverEndpointType string,
	protocols []string,
) (*ResolverEndpoint, error) {
	b.mu.Lock("UpdateResolverEndpoint")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	ep, ok := b.endpointsStore(region)[id]
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
			return nil, fmt.Errorf(
				"%w: ResolverEndpointType must be IPV4, IPV6, or DUALSTACK",
				ErrValidation,
			)
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
func (b *InMemoryBackend) DisassociateResolverEndpointIPAddress(
	ctx context.Context,
	endpointID, ipID string,
) (*ResolverEndpoint, error) {
	b.mu.Lock("DisassociateResolverEndpointIPAddress")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	ep, ok := b.endpointsStore(region)[endpointID]
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
		return nil, fmt.Errorf(
			"%w: IP address %s not found on endpoint %s",
			ErrNotFound,
			ipID,
			endpointID,
		)
	}
	ep.IPAddresses = newIPs

	return cloneEndpoint(ep), nil
}

// --- Resolver Rule Update ---

// UpdateResolverRule updates fields of a resolver rule.
func (b *InMemoryBackend) UpdateResolverRule(
	ctx context.Context,
	id, name, resolverEndpointID string,
	targetIps []TargetIP,
) (*ResolverRule, error) {
	b.mu.Lock("UpdateResolverRule")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	r, ok := b.rulesStore(region)[id]
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
