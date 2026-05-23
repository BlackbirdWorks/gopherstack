package route53resolver

import svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"

// StorageBackend defines the interface for Route 53 Resolver backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	// Endpoint operations
	CreateResolverEndpoint(
		name, direction, vpcID string,
		ips []IPAddress,
		securityGroupIDs []string,
		resolverEndpointType string,
		protocols []string,
		outpostArn, preferredInstanceType, creatorRequestID string,
	) (*ResolverEndpoint, error)
	GetResolverEndpoint(id string) (*ResolverEndpoint, error)
	ListResolverEndpoints() []*ResolverEndpoint
	DeleteResolverEndpoint(id string) error
	ListResolverEndpointIPAddresses(endpointID string) ([]IPAddress, error)
	AssociateResolverEndpointIPAddress(
		endpointID, subnetID, ip, ipv6 string,
	) (*ResolverEndpoint, error)
	UpdateResolverEndpoint(
		id, name, resolverEndpointType string,
		protocols []string,
	) (*ResolverEndpoint, error)
	DisassociateResolverEndpointIPAddress(endpointID, ipID string) (*ResolverEndpoint, error)

	// Rule operations
	CreateResolverRule(
		name, domainName, ruleType, endpointID, creatorRequestID string,
		targetIps []TargetIP,
	) (*ResolverRule, error)
	GetResolverRule(id string) (*ResolverRule, error)
	ListResolverRules() []*ResolverRule
	DeleteResolverRule(id string) error
	UpdateResolverRule(
		id, name, resolverEndpointID string,
		targetIps []TargetIP,
	) (*ResolverRule, error)
	AssociateResolverRule(resolverRuleID, vpcID, name string) (*ResolverRuleAssociation, error)
	GetResolverRuleAssociation(id string) (*ResolverRuleAssociation, error)
	DisassociateResolverRule(id string) (*ResolverRuleAssociation, error)
	ListResolverRuleAssociations() []*ResolverRuleAssociation
	GetResolverRulePolicy(arn string) string
	PutResolverRulePolicy(arn, policy string) error

	// Firewall rule group operations
	CreateFirewallRuleGroup(name, creatorRequestID string) (*FirewallRuleGroup, error)
	GetFirewallRuleGroup(id string) (*FirewallRuleGroup, error)
	ListFirewallRuleGroups() []*FirewallRuleGroup
	DeleteFirewallRuleGroup(id string) (*FirewallRuleGroup, error)
	GetFirewallRuleGroupPolicy(arn string) string
	PutFirewallRuleGroupPolicy(arn, policy string) error
	AssociateFirewallRuleGroup(
		firewallRuleGroupID, vpcID, name, creatorRequestID, mutationProtection string,
		priority int32,
	) (*FirewallRuleGroupAssociation, error)
	GetFirewallRuleGroupAssociation(id string) (*FirewallRuleGroupAssociation, error)
	ListFirewallRuleGroupAssociations(
		vpcID, firewallRuleGroupID string,
	) []*FirewallRuleGroupAssociation
	DisassociateFirewallRuleGroup(id string) (*FirewallRuleGroupAssociation, error)
	UpdateFirewallRuleGroupAssociation(
		id, name, mutationProtection string,
		priority int32,
	) (*FirewallRuleGroupAssociation, error)

	// Firewall domain list operations
	CreateFirewallDomainList(name, creatorRequestID string) (*FirewallDomainList, error)
	GetFirewallDomainList(id string) (*FirewallDomainList, error)
	ListFirewallDomainLists() []*FirewallDomainList
	DeleteFirewallDomainList(id string) (*FirewallDomainList, error)
	ListFirewallDomains(id string) ([]string, error)
	UpdateFirewallDomains(id, operation string, domains []string) (*FirewallDomainList, error)
	ImportFirewallDomains(id, operation, domainFileURL string) (*FirewallDomainList, error)

	// Firewall rule operations
	CreateFirewallRule(p CreateFirewallRuleParams) (*FirewallRule, error)
	DeleteFirewallRule(id string) (*FirewallRule, error)
	UpdateFirewallRule(p UpdateFirewallRuleParams) (*FirewallRule, error)
	ListFirewallRules(firewallRuleGroupID string) []*FirewallRule

	// Firewall config operations
	GetFirewallConfig(resourceID string) *FirewallConfig
	UpdateFirewallConfig(resourceID, firewallFailOpen string) (*FirewallConfig, error)
	ListFirewallConfigs() []*FirewallConfig

	// Outpost resolver operations
	CreateOutpostResolver(
		name, creatorRequestID, outpostARN, preferredInstanceType string,
		instanceCount int32,
	) (*OutpostResolver, error)
	GetOutpostResolver(id string) (*OutpostResolver, error)
	ListOutpostResolvers() []*OutpostResolver
	DeleteOutpostResolver(id string) (*OutpostResolver, error)
	UpdateOutpostResolver(
		id, name, preferredInstanceType string,
		instanceCount int32,
	) (*OutpostResolver, error)

	// Query log config operations
	CreateResolverQueryLogConfig(
		name, creatorRequestID, destinationARN string,
	) (*ResolverQueryLogConfig, error)
	GetResolverQueryLogConfig(id string) (*ResolverQueryLogConfig, error)
	ListResolverQueryLogConfigs() []*ResolverQueryLogConfig
	DeleteResolverQueryLogConfig(id string) (*ResolverQueryLogConfig, error)
	AssociateResolverQueryLogConfig(
		queryLogConfigID, resourceID string,
	) (*ResolverQueryLogConfigAssociation, error)
	GetResolverQueryLogConfigAssociation(id string) (*ResolverQueryLogConfigAssociation, error)
	DisassociateResolverQueryLogConfig(id string) (*ResolverQueryLogConfigAssociation, error)
	ListResolverQueryLogConfigAssociations() []*ResolverQueryLogConfigAssociation
	GetResolverQueryLogConfigPolicy(arn string) string
	PutResolverQueryLogConfigPolicy(arn, policy string) error

	// Resolver config operations
	GetResolverConfig(resourceID string) *ResolverConfig
	UpdateResolverConfig(resourceID, autodefinedReverse string) (*ResolverConfig, error)
	ListResolverConfigs() []*ResolverConfig

	// Resolver DNSSEC config operations
	GetResolverDnssecConfig(resourceID string) *ResolverDnssecConfig
	UpdateResolverDnssecConfig(resourceID, validation string) (*ResolverDnssecConfig, error)
	ListResolverDnssecConfigs() []*ResolverDnssecConfig

	// Tag operations
	TagResource(resourceARN string, kvs []svcTags.KV) error
	UntagResource(resourceARN string, keys []string) error
	ListTagsForResource(resourceARN string) []svcTags.KV

	// Lifecycle
	Reset()
	Region() string
	AccountID() string
	Snapshot() []byte
	Restore(data []byte) error
}

// compile-time assertion that InMemoryBackend satisfies StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
