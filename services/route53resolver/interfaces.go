package route53resolver

import (
	"context"

	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// StorageBackend defines the interface for Route 53 Resolver backend implementations.
// All mutating methods must be safe for concurrent use.
//
// Regional operations take a context.Context from which the target AWS region is
// resolved (see getRegion); same-named resources are isolated per region.
type StorageBackend interface {
	// Endpoint operations
	CreateResolverEndpoint(
		ctx context.Context,
		name, direction, vpcID string,
		ips []IPAddress,
		securityGroupIDs []string,
		resolverEndpointType string,
		protocols []string,
		outpostArn, preferredInstanceType, creatorRequestID string,
		rniEnhancedMetricsEnabled, targetNameServerMetricsEnabled bool,
	) (*ResolverEndpoint, error)
	GetResolverEndpoint(ctx context.Context, id string) (*ResolverEndpoint, error)
	ListResolverEndpoints(ctx context.Context) []*ResolverEndpoint
	DeleteResolverEndpoint(ctx context.Context, id string) error
	ListResolverEndpointIPAddresses(ctx context.Context, endpointID string) ([]IPAddress, error)
	AssociateResolverEndpointIPAddress(
		ctx context.Context,
		endpointID, subnetID, ip, ipv6 string,
	) (*ResolverEndpoint, error)
	UpdateResolverEndpoint(
		ctx context.Context,
		id, name, resolverEndpointType string,
		protocols []string,
		rniEnhancedMetricsEnabled, targetNameServerMetricsEnabled *bool,
	) (*ResolverEndpoint, error)
	DisassociateResolverEndpointIPAddress(ctx context.Context, endpointID, ipID string) (*ResolverEndpoint, error)

	// Rule operations
	CreateResolverRule(
		ctx context.Context,
		name, domainName, ruleType, endpointID, creatorRequestID string,
		targetIps []TargetIP,
	) (*ResolverRule, error)
	GetResolverRule(ctx context.Context, id string) (*ResolverRule, error)
	ListResolverRules(ctx context.Context) []*ResolverRule
	DeleteResolverRule(ctx context.Context, id string) error
	UpdateResolverRule(
		ctx context.Context,
		id, name, resolverEndpointID string,
		targetIps []TargetIP,
	) (*ResolverRule, error)
	AssociateResolverRule(ctx context.Context, resolverRuleID, vpcID, name string) (*ResolverRuleAssociation, error)
	GetResolverRuleAssociation(ctx context.Context, id string) (*ResolverRuleAssociation, error)
	DisassociateResolverRule(
		ctx context.Context,
		resolverRuleID, vpcID string,
	) (*ResolverRuleAssociation, error)
	ListResolverRuleAssociations(ctx context.Context) []*ResolverRuleAssociation
	GetResolverRulePolicy(ctx context.Context, arn string) string
	PutResolverRulePolicy(ctx context.Context, arn, policy string) error

	// Firewall rule group operations
	CreateFirewallRuleGroup(ctx context.Context, name, creatorRequestID string) (*FirewallRuleGroup, error)
	GetFirewallRuleGroup(ctx context.Context, id string) (*FirewallRuleGroup, error)
	ListFirewallRuleGroups(ctx context.Context) []*FirewallRuleGroup
	DeleteFirewallRuleGroup(ctx context.Context, id string) (*FirewallRuleGroup, error)
	GetFirewallRuleGroupPolicy(ctx context.Context, arn string) string
	PutFirewallRuleGroupPolicy(ctx context.Context, arn, policy string) error
	AssociateFirewallRuleGroup(
		ctx context.Context,
		firewallRuleGroupID, vpcID, name, creatorRequestID, mutationProtection string,
		priority int32,
	) (*FirewallRuleGroupAssociation, error)
	GetFirewallRuleGroupAssociation(ctx context.Context, id string) (*FirewallRuleGroupAssociation, error)
	ListFirewallRuleGroupAssociations(
		ctx context.Context,
		vpcID, firewallRuleGroupID string,
	) []*FirewallRuleGroupAssociation
	DisassociateFirewallRuleGroup(ctx context.Context, id string) (*FirewallRuleGroupAssociation, error)
	UpdateFirewallRuleGroupAssociation(
		ctx context.Context,
		id, name, mutationProtection string,
		priority int32,
	) (*FirewallRuleGroupAssociation, error)

	// Firewall domain list operations
	CreateFirewallDomainList(ctx context.Context, name, creatorRequestID string) (*FirewallDomainList, error)
	GetFirewallDomainList(ctx context.Context, id string) (*FirewallDomainList, error)
	ListFirewallDomainLists(ctx context.Context) []*FirewallDomainList
	DeleteFirewallDomainList(ctx context.Context, id string) (*FirewallDomainList, error)
	ListFirewallDomains(ctx context.Context, id string) ([]string, error)
	UpdateFirewallDomains(ctx context.Context, id, operation string, domains []string) (*FirewallDomainList, error)
	ImportFirewallDomains(ctx context.Context, id, operation, domainFileURL string) (*FirewallDomainList, error)

	// Firewall rule operations
	CreateFirewallRule(ctx context.Context, p CreateFirewallRuleParams) (*FirewallRule, error)
	DeleteFirewallRule(
		ctx context.Context,
		firewallRuleGroupID, firewallDomainListID, firewallThreatProtectionID string,
	) (*FirewallRule, error)
	UpdateFirewallRule(ctx context.Context, p UpdateFirewallRuleParams) (*FirewallRule, error)
	ListFirewallRules(ctx context.Context, firewallRuleGroupID string) []*FirewallRule

	// Firewall config operations
	GetFirewallConfig(ctx context.Context, resourceID string) *FirewallConfig
	UpdateFirewallConfig(ctx context.Context, resourceID, firewallFailOpen string) (*FirewallConfig, error)
	ListFirewallConfigs(ctx context.Context) []*FirewallConfig

	// Outpost resolver operations
	CreateOutpostResolver(
		ctx context.Context,
		name, creatorRequestID, outpostARN, preferredInstanceType string,
		instanceCount int32,
	) (*OutpostResolver, error)
	GetOutpostResolver(ctx context.Context, id string) (*OutpostResolver, error)
	ListOutpostResolvers(ctx context.Context) []*OutpostResolver
	DeleteOutpostResolver(ctx context.Context, id string) (*OutpostResolver, error)
	UpdateOutpostResolver(
		ctx context.Context,
		id, name, preferredInstanceType string,
		instanceCount int32,
	) (*OutpostResolver, error)

	// Query log config operations
	CreateResolverQueryLogConfig(
		ctx context.Context,
		name, creatorRequestID, destinationARN string,
	) (*ResolverQueryLogConfig, error)
	GetResolverQueryLogConfig(ctx context.Context, id string) (*ResolverQueryLogConfig, error)
	ListResolverQueryLogConfigs(ctx context.Context) []*ResolverQueryLogConfig
	DeleteResolverQueryLogConfig(ctx context.Context, id string) (*ResolverQueryLogConfig, error)
	AssociateResolverQueryLogConfig(
		ctx context.Context,
		queryLogConfigID, resourceID string,
	) (*ResolverQueryLogConfigAssociation, error)
	GetResolverQueryLogConfigAssociation(ctx context.Context, id string) (*ResolverQueryLogConfigAssociation, error)
	DisassociateResolverQueryLogConfig(
		ctx context.Context,
		queryLogConfigID, resourceID string,
	) (*ResolverQueryLogConfigAssociation, error)
	ListResolverQueryLogConfigAssociations(ctx context.Context) []*ResolverQueryLogConfigAssociation
	GetResolverQueryLogConfigPolicy(ctx context.Context, arn string) string
	PutResolverQueryLogConfigPolicy(ctx context.Context, arn, policy string) error

	// Resolver config operations
	GetResolverConfig(ctx context.Context, resourceID string) *ResolverConfig
	UpdateResolverConfig(ctx context.Context, resourceID, autodefinedReverse string) (*ResolverConfig, error)
	ListResolverConfigs(ctx context.Context) []*ResolverConfig

	// Resolver DNSSEC config operations
	GetResolverDnssecConfig(ctx context.Context, resourceID string) *ResolverDnssecConfig
	UpdateResolverDnssecConfig(ctx context.Context, resourceID, validation string) (*ResolverDnssecConfig, error)
	ListResolverDnssecConfigs(ctx context.Context) []*ResolverDnssecConfig

	// Tag operations
	TagResource(ctx context.Context, resourceARN string, kvs []svcTags.KV) error
	UntagResource(ctx context.Context, resourceARN string, keys []string) error
	ListTagsForResource(ctx context.Context, resourceARN string) []svcTags.KV

	// Lifecycle
	Reset()
	Region() string
	AccountID() string
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

// compile-time assertion that InMemoryBackend satisfies StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
