package route53resolver

import svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"

// StorageBackend defines the interface for Route 53 Resolver backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	// Endpoint operations
	CreateResolverEndpoint(name, direction, vpcID string, ips []IPAddress) (*ResolverEndpoint, error)
	GetResolverEndpoint(id string) (*ResolverEndpoint, error)
	ListResolverEndpoints() []*ResolverEndpoint
	DeleteResolverEndpoint(id string) error
	ListResolverEndpointIPAddresses(endpointID string) ([]IPAddress, error)
	AssociateResolverEndpointIPAddress(endpointID, subnetID, ip string) (*ResolverEndpoint, error)

	// Rule operations
	CreateResolverRule(name, domainName, ruleType, endpointID string) (*ResolverRule, error)
	GetResolverRule(id string) (*ResolverRule, error)
	ListResolverRules() []*ResolverRule
	DeleteResolverRule(id string) error
	AssociateResolverRule(resolverRuleID, vpcID, name string) (*ResolverRuleAssociation, error)

	// Firewall rule group operations
	CreateFirewallRuleGroup(name, creatorRequestID string) (*FirewallRuleGroup, error)
	AssociateFirewallRuleGroup(
		firewallRuleGroupID, vpcID, name, creatorRequestID string,
		priority int32,
	) (*FirewallRuleGroupAssociation, error)

	// Firewall domain list operations
	CreateFirewallDomainList(name, creatorRequestID string) (*FirewallDomainList, error)
	DeleteFirewallDomainList(id string) (*FirewallDomainList, error)

	// Firewall rule operations
	CreateFirewallRule(
		firewallRuleGroupID, name, action, creatorRequestID string,
		priority int32,
		firewallDomainListID string,
	) (*FirewallRule, error)

	// Outpost resolver operations
	CreateOutpostResolver(
		name, creatorRequestID, outpostARN, preferredInstanceType string,
		instanceCount int32,
	) (*OutpostResolver, error)

	// Query log config operations
	CreateResolverQueryLogConfig(name, creatorRequestID, destinationARN string) (*ResolverQueryLogConfig, error)
	AssociateResolverQueryLogConfig(queryLogConfigID, resourceID string) (*ResolverQueryLogConfigAssociation, error)

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
