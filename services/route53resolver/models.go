package route53resolver

import (
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	shareStatusNotShared = "NOT_SHARED"
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
	firewallFailOpenUseLocal = "USE_LOCAL_RESOURCE_SETTING"

	autodefinedReverseEnabled  = "ENABLE"
	autodefinedReverseDisabled = "DISABLE"
	autodefinedReverseUseLocal = "USE_LOCAL_RESOURCE_SETTING"

	dnssecValidationEnable   = "ENABLE"
	dnssecValidationDisable  = "DISABLE"
	dnssecValidationUseLocal = "USE_LOCAL_RESOURCE_SETTING"

	validationStatusEnabled            = "ENABLED"
	validationStatusEnabling           = "ENABLING"
	validationStatusDisabled           = "DISABLED"
	validationStatusDisabling          = "DISABLING"
	validationStatusUseLocal           = "USE_LOCAL_RESOURCE_SETTING"
	validationStatusUpdatingToUseLocal = "UPDATING_TO_USE_LOCAL_RESOURCE_SETTING"

	mutationProtectionEnabled  = "ENABLED"
	mutationProtectionDisabled = "DISABLED"

	endpointTypeIPV4      = "IPV4"
	endpointTypeIPV6      = "IPV6"
	endpointTypeDualStack = "DUALSTACK"

	blockResponseNODATA   = "NODATA"
	blockResponseNXDOMAIN = "NXDOMAIN"
	blockResponseOVERRIDE = "OVERRIDE"

	// DnsThreatProtection values (types.DnsThreatProtection in
	// aws-sdk-go-v2/service/route53resolver@v1.48.0): the DNS Firewall
	// Advanced built-in threat detectors a rule can match on instead of a
	// domain list.
	dnsThreatProtectionDGA           = "DGA"
	dnsThreatProtectionDNSTunneling  = "DNS_TUNNELING"
	dnsThreatProtectionDictionaryDGA = "DICTIONARY_DGA"

	// FirewallDomainRedirectionAction values (types.FirewallDomainRedirectionAction):
	// how a domain-list rule evaluates a DNS redirection chain (CNAME/DNAME).
	// INSPECT_REDIRECTION_DOMAIN is the real API's documented default.
	firewallDomainRedirectionInspect = "INSPECT_REDIRECTION_DOMAIN"
	firewallDomainRedirectionTrust   = "TRUST_REDIRECTION_DOMAIN"
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
	ID                             string       `json:"id"`
	ARN                            string       `json:"arn"`
	Direction                      string       `json:"direction"`
	Name                           string       `json:"name"`
	Status                         string       `json:"status"`
	StatusMessage                  string       `json:"statusMessage,omitempty"`
	VpcID                          string       `json:"vpcID"`
	HostVPCID                      string       `json:"hostVpcId"`
	AccountID                      string       `json:"accountID"`
	Region                         string       `json:"region"`
	ResolverEndpointType           string       `json:"resolverEndpointType"`
	OutpostArn                     string       `json:"outpostArn,omitempty"`
	PreferredInstanceType          string       `json:"preferredInstanceType,omitempty"`
	CreatorRequestID               string       `json:"creatorRequestId,omitempty"`
	CreationTime                   string       `json:"creationTime,omitempty"`
	ModificationTime               string       `json:"modificationTime,omitempty"`
	SecurityGroupIDs               []string     `json:"securityGroupIds"`
	IPAddresses                    []IPAddress  `json:"ipAddresses"`
	Tags                           []svcTags.KV `json:"tags,omitempty"`
	Protocols                      []string     `json:"protocols,omitempty"`
	RniEnhancedMetricsEnabled      bool         `json:"rniEnhancedMetricsEnabled"`
	TargetNameServerMetricsEnabled bool         `json:"targetNameServerMetricsEnabled"`
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
	ID               string `json:"id"`
	ARN              string `json:"arn"`
	Name             string `json:"name"`
	CreatorRequestID string `json:"creatorRequestId"`
	Status           string `json:"status"`
	StatusMessage    string `json:"statusMessage,omitempty"`
	OwnerID          string `json:"ownerId"`
	ShareStatus      string `json:"shareStatus"`
	CreationTime     string `json:"creationTime,omitempty"`
	ModificationTime string `json:"modificationTime,omitempty"`
	// Region is the AWS region this rule group belongs to. It is not part of
	// the wire response (handler.go builds a separate response type) -- it
	// exists purely so store.Table's key function (see store_setup.go) can
	// derive the region-scoped composite key ("<region>|<id>") that replaces
	// the old map[region]map[id]*FirewallRuleGroup nesting.
	Region    string       `json:"region"`
	Tags      []svcTags.KV `json:"tags,omitempty"`
	RuleCount int32        `json:"ruleCount"`
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
	// Region -- see FirewallRuleGroup.Region doc comment.
	Region   string `json:"region"`
	Priority int32  `json:"priority"`
}

// FirewallDomainList represents a DNS Firewall domain list.
type FirewallDomainList struct {
	ID               string `json:"id"`
	ARN              string `json:"arn"`
	Name             string `json:"name"`
	CreatorRequestID string `json:"creatorRequestId"`
	Status           string `json:"status"`
	StatusMessage    string `json:"statusMessage,omitempty"`
	ManagedOwnerName string `json:"managedOwnerName,omitempty"`
	CreationTime     string `json:"creationTime,omitempty"`
	ModificationTime string `json:"modificationTime,omitempty"`
	// Region -- see FirewallRuleGroup.Region doc comment.
	Region      string       `json:"region"`
	Tags        []svcTags.KV `json:"tags,omitempty"`
	Domains     []string     `json:"domains,omitempty"`
	DomainCount int32        `json:"domainCount"`
}

// FirewallRule represents a single rule within a DNS Firewall rule group.
//
// DnsThreatProtection/FirewallThreatProtectionID/FirewallDomainRedirectionAction
// back the DNS Firewall Advanced match source (verified against
// types.FirewallRule/CreateFirewallRuleInput/UpdateFirewallRuleInput/
// DeleteFirewallRuleInput in aws-sdk-go-v2/service/route53resolver@v1.48.0).
// A rule matches EITHER a domain list (FirewallDomainListID, the original
// path) OR a DnsThreatProtection detector (DGA/DNS_TUNNELING/DICTIONARY_DGA)
// -- the two are mutually exclusive, matching the real API's documented
// match sources. A DnsThreatProtection rule has no domain list, so it's
// identified on the wire by the system-generated FirewallThreatProtectionID
// instead. The other two FirewallRuleType tagged-union variants
// (FirewallAdvancedContentCategory/FirewallAdvancedThreatCategory) and
// PartnerThreatProtection are intentionally NOT modeled here -- see
// PARITY.md; they are AWS-managed catalogs with no closed SDK enum to
// source concrete values from, so implementing them would mean inventing
// category/partner identifiers.
type FirewallRule struct {
	ID                              string `json:"id"`
	ARN                             string `json:"arn"`
	Name                            string `json:"name"`
	FirewallRuleGroupID             string `json:"firewallRuleGroupId"`
	FirewallDomainListID            string `json:"firewallDomainListId"`
	Action                          string `json:"action"`
	BlockResponse                   string `json:"blockResponse,omitempty"`
	BlockOverrideDomain             string `json:"blockOverrideDomain,omitempty"`
	BlockOverrideDNSType            string `json:"blockOverrideDnsType,omitempty"`
	Qtype                           string `json:"qtype,omitempty"`
	ConfidenceThreshold             string `json:"confidenceThreshold,omitempty"`
	CreatorRequestID                string `json:"creatorRequestId,omitempty"`
	CreationTime                    string `json:"creationTime,omitempty"`
	ModificationTime                string `json:"modificationTime,omitempty"`
	DNSThreatProtection             string `json:"dnsThreatProtection,omitempty"`
	FirewallThreatProtectionID      string `json:"firewallThreatProtectionId,omitempty"`
	FirewallDomainRedirectionAction string `json:"firewallDomainRedirectionAction,omitempty"`
	// Region -- see FirewallRuleGroup.Region doc comment.
	Region           string `json:"region"`
	BlockOverrideTTL int32  `json:"blockOverrideTtl,omitempty"`
	Priority         int32  `json:"priority"`
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
	// Region -- see FirewallRuleGroup.Region doc comment.
	Region        string       `json:"region"`
	Tags          []svcTags.KV `json:"tags,omitempty"`
	InstanceCount int32        `json:"instanceCount"`
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
	ShareStatus      string `json:"shareStatus"`
	CreationTime     string `json:"creationTime,omitempty"`
	// Region -- see FirewallRuleGroup.Region doc comment.
	Region           string       `json:"region"`
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
	// Region -- see FirewallRuleGroup.Region doc comment.
	Region string `json:"region"`
}

// ResolverRuleAssociation represents an association between a Resolver rule and a VPC.
type ResolverRuleAssociation struct {
	ID             string `json:"id"`
	Name           string `json:"name"`
	ResolverRuleID string `json:"resolverRuleId"`
	VPCID          string `json:"vpcId"`
	Status         string `json:"status"`
	// Region -- see FirewallRuleGroup.Region doc comment.
	Region string `json:"region"`
}

// FirewallConfig represents the DNS Firewall configuration for a VPC.
type FirewallConfig struct {
	ID               string `json:"id"`
	OwnerID          string `json:"ownerId"`
	ResourceID       string `json:"resourceId"`
	FirewallFailOpen string `json:"firewallFailOpen"`
	// Region -- see FirewallRuleGroup.Region doc comment. FirewallConfig is
	// keyed by ResourceID (not ID) in the firewallConfigs table.
	Region string `json:"region"`
}

// ResolverConfig represents the Resolver configuration for a VPC.
type ResolverConfig struct {
	ID                 string `json:"id"`
	ARN                string `json:"arn"`
	OwnerID            string `json:"ownerId"`
	ResourceID         string `json:"resourceId"`
	AutodefinedReverse string `json:"autodefinedReverse"`
	// Region -- see FirewallConfig.Region doc comment; also keyed by ResourceID.
	Region string `json:"region"`
}

// ResolverDnssecConfig represents the DNSSEC configuration for a VPC.
type ResolverDnssecConfig struct {
	ID               string `json:"id"`
	OwnerID          string `json:"ownerId"`
	ResourceID       string `json:"resourceId"`
	ValidationStatus string `json:"validationStatus"`
	// Region -- see FirewallConfig.Region doc comment; also keyed by ResourceID.
	Region string `json:"region"`
}
