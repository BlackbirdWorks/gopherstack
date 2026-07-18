package ec2

import (
	"errors"
	"time"
)

// ipam_discovery.go implements the IPAM sub-families layered on top of the core
// Ipam/IpamScope/IpamPool state in advanced_networking.go: user-created (non-default)
// Resource Discoveries and their associations, Resource CIDR monitoring, BYOASN
// provisioning/association, External Resource Verification Tokens, and Prefix List Resolvers
// (with their Targets, CIDR selection Rules, and Versions).

// ---- Errors ----

var (
	// ErrIpamResourceDiscoveryAssociationNotFound is returned when a resource discovery
	// association ID does not exist.
	ErrIpamResourceDiscoveryAssociationNotFound = errors.New("InvalidIpamResourceDiscoveryAssociationId.NotFound")
	// ErrIpamResourceDiscoveryInUse is returned when deleting a resource discovery that is
	// still associated with one or more IPAMs, or is the default discovery of an IPAM.
	ErrIpamResourceDiscoveryInUse = errors.New("IncorrectState")
	// ErrIpamByoasnNotFound is returned when an ASN has not been provisioned to any IPAM.
	// BYOASNs are keyed by their ASN value rather than a generated resource ID, so AWS has no
	// dedicated "Invalid...Id.NotFound" error family for them.
	ErrIpamByoasnNotFound = errors.New("InvalidParameterValue")
	// ErrIpamAsnAssociationNotFound is returned when disassociating an ASN/CIDR pair that is
	// not currently associated.
	ErrIpamAsnAssociationNotFound = errors.New("InvalidParameterValue")
	// ErrIpamVerificationTokenNotFound is returned when a verification token ID does not exist.
	ErrIpamVerificationTokenNotFound = errors.New(
		"InvalidIpamExternalResourceVerificationTokenId.NotFound",
	)
	// ErrIpamResourceCidrNotFound is returned when a (ResourceId, ResourceCidr) pair is not
	// currently monitored by IPAM.
	ErrIpamResourceCidrNotFound = errors.New("InvalidParameterValue")
	// ErrIpamPrefixListResolverNotFound is returned when a prefix list resolver ID does not exist.
	ErrIpamPrefixListResolverNotFound = errors.New("InvalidIpamPrefixListResolverId.NotFound")
	// ErrIpamPrefixListResolverTargetNotFound is returned when a prefix list resolver target ID
	// does not exist.
	ErrIpamPrefixListResolverTargetNotFound = errors.New(
		"InvalidIpamPrefixListResolverTargetId.NotFound",
	)
	// ErrIpamPrefixListResolverVersionNotFound is returned when a requested resolver version
	// number was never created.
	ErrIpamPrefixListResolverVersionNotFound = errors.New("InvalidParameterValue")
)

// ---- Constants ----

const (
	// ipamAssocStateAssociateComplete and ipamAssocStateDisassociateComplete are the
	// steady-state IpamResourceDiscoveryAssociationState values once an association/
	// disassociation completes.
	ipamAssocStateAssociateComplete    = "associate-complete"
	ipamAssocStateDisassociateComplete = "disassociate-complete"

	// ipamByoasnStateProvisioned and ipamByoasnStateDeprovisioned are the steady-state
	// AsnState values for a BYOASN.
	ipamByoasnStateProvisioned   = "provisioned"
	ipamByoasnStateDeprovisioned = "deprovisioned"

	// ipamAsnAssocStateAssociated and ipamAsnAssocStateDisassociated are the steady-state
	// AsnAssociationState values for an ASN/CIDR association.
	ipamAsnAssocStateAssociated    = "associated"
	ipamAsnAssocStateDisassociated = "disassociated"

	// ipamTokenStatusValid is the steady-state TokenState for a freshly-created verification
	// token.
	ipamTokenStatusValid = "valid"
	// ipamVerificationTokenValidity is how long a newly-created external resource
	// verification token remains valid.
	ipamVerificationTokenValidity = 7 * 24 * time.Hour

	// ipamPrefixListResolverVersionStatusSuccess is the steady-state
	// IpamPrefixListResolverVersionCreationStatus for this mock: every version "creation"
	// completes synchronously.
	ipamPrefixListResolverVersionStatusSuccess = "success"
)

// ---- Data types ----

// IpamAsnAssociation represents the association between a BYOASN and a BYOIP CIDR, as
// returned by AssociateIpamByoasn/DisassociateIpamByoasn.
type IpamAsnAssociation struct {
	Asn           string `json:"asn,omitempty"`
	Cidr          string `json:"cidr,omitempty"`
	State         string `json:"state,omitempty"`
	StatusMessage string `json:"statusMessage,omitempty"`
}

// IpamByoasn represents a Bring-Your-Own ASN provisioned to an IPAM.
type IpamByoasn struct {
	Asn           string `json:"asn,omitempty"`
	IpamID        string `json:"ipamId,omitempty"`
	State         string `json:"state,omitempty"`
	StatusMessage string `json:"statusMessage,omitempty"`
}

// IpamExternalResourceVerificationToken represents a token used to verify ownership of a
// non-AWS ("external") resource so IPAM can monitor its CIDR.
type IpamExternalResourceVerificationToken struct {
	NotAfter                                 time.Time `json:"notAfter"`
	IpamExternalResourceVerificationTokenID  string    `json:"tokenId,omitempty"`
	IpamExternalResourceVerificationTokenARN string    `json:"tokenArn,omitempty"`
	IpamID                                   string    `json:"ipamId,omitempty"`
	IpamARN                                  string    `json:"ipamArn,omitempty"`
	IpamRegion                               string    `json:"ipamRegion,omitempty"`
	State                                    string    `json:"state,omitempty"`
	Status                                   string    `json:"status,omitempty"`
	TokenName                                string    `json:"tokenName,omitempty"`
	TokenValue                               string    `json:"tokenValue,omitempty"`
}

// IpamResourceCidr represents a resource CIDR monitored by IPAM. This mock derives resource
// CIDRs from IPAM pool allocations (see AllocateIpamPoolCidr): allocating a pool CIDR to a
// resource ID creates a corresponding monitored resource CIDR entry, and releasing the
// allocation removes it.
type IpamResourceCidr struct {
	IpamID          string  `json:"ipamId,omitempty"`
	IpamPoolID      string  `json:"ipamPoolId,omitempty"`
	IpamScopeID     string  `json:"ipamScopeId,omitempty"`
	ResourceID      string  `json:"resourceId,omitempty"`
	ResourceCidr    string  `json:"resourceCidr,omitempty"`
	ResourceRegion  string  `json:"resourceRegion,omitempty"`
	ResourceType    string  `json:"resourceType,omitempty"`
	ResourceOwnerID string  `json:"resourceOwnerId,omitempty"`
	ManagementState string  `json:"managementState,omitempty"`
	Monitored       bool    `json:"-"`
	IPUsage         float64 `json:"ipUsage,omitempty"`
}

// IpamPrefixListResolverRuleCondition is a single condition of a CIDR selection rule.
type IpamPrefixListResolverRuleCondition struct {
	Operation      string `json:"operation,omitempty"`
	Cidr           string `json:"cidr,omitempty"`
	IpamPoolID     string `json:"ipamPoolId,omitempty"`
	ResourceID     string `json:"resourceId,omitempty"`
	ResourceOwner  string `json:"resourceOwner,omitempty"`
	ResourceRegion string `json:"resourceRegion,omitempty"`
}

// IpamPrefixListResolverRule is a CIDR selection rule for a prefix list resolver.
type IpamPrefixListResolverRule struct {
	RuleType     string                                `json:"ruleType,omitempty"`
	IpamScopeID  string                                `json:"ipamScopeId,omitempty"`
	ResourceType string                                `json:"resourceType,omitempty"`
	StaticCidr   string                                `json:"staticCidr,omitempty"`
	Conditions   []IpamPrefixListResolverRuleCondition `json:"conditions,omitempty"`
}

// IpamPrefixListResolver represents an IPAM prefix list resolver: a component that
// synchronizes CIDRs selected from an IPAM's tracked address space into a managed prefix list.
type IpamPrefixListResolver struct {
	IpamPrefixListResolverID         string                       `json:"ipamPrefixListResolverId,omitempty"`
	IpamPrefixListResolverARN        string                       `json:"ipamPrefixListResolverArn,omitempty"`
	IpamID                           string                       `json:"ipamId,omitempty"`
	IpamARN                          string                       `json:"ipamArn,omitempty"`
	IpamRegion                       string                       `json:"ipamRegion,omitempty"`
	OwnerID                          string                       `json:"ownerId,omitempty"`
	AddressFamily                    string                       `json:"addressFamily,omitempty"`
	Description                      string                       `json:"description,omitempty"`
	State                            string                       `json:"state,omitempty"`
	LastVersionCreationStatus        string                       `json:"lastVersionCreationStatus,omitempty"`
	LastVersionCreationStatusMessage string                       `json:"lastVersionCreationStatusMessage,omitempty"`
	Rules                            []IpamPrefixListResolverRule `json:"rules,omitempty"`
	CurrentVersion                   int64                        `json:"-"`
}

// IpamPrefixListResolverTarget represents a managed prefix list kept in sync by an IPAM
// prefix list resolver.
type IpamPrefixListResolverTarget struct {
	DesiredVersion                  *int64 `json:"desiredVersion,omitempty"`
	LastSyncedVersion               *int64 `json:"lastSyncedVersion,omitempty"`
	IpamPrefixListResolverTargetID  string `json:"ipamPrefixListResolverTargetId,omitempty"`
	IpamPrefixListResolverTargetARN string `json:"ipamPrefixListResolverTargetArn,omitempty"`
	IpamPrefixListResolverID        string `json:"ipamPrefixListResolverId,omitempty"`
	OwnerID                         string `json:"ownerId,omitempty"`
	PrefixListID                    string `json:"prefixListId,omitempty"`
	PrefixListRegion                string `json:"prefixListRegion,omitempty"`
	State                           string `json:"state,omitempty"`
	StateMessage                    string `json:"stateMessage,omitempty"`
	TrackLatestVersion              bool   `json:"trackLatestVersion,omitempty"`
}

// ---- Reset helpers ----

// resetIpamDiscoveryMapsLocked re-initialises all maps owned by this file. Must be called
// with b.mu held.
func (b *InMemoryBackend) resetIpamDiscoveryMapsLocked() {
	b.ipamPrefixListResolverVersions = make(map[string][]int64)
}
