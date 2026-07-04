package ec2

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"
)

// backend_ipam_discovery.go implements the IPAM sub-families layered on top of the core
// Ipam/IpamScope/IpamPool state in backend_advanced_networking.go: user-created (non-default)
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
	b.ipamByoasns = make(map[string]*IpamByoasn)
	b.ipamAsnAssociations = make(map[string]*IpamAsnAssociation)
	b.ipamVerificationTokens = make(map[string]*IpamExternalResourceVerificationToken)
	b.ipamResourceCidrs = make(map[string]*IpamResourceCidr)
	b.ipamPrefixListResolvers = make(map[string]*IpamPrefixListResolver)
	b.ipamPrefixListResolverVersions = make(map[string][]int64)
	b.ipamPrefixListResolverTargets = make(map[string]*IpamPrefixListResolverTarget)
}

// ---- IPAM Resource Discoveries (user-created) ----

// CreateIpamResourceDiscovery creates a standalone (non-default) IPAM resource discovery that
// can later be associated with one or more IPAMs via AssociateIpamResourceDiscovery.
func (b *InMemoryBackend) CreateIpamResourceDiscovery(
	description string, operatingRegions []string,
) (*IpamResourceDiscovery, error) {
	b.mu.Lock("CreateIpamResourceDiscovery")
	defer b.mu.Unlock()

	id := "ipam-res-disco-" + uuid.New().String()[:8]
	d := &IpamResourceDiscovery{
		IpamResourceDiscoveryID: id,
		IpamResourceDiscoveryARN: "arn:aws:ec2:" + b.Region + ":" + b.AccountID +
			":ipam-resource-discovery/" + id,
		OwnerID:          b.AccountID,
		Region:           b.Region,
		State:            ipamStateCreateComplete,
		Description:      description,
		OperatingRegions: append([]string(nil), operatingRegions...),
	}
	b.ipamResourceDiscoveries[id] = d

	cp := *d
	cp.OperatingRegions = append([]string(nil), d.OperatingRegions...)

	return &cp, nil
}

// DeleteIpamResourceDiscovery removes a non-default IPAM resource discovery. Default resource
// discoveries (auto-created by CreateIpam) and discoveries still associated with an IPAM
// cannot be deleted.
func (b *InMemoryBackend) DeleteIpamResourceDiscovery(id string) (*IpamResourceDiscovery, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: IpamResourceDiscoveryId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteIpamResourceDiscovery")
	defer b.mu.Unlock()

	d, ok := b.ipamResourceDiscoveries[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamResourceDiscoveryNotFound, id)
	}

	if d.IsDefault {
		return nil, fmt.Errorf(
			"%w: the default resource discovery of an IPAM cannot be deleted", ErrIpamResourceDiscoveryInUse,
		)
	}

	for _, a := range b.ipamResourceDiscoveryAssocs {
		if a.IpamResourceDiscoveryID == id {
			return nil, fmt.Errorf(
				"%w: resource discovery %s has existing IPAM associations", ErrIpamResourceDiscoveryInUse, id,
			)
		}
	}

	delete(b.ipamResourceDiscoveries, id)

	cp := *d
	cp.OperatingRegions = append([]string(nil), d.OperatingRegions...)
	cp.State = ipamStateDeleteComplete

	return &cp, nil
}

// AssociateIpamResourceDiscovery associates a standalone resource discovery with an IPAM.
func (b *InMemoryBackend) AssociateIpamResourceDiscovery(
	ipamID, discoveryID string,
) (*IpamResourceDiscoveryAssociation, error) {
	if ipamID == "" || discoveryID == "" {
		return nil, fmt.Errorf("%w: IpamId and IpamResourceDiscoveryId are required", ErrInvalidParameter)
	}

	b.mu.Lock("AssociateIpamResourceDiscovery")
	defer b.mu.Unlock()

	ipam, ok := b.ipams[ipamID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamNotFound, ipamID)
	}

	if _, discoveryOK := b.ipamResourceDiscoveries[discoveryID]; !discoveryOK {
		return nil, fmt.Errorf("%w: %s", ErrIpamResourceDiscoveryNotFound, discoveryID)
	}

	assocID := "ipam-res-disco-assoc-" + uuid.New().String()[:8]
	assoc := &IpamResourceDiscoveryAssociation{
		IpamResourceDiscoveryAssociationID: assocID,
		IpamID:                             ipamID,
		IpamARN:                            ipam.IpamARN,
		IpamRegion:                         b.Region,
		IpamResourceDiscoveryID:            discoveryID,
		OwnerID:                            b.AccountID,
		IsDefault:                          false,
		ResourceDiscoveryStatus:            ipamResourceDiscoveryAssocStatus,
		State:                              ipamAssocStateAssociateComplete,
	}
	assoc.IpamResourceDiscoveryAssociationARN = "arn:aws:ec2:" + b.Region + ":" + b.AccountID +
		":ipam-resource-discovery-association/" + assocID
	b.ipamResourceDiscoveryAssocs[assocID] = assoc
	ipam.ResourceDiscoveryAssociationCount++

	cp := *assoc

	return &cp, nil
}

// DisassociateIpamResourceDiscovery removes a (non-default) resource discovery association
// from an IPAM.
func (b *InMemoryBackend) DisassociateIpamResourceDiscovery(
	assocID string,
) (*IpamResourceDiscoveryAssociation, error) {
	if assocID == "" {
		return nil, fmt.Errorf("%w: IpamResourceDiscoveryAssociationId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DisassociateIpamResourceDiscovery")
	defer b.mu.Unlock()

	assoc, ok := b.ipamResourceDiscoveryAssocs[assocID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamResourceDiscoveryAssociationNotFound, assocID)
	}

	if assoc.IsDefault {
		return nil, fmt.Errorf(
			"%w: the default resource discovery association of an IPAM cannot be disassociated",
			ErrIpamResourceDiscoveryInUse,
		)
	}

	if ipam, ipamOK := b.ipams[assoc.IpamID]; ipamOK && ipam.ResourceDiscoveryAssociationCount > 0 {
		ipam.ResourceDiscoveryAssociationCount--
	}

	delete(b.ipamResourceDiscoveryAssocs, assocID)

	cp := *assoc
	cp.State = ipamAssocStateDisassociateComplete

	return &cp, nil
}

// ModifyIpamResourceDiscovery updates a resource discovery's description and operating regions.
func (b *InMemoryBackend) ModifyIpamResourceDiscovery(
	id, description string, addOperatingRegions, removeOperatingRegions []string,
) (*IpamResourceDiscovery, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: IpamResourceDiscoveryId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyIpamResourceDiscovery")
	defer b.mu.Unlock()

	d, ok := b.ipamResourceDiscoveries[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamResourceDiscoveryNotFound, id)
	}

	if description != "" {
		d.Description = description
	}

	d.OperatingRegions = applyOperatingRegionDelta(d.OperatingRegions, addOperatingRegions, removeOperatingRegions)
	d.State = ipamStateModifyComplete

	cp := *d
	cp.OperatingRegions = append([]string(nil), d.OperatingRegions...)

	return &cp, nil
}

// applyOperatingRegionDelta returns current with removeRegions removed and addRegions added
// (deduplicated), mirroring the Add.../Remove... semantics of ModifyIpamResourceDiscovery.
func applyOperatingRegionDelta(current, addRegions, removeRegions []string) []string {
	remove := make(map[string]bool, len(removeRegions))
	for _, r := range removeRegions {
		remove[r] = true
	}

	kept := make(map[string]bool, len(current))
	out := make([]string, 0, len(current)+len(addRegions))

	for _, r := range current {
		if remove[r] || kept[r] {
			continue
		}

		kept[r] = true
		out = append(out, r)
	}

	for _, r := range addRegions {
		if kept[r] {
			continue
		}

		kept[r] = true
		out = append(out, r)
	}

	return out
}

// ---- IPAM Resource CIDRs ----

// ipamResourceCidrKey builds the internal map key for a monitored resource CIDR.
func ipamResourceCidrKey(resourceID, resourceCidr string) string {
	return resourceID + "|" + resourceCidr
}

// recordIpamResourceCidrLocked creates or refreshes the monitored resource CIDR entry backing
// a pool allocation. Must be called with b.mu held.
func (b *InMemoryBackend) recordIpamResourceCidrLocked(pool *IpamPool, alloc *IpamPoolAllocation) {
	if alloc.ResourceID == "" {
		return
	}

	key := ipamResourceCidrKey(alloc.ResourceID, alloc.Cidr)
	b.ipamResourceCidrs[key] = &IpamResourceCidr{
		IpamID:          pool.IpamID,
		IpamPoolID:      pool.IpamPoolID,
		IpamScopeID:     pool.IpamScopeID,
		ResourceID:      alloc.ResourceID,
		ResourceCidr:    alloc.Cidr,
		ResourceRegion:  b.Region,
		ResourceType:    alloc.ResourceType,
		ResourceOwnerID: alloc.ResourceOwner,
		ManagementState: "managed",
		Monitored:       true,
	}
}

// forgetIpamResourceCidrLocked removes the monitored resource CIDR entry backing a released
// pool allocation. Must be called with b.mu held.
func (b *InMemoryBackend) forgetIpamResourceCidrLocked(alloc *IpamPoolAllocation) {
	delete(b.ipamResourceCidrs, ipamResourceCidrKey(alloc.ResourceID, alloc.Cidr))
}

// GetIpamResourceCidrs returns resource CIDRs monitored by IPAM in the given scope, optionally
// filtered by pool, resource ID, resource owner, or resource type.
func (b *InMemoryBackend) GetIpamResourceCidrs(
	scopeID, poolID, resourceID, resourceOwner, resourceType string,
) ([]*IpamResourceCidr, error) {
	if scopeID == "" {
		return nil, fmt.Errorf("%w: IpamScopeId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetIpamResourceCidrs")
	defer b.mu.RUnlock()

	if _, ok := b.ipamScopes[scopeID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamScopeNotFound, scopeID)
	}

	out := make([]*IpamResourceCidr, 0, len(b.ipamResourceCidrs))

	for _, c := range b.ipamResourceCidrs {
		if c.IpamScopeID != scopeID {
			continue
		}

		if poolID != "" && c.IpamPoolID != poolID {
			continue
		}

		if resourceID != "" && c.ResourceID != resourceID {
			continue
		}

		if resourceOwner != "" && c.ResourceOwnerID != resourceOwner {
			continue
		}

		if resourceType != "" && c.ResourceType != resourceType {
			continue
		}

		cp := *c
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].ResourceID < out[j].ResourceID
	})

	return out, nil
}

// ModifyIpamResourceCidr moves a monitored resource CIDR between scopes and/or updates its
// monitored flag.
func (b *InMemoryBackend) ModifyIpamResourceCidr(
	currentScopeID, resourceCidr, resourceID, resourceRegion string, monitored bool, destScopeID string,
) (*IpamResourceCidr, error) {
	if currentScopeID == "" || resourceCidr == "" || resourceID == "" {
		return nil, fmt.Errorf(
			"%w: CurrentIpamScopeId, ResourceCidr, and ResourceId are required", ErrInvalidParameter,
		)
	}

	b.mu.Lock("ModifyIpamResourceCidr")
	defer b.mu.Unlock()

	key := ipamResourceCidrKey(resourceID, resourceCidr)

	c, ok := b.ipamResourceCidrs[key]
	if !ok {
		return nil, fmt.Errorf("%w: %s %s", ErrIpamResourceCidrNotFound, resourceID, resourceCidr)
	}

	if c.IpamScopeID != currentScopeID {
		return nil, fmt.Errorf(
			"%w: %s is not currently in scope %s", ErrIpamResourceCidrNotFound, resourceID, currentScopeID,
		)
	}

	if resourceRegion != "" {
		c.ResourceRegion = resourceRegion
	}

	c.Monitored = monitored
	if !monitored {
		c.ManagementState = "unmanaged"
	} else {
		c.ManagementState = "managed"
	}

	if destScopeID != "" {
		c.IpamScopeID = destScopeID
	}

	cp := *c

	return &cp, nil
}

// ---- BYOASN ----

// ProvisionIpamByoasn provisions a public ASN for use with an IPAM's BYOIP CIDRs.
func (b *InMemoryBackend) ProvisionIpamByoasn(ipamID, asn string) (*IpamByoasn, error) {
	if ipamID == "" || asn == "" {
		return nil, fmt.Errorf("%w: IpamId and Asn are required", ErrInvalidParameter)
	}

	b.mu.Lock("ProvisionIpamByoasn")
	defer b.mu.Unlock()

	if _, ok := b.ipams[ipamID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamNotFound, ipamID)
	}

	byoasn := &IpamByoasn{
		Asn:    asn,
		IpamID: ipamID,
		State:  ipamByoasnStateProvisioned,
	}
	b.ipamByoasns[asn] = byoasn

	cp := *byoasn

	return &cp, nil
}

// DeprovisionIpamByoasn releases a previously-provisioned BYOASN.
func (b *InMemoryBackend) DeprovisionIpamByoasn(ipamID, asn string) (*IpamByoasn, error) {
	if ipamID == "" || asn == "" {
		return nil, fmt.Errorf("%w: IpamId and Asn are required", ErrInvalidParameter)
	}

	b.mu.Lock("DeprovisionIpamByoasn")
	defer b.mu.Unlock()

	byoasn, ok := b.ipamByoasns[asn]
	if !ok || byoasn.IpamID != ipamID {
		return nil, fmt.Errorf("%w: %s", ErrIpamByoasnNotFound, asn)
	}

	delete(b.ipamByoasns, asn)

	cp := *byoasn
	cp.State = ipamByoasnStateDeprovisioned

	return &cp, nil
}

// DescribeIpamByoasn returns all BYOASNs provisioned across all IPAMs.
func (b *InMemoryBackend) DescribeIpamByoasn() []*IpamByoasn {
	b.mu.RLock("DescribeIpamByoasn")
	defer b.mu.RUnlock()

	out := make([]*IpamByoasn, 0, len(b.ipamByoasns))
	for _, a := range b.ipamByoasns {
		cp := *a
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Asn < out[j].Asn })

	return out
}

// AssociateIpamByoasn associates a provisioned BYOASN with a BYOIP CIDR.
func (b *InMemoryBackend) AssociateIpamByoasn(asn, cidr string) (*IpamAsnAssociation, error) {
	if asn == "" || cidr == "" {
		return nil, fmt.Errorf("%w: Asn and Cidr are required", ErrInvalidParameter)
	}

	b.mu.Lock("AssociateIpamByoasn")
	defer b.mu.Unlock()

	if _, ok := b.ipamByoasns[asn]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamByoasnNotFound, asn)
	}

	key := asn + "|" + cidr
	assoc := &IpamAsnAssociation{Asn: asn, Cidr: cidr, State: ipamAsnAssocStateAssociated}
	b.ipamAsnAssociations[key] = assoc

	cp := *assoc

	return &cp, nil
}

// DisassociateIpamByoasn removes the association between a BYOASN and a BYOIP CIDR.
func (b *InMemoryBackend) DisassociateIpamByoasn(asn, cidr string) (*IpamAsnAssociation, error) {
	if asn == "" || cidr == "" {
		return nil, fmt.Errorf("%w: Asn and Cidr are required", ErrInvalidParameter)
	}

	b.mu.Lock("DisassociateIpamByoasn")
	defer b.mu.Unlock()

	key := asn + "|" + cidr

	assoc, ok := b.ipamAsnAssociations[key]
	if !ok {
		return nil, fmt.Errorf("%w: %s / %s", ErrIpamAsnAssociationNotFound, asn, cidr)
	}

	delete(b.ipamAsnAssociations, key)

	cp := *assoc
	cp.State = ipamAsnAssocStateDisassociated

	return &cp, nil
}

// ---- External Resource Verification Tokens ----

// CreateIpamExternalResourceVerificationToken creates a token an external (non-AWS) resource
// owner can use to prove ownership of a resource so IPAM can monitor its CIDR.
func (b *InMemoryBackend) CreateIpamExternalResourceVerificationToken(
	ipamID string,
) (*IpamExternalResourceVerificationToken, error) {
	if ipamID == "" {
		return nil, fmt.Errorf("%w: IpamId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateIpamExternalResourceVerificationToken")
	defer b.mu.Unlock()

	ipam, ok := b.ipams[ipamID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamNotFound, ipamID)
	}

	id := "ipam-ext-res-verification-token-" + uuid.New().String()[:8]
	now := time.Now().UTC()
	token := &IpamExternalResourceVerificationToken{
		IpamExternalResourceVerificationTokenID: id,
		IpamExternalResourceVerificationTokenARN: "arn:aws:ec2:" + b.Region + ":" + b.AccountID +
			":ipam-external-resource-verification-token/" + id,
		IpamID:     ipamID,
		IpamARN:    ipam.IpamARN,
		IpamRegion: b.Region,
		State:      ipamStateCreateComplete,
		Status:     ipamTokenStatusValid,
		TokenName:  "ipam-verify-" + uuid.New().String()[:12],
		TokenValue: uuid.New().String(),
		NotAfter:   now.Add(ipamVerificationTokenValidity),
	}
	b.ipamVerificationTokens[id] = token

	cp := *token

	return &cp, nil
}

// DeleteIpamExternalResourceVerificationToken removes a verification token.
func (b *InMemoryBackend) DeleteIpamExternalResourceVerificationToken(
	id string,
) (*IpamExternalResourceVerificationToken, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: IpamExternalResourceVerificationTokenId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteIpamExternalResourceVerificationToken")
	defer b.mu.Unlock()

	token, ok := b.ipamVerificationTokens[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamVerificationTokenNotFound, id)
	}

	delete(b.ipamVerificationTokens, id)

	cp := *token
	cp.State = ipamStateDeleteComplete

	return &cp, nil
}

// DescribeIpamExternalResourceVerificationTokens returns verification tokens, optionally
// filtered by IDs.
func (b *InMemoryBackend) DescribeIpamExternalResourceVerificationTokens(
	ids []string,
) []*IpamExternalResourceVerificationToken {
	b.mu.RLock("DescribeIpamExternalResourceVerificationTokens")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*IpamExternalResourceVerificationToken, 0, len(b.ipamVerificationTokens))

	for _, t := range b.ipamVerificationTokens {
		if len(idSet) > 0 && !idSet[t.IpamExternalResourceVerificationTokenID] {
			continue
		}

		cp := *t
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].IpamExternalResourceVerificationTokenID < out[j].IpamExternalResourceVerificationTokenID
	})

	return out
}

// ---- Prefix List Resolvers ----

// CreateIpamPrefixListResolver creates a new IPAM prefix list resolver: a component that
// selects CIDRs from an IPAM's tracked address space according to a set of rules, producing
// numbered "versions" that a Target can synchronize into a managed prefix list.
func (b *InMemoryBackend) CreateIpamPrefixListResolver(
	ipamID, addressFamily, description string, rules []IpamPrefixListResolverRule,
) (*IpamPrefixListResolver, error) {
	if ipamID == "" {
		return nil, fmt.Errorf("%w: IpamId is required", ErrInvalidParameter)
	}

	if addressFamily == "" {
		addressFamily = "ipv4"
	}

	b.mu.Lock("CreateIpamPrefixListResolver")
	defer b.mu.Unlock()

	ipam, ok := b.ipams[ipamID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamNotFound, ipamID)
	}

	id := "ipam-prefix-list-resolver-" + uuid.New().String()[:8]
	resolver := &IpamPrefixListResolver{
		IpamPrefixListResolverID: id,
		IpamPrefixListResolverARN: "arn:aws:ec2:" + b.Region + ":" + b.AccountID +
			":ipam-prefix-list-resolver/" + id,
		IpamID:                    ipamID,
		IpamARN:                   ipam.IpamARN,
		IpamRegion:                b.Region,
		OwnerID:                   b.AccountID,
		AddressFamily:             addressFamily,
		Description:               description,
		State:                     ipamStateCreateComplete,
		LastVersionCreationStatus: ipamPrefixListResolverVersionStatusSuccess,
		Rules:                     append([]IpamPrefixListResolverRule(nil), rules...),
		CurrentVersion:            1,
	}
	b.ipamPrefixListResolvers[id] = resolver
	b.ipamPrefixListResolverVersions[id] = []int64{1}

	return copyIpamPrefixListResolver(resolver), nil
}

// DeleteIpamPrefixListResolver removes a prefix list resolver and its version history.
func (b *InMemoryBackend) DeleteIpamPrefixListResolver(id string) (*IpamPrefixListResolver, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: IpamPrefixListResolverId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteIpamPrefixListResolver")
	defer b.mu.Unlock()

	resolver, ok := b.ipamPrefixListResolvers[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamPrefixListResolverNotFound, id)
	}

	delete(b.ipamPrefixListResolvers, id)
	delete(b.ipamPrefixListResolverVersions, id)

	for targetID, t := range b.ipamPrefixListResolverTargets {
		if t.IpamPrefixListResolverID == id {
			delete(b.ipamPrefixListResolverTargets, targetID)
		}
	}

	cp := copyIpamPrefixListResolver(resolver)
	cp.State = ipamStateDeleteComplete

	return cp, nil
}

// DescribeIpamPrefixListResolvers returns prefix list resolvers, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeIpamPrefixListResolvers(ids []string) []*IpamPrefixListResolver {
	b.mu.RLock("DescribeIpamPrefixListResolvers")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*IpamPrefixListResolver, 0, len(b.ipamPrefixListResolvers))

	for _, r := range b.ipamPrefixListResolvers {
		if len(idSet) > 0 && !idSet[r.IpamPrefixListResolverID] {
			continue
		}

		out = append(out, copyIpamPrefixListResolver(r))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].IpamPrefixListResolverID < out[j].IpamPrefixListResolverID
	})

	return out
}

// ModifyIpamPrefixListResolver updates a resolver's description and, if rulesProvided is true,
// replaces its CIDR selection rules entirely (creating a new version).
func (b *InMemoryBackend) ModifyIpamPrefixListResolver(
	id, description string, rules []IpamPrefixListResolverRule, rulesProvided bool,
) (*IpamPrefixListResolver, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: IpamPrefixListResolverId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyIpamPrefixListResolver")
	defer b.mu.Unlock()

	resolver, ok := b.ipamPrefixListResolvers[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamPrefixListResolverNotFound, id)
	}

	if description != "" {
		resolver.Description = description
	}

	if rulesProvided {
		resolver.Rules = append([]IpamPrefixListResolverRule(nil), rules...)
		resolver.CurrentVersion++
		b.ipamPrefixListResolverVersions[id] = append(b.ipamPrefixListResolverVersions[id], resolver.CurrentVersion)
	}

	resolver.State = ipamStateModifyComplete
	resolver.LastVersionCreationStatus = ipamPrefixListResolverVersionStatusSuccess

	return copyIpamPrefixListResolver(resolver), nil
}

// copyIpamPrefixListResolver returns a deep copy of a resolver so callers cannot mutate
// backend state (in particular its Rules slice) through the returned pointer.
func copyIpamPrefixListResolver(r *IpamPrefixListResolver) *IpamPrefixListResolver {
	cp := *r
	cp.Rules = append([]IpamPrefixListResolverRule(nil), r.Rules...)

	for i, rule := range cp.Rules {
		rule.Conditions = append([]IpamPrefixListResolverRuleCondition(nil), rule.Conditions...)
		cp.Rules[i] = rule
	}

	return &cp
}

// GetIpamPrefixListResolverRules returns the current CIDR selection rules of a resolver.
func (b *InMemoryBackend) GetIpamPrefixListResolverRules(resolverID string) ([]IpamPrefixListResolverRule, error) {
	if resolverID == "" {
		return nil, fmt.Errorf("%w: IpamPrefixListResolverId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetIpamPrefixListResolverRules")
	defer b.mu.RUnlock()

	resolver, ok := b.ipamPrefixListResolvers[resolverID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamPrefixListResolverNotFound, resolverID)
	}

	return copyIpamPrefixListResolver(resolver).Rules, nil
}

// GetIpamPrefixListResolverVersions returns the version numbers recorded for a resolver: one
// at creation, plus one for every Modify call that replaced its rules.
func (b *InMemoryBackend) GetIpamPrefixListResolverVersions(resolverID string) ([]int64, error) {
	if resolverID == "" {
		return nil, fmt.Errorf("%w: IpamPrefixListResolverId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetIpamPrefixListResolverVersions")
	defer b.mu.RUnlock()

	if _, ok := b.ipamPrefixListResolvers[resolverID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamPrefixListResolverNotFound, resolverID)
	}

	versions := b.ipamPrefixListResolverVersions[resolverID]
	out := append([]int64(nil), versions...)
	slices.Sort(out)

	return out, nil
}

// GetIpamPrefixListResolverVersionEntries returns the CIDR entries selected by a resolver at a
// given version. This mock does not implement the live CIDR selection pipeline (matching
// resources/rules against real infrastructure), so it always returns an empty (but correctly
// validated and shaped) entry list for any version that was actually created.
func (b *InMemoryBackend) GetIpamPrefixListResolverVersionEntries(resolverID string, version int64) ([]string, error) {
	if resolverID == "" {
		return nil, fmt.Errorf("%w: IpamPrefixListResolverId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetIpamPrefixListResolverVersionEntries")
	defer b.mu.RUnlock()

	if _, ok := b.ipamPrefixListResolvers[resolverID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamPrefixListResolverNotFound, resolverID)
	}

	if slices.Contains(b.ipamPrefixListResolverVersions[resolverID], version) {
		return []string{}, nil
	}

	return nil, fmt.Errorf(
		"%w: version %d of %s does not exist", ErrIpamPrefixListResolverVersionNotFound, version, resolverID,
	)
}

// ---- Prefix List Resolver Targets ----

// CreateIpamPrefixListResolverTarget associates a managed prefix list with a resolver, making
// it an "IPAM managed prefix list" whose CIDRs are kept in sync with the resolver's rules.
func (b *InMemoryBackend) CreateIpamPrefixListResolverTarget(
	resolverID, prefixListID, prefixListRegion string, trackLatestVersion bool, desiredVersion *int64,
) (*IpamPrefixListResolverTarget, error) {
	if resolverID == "" || prefixListID == "" {
		return nil, fmt.Errorf("%w: IpamPrefixListResolverId and PrefixListId are required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateIpamPrefixListResolverTarget")
	defer b.mu.Unlock()

	resolver, ok := b.ipamPrefixListResolvers[resolverID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamPrefixListResolverNotFound, resolverID)
	}

	if prefixListRegion == "" {
		prefixListRegion = b.Region
	}

	id := "ipam-prefix-list-resolver-target-" + uuid.New().String()[:8]
	target := &IpamPrefixListResolverTarget{
		IpamPrefixListResolverTargetID: id,
		IpamPrefixListResolverTargetARN: "arn:aws:ec2:" + b.Region + ":" + b.AccountID +
			":ipam-prefix-list-resolver-target/" + id,
		IpamPrefixListResolverID: resolverID,
		OwnerID:                  b.AccountID,
		PrefixListID:             prefixListID,
		PrefixListRegion:         prefixListRegion,
		TrackLatestVersion:       trackLatestVersion,
		DesiredVersion:           desiredVersion,
		State:                    ipamStateCreateComplete,
	}

	synced := resolver.CurrentVersion
	target.LastSyncedVersion = &synced

	b.ipamPrefixListResolverTargets[id] = target

	return copyIpamPrefixListResolverTarget(target), nil
}

// DeleteIpamPrefixListResolverTarget removes a resolver target.
func (b *InMemoryBackend) DeleteIpamPrefixListResolverTarget(id string) (*IpamPrefixListResolverTarget, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: IpamPrefixListResolverTargetId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteIpamPrefixListResolverTarget")
	defer b.mu.Unlock()

	target, ok := b.ipamPrefixListResolverTargets[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamPrefixListResolverTargetNotFound, id)
	}

	delete(b.ipamPrefixListResolverTargets, id)

	cp := copyIpamPrefixListResolverTarget(target)
	cp.State = ipamStateDeleteComplete

	return cp, nil
}

// DescribeIpamPrefixListResolverTargets returns resolver targets, optionally filtered by
// resolver ID and/or target IDs.
func (b *InMemoryBackend) DescribeIpamPrefixListResolverTargets(
	resolverID string, ids []string,
) []*IpamPrefixListResolverTarget {
	b.mu.RLock("DescribeIpamPrefixListResolverTargets")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*IpamPrefixListResolverTarget, 0, len(b.ipamPrefixListResolverTargets))

	for _, t := range b.ipamPrefixListResolverTargets {
		if resolverID != "" && t.IpamPrefixListResolverID != resolverID {
			continue
		}

		if len(idSet) > 0 && !idSet[t.IpamPrefixListResolverTargetID] {
			continue
		}

		out = append(out, copyIpamPrefixListResolverTarget(t))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].IpamPrefixListResolverTargetID < out[j].IpamPrefixListResolverTargetID
	})

	return out
}

// ModifyIpamPrefixListResolverTarget updates a target's desired version and/or
// track-latest-version flag. Nil pointers leave the corresponding field unchanged.
func (b *InMemoryBackend) ModifyIpamPrefixListResolverTarget(
	id string, desiredVersion *int64, trackLatestVersion *bool,
) (*IpamPrefixListResolverTarget, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: IpamPrefixListResolverTargetId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyIpamPrefixListResolverTarget")
	defer b.mu.Unlock()

	target, ok := b.ipamPrefixListResolverTargets[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamPrefixListResolverTargetNotFound, id)
	}

	if desiredVersion != nil {
		v := *desiredVersion
		target.DesiredVersion = &v
	}

	if trackLatestVersion != nil {
		target.TrackLatestVersion = *trackLatestVersion
	}

	target.State = ipamStateModifyComplete

	return copyIpamPrefixListResolverTarget(target), nil
}

// copyIpamPrefixListResolverTarget returns a deep copy of a target so callers cannot mutate
// backend state through the returned pointer's DesiredVersion/LastSyncedVersion pointers.
func copyIpamPrefixListResolverTarget(t *IpamPrefixListResolverTarget) *IpamPrefixListResolverTarget {
	cp := *t

	if t.DesiredVersion != nil {
		v := *t.DesiredVersion
		cp.DesiredVersion = &v
	}

	if t.LastSyncedVersion != nil {
		v := *t.LastSyncedVersion
		cp.LastSyncedVersion = &v
	}

	return &cp
}
