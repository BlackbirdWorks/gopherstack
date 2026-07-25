package ec2

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// ---- errors ----

var (
	// ErrTGWMulticastDomainNotFound is returned when a TGW multicast domain ID does not exist.
	ErrTGWMulticastDomainNotFound = errors.New("InvalidTransitGatewayMulticastDomainId.NotFound")
	// ErrTGWMeteringPolicyNotFound is returned when a TGW metering policy ID does not exist.
	ErrTGWMeteringPolicyNotFound = errors.New("InvalidTransitGatewayMeteringPolicyId.NotFound")
)

// ---- constants ----

const (
	tgwMcastAssocStateAssociated    = "associated"
	tgwMcastAssocStateDisassociated = "disassociated"
	tgwResourceTypeVPC              = "vpc"
	tgwMcastOptionDisable           = "disable"
	// tgwResourceTypeClientVpn is the DescribeTransitGatewayAttachments
	// resourceType for an attachment backing a Client VPN endpoint (parity-4).
	tgwResourceTypeClientVpn = "client-vpn"
)

// ---- models ----

// TransitGatewayMulticastDomain represents an EC2 transit gateway multicast domain.
type TransitGatewayMulticastDomain struct {
	CreationTime                 time.Time `json:"creationTime"`
	ID                           string    `json:"id,omitempty"`
	TransitGatewayID             string    `json:"transitGatewayID,omitempty"`
	State                        string    `json:"state,omitempty"`
	OwnerID                      string    `json:"ownerID,omitempty"`
	AutoAcceptSharedAssociations string    `json:"autoAcceptSharedAssociations,omitempty"`
	Igmpv2Support                string    `json:"igmpv2Support,omitempty"`
	StaticSourcesSupport         string    `json:"staticSourcesSupport,omitempty"`
}

// TransitGatewayMulticastGroupEntry represents a network interface's membership
// and/or source registration within a transit gateway multicast group, keyed by
// (domain, group IP, network interface).
type TransitGatewayMulticastGroupEntry struct {
	TransitGatewayMulticastDomainID string `json:"transitGatewayMulticastDomainID,omitempty"`
	GroupIPAddress                  string `json:"groupIPAddress,omitempty"`
	NetworkInterfaceID              string `json:"networkInterfaceID,omitempty"`
	ResourceID                      string `json:"resourceID,omitempty"`
	ResourceType                    string `json:"resourceType,omitempty"`
	IsMember                        bool   `json:"isMember,omitempty"`
	IsSource                        bool   `json:"isSource,omitempty"`
}

// TransitGatewayMulticastGroupMembership is returned by the multicast group
// member/source register and deregister operations, listing the network
// interfaces affected by the call.
type TransitGatewayMulticastGroupMembership struct {
	TransitGatewayMulticastDomainID string   `json:"transitGatewayMulticastDomainID,omitempty"`
	GroupIPAddress                  string   `json:"groupIPAddress,omitempty"`
	NetworkInterfaceIDs             []string `json:"networkInterfaceIDs,omitempty"`
}

// TransitGatewayMeteringPolicy represents an EC2 transit gateway metering policy.
type TransitGatewayMeteringPolicy struct {
	UpdateEffectiveAt      time.Time `json:"updateEffectiveAt"`
	ID                     string    `json:"id,omitempty"`
	TransitGatewayID       string    `json:"transitGatewayID,omitempty"`
	State                  string    `json:"state,omitempty"`
	MiddleboxAttachmentIDs []string  `json:"middleboxAttachmentIDs,omitempty"`
}

// TransitGatewayMeteringPolicyEntry represents a single traffic-matching rule
// within a transit gateway metering policy.
type TransitGatewayMeteringPolicyEntry struct {
	UpdateEffectiveAt                       time.Time `json:"updateEffectiveAt"`
	UpdatedAt                               time.Time `json:"updatedAt"`
	TransitGatewayMeteringPolicyID          string    `json:"transitGatewayMeteringPolicyID,omitempty"`
	MeteredAccount                          string    `json:"meteredAccount,omitempty"`
	State                                   string    `json:"state,omitempty"`
	DestinationCidrBlock                    string    `json:"destinationCidrBlock,omitempty"`
	DestinationPortRange                    string    `json:"destinationPortRange,omitempty"`
	DestinationTransitGatewayAttachmentID   string    `json:"destinationTransitGatewayAttachmentID,omitempty"`
	DestinationTransitGatewayAttachmentType string    `json:"destinationTransitGatewayAttachmentType,omitempty"`
	Protocol                                string    `json:"protocol,omitempty"`
	SourceCidrBlock                         string    `json:"sourceCidrBlock,omitempty"`
	SourcePortRange                         string    `json:"sourcePortRange,omitempty"`
	SourceTransitGatewayAttachmentID        string    `json:"sourceTransitGatewayAttachmentID,omitempty"`
	SourceTransitGatewayAttachmentType      string    `json:"sourceTransitGatewayAttachmentType,omitempty"`
	PolicyRuleNumber                        int       `json:"policyRuleNumber,omitempty"`
}

// ---- Transit Gateway Multicast Domains ----

// CreateTransitGatewayMulticastDomain creates a new multicast domain on the
// given transit gateway. Any of the three option strings may be empty, in
// which case they default to "disable" as AWS does.
func (b *InMemoryBackend) CreateTransitGatewayMulticastDomain(
	tgwID, autoAcceptSharedAssociations, igmpv2Support, staticSourcesSupport string,
) (*TransitGatewayMulticastDomain, error) {
	if tgwID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateTransitGatewayMulticastDomain")
	defer b.mu.Unlock()

	if _, ok := b.transitGateways.Get(tgwID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrTransitGatewayNotFound, tgwID)
	}

	if autoAcceptSharedAssociations == "" {
		autoAcceptSharedAssociations = tgwMcastOptionDisable
	}

	if igmpv2Support == "" {
		igmpv2Support = tgwMcastOptionDisable
	}

	if staticSourcesSupport == "" {
		staticSourcesSupport = tgwMcastOptionDisable
	}

	domain := &TransitGatewayMulticastDomain{
		ID:                           "tgw-mcast-domain-" + uuid.New().String()[:17],
		TransitGatewayID:             tgwID,
		State:                        stateAvailable,
		OwnerID:                      b.AccountID,
		CreationTime:                 time.Now().UTC(),
		AutoAcceptSharedAssociations: autoAcceptSharedAssociations,
		Igmpv2Support:                igmpv2Support,
		StaticSourcesSupport:         staticSourcesSupport,
	}
	b.tgwMulticastDomains.Put(domain)

	cp := *domain

	return &cp, nil
}

// DescribeTransitGatewayMulticastDomains returns multicast domains, optionally
// filtered by domain ID.
func (b *InMemoryBackend) DescribeTransitGatewayMulticastDomains(
	ids []string,
) []*TransitGatewayMulticastDomain {
	b.mu.RLock("DescribeTransitGatewayMulticastDomains")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*TransitGatewayMulticastDomain, 0, b.tgwMulticastDomains.Len())

	for _, d := range b.tgwMulticastDomains.All() {
		if len(idSet) > 0 && !idSet[d.ID] {
			continue
		}

		cp := *d
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].ID < out[j].ID
	})

	return out
}

// DeleteTransitGatewayMulticastDomain removes a multicast domain along with
// any subnet associations and multicast group registrations tied to it.
func (b *InMemoryBackend) DeleteTransitGatewayMulticastDomain(id string) error {
	if id == "" {
		return fmt.Errorf("%w: TransitGatewayMulticastDomainId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteTransitGatewayMulticastDomain")
	defer b.mu.Unlock()

	if _, ok := b.tgwMulticastDomains.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrTGWMulticastDomainNotFound, id)
	}
	b.tgwMulticastDomains.Delete(id)
	delete(b.tags, id)

	for _, assoc := range b.tgwMulticastDomainAssociations.All() {
		key := tgwMulticastDomainAssociationsKeyFn(assoc)
		if assoc.TransitGatewayMulticastDomainID == id {
			b.tgwMulticastDomainAssociations.Delete(key)
		}
	}

	for _, entry := range b.tgwMulticastGroupEntries.All() {
		key := tgwMulticastGroupEntriesKeyFn(entry)
		if entry.TransitGatewayMulticastDomainID == id {
			b.tgwMulticastGroupEntries.Delete(key)
		}
	}

	return nil
}

// ---- Transit Gateway Multicast Domain associations ----

// AssociateTransitGatewayMulticastDomain associates one or more subnets, via a
// transit gateway attachment, with a transit gateway multicast domain.
func (b *InMemoryBackend) AssociateTransitGatewayMulticastDomain(
	domainID, attachmentID string,
	subnetIDs []string,
) ([]*TransitGatewayMulticastDomainAssociation, error) {
	if domainID == "" {
		return nil, fmt.Errorf(
			"%w: TransitGatewayMulticastDomainId is required",
			ErrInvalidParameter,
		)
	}

	if attachmentID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayAttachmentId is required", ErrInvalidParameter)
	}

	if len(subnetIDs) == 0 {
		return nil, fmt.Errorf("%w: at least one SubnetId is required", ErrInvalidParameter)
	}

	b.mu.Lock("AssociateTransitGatewayMulticastDomain")
	defer b.mu.Unlock()

	if _, ok := b.tgwMulticastDomains.Get(domainID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrTGWMulticastDomainNotFound, domainID)
	}

	assocs := make([]*TransitGatewayMulticastDomainAssociation, 0, len(subnetIDs))

	for _, subnetID := range subnetIDs {
		assoc := &TransitGatewayMulticastDomainAssociation{
			TransitGatewayMulticastDomainID: domainID,
			TransitGatewayAttachmentID:      attachmentID,
			SubnetID:                        subnetID,
			State:                           tgwMcastAssocStateAssociated,
		}
		b.tgwMulticastDomainAssociations.Put(assoc)

		cp := *assoc
		assocs = append(assocs, &cp)
	}

	return assocs, nil
}

// DisassociateTransitGatewayMulticastDomain removes the association between the
// given subnets and a transit gateway multicast domain.
func (b *InMemoryBackend) DisassociateTransitGatewayMulticastDomain(
	domainID, attachmentID string,
	subnetIDs []string,
) ([]*TransitGatewayMulticastDomainAssociation, error) {
	if domainID == "" {
		return nil, fmt.Errorf(
			"%w: TransitGatewayMulticastDomainId is required",
			ErrInvalidParameter,
		)
	}

	if len(subnetIDs) == 0 {
		return nil, fmt.Errorf("%w: at least one SubnetId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DisassociateTransitGatewayMulticastDomain")
	defer b.mu.Unlock()

	assocs := make([]*TransitGatewayMulticastDomainAssociation, 0, len(subnetIDs))

	for _, subnetID := range subnetIDs {
		key := domainID + ":" + subnetID

		existing, ok := b.tgwMulticastDomainAssociations.Get(key)
		if ok {
			b.tgwMulticastDomainAssociations.Delete(key)
		} else {
			existing = &TransitGatewayMulticastDomainAssociation{
				TransitGatewayMulticastDomainID: domainID,
				TransitGatewayAttachmentID:      attachmentID,
				SubnetID:                        subnetID,
			}
		}

		cp := *existing
		cp.State = tgwMcastAssocStateDisassociated
		assocs = append(assocs, &cp)
	}

	return assocs, nil
}

// GetTransitGatewayMulticastDomainAssociations returns the subnet associations
// for a transit gateway multicast domain.
func (b *InMemoryBackend) GetTransitGatewayMulticastDomainAssociations(
	domainID string,
) []*TransitGatewayMulticastDomainAssociation {
	b.mu.RLock("GetTransitGatewayMulticastDomainAssociations")
	defer b.mu.RUnlock()

	out := make([]*TransitGatewayMulticastDomainAssociation, 0)

	for _, assoc := range b.tgwMulticastDomainAssociations.All() {
		if domainID != "" && assoc.TransitGatewayMulticastDomainID != domainID {
			continue
		}

		cp := *assoc
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].SubnetID < out[j].SubnetID
	})

	return out
}

// ---- Transit Gateway Multicast Group members / sources ----

// registerMulticastGroupResource registers (or updates) a set of network
// interfaces as members (asMember=true) or sources (asMember=false) of a
// multicast group within a transit gateway multicast domain.
// Must be called without b.mu held; it acquires the write lock itself.
func (b *InMemoryBackend) registerMulticastGroupResource(
	domainID, groupIP string,
	eniIDs []string,
	asMember bool,
) (*TransitGatewayMulticastGroupMembership, error) {
	if domainID == "" {
		return nil, fmt.Errorf(
			"%w: TransitGatewayMulticastDomainId is required",
			ErrInvalidParameter,
		)
	}

	if len(eniIDs) == 0 {
		return nil, fmt.Errorf("%w: at least one NetworkInterfaceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("registerMulticastGroupResource")
	defer b.mu.Unlock()

	if _, ok := b.tgwMulticastDomains.Get(domainID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrTGWMulticastDomainNotFound, domainID)
	}

	registered := make([]string, 0, len(eniIDs))

	for _, eniID := range eniIDs {
		key := domainID + ":" + groupIP + ":" + eniID

		entry, ok := b.tgwMulticastGroupEntries.Get(key)
		if !ok {
			entry = &TransitGatewayMulticastGroupEntry{
				TransitGatewayMulticastDomainID: domainID,
				GroupIPAddress:                  groupIP,
				NetworkInterfaceID:              eniID,
				ResourceID:                      eniID,
				ResourceType:                    tgwResourceTypeVPC,
			}
			b.tgwMulticastGroupEntries.Put(entry)
		}

		if asMember {
			entry.IsMember = true
		} else {
			entry.IsSource = true
		}

		registered = append(registered, eniID)
	}

	return &TransitGatewayMulticastGroupMembership{
		TransitGatewayMulticastDomainID: domainID,
		GroupIPAddress:                  groupIP,
		NetworkInterfaceIDs:             registered,
	}, nil
}

// deregisterMulticastGroupResource is the inverse of registerMulticastGroupResource.
// A network interface's group entry is removed entirely once it is neither a
// member nor a source. Unknown network interfaces are treated as a no-op.
func (b *InMemoryBackend) deregisterMulticastGroupResource(
	domainID, groupIP string,
	eniIDs []string,
	asMember bool,
) (*TransitGatewayMulticastGroupMembership, error) {
	if domainID == "" {
		return nil, fmt.Errorf(
			"%w: TransitGatewayMulticastDomainId is required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("deregisterMulticastGroupResource")
	defer b.mu.Unlock()

	deregistered := make([]string, 0, len(eniIDs))

	for _, eniID := range eniIDs {
		key := domainID + ":" + groupIP + ":" + eniID

		entry, ok := b.tgwMulticastGroupEntries.Get(key)
		if !ok {
			deregistered = append(deregistered, eniID)

			continue
		}

		if asMember {
			entry.IsMember = false
		} else {
			entry.IsSource = false
		}

		if !entry.IsMember && !entry.IsSource {
			b.tgwMulticastGroupEntries.Delete(key)
		}

		deregistered = append(deregistered, eniID)
	}

	return &TransitGatewayMulticastGroupMembership{
		TransitGatewayMulticastDomainID: domainID,
		GroupIPAddress:                  groupIP,
		NetworkInterfaceIDs:             deregistered,
	}, nil
}

// RegisterTransitGatewayMulticastGroupMembers registers network interfaces as
// members of a transit gateway multicast group.
func (b *InMemoryBackend) RegisterTransitGatewayMulticastGroupMembers(
	domainID, groupIP string,
	eniIDs []string,
) (*TransitGatewayMulticastGroupMembership, error) {
	return b.registerMulticastGroupResource(domainID, groupIP, eniIDs, true)
}

// DeregisterTransitGatewayMulticastGroupMembers removes network interfaces as
// members of a transit gateway multicast group.
func (b *InMemoryBackend) DeregisterTransitGatewayMulticastGroupMembers(
	domainID, groupIP string,
	eniIDs []string,
) (*TransitGatewayMulticastGroupMembership, error) {
	return b.deregisterMulticastGroupResource(domainID, groupIP, eniIDs, true)
}

// RegisterTransitGatewayMulticastGroupSources registers network interfaces as
// sources of a transit gateway multicast group.
func (b *InMemoryBackend) RegisterTransitGatewayMulticastGroupSources(
	domainID, groupIP string,
	eniIDs []string,
) (*TransitGatewayMulticastGroupMembership, error) {
	return b.registerMulticastGroupResource(domainID, groupIP, eniIDs, false)
}

// DeregisterTransitGatewayMulticastGroupSources removes network interfaces as
// sources of a transit gateway multicast group.
func (b *InMemoryBackend) DeregisterTransitGatewayMulticastGroupSources(
	domainID, groupIP string,
	eniIDs []string,
) (*TransitGatewayMulticastGroupMembership, error) {
	return b.deregisterMulticastGroupResource(domainID, groupIP, eniIDs, false)
}

// SearchTransitGatewayMulticastGroups returns the multicast group member/source
// entries registered within a transit gateway multicast domain.
func (b *InMemoryBackend) SearchTransitGatewayMulticastGroups(
	domainID string,
) []*TransitGatewayMulticastGroupEntry {
	b.mu.RLock("SearchTransitGatewayMulticastGroups")
	defer b.mu.RUnlock()

	out := make([]*TransitGatewayMulticastGroupEntry, 0)

	for _, entry := range b.tgwMulticastGroupEntries.All() {
		if domainID != "" && entry.TransitGatewayMulticastDomainID != domainID {
			continue
		}

		cp := *entry
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].GroupIPAddress != out[j].GroupIPAddress {
			return out[i].GroupIPAddress < out[j].GroupIPAddress
		}

		return out[i].NetworkInterfaceID < out[j].NetworkInterfaceID
	})

	return out
}

// ---- Transit Gateway Metering Policies ----

// CreateTransitGatewayMeteringPolicy creates a new metering policy on the
// given transit gateway.
func (b *InMemoryBackend) CreateTransitGatewayMeteringPolicy(
	tgwID string,
	middleboxAttachmentIDs []string,
) (*TransitGatewayMeteringPolicy, error) {
	if tgwID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateTransitGatewayMeteringPolicy")
	defer b.mu.Unlock()

	if _, ok := b.transitGateways.Get(tgwID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrTransitGatewayNotFound, tgwID)
	}

	ids := make([]string, len(middleboxAttachmentIDs))
	copy(ids, middleboxAttachmentIDs)

	policy := &TransitGatewayMeteringPolicy{
		ID:                     "tgw-metering-policy-" + uuid.New().String()[:17],
		TransitGatewayID:       tgwID,
		State:                  stateAvailable,
		MiddleboxAttachmentIDs: ids,
		UpdateEffectiveAt:      time.Now().UTC(),
	}
	b.tgwMeteringPolicies.Put(policy)

	return copyTGWMeteringPolicy(policy), nil
}

// copyTGWMeteringPolicy returns a deep copy of a metering policy, including its
// middlebox attachment ID slice.
func copyTGWMeteringPolicy(p *TransitGatewayMeteringPolicy) *TransitGatewayMeteringPolicy {
	cp := *p
	cp.MiddleboxAttachmentIDs = make([]string, len(p.MiddleboxAttachmentIDs))
	copy(cp.MiddleboxAttachmentIDs, p.MiddleboxAttachmentIDs)

	return &cp
}

// DescribeTransitGatewayMeteringPolicies returns metering policies, optionally
// filtered by policy ID.
func (b *InMemoryBackend) DescribeTransitGatewayMeteringPolicies(
	ids []string,
) []*TransitGatewayMeteringPolicy {
	b.mu.RLock("DescribeTransitGatewayMeteringPolicies")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*TransitGatewayMeteringPolicy, 0, b.tgwMeteringPolicies.Len())

	for _, p := range b.tgwMeteringPolicies.All() {
		if len(idSet) > 0 && !idSet[p.ID] {
			continue
		}

		out = append(out, copyTGWMeteringPolicy(p))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].ID < out[j].ID
	})

	return out
}

// DeleteTransitGatewayMeteringPolicy removes a metering policy along with any
// entries defined within it.
func (b *InMemoryBackend) DeleteTransitGatewayMeteringPolicy(id string) error {
	if id == "" {
		return fmt.Errorf("%w: TransitGatewayMeteringPolicyId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteTransitGatewayMeteringPolicy")
	defer b.mu.Unlock()

	if _, ok := b.tgwMeteringPolicies.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrTGWMeteringPolicyNotFound, id)
	}
	b.tgwMeteringPolicies.Delete(id)
	delete(b.tags, id)

	for _, entry := range b.tgwMeteringPolicyEntries.All() {
		key := tgwMeteringPolicyEntriesKeyFn(entry)
		if entry.TransitGatewayMeteringPolicyID == id {
			b.tgwMeteringPolicyEntries.Delete(key)
		}
	}

	return nil
}

// ---- Transit Gateway Metering Policy entries ----

// CreateTransitGatewayMeteringPolicyEntry adds a traffic-matching rule to a
// metering policy. entry's TransitGatewayMeteringPolicyID, State, and
// timestamp fields are set by this call; the caller fills in PolicyRuleNumber,
// MeteredAccount, and the rule-matching fields.
func (b *InMemoryBackend) CreateTransitGatewayMeteringPolicyEntry(
	policyID string,
	entry *TransitGatewayMeteringPolicyEntry,
) (*TransitGatewayMeteringPolicyEntry, error) {
	if policyID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayMeteringPolicyId is required", ErrInvalidParameter)
	}

	if entry == nil || entry.PolicyRuleNumber <= 0 {
		return nil, fmt.Errorf("%w: PolicyRuleNumber is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateTransitGatewayMeteringPolicyEntry")
	defer b.mu.Unlock()

	if _, ok := b.tgwMeteringPolicies.Get(policyID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrTGWMeteringPolicyNotFound, policyID)
	}

	now := time.Now().UTC()
	stored := *entry
	stored.TransitGatewayMeteringPolicyID = policyID
	stored.State = stateAvailable
	stored.UpdateEffectiveAt = now
	stored.UpdatedAt = now

	b.tgwMeteringPolicyEntries.Put(&stored)

	cp := stored

	return &cp, nil
}

// DeleteTransitGatewayMeteringPolicyEntry removes a single rule from a
// metering policy.
func (b *InMemoryBackend) DeleteTransitGatewayMeteringPolicyEntry(
	policyID string,
	ruleNumber int,
) (*TransitGatewayMeteringPolicyEntry, error) {
	if policyID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayMeteringPolicyId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteTransitGatewayMeteringPolicyEntry")
	defer b.mu.Unlock()

	key := policyID + ":" + strconv.Itoa(ruleNumber)

	entry, ok := b.tgwMeteringPolicyEntries.Get(key)
	if !ok {
		return nil, fmt.Errorf(
			"%w: metering policy entry %d in %s not found",
			ErrInvalidParameter,
			ruleNumber,
			policyID,
		)
	}
	b.tgwMeteringPolicyEntries.Delete(key)

	cp := *entry
	cp.State = tgwRouteStateDeleted

	return &cp, nil
}
