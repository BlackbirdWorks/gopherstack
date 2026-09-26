package ec2

import (
	"fmt"
	"sort"

	"github.com/google/uuid"
)

// ---- IPAM Internet Registry Associations ----

// CreateIpamInternetRegistryAssociation creates an association between an IPAM and a Regional
// Internet Registry for RPKI/ROA management. The association lands directly on the real
// steady "pending-enable" state (ec2@v1.329.0 types/enums.go
// IpamInternetRegistryAssociationState) -- the AWS lifecycle's actual resting point while
// awaiting the BPKI setup EnableIpamInternetRegistryAssociation performs, not a synthetic
// "create-complete" this state machine has no member for.
func (b *InMemoryBackend) CreateIpamInternetRegistryAssociation(
	ipamID, organizationHandle, rir, description string,
) (*IpamInternetRegistryAssociation, error) {
	if ipamID == "" {
		return nil, fmt.Errorf("%w: IpamId is required", ErrInvalidParameter)
	}

	if organizationHandle == "" {
		return nil, fmt.Errorf("%w: OrganizationHandle is required", ErrInvalidParameter)
	}

	if !ipamValidRirs[rir] {
		return nil, fmt.Errorf("%w: Rir must be one of ripe, apnic, arin, lacnic; got %q", ErrInvalidParameter, rir)
	}

	b.mu.Lock("CreateIpamInternetRegistryAssociation")
	defer b.mu.Unlock()

	ipam, ok := b.ipams.Get(ipamID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamNotFound, ipamID)
	}

	id := "ipam-ira-" + uuid.New().String()[:8]
	assoc := &IpamInternetRegistryAssociation{
		IpamInternetRegistryAssociationID: id,
		IpamInternetRegistryAssociationARN: "arn:aws:ec2:" + b.Region + ":" + b.AccountID +
			":ipam-internet-registry-association/" + id,
		IpamID:             ipamID,
		IpamRegion:         ipam.Region,
		OrganizationHandle: organizationHandle,
		Rir:                rir,
		Description:        description,
		OwnerID:            b.AccountID,
		State:              ipamIRAStatePendingEnable,
	}
	b.ipamInternetRegistryAssociations.Put(assoc)

	cp := *assoc

	return &cp, nil
}

// ipamInternetRegistryAssociationMatchesFilter reports whether assoc matches a single
// Filter.N.Name/Value.M entry. No canonical filter-name list for this operation is published
// in the pinned SDK's doc comments (only "One or more filters to apply to the results."), so
// this maps the request's documented response fields 1:1 -- flagged as best-effort in
// PARITY.md pending confirmation against real AWS filter names.
func ipamInternetRegistryAssociationMatchesFilter(
	assoc *IpamInternetRegistryAssociation, name string, values []string,
) bool {
	switch name {
	case "ipam-id":
		return anyEqual(assoc.IpamID, values)
	case filterKeyState:
		return anyEqual(assoc.State, values)
	case "rir":
		return anyEqual(assoc.Rir, values)
	case "organization-handle":
		return anyEqual(assoc.OrganizationHandle, values)
	default:
		return true
	}
}

// DescribeIpamInternetRegistryAssociations returns internet registry associations, optionally
// filtered by ID and/or generic filters.
func (b *InMemoryBackend) DescribeIpamInternetRegistryAssociations(
	ids []string, filters map[string][]string,
) []*IpamInternetRegistryAssociation {
	b.mu.RLock("DescribeIpamInternetRegistryAssociations")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*IpamInternetRegistryAssociation, 0, b.ipamInternetRegistryAssociations.Len())

assocLoop:
	for _, assoc := range b.ipamInternetRegistryAssociations.All() {
		if len(idSet) > 0 && !idSet[assoc.IpamInternetRegistryAssociationID] {
			continue
		}

		for name, values := range filters {
			if !ipamInternetRegistryAssociationMatchesFilter(assoc, name, values) {
				continue assocLoop
			}
		}

		cp := *assoc
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].IpamInternetRegistryAssociationID < out[j].IpamInternetRegistryAssociationID
	})

	return out
}

// EnableIpamInternetRegistryAssociation completes BPKI setup for a pending internet registry
// association, moving it to "enable-complete". Real AWS's EnableIpamInternetRegistryAssociation
// output stores no ChildHandle/ParentBpkiTa/ParentHandle/RpkiVersion/ServiceUri members on
// IpamInternetRegistryAssociation (ec2@v1.329.0 types.go), so these required inputs are
// validated but not persisted.
func (b *InMemoryBackend) EnableIpamInternetRegistryAssociation(
	id, childHandle, parentBpkiTa, parentHandle, rpkiVersion, serviceURI string,
) (*IpamInternetRegistryAssociation, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: IpamInternetRegistryAssociationId is required", ErrInvalidParameter)
	}

	for name, v := range map[string]string{
		"ChildHandle": childHandle, "ParentBpkiTa": parentBpkiTa, "ParentHandle": parentHandle,
		"RpkiVersion": rpkiVersion, "ServiceUri": serviceURI,
	} {
		if v == "" {
			return nil, fmt.Errorf("%w: %s is required", ErrMissingParameter, name)
		}
	}

	b.mu.Lock("EnableIpamInternetRegistryAssociation")
	defer b.mu.Unlock()

	assoc, ok := b.ipamInternetRegistryAssociations.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamInternetRegistryAssociationNotFound, id)
	}

	if assoc.State != ipamIRAStatePendingEnable {
		return nil, fmt.Errorf(
			"%w: internet registry association %s is not pending-enable (current state: %s)",
			ErrIpamInternetRegistryAssociationState, id, assoc.State,
		)
	}

	assoc.State = ipamIRAStateEnableComplete

	cp := *assoc

	return &cp, nil
}

// DeleteIpamInternetRegistryAssociation removes an internet registry association. Real AWS
// requires all routing policy registrations against it to be removed first
// (api_op_DeleteIpamInternetRegistryAssociation.go doc comment).
func (b *InMemoryBackend) DeleteIpamInternetRegistryAssociation(id string) (*IpamInternetRegistryAssociation, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: IpamInternetRegistryAssociationId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteIpamInternetRegistryAssociation")
	defer b.mu.Unlock()

	assoc, ok := b.ipamInternetRegistryAssociations.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamInternetRegistryAssociationNotFound, id)
	}

	for _, reg := range b.ipamRoutingPolicyRegistrations.All() {
		if reg.IpamInternetRegistryAssociationID == id {
			return nil, fmt.Errorf(
				"%w: internet registry association %s still has routing policy registrations",
				ErrDependencyViolation, id,
			)
		}
	}

	b.ipamInternetRegistryAssociations.Delete(id)
	delete(b.tags, id)

	cp := *assoc
	cp.State = ipamIRAStateDeleteComplete

	return &cp, nil
}

// GetIpamInternetRegistryAssociationAsns returns the ASNs registered with the internet
// registry for an association, derived from the ASNs supplied in that association's routing
// policy registrations (real, stored client input -- never fabricated).
func (b *InMemoryBackend) GetIpamInternetRegistryAssociationAsns(
	id string,
) ([]*IpamInternetRegistryAssociationAsn, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: IpamInternetRegistryAssociationId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetIpamInternetRegistryAssociationAsns")
	defer b.mu.RUnlock()

	if !b.ipamInternetRegistryAssociations.Has(id) {
		return nil, fmt.Errorf("%w: %s", ErrIpamInternetRegistryAssociationNotFound, id)
	}

	seen := make(map[string]bool)

	var out []*IpamInternetRegistryAssociationAsn

	for _, reg := range b.ipamRoutingPolicyRegistrations.All() {
		if reg.IpamInternetRegistryAssociationID != id {
			continue
		}

		for _, asn := range reg.Asns {
			if seen[asn] {
				continue
			}

			seen[asn] = true
			out = append(out, &IpamInternetRegistryAssociationAsn{Asn: asn})
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Asn < out[j].Asn })

	return out, nil
}

// GetIpamInternetRegistryAssociationCidrs returns the CIDRs registered with the internet
// registry for an association, derived from that association's routing policy registrations
// (real, stored client input -- never fabricated).
func (b *InMemoryBackend) GetIpamInternetRegistryAssociationCidrs(
	id string,
) ([]*IpamInternetRegistryAssociationCidr, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: IpamInternetRegistryAssociationId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetIpamInternetRegistryAssociationCidrs")
	defer b.mu.RUnlock()

	if !b.ipamInternetRegistryAssociations.Has(id) {
		return nil, fmt.Errorf("%w: %s", ErrIpamInternetRegistryAssociationNotFound, id)
	}

	var out []*IpamInternetRegistryAssociationCidr

	for _, reg := range b.ipamRoutingPolicyRegistrations.All() {
		if reg.IpamInternetRegistryAssociationID != id {
			continue
		}

		out = append(out, &IpamInternetRegistryAssociationCidr{Cidr: reg.Cidr})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Cidr < out[j].Cidr })

	return out, nil
}
