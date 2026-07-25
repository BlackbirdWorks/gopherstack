package ec2

import (
	"errors"
	"fmt"
	"maps"
	"sort"

	"github.com/google/uuid"
)

// ---- errors ----

var (
	// ErrLocalGatewayRouteTableNotFound is returned when a local gateway route table is not found.
	ErrLocalGatewayRouteTableNotFound = errors.New("InvalidLocalGatewayRouteTableID.NotFound")
	// ErrLocalGatewayRouteNotFound is returned when a local gateway route is not found.
	ErrLocalGatewayRouteNotFound = errors.New("InvalidLocalGatewayRoute.NotFound")
	// ErrLocalGatewayVpcAssociationNotFound is returned when a local gateway route table VPC
	// association is not found.
	ErrLocalGatewayVpcAssociationNotFound = errors.New(
		"InvalidLocalGatewayRouteTableVpcAssociationID.NotFound",
	)
	// ErrLocalGatewayVifGroupAssociationNotFound is returned when a local gateway route table
	// virtual interface group association is not found.
	ErrLocalGatewayVifGroupAssociationNotFound = errors.New(
		"InvalidLocalGatewayRouteTableVirtualInterfaceGroupAssociationID.NotFound",
	)
	// ErrLocalGatewayVifNotFound is returned when a local gateway virtual interface is
	// not found.
	ErrLocalGatewayVifNotFound = errors.New("InvalidLocalGatewayVirtualInterfaceID.NotFound")
	// ErrLocalGatewayVifGroupNotFound is returned when a local gateway virtual
	// interface group is not found.
	ErrLocalGatewayVifGroupNotFound = errors.New("InvalidLocalGatewayVirtualInterfaceGroupID.NotFound")
)

// ---- constants ----

const (
	localGatewayRouteTypeStatic    = "static"
	localGatewayRouteStateActive   = "active"
	localGatewayRouteStateDeleted  = "deleted"
	localGatewayAssocStateAssoc    = "associated"
	localGatewayAssocStateDisassoc = "disassociated"
)

// ---- models ----

// LocalGateway represents an Outpost local gateway. Local gateways are provisioned by
// AWS alongside an Outpost and have no Create/Delete API; SeedLocalGateway populates
// them so Describe calls return realistic data.
type LocalGateway struct {
	LocalGatewayID string `json:"localGatewayID,omitempty"`
	OutpostArn     string `json:"outpostArn,omitempty"`
	OwnerID        string `json:"ownerID,omitempty"`
	State          string `json:"state,omitempty"`
}

// LocalGatewayVirtualInterface represents an Outpost local gateway virtual interface.
// Like LocalGateway, VIFs are Outpost-provisioned; SeedLocalGatewayVirtualInterface
// populates them so Describe calls return realistic data.
type LocalGatewayVirtualInterface struct {
	Tags                                map[string]string `json:"tags,omitempty"`
	LocalGatewayVirtualInterfaceGroupID string            `json:"localGatewayVirtualInterfaceGroupID,omitempty"`
	LocalGatewayID                      string            `json:"localGatewayID,omitempty"`
	LocalGatewayVirtualInterfaceID      string            `json:"localGatewayVirtualInterfaceID,omitempty"`
	OutpostLagID                        string            `json:"outpostLagID,omitempty"`
	ConfigurationState                  string            `json:"configurationState,omitempty"`
	LocalAddress                        string            `json:"localAddress,omitempty"`
	PeerAddress                         string            `json:"peerAddress,omitempty"`
	OwnerID                             string            `json:"ownerID,omitempty"`
	LocalGatewayVirtualInterfaceArn     string            `json:"localGatewayVirtualInterfaceArn,omitempty"`
	PeerBgpAsnExtended                  int64             `json:"peerBgpAsnExtended,omitempty"`
	LocalBgpAsn                         int32             `json:"localBgpAsn,omitempty"`
	PeerBgpAsn                          int32             `json:"peerBgpAsn,omitempty"`
	Vlan                                int32             `json:"vlan,omitempty"`
}

// LocalGatewayVirtualInterfaceGroup represents an Outpost local gateway virtual
// interface group. Seeded via SeedLocalGatewayVirtualInterfaceGroup.
type LocalGatewayVirtualInterfaceGroup struct {
	LocalGatewayVirtualInterfaceGroupID  string            `json:"localGatewayVirtualInterfaceGroupID,omitempty"`
	LocalGatewayVirtualInterfaceGroupArn string            `json:"localGatewayVirtualInterfaceGroupArn,omitempty"`
	LocalGatewayID                       string            `json:"localGatewayID,omitempty"`
	ConfigurationState                   string            `json:"configurationState,omitempty"`
	OwnerID                              string            `json:"ownerID,omitempty"`
	Tags                                 map[string]string `json:"tags,omitempty"`
	LocalGatewayVirtualInterfaceIDs      []string          `json:"localGatewayVirtualInterfaceIDs,omitempty"`
	LocalBgpAsn                          int32             `json:"localBgpAsn,omitempty"`
	LocalBgpAsnExtended                  int64             `json:"localBgpAsnExtended,omitempty"`
}

// LocalGatewayRouteTable represents a local gateway route table.
type LocalGatewayRouteTable struct {
	LocalGatewayRouteTableID  string `json:"localGatewayRouteTableID,omitempty"`
	LocalGatewayRouteTableArn string `json:"localGatewayRouteTableArn,omitempty"`
	LocalGatewayID            string `json:"localGatewayID,omitempty"`
	OutpostArn                string `json:"outpostArn,omitempty"`
	Mode                      string `json:"mode,omitempty"`
	State                     string `json:"state,omitempty"`
	OwnerID                   string `json:"ownerID,omitempty"`
}

// LocalGatewayRoute represents a route in a local gateway route table.
type LocalGatewayRoute struct {
	LocalGatewayRouteTableID            string `json:"localGatewayRouteTableID,omitempty"`
	LocalGatewayRouteTableArn           string `json:"localGatewayRouteTableArn,omitempty"`
	DestinationCidrBlock                string `json:"destinationCidrBlock,omitempty"`
	DestinationPrefixListID             string `json:"destinationPrefixListID,omitempty"`
	LocalGatewayVirtualInterfaceGroupID string `json:"localGatewayVirtualInterfaceGroupID,omitempty"`
	NetworkInterfaceID                  string `json:"networkInterfaceID,omitempty"`
	Type                                string `json:"type,omitempty"`
	State                               string `json:"state,omitempty"`
	OwnerID                             string `json:"ownerID,omitempty"`
}

// LocalGatewayRouteTableVpcAssociation represents an association between a local
// gateway route table and a VPC.
type LocalGatewayRouteTableVpcAssociation struct {
	LocalGatewayRouteTableVpcAssociationID string `json:"localGatewayRouteTableVpcAssociationID,omitempty"`
	LocalGatewayRouteTableID               string `json:"localGatewayRouteTableID,omitempty"`
	LocalGatewayRouteTableArn              string `json:"localGatewayRouteTableArn,omitempty"`
	LocalGatewayID                         string `json:"localGatewayID,omitempty"`
	VpcID                                  string `json:"vpcID,omitempty"`
	State                                  string `json:"state,omitempty"`
	OwnerID                                string `json:"ownerID,omitempty"`
}

// LocalGatewayRouteTableVirtualInterfaceGroupAssociation represents an association
// between a local gateway route table and a virtual interface group.
type LocalGatewayRouteTableVirtualInterfaceGroupAssociation struct {
	LocalGatewayRouteTableVirtualInterfaceGroupAssociationID string `json:"id,omitempty"`
	LocalGatewayRouteTableID                                 string `json:"localGatewayRouteTableID,omitempty"`
	LocalGatewayRouteTableArn                                string `json:"localGatewayRouteTableArn,omitempty"`
	LocalGatewayID                                           string `json:"localGatewayID,omitempty"`
	LocalGatewayVirtualInterfaceGroupID                      string `json:"localGatewayVirtualInterfaceGroupID,omitempty"`
	State                                                    string `json:"state,omitempty"`
	OwnerID                                                  string `json:"ownerID,omitempty"`
}

// ---- seed helpers (no Create API — Outpost-provisioned) ----

// SeedLocalGateway injects a local gateway into the backend so Describe calls return
// realistic data, mirroring inspector2.SeedFinding: local gateways are provisioned by
// AWS alongside an Outpost and have no Create API, so operators/tests populate them
// directly. Unset fields are defaulted to AWS-plausible values. Returns the stored
// local gateway (with a generated ID when none was supplied).
func (b *InMemoryBackend) SeedLocalGateway(lg LocalGateway) (*LocalGateway, error) {
	b.mu.Lock("SeedLocalGateway")
	defer b.mu.Unlock()

	stored := lg
	if stored.LocalGatewayID == "" {
		stored.LocalGatewayID = "lgw-" + uuid.New().String()[:17]
	}

	if stored.OwnerID == "" {
		stored.OwnerID = b.AccountID
	}

	if stored.State == "" {
		stored.State = stateAvailable
	}

	cp := stored
	b.localGateways.Put(&cp)

	out := cp

	return &out, nil
}

// SeedLocalGatewayVirtualInterface injects a local gateway virtual interface into the
// backend so DescribeLocalGatewayVirtualInterfaces returns realistic data. See
// SeedLocalGateway for rationale.
func (b *InMemoryBackend) SeedLocalGatewayVirtualInterface(
	vif LocalGatewayVirtualInterface,
) (*LocalGatewayVirtualInterface, error) {
	b.mu.Lock("SeedLocalGatewayVirtualInterface")
	defer b.mu.Unlock()

	stored := vif
	if stored.LocalGatewayVirtualInterfaceID == "" {
		stored.LocalGatewayVirtualInterfaceID = "lgw-vif-" + uuid.New().String()[:17]
	}

	if stored.OwnerID == "" {
		stored.OwnerID = b.AccountID
	}

	if stored.ConfigurationState == "" {
		stored.ConfigurationState = stateAvailable
	}

	cp := stored
	b.localGatewayVirtualInterfaces.Put(&cp)

	out := cp

	return &out, nil
}

// SeedLocalGatewayVirtualInterfaceGroup injects a local gateway virtual interface group
// into the backend so DescribeLocalGatewayVirtualInterfaceGroups returns realistic data.
// See SeedLocalGateway for rationale.
func (b *InMemoryBackend) SeedLocalGatewayVirtualInterfaceGroup(
	group LocalGatewayVirtualInterfaceGroup,
) (*LocalGatewayVirtualInterfaceGroup, error) {
	b.mu.Lock("SeedLocalGatewayVirtualInterfaceGroup")
	defer b.mu.Unlock()

	stored := group
	if stored.LocalGatewayVirtualInterfaceGroupID == "" {
		stored.LocalGatewayVirtualInterfaceGroupID = "lgw-vif-grp-" + uuid.New().String()[:17]
	}

	if stored.OwnerID == "" {
		stored.OwnerID = b.AccountID
	}

	if stored.ConfigurationState == "" {
		stored.ConfigurationState = stateAvailable
	}

	cp := stored
	b.localGatewayVirtualInterfaceGroups.Put(&cp)

	out := cp

	return &out, nil
}

// ---- Local Gateway Virtual Interface Groups (Create/Delete API) ----

func localGatewayVifGroupArn(b *InMemoryBackend, id string) string {
	return "arn:aws:ec2:" + b.Region + ":" + b.AccountID + ":local-gateway-virtual-interface-group/" + id
}

// CreateLocalGatewayVirtualInterfaceGroup creates a virtual interface group on a real
// local gateway.
func (b *InMemoryBackend) CreateLocalGatewayVirtualInterfaceGroup(
	localGatewayID string,
	localBgpAsn int32,
	localBgpAsnExtended int64,
	tags map[string]string,
) (*LocalGatewayVirtualInterfaceGroup, error) {
	if localGatewayID == "" {
		return nil, fmt.Errorf("%w: LocalGatewayId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateLocalGatewayVirtualInterfaceGroup")
	defer b.mu.Unlock()

	if _, ok := b.localGateways.Get(localGatewayID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrInvalidParameter, localGatewayID)
	}

	group := &LocalGatewayVirtualInterfaceGroup{
		LocalGatewayVirtualInterfaceGroupID: "lgw-vif-grp-" + uuid.New().String()[:17],
		LocalGatewayID:                      localGatewayID,
		ConfigurationState:                  stateAvailable,
		OwnerID:                             b.AccountID,
		LocalBgpAsn:                         localBgpAsn,
		LocalBgpAsnExtended:                 localBgpAsnExtended,
		Tags:                                maps.Clone(tags),
	}
	group.LocalGatewayVirtualInterfaceGroupArn = localGatewayVifGroupArn(b, group.LocalGatewayVirtualInterfaceGroupID)
	b.localGatewayVirtualInterfaceGroups.Put(group)

	cp := *group
	cp.Tags = maps.Clone(group.Tags)

	return &cp, nil
}

// DeleteLocalGatewayVirtualInterfaceGroup removes a virtual interface group. It fails
// if any virtual interfaces still reference the group, matching AWS's dependency
// check.
func (b *InMemoryBackend) DeleteLocalGatewayVirtualInterfaceGroup(
	id string,
) (*LocalGatewayVirtualInterfaceGroup, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: LocalGatewayVirtualInterfaceGroupId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteLocalGatewayVirtualInterfaceGroup")
	defer b.mu.Unlock()

	group, ok := b.localGatewayVirtualInterfaceGroups.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrLocalGatewayVifGroupNotFound, id)
	}

	for _, vif := range b.localGatewayVirtualInterfaces.All() {
		if vif.LocalGatewayVirtualInterfaceGroupID == id {
			return nil, fmt.Errorf(
				"%w: %s has associated local gateway virtual interfaces", ErrDependencyViolation, id,
			)
		}
	}

	deleted := *group
	deleted.Tags = maps.Clone(group.Tags)
	deleted.ConfigurationState = localGatewayRouteStateDeleted
	b.localGatewayVirtualInterfaceGroups.Delete(id)
	delete(b.tags, id)

	return &deleted, nil
}

// ---- Local Gateway Virtual Interfaces (Create/Delete API) ----

func localGatewayVifArn(b *InMemoryBackend, id string) string {
	return "arn:aws:ec2:" + b.Region + ":" + b.AccountID + ":local-gateway-virtual-interface/" + id
}

// LocalGatewayVirtualInterfaceParams holds the parameters accepted by
// CreateLocalGatewayVirtualInterface.
type LocalGatewayVirtualInterfaceParams struct {
	Tags                                map[string]string
	LocalGatewayVirtualInterfaceGroupID string
	OutpostLagID                        string
	LocalAddress                        string
	PeerAddress                         string
	PeerBgpAsnExtended                  int64
	Vlan                                int32
	PeerBgpAsn                          int32
}

// CreateLocalGatewayVirtualInterface creates a virtual interface within a real local
// gateway virtual interface group. The created VIF's ID is appended to the group's
// LocalGatewayVirtualInterfaceIDs, mirroring AWS's DescribeLocalGatewayVirtualInterfaceGroups
// behavior.
func (b *InMemoryBackend) CreateLocalGatewayVirtualInterface(
	p LocalGatewayVirtualInterfaceParams,
) (*LocalGatewayVirtualInterface, error) {
	if p.LocalGatewayVirtualInterfaceGroupID == "" {
		return nil, fmt.Errorf(
			"%w: LocalGatewayVirtualInterfaceGroupId is required", ErrInvalidParameter,
		)
	}

	if p.OutpostLagID == "" {
		return nil, fmt.Errorf("%w: OutpostLagId is required", ErrInvalidParameter)
	}

	if p.LocalAddress == "" || p.PeerAddress == "" {
		return nil, fmt.Errorf("%w: LocalAddress and PeerAddress are required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateLocalGatewayVirtualInterface")
	defer b.mu.Unlock()

	group, ok := b.localGatewayVirtualInterfaceGroups.Get(p.LocalGatewayVirtualInterfaceGroupID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrLocalGatewayVifGroupNotFound, p.LocalGatewayVirtualInterfaceGroupID)
	}

	vif := &LocalGatewayVirtualInterface{
		LocalGatewayVirtualInterfaceID:      "lgw-vif-" + uuid.New().String()[:17],
		LocalGatewayID:                      group.LocalGatewayID,
		LocalGatewayVirtualInterfaceGroupID: group.LocalGatewayVirtualInterfaceGroupID,
		OutpostLagID:                        p.OutpostLagID,
		ConfigurationState:                  stateAvailable,
		LocalAddress:                        p.LocalAddress,
		PeerAddress:                         p.PeerAddress,
		Vlan:                                p.Vlan,
		PeerBgpAsn:                          p.PeerBgpAsn,
		PeerBgpAsnExtended:                  p.PeerBgpAsnExtended,
		LocalBgpAsn:                         group.LocalBgpAsn,
		OwnerID:                             b.AccountID,
		Tags:                                maps.Clone(p.Tags),
	}
	vif.LocalGatewayVirtualInterfaceArn = localGatewayVifArn(b, vif.LocalGatewayVirtualInterfaceID)
	b.localGatewayVirtualInterfaces.Put(vif)

	group.LocalGatewayVirtualInterfaceIDs = append(
		group.LocalGatewayVirtualInterfaceIDs, vif.LocalGatewayVirtualInterfaceID,
	)

	cp := *vif
	cp.Tags = maps.Clone(vif.Tags)

	return &cp, nil
}

// DeleteLocalGatewayVirtualInterface removes a virtual interface, also removing its ID
// from its group's LocalGatewayVirtualInterfaceIDs.
func (b *InMemoryBackend) DeleteLocalGatewayVirtualInterface(
	id string,
) (*LocalGatewayVirtualInterface, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: LocalGatewayVirtualInterfaceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteLocalGatewayVirtualInterface")
	defer b.mu.Unlock()

	vif, ok := b.localGatewayVirtualInterfaces.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrLocalGatewayVifNotFound, id)
	}

	if group, groupOK := b.localGatewayVirtualInterfaceGroups.Get(vif.LocalGatewayVirtualInterfaceGroupID); groupOK {
		group.LocalGatewayVirtualInterfaceIDs = removeString(
			group.LocalGatewayVirtualInterfaceIDs, id,
		)
	}

	deleted := *vif
	deleted.Tags = maps.Clone(vif.Tags)
	deleted.ConfigurationState = localGatewayRouteStateDeleted
	b.localGatewayVirtualInterfaces.Delete(id)
	delete(b.tags, id)

	return &deleted, nil
}

// ---- Describe (no Create API for LocalGateway itself; see above for VIF/Group) ----

// DescribeLocalGateways returns local gateways, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeLocalGateways(ids []string) []*LocalGateway {
	b.mu.RLock("DescribeLocalGateways")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*LocalGateway, 0, b.localGateways.Len())

	for _, lg := range b.localGateways.All() {
		if len(idSet) > 0 && !idSet[lg.LocalGatewayID] {
			continue
		}

		cp := *lg
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].LocalGatewayID < out[j].LocalGatewayID })

	return out
}

// DescribeLocalGatewayVirtualInterfaces returns local gateway virtual interfaces,
// optionally filtered by IDs.
func (b *InMemoryBackend) DescribeLocalGatewayVirtualInterfaces(
	ids []string,
) []*LocalGatewayVirtualInterface {
	b.mu.RLock("DescribeLocalGatewayVirtualInterfaces")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*LocalGatewayVirtualInterface, 0, b.localGatewayVirtualInterfaces.Len())

	for _, vif := range b.localGatewayVirtualInterfaces.All() {
		if len(idSet) > 0 && !idSet[vif.LocalGatewayVirtualInterfaceID] {
			continue
		}

		cp := *vif
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].LocalGatewayVirtualInterfaceID < out[j].LocalGatewayVirtualInterfaceID
	})

	return out
}

// DescribeLocalGatewayVirtualInterfaceGroups returns local gateway virtual interface
// groups, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeLocalGatewayVirtualInterfaceGroups(
	ids []string,
) []*LocalGatewayVirtualInterfaceGroup {
	b.mu.RLock("DescribeLocalGatewayVirtualInterfaceGroups")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*LocalGatewayVirtualInterfaceGroup, 0, b.localGatewayVirtualInterfaceGroups.Len())

	for _, group := range b.localGatewayVirtualInterfaceGroups.All() {
		if len(idSet) > 0 && !idSet[group.LocalGatewayVirtualInterfaceGroupID] {
			continue
		}

		cp := *group
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].LocalGatewayVirtualInterfaceGroupID < out[j].LocalGatewayVirtualInterfaceGroupID
	})

	return out
}

// ---- Local Gateway Route Tables ----

func localGatewayRouteTableArn(b *InMemoryBackend, id string) string {
	return "arn:aws:ec2:" + b.Region + ":" + b.AccountID + ":local-gateway-route-table/" + id
}

// CreateLocalGatewayRouteTable creates a new local gateway route table.
func (b *InMemoryBackend) CreateLocalGatewayRouteTable(
	localGatewayID, mode string,
) (*LocalGatewayRouteTable, error) {
	if localGatewayID == "" {
		return nil, fmt.Errorf("%w: LocalGatewayId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateLocalGatewayRouteTable")
	defer b.mu.Unlock()

	lg, ok := b.localGateways.Get(localGatewayID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrInvalidParameter, localGatewayID)
	}

	rt := &LocalGatewayRouteTable{
		LocalGatewayRouteTableID: "lgw-rtb-" + uuid.New().String()[:17],
		LocalGatewayID:           localGatewayID,
		OutpostArn:               lg.OutpostArn,
		Mode:                     mode,
		State:                    stateAvailable,
		OwnerID:                  b.AccountID,
	}
	rt.LocalGatewayRouteTableArn = localGatewayRouteTableArn(b, rt.LocalGatewayRouteTableID)
	b.localGatewayRouteTables.Put(rt)

	cp := *rt

	return &cp, nil
}

// DescribeLocalGatewayRouteTables returns local gateway route tables, optionally
// filtered by IDs.
func (b *InMemoryBackend) DescribeLocalGatewayRouteTables(ids []string) []*LocalGatewayRouteTable {
	b.mu.RLock("DescribeLocalGatewayRouteTables")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*LocalGatewayRouteTable, 0, b.localGatewayRouteTables.Len())

	for _, rt := range b.localGatewayRouteTables.All() {
		if len(idSet) > 0 && !idSet[rt.LocalGatewayRouteTableID] {
			continue
		}

		cp := *rt
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].LocalGatewayRouteTableID < out[j].LocalGatewayRouteTableID
	})

	return out
}

// DeleteLocalGatewayRouteTable removes a local gateway route table.
func (b *InMemoryBackend) DeleteLocalGatewayRouteTable(id string) error {
	if id == "" {
		return fmt.Errorf("%w: LocalGatewayRouteTableId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteLocalGatewayRouteTable")
	defer b.mu.Unlock()

	if _, ok := b.localGatewayRouteTables.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrLocalGatewayRouteTableNotFound, id)
	}
	b.localGatewayRouteTables.Delete(id)
	delete(b.tags, id)

	return nil
}

// ---- Local Gateway Routes ----

func localGatewayRouteKey(routeTableID, destinationCIDR, destinationPrefixListID string) string {
	if destinationPrefixListID != "" {
		return routeTableID + ":" + destinationPrefixListID
	}

	return routeTableID + ":" + destinationCIDR
}

// requireLocalGatewayRouteTable looks up a local gateway route table by ID, returning
// ErrLocalGatewayRouteTableNotFound if it does not exist. Must be called with b.mu held.
func (b *InMemoryBackend) requireLocalGatewayRouteTable(routeTableID string) (*LocalGatewayRouteTable, error) {
	rt, ok := b.localGatewayRouteTables.Get(routeTableID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrLocalGatewayRouteTableNotFound, routeTableID)
	}

	return rt, nil
}

// CreateLocalGatewayRoute adds a static route to a local gateway route table.
func (b *InMemoryBackend) CreateLocalGatewayRoute(
	routeTableID, destinationCIDR, destinationPrefixListID, vifGroupID, eniID string,
) (*LocalGatewayRoute, error) {
	if routeTableID == "" {
		return nil, fmt.Errorf("%w: LocalGatewayRouteTableId is required", ErrInvalidParameter)
	}

	if destinationCIDR == "" && destinationPrefixListID == "" {
		return nil, fmt.Errorf(
			"%w: DestinationCidrBlock or DestinationPrefixListId is required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("CreateLocalGatewayRoute")
	defer b.mu.Unlock()

	rt, err := b.requireLocalGatewayRouteTable(routeTableID)
	if err != nil {
		return nil, err
	}

	key := localGatewayRouteKey(routeTableID, destinationCIDR, destinationPrefixListID)
	if _, exists := b.localGatewayRoutes.Get(key); exists {
		return nil, fmt.Errorf("%w: route already exists in %s", ErrInvalidParameter, routeTableID)
	}

	route := &LocalGatewayRoute{
		LocalGatewayRouteTableID:            routeTableID,
		LocalGatewayRouteTableArn:           rt.LocalGatewayRouteTableArn,
		DestinationCidrBlock:                destinationCIDR,
		DestinationPrefixListID:             destinationPrefixListID,
		LocalGatewayVirtualInterfaceGroupID: vifGroupID,
		NetworkInterfaceID:                  eniID,
		Type:                                localGatewayRouteTypeStatic,
		State:                               localGatewayRouteStateActive,
		OwnerID:                             b.AccountID,
	}
	b.localGatewayRoutes.Put(route)

	cp := *route

	return &cp, nil
}

// DeleteLocalGatewayRoute removes a route from a local gateway route table.
func (b *InMemoryBackend) DeleteLocalGatewayRoute(
	routeTableID, destinationCIDR, destinationPrefixListID string,
) (*LocalGatewayRoute, error) {
	if routeTableID == "" {
		return nil, fmt.Errorf("%w: LocalGatewayRouteTableId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteLocalGatewayRoute")
	defer b.mu.Unlock()

	key := localGatewayRouteKey(routeTableID, destinationCIDR, destinationPrefixListID)

	route, ok := b.localGatewayRoutes.Get(key)
	if !ok {
		return nil, fmt.Errorf(
			"%w: route %s in %s not found",
			ErrLocalGatewayRouteNotFound,
			destinationCIDR,
			routeTableID,
		)
	}

	deleted := *route
	deleted.State = localGatewayRouteStateDeleted
	b.localGatewayRoutes.Delete(key)

	return &deleted, nil
}

// ModifyLocalGatewayRoute updates the target of an existing local gateway route.
func (b *InMemoryBackend) ModifyLocalGatewayRoute(
	routeTableID, destinationCIDR, destinationPrefixListID, vifGroupID, eniID string,
) (*LocalGatewayRoute, error) {
	if routeTableID == "" {
		return nil, fmt.Errorf("%w: LocalGatewayRouteTableId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyLocalGatewayRoute")
	defer b.mu.Unlock()

	key := localGatewayRouteKey(routeTableID, destinationCIDR, destinationPrefixListID)

	existing, ok := b.localGatewayRoutes.Get(key)
	if !ok {
		return nil, fmt.Errorf(
			"%w: route %s in %s not found",
			ErrLocalGatewayRouteNotFound,
			destinationCIDR,
			routeTableID,
		)
	}

	updated := *existing
	if vifGroupID != "" {
		updated.LocalGatewayVirtualInterfaceGroupID = vifGroupID
		updated.NetworkInterfaceID = ""
	}

	if eniID != "" {
		updated.NetworkInterfaceID = eniID
		updated.LocalGatewayVirtualInterfaceGroupID = ""
	}
	b.localGatewayRoutes.Put(&updated)

	cp := updated

	return &cp, nil
}

// SearchLocalGatewayRoutes returns routes in a local gateway route table, optionally
// filtered by state.
func (b *InMemoryBackend) SearchLocalGatewayRoutes(
	routeTableID string,
	states []string,
) ([]*LocalGatewayRoute, error) {
	if routeTableID == "" {
		return nil, fmt.Errorf("%w: LocalGatewayRouteTableId is required", ErrInvalidParameter)
	}

	b.mu.RLock("SearchLocalGatewayRoutes")
	defer b.mu.RUnlock()

	if _, err := b.requireLocalGatewayRouteTable(routeTableID); err != nil {
		return nil, err
	}

	stateSet := make(map[string]bool, len(states))
	for _, s := range states {
		stateSet[s] = true
	}

	out := make([]*LocalGatewayRoute, 0)

	for _, r := range b.localGatewayRoutes.All() {
		if r.LocalGatewayRouteTableID != routeTableID {
			continue
		}

		if len(stateSet) > 0 && !stateSet[r.State] {
			continue
		}

		cp := *r
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].DestinationCidrBlock < out[j].DestinationCidrBlock
	})

	return out, nil
}

// createLocalGatewayAssociation runs the lock + route-table-lookup steps shared by the
// local gateway route table VPC and virtual-interface-group association Create
// operations, then delegates construction and storage of the association to build.
func createLocalGatewayAssociation[T any](
	b *InMemoryBackend,
	lockName, routeTableID string,
	build func(rt *LocalGatewayRouteTable) (*T, error),
) (*T, error) {
	if routeTableID == "" {
		return nil, fmt.Errorf("%w: LocalGatewayRouteTableId is required", ErrInvalidParameter)
	}

	b.mu.Lock(lockName)
	defer b.mu.Unlock()

	rt, err := b.requireLocalGatewayRouteTable(routeTableID)
	if err != nil {
		return nil, err
	}

	return build(rt)
}

// ---- Local Gateway Route Table VPC Associations ----

// CreateLocalGatewayRouteTableVpcAssociation associates a VPC with a local gateway
// route table.
func (b *InMemoryBackend) CreateLocalGatewayRouteTableVpcAssociation(
	routeTableID, vpcID string,
) (*LocalGatewayRouteTableVpcAssociation, error) {
	if vpcID == "" {
		return nil, fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}

	return createLocalGatewayAssociation(
		b,
		"CreateLocalGatewayRouteTableVpcAssociation",
		routeTableID,
		func(rt *LocalGatewayRouteTable) (*LocalGatewayRouteTableVpcAssociation, error) {
			if _, vpcExists := b.vpcs.Get(vpcID); !vpcExists {
				return nil, fmt.Errorf("%w: %s", ErrVPCNotFound, vpcID)
			}

			assoc := &LocalGatewayRouteTableVpcAssociation{
				LocalGatewayRouteTableVpcAssociationID: "lgw-route-table-vpc-assoc-" + uuid.New().String()[:17],
				LocalGatewayRouteTableID:               routeTableID,
				LocalGatewayRouteTableArn:              rt.LocalGatewayRouteTableArn,
				LocalGatewayID:                         rt.LocalGatewayID,
				VpcID:                                  vpcID,
				State:                                  localGatewayAssocStateAssoc,
				OwnerID:                                b.AccountID,
			}
			b.localGatewayRouteTableVpcAssociations.Put(assoc)

			cp := *assoc

			return &cp, nil
		},
	)
}

// DeleteLocalGatewayRouteTableVpcAssociation removes a VPC association from a local
// gateway route table.
func (b *InMemoryBackend) DeleteLocalGatewayRouteTableVpcAssociation(
	id string,
) (*LocalGatewayRouteTableVpcAssociation, error) {
	if id == "" {
		return nil, fmt.Errorf(
			"%w: LocalGatewayRouteTableVpcAssociationId is required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("DeleteLocalGatewayRouteTableVpcAssociation")
	defer b.mu.Unlock()

	assoc, ok := b.localGatewayRouteTableVpcAssociations.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrLocalGatewayVpcAssociationNotFound, id)
	}

	deleted := *assoc
	deleted.State = localGatewayAssocStateDisassoc
	b.localGatewayRouteTableVpcAssociations.Delete(id)
	delete(b.tags, id)

	return &deleted, nil
}

// DescribeLocalGatewayRouteTableVpcAssociations returns local gateway route table VPC
// associations, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeLocalGatewayRouteTableVpcAssociations(
	ids []string,
) []*LocalGatewayRouteTableVpcAssociation {
	b.mu.RLock("DescribeLocalGatewayRouteTableVpcAssociations")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*LocalGatewayRouteTableVpcAssociation, 0, b.localGatewayRouteTableVpcAssociations.Len())

	for _, a := range b.localGatewayRouteTableVpcAssociations.All() {
		if len(idSet) > 0 && !idSet[a.LocalGatewayRouteTableVpcAssociationID] {
			continue
		}

		cp := *a
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].LocalGatewayRouteTableVpcAssociationID < out[j].LocalGatewayRouteTableVpcAssociationID
	})

	return out
}

// ---- Local Gateway Route Table Virtual Interface Group Associations ----

// CreateLocalGatewayRouteTableVirtualInterfaceGroupAssociation associates a virtual
// interface group with a local gateway route table.
func (b *InMemoryBackend) CreateLocalGatewayRouteTableVirtualInterfaceGroupAssociation(
	routeTableID, vifGroupID string,
) (*LocalGatewayRouteTableVirtualInterfaceGroupAssociation, error) {
	if vifGroupID == "" {
		return nil, fmt.Errorf(
			"%w: LocalGatewayVirtualInterfaceGroupId is required",
			ErrInvalidParameter,
		)
	}

	return createLocalGatewayAssociation(
		b,
		"CreateLocalGatewayRouteTableVirtualInterfaceGroupAssociation",
		routeTableID,
		func(rt *LocalGatewayRouteTable) (*LocalGatewayRouteTableVirtualInterfaceGroupAssociation, error) {
			if _, groupExists := b.localGatewayVirtualInterfaceGroups.Get(vifGroupID); !groupExists {
				return nil, fmt.Errorf("%w: %s", ErrInvalidParameter, vifGroupID)
			}

			assoc := &LocalGatewayRouteTableVirtualInterfaceGroupAssociation{
				LocalGatewayRouteTableVirtualInterfaceGroupAssociationID: "lgw-route-table-virtual-interface-group-assoc-" +
					uuid.New().String()[:17],
				LocalGatewayRouteTableID:            routeTableID,
				LocalGatewayRouteTableArn:           rt.LocalGatewayRouteTableArn,
				LocalGatewayID:                      rt.LocalGatewayID,
				LocalGatewayVirtualInterfaceGroupID: vifGroupID,
				State:                               localGatewayAssocStateAssoc,
				OwnerID:                             b.AccountID,
			}
			b.localGatewayRouteTableVifGroupAssociations.Put(assoc)

			cp := *assoc

			return &cp, nil
		},
	)
}

// DeleteLocalGatewayRouteTableVirtualInterfaceGroupAssociation removes a virtual
// interface group association from a local gateway route table.
func (b *InMemoryBackend) DeleteLocalGatewayRouteTableVirtualInterfaceGroupAssociation(
	id string,
) (*LocalGatewayRouteTableVirtualInterfaceGroupAssociation, error) {
	if id == "" {
		return nil, fmt.Errorf(
			"%w: LocalGatewayRouteTableVirtualInterfaceGroupAssociationId is required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("DeleteLocalGatewayRouteTableVirtualInterfaceGroupAssociation")
	defer b.mu.Unlock()

	assoc, ok := b.localGatewayRouteTableVifGroupAssociations.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrLocalGatewayVifGroupAssociationNotFound, id)
	}

	deleted := *assoc
	deleted.State = localGatewayAssocStateDisassoc
	b.localGatewayRouteTableVifGroupAssociations.Delete(id)
	delete(b.tags, id)

	return &deleted, nil
}

// DescribeLocalGatewayRouteTableVirtualInterfaceGroupAssociations returns local gateway
// route table virtual interface group associations, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeLocalGatewayRouteTableVirtualInterfaceGroupAssociations(
	ids []string,
) []*LocalGatewayRouteTableVirtualInterfaceGroupAssociation {
	b.mu.RLock("DescribeLocalGatewayRouteTableVirtualInterfaceGroupAssociations")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make(
		[]*LocalGatewayRouteTableVirtualInterfaceGroupAssociation,
		0, b.localGatewayRouteTableVifGroupAssociations.Len(),
	)

	for _, a := range b.localGatewayRouteTableVifGroupAssociations.All() {
		if len(idSet) > 0 && !idSet[a.LocalGatewayRouteTableVirtualInterfaceGroupAssociationID] {
			continue
		}

		cp := *a
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].LocalGatewayRouteTableVirtualInterfaceGroupAssociationID <
			out[j].LocalGatewayRouteTableVirtualInterfaceGroupAssociationID
	})

	return out
}
