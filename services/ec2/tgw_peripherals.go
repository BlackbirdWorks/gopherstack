package ec2

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"time"
)

// ---- errors ----

var (
	// ErrTGWPolicyTableNotFound is returned when a TGW policy table ID does not exist.
	ErrTGWPolicyTableNotFound = errors.New("InvalidTransitGatewayPolicyTableId.NotFound")
	// ErrTGWRouteTableAnnouncementNotFound is returned when a TGW route table
	// announcement ID does not exist.
	ErrTGWRouteTableAnnouncementNotFound = errors.New(
		"InvalidTransitGatewayRouteTableAnnouncementId.NotFound",
	)
)

// ---- constants ----

const (
	tgwAssocStateAssociated    = "associated"
	tgwAssocStateDisassociated = "disassociated"
	tgwAttachmentStateRejected = "rejected"
	// tgwPolicyTableEntryStateActive mirrors the real AWS
	// TransitGatewayPolicyTableEntryState "active" value.
	tgwPolicyTableEntryStateActive = "active"
)

// ---- models ----

// TransitGatewayPolicyTable represents a TGW policy table.
type TransitGatewayPolicyTable struct {
	CreationTime                time.Time `json:"creationTime"`
	TransitGatewayPolicyTableID string    `json:"transitGatewayPolicyTableID,omitempty"`
	TransitGatewayID            string    `json:"transitGatewayID,omitempty"`
	State                       string    `json:"state,omitempty"`
}

// TransitGatewayPolicyTableAssociation represents an association between a
// TGW policy table and an attachment.
type TransitGatewayPolicyTableAssociation struct {
	TransitGatewayPolicyTableID string `json:"transitGatewayPolicyTableID,omitempty"`
	TransitGatewayAttachmentID  string `json:"transitGatewayAttachmentID,omitempty"`
	ResourceID                  string `json:"resourceID,omitempty"`
	ResourceType                string `json:"resourceType,omitempty"`
	State                       string `json:"state,omitempty"`
}

// TransitGatewayPolicyTableEntry represents a single traffic-matching rule
// within a TGW policy table, directing matching traffic to a target transit
// gateway route table. Mirrors the real AWS TransitGatewayPolicyTableEntry
// shape (types.TransitGatewayPolicyTableEntry / TransitGatewayPolicyRule).
type TransitGatewayPolicyTableEntry struct {
	TransitGatewayPolicyTableID string `json:"transitGatewayPolicyTableID,omitempty"`
	TargetRouteTableID          string `json:"targetRouteTableID,omitempty"`
	State                       string `json:"state,omitempty"`
	SourceCidrBlock             string `json:"sourceCidrBlock,omitempty"`
	SourcePortRange             string `json:"sourcePortRange,omitempty"`
	DestinationCidrBlock        string `json:"destinationCidrBlock,omitempty"`
	DestinationPortRange        string `json:"destinationPortRange,omitempty"`
	Protocol                    string `json:"protocol,omitempty"`
	MetaDataKey                 string `json:"metaDataKey,omitempty"`
	MetaDataValue               string `json:"metaDataValue,omitempty"`
	PolicyRuleNumber            int    `json:"policyRuleNumber,omitempty"`
}

// TransitGatewayRouteTableAnnouncement represents a TGW route table
// announcement across a peering attachment.
type TransitGatewayRouteTableAnnouncement struct {
	CreationTime                           time.Time `json:"creationTime"`
	TransitGatewayRouteTableAnnouncementID string    `json:"transitGatewayRouteTableAnnouncementID,omitempty"`
	TransitGatewayID                       string    `json:"transitGatewayID,omitempty"`
	TransitGatewayRouteTableID             string    `json:"transitGatewayRouteTableID,omitempty"`
	PeeringAttachmentID                    string    `json:"peeringAttachmentID,omitempty"`
	AnnouncementDirection                  string    `json:"announcementDirection,omitempty"`
	State                                  string    `json:"state,omitempty"`
	// PeerTransitGatewayID is the transit gateway on the other side of the
	// peering attachment (real AWS PeerTransitGatewayId), derived from the
	// peering attachment's Requester/Accepter transit gateway IDs.
	PeerTransitGatewayID string `json:"peerTransitGatewayID,omitempty"`
}

// TransitGatewayRouteTablePropagation represents an attachment enabled (via
// EnableTransitGatewayRouteTablePropagation, in transit_gateways.go) to
// propagate routes into a TGW route table.
type TransitGatewayRouteTablePropagation struct {
	ResourceID                             string `json:"resourceID,omitempty"`
	ResourceType                           string `json:"resourceType,omitempty"`
	State                                  string `json:"state,omitempty"`
	TransitGatewayAttachmentID             string `json:"transitGatewayAttachmentID,omitempty"`
	TransitGatewayRouteTableAnnouncementID string `json:"transitGatewayRouteTableAnnouncementID,omitempty"`
}

// TransitGatewayAttachmentPropagation represents a route table an attachment
// propagates routes into.
type TransitGatewayAttachmentPropagation struct {
	TransitGatewayRouteTableID string `json:"transitGatewayRouteTableID,omitempty"`
	State                      string `json:"state,omitempty"`
}

// ---- Transit Gateway Policy Tables ----

// CreateTransitGatewayPolicyTable creates a new policy table on the given
// transit gateway.
func (b *InMemoryBackend) CreateTransitGatewayPolicyTable(
	tgwID string,
) (*TransitGatewayPolicyTable, error) {
	if tgwID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateTransitGatewayPolicyTable")
	defer b.mu.Unlock()

	if _, ok := b.transitGateways.Get(tgwID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrTransitGatewayNotFound, tgwID)
	}

	pt := &TransitGatewayPolicyTable{
		TransitGatewayPolicyTableID: newTransitGatewayPolicyTableID(),
		TransitGatewayID:            tgwID,
		State:                       stateAvailable,
		CreationTime:                time.Now().UTC(),
	}
	b.tgwPolicyTables.Put(pt)

	cp := *pt

	return &cp, nil
}

// DescribeTransitGatewayPolicyTables returns policy tables, optionally
// filtered by ID.
func (b *InMemoryBackend) DescribeTransitGatewayPolicyTables(
	ids []string,
) []*TransitGatewayPolicyTable {
	b.mu.RLock("DescribeTransitGatewayPolicyTables")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*TransitGatewayPolicyTable, 0, b.tgwPolicyTables.Len())

	for _, pt := range b.tgwPolicyTables.All() {
		if len(idSet) > 0 && !idSet[pt.TransitGatewayPolicyTableID] {
			continue
		}

		cp := *pt
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].TransitGatewayPolicyTableID < out[j].TransitGatewayPolicyTableID
	})

	return out
}

// DeleteTransitGatewayPolicyTable removes a policy table along with any
// attachment associations tied to it.
func (b *InMemoryBackend) DeleteTransitGatewayPolicyTable(id string) error {
	if id == "" {
		return fmt.Errorf("%w: TransitGatewayPolicyTableId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteTransitGatewayPolicyTable")
	defer b.mu.Unlock()

	if _, ok := b.tgwPolicyTables.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrTGWPolicyTableNotFound, id)
	}
	b.tgwPolicyTables.Delete(id)
	delete(b.tags, id)

	for _, assoc := range b.tgwPolicyTableAssociations.All() {
		key := tgwPolicyTableAssociationsKeyFn(assoc)
		if assoc.TransitGatewayPolicyTableID == id {
			b.tgwPolicyTableAssociations.Delete(key)
		}
	}

	for _, entry := range b.tgwPolicyTableEntries.All() {
		if entry.TransitGatewayPolicyTableID == id {
			b.tgwPolicyTableEntries.Delete(tgwPolicyTableEntriesKeyFn(entry))
		}
	}

	return nil
}

// ---- Transit Gateway Policy Table associations ----

// AssociateTransitGatewayPolicyTable associates a TGW attachment with a
// policy table.
func (b *InMemoryBackend) AssociateTransitGatewayPolicyTable(
	policyTableID, attachmentID string,
) (*TransitGatewayPolicyTableAssociation, error) {
	if policyTableID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayPolicyTableId is required", ErrInvalidParameter)
	}

	if attachmentID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayAttachmentId is required", ErrInvalidParameter)
	}

	b.mu.Lock("AssociateTransitGatewayPolicyTable")
	defer b.mu.Unlock()

	if _, ok := b.tgwPolicyTables.Get(policyTableID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrTGWPolicyTableNotFound, policyTableID)
	}

	assoc := &TransitGatewayPolicyTableAssociation{
		TransitGatewayPolicyTableID: policyTableID,
		TransitGatewayAttachmentID:  attachmentID,
		ResourceType:                tgwResourceTypeVPC,
		State:                       tgwAssocStateAssociated,
	}
	b.tgwPolicyTableAssociations.Put(assoc)

	cp := *assoc

	return &cp, nil
}

// DisassociateTransitGatewayPolicyTable removes an association between a TGW
// attachment and a policy table.
func (b *InMemoryBackend) DisassociateTransitGatewayPolicyTable(
	policyTableID, attachmentID string,
) (*TransitGatewayPolicyTableAssociation, error) {
	if policyTableID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayPolicyTableId is required", ErrInvalidParameter)
	}

	if attachmentID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayAttachmentId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DisassociateTransitGatewayPolicyTable")
	defer b.mu.Unlock()

	key := policyTableID + ":" + attachmentID
	assoc, ok := b.tgwPolicyTableAssociations.Get(key)
	if !ok {
		return nil, fmt.Errorf(
			"%w: association between %s and %s not found",
			ErrInvalidParameter,
			policyTableID,
			attachmentID,
		)
	}
	b.tgwPolicyTableAssociations.Delete(key)

	cp := *assoc
	cp.State = tgwAssocStateDisassociated

	return &cp, nil
}

// GetTransitGatewayPolicyTableAssociations returns the attachment
// associations for a policy table.
func (b *InMemoryBackend) GetTransitGatewayPolicyTableAssociations(
	policyTableID string,
) []*TransitGatewayPolicyTableAssociation {
	b.mu.RLock("GetTransitGatewayPolicyTableAssociations")
	defer b.mu.RUnlock()

	out := make([]*TransitGatewayPolicyTableAssociation, 0)

	for _, assoc := range b.tgwPolicyTableAssociations.All() {
		if policyTableID != "" && assoc.TransitGatewayPolicyTableID != policyTableID {
			continue
		}

		cp := *assoc
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].TransitGatewayAttachmentID < out[j].TransitGatewayAttachmentID
	})

	return out
}

// GetTransitGatewayPolicyTableEntries validates that a policy table exists
// and returns the entries created against it via
// CreateTransitGatewayPolicyTableEntry.
func (b *InMemoryBackend) GetTransitGatewayPolicyTableEntries(
	policyTableID string,
) ([]*TransitGatewayPolicyTableEntry, error) {
	if policyTableID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayPolicyTableId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetTransitGatewayPolicyTableEntries")
	defer b.mu.RUnlock()

	if _, ok := b.tgwPolicyTables.Get(policyTableID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrTGWPolicyTableNotFound, policyTableID)
	}

	out := make([]*TransitGatewayPolicyTableEntry, 0)

	for _, e := range b.tgwPolicyTableEntries.All() {
		if e.TransitGatewayPolicyTableID != policyTableID {
			continue
		}

		cp := *e
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].PolicyRuleNumber < out[j].PolicyRuleNumber
	})

	return out, nil
}

// CreateTransitGatewayPolicyTableEntry adds a traffic-matching rule to a
// policy table. TransitGatewayPolicyTableID and State are set by this call;
// the caller fills in PolicyRuleNumber, TargetRouteTableID, and the
// rule-matching fields.
func (b *InMemoryBackend) CreateTransitGatewayPolicyTableEntry(
	policyTableID string,
	entry *TransitGatewayPolicyTableEntry,
) (*TransitGatewayPolicyTableEntry, error) {
	if policyTableID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayPolicyTableId is required", ErrInvalidParameter)
	}

	if entry == nil || entry.PolicyRuleNumber <= 0 {
		return nil, fmt.Errorf("%w: PolicyRuleNumber is required", ErrInvalidParameter)
	}

	if entry.TargetRouteTableID == "" {
		return nil, fmt.Errorf("%w: TargetRouteTableId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateTransitGatewayPolicyTableEntry")
	defer b.mu.Unlock()

	if _, ok := b.tgwPolicyTables.Get(policyTableID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrTGWPolicyTableNotFound, policyTableID)
	}

	if _, ok := b.tgwRouteTables.Get(entry.TargetRouteTableID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrTGWRouteTableNotFound, entry.TargetRouteTableID)
	}

	stored := *entry
	stored.TransitGatewayPolicyTableID = policyTableID
	stored.State = tgwPolicyTableEntryStateActive

	b.tgwPolicyTableEntries.Put(&stored)

	cp := stored

	return &cp, nil
}

// ModifyTransitGatewayPolicyTableEntry updates the target route table and/or
// matching rule of an existing policy table entry. Fields left unset in
// updates retain their current stored value (real API: "Unspecified fields
// retain their current values").
func (b *InMemoryBackend) ModifyTransitGatewayPolicyTableEntry(
	policyTableID string,
	ruleNumber int,
	updates *TransitGatewayPolicyTableEntry,
) (*TransitGatewayPolicyTableEntry, error) {
	if policyTableID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayPolicyTableId is required", ErrInvalidParameter)
	}

	if ruleNumber <= 0 {
		return nil, fmt.Errorf("%w: PolicyRuleNumber is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyTransitGatewayPolicyTableEntry")
	defer b.mu.Unlock()

	if _, ok := b.tgwPolicyTables.Get(policyTableID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrTGWPolicyTableNotFound, policyTableID)
	}

	key := policyTableID + ":" + strconv.Itoa(ruleNumber)

	entry, ok := b.tgwPolicyTableEntries.Get(key)
	if !ok {
		return nil, fmt.Errorf(
			"%w: policy table entry %d in %s not found",
			ErrInvalidParameter,
			ruleNumber,
			policyTableID,
		)
	}

	if updates != nil {
		if updates.TargetRouteTableID != "" {
			if _, rtOK := b.tgwRouteTables.Get(updates.TargetRouteTableID); !rtOK {
				return nil, fmt.Errorf("%w: %s", ErrTGWRouteTableNotFound, updates.TargetRouteTableID)
			}

			entry.TargetRouteTableID = updates.TargetRouteTableID
		}

		applyTGWPolicyRuleUpdates(entry, updates)
	}

	cp := *entry

	return &cp, nil
}

// applyTGWPolicyRuleUpdates copies non-empty PolicyRule-matching fields from
// updates onto entry, leaving fields entry already has unset in updates
// untouched.
func applyTGWPolicyRuleUpdates(entry, updates *TransitGatewayPolicyTableEntry) {
	if updates.SourceCidrBlock != "" {
		entry.SourceCidrBlock = updates.SourceCidrBlock
	}

	if updates.SourcePortRange != "" {
		entry.SourcePortRange = updates.SourcePortRange
	}

	if updates.DestinationCidrBlock != "" {
		entry.DestinationCidrBlock = updates.DestinationCidrBlock
	}

	if updates.DestinationPortRange != "" {
		entry.DestinationPortRange = updates.DestinationPortRange
	}

	if updates.Protocol != "" {
		entry.Protocol = updates.Protocol
	}

	if updates.MetaDataKey != "" {
		entry.MetaDataKey = updates.MetaDataKey
	}

	if updates.MetaDataValue != "" {
		entry.MetaDataValue = updates.MetaDataValue
	}
}

// DeleteTransitGatewayPolicyTableEntry removes a single rule from a policy
// table.
func (b *InMemoryBackend) DeleteTransitGatewayPolicyTableEntry(
	policyTableID string,
	ruleNumber int,
) (*TransitGatewayPolicyTableEntry, error) {
	if policyTableID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayPolicyTableId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteTransitGatewayPolicyTableEntry")
	defer b.mu.Unlock()

	if _, ok := b.tgwPolicyTables.Get(policyTableID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrTGWPolicyTableNotFound, policyTableID)
	}

	key := policyTableID + ":" + strconv.Itoa(ruleNumber)

	entry, ok := b.tgwPolicyTableEntries.Get(key)
	if !ok {
		return nil, fmt.Errorf(
			"%w: policy table entry %d in %s not found",
			ErrInvalidParameter,
			ruleNumber,
			policyTableID,
		)
	}
	b.tgwPolicyTableEntries.Delete(key)

	cp := *entry
	cp.State = tgwRouteStateDeleted

	return &cp, nil
}

// ---- Transit Gateway Route Table Announcements ----

// CreateTransitGatewayRouteTableAnnouncement announces a TGW route table
// across a peering attachment.
func (b *InMemoryBackend) CreateTransitGatewayRouteTableAnnouncement(
	routeTableID, peeringAttachmentID string,
) (*TransitGatewayRouteTableAnnouncement, error) {
	if routeTableID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayRouteTableId is required", ErrInvalidParameter)
	}

	if peeringAttachmentID == "" {
		return nil, fmt.Errorf("%w: PeeringAttachmentId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateTransitGatewayRouteTableAnnouncement")
	defer b.mu.Unlock()

	rt, ok := b.tgwRouteTables.Get(routeTableID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTGWRouteTableNotFound, routeTableID)
	}

	peeringAtt, attExists := b.tgwPeeringAttachments.Get(peeringAttachmentID)
	if !attExists {
		return nil, fmt.Errorf("%w: %s", ErrTransitGatewayAttachmentNotFound, peeringAttachmentID)
	}

	// The peer TGW is whichever side of the peering attachment is not this
	// route table's own transit gateway.
	peerTGWID := peeringAtt.AccepterTransitGatewayID
	if rt.TransitGatewayID == peeringAtt.AccepterTransitGatewayID {
		peerTGWID = peeringAtt.RequesterTransitGatewayID
	}

	ann := &TransitGatewayRouteTableAnnouncement{
		TransitGatewayRouteTableAnnouncementID: newTransitGatewayRouteTableAnnouncementID(),
		TransitGatewayID:                       rt.TransitGatewayID,
		TransitGatewayRouteTableID:             routeTableID,
		PeeringAttachmentID:                    peeringAttachmentID,
		AnnouncementDirection:                  "outgoing",
		State:                                  stateAvailable,
		CreationTime:                           time.Now().UTC(),
		PeerTransitGatewayID:                   peerTGWID,
	}
	b.tgwRouteTableAnnouncements.Put(ann)

	cp := *ann

	return &cp, nil
}

// DescribeTransitGatewayRouteTableAnnouncements returns route table
// announcements, optionally filtered by ID.
func (b *InMemoryBackend) DescribeTransitGatewayRouteTableAnnouncements(
	ids []string,
) []*TransitGatewayRouteTableAnnouncement {
	b.mu.RLock("DescribeTransitGatewayRouteTableAnnouncements")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*TransitGatewayRouteTableAnnouncement, 0, b.tgwRouteTableAnnouncements.Len())

	for _, ann := range b.tgwRouteTableAnnouncements.All() {
		if len(idSet) > 0 && !idSet[ann.TransitGatewayRouteTableAnnouncementID] {
			continue
		}

		cp := *ann
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].TransitGatewayRouteTableAnnouncementID <
			out[j].TransitGatewayRouteTableAnnouncementID
	})

	return out
}

// DeleteTransitGatewayRouteTableAnnouncement removes a route table
// announcement.
func (b *InMemoryBackend) DeleteTransitGatewayRouteTableAnnouncement(id string) error {
	if id == "" {
		return fmt.Errorf(
			"%w: TransitGatewayRouteTableAnnouncementId is required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("DeleteTransitGatewayRouteTableAnnouncement")
	defer b.mu.Unlock()

	if _, ok := b.tgwRouteTableAnnouncements.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrTGWRouteTableAnnouncementNotFound, id)
	}
	b.tgwRouteTableAnnouncements.Delete(id)
	delete(b.tags, id)

	return nil
}

// ---- Transit Gateway Route Table associations / propagations / search / export ----

// GetTransitGatewayRouteTableAssociations returns the attachment associations
// for a TGW route table, sourced from the same state AssociateTransitGatewayRouteTable
// and DisassociateTransitGatewayRouteTable maintain.
func (b *InMemoryBackend) GetTransitGatewayRouteTableAssociations(
	routeTableID string,
) ([]*TransitGatewayRouteTableAssociation, error) {
	if routeTableID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayRouteTableId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetTransitGatewayRouteTableAssociations")
	defer b.mu.RUnlock()

	if _, ok := b.tgwRouteTables.Get(routeTableID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrTGWRouteTableNotFound, routeTableID)
	}

	out := make([]*TransitGatewayRouteTableAssociation, 0)

	for _, a := range b.tgwRTAssociations.All() {
		if a.TransitGatewayRouteTableID != routeTableID {
			continue
		}

		cp := *a
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].TransitGatewayAttachmentID < out[j].TransitGatewayAttachmentID
	})

	return out, nil
}

// GetTransitGatewayRouteTablePropagations validates the route table exists
// and returns its propagations, sourced from the same state
// EnableTransitGatewayRouteTablePropagation and
// DisableTransitGatewayRouteTablePropagation maintain.
func (b *InMemoryBackend) GetTransitGatewayRouteTablePropagations(
	routeTableID string,
) ([]*TransitGatewayRouteTablePropagation, error) {
	if routeTableID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayRouteTableId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetTransitGatewayRouteTablePropagations")
	defer b.mu.RUnlock()

	if _, ok := b.tgwRouteTables.Get(routeTableID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrTGWRouteTableNotFound, routeTableID)
	}

	out := make([]*TransitGatewayRouteTablePropagation, 0, len(b.tgwRTPropagations[routeTableID]))
	for _, prop := range b.tgwRTPropagations[routeTableID] {
		cp := *prop
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].TransitGatewayAttachmentID < out[j].TransitGatewayAttachmentID
	})

	return out, nil
}

// transitGatewayAttachmentExistsLocked reports whether an attachment ID exists in
// any of the known TGW attachment maps. Must be called with b.mu held (read or
// write). Must stay in sync with tgwAttachmentResourceLocked's map set, or a real
// attachment ID gets misreported as ErrTGWAttachmentNotFound.
func (b *InMemoryBackend) transitGatewayAttachmentExistsLocked(id string) bool {
	if _, ok := b.tgwVpcAttachments.Get(id); ok {
		return true
	}

	if _, ok := b.tgwPeeringAttachments.Get(id); ok {
		return true
	}

	if _, ok := b.tgwConnects.Get(id); ok {
		return true
	}

	if _, ok := b.tgwClientVpnAttachments.Get(id); ok {
		return true
	}

	return false
}

// GetTransitGatewayAttachmentPropagations validates the attachment exists and
// returns its propagations, sourced from the same state
// EnableTransitGatewayRouteTablePropagation and
// DisableTransitGatewayRouteTablePropagation maintain.
func (b *InMemoryBackend) GetTransitGatewayAttachmentPropagations(
	attachmentID string,
) ([]*TransitGatewayAttachmentPropagation, error) {
	if attachmentID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayAttachmentId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetTransitGatewayAttachmentPropagations")
	defer b.mu.RUnlock()

	if !b.transitGatewayAttachmentExistsLocked(attachmentID) {
		return nil, fmt.Errorf("%w: %s", ErrTGWAttachmentNotFound, attachmentID)
	}

	out := make([]*TransitGatewayAttachmentPropagation, 0)

	for routeTableID, inner := range b.tgwRTPropagations {
		if prop, ok := inner[attachmentID]; ok {
			out = append(out, &TransitGatewayAttachmentPropagation{
				TransitGatewayRouteTableID: routeTableID,
				State:                      prop.State,
			})
		}
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].TransitGatewayRouteTableID < out[j].TransitGatewayRouteTableID
	})

	return out, nil
}

// matchesTGWRouteFilters reports whether route r matches all supplied
// SearchTransitGatewayRoutes/ExportTransitGatewayRoutes filters. Unrecognised
// filter names are ignored (permissive, matching this backend's minimal
// filter support elsewhere).
func matchesTGWRouteFilters(r *TransitGatewayRoute, filters map[string][]string) bool {
	for name, values := range filters {
		var actual string

		switch name {
		case "route-search.exact-match":
			actual = r.DestinationCidrBlock
		case "state":
			actual = r.State
		case "type":
			actual = r.Type
		case "attachment.transit-gateway-attachment-id":
			actual = r.TransitGatewayAttachmentID
		default:
			continue
		}

		if !slices.Contains(values, actual) {
			return false
		}
	}

	return true
}

// SearchTransitGatewayRoutes returns the routes in a TGW route table matching
// the given filters.
func (b *InMemoryBackend) SearchTransitGatewayRoutes(
	routeTableID string,
	filters map[string][]string,
) ([]*TransitGatewayRoute, error) {
	if routeTableID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayRouteTableId is required", ErrInvalidParameter)
	}

	b.mu.RLock("SearchTransitGatewayRoutes")
	defer b.mu.RUnlock()

	if _, ok := b.tgwRouteTables.Get(routeTableID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrTGWRouteTableNotFound, routeTableID)
	}

	out := make([]*TransitGatewayRoute, 0)

	for _, r := range b.tgwRoutes.All() {
		if r.TransitGatewayRouteTableID != routeTableID {
			continue
		}

		if !matchesTGWRouteFilters(r, filters) {
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

// ExportTransitGatewayRoutes validates the route table exists and returns the
// S3 URL the (simulated) export was written to.
func (b *InMemoryBackend) ExportTransitGatewayRoutes(routeTableID, s3Bucket string) (string, error) {
	if routeTableID == "" {
		return "", fmt.Errorf("%w: TransitGatewayRouteTableId is required", ErrInvalidParameter)
	}

	if s3Bucket == "" {
		return "", fmt.Errorf("%w: S3Bucket is required", ErrInvalidParameter)
	}

	b.mu.RLock("ExportTransitGatewayRoutes")
	defer b.mu.RUnlock()

	if _, ok := b.tgwRouteTables.Get(routeTableID); !ok {
		return "", fmt.Errorf("%w: %s", ErrTGWRouteTableNotFound, routeTableID)
	}

	return fmt.Sprintf(
		"s3://%s/VPCTransitGateway/TransitGatewayRouteTables/%s.csv",
		s3Bucket,
		routeTableID,
	), nil
}

// ---- Transit Gateway VPC attachment / metering policy / prefix list ref modifications ----

// ModifyTransitGatewayVpcAttachment adds and/or removes subnets from a TGW
// VPC attachment.
func (b *InMemoryBackend) ModifyTransitGatewayVpcAttachment(
	attachmentID string,
	addSubnetIDs, removeSubnetIDs []string,
) (*TransitGatewayVpcAttachment, error) {
	if attachmentID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayAttachmentId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyTransitGatewayVpcAttachment")
	defer b.mu.Unlock()

	att, ok := b.tgwVpcAttachments.Get(attachmentID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTGWAttachmentNotFound, attachmentID)
	}

	att.SubnetIDs = mergeSubnetIDs(att.SubnetIDs, addSubnetIDs, removeSubnetIDs)

	cp := *att
	cp.SubnetIDs = append([]string(nil), att.SubnetIDs...)

	return &cp, nil
}

// mergeSubnetIDs returns current with removeIDs removed and addIDs appended,
// de-duplicated.
func mergeSubnetIDs(current, addIDs, removeIDs []string) []string {
	removeSet := make(map[string]bool, len(removeIDs))
	for _, id := range removeIDs {
		removeSet[id] = true
	}

	kept := make([]string, 0, len(current))
	existing := make(map[string]bool, len(current))

	for _, id := range current {
		if removeSet[id] {
			continue
		}

		kept = append(kept, id)
		existing[id] = true
	}

	for _, id := range addIDs {
		if existing[id] {
			continue
		}

		kept = append(kept, id)
		existing[id] = true
	}

	return kept
}

// ModifyTransitGatewayMeteringPolicy adds and/or removes middlebox attachment
// IDs from a TGW metering policy.
func (b *InMemoryBackend) ModifyTransitGatewayMeteringPolicy(
	policyID string,
	addAttachmentIDs, removeAttachmentIDs []string,
) (*TransitGatewayMeteringPolicy, error) {
	if policyID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayMeteringPolicyId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyTransitGatewayMeteringPolicy")
	defer b.mu.Unlock()

	p, ok := b.tgwMeteringPolicies.Get(policyID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTGWMeteringPolicyNotFound, policyID)
	}

	p.MiddleboxAttachmentIDs = mergeSubnetIDs(
		p.MiddleboxAttachmentIDs,
		addAttachmentIDs,
		removeAttachmentIDs,
	)
	p.UpdateEffectiveAt = time.Now().UTC()

	return copyTGWMeteringPolicy(p), nil
}

// GetTransitGatewayMeteringPolicyEntries returns the entries for a TGW
// metering policy, sourced from the same state CreateTransitGatewayMeteringPolicyEntry
// and DeleteTransitGatewayMeteringPolicyEntry maintain.
func (b *InMemoryBackend) GetTransitGatewayMeteringPolicyEntries(
	policyID string,
) ([]*TransitGatewayMeteringPolicyEntry, error) {
	if policyID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayMeteringPolicyId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetTransitGatewayMeteringPolicyEntries")
	defer b.mu.RUnlock()

	if _, ok := b.tgwMeteringPolicies.Get(policyID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrTGWMeteringPolicyNotFound, policyID)
	}

	out := make([]*TransitGatewayMeteringPolicyEntry, 0)

	for _, e := range b.tgwMeteringPolicyEntries.All() {
		if e.TransitGatewayMeteringPolicyID != policyID {
			continue
		}

		cp := *e
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].PolicyRuleNumber < out[j].PolicyRuleNumber
	})

	return out, nil
}

// ---- Reject operations ----

// RejectTransitGatewayVpcAttachment rejects a pending TGW VPC attachment,
// transitioning its state to "rejected".
func (b *InMemoryBackend) RejectTransitGatewayVpcAttachment(
	attachmentID string,
) (*TransitGatewayVpcAttachment, error) {
	if attachmentID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayAttachmentId is required", ErrInvalidParameter)
	}

	b.mu.Lock("RejectTransitGatewayVpcAttachment")
	defer b.mu.Unlock()

	att, ok := b.tgwVpcAttachments.Get(attachmentID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTransitGatewayAttachmentNotFound, attachmentID)
	}

	att.State = tgwAttachmentStateRejected
	cp := *att

	return &cp, nil
}

// RejectTransitGatewayPeeringAttachment rejects a pending TGW peering
// attachment, transitioning its state to "rejected".
func (b *InMemoryBackend) RejectTransitGatewayPeeringAttachment(
	attachmentID string,
) (*TransitGatewayPeeringAttachment, error) {
	if attachmentID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayAttachmentId is required", ErrInvalidParameter)
	}

	b.mu.Lock("RejectTransitGatewayPeeringAttachment")
	defer b.mu.Unlock()

	att, ok := b.tgwPeeringAttachments.Get(attachmentID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTransitGatewayAttachmentNotFound, attachmentID)
	}

	att.State = tgwAttachmentStateRejected
	cp := *att

	return &cp, nil
}

// RejectTransitGatewayMulticastDomainAssociations rejects a request to
// associate subnets with a transit gateway multicast domain, transitioning
// the association state to "rejected". Mirrors DisassociateTransitGatewayMulticastDomain.
func (b *InMemoryBackend) RejectTransitGatewayMulticastDomainAssociations(
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

	b.mu.Lock("RejectTransitGatewayMulticastDomainAssociations")
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
		cp.State = tgwAttachmentStateRejected
		assocs = append(assocs, &cp)
	}

	return assocs, nil
}
