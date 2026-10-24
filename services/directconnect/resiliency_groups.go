package directconnect

import (
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	resourceResiliencyGroup = "resiliency group"

	resiliencyGroupTypeManaged = "Managed"

	resiliencyGroupStatePending   = "pending"
	resiliencyGroupStateAvailable = "available"
	resiliencyGroupStateDeleting  = "deleting"
	resiliencyGroupStateDeleted   = "deleted"

	rgAssociationAssociating    = "associating"
	rgAssociationAssociated     = "associated"
	rgAssociationDisassociating = "disassociating"

	maxBillingModeConnections = 200
)

// ResiliencyGroup groups connections that together meet a target resiliency model.
type ResiliencyGroup struct {
	Tags                    *tags.Tags
	ResiliencyGroupID       string
	ResiliencyGroupName     string
	IntendedResiliencyModel string
	State                   string
	ClientToken             string
}

// ResiliencyGroupAssociation links one connection to a resiliency group.
type ResiliencyGroupAssociation struct {
	ResiliencyGroupID string
	ConnectionID      string
	State             string
}

func (g *ResiliencyGroup) clone() *ResiliencyGroup {
	if g == nil {
		return nil
	}

	cp := *g

	return &cp
}

func resiliencyGroupKeyFn(v *ResiliencyGroup) string { return v.ResiliencyGroupID }

func rgAssociationKey(groupID, connectionID string) string { return groupID + "|" + connectionID }

func rgAssociationKeyFn(v *ResiliencyGroupAssociation) string {
	return rgAssociationKey(v.ResiliencyGroupID, v.ConnectionID)
}

func rgAssociationGroupIndexKeyFn(v *ResiliencyGroupAssociation) string { return v.ResiliencyGroupID }

func newResiliencyGroupID() string { return "dxrg-" + randomHex() }

// ResiliencyGroupARN builds the ARN for a resiliency group ID.
func (b *InMemoryBackend) ResiliencyGroupARN(id string) string {
	return arn.Build("directconnect", b.region, b.accountID, "dxrg/"+id)
}

func validResiliencyModel(m string) bool {
	switch m {
	case "maximum-resiliency", "high-resiliency", "basic-resiliency":
		return true
	}

	return false
}

// CreateResiliencyGroup creates a resiliency group; a repeated ClientToken returns the original group.
func (b *InMemoryBackend) CreateResiliencyGroup(req *createResiliencyGroupRequest) (*ResiliencyGroup, error) {
	if req.ResiliencyGroupName == "" {
		return nil, clientError("resiliencyGroupName is required")
	}

	if !validResiliencyModel(req.IntendedResiliencyModel) {
		return nil, clientError("Invalid intendedResiliencyModel " + req.IntendedResiliencyModel +
			"; valid values are maximum-resiliency, high-resiliency, and basic-resiliency")
	}

	if err := validateNewTags(tagWireKeys(req.Tags)); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateResiliencyGroup")
	defer b.mu.Unlock()

	if req.ClientToken != "" {
		for _, g := range b.resiliencyGroups.All() {
			if g.ClientToken == req.ClientToken {
				return g.clone(), nil
			}
		}
	}

	id := newResiliencyGroupID()
	t := tags.New("directconnect.resiliencygroup." + id + ".tags")
	t.Merge(tagWireToMap(req.Tags))

	g := &ResiliencyGroup{
		ResiliencyGroupID:       id,
		ResiliencyGroupName:     req.ResiliencyGroupName,
		IntendedResiliencyModel: req.IntendedResiliencyModel,
		State:                   resiliencyGroupStatePending,
		ClientToken:             req.ClientToken,
		Tags:                    t,
	}
	b.resiliencyGroups.Put(g)
	b.scheduleTransition("resiliencygroup:"+id, []string{resiliencyGroupStateAvailable}, &g.State)

	return g.clone(), nil
}

// GetResiliencyGroup returns one resiliency group.
func (b *InMemoryBackend) GetResiliencyGroup(id string) (*ResiliencyGroup, error) {
	b.mu.RLock("GetResiliencyGroup")
	defer b.mu.RUnlock()

	g, ok := b.resiliencyGroups.Get(id)
	if !ok {
		return nil, notFoundError(resourceResiliencyGroup, id)
	}

	return g.clone(), nil
}

// ListResiliencyGroups returns a page of resiliency groups ordered by ID.
func (b *InMemoryBackend) ListResiliencyGroups(token string, maxResults *int32) page.Page[*ResiliencyGroup] {
	b.mu.RLock("ListResiliencyGroups")
	all := b.resiliencyGroups.Snapshot()
	out := make([]*ResiliencyGroup, 0, len(all))

	for _, g := range all {
		out = append(out, g.clone())
	}

	b.mu.RUnlock()

	slices.SortFunc(out, func(a, c *ResiliencyGroup) int {
		return strings.Compare(a.ResiliencyGroupID, c.ResiliencyGroupID)
	})

	return page.New(out, token, pageLimit(maxResults), defaultPageLimit)
}

func pageLimit(maxResults *int32) int {
	if maxResults == nil {
		return 0
	}

	return min(int(*maxResults), defaultPageLimit)
}

// UpdateResiliencyGroup renames a resiliency group.
func (b *InMemoryBackend) UpdateResiliencyGroup(id, name string) (*ResiliencyGroup, error) {
	if name == "" {
		return nil, clientError("resiliencyGroupName is required")
	}

	b.mu.Lock("UpdateResiliencyGroup")
	defer b.mu.Unlock()

	g, ok := b.resiliencyGroups.Get(id)
	if !ok {
		return nil, notFoundError(resourceResiliencyGroup, id)
	}

	if g.State == resiliencyGroupStateDeleting || g.State == resiliencyGroupStateDeleted {
		return nil, clientError("resiliency group " + id + " is " + g.State)
	}

	g.ResiliencyGroupName = name

	return g.clone(), nil
}

// DeleteResiliencyGroup starts the asynchronous deleting -> deleted transition.
func (b *InMemoryBackend) DeleteResiliencyGroup(id string) (*ResiliencyGroup, error) {
	b.mu.Lock("DeleteResiliencyGroup")
	defer b.mu.Unlock()

	g, ok := b.resiliencyGroups.Get(id)
	if !ok {
		return nil, notFoundError(resourceResiliencyGroup, id)
	}

	if g.State == resiliencyGroupStateDeleting || g.State == resiliencyGroupStateDeleted {
		return g.clone(), nil
	}

	if len(b.rgAssociationsByGroup.Get(id)) > 0 {
		return nil, clientError("resiliency group " + id + " still has associated connections")
	}

	g.State = resiliencyGroupStateDeleting
	b.scheduleTransition("resiliencygroup:"+id, []string{resiliencyGroupStateDeleted}, &g.State)

	return g.clone(), nil
}

// connectionIDFromIdentifier accepts a connection ID or a connection ARN.
func connectionIDFromIdentifier(identifier string) string {
	if id, ok := resourceIDFromARN(identifier, ":dxcon/"); ok {
		return id
	}

	return identifier
}

// resolveRGConnectionsLocked validates identifiers and returns de-duplicated connection IDs in request order.
func (b *InMemoryBackend) resolveRGConnectionsLocked(identifiers []string) ([]string, error) {
	if len(identifiers) == 0 {
		return nil, clientError("connectionIdentifiers is required")
	}

	ids := make([]string, 0, len(identifiers))

	for _, ident := range identifiers {
		id := connectionIDFromIdentifier(ident)

		c, ok := b.connections.Get(id)
		if !ok {
			return nil, notFoundError(resourceConnection, ident)
		}

		if c.ConnectionState == ConnectionStateDeleting || c.ConnectionState == ConnectionStateDeleted ||
			c.ConnectionState == ConnectionStateRejected {
			return nil, clientError("connection " + id + " is " + c.ConnectionState)
		}

		if !slices.Contains(ids, id) {
			ids = append(ids, id)
		}
	}

	return ids, nil
}

func (b *InMemoryBackend) activeResiliencyGroupLocked(id string) (*ResiliencyGroup, error) {
	g, ok := b.resiliencyGroups.Get(id)
	if !ok {
		return nil, notFoundError(resourceResiliencyGroup, id)
	}

	if g.State == resiliencyGroupStateDeleting || g.State == resiliencyGroupStateDeleted {
		return nil, clientError("resiliency group " + id + " is " + g.State)
	}

	return g, nil
}

// AssociateConnectionsToResiliencyGroup atomically associates connections with a group.
func (b *InMemoryBackend) AssociateConnectionsToResiliencyGroup(
	groupID string,
	identifiers []string,
) ([]*ResiliencyGroupAssociation, error) {
	b.mu.Lock("AssociateConnectionsToResiliencyGroup")
	defer b.mu.Unlock()

	if _, err := b.activeResiliencyGroupLocked(groupID); err != nil {
		return nil, err
	}

	ids, err := b.resolveRGConnectionsLocked(identifiers)
	if err != nil {
		return nil, err
	}

	for _, id := range ids {
		for _, a := range b.rgAssociations.All() {
			if a.ConnectionID == id {
				return nil, clientError("connection " + id + " is already associated with resiliency group " +
					a.ResiliencyGroupID)
			}
		}
	}

	out := make([]*ResiliencyGroupAssociation, 0, len(ids))

	for _, id := range ids {
		a := &ResiliencyGroupAssociation{
			ResiliencyGroupID: groupID,
			ConnectionID:      id,
			State:             rgAssociationAssociating,
		}
		b.rgAssociations.Put(a)
		b.scheduleTransition("rgassoc:"+rgAssociationKey(groupID, id), []string{rgAssociationAssociated}, &a.State)

		cp := *a
		out = append(out, &cp)
	}

	return out, nil
}

// DisassociateConnectionsFromResiliencyGroup atomically starts disassociating connections from a group.
func (b *InMemoryBackend) DisassociateConnectionsFromResiliencyGroup(
	groupID string,
	identifiers []string,
) ([]*ResiliencyGroupAssociation, error) {
	b.mu.Lock("DisassociateConnectionsFromResiliencyGroup")
	defer b.mu.Unlock()

	if _, ok := b.resiliencyGroups.Get(groupID); !ok {
		return nil, notFoundError(resourceResiliencyGroup, groupID)
	}

	if len(identifiers) == 0 {
		return nil, clientError("connectionIdentifiers is required")
	}

	var ids []string

	for _, ident := range identifiers {
		id := connectionIDFromIdentifier(ident)

		if _, ok := b.rgAssociations.Get(rgAssociationKey(groupID, id)); !ok {
			return nil, clientError("connection " + id + " is not associated with resiliency group " + groupID)
		}

		if !slices.Contains(ids, id) {
			ids = append(ids, id)
		}
	}

	out := make([]*ResiliencyGroupAssociation, 0, len(ids))

	for _, id := range ids {
		a, _ := b.rgAssociations.Get(rgAssociationKey(groupID, id))
		a.State = rgAssociationDisassociating
		b.scheduleRGAssociationRemoval(a)

		cp := *a
		out = append(out, &cp)
	}

	return out, nil
}

// scheduleRGAssociationRemoval drops a disassociating association once the delay elapses.
func (b *InMemoryBackend) scheduleRGAssociationRemoval(a *ResiliencyGroupAssociation) {
	key := rgAssociationKeyFn(a)

	b.work.After("rgassoc:"+key, asyncTransitionDelay, func() {
		b.mu.Lock("asyncTransition:rgassoc")
		defer b.mu.Unlock()

		if cur, ok := b.rgAssociations.Get(key); ok && cur.State == rgAssociationDisassociating {
			b.rgAssociations.Delete(key)
		}
	})
}

// ListResiliencyGroupAssociations returns a page of a group's associations ordered by connection ID.
func (b *InMemoryBackend) ListResiliencyGroupAssociations(
	groupID, token string,
	maxResults *int32,
) (page.Page[*ResiliencyGroupAssociation], error) {
	b.mu.RLock("ListResiliencyGroupAssociations")

	if _, ok := b.resiliencyGroups.Get(groupID); !ok {
		b.mu.RUnlock()

		return page.Page[*ResiliencyGroupAssociation]{}, notFoundError(resourceResiliencyGroup, groupID)
	}

	assocs := b.rgAssociationsByGroup.Get(groupID)
	out := make([]*ResiliencyGroupAssociation, 0, len(assocs))

	for _, a := range assocs {
		cp := *a
		out = append(out, &cp)
	}

	b.mu.RUnlock()

	slices.SortFunc(out, func(a, c *ResiliencyGroupAssociation) int {
		return strings.Compare(a.ConnectionID, c.ConnectionID)
	})

	return page.New(out, token, pageLimit(maxResults), defaultPageLimit), nil
}

// resumeResiliencyTransitionsLocked restarts async transitions interrupted by a snapshot restore.
func (b *InMemoryBackend) resumeResiliencyTransitionsLocked() {
	for _, g := range b.resiliencyGroups.All() {
		switch g.State {
		case resiliencyGroupStatePending:
			b.scheduleTransition("resiliencygroup:"+g.ResiliencyGroupID,
				[]string{resiliencyGroupStateAvailable}, &g.State)
		case resiliencyGroupStateDeleting:
			b.scheduleTransition("resiliencygroup:"+g.ResiliencyGroupID,
				[]string{resiliencyGroupStateDeleted}, &g.State)
		}
	}

	for _, a := range b.rgAssociations.All() {
		switch a.State {
		case rgAssociationAssociating:
			b.scheduleTransition("rgassoc:"+rgAssociationKeyFn(a), []string{rgAssociationAssociated}, &a.State)
		case rgAssociationDisassociating:
			b.scheduleRGAssociationRemoval(a)
		}
	}
}

// UpdateConnectionsBillingMode sets the billing mode on every listed connection, all or nothing.
func (b *InMemoryBackend) UpdateConnectionsBillingMode(mode string, connectionIDs []string) ([]*Connection, error) {
	switch mode {
	case "PayAsYouGo", "FlatRateTier1", "FlatRateTier2", "FlatRateTier3", "FlatRateTier4", "FlatRateTier5":
	default:
		return nil, clientError("Invalid billingMode " + mode)
	}

	if len(connectionIDs) == 0 || len(connectionIDs) > maxBillingModeConnections {
		return nil, clientError("connectionIds must contain between 1 and 200 connections")
	}

	b.mu.Lock("UpdateConnectionsBillingMode")
	defer b.mu.Unlock()

	conns := make([]*Connection, 0, len(connectionIDs))

	for _, id := range connectionIDs {
		c, ok := b.connections.Get(id)
		if !ok {
			return nil, notFoundError(resourceConnection, id)
		}

		conns = append(conns, c)
	}

	out := make([]*Connection, 0, len(conns))

	for _, c := range conns {
		c.BillingMode = mode
		out = append(out, c.clone())
	}

	return out, nil
}
