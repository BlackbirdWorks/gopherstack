package networkmanager

import (
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// This file implements PARITY.md families A-F: the Global Networks base
// container hierarchy (GlobalNetwork > Site > Device > Link, bound together
// by LinkAssociation) plus Connection, the on-prem device-to-device logical
// link. All six resource kinds share the same 4-value state shape
// (PENDING/AVAILABLE/DELETING/UPDATING, except LinkAssociation/Connection's
// own association-state variants) and the same real, honestly-simulatable
// PENDING->AVAILABLE / UPDATING->AVAILABLE / DELETING->(gone) transitions
// via store.go's scheduleAdvance/scheduleRemoval.

// ---- GlobalNetwork ----

// CreateGlobalNetwork creates a new global network in PENDING state,
// transitioning to AVAILABLE after asyncTransitionDelay.
func (b *InMemoryBackend) CreateGlobalNetwork(description string, tagMap map[string]string) *GlobalNetwork {
	b.mu.Lock("CreateGlobalNetwork")
	defer b.mu.Unlock()

	id := newGlobalNetworkID()
	g := &GlobalNetwork{
		GlobalNetworkID:  id,
		GlobalNetworkArn: b.globalNetworkARN(id),
		CreatedAt:        nowUTC(),
		Description:      description,
		State:            statePending,
		Tags:             tags.FromMap("networkmanager.globalnetwork."+id+".tags", tagMap),
	}
	b.globalNetworks.Put(g)

	scheduleAdvance(b, "GlobalNetworkAvailable", b.globalNetworks, id,
		func(v *GlobalNetwork) *string { return &v.State }, statePending, stateAvailable)

	return g.clone()
}

// UpdateGlobalNetwork updates description, transitioning through UPDATING.
func (b *InMemoryBackend) UpdateGlobalNetwork(id, description string) (*GlobalNetwork, error) {
	b.mu.Lock("UpdateGlobalNetwork")
	defer b.mu.Unlock()

	g, ok := b.globalNetworks.Get(id)
	if !ok {
		return nil, notFoundError(resourceGlobalNetwork, id)
	}

	g.Description = description
	g.State = stateUpdating

	scheduleAdvance(b, "GlobalNetworkUpdated", b.globalNetworks, id,
		func(v *GlobalNetwork) *string { return &v.State }, stateUpdating, stateAvailable)

	return g.clone(), nil
}

// DeleteGlobalNetwork marks id DELETING, removing it after
// asyncTransitionDelay.
func (b *InMemoryBackend) DeleteGlobalNetwork(id string) (*GlobalNetwork, error) {
	b.mu.Lock("DeleteGlobalNetwork")
	defer b.mu.Unlock()

	g, ok := b.globalNetworks.Get(id)
	if !ok {
		return nil, notFoundError(resourceGlobalNetwork, id)
	}

	g.State = stateDeleting
	scheduleRemoval(b, "GlobalNetworkDeleted", b.globalNetworks, id)

	return g.clone(), nil
}

// DescribeGlobalNetworks returns a page of global networks, optionally
// filtered by ids.
func (b *InMemoryBackend) DescribeGlobalNetworks(
	ids []string,
	token string,
	limit int,
) (page.Page[*GlobalNetwork], error) {
	b.mu.RLock("DescribeGlobalNetworks")
	defer b.mu.RUnlock()

	if len(ids) == 0 {
		return page.New(
			cloneAll(b.globalNetworks.Snapshot(), (*GlobalNetwork).clone),
			token,
			limit,
			defaultPageLimit,
		), nil
	}

	out := make([]*GlobalNetwork, 0, len(ids))

	for _, id := range ids {
		g, ok := b.globalNetworks.Get(id)
		if !ok {
			return page.Page[*GlobalNetwork]{}, notFoundError(resourceGlobalNetwork, id)
		}

		out = append(out, g.clone())
	}

	return page.New(out, token, limit, defaultPageLimit), nil
}

// globalNetworkExists reports whether id names a known global network --
// used by every family below to validate the GlobalNetworkId path
// parameter before creating a scoped child resource.
func (b *InMemoryBackend) globalNetworkExists(id string) bool {
	_, ok := b.globalNetworks.Get(id)

	return ok
}

// ---- Site ----

// CreateSite creates a new site under globalNetworkID.
func (b *InMemoryBackend) CreateSite(
	globalNetworkID, description string, loc *Location, tagMap map[string]string,
) (*Site, error) {
	b.mu.Lock("CreateSite")
	defer b.mu.Unlock()

	if !b.globalNetworkExists(globalNetworkID) {
		return nil, notFoundError(resourceGlobalNetwork, globalNetworkID)
	}

	id := newSiteID()
	s := &Site{
		SiteID:          id,
		SiteArn:         b.siteARN(globalNetworkID, id),
		GlobalNetworkID: globalNetworkID,
		CreatedAt:       nowUTC(),
		Description:     description,
		Location:        loc,
		State:           statePending,
		Tags:            tags.FromMap("networkmanager.site."+id+".tags", tagMap),
	}
	b.sites.Put(s)

	scheduleAdvance(
		b,
		"SiteAvailable",
		b.sites,
		id,
		func(v *Site) *string { return &v.State },
		statePending,
		stateAvailable,
	)

	return s.clone(), nil
}

func (b *InMemoryBackend) UpdateSite(globalNetworkID, id, description string, loc *Location) (*Site, error) {
	b.mu.Lock("UpdateSite")
	defer b.mu.Unlock()

	s, ok := b.sites.Get(id)
	if !ok || s.GlobalNetworkID != globalNetworkID {
		return nil, notFoundError(resourceSite, id)
	}

	if description != "" {
		s.Description = description
	}

	if loc != nil {
		s.Location = loc
	}

	s.State = stateUpdating
	scheduleAdvance(
		b,
		"SiteUpdated",
		b.sites,
		id,
		func(v *Site) *string { return &v.State },
		stateUpdating,
		stateAvailable,
	)

	return s.clone(), nil
}

func (b *InMemoryBackend) DeleteSite(globalNetworkID, id string) (*Site, error) {
	b.mu.Lock("DeleteSite")
	defer b.mu.Unlock()

	s, ok := b.sites.Get(id)
	if !ok || s.GlobalNetworkID != globalNetworkID {
		return nil, notFoundError(resourceSite, id)
	}

	s.State = stateDeleting
	scheduleRemoval(b, "SiteDeleted", b.sites, id)

	return s.clone(), nil
}

func (b *InMemoryBackend) GetSites(
	globalNetworkID string,
	ids []string,
	token string,
	limit int,
) (page.Page[*Site], error) {
	b.mu.RLock("GetSites")
	defer b.mu.RUnlock()

	if !b.globalNetworkExists(globalNetworkID) {
		return page.Page[*Site]{}, notFoundError(resourceGlobalNetwork, globalNetworkID)
	}

	all := filterByGlobalNetwork(b.sites.Snapshot(), globalNetworkID, func(s *Site) string { return s.GlobalNetworkID })
	if len(ids) > 0 {
		all = filterByIDs(all, ids, func(s *Site) string { return s.SiteID })
	}

	return page.New(cloneAll(all, (*Site).clone), token, limit, defaultPageLimit), nil
}

// ---- Device ----

func (b *InMemoryBackend) CreateDevice(
	globalNetworkID string, awsLoc *AWSLocation, loc *Location,
	description, model, serialNumber, siteID, devType, vendor string, tagMap map[string]string,
) (*Device, error) {
	b.mu.Lock("CreateDevice")
	defer b.mu.Unlock()

	if !b.globalNetworkExists(globalNetworkID) {
		return nil, notFoundError(resourceGlobalNetwork, globalNetworkID)
	}

	if siteID != "" {
		if s, ok := b.sites.Get(siteID); !ok || s.GlobalNetworkID != globalNetworkID {
			return nil, notFoundError(resourceSite, siteID)
		}
	}

	id := newDeviceID()
	d := &Device{
		DeviceID:        id,
		DeviceArn:       b.deviceARN(globalNetworkID, id),
		GlobalNetworkID: globalNetworkID,
		CreatedAt:       nowUTC(),
		AWSLocation:     awsLoc,
		Location:        loc,
		Description:     description,
		Model:           model,
		SerialNumber:    serialNumber,
		SiteID:          siteID,
		Type:            devType,
		Vendor:          vendor,
		State:           statePending,
		Tags:            tags.FromMap("networkmanager.device."+id+".tags", tagMap),
	}
	b.devices.Put(d)

	scheduleAdvance(b, "DeviceAvailable", b.devices, id,
		func(v *Device) *string { return &v.State }, statePending, stateAvailable)

	return d.clone(), nil
}

func (b *InMemoryBackend) UpdateDevice(
	globalNetworkID, id string, awsLoc *AWSLocation, loc *Location,
	description, model, serialNumber, siteID, devType, vendor string,
) (*Device, error) {
	b.mu.Lock("UpdateDevice")
	defer b.mu.Unlock()

	d, ok := b.devices.Get(id)
	if !ok || d.GlobalNetworkID != globalNetworkID {
		return nil, notFoundError(resourceDevice, id)
	}

	if awsLoc != nil {
		d.AWSLocation = awsLoc
	}

	if loc != nil {
		d.Location = loc
	}

	if description != "" {
		d.Description = description
	}

	if model != "" {
		d.Model = model
	}

	if serialNumber != "" {
		d.SerialNumber = serialNumber
	}

	if siteID != "" {
		d.SiteID = siteID
	}

	if devType != "" {
		d.Type = devType
	}

	if vendor != "" {
		d.Vendor = vendor
	}

	d.State = stateUpdating
	scheduleAdvance(b, "DeviceUpdated", b.devices, id,
		func(v *Device) *string { return &v.State }, stateUpdating, stateAvailable)

	return d.clone(), nil
}

func (b *InMemoryBackend) DeleteDevice(globalNetworkID, id string) (*Device, error) {
	b.mu.Lock("DeleteDevice")
	defer b.mu.Unlock()

	d, ok := b.devices.Get(id)
	if !ok || d.GlobalNetworkID != globalNetworkID {
		return nil, notFoundError(resourceDevice, id)
	}

	d.State = stateDeleting
	scheduleRemoval(b, "DeviceDeleted", b.devices, id)

	return d.clone(), nil
}

func (b *InMemoryBackend) GetDevices(
	globalNetworkID string, ids []string, siteID, token string, limit int,
) (page.Page[*Device], error) {
	b.mu.RLock("GetDevices")
	defer b.mu.RUnlock()

	if !b.globalNetworkExists(globalNetworkID) {
		return page.Page[*Device]{}, notFoundError(resourceGlobalNetwork, globalNetworkID)
	}

	all := filterByGlobalNetwork(
		b.devices.Snapshot(),
		globalNetworkID,
		func(d *Device) string { return d.GlobalNetworkID },
	)
	if len(ids) > 0 {
		all = filterByIDs(all, ids, func(d *Device) string { return d.DeviceID })
	}

	if siteID != "" {
		filtered := all[:0:0]

		for _, d := range all {
			if d.SiteID == siteID {
				filtered = append(filtered, d)
			}
		}

		all = filtered
	}

	return page.New(cloneAll(all, (*Device).clone), token, limit, defaultPageLimit), nil
}

// ---- Link ----

func (b *InMemoryBackend) CreateLink(
	globalNetworkID, siteID string, bw *Bandwidth, description, provider, linkType string, tagMap map[string]string,
) (*Link, error) {
	b.mu.Lock("CreateLink")
	defer b.mu.Unlock()

	if !b.globalNetworkExists(globalNetworkID) {
		return nil, notFoundError(resourceGlobalNetwork, globalNetworkID)
	}

	if s, ok := b.sites.Get(siteID); !ok || s.GlobalNetworkID != globalNetworkID {
		return nil, notFoundError(resourceSite, siteID)
	}

	id := newLinkID()
	l := &Link{
		LinkID:          id,
		LinkArn:         b.linkARN(globalNetworkID, id),
		GlobalNetworkID: globalNetworkID,
		SiteID:          siteID,
		CreatedAt:       nowUTC(),
		Bandwidth:       bw,
		Description:     description,
		Provider:        provider,
		Type:            linkType,
		State:           statePending,
		Tags:            tags.FromMap("networkmanager.link."+id+".tags", tagMap),
	}
	b.links.Put(l)

	scheduleAdvance(
		b,
		"LinkAvailable",
		b.links,
		id,
		func(v *Link) *string { return &v.State },
		statePending,
		stateAvailable,
	)

	return l.clone(), nil
}

func (b *InMemoryBackend) UpdateLink(
	globalNetworkID, id string, bw *Bandwidth, description, provider, linkType string,
) (*Link, error) {
	b.mu.Lock("UpdateLink")
	defer b.mu.Unlock()

	l, ok := b.links.Get(id)
	if !ok || l.GlobalNetworkID != globalNetworkID {
		return nil, notFoundError(resourceLink, id)
	}

	if bw != nil {
		l.Bandwidth = bw
	}

	if description != "" {
		l.Description = description
	}

	if provider != "" {
		l.Provider = provider
	}

	if linkType != "" {
		l.Type = linkType
	}

	l.State = stateUpdating
	scheduleAdvance(
		b,
		"LinkUpdated",
		b.links,
		id,
		func(v *Link) *string { return &v.State },
		stateUpdating,
		stateAvailable,
	)

	return l.clone(), nil
}

func (b *InMemoryBackend) DeleteLink(globalNetworkID, id string) (*Link, error) {
	b.mu.Lock("DeleteLink")
	defer b.mu.Unlock()

	l, ok := b.links.Get(id)
	if !ok || l.GlobalNetworkID != globalNetworkID {
		return nil, notFoundError(resourceLink, id)
	}

	l.State = stateDeleting
	scheduleRemoval(b, "LinkDeleted", b.links, id)

	return l.clone(), nil
}

func (b *InMemoryBackend) GetLinks(
	globalNetworkID string, ids []string, provider, siteID, linkType, token string, limit int,
) (page.Page[*Link], error) {
	b.mu.RLock("GetLinks")
	defer b.mu.RUnlock()

	if !b.globalNetworkExists(globalNetworkID) {
		return page.Page[*Link]{}, notFoundError(resourceGlobalNetwork, globalNetworkID)
	}

	all := filterByGlobalNetwork(b.links.Snapshot(), globalNetworkID, func(l *Link) string { return l.GlobalNetworkID })
	if len(ids) > 0 {
		all = filterByIDs(all, ids, func(l *Link) string { return l.LinkID })
	}

	all = filterOptional(all, provider, func(l *Link) string { return l.Provider })
	all = filterOptional(all, siteID, func(l *Link) string { return l.SiteID })
	all = filterOptional(all, linkType, func(l *Link) string { return l.Type })

	return page.New(cloneAll(all, (*Link).clone), token, limit, defaultPageLimit), nil
}

// ---- Link Association ----

func (b *InMemoryBackend) AssociateLink(globalNetworkID, deviceID, linkID string) (*LinkAssociation, error) {
	b.mu.Lock("AssociateLink")
	defer b.mu.Unlock()

	if d, ok := b.devices.Get(deviceID); !ok || d.GlobalNetworkID != globalNetworkID {
		return nil, notFoundError(resourceDevice, deviceID)
	}

	if l, ok := b.links.Get(linkID); !ok || l.GlobalNetworkID != globalNetworkID {
		return nil, notFoundError(resourceLink, linkID)
	}

	a := &LinkAssociation{
		DeviceID: deviceID, GlobalNetworkID: globalNetworkID, LinkID: linkID,
		LinkAssociationState: assocStatePending,
	}
	b.linkAssociations.Put(a)

	key := linkAssociationKey(globalNetworkID, deviceID, linkID)
	scheduleAdvance(b, "LinkAssociationAvailable", b.linkAssociations, key,
		func(v *LinkAssociation) *string { return &v.LinkAssociationState }, assocStatePending, assocStateAvailable)

	return a.clone(), nil
}

func (b *InMemoryBackend) DisassociateLink(globalNetworkID, deviceID, linkID string) (*LinkAssociation, error) {
	b.mu.Lock("DisassociateLink")
	defer b.mu.Unlock()

	key := linkAssociationKey(globalNetworkID, deviceID, linkID)

	a, ok := b.linkAssociations.Get(key)
	if !ok {
		return nil, notFoundError(resourceLinkAssoc, key)
	}

	a.LinkAssociationState = assocStateDeleting
	scheduleRemoval(b, "LinkAssociationDeleted", b.linkAssociations, key)

	return a.clone(), nil
}

func (b *InMemoryBackend) GetLinkAssociations(
	globalNetworkID, deviceID, linkID, token string, limit int,
) (page.Page[*LinkAssociation], error) {
	b.mu.RLock("GetLinkAssociations")
	defer b.mu.RUnlock()

	if !b.globalNetworkExists(globalNetworkID) {
		return page.Page[*LinkAssociation]{}, notFoundError(resourceGlobalNetwork, globalNetworkID)
	}

	all := filterByGlobalNetwork(
		b.linkAssociations.Snapshot(), globalNetworkID, func(a *LinkAssociation) string { return a.GlobalNetworkID },
	)
	all = filterOptional(all, deviceID, func(a *LinkAssociation) string { return a.DeviceID })
	all = filterOptional(all, linkID, func(a *LinkAssociation) string { return a.LinkID })

	sort.Slice(all, func(i, j int) bool {
		return linkAssociationKeyFn(all[i]) < linkAssociationKeyFn(all[j])
	})

	return page.New(cloneAll(all, (*LinkAssociation).clone), token, limit, defaultPageLimit), nil
}

// ---- Connection ----

func (b *InMemoryBackend) CreateConnection(
	globalNetworkID, connectedDeviceID, deviceID, connectedLinkID, description, linkID string, tagMap map[string]string,
) (*Connection, error) {
	b.mu.Lock("CreateConnection")
	defer b.mu.Unlock()

	// CreateConnection genuinely lacks ResourceNotFoundException in the real
	// SDK's error set despite referencing GlobalNetworkId/ConnectedDeviceId/
	// DeviceId/ConnectedLinkId/LinkId (PARITY.md family F's note -- likely
	// an SDK-model oversight, reported as read not corrected). This backend
	// still validates GlobalNetworkId/DeviceId/ConnectedDeviceId exist to
	// avoid modeling an orphaned Connection. A prior pass used notFoundError
	// here reasoning it was "the closest honest match available" -- but
	// ResourceNotFoundException isn't in this op's declared set either, so
	// that produces an untyped GenericAPIError for every real client
	// regardless. ValidationException (declared for this op, reason
	// FieldValidationFailed) is the only choice that actually decodes into
	// a typed exception.
	if !b.globalNetworkExists(globalNetworkID) {
		return nil, validationError(fmt.Sprintf("%s %s not found", resourceGlobalNetwork, globalNetworkID))
	}

	if d, ok := b.devices.Get(deviceID); !ok || d.GlobalNetworkID != globalNetworkID {
		return nil, validationError(fmt.Sprintf("%s %s not found", resourceDevice, deviceID))
	}

	if d, ok := b.devices.Get(connectedDeviceID); !ok || d.GlobalNetworkID != globalNetworkID {
		return nil, validationError(fmt.Sprintf("%s %s not found", resourceDevice, connectedDeviceID))
	}

	id := newConnectionID()
	c := &Connection{
		ConnectionID:      id,
		ConnectionArn:     b.connectionARN(globalNetworkID, id),
		ConnectedDeviceID: connectedDeviceID,
		ConnectedLinkID:   connectedLinkID,
		DeviceID:          deviceID,
		GlobalNetworkID:   globalNetworkID,
		LinkID:            linkID,
		CreatedAt:         nowUTC(),
		Description:       description,
		State:             statePending,
		Tags:              tags.FromMap("networkmanager.connection."+id+".tags", tagMap),
	}
	b.connections.Put(c)

	scheduleAdvance(b, "ConnectionAvailable", b.connections, id,
		func(v *Connection) *string { return &v.State }, statePending, stateAvailable)

	return c.clone(), nil
}

func (b *InMemoryBackend) UpdateConnection(
	globalNetworkID, id, connectedLinkID, description, linkID string,
) (*Connection, error) {
	b.mu.Lock("UpdateConnection")
	defer b.mu.Unlock()

	c, ok := b.connections.Get(id)
	if !ok || c.GlobalNetworkID != globalNetworkID {
		return nil, notFoundError(resourceConnection, id)
	}

	if connectedLinkID != "" {
		c.ConnectedLinkID = connectedLinkID
	}

	if description != "" {
		c.Description = description
	}

	if linkID != "" {
		c.LinkID = linkID
	}

	c.State = stateUpdating
	scheduleAdvance(b, "ConnectionUpdated", b.connections, id,
		func(v *Connection) *string { return &v.State }, stateUpdating, stateAvailable)

	return c.clone(), nil
}

func (b *InMemoryBackend) DeleteConnection(globalNetworkID, id string) (*Connection, error) {
	b.mu.Lock("DeleteConnection")
	defer b.mu.Unlock()

	c, ok := b.connections.Get(id)
	if !ok || c.GlobalNetworkID != globalNetworkID {
		return nil, notFoundError(resourceConnection, id)
	}

	c.State = stateDeleting
	scheduleRemoval(b, "ConnectionDeleted", b.connections, id)

	return c.clone(), nil
}

func (b *InMemoryBackend) GetConnections(
	globalNetworkID string, ids []string, deviceID, token string, limit int,
) (page.Page[*Connection], error) {
	b.mu.RLock("GetConnections")
	defer b.mu.RUnlock()

	if !b.globalNetworkExists(globalNetworkID) {
		return page.Page[*Connection]{}, notFoundError(resourceGlobalNetwork, globalNetworkID)
	}

	all := filterByGlobalNetwork(
		b.connections.Snapshot(), globalNetworkID, func(c *Connection) string { return c.GlobalNetworkID },
	)
	if len(ids) > 0 {
		all = filterByIDs(all, ids, func(c *Connection) string { return c.ConnectionID })
	}

	all = filterOptional(all, deviceID, func(c *Connection) string { return c.DeviceID })

	return page.New(cloneAll(all, (*Connection).clone), token, limit, defaultPageLimit), nil
}

// ---- generic list-filter helpers shared across every family in this
// package ----

func cloneAll[V any](items []*V, cloneFn func(*V) *V) []*V {
	out := make([]*V, len(items))
	for i, v := range items {
		out[i] = cloneFn(v)
	}

	return out
}

// sortAndPage sorts items by keyFn ascending (for deterministic List/Get
// output, matching store.Table.Snapshot's own ordering convention) and
// returns the requested page of clones. Shared by every family's List* op
// so the sort+clone+paginate tail isn't repeated per resource kind.
func sortAndPage[V any](items []*V, keyFn func(*V) string, cloneFn func(*V) *V, token string, limit int) page.Page[*V] {
	sort.Slice(items, func(i, j int) bool { return keyFn(items[i]) < keyFn(items[j]) })

	return page.New(cloneAll(items, cloneFn), token, limit, defaultPageLimit)
}

func filterByGlobalNetwork[V any](items []*V, globalNetworkID string, get func(*V) string) []*V {
	out := items[:0:0]

	for _, v := range items {
		if get(v) == globalNetworkID {
			out = append(out, v)
		}
	}

	return out
}

func filterByIDs[V any](items []*V, ids []string, get func(*V) string) []*V {
	want := make(map[string]bool, len(ids))
	for _, id := range ids {
		want[id] = true
	}

	out := items[:0:0]

	for _, v := range items {
		if want[get(v)] {
			out = append(out, v)
		}
	}

	return out
}

// filterOptional keeps only items where get(v) == want, unless want is
// empty (no filter applied).
func filterOptional[V any](items []*V, want string, get func(*V) string) []*V {
	if want == "" {
		return items
	}

	out := items[:0:0]

	for _, v := range items {
		if get(v) == want {
			out = append(out, v)
		}
	}

	return out
}
