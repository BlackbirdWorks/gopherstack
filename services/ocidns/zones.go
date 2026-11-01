package ocidns

import (
	"cmp"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	dnsPort       = 53
	maxPort       = 65535
	oraclevcnZone = "oraclevcn.com"
)

type extWire struct {
	Port      *int    `json:"port"`
	TsigKeyID *string `json:"tsigKeyId"`
	Address   string  `json:"address"`
}

type nameserverWire struct {
	Hostname string `json:"hostname"`
}

type transferWire struct {
	Address               string `json:"address"`
	Port                  int    `json:"port"`
	IsTransferSource      bool   `json:"isTransferSource"`
	IsTransferDestination bool   `json:"isTransferDestination"`
}

// ZoneWire is the wire form of a zone.
type ZoneWire struct {
	TimeCreated         time.Time                 `json:"timeCreated"`
	FreeformTags        map[string]string         `json:"freeformTags"`
	DefinedTags         map[string]map[string]any `json:"definedTags"`
	ViewID              *string                   `json:"viewId,omitempty"`
	Name                string                    `json:"name"`
	ZoneType            string                    `json:"zoneType"`
	CompartmentID       string                    `json:"compartmentId"`
	Scope               string                    `json:"scope"`
	ResolutionMode      string                    `json:"resolutionMode"`
	DnssecState         string                    `json:"dnssecState"`
	Self                string                    `json:"self"`
	ID                  string                    `json:"id"`
	Version             string                    `json:"version"`
	LifecycleState      string                    `json:"lifecycleState"`
	ExternalMasters     []extWire                 `json:"externalMasters"`
	ExternalDownstreams []extWire                 `json:"externalDownstreams"`
	Nameservers         []nameserverWire          `json:"nameservers"`
	ZoneTransferServers []transferWire            `json:"zoneTransferServers,omitempty"`
	Serial              int64                     `json:"serial"`
	IsProtected         bool                      `json:"isProtected"`
}

// ZoneSummaryWire is the wire form of a zone in a list.
type ZoneSummaryWire struct {
	TimeCreated    time.Time                 `json:"timeCreated"`
	FreeformTags   map[string]string         `json:"freeformTags"`
	DefinedTags    map[string]map[string]any `json:"definedTags"`
	ViewID         *string                   `json:"viewId,omitempty"`
	Name           string                    `json:"name"`
	ZoneType       string                    `json:"zoneType"`
	CompartmentID  string                    `json:"compartmentId"`
	Scope          string                    `json:"scope"`
	ResolutionMode string                    `json:"resolutionMode"`
	DnssecState    string                    `json:"dnssecState"`
	Self           string                    `json:"self"`
	ID             string                    `json:"id"`
	Version        string                    `json:"version"`
	LifecycleState string                    `json:"lifecycleState"`
	Serial         int64                     `json:"serial"`
	IsProtected    bool                      `json:"isProtected"`
}

func extWires(in []ExtServer) []extWire {
	out := make([]extWire, 0, len(in))

	for _, e := range in {
		w := extWire{Address: e.Address, Port: e.Port}
		if e.TsigKeyID != "" {
			id := e.TsigKeyID
			w.TsigKeyID = &id
		}

		out = append(out, w)
	}

	return out
}

func viewPtr(id string) *string {
	if id == "" {
		return nil
	}

	return &id
}

func (b *InMemoryBackend) zoneWire(z *Zone) ZoneWire {
	w := ZoneWire{
		TimeCreated: z.Created, FreeformTags: z.free(), DefinedTags: z.defined(), ViewID: viewPtr(z.ViewID),
		Name: z.Name, ZoneType: z.ZoneType, CompartmentID: z.CompartmentID, Scope: z.Scope,
		ResolutionMode: z.ResolutionMode, DnssecState: z.DnssecState, Self: b.self("zones", z.ID), ID: z.ID,
		Version: strconv.FormatInt(z.Version, 10), Serial: z.Version, LifecycleState: stateActive,
		ExternalMasters: extWires(z.ExternalMasters), ExternalDownstreams: extWires(z.ExternalDownstreams),
		Nameservers: []nameserverWire{},
	}

	for _, ns := range b.nameservers(z) {
		w.Nameservers = append(w.Nameservers, nameserverWire{Hostname: ns})
	}

	if z.ZoneType == zoneSecondary || len(z.ExternalDownstreams) > 0 {
		w.ZoneTransferServers = transferServers()
	}

	return w
}

func (b *InMemoryBackend) zoneSummary(z *Zone) ZoneSummaryWire {
	return ZoneSummaryWire{
		TimeCreated: z.Created, FreeformTags: z.free(), DefinedTags: z.defined(), ViewID: viewPtr(z.ViewID),
		Name: z.Name, ZoneType: z.ZoneType, CompartmentID: z.CompartmentID, Scope: z.Scope,
		ResolutionMode: z.ResolutionMode, DnssecState: z.DnssecState, Self: b.self("zones", z.ID), ID: z.ID,
		Version: strconv.FormatInt(z.Version, 10), Serial: z.Version, LifecycleState: stateActive,
	}
}

// transferServers are the emulator's stand-ins for the OCI nameservers that transfer zones with external servers.
func transferServers() []transferWire {
	return []transferWire{
		{Address: "192.0.2.53", Port: dnsPort, IsTransferSource: true},
		{Address: "192.0.2.54", Port: dnsPort, IsTransferDestination: true},
	}
}

// ZoneRef addresses a zone by name or OCID, with the scope and view that disambiguate a name.
type ZoneRef struct {
	Ref    string
	Scope  string
	ViewID string
}

func (b *InMemoryBackend) findZone(r ZoneRef) (*Zone, *APIError) {
	if isOCID(r.Ref) {
		if z, ok := b.zones[r.Ref]; ok {
			return z, nil
		}

		return nil, errNotFound(r.Ref)
	}

	scope := strings.ToUpper(r.Scope)

	switch {
	case scope == "" && r.ViewID != "":
		scope = scopePrivate
	case scope == "":
		scope = scopeGlobal
	case scope != scopeGlobal && scope != scopePrivate:
		return nil, errParam("scope must be GLOBAL or PRIVATE")
	}

	if scope == scopePrivate && r.ViewID == "" {
		return nil, errMissing("viewId is required when scope is PRIVATE")
	}

	name, ok := normName(r.Ref, false)
	if !ok {
		return nil, errNotFound(r.Ref)
	}

	for _, z := range b.zones {
		if z.Name == name && z.Scope == scope && z.ViewID == r.ViewID {
			return z, nil
		}
	}

	return nil, errNotFound(r.Ref)
}

// CreateZoneIn is the body of CreateZone.
type CreateZoneIn struct {
	Tags
	Name                string      `json:"name"`
	CompartmentID       string      `json:"compartmentId"`
	ViewID              string      `json:"viewId"`
	ZoneType            string      `json:"zoneType"`
	Scope               string      `json:"scope"`
	ResolutionMode      string      `json:"resolutionMode"`
	DnssecState         string      `json:"dnssecState"`
	MigrationSource     string      `json:"migrationSource"`
	ExternalMasters     []ExtServer `json:"externalMasters"`
	ExternalDownstreams []ExtServer `json:"externalDownstreams"`
}

func (b *InMemoryBackend) checkExternal(servers []ExtServer, what string) *APIError {
	for i, s := range servers {
		if _, err := netip.ParseAddr(s.Address); err != nil {
			return errParam(what + "[" + strconv.Itoa(i) + "].address is not a valid IP address")
		}

		if s.Port != nil && (*s.Port < 1 || *s.Port > maxPort) {
			return errParam(what + "[" + strconv.Itoa(i) + "].port must be between 1 and 65535")
		}

		if s.TsigKeyID != "" {
			if _, ok := b.tsigs[s.TsigKeyID]; !ok {
				return errNotFound(s.TsigKeyID)
			}
		}
	}

	return nil
}

func (b *InMemoryBackend) validateZoneEnums(in *CreateZoneIn) *APIError {
	switch in.ZoneType = cmp.Or(strings.ToUpper(in.ZoneType), zonePrimary); in.ZoneType {
	case zonePrimary, zoneSecondary:
	default:
		return errParam("zoneType must be PRIMARY or SECONDARY")
	}

	switch in.ResolutionMode = cmp.Or(strings.ToUpper(in.ResolutionMode), modeStatic); in.ResolutionMode {
	case modeStatic, "TRANSPARENT", "RTYPE_TRANSPARENT":
	default:
		return errParam("resolutionMode must be STATIC, TRANSPARENT or RTYPE_TRANSPARENT")
	}

	switch in.DnssecState = cmp.Or(strings.ToUpper(in.DnssecState), dnssecDisabled); in.DnssecState {
	case dnssecEnabled, dnssecDisabled:
	default:
		return errParam("dnssecState must be ENABLED or DISABLED")
	}

	switch ms := strings.ToUpper(in.MigrationSource); ms {
	case "", "NONE":
	default:
		return errParam("migrationSource " + ms + " is not supported")
	}

	return nil
}

func (b *InMemoryBackend) resolveScope(in *CreateZoneIn) *APIError {
	in.Scope = strings.ToUpper(in.Scope)

	switch {
	case in.Scope == "" && in.ViewID != "":
		in.Scope = scopePrivate
	case in.Scope == "":
		in.Scope = scopeGlobal
	case in.Scope != scopeGlobal && in.Scope != scopePrivate:
		return errParam("scope must be GLOBAL or PRIVATE")
	}

	switch in.Scope {
	case scopePrivate:
		if in.ViewID == "" {
			return errMissing("viewId is required for a PRIVATE zone")
		}

		if _, ok := b.views[in.ViewID]; !ok {
			return errNotFound(in.ViewID)
		}

		if in.ZoneType != zonePrimary {
			return errParam("a PRIVATE zone must have zoneType PRIMARY")
		}

		if len(in.ExternalDownstreams) > 0 || len(in.ExternalMasters) > 0 {
			return errParam("externalMasters and externalDownstreams are not supported for PRIVATE zones")
		}

		if v := b.views[in.ViewID]; v.Protected && inZone(oraclevcnZone, in.Name) {
			return errParam("a zone at or under oraclevcn.com cannot be created in a protected view")
		}
	default:
		if in.ViewID != "" {
			return errParam("viewId is only valid for PRIVATE zones")
		}

		if in.ResolutionMode != modeStatic {
			return errParam("resolutionMode other than STATIC is only valid for PRIVATE zones")
		}
	}

	return nil
}

func (b *InMemoryBackend) validateZoneIn(in *CreateZoneIn) *APIError {
	if err := b.checkCompartment(in.CompartmentID); err != nil {
		return err
	}

	name, ok := normName(in.Name, false)
	if !ok {
		return errParam("name is not a valid zone name")
	}

	in.Name = name

	if err := b.validateZoneEnums(in); err != nil {
		return err
	}

	if err := b.resolveScope(in); err != nil {
		return err
	}

	if err := b.checkExternal(in.ExternalMasters, "externalMasters"); err != nil {
		return err
	}

	if err := b.checkExternal(in.ExternalDownstreams, "externalDownstreams"); err != nil {
		return err
	}

	switch {
	case in.ZoneType == zoneSecondary && len(in.ExternalMasters) == 0:
		return errMissing("externalMasters is required for a SECONDARY zone")
	case in.ZoneType == zonePrimary && len(in.ExternalMasters) > 0:
		return errParam("externalMasters is only valid for SECONDARY zones")
	case in.ZoneType == zoneSecondary && len(in.ExternalDownstreams) > 0:
		return errParam("externalDownstreams is not supported for SECONDARY zones")
	case in.DnssecState == dnssecEnabled && (in.Scope != scopeGlobal || in.ZoneType != zonePrimary):
		return errParam("DNSSEC can only be enabled on GLOBAL PRIMARY zones")
	}

	for _, z := range b.zones {
		if z.Name == in.Name && z.Scope == in.Scope && z.ViewID == in.ViewID {
			return errConflict("A zone named " + in.Name + " already exists")
		}
	}

	return nil
}

func (b *InMemoryBackend) newZone(in CreateZoneIn) *Zone {
	now := b.nowUTC()
	z := &Zone{
		ID: newOCID("dns-zone"), Name: in.Name, ZoneType: in.ZoneType, CompartmentID: in.CompartmentID,
		Scope: in.Scope, ViewID: in.ViewID, ResolutionMode: in.ResolutionMode, DnssecState: in.DnssecState,
		ExternalMasters: nonNil(in.ExternalMasters), ExternalDownstreams: nonNil(in.ExternalDownstreams),
		Records: map[string]*RRSet{}, Created: now, Version: 1, Seq: b.nextSeq(), Tags: in.Tags,
	}
	z.touch(now)
	b.zones[z.ID] = z

	return z
}

// CreateZone creates a zone with its managed SOA and NS records.
func (b *InMemoryBackend) CreateZone(in CreateZoneIn) (ZoneWire, tag, error) {
	b.mu.Lock("CreateZone")
	defer b.mu.Unlock()

	if err := b.validateZoneIn(&in); err != nil {
		return ZoneWire{}, tag{}, err
	}

	z := b.newZone(in)

	return b.zoneWire(z), tagOf(z.ID, z.meta), nil
}

// GetZone returns a zone.
func (b *InMemoryBackend) GetZone(r ZoneRef) (ZoneWire, tag, error) {
	b.mu.RLock("GetZone")
	defer b.mu.RUnlock()

	z, err := b.findZone(r)
	if err != nil {
		return ZoneWire{}, tag{}, err
	}

	return b.zoneWire(z), tagOf(z.ID, z.meta), nil
}

// ZoneFilter selects zones in a list.
type ZoneFilter struct {
	Created   func(time.Time) bool
	Compart   string
	Name      string
	Contains  string
	ZoneType  string
	State     string
	Scope     string
	ViewID    string
	TsigKeyID string
	Dnssec    string
}

func (f ZoneFilter) match(z *Zone) bool {
	scope := strings.ToUpper(f.Scope)
	if scope == "" {
		scope = scopeGlobal
		if f.ViewID != "" {
			scope = scopePrivate
		}
	}

	if z.CompartmentID != f.Compart || z.Scope != scope || (f.ViewID != "" && z.ViewID != f.ViewID) {
		return false
	}

	if (f.Name != "" && !strs.Equal(z.Name, f.Name)) || (f.Contains != "" && !containsFold(z.Name, f.Contains)) ||
		(f.ZoneType != "" && !strings.EqualFold(z.ZoneType, f.ZoneType)) ||
		(f.State != "" && !strings.EqualFold(f.State, stateActive)) ||
		(f.Dnssec != "" && !strings.EqualFold(z.DnssecState, f.Dnssec)) || (f.Created != nil && !f.Created(z.Created)) {
		return false
	}

	if f.TsigKeyID != "" && !usesTsig(z, f.TsigKeyID) {
		return false
	}

	return true
}

func usesTsig(z *Zone, id string) bool {
	for _, list := range [][]ExtServer{z.ExternalMasters, z.ExternalDownstreams} {
		for _, e := range list {
			if e.TsigKeyID == id {
				return true
			}
		}
	}

	return false
}

var zoneSorters = sorter[*Zone]{ //nolint:gochecknoglobals // immutable comparator table
	"name":        byString(func(z *Zone) string { return z.Name }, func(z *Zone) int64 { return z.Seq }),
	"zoneType":    byString(func(z *Zone) string { return z.ZoneType }, func(z *Zone) int64 { return z.Seq }),
	"timeCreated": byCreated(func(z *Zone) time.Time { return z.Created }, func(z *Zone) int64 { return z.Seq }),
}

// ListZones lists the zones of a compartment.
func (b *InMemoryBackend) ListZones(f ZoneFilter, o ListOpts) (listResult[ZoneSummaryWire], error) {
	b.mu.RLock("ListZones")
	defer b.mu.RUnlock()

	if err := b.checkCompartment(f.Compart); err != nil {
		return listResult[ZoneSummaryWire]{}, err
	}

	var all []*Zone

	for _, z := range b.zones {
		if f.match(z) {
			all = append(all, z)
		}
	}

	pg, err := sortPage(all, o, "timeCreated", true, zoneSorters)
	if err != nil {
		return listResult[ZoneSummaryWire]{}, err
	}

	return finish(pg, len(all), b.zoneSummary), nil
}

// UpdateZoneIn is the body of UpdateZone.
type UpdateZoneIn struct {
	Tags
	ResolutionMode      string      `json:"resolutionMode"`
	DnssecState         string      `json:"dnssecState"`
	ExternalMasters     []ExtServer `json:"externalMasters"`
	ExternalDownstreams []ExtServer `json:"externalDownstreams"`
}

func (b *InMemoryBackend) applyZoneUpdate(z *Zone, in UpdateZoneIn) *APIError {
	if m := strings.ToUpper(in.ResolutionMode); m != "" {
		switch {
		case m != modeStatic && m != "TRANSPARENT" && m != "RTYPE_TRANSPARENT":
			return errParam("resolutionMode must be STATIC, TRANSPARENT or RTYPE_TRANSPARENT")
		case m != modeStatic && z.Scope != scopePrivate:
			return errParam("resolutionMode other than STATIC is only valid for PRIVATE zones")
		}

		z.ResolutionMode = m
	}

	if d := strings.ToUpper(in.DnssecState); d != "" {
		if d != dnssecEnabled && d != dnssecDisabled {
			return errParam("dnssecState must be ENABLED or DISABLED")
		}

		if d == dnssecEnabled && (z.Scope != scopeGlobal || z.ZoneType != zonePrimary) {
			return errParam("DNSSEC can only be enabled on GLOBAL PRIMARY zones")
		}

		z.DnssecState = d
	}

	return b.applyExternals(z, in)
}

func (b *InMemoryBackend) applyExternals(z *Zone, in UpdateZoneIn) *APIError {
	if in.ExternalMasters != nil {
		if err := b.checkExternal(in.ExternalMasters, "externalMasters"); err != nil {
			return err
		}

		if z.ZoneType != zoneSecondary && len(in.ExternalMasters) > 0 {
			return errParam("externalMasters is only valid for SECONDARY zones")
		}

		if z.ZoneType == zoneSecondary && len(in.ExternalMasters) == 0 {
			return errMissing("externalMasters is required for a SECONDARY zone")
		}

		z.ExternalMasters = in.ExternalMasters
	}

	if in.ExternalDownstreams != nil {
		if err := b.checkExternal(in.ExternalDownstreams, "externalDownstreams"); err != nil {
			return err
		}

		if len(in.ExternalDownstreams) > 0 && (z.ZoneType == zoneSecondary || z.Scope == scopePrivate) {
			return errParam("externalDownstreams is not supported for SECONDARY or PRIVATE zones")
		}

		z.ExternalDownstreams = in.ExternalDownstreams
	}

	return nil
}

// UpdateZone changes the mutable attributes of a zone.
func (b *InMemoryBackend) UpdateZone(r ZoneRef, c cond, in UpdateZoneIn) (ZoneWire, tag, error) {
	b.mu.Lock("UpdateZone")
	defer b.mu.Unlock()

	z, err := b.findZone(r)
	if err != nil {
		return ZoneWire{}, tag{}, err
	}

	if err = c.check(z.ID, z.meta); err != nil {
		return ZoneWire{}, tag{}, err
	}

	if err = b.applyZoneUpdate(z, in); err != nil {
		return ZoneWire{}, tag{}, err
	}

	z.Tags.merge(in.Tags)
	z.touch(b.nowUTC())

	return b.zoneWire(z), tagOf(z.ID, z.meta), nil
}

// DeleteZone deletes a zone and its steering policy attachments.
func (b *InMemoryBackend) DeleteZone(r ZoneRef, c cond) error {
	b.mu.Lock("DeleteZone")
	defer b.mu.Unlock()

	z, err := b.findZone(r)
	if err != nil {
		return err
	}

	if err = c.check(z.ID, z.meta); err != nil {
		return err
	}

	for id, a := range b.attachments {
		if a.ZoneID == z.ID {
			delete(b.attachments, id)
		}
	}

	delete(b.zones, z.ID)

	return nil
}

// ChangeCompartmentIn is the body of the change-compartment operations.
type ChangeCompartmentIn struct {
	CompartmentID string `json:"compartmentId"`
}

// ChangeZoneCompartment moves a zone and its steering policy attachments.
func (b *InMemoryBackend) ChangeZoneCompartment(r ZoneRef, c cond, in ChangeCompartmentIn) error {
	b.mu.Lock("ChangeZoneCompartment")
	defer b.mu.Unlock()

	z, err := b.findZone(r)
	if err != nil {
		return err
	}

	if err = c.check(z.ID, z.meta); err != nil {
		return err
	}

	if err = b.checkCompartment(in.CompartmentID); err != nil {
		return err
	}

	z.CompartmentID = in.CompartmentID
	z.touch(b.nowUTC())

	for _, a := range b.attachments {
		if a.ZoneID == z.ID {
			a.CompartmentID = in.CompartmentID
			a.touch(b.nowUTC())
		}
	}

	return nil
}

// ListZoneTransferServers lists the OCI nameservers that transfer zones with external servers.
func (b *InMemoryBackend) ListZoneTransferServers(compartment string, o ListOpts) (listResult[transferWire], error) {
	b.mu.RLock("ListZoneTransferServers")
	defer b.mu.RUnlock()

	if err := b.checkCompartment(compartment); err != nil {
		return listResult[transferWire]{}, err
	}

	all := transferServers()

	pg, err := sortPage(all, o, "address", false, sorter[transferWire]{
		"address": func(a, c transferWire) int { return strings.Compare(a.Address, c.Address) },
	})
	if err != nil {
		return listResult[transferWire]{}, err
	}

	return finish(pg, len(all), func(t transferWire) transferWire { return t }), nil
}
