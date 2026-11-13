package ocidns

import (
	"cmp"
	"net/netip"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	maxEndpointName = 255
	reservedLow     = 2
)

var endpointNameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]*$`)

type attachedViewWire struct {
	ViewID string `json:"viewId"`
}

type ruleWire struct {
	SourceEndpointName      string   `json:"sourceEndpointName"`
	Action                  string   `json:"action"`
	ClientAddressConditions []string `json:"clientAddressConditions"`
	QnameCoverConditions    []string `json:"qnameCoverConditions"`
	DestinationAddresses    []string `json:"destinationAddresses"`
}

// EndpointWire is the wire form of a VNIC resolver endpoint.
type EndpointWire struct {
	TimeCreated        time.Time                 `json:"timeCreated"`
	TimeUpdated        time.Time                 `json:"timeUpdated"`
	FreeformTags       map[string]string         `json:"freeformTags"`
	DefinedTags        map[string]map[string]any `json:"definedTags"`
	SecurityAttributes map[string]map[string]any `json:"securityAttributes"`
	ForwardingAddress  *string                   `json:"forwardingAddress"`
	ListeningAddress   *string                   `json:"listeningAddress"`
	ResolverID         string                    `json:"resolverId"`
	ID                 string                    `json:"id"`
	CompartmentID      string                    `json:"compartmentId"`
	LifecycleState     string                    `json:"lifecycleState"`
	Self               string                    `json:"self"`
	EndpointType       string                    `json:"endpointType"`
	SubnetID           string                    `json:"subnetId"`
	Name               string                    `json:"name"`
	NsgIDs             []string                  `json:"nsgIds"`
	IsForwarding       bool                      `json:"isForwarding"`
	IsListening        bool                      `json:"isListening"`
}

// ResolverWire is the wire form of a resolver.
type ResolverWire struct {
	TimeCreated    time.Time                 `json:"timeCreated"`
	TimeUpdated    time.Time                 `json:"timeUpdated"`
	FreeformTags   map[string]string         `json:"freeformTags"`
	DefinedTags    map[string]map[string]any `json:"definedTags"`
	CompartmentID  string                    `json:"compartmentId"`
	DisplayName    string                    `json:"displayName"`
	ID             string                    `json:"id"`
	LifecycleState string                    `json:"lifecycleState"`
	Self           string                    `json:"self"`
	AttachedVcnID  string                    `json:"attachedVcnId"`
	DefaultViewID  string                    `json:"defaultViewId"`
	Endpoints      []EndpointWire            `json:"endpoints"`
	AttachedViews  []attachedViewWire        `json:"attachedViews"`
	Rules          []ruleWire                `json:"rules"`
	IsProtected    bool                      `json:"isProtected"`
}

// ResolverSummaryWire is the list form of a resolver.
type ResolverSummaryWire struct {
	TimeCreated    time.Time                 `json:"timeCreated"`
	TimeUpdated    time.Time                 `json:"timeUpdated"`
	FreeformTags   map[string]string         `json:"freeformTags"`
	DefinedTags    map[string]map[string]any `json:"definedTags"`
	CompartmentID  string                    `json:"compartmentId"`
	DisplayName    string                    `json:"displayName"`
	ID             string                    `json:"id"`
	LifecycleState string                    `json:"lifecycleState"`
	Self           string                    `json:"self"`
	AttachedVcnID  string                    `json:"attachedVcnId"`
	DefaultViewID  string                    `json:"defaultViewId"`
	IsProtected    bool                      `json:"isProtected"`
}

func optStr(s string) *string {
	if s == "" {
		return nil
	}

	return &s
}

func (b *InMemoryBackend) endpointWire(e *Endpoint) EndpointWire {
	return EndpointWire{
		TimeCreated: e.Created, TimeUpdated: e.Updated, FreeformTags: e.free(), DefinedTags: e.defined(),
		ForwardingAddress: optStr(e.ForwardingAddress), ListeningAddress: optStr(e.ListeningAddress), Name: e.Name,
		ID: e.ID, ResolverID: e.ResolverID, CompartmentID: e.CompartmentID, LifecycleState: stateActive,
		Self: b.self("resolvers/"+e.ResolverID+"/endpoints", e.Name), EndpointType: endpointVnic,
		SubnetID: e.SubnetID, NsgIDs: nonNil(e.NsgIDs), SecurityAttributes: map[string]map[string]any{},
		IsForwarding: e.IsForwarding, IsListening: e.IsListening,
	}
}

func (b *InMemoryBackend) resolverEndpoints(resolverID string) []*Endpoint {
	var out []*Endpoint

	for _, e := range b.endpoints {
		if e.ResolverID == resolverID {
			out = append(out, e)
		}
	}

	slices.SortFunc(out, func(a, c *Endpoint) int { return cmp.Compare(a.Seq, c.Seq) })

	return out
}

func (b *InMemoryBackend) resolverWire(r *Resolver) ResolverWire {
	w := ResolverWire{
		TimeCreated: r.Created, TimeUpdated: r.Updated, FreeformTags: r.free(), DefinedTags: r.defined(),
		CompartmentID: r.CompartmentID, DisplayName: r.DisplayName, ID: r.ID, LifecycleState: stateActive,
		Self: b.self("resolvers", r.ID), AttachedVcnID: r.VcnID, DefaultViewID: r.DefaultViewID,
		Endpoints: []EndpointWire{}, AttachedViews: []attachedViewWire{}, Rules: []ruleWire{},
	}

	for _, e := range b.resolverEndpoints(r.ID) {
		w.Endpoints = append(w.Endpoints, b.endpointWire(e))
	}

	for _, v := range r.AttachedViews {
		w.AttachedViews = append(w.AttachedViews, attachedViewWire{ViewID: v})
	}

	for _, rl := range r.Rules {
		w.Rules = append(w.Rules, ruleWire{
			SourceEndpointName:      rl.SourceEndpointName,
			Action:                  actionFwd,
			ClientAddressConditions: nonNil(rl.ClientAddressConditions),
			QnameCoverConditions: nonNil(
				rl.QnameCoverConditions,
			),
			DestinationAddresses: nonNil(rl.DestinationAddresses),
		})
	}

	return w
}

func (b *InMemoryBackend) resolverSummary(r *Resolver) ResolverSummaryWire {
	return ResolverSummaryWire{
		TimeCreated: r.Created, TimeUpdated: r.Updated, FreeformTags: r.free(), DefinedTags: r.defined(),
		CompartmentID: r.CompartmentID, DisplayName: r.DisplayName, ID: r.ID, LifecycleState: stateActive,
		Self: b.self("resolvers", r.ID), AttachedVcnID: r.VcnID, DefaultViewID: r.DefaultViewID,
	}
}

// SyncVcns creates a resolver and a protected default view for every VCN and drops those of deleted VCNs.
func (b *InMemoryBackend) SyncVcns() {
	b.mu.RLock("SyncVcns")
	src := b.vcns
	b.mu.RUnlock()

	if src == nil {
		return
	}

	vcns := src()

	b.mu.Lock("SyncVcns")
	defer b.mu.Unlock()

	live := map[string]bool{}

	for _, v := range vcns {
		live[v.ID] = true

		if !b.hasResolver(v.ID) {
			b.newResolver(v)
		}
	}

	for id, r := range b.resolvers {
		if !live[r.VcnID] {
			b.dropResolver(id, r)
		}
	}
}

func (b *InMemoryBackend) hasResolver(vcnID string) bool {
	for _, r := range b.resolvers {
		if r.VcnID == vcnID {
			return true
		}
	}

	return false
}

func (b *InMemoryBackend) newResolver(v VcnInfo) {
	view := b.newView(v.CompartmentID, v.DisplayName, true, Tags{})
	now := b.nowUTC()
	r := &Resolver{
		ID: newOCID("dnsresolver"), CompartmentID: v.CompartmentID, DisplayName: v.DisplayName, VcnID: v.ID,
		DefaultViewID: view.ID, AttachedViews: []string{}, Rules: []ResolverRule{}, Created: now, Updated: now,
		Seq: b.nextSeq(),
	}
	r.touch(now)
	b.resolvers[r.ID] = r
}

func (b *InMemoryBackend) dropResolver(id string, r *Resolver) {
	for eid, e := range b.endpoints {
		if e.ResolverID == id {
			delete(b.endpoints, eid)
		}
	}

	for zid, z := range b.zones {
		if z.ViewID == r.DefaultViewID {
			b.dropZone(zid)
		}
	}

	delete(b.views, r.DefaultViewID)
	delete(b.resolvers, id)
}

func (b *InMemoryBackend) dropZone(id string) {
	for aid, a := range b.attachments {
		if a.ZoneID == id {
			delete(b.attachments, aid)
		}
	}

	delete(b.zones, id)
}

func (b *InMemoryBackend) resolverByID(id string) (*Resolver, *APIError) {
	r, ok := b.resolvers[id]
	if !ok {
		return nil, errNotFound(id)
	}

	return r, nil
}

// GetResolver returns a resolver.
func (b *InMemoryBackend) GetResolver(id string) (ResolverWire, tag, error) {
	b.mu.RLock("GetResolver")
	defer b.mu.RUnlock()

	r, err := b.resolverByID(id)
	if err != nil {
		return ResolverWire{}, tag{}, err
	}

	return b.resolverWire(r), tagOf(r.ID, r.meta), nil
}

// ResolverFilter selects resolvers in a list.
type ResolverFilter struct {
	Compart string
	Name    string
	ID      string
	State   string
}

var resolverSorters = sorter[*Resolver]{ //nolint:gochecknoglobals // immutable comparator table
	keyDisplayName: byString(
		func(r *Resolver) string { return r.DisplayName },
		func(r *Resolver) int64 { return r.Seq },
	),
	keyTimeCreated: byCreated(
		func(r *Resolver) time.Time { return r.Created },
		func(r *Resolver) int64 { return r.Seq },
	),
}

// ListResolvers lists the resolvers of a compartment.
func (b *InMemoryBackend) ListResolvers(f ResolverFilter, o ListOpts) (listResult[ResolverSummaryWire], error) {
	b.mu.RLock("ListResolvers")
	defer b.mu.RUnlock()

	if err := b.checkCompartment(f.Compart); err != nil {
		return listResult[ResolverSummaryWire]{}, err
	}

	var all []*Resolver

	for _, r := range b.resolvers {
		if r.CompartmentID == f.Compart && (f.Name == "" || strs.Equal(r.DisplayName, f.Name)) &&
			(f.ID == "" || r.ID == f.ID) && (f.State == "" || strings.EqualFold(f.State, stateActive)) {
			all = append(all, r)
		}
	}

	pg, err := sortPage(all, o, keyTimeCreated, true, resolverSorters)
	if err != nil {
		return listResult[ResolverSummaryWire]{}, err
	}

	return finish(pg, len(all), b.resolverSummary), nil
}

// RuleIn is a resolver rule in an UpdateResolver body.
type RuleIn struct {
	SourceEndpointName      string   `json:"sourceEndpointName"`
	Action                  string   `json:"action"`
	ClientAddressConditions []string `json:"clientAddressConditions"`
	QnameCoverConditions    []string `json:"qnameCoverConditions"`
	DestinationAddresses    []string `json:"destinationAddresses"`
}

// UpdateResolverIn is the body of UpdateResolver.
type UpdateResolverIn struct {
	Tags
	DisplayName   *string            `json:"displayName"`
	Rules         *[]RuleIn          `json:"rules"`
	AttachedViews []attachedViewWire `json:"attachedViews"`
}

func checkRuleShape(in RuleIn) *APIError {
	if a := strings.ToUpper(in.Action); a != "" && a != actionFwd {
		return errParam("rules action must be FORWARD")
	}

	if len(in.DestinationAddresses) == 0 || in.SourceEndpointName == "" {
		return errMissing("destinationAddresses and sourceEndpointName are required for each rule")
	}

	for _, d := range in.DestinationAddresses {
		if _, err := netip.ParseAddr(d); err != nil {
			return errParam("rules destinationAddresses must be IP addresses")
		}
	}

	for _, c := range in.ClientAddressConditions {
		if !isIPOrCIDR(c) {
			return errParam("rules clientAddressConditions must be IP addresses or CIDR blocks")
		}
	}

	for _, q := range in.QnameCoverConditions {
		if _, ok := normName(q, false); !ok {
			return errParam("rules qnameCoverConditions must be domain names")
		}
	}

	return nil
}

func isIPOrCIDR(s string) bool {
	if _, err := netip.ParsePrefix(s); err == nil {
		return true
	}

	_, err := netip.ParseAddr(s)

	return err == nil
}

func (b *InMemoryBackend) checkRules(r *Resolver, rules []RuleIn) ([]ResolverRule, *APIError) {
	out := make([]ResolverRule, 0, len(rules))

	for i, in := range rules {
		if err := checkRuleShape(in); err != nil {
			return nil, err
		}

		ep := b.endpointByName(r.ID, in.SourceEndpointName)
		if ep == nil || !ep.IsForwarding {
			return nil, errParam("rules[" + strconv.Itoa(i) + "].sourceEndpointName must name a forwarding endpoint")
		}

		out = append(out, ResolverRule{
			SourceEndpointName: ep.Name, ClientAddressConditions: in.ClientAddressConditions,
			QnameCoverConditions: in.QnameCoverConditions, DestinationAddresses: in.DestinationAddresses,
		})
	}

	return out, nil
}

func (b *InMemoryBackend) endpointByName(resolverID, name string) *Endpoint {
	for _, e := range b.endpoints {
		if e.ResolverID == resolverID && strs.Equal(e.Name, name) {
			return e
		}
	}

	return nil
}

// UpdateResolver changes the name, tags, attached views and rules of a resolver.
func (b *InMemoryBackend) UpdateResolver(id string, c cond, in UpdateResolverIn) (ResolverWire, tag, error) {
	b.mu.Lock("UpdateResolver")
	defer b.mu.Unlock()

	r, err := b.resolverByID(id)
	if err != nil {
		return ResolverWire{}, tag{}, err
	}

	if err = firstAPIErr(c.check(r.ID, r.meta), b.checkTags(in.Tags)); err != nil {
		return ResolverWire{}, tag{}, err
	}

	var views []string

	if in.AttachedViews != nil {
		seen := map[string]bool{}

		for _, v := range in.AttachedViews {
			if _, ok := b.views[v.ViewID]; !ok {
				return ResolverWire{}, tag{}, errNotFound(v.ViewID)
			}

			if seen[v.ViewID] {
				return ResolverWire{}, tag{}, errParam("attachedViews must not repeat a view")
			}

			seen[v.ViewID] = true

			views = append(views, v.ViewID)
		}
	}

	var rules []ResolverRule

	if in.Rules != nil {
		if rules, err = b.checkRules(r, *in.Rules); err != nil {
			return ResolverWire{}, tag{}, err
		}
	}

	if in.DisplayName != nil {
		r.DisplayName = *in.DisplayName
	}

	if in.AttachedViews != nil {
		r.AttachedViews = nonNil(views)
	}

	if in.Rules != nil {
		r.Rules = nonNil(rules)
	}

	r.Tags.merge(in.Tags)
	now := b.nowUTC()
	r.Updated = now
	r.touch(now)

	return b.resolverWire(r), tagOf(r.ID, r.meta), nil
}

// ChangeResolverCompartment moves a resolver.
func (b *InMemoryBackend) ChangeResolverCompartment(id string, c cond, in ChangeCompartmentIn) error {
	b.mu.Lock("ChangeResolverCompartment")
	defer b.mu.Unlock()

	r, err := b.resolverByID(id)
	if err != nil {
		return err
	}

	if err = c.check(r.ID, r.meta); err != nil {
		return err
	}

	if err = b.checkCompartment(in.CompartmentID); err != nil {
		return err
	}

	r.CompartmentID = in.CompartmentID
	r.touch(b.nowUTC())

	return nil
}

// CreateEndpointIn is the body of CreateResolverEndpoint.
type CreateEndpointIn struct {
	Tags
	RetryToken        string   `json:"-"`
	IsForwarding      *bool    `json:"isForwarding"`
	IsListening       *bool    `json:"isListening"`
	Name              string   `json:"name"`
	EndpointType      string   `json:"endpointType"`
	SubnetID          string   `json:"subnetId"`
	ForwardingAddress string   `json:"forwardingAddress"`
	ListeningAddress  string   `json:"listeningAddress"`
	NsgIDs            []string `json:"nsgIds"`
}

func (b *InMemoryBackend) usedAddresses(subnetID string) map[netip.Addr]bool {
	used := map[netip.Addr]bool{}

	for _, e := range b.endpoints {
		if e.SubnetID != subnetID {
			continue
		}

		for _, a := range []string{e.ForwardingAddress, e.ListeningAddress} {
			if ip, err := netip.ParseAddr(a); err == nil {
				used[ip] = true
			}
		}
	}

	return used
}

// pickAddress validates a requested address against the subnet, or assigns the first free one.
func pickAddress(want string, prefix netip.Prefix, used map[netip.Addr]bool, what string) (string, *APIError) {
	if want != "" {
		ip, err := netip.ParseAddr(want)
		if err != nil {
			return "", errParam(what + " is not a valid IP address")
		}

		if prefix.IsValid() && !prefix.Contains(ip) {
			return "", errParam(what + " is not within the subnet")
		}

		if used[ip] {
			return "", errConflict(what + " " + want + " is already in use")
		}

		used[ip] = true

		return want, nil
	}

	ip := prefix.Masked().Addr()
	for range reservedLow {
		ip = ip.Next()
	}

	for ; prefix.Contains(ip); ip = ip.Next() {
		if !used[ip] {
			used[ip] = true

			return ip.String(), nil
		}
	}

	return "", errConflict("the subnet has no free IP address")
}

func (b *InMemoryBackend) endpointSubnet(r *Resolver, subnetID string) (netip.Prefix, *APIError) {
	if b.subnet == nil {
		return netip.Prefix{}, nil
	}

	sn, ok := b.subnet(subnetID)
	if !ok {
		return netip.Prefix{}, errNotFound(subnetID)
	}

	if sn.VcnID != r.VcnID {
		return netip.Prefix{}, errParam("subnetId must be part of the VCN the resolver is attached to")
	}

	p, err := netip.ParsePrefix(sn.Cidr)
	if err != nil {
		return netip.Prefix{}, nil //nolint:nilerr // an unparsable subnet CIDR leaves addresses unchecked
	}

	return p, nil
}

func checkEndpointShape(in CreateEndpointIn) *APIError {
	if et := strings.ToUpper(in.EndpointType); et != "" && et != endpointVnic {
		return errParam("endpointType must be VNIC")
	}

	if in.Name == "" || in.IsForwarding == nil || in.IsListening == nil || in.SubnetID == "" {
		return errMissing("name, isForwarding, isListening and subnetId are required")
	}

	if len(in.Name) > maxEndpointName || !endpointNameRe.MatchString(in.Name) {
		return errParam("name must be 1 to 255 letters, digits, '.', '_' or '-'")
	}

	if !*in.IsForwarding && !*in.IsListening {
		return errParam("an endpoint must forward, listen, or both")
	}

	if (!*in.IsForwarding && in.ForwardingAddress != "") || (!*in.IsListening && in.ListeningAddress != "") {
		return errParam("an address is only valid for the role that is enabled")
	}

	return nil
}

func (b *InMemoryBackend) validateEndpoint(r *Resolver, in CreateEndpointIn) (netip.Prefix, *APIError) {
	if err := checkEndpointShape(in); err != nil {
		return netip.Prefix{}, err
	}

	if b.endpointByName(r.ID, in.Name) != nil {
		return netip.Prefix{}, errExists("An endpoint named " + in.Name + " already exists")
	}

	return b.endpointSubnet(r, in.SubnetID)
}

// CreateEndpoint creates a VNIC endpoint on a resolver.
func (b *InMemoryBackend) CreateEndpoint(resolverID string, in CreateEndpointIn) (EndpointWire, tag, error) {
	b.mu.Lock("CreateEndpoint")
	defer b.mu.Unlock()

	r, err := b.resolverByID(resolverID)
	if err != nil {
		return EndpointWire{}, tag{}, err
	}

	if id, ok := b.retry.Lookup("CreateResolverEndpoint/"+resolverID, in.RetryToken, b.nowUTC()); ok {
		e, live := b.endpoints[id]
		if !live {
			return EndpointWire{}, tag{}, errInvalidatedToken()
		}

		return b.endpointWire(e), tagOf(e.ID, e.meta), nil
	}

	if aerr := b.checkTags(in.Tags); aerr != nil {
		return EndpointWire{}, tag{}, aerr
	}

	prefix, aerr := b.validateEndpoint(r, in)
	if aerr != nil {
		return EndpointWire{}, tag{}, aerr
	}

	used := b.usedAddresses(in.SubnetID)
	e := &Endpoint{
		ID: newOCID("dnsresolverendpoint"), ResolverID: r.ID, CompartmentID: r.CompartmentID, Name: in.Name,
		SubnetID: in.SubnetID, NsgIDs: in.NsgIDs, IsForwarding: *in.IsForwarding, IsListening: *in.IsListening,
		Seq: b.nextSeq(), Tags: in.Tags,
	}

	if e.IsForwarding {
		if e.ForwardingAddress, aerr = pickAddress(
			in.ForwardingAddress,
			prefix,
			used,
			"forwardingAddress",
		); aerr != nil {
			return EndpointWire{}, tag{}, aerr
		}
	}

	if e.IsListening {
		if e.ListeningAddress, aerr = pickAddress(in.ListeningAddress, prefix, used, "listeningAddress"); aerr != nil {
			return EndpointWire{}, tag{}, aerr
		}
	}

	now := b.nowUTC()
	e.Created, e.Updated = now, now
	e.touch(now)
	b.endpoints[e.ID] = e
	b.retry.Remember("CreateResolverEndpoint/"+resolverID, in.RetryToken, e.ID, now)

	return b.endpointWire(e), tagOf(e.ID, e.meta), nil
}

func (b *InMemoryBackend) findEndpoint(resolverID, name string) (*Endpoint, *APIError) {
	if _, err := b.resolverByID(resolverID); err != nil {
		return nil, err
	}

	if e := b.endpointByName(resolverID, name); e != nil {
		return e, nil
	}

	return nil, errNotFound(name)
}

// GetEndpoint returns a resolver endpoint.
func (b *InMemoryBackend) GetEndpoint(resolverID, name string) (EndpointWire, tag, error) {
	b.mu.RLock("GetEndpoint")
	defer b.mu.RUnlock()

	e, err := b.findEndpoint(resolverID, name)
	if err != nil {
		return EndpointWire{}, tag{}, err
	}

	return b.endpointWire(e), tagOf(e.ID, e.meta), nil
}

var endpointSorters = sorter[*Endpoint]{ //nolint:gochecknoglobals // immutable comparator table
	keyName: byString(
		func(e *Endpoint) string { return strings.ToLower(e.Name) },
		func(e *Endpoint) int64 { return e.Seq },
	),
	keyTimeCreated: byCreated(
		func(e *Endpoint) time.Time { return e.Created },
		func(e *Endpoint) int64 { return e.Seq },
	),
}

// ListEndpoints lists the endpoints of a resolver.
func (b *InMemoryBackend) ListEndpoints(resolverID, name, state string, o ListOpts) (listResult[EndpointWire], error) {
	b.mu.RLock("ListEndpoints")
	defer b.mu.RUnlock()

	if _, err := b.resolverByID(resolverID); err != nil {
		return listResult[EndpointWire]{}, err
	}

	var all []*Endpoint

	for _, e := range b.resolverEndpoints(resolverID) {
		if (name == "" || strs.Equal(e.Name, name)) && (state == "" || strings.EqualFold(state, stateActive)) {
			all = append(all, e)
		}
	}

	pg, err := sortPage(all, o, keyTimeCreated, true, endpointSorters)
	if err != nil {
		return listResult[EndpointWire]{}, err
	}

	return finish(pg, len(all), b.endpointWire), nil
}

// UpdateEndpointIn is the body of UpdateResolverEndpoint.
type UpdateEndpointIn struct {
	Tags
	NsgIDs       *[]string `json:"nsgIds"`
	EndpointType string    `json:"endpointType"`
}

// UpdateEndpoint changes the tags and network security groups of an endpoint.
func (b *InMemoryBackend) UpdateEndpoint(
	resolverID, name string,
	c cond,
	in UpdateEndpointIn,
) (EndpointWire, tag, error) {
	b.mu.Lock("UpdateEndpoint")
	defer b.mu.Unlock()

	e, err := b.findEndpoint(resolverID, name)
	if err != nil {
		return EndpointWire{}, tag{}, err
	}

	if err = firstAPIErr(c.check(e.ID, e.meta), b.checkTags(in.Tags)); err != nil {
		return EndpointWire{}, tag{}, err
	}

	if et := strings.ToUpper(in.EndpointType); et != "" && et != endpointVnic {
		return EndpointWire{}, tag{}, errParam("endpointType must be VNIC")
	}

	if in.NsgIDs != nil {
		e.NsgIDs = *in.NsgIDs
	}

	e.Tags.merge(in.Tags)
	now := b.nowUTC()
	e.Updated = now
	e.touch(now)

	return b.endpointWire(e), tagOf(e.ID, e.meta), nil
}

// DeleteEndpoint deletes an endpoint that no resolver rule uses.
func (b *InMemoryBackend) DeleteEndpoint(resolverID, name string, c cond) error {
	b.mu.Lock("DeleteEndpoint")
	defer b.mu.Unlock()

	e, err := b.findEndpoint(resolverID, name)
	if err != nil {
		return err
	}

	if err = c.check(e.ID, e.meta); err != nil {
		return err
	}

	r := b.resolvers[resolverID]
	for _, rl := range r.Rules {
		if strs.Equal(rl.SourceEndpointName, e.Name) {
			return errConflict("The endpoint is the source of a resolver rule")
		}
	}

	delete(b.endpoints, e.ID)

	return nil
}
