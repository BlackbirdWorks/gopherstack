package azurearm

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"regexp"
	"sort"
	"strings"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	namespaceMicrosoftNetwork = "Microsoft.Network"

	netVNets     = "virtualNetworks"
	netSubnets   = "subnets"
	netPeerings  = "virtualNetworkPeerings"
	netNSGs      = "networkSecurityGroups"
	netRules     = "securityRules"
	netDefRules  = "defaultSecurityRules"
	netPIPs      = "publicIPAddresses"
	netNICs      = "networkInterfaces"
	netIPConfigs = "ipConfigurations"
	netRTs       = "routeTables"
	netRoutes    = "routes"
	netZones     = "privateDnsZones"
	netLinks     = "virtualNetworkLinks"

	netCodeInvalid       = "InvalidRequestFormat"
	netCodeRef           = "InvalidResourceReference"
	netCodeName          = "InvalidResourceName"
	netCodeLocation      = "LocationRequired"
	netCodeCIDR          = "InvalidCIDRNotation"
	netCodeNotFound      = armErrorCodeResourceNotFound
	netFieldID           = "id"
	netFieldEtag         = "etag"
	netFieldSubnet       = "subnet"
	netFieldSKU          = "sku"
	netFieldZones        = "zones"
	netStatusSucceeded   = "Succeeded"
	netGlobalLocation    = "global"
	netPrefixSubnetsPath = "/subnets/"
	netFieldAddrPrefix   = "addressPrefix"
	netFieldAddrPrefixes = "addressPrefixes"
	netFieldResourceGUID = "resourceGuid"
	netFieldPrefixes     = "addressSpace"
	netFieldTier         = "tier"
	netNone              = "None"
	netEnabled           = "Enabled"
	netDisabled          = "Disabled"
	netFieldTenantID     = "tenantId"
	netPropStatus        = "status"
	netPropDescription   = "description"
	netPropPriority      = "priority"
	netPropAvailable     = "available"
	ruleTagVNet          = "VirtualNetwork"
	subnetJoinAction     = "Microsoft.Network/virtualNetworks/subnets/join/action"
)

var netNameRe = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9_])?$`)

const (
	netMaxName      = 80
	netMaxVNetName  = 64
	netMaxVNetNames = netMaxVNetName
)

func networkAPIVersions() []string {
	return []string{"2025-03-01", "2024-10-01", "2024-05-01", "2023-11-01", "2023-09-01", "2023-05-01", "2022-11-01"}
}

// netError is an ARM error with an explicit code and status.
type netError struct {
	code   string
	msg    string
	status int
}

func (e *netError) Error() string { return e.code + ": " + e.msg }

func netErr(status int, code, format string, args ...any) error {
	return &netError{status: status, code: code, msg: fmt.Sprintf(format, args...)}
}

func netBad(format string, args ...any) error {
	return netErr(http.StatusBadRequest, netCodeInvalid, format, args...)
}

func netErrorDetails(err error) (errorEntry, bool) {
	if ne, ok := errors.AsType[*netError](err); ok {
		return errorEntry{ne.code, ne.msg, ne.status}, true
	}

	return errorEntry{}, false
}

// netObj is the stored form of any Network resource or child. Props holds the caller's writable properties minus
// children and read-only fields; Aux holds server-assigned values (addresses, states).
type netObj struct {
	Tags     map[string]string `json:"tags,omitempty"`
	Props    map[string]any    `json:"props,omitempty"`
	Extra    map[string]any    `json:"extra,omitempty"`
	Aux      map[string]string `json:"aux,omitempty"`
	Name     string            `json:"name"`
	Location string            `json:"location,omitempty"`
	Etag     string            `json:"etag"`
	GUID     string            `json:"guid"`
}

// netParent is a top-level resource with its children grouped by child type.
type netParent struct {
	Kids map[string][]*netObj `json:"kids,omitempty"`
	Type string               `json:"type"`
	RG   string               `json:"rg"`
	Sub  string               `json:"sub"`
	Obj  netObj               `json:"obj"`
}

func (n *netParent) armID() string {
	return ResourceID{
		SubscriptionID: n.Sub, ResourceGroup: n.RG, Namespace: namespaceMicrosoftNetwork,
		Types: []string{n.Type}, Names: []string{n.Obj.Name},
	}.ARMID()
}

func (n *netParent) kid(typ, name string) (*netObj, int) {
	for i, k := range n.Kids[typ] {
		if strings.EqualFold(k.Name, name) {
			return k, i
		}
	}

	return nil, -1
}

// NetworkProvider implements ResourceProvider for Microsoft.Network.
type NetworkProvider struct {
	mu      *lockmetrics.RWMutex
	backend *InMemoryBackend
	items   map[string]*netParent
	lookup  func(context.Context, ResourceID) (map[string]any, error)
}

var _ ResourceProvider = (*NetworkProvider)(nil)

// NewNetworkProvider creates a provider; types it does not model fall through to backend's generic store.
func NewNetworkProvider(backend *InMemoryBackend) *NetworkProvider {
	return &NetworkProvider{
		mu: lockmetrics.New("azurearm.networkprovider"), backend: backend, items: map[string]*netParent{},
	}
}

// Namespace implements ResourceProvider.
func (p *NetworkProvider) Namespace() string { return namespaceMicrosoftNetwork }

// ResourceTypes implements ResourceProvider.
func (p *NetworkProvider) ResourceTypes() []ResourceTypeDef {
	v := networkAPIVersions()
	defs := make([]ResourceTypeDef, 0, 27+len(dnsRecordTypes)) //nolint:mnd // modelled type count

	for _, t := range []string{
		netVNets, netVNets + "/" + netSubnets, netVNets + "/" + netPeerings,
		netNSGs, netNSGs + "/" + netRules, netNSGs + "/" + netDefRules,
		netPIPs, netNICs, netNICs + "/" + netIPConfigs,
		netRTs, netRTs + "/" + netRoutes,
		netZones, netZones + "/" + netLinks,
		netLBs, netLBs + "/" + lbFrontends, netLBs + "/" + lbPools, netLBs + "/" + lbProbes, netLBs + "/" + lbRules,
		netLBs + "/" + lbNATRules, netLBs + "/" + lbNATPools, netLBs + "/" + lbOutbound, netLBs + "/" + lbNICs,
		netNATs, netPrefixes, netASGs, netPEs, netPEs + "/" + peZoneGroups, netNICs + "/" + nicLBs,
	} {
		defs = append(defs, ResourceTypeDef{Type: t, APIVersions: v, HasChildren: !strings.Contains(t, "/")})
	}

	for _, rt := range dnsRecordTypes {
		defs = append(defs, ResourceTypeDef{Type: netZones + "/" + rt, APIVersions: v})
	}

	return defs
}

// ListKeys implements ResourceProvider.
func (p *NetworkProvider) ListKeys(context.Context, ResourceID) (map[string]any, error) {
	return nil, ErrResourceNotFound
}

// Reset implements ResourceProvider.
func (p *NetworkProvider) Reset() {
	p.mu.Lock("Reset")
	p.items = map[string]*netParent{}
	p.mu.Unlock()
}

// DeleteResourcesInGroup implements ResourceProvider; references from other groups to the removed resources are
// dropped so no association dangles.
func (p *NetworkProvider) DeleteResourcesInGroup(_ context.Context, resourceGroup string) {
	p.mu.Lock("DeleteResourcesInGroup")
	defer p.mu.Unlock()

	for k, it := range p.items {
		if strings.EqualFold(it.RG, resourceGroup) {
			delete(p.items, k)
		}
	}

	p.pruneDanglingLocked()
}

func netKey(typ, rg, name string) string {
	return strings.ToLower(typ + "/" + rg + "/" + name)
}

func (p *NetworkProvider) top(typ, rg, name string) (*netParent, bool) {
	it, ok := p.items[netKey(typ, rg, name)]

	return it, ok
}

func (p *NetworkProvider) ofType(typ string) []*netParent {
	var out []*netParent

	for _, it := range p.items {
		if it.Type == typ {
			out = append(out, it)
		}
	}

	sort.Slice(out, func(i, j int) bool {
		return netKey(out[i].Type, out[i].RG, out[i].Obj.Name) < netKey(out[j].Type, out[j].RG, out[j].Obj.Name)
	})

	return out
}

func (n *netParent) id(sub string) ResourceID {
	return ResourceID{
		SubscriptionID: sub, ResourceGroup: n.RG, Namespace: namespaceMicrosoftNetwork,
		Types: []string{n.Type}, Names: []string{n.Obj.Name},
	}
}

func newEtag() string { return `W/"` + uuid.NewString() + `"` }

func normLocation(s string) string {
	return strings.ToLower(strings.ReplaceAll(s, " ", ""))
}

func validNetName(name string, limit int) error {
	if len(name) > limit || !netNameRe.MatchString(name) {
		return netErr(http.StatusBadRequest, netCodeName,
			"Resource name %s is invalid. The name can be up to %d characters long, it must begin with a word "+
				"character, and it must end with a word character or with '_'. The name may contain word "+
				"characters or '.', '-', '_'.", name, limit)
	}

	return nil
}

func propsOf(body map[string]any) map[string]any {
	props, _ := body[fieldProperties].(map[string]any)

	return props
}

// takeProps returns a copy of the body's properties without the listed keys.
func takeProps(body map[string]any, drop ...string) map[string]any {
	out := map[string]any{}

	maps.Copy(out, propsOf(body))

	for _, k := range drop {
		delete(out, k)
	}

	return out
}

func strList(v any) []string {
	if ss, ok := v.([]string); ok {
		return ss
	}

	arr, _ := v.([]any)
	out := make([]string, 0, len(arr))

	for _, e := range arr {
		if s, ok := e.(string); ok {
			out = append(out, s)
		}
	}

	return out
}

func strOf(m map[string]any, k string) string {
	s, _ := m[k].(string)

	return s
}

func subID(m map[string]any, k string) string {
	if sub, ok := m[k].(map[string]any); ok {
		return strOf(sub, netFieldID)
	}

	return ""
}

// netRef resolves an ARM id string to an existing top-level resource of typ.
func (p *NetworkProvider) netRef(idStr, typ string) (*netParent, bool) {
	rid, err := ParseGenericResourcePath(idStr)
	if err != nil || !strings.EqualFold(rid.Namespace, namespaceMicrosoftNetwork) || len(rid.Types) == 0 ||
		!strings.EqualFold(rid.Types[0], typ) {
		return nil, false
	}

	return p.top(typ, rid.ResourceGroup, rid.Names[0])
}

func refMissing(idStr string) error {
	return netErr(http.StatusBadRequest, netCodeRef,
		"Resource %s referenced by the request was not found. Please make sure that the referenced resource exists, "+
			"and that both resources are in the same region.", idStr)
}

func (p *NetworkProvider) envelope(id ResourceID, typ string, o *netObj, props map[string]any) map[string]any {
	out := map[string]any{
		netFieldID: id.ARMID(), fieldName: o.Name, fieldType: namespaceMicrosoftNetwork + "/" + typ,
		netFieldEtag: o.Etag, fieldProperties: props,
	}

	if o.Location != "" {
		out[fieldLocation] = o.Location
	}

	if o.Tags != nil {
		out[fieldTags] = o.Tags
	}

	maps.Copy(out, o.Extra)

	return out
}

// Get implements ResourceProvider.
func (p *NetworkProvider) Get(_ context.Context, id ResourceID) (map[string]any, error) {
	p.mu.RLock("Get")
	defer p.mu.RUnlock()

	return p.getLocked(id)
}

// Put implements ResourceProvider.
func (p *NetworkProvider) Put(_ context.Context, id ResourceID, body map[string]any) (map[string]any, error) {
	out, _, err := p.put(id, body)

	return out, err
}

// Delete implements ResourceProvider.
func (p *NetworkProvider) Delete(_ context.Context, id ResourceID) error {
	return p.remove(id)
}

// List implements ResourceProvider.
func (p *NetworkProvider) List(_ context.Context, id ResourceID) ([]map[string]any, error) {
	if len(id.Types) == 1 && !p.modeled(id.Types[0]) {
		return p.backend.listGenericResources(id), nil
	}

	p.mu.RLock("List")
	defer p.mu.RUnlock()

	return p.listLocked(id)
}

func (p *NetworkProvider) modeled(typ string) bool {
	switch strings.ToLower(typ) {
	case strings.ToLower(netVNets), strings.ToLower(netNSGs), strings.ToLower(netPIPs), strings.ToLower(netNICs),
		strings.ToLower(netRTs), strings.ToLower(netZones), strings.ToLower(netLBs), strings.ToLower(netNATs),
		strings.ToLower(netPrefixes), strings.ToLower(netASGs), strings.ToLower(netPEs):
		return true
	}

	return false
}

func canonicalTop(typ string) string {
	for _, t := range []string{
		netVNets, netNSGs, netPIPs, netNICs, netRTs, netZones, netLBs, netNATs, netPrefixes, netASGs, netPEs,
	} {
		if strings.EqualFold(t, typ) {
			return t
		}
	}

	return ""
}

func canonicalKid(parent, typ string) string {
	cands := map[string][]string{
		netVNets: {netSubnets, netPeerings}, netNSGs: {netRules, netDefRules}, netNICs: {netIPConfigs, nicLBs},
		netLBs: append(append([]string{}, lbKidTypes...), lbNICs), netPEs: {peZoneGroups},
		netRTs: {netRoutes}, netZones: append([]string{netLinks}, dnsRecordTypes...),
	}

	for _, t := range cands[parent] {
		if strings.EqualFold(t, typ) {
			return t
		}
	}

	return ""
}

func (p *NetworkProvider) put(id ResourceID, body map[string]any) (map[string]any, bool, error) {
	top := canonicalTop(firstType(id))
	if top == "" {
		out, created, err := p.backend.putGenericResource(id, body)

		return out, created, err
	}

	p.mu.Lock("Put")
	defer p.mu.Unlock()

	var (
		out     map[string]any
		created bool
		err     error
	)

	switch {
	case len(id.Types) == 1:
		out, created, err = p.putTop(top, id, body)
	case len(id.Types) == 2 && canonicalKid(top, id.Types[1]) != "":
		out, created, err = p.putKid(top, canonicalKid(top, id.Types[1]), id, body)
	default:
		err = ErrResourceNotFound
	}

	if err != nil {
		return nil, false, err
	}

	p.reconcileLocked()

	return out, created, nil
}

func firstType(id ResourceID) string {
	if len(id.Types) == 0 {
		return ""
	}

	return id.Types[0]
}

func (p *NetworkProvider) remove(id ResourceID) error {
	top := canonicalTop(firstType(id))
	if top == "" {
		return p.backend.deleteGenericResource(id)
	}

	p.mu.Lock("Delete")
	defer p.mu.Unlock()

	var err error

	switch {
	case len(id.Types) == 1:
		err = p.removeTop(top, id)
	case len(id.Types) == 2 && canonicalKid(top, id.Types[1]) != "":
		err = p.removeKid(top, canonicalKid(top, id.Types[1]), id)
	default:
		err = ErrResourceNotFound
	}

	if err != nil {
		return err
	}

	p.reconcileLocked()

	return nil
}

func (p *NetworkProvider) getLocked(id ResourceID) (map[string]any, error) {
	top := canonicalTop(firstType(id))
	if top == "" {
		return p.backend.getGenericResource(id)
	}

	parent, ok := p.top(top, id.ResourceGroup, id.Names[0])
	if !ok {
		return nil, ErrResourceNotFound
	}

	if len(id.Types) == 1 {
		return p.bodyTop(id, parent), nil
	}

	kt := canonicalKid(top, id.Types[1])
	if len(id.Types) != 2 || kt == "" {
		return nil, ErrResourceNotFound
	}

	return p.getKid(parent, kt, id)
}

func (p *NetworkProvider) listLocked(id ResourceID) ([]map[string]any, error) {
	top := canonicalTop(firstType(id))

	if len(id.Names) == 0 {
		out := []map[string]any{}

		for _, it := range p.ofType(top) {
			if id.ResourceGroup != "" && !resourceGroupsEqual(it.RG, id.ResourceGroup) {
				continue
			}

			out = append(out, p.bodyTop(it.id(id.SubscriptionID), it))
		}

		return out, nil
	}

	parent, ok := p.top(top, id.ResourceGroup, id.Names[0])
	if !ok {
		return nil, ErrResourceNotFound
	}

	if top == netZones && strings.EqualFold(lastType(id), dnsAllRecords) {
		return p.listAllRecords(parent, id), nil
	}

	kt := canonicalKid(top, lastType(id))
	if kt == "" {
		return nil, ErrResourceNotFound
	}

	return p.listKids(parent, kt, id), nil
}

func lastType(id ResourceID) string { return id.LeafType() }

func (p *NetworkProvider) putTop(top string, id ResourceID, body map[string]any) (map[string]any, bool, error) {
	if err := p.validateTopCommon(top, id, body); err != nil {
		return nil, false, err
	}

	switch top {
	case netVNets:
		return p.putVNet(id, body)
	case netNSGs:
		return p.putNSG(id, body)
	case netPIPs:
		return p.putPIP(id, body)
	case netNICs:
		return p.putNIC(id, body)
	case netRTs:
		return p.putRT(id, body)
	case netLBs:
		return p.putLB(id, body)
	case netNATs:
		return p.putNAT(id, body)
	case netPrefixes:
		return p.putPrefix(id, body)
	case netASGs:
		return p.putASG(id, body)
	case netPEs:
		return p.putPE(id, body)
	default:
		return p.putZone(id, body)
	}
}

func (p *NetworkProvider) validateTopCommon(top string, id ResourceID, body map[string]any) error {
	limit := netMaxName

	switch top {
	case netVNets:
		limit = netMaxVNetName
	case netZones:
		return validZoneName(id.Names[0])
	}

	if err := validNetName(id.Names[0], limit); err != nil {
		return err
	}

	if loc, _ := body[fieldLocation].(string); loc == "" {
		if _, exists := p.top(top, id.ResourceGroup, id.Names[0]); !exists {
			return netErr(
				http.StatusBadRequest,
				netCodeLocation,
				"The location property is required for this definition.",
			)
		}
	}

	return nil
}

// upsertTop stores a new or replaced top-level resource, preserving identity fields.
func (p *NetworkProvider) upsertTop(top string, id ResourceID, body map[string]any) (*netParent, *netObj, bool) {
	name := id.Names[0]
	key := netKey(top, id.ResourceGroup, name)
	old, existed := p.items[key]

	obj := netObj{Name: name, Etag: newEtag(), Tags: stringTags(body[fieldTags])}

	if loc, _ := body[fieldLocation].(string); loc != "" {
		obj.Location = normLocation(loc)
	}

	if existed {
		obj.GUID = old.Obj.GUID
		obj.Aux = old.Obj.Aux

		if obj.Location == "" {
			obj.Location = old.Obj.Location
		}
	} else {
		obj.GUID = uuid.NewString()
	}

	if obj.Aux == nil {
		obj.Aux = map[string]string{}
	}

	it := &netParent{Type: top, RG: id.ResourceGroup, Sub: id.SubscriptionID, Obj: obj, Kids: map[string][]*netObj{}}
	if existed {
		it.Kids = old.Kids
	}

	p.items[key] = it

	return it, &it.Obj, !existed
}

func (p *NetworkProvider) bodyTop(id ResourceID, it *netParent) map[string]any {
	switch it.Type {
	case netVNets:
		return p.bodyVNet(id, it)
	case netNSGs:
		return p.bodyNSG(id, it)
	case netPIPs:
		return p.bodyPIP(id, it)
	case netNICs:
		return p.bodyNIC(id, it)
	case netRTs:
		return p.bodyRT(id, it)
	case netLBs:
		return p.bodyLB(id, it)
	case netNATs:
		return p.bodyNAT(id, it)
	case netPrefixes:
		return p.bodyPrefix(id, it)
	case netASGs:
		return p.bodyASG(id, it)
	case netPEs:
		return p.bodyPE(id, it)
	default:
		return p.bodyZone(id, it)
	}
}

func (p *NetworkProvider) putKid(top, kt string, id ResourceID, body map[string]any) (map[string]any, bool, error) {
	parent, ok := p.top(top, id.ResourceGroup, id.Names[0])
	if !ok {
		return nil, false, ErrResourceNotFound
	}

	if err := validNetName(id.Names[1], netMaxName); top != netZones && err != nil {
		return nil, false, err
	}

	var (
		created bool
		err     error
	)

	switch {
	case top == netLBs && kt == lbNICs, top == netNICs && kt == nicLBs:
		err = ErrResourceNotFound
	case top == netLBs:
		created, err = p.writeLBKid(parent, kt, id.Names[1], body)
	case top == netPEs:
		created, err = p.writeZoneGroup(parent, id.Names[1], body)
	default:
		created, err = p.writeKidClassic(parent, top, kt, id, body)
	}

	if err != nil {
		return nil, false, err
	}

	out, getErr := p.getKid(parent, kt, id)

	return out, created, getErr
}

func (p *NetworkProvider) writeKidClassic(
	parent *netParent, _, kt string, id ResourceID, body map[string]any,
) (bool, error) {
	var (
		created bool
		err     error
	)

	switch kt {
	case netSubnets:
		created, err = p.writeSubnet(parent, id.Names[1], body)
	case netPeerings:
		created, err = p.writePeering(parent, id, id.Names[1], body)
	case netRules:
		created, err = p.writeRule(parent, id.Names[1], body)
	case netRoutes:
		created, err = p.writeRoute(parent, id.Names[1], body)
	case netLinks:
		created, err = p.writeLink(parent, id.Names[1], body)
	case netDefRules, netIPConfigs:
		err = ErrResourceNotFound
	default:
		created, err = p.writeRecord(parent, kt, id.Names[1], body)
	}

	return created, err
}

func (p *NetworkProvider) removeKid(top, kt string, id ResourceID) error {
	parent, ok := p.top(top, id.ResourceGroup, id.Names[0])
	if !ok {
		return ErrResourceNotFound
	}

	removed, idx := parent.kid(kt, id.Names[1])
	if idx < 0 {
		return ErrResourceNotFound
	}

	if err := p.checkKidDelete(parent, kt, id); err != nil {
		return err
	}

	parent.Kids[kt] = append(parent.Kids[kt][:idx], parent.Kids[kt][idx+1:]...)
	p.afterKidRemoved(parent, kt, removed)
	p.touch(parent)

	return nil
}

func (p *NetworkProvider) touch(parent *netParent) {
	if parent.Type == netZones {
		parent.Obj.Etag = uuid.NewString()

		return
	}

	parent.Obj.Etag = newEtag()
}

func (p *NetworkProvider) getKid(parent *netParent, kt string, id ResourceID) (map[string]any, error) {
	k, idx := parent.kid(kt, id.Names[1])
	if idx < 0 {
		if kt == netDefRules {
			return p.getDefaultRule(parent, id)
		}

		if kt == netIPConfigs {
			return p.getIPConfig(parent, id)
		}

		return nil, ErrResourceNotFound
	}

	return p.bodyKid(parent, kt, id, k), nil
}

func (p *NetworkProvider) listKids(parent *netParent, kt string, id ResourceID) []map[string]any {
	out := []map[string]any{}

	switch kt {
	case netDefRules:
		return append(out, defaultRulesFor(parent)...)
	case netIPConfigs:
		return p.listIPConfigs(parent)
	case lbNICs:
		return p.listLBNICs(parent, id)
	case nicLBs:
		return p.listNICLBs(parent, id)
	}

	for _, k := range parent.Kids[kt] {
		kid := id
		kid.Names = append(append([]string{}, id.Names[:1]...), k.Name)
		kid.Types = append(append([]string{}, id.Types[:1]...), kt)
		out = append(out, p.bodyKid(parent, kt, kid, k))
	}

	return out
}

func (p *NetworkProvider) bodyKid(parent *netParent, kt string, id ResourceID, k *netObj) map[string]any {
	switch kt {
	case netSubnets:
		return p.bodySubnet(id, k)
	case netPeerings:
		return p.bodyPeering(parent, id, k)
	case netRules:
		return p.bodyRule(id, k)
	case netRoutes:
		return p.bodyRoute(id, k)
	case netLinks:
		return p.bodyLink(parent, id, k)
	case peZoneGroups:
		return p.bodyZoneGroup(parent, id, k)
	default:
		if parent.Type == netLBs {
			return p.bodyLBKid(parent, kt, id, k)
		}

		return p.bodyRecord(parent, kt, id, k)
	}
}

func (p *NetworkProvider) removeTop(top string, id ResourceID) error {
	parent, ok := p.top(top, id.ResourceGroup, id.Names[0])
	if !ok {
		return ErrResourceNotFound
	}

	if err := p.checkTopDelete(parent, id); err != nil {
		return err
	}

	delete(p.items, netKey(top, id.ResourceGroup, id.Names[0]))
	p.afterTopRemoved(parent, id)

	return nil
}

// PatchTags replaces the tags of a top-level resource, as the UpdateTags operations do.
func (p *NetworkProvider) PatchTags(id ResourceID, tags map[string]string) (map[string]any, error) {
	top := canonicalTop(firstType(id))
	if top == "" || len(id.Types) != 1 || top == netZones {
		return nil, ErrResourceNotFound
	}

	p.mu.Lock("PatchTags")
	defer p.mu.Unlock()

	it, ok := p.top(top, id.ResourceGroup, id.Names[0])
	if !ok {
		return nil, ErrResourceNotFound
	}

	it.Obj.Tags = tags
	p.touch(it)

	return p.bodyTop(id, it), nil
}

// CheckIPAddressAvailability implements the virtual network action of the same name.
func (p *NetworkProvider) CheckIPAddressAvailability(id ResourceID, ip string) (map[string]any, error) {
	p.mu.RLock("CheckIPAddressAvailability")
	defer p.mu.RUnlock()

	v, ok := p.top(netVNets, id.ResourceGroup, id.Names[0])
	if !ok {
		return nil, ErrResourceNotFound
	}

	return p.checkIPAvailability(v, ip)
}
