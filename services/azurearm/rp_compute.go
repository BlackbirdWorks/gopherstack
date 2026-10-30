package azurearm

import (
	"context"
	"maps"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	namespaceMicrosoftCompute = "Microsoft.Compute"

	cmpVMs    = "virtualMachines"
	cmpDisks  = "disks"
	cmpSnaps  = "snapshots"
	cmpAvSets = "availabilitySets"
	cmpVMSS   = "virtualMachineScaleSets"

	cmpCodeInvalid     = "InvalidParameter"
	cmpCodeNotAllowed  = "OperationNotAllowed"
	cmpCodeChange      = "PropertyChangeNotAllowed"
	cmpCodeConflict    = "Conflict"
	cmpFieldZones      = "zones"
	cmpFieldSKU        = "sku"
	cmpFieldVMID       = "vmId"
	cmpFieldTimeMade   = "timeCreated"
	cmpFieldID         = "id"
	cmpFieldManagedBy  = "managedBy"
	cmpFieldManagedDsk = "managedDisk"
	cmpFieldDiskSizeGB = "diskSizeGB"
	cmpMaxNameLen      = 80
	cmpMaxVMNameLen    = 64
	cmpKeyDisplay      = "displayStatus"
	cmpKeyStatuses     = "statuses"
	cmpKeyOSType       = "osType"
	cmpKeyTier         = "tier"
	cmpKeyLocations    = "locations"
	cmpKeyResourceType = "resourceType"
	cmpKeyFamily       = "family"
	cmpKeyCaps         = "capabilities"
	cmpKeyLevel        = "level"
	cmpTrue            = "true"
	cmpFieldPrimary    = "primary"
	cmpFieldCaching    = "caching"
	cmpCachingNone     = "None"
	cmpCachingRW       = "ReadWrite"
	cmpStateCompleted  = "Completed"
	cmpFieldMemory     = "memory"
	cmpVersion10       = "1.0"
	cmpSKUPremium      = "Premium_LRS"
)

var cmpNameRe = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9_])?$`)

func computeAPIVersions() []string {
	return []string{
		"2026-04-01", "2026-03-02", "2025-04-01", "2025-01-02", "2024-11-01", "2024-07-01", "2024-03-02",
		"2024-03-01", "2023-10-02",
	}
}

func cmpInvalid(format string, args ...any) error {
	return netErr(http.StatusBadRequest, cmpCodeInvalid, format, args...)
}

func cmpNotAllowed(format string, args ...any) error {
	return netErr(http.StatusConflict, cmpCodeNotAllowed, format, args...)
}

func cmpChanged(prop string) error {
	return netErr(http.StatusConflict, cmpCodeChange, "Changing property '%s' is not allowed.", prop)
}

// cmpRes is the stored form of any Microsoft.Compute resource.
type cmpRes struct {
	Created   time.Time          `json:"created"`
	Tags      map[string]string  `json:"tags,omitempty"`
	Props     map[string]any     `json:"props,omitempty"`
	Extra     map[string]any     `json:"extra,omitempty"`
	SKU       map[string]any     `json:"sku,omitempty"`
	Aux       map[string]string  `json:"aux,omitempty"`
	Name      string             `json:"name"`
	Type      string             `json:"type"`
	RG        string             `json:"rg"`
	Sub       string             `json:"sub"`
	Location  string             `json:"location"`
	Etag      string             `json:"etag"`
	UID       string             `json:"uid"`
	Identity  resourceIdentity   `json:"identity"`
	Instances []*cmpInstance     `json:"instances,omitempty"`
	Kids      map[string]*cmpKid `json:"kids,omitempty"`
	Zones     []string           `json:"zones,omitempty"`
}

func (r *cmpRes) id() ResourceID {
	return ResourceID{
		SubscriptionID: r.Sub, ResourceGroup: r.RG, Namespace: namespaceMicrosoftCompute,
		Types: []string{r.Type}, Names: []string{r.Name},
	}
}

func (r *cmpRes) armID() string { return r.id().ARMID() }

func cmpKey(typ, rg, name string) string { return strings.ToLower(typ + "/" + rg + "/" + name) }

// ComputeProvider implements ResourceProvider for Microsoft.Compute. It is state only: no virtual machine runs.
// Lock order is compute then network; the network provider never calls back into compute.
type ComputeProvider struct {
	mu      *lockmetrics.RWMutex
	backend *InMemoryBackend
	net     *NetworkProvider
	ids     *identityService
	items   map[string]*cmpRes
}

var _ ResourceProvider = (*ComputeProvider)(nil)

// NewComputeProvider creates a provider that references NICs through net and identities through ids.
func NewComputeProvider(backend *InMemoryBackend, net *NetworkProvider, ids *identityService) *ComputeProvider {
	return &ComputeProvider{
		mu: lockmetrics.New(
			"azurearm.computeprovider",
		), backend: backend, net: net, ids: ids, items: map[string]*cmpRes{},
	}
}

// Namespace implements ResourceProvider.
func (p *ComputeProvider) Namespace() string { return namespaceMicrosoftCompute }

// ResourceTypes implements ResourceProvider.
func (p *ComputeProvider) ResourceTypes() []ResourceTypeDef {
	v := computeAPIVersions()

	return []ResourceTypeDef{
		{Type: cmpVMs, APIVersions: v},
		{Type: cmpDisks, APIVersions: v},
		{Type: cmpSnaps, APIVersions: v},
		{Type: cmpAvSets, APIVersions: v},
		{Type: cmpVMSS, APIVersions: v, HasChildren: true},
		{Type: cmpVMSS + "/" + cmpVMs, APIVersions: v},
		{Type: cmpVMs + "/" + kidExtensions, APIVersions: v},
		{Type: cmpVMs + "/" + kidRunCommands, APIVersions: v},
		{Type: cmpVMSS + "/" + kidExtensions, APIVersions: v},
		{Type: cmpImages, APIVersions: v},
		{Type: cmpGalleries, APIVersions: v, HasChildren: true},
		{Type: cmpGalleries + "/" + kidImages, APIVersions: v, HasChildren: true},
		{Type: cmpGalleries + "/" + kidVersions, APIVersions: v},
	}
}

// ListKeys implements ResourceProvider.
func (p *ComputeProvider) ListKeys(context.Context, ResourceID) (map[string]any, error) {
	return nil, ErrResourceNotFound
}

// Reset implements ResourceProvider.
func (p *ComputeProvider) Reset() {
	p.mu.Lock("Reset")
	p.items = map[string]*cmpRes{}
	p.mu.Unlock()
}

// DeleteResourcesInGroup implements ResourceProvider; network interfaces of removed machines are detached.
func (p *ComputeProvider) DeleteResourcesInGroup(_ context.Context, resourceGroup string) {
	p.mu.Lock("DeleteResourcesInGroup")
	defer p.mu.Unlock()

	for k, r := range p.items {
		if !strings.EqualFold(r.RG, resourceGroup) {
			continue
		}

		if r.Type == cmpVMs {
			p.net.releaseNICs(r.armID(), vmNICIDs(r), false)
		}

		delete(p.items, k)
	}

	for _, r := range p.items {
		if r.Type == cmpDisks && r.Aux[auxManagedBy] != "" {
			if _, ok := p.item(cmpVMs, r.Aux[auxManagedBy]); !ok {
				delete(r.Aux, auxManagedBy)
			}
		}
	}
}

func (p *ComputeProvider) top(typ, rg, name string) (*cmpRes, bool) {
	r, ok := p.items[cmpKey(typ, rg, name)]

	return r, ok
}

// item resolves an ARM id string to a stored resource of typ.
func (p *ComputeProvider) item(typ, idStr string) (*cmpRes, bool) {
	rid, err := ParseGenericResourcePath(idStr)
	if err != nil || !strings.EqualFold(rid.Namespace, namespaceMicrosoftCompute) || len(rid.Types) != 1 ||
		!strings.EqualFold(rid.Types[0], typ) {
		return nil, false
	}

	return p.top(typ, rid.ResourceGroup, rid.Names[0])
}

func (p *ComputeProvider) ofType(typ string) []*cmpRes {
	var out []*cmpRes

	for _, r := range p.items {
		if r.Type == typ {
			out = append(out, r)
		}
	}

	sort.Slice(out, func(i, j int) bool {
		return cmpKey(out[i].Type, out[i].RG, out[i].Name) < cmpKey(out[j].Type, out[j].RG, out[j].Name)
	})

	return out
}

func isVMSSInstanceID(id ResourceID) bool {
	return len(id.Types) == 2 && strings.EqualFold(id.Types[0], cmpVMSS) && strings.EqualFold(id.Types[1], cmpVMs)
}

func canonicalCmpType(typ string) string {
	for _, t := range []string{cmpVMs, cmpDisks, cmpSnaps, cmpAvSets, cmpVMSS, cmpImages, cmpGalleries} {
		if strings.EqualFold(t, typ) {
			return t
		}
	}

	return ""
}

func validCmpName(name string, limit int) error {
	if len(name) > limit || !cmpNameRe.MatchString(name) {
		return netErr(http.StatusBadRequest, netCodeName,
			"Resource name %s is invalid. The name can be up to %d characters long, it must begin with a word "+
				"character, and it must end with a word character or with '_'. The name may contain word "+
				"characters or '.', '-', '_'.", name, limit)
	}

	return nil
}

// Get implements ResourceProvider.
func (p *ComputeProvider) Get(_ context.Context, id ResourceID) (map[string]any, error) {
	p.mu.RLock("Get")
	defer p.mu.RUnlock()

	return p.getLocked(id, false)
}

// GetExpanded returns the resource with its instance view, as GET with $expand=instanceView does.
func (p *ComputeProvider) GetExpanded(id ResourceID) (map[string]any, error) {
	p.mu.RLock("GetExpanded")
	defer p.mu.RUnlock()

	return p.getLocked(id, true)
}

func (p *ComputeProvider) getLocked(id ResourceID, expand bool) (map[string]any, error) {
	typ := canonicalCmpType(firstType(id))
	if typ == "" || len(id.Names) == 0 {
		return nil, ErrResourceNotFound
	}

	r, ok := p.top(typ, id.ResourceGroup, id.Names[0])
	if !ok {
		return nil, ErrResourceNotFound
	}

	if isVMSSInstanceID(id) {
		return p.vmssInstanceGet(r, id)
	}

	if len(id.Types) > 1 {
		return p.kidGet(id, "")
	}

	return p.render(r, expand), nil
}

func (p *ComputeProvider) render(r *cmpRes, expand bool) map[string]any {
	var props map[string]any

	switch r.Type {
	case cmpVMs:
		props = p.vmProps(r, expand)
	case cmpDisks:
		props = p.diskProps(r)
	case cmpSnaps:
		props = p.snapshotProps(r)
	case cmpAvSets:
		props = p.avSetProps(r)
	case cmpImages:
		props = p.imageProps(r)
	case cmpGalleries:
		props = p.galleryProps(r)
	default:
		props = p.vmssProps(r)
	}

	out := map[string]any{
		cmpFieldID: r.armID(), fieldName: r.Name, fieldType: namespaceMicrosoftCompute + "/" + r.Type,
		fieldLocation: r.Location, fieldTags: tagsOrEmpty(r.Tags), fieldProperties: props,
	}

	if len(r.Zones) > 0 {
		out[cmpFieldZones] = r.Zones
	}

	if r.SKU != nil {
		out[cmpFieldSKU] = deepClone(r.SKU)
	}

	if ident := p.ids.body(r.Identity); ident != nil {
		out[fieldIdentity] = ident
	}

	if mb := r.Aux[auxManagedBy]; mb != "" {
		out[cmpFieldManagedBy] = mb
	}

	if res := p.vmResources(r); len(res) > 0 {
		out["resources"] = res
	}

	for k, v := range r.Extra {
		out[k] = deepClone(v)
	}

	return out
}

// etagOf returns the stored etag of id for If-Match checks.
func (p *ComputeProvider) etagOf(id ResourceID) (string, bool) {
	p.mu.RLock("etagOf")
	defer p.mu.RUnlock()

	typ := canonicalCmpType(firstType(id))
	if typ == "" || len(id.Types) != 1 || len(id.Names) == 0 {
		return "", false
	}

	r, ok := p.top(typ, id.ResourceGroup, id.Names[0])
	if !ok {
		return "", false
	}

	return r.Etag, true
}

// List implements ResourceProvider.
func (p *ComputeProvider) List(_ context.Context, id ResourceID) ([]map[string]any, error) {
	return p.list(id, false)
}

func (p *ComputeProvider) list(id ResourceID, statusOnly bool) ([]map[string]any, error) {
	typ := canonicalCmpType(firstType(id))
	if typ == "" {
		return nil, ErrResourceNotFound
	}

	p.mu.RLock("List")
	defer p.mu.RUnlock()

	if len(id.Names) > 0 {
		r, ok := p.top(typ, id.ResourceGroup, id.Names[0])
		if ok && isVMSSInstanceID(id) {
			return p.vmssInstanceList(r, id), nil
		}

		return p.kidList(id)
	}

	out := []map[string]any{}

	for _, r := range p.ofType(typ) {
		if id.ResourceGroup != "" && !resourceGroupsEqual(r.RG, id.ResourceGroup) {
			continue
		}

		out = append(out, p.render(r, statusOnly))
	}

	return out, nil
}

// Put implements ResourceProvider.
func (p *ComputeProvider) Put(_ context.Context, id ResourceID, body map[string]any) (map[string]any, error) {
	typ := canonicalCmpType(firstType(id))
	if typ == "" || len(id.Names) < 1 || (len(id.Types) == 1 && len(id.Names) != 1) {
		return nil, ErrResourceNotFound
	}

	if _, err := p.backend.GetResourceGroup(id.ResourceGroup); err != nil {
		return nil, err
	}

	p.mu.Lock("Put")
	defer p.mu.Unlock()

	if len(id.Types) > 1 {
		return p.putKid(id, body)
	}

	var (
		res *cmpRes
		err error
	)

	switch typ {
	case cmpVMs:
		res, err = p.putVM(id, body)
	case cmpDisks:
		res, err = p.putDisk(id, body, false)
	case cmpSnaps:
		res, err = p.putDisk(id, body, true)
	case cmpAvSets:
		res, err = p.putAvSet(id, body)
	case cmpImages:
		res, err = p.putImage(id, body)
	case cmpGalleries:
		res, err = p.putGallery(id, body)
	default:
		res, err = p.putVMSS(id, body)
	}

	if err != nil {
		return nil, err
	}

	return p.render(res, false), nil
}

// Delete implements ResourceProvider.
func (p *ComputeProvider) Delete(_ context.Context, id ResourceID) error {
	typ := canonicalCmpType(firstType(id))
	if typ == "" || len(id.Names) == 0 {
		return ErrResourceNotFound
	}

	p.mu.Lock("Delete")
	defer p.mu.Unlock()

	r, ok := p.top(typ, id.ResourceGroup, id.Names[0])
	if !ok {
		return ErrResourceNotFound
	}

	if isVMSSInstanceID(id) {
		return p.vmssInstanceDelete(r, id)
	}

	if len(id.Types) > 1 {
		return p.deleteKid(id)
	}

	switch typ {
	case cmpVMs:
		p.deleteVM(r)
	case cmpVMSS:
		p.deleteVMSSMembers(r)
	case cmpGalleries:
		if len(r.Kids) > 0 {
			return cannotDeleteNested()
		}
	case cmpDisks:
		if err := p.checkDiskDelete(r); err != nil {
			return err
		}
	case cmpAvSets:
		if err := p.checkAvSetDelete(r); err != nil {
			return err
		}
	}

	delete(p.items, cmpKey(typ, id.ResourceGroup, id.Names[0]))

	return nil
}

// PatchResource applies an Update (PATCH): tags, identity and properties replace what the body names.
func (p *ComputeProvider) PatchResource(id ResourceID, body map[string]any) (map[string]any, error) {
	typ := canonicalCmpType(firstType(id))
	if typ == "" || len(id.Names) < 1 {
		return nil, ErrResourceNotFound
	}

	if len(id.Types) > 1 {
		return p.patchKid(id, body)
	}

	if len(id.Types) != 1 || len(id.Names) != 1 {
		return nil, ErrResourceNotFound
	}

	p.mu.RLock("PatchResource")
	r, ok := p.top(typ, id.ResourceGroup, id.Names[0])

	if !ok {
		p.mu.RUnlock()

		return nil, ErrResourceNotFound
	}

	merged := p.render(r, false)
	p.mu.RUnlock()

	for _, k := range []string{fieldTags, fieldIdentity, cmpFieldSKU, cmpFieldZones} {
		if v, found := body[k]; found {
			merged[k] = v
		}
	}

	maps.Copy(mapOf(merged[fieldProperties]), mapOf(body[fieldProperties]))

	return p.Put(context.Background(), id, merged)
}

// PatchTags replaces the tags of a resource.
func (p *ComputeProvider) PatchTags(id ResourceID, tags map[string]string) (map[string]any, error) {
	return p.PatchResource(id, map[string]any{fieldTags: tagsToAny(tags)})
}

func tagsToAny(tags map[string]string) map[string]any {
	out := make(map[string]any, len(tags))
	for k, v := range tags {
		out[k] = v
	}

	return out
}

// upsert builds the record a Put leaves behind, keeping identity fields of an existing resource.
func (p *ComputeProvider) upsert(typ string, id ResourceID, body map[string]any) (*cmpRes, *cmpRes, error) {
	old, existed := p.top(typ, id.ResourceGroup, id.Names[0])

	loc := normLocation(strOf(body, fieldLocation))
	if loc == "" && !existed {
		return nil, nil, netErr(http.StatusBadRequest, netCodeLocation,
			"The location property is required for this definition.")
	}

	r := &cmpRes{
		Type: typ, RG: id.ResourceGroup, Sub: id.SubscriptionID, Name: id.Names[0], Location: loc, Etag: newEtag(),
		Tags: stringTags(body[fieldTags]), Aux: map[string]string{}, Created: time.Now().UTC(), UID: uuid.NewString(),
	}

	if !existed {
		return r, nil, nil
	}

	r.Created, r.UID = old.Created, old.UID
	r.Aux, r.Instances, r.Kids = old.Aux, old.Instances, old.Kids

	if loc == "" {
		r.Location = old.Location
	}

	if r.Location != old.Location {
		return nil, nil, cmpChanged(fieldLocation)
	}

	return r, old, nil
}

func (p *ComputeProvider) applyIdentity(r, old *cmpRes, body map[string]any) error {
	var prev resourceIdentity
	if old != nil {
		prev = old.Identity
	}

	_, present := body[fieldIdentity]

	ident, err := p.ids.apply(body[fieldIdentity], present, prev)
	if err != nil {
		return err
	}

	r.Identity = ident

	return nil
}

// extraFields keeps the top-level fields the provider does not interpret so they round-trip.
func extraFields(body map[string]any, keys ...string) map[string]any {
	var out map[string]any

	for _, k := range keys {
		if v, ok := body[k]; ok {
			if out == nil {
				out = map[string]any{}
			}

			out[k] = deepClone(v)
		}
	}

	return out
}

func deepClone(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, e := range t {
			out[k] = deepClone(e)
		}

		return out
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = deepClone(e)
		}

		return out
	}

	return v
}

func cloneProps(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}

	c, _ := deepClone(m).(map[string]any)

	return c
}

func cmpNotImplemented(what string) error {
	return netErr(http.StatusNotImplemented, "NotImplemented", "%s is not implemented by the emulator.", what)
}

// vmResources lists the extension children a virtual machine reports as resources[].
func (p *ComputeProvider) vmResources(r *cmpRes) []any {
	if r.Type != cmpVMs {
		return nil
	}

	exts := kidsOf(r, kidExtensions)
	res := make([]any, 0, len(exts))

	for _, k := range exts {
		res = append(res, renderExtension(p, r, k, ""))
	}

	return res
}
