package azurearm

import (
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

// genericAdopter is implemented by providers that now model types an older snapshot kept in the generic store.
type genericAdopter interface {
	adoptGeneric(r *Resource) bool
}

// migrateGenericStore moves generic-store entries into the typed stores that now own their types. Entries are
// converted as stored, without validation; ones no provider takes stay generic.
func (h *Handler) migrateGenericStore() {
	b := h.Backend

	b.mu.RLock("migrateGenericStore")

	cands := make([]*Resource, 0, len(b.resources))
	for _, r := range b.resources {
		cands = append(cands, r)
	}

	b.mu.RUnlock()

	sort.Slice(cands, func(i, j int) bool {
		if li, lj := len(cands[i].ID.Types), len(cands[j].ID.Types); li != lj {
			return li < lj
		}

		return cands[i].ID.storeKey() < cands[j].ID.storeKey()
	})

	var adopted []string

	for _, r := range cands {
		p, ok := h.Registry.ProviderNamed(r.ID.Namespace)
		if !ok {
			continue
		}

		if a, isAdopter := p.(genericAdopter); isAdopter && len(r.ID.Names) == len(r.ID.Types) && a.adoptGeneric(r) {
			adopted = append(adopted, r.ID.storeKey())
		}
	}

	if len(adopted) == 0 {
		return
	}

	b.mu.Lock("migrateGenericStore")
	defer b.mu.Unlock()

	for _, k := range adopted {
		delete(b.resources, k)
	}

	b.byType.Store(nil)
}

func adoptProps(r *Resource) map[string]any {
	props := map[string]any{}

	maps.Copy(props, r.Properties)
	delete(props, fieldProvisioningState)

	cloned, _ := deepClone(props).(map[string]any)

	return cloned
}

// ---- network ----

func (p *NetworkProvider) adoptGeneric(r *Resource) bool {
	top := canonicalTop(firstType(r.ID))

	switch top {
	case netPLS, netRFs, netSEPs, netWatchers, netTaps, netDDoS, netNICs:
	default:
		return false
	}

	p.mu.Lock("adoptGeneric")
	defer p.mu.Unlock()

	if len(r.ID.Types) == 1 {
		return top != netNICs && p.adoptTop(top, r)
	}

	return len(r.ID.Types) == 2 && p.adoptKid(top, r)
}

func (p *NetworkProvider) legacyObj(r *Resource) netObj {
	o := netObj{
		Name: r.ID.LeafName(), Location: normLocation(r.Location), Tags: r.Tags, Props: adoptProps(r),
		Etag: newEtag(), GUID: uuid.NewString(), Aux: map[string]string{},
	}

	if len(r.SKU) > 0 || r.Kind != "" {
		o.Extra = map[string]any{}
		if len(r.SKU) > 0 {
			o.Extra[netFieldSKU] = r.SKU
		}

		if r.Kind != "" {
			o.Extra["kind"] = r.Kind
		}
	}

	return o
}

func (p *NetworkProvider) adoptTop(top string, r *Resource) bool {
	key := netKey(top, r.ID.ResourceGroup, r.ID.Names[0])
	if _, exists := p.items[key]; exists {
		return false
	}

	it := &netParent{
		Type: top, RG: r.ID.ResourceGroup, Sub: r.ID.SubscriptionID, Obj: p.legacyObj(r),
		Kids: map[string][]*netObj{},
	}

	switch top {
	case netRFs:
		it.Kids[rfRules] = p.legacyInline(it, rfPropRules)
	case netSEPs:
		it.Kids[sepDefs] = p.legacyInline(it, sepDefs)
	case netPLS:
		it.Obj.Aux[auxPLSAlias] = it.Obj.Name + "." + it.Obj.GUID + "." + it.Obj.Location + ".azure.privatelinkservice"
	case netDDoS:
		it.Obj.Props = map[string]any{}
	}

	p.items[key] = it

	return true
}

// legacyInline turns an inline child array of a stored parent into children, keeping each entry as written.
func (p *NetworkProvider) legacyInline(it *netParent, key string) []*netObj {
	var out []*netObj

	for _, e := range anyList(it.Obj.Props[key]) {
		m, _ := e.(map[string]any)
		if strOf(m, fieldName) == "" {
			continue
		}

		props := propsOf(m)
		if props == nil {
			props = map[string]any{}
		}

		out = append(out, &netObj{Name: strOf(m, fieldName), Etag: newEtag(), GUID: uuid.NewString(), Props: props})
	}

	delete(it.Obj.Props, key)

	return out
}

func (p *NetworkProvider) adoptKid(top string, r *Resource) bool {
	kt := canonicalKid(top, r.ID.Types[1])

	switch kt {
	case rfRules, sepDefs, wFlowLogs, nicTapCfgs:
	default:
		return false
	}

	parent, ok := p.top(top, r.ID.ResourceGroup, r.ID.Names[0])
	if !ok {
		if top == netNICs {
			return false
		}

		parent = &netParent{
			Type: top, RG: r.ID.ResourceGroup, Sub: r.ID.SubscriptionID, Kids: map[string][]*netObj{},
			Obj: netObj{
				Name: r.ID.Names[0], Etag: newEtag(), GUID: uuid.NewString(), Aux: map[string]string{},
				Props: map[string]any{},
			},
		}
		p.items[netKey(top, r.ID.ResourceGroup, r.ID.Names[0])] = parent
	}

	if _, idx := parent.kid(kt, r.ID.Names[1]); idx >= 0 {
		return false
	}

	o := p.legacyObj(r)
	if kt == wFlowLogs {
		guid, _ := p.flowTarget(strOf(o.Props, flPropTarget))
		o.Aux["target"], o.Aux["legacy"] = guid, "1"
	}

	parent.Kids[kt] = append(parent.Kids[kt], &o)

	return true
}

// ---- compute ----

func (p *ComputeProvider) adoptGeneric(r *Resource) bool {
	typ := canonicalCmpType(firstType(r.ID))

	switch typ {
	case cmpPPGs, cmpSSHKeys, cmpCapGroups, cmpHostGroups, cmpDiskEncSets, cmpDiskAccess, cmpRPCs, cmpGalleries:
	default:
		return false
	}

	p.mu.Lock("adoptGeneric")
	defer p.mu.Unlock()

	if len(r.ID.Types) == 1 {
		key := cmpKey(typ, r.ID.ResourceGroup, r.ID.Names[0])
		if _, exists := p.items[key]; exists {
			return false
		}

		p.items[key] = p.legacyRes(typ, r)

		return true
	}

	return p.adoptKidRes(typ, r)
}

func (p *ComputeProvider) legacyRes(typ string, r *Resource) *cmpRes {
	res := &cmpRes{
		Type: typ, RG: r.ID.ResourceGroup, Sub: r.ID.SubscriptionID, Name: r.ID.Names[0],
		Location: normLocation(r.Location), Etag: newEtag(), Tags: r.Tags, Props: adoptProps(r),
		Aux: map[string]string{}, Created: time.Now().UTC(), UID: uuid.NewString(), Kids: map[string]*cmpKid{},
	}

	if len(r.SKU) > 0 {
		res.SKU, _ = deepClone(r.SKU).(map[string]any)
	}

	if typ == cmpRPCs {
		res.Aux[rpFieldCollection] = res.UID
	}

	return res
}

func (p *ComputeProvider) adoptKidRes(typ string, r *Resource) bool {
	ref, ok := p.kidRefOf(r.ID)
	if !ok || ref.key == "" {
		return false
	}

	if ref.parent == nil {
		ref.parent = p.legacyRes(typ, &Resource{ID: ResourceID{
			SubscriptionID: r.ID.SubscriptionID, ResourceGroup: r.ID.ResourceGroup, Namespace: r.ID.Namespace,
			Types: r.ID.Types[:1], Names: r.ID.Names[:1],
		}})
		p.items[cmpKey(typ, r.ID.ResourceGroup, r.ID.Names[0])] = ref.parent
	}

	if _, exists := ref.parent.Kids[ref.key]; exists {
		return false
	}

	props := adoptProps(r)
	if len(r.SKU) > 0 {
		props[cmpFieldInternal+cmpFieldSKU] = deepClone(r.SKU)
	}

	ref.parent.Kids[ref.key] = &cmpKid{
		Type: ref.spec.rel, Names: ref.names, Etag: newEtag(), Created: time.Now().UTC(),
		Location: normLocation(r.Location), Tags: r.Tags, Props: props,
	}

	return true
}

// ---- container apps ----

func (p *ContainerAppsProvider) adoptGeneric(r *Resource) bool {
	t := acaTargetOf(r.ID)
	now := p.now().UTC()

	p.mu.Lock("adoptGeneric")
	defer p.mu.Unlock()

	switch {
	case isACASubKind(t.kind):
		env, ok := p.envs[acaKey(r.ID.ResourceGroup, t.names[0])]
		if !ok {
			return false
		}

		return adoptSub(&env.Subs, acaSubType(t.kind), t.names[1], r, now)
	case isACAAppSubKind(t.kind):
		app, ok := p.apps[acaKey(r.ID.ResourceGroup, t.names[0])]
		if !ok {
			return false
		}

		return adoptSub(&app.Subs, acaAppSubType(t.kind), t.names[1], r, now)
	case t.kind == acaKindPool:
		key := acaKey(r.ID.ResourceGroup, t.names[0])
		if _, exists := p.pools[key]; exists {
			return false
		}

		p.pools[key] = &storedACAPool{
			Created: now, Modified: now, Tags: r.Tags, Props: adoptProps(r), Sub: r.ID.SubscriptionID,
			Name: r.ID.LeafName(), ResourceGroup: r.ID.ResourceGroup, Location: acaRegion(r.Location),
		}

		return true
	}

	return false
}

func adoptSub(dst *map[string]*storedACASub, typ, name string, r *Resource, now time.Time) bool {
	key := acaSubKey(typ, name)
	if _, exists := (*dst)[key]; exists {
		return false
	}

	if *dst == nil {
		*dst = map[string]*storedACASub{}
	}

	(*dst)[key] = &storedACASub{
		Name: name, Type: typ, Created: now, Modified: now, Tags: r.Tags, Props: adoptProps(r),
		Location: normLocation(r.Location),
	}

	return true
}

// ---- container service ----

func (p *ContainerServiceProvider) adoptGeneric(r *Resource) bool {
	if aksTargetOf(r.ID).kind != aksKindSnapshot {
		return false
	}

	p.mu.Lock("adoptGeneric")
	defer p.mu.Unlock()

	key := aksSnapshotKey(r.ID.ResourceGroup, r.ID.LeafName())
	if _, exists := p.snapshots[key]; exists {
		return false
	}

	p.snapshots[key] = &storedAKSSnapshot{
		Created: time.Now().UTC(), Tags: r.Tags, Props: adoptProps(r), Name: r.ID.LeafName(),
		Sub: r.ID.SubscriptionID, ResourceGroup: r.ID.ResourceGroup, Location: r.Location,
	}

	return true
}

// ---- storage file shares ----

func (p *StorageProvider) adoptGeneric(r *Resource) bool {
	if len(r.ID.Types) != 3 || !strings.EqualFold(r.ID.Types[0], storageAccountsType) ||
		!strings.EqualFold(r.ID.Types[1], fileServicesType) || !strings.EqualFold(r.ID.Types[2], fileSharesSeg) ||
		p.fileShares == nil {
		return false
	}

	props := r.Properties
	in := FileShareInput{
		Metadata: stringTags(props["metadata"]), HasMetadata: props["metadata"] != nil,
		Tier: strOf(props, "accessTier"), Protocols: strOf(props, "enabledProtocols"),
		RootSquash: strOf(props, "rootSquash"),
	}

	if q, ok := props["shareQuota"].(float64); ok {
		n := int64(q)
		in.Quota = &n
	}

	_, _, err := p.fileShares.PutFileShare(r.ID.Names[0], r.ID.Names[2], in, true)

	return err == nil
}
