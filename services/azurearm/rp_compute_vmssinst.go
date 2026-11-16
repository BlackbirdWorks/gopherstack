package azurearm

import (
	"slices"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

const (
	scaleInDefault  = "Default"
	scaleInOldest   = "OldestVM"
	scaleInNewest   = "NewestVM"
	cmpFieldScaleIn = "scaleInPolicy"
	cmpFieldRules   = "rules"
	cmpFieldNICs    = "networkInterfaces"

	cmpFieldLatestModel = "latestModelApplied"
	cmpFieldDiagProfile = "diagnosticsProfile"
	vmssNICScopeLen     = 2
	vmssNICInstanceLen  = 3
)

func vmssInstanceARMID(r *cmpRes, in *cmpInstance) string {
	id := r.id()
	id.Types = append(id.Types, cmpVMs)
	id.Names = append(id.Names, in.ID)

	return id.ARMID()
}

func normalizeScaleInPolicy(props map[string]any) error {
	pol, ok := props[cmpFieldScaleIn].(map[string]any)
	if !ok {
		return nil
	}

	pol = cloneProps(pol)

	rules := make([]any, 0)

	for _, name := range strList(pol[cmpFieldRules]) {
		canon, valid := canonicalIn([]string{scaleInDefault, scaleInOldest, scaleInNewest}, name)
		if !valid {
			return cmpInvalid("The scaleInPolicy.rules value '%s' is not valid; allowed values are Default, "+
				"NewestVM and OldestVM.", name)
		}

		rules = append(rules, canon)
	}

	if len(rules) > 0 {
		pol[cmpFieldRules] = rules
	}

	props[cmpFieldScaleIn] = pol

	return nil
}

func vmssScaleInRule(r *cmpRes) string {
	if rules := strList(mapOf(r.Props[cmpFieldScaleIn])[cmpFieldRules]); len(rules) > 0 {
		return rules[0]
	}

	return scaleInDefault
}

func (in *cmpInstance) protected() bool { return in.ProtectScaleIn || in.ProtectActions }

func (in *cmpInstance) number() int { return atoiOr(in.ID, 0) }

// vmssScaleInVictims picks n instances to delete: OldestVM and NewestVM by creation order, Default balanced across
// zones first and then newest first (Learn: virtual-machine-scale-sets-scale-in-policy).
func vmssScaleInVictims(r *cmpRes, n int) ([]*cmpInstance, error) {
	if n <= 0 {
		return nil, nil
	}

	pool := make([]*cmpInstance, 0, len(r.Instances))

	for _, in := range r.Instances {
		if !in.protected() {
			pool = append(pool, in)
		}
	}

	if len(pool) < n {
		return nil, cmpNotAllowed("The scale set %s cannot be scaled in to %d instances because %d instances are "+
			"protected from scale-in.", r.Name, len(r.Instances)-n, len(r.Instances)-len(pool))
	}

	slices.SortFunc(pool, func(a, b *cmpInstance) int { return a.number() - b.number() })

	switch vmssScaleInRule(r) {
	case scaleInOldest:
		return pool[:n], nil
	case scaleInNewest:
		slices.Reverse(pool)

		return pool[:n], nil
	}

	return balancedVictims(r, pool, n), nil
}

func balancedVictims(r *cmpRes, pool []*cmpInstance, n int) []*cmpInstance {
	counts := map[string]int{}
	for _, in := range r.Instances {
		counts[in.Zone]++
	}

	out := make([]*cmpInstance, 0, n)

	for range n {
		pick := -1

		for i := range slices.Backward(pool) {
			if pick < 0 || counts[pool[i].Zone] > counts[pool[pick].Zone] {
				pick = i
			}
		}

		counts[pool[pick].Zone]--
		out = append(out, pool[pick])
		pool = slices.Delete(pool, pick, pick+1)
	}

	return out
}

func vmssPickZone(r *cmpRes, added []*cmpInstance) string {
	if len(r.Zones) == 0 {
		return ""
	}

	counts := map[string]int{}

	for _, in := range slices.Concat(r.Instances, added) {
		counts[in.Zone]++
	}

	best := r.Zones[0]

	for _, z := range r.Zones[1:] {
		if counts[z] < counts[best] {
			best = z
		}
	}

	return best
}

// vmssReconcile resizes the instance list to capacity, applies the model to instances the upgrade policy covers and
// keeps each instance's network interfaces in Microsoft.Network in step.
func (p *ComputeProvider) vmssReconcile(r, old *cmpRes, capacity int) error {
	model := 1
	changed := false

	if old != nil {
		model = atoiOr(old.Aux[auxModel], 1)

		if !jsonEqual(old.Props[cmpFieldVMProfile], r.Props[cmpFieldVMProfile]) ||
			strOf(old.SKU, fieldName) != strOf(r.SKU, fieldName) {
			model++
			changed = true
		}
	}

	existing := slices.Clone(r.Instances)

	victims, err := vmssScaleInVictims(r, len(existing)-capacity)
	if err != nil {
		return err
	}

	next := atoiOr(r.Aux[auxNextInstance], 0)

	added, err := p.vmssGrow(r, next, model, capacity-len(existing))
	if err != nil {
		return err
	}

	r.Aux[auxModel] = strconv.Itoa(model)

	upgraded, err := p.vmssApplyModel(r, existing, victims, model)
	if err != nil {
		p.dropInstances(r, added)

		return err
	}

	r.Instances = slices.Concat(vmssWithout(existing, victims), added)
	r.Aux[auxNextInstance] = strconv.Itoa(next + len(added))

	for _, in := range victims {
		p.dropInstanceNICs(r, in)
	}

	if changed && strOf(mapOf(r.Props[cmpFieldUpgrade]), "mode") == upgradeRolling {
		recordRollingUpgrade(r, upgraded)
	}

	return nil
}

func vmssWithout(list, drop []*cmpInstance) []*cmpInstance {
	out := make([]*cmpInstance, 0, len(list))

	for _, in := range list {
		if !slices.Contains(drop, in) {
			out = append(out, in)
		}
	}

	return out
}

func (p *ComputeProvider) vmssGrow(r *cmpRes, next, model, n int) ([]*cmpInstance, error) {
	var added []*cmpInstance

	for i := range max(n, 0) {
		in := &cmpInstance{
			ID: strconv.Itoa(next + i), VMID: uuid.NewString(), Power: powerRunning, Model: model,
			Zone: vmssPickZone(r, added),
		}

		if err := p.syncInstanceNICs(r, in); err != nil {
			p.dropInstances(r, append(added, in))

			return nil, err
		}

		added = append(added, in)
	}

	return added, nil
}

func (p *ComputeProvider) dropInstances(r *cmpRes, list []*cmpInstance) {
	for _, in := range list {
		p.dropInstanceNICs(r, in)
	}
}

// vmssApplyModel moves the unprotected instances to model when the upgrade policy is not Manual and returns how many.
func (p *ComputeProvider) vmssApplyModel(r *cmpRes, existing, victims []*cmpInstance, model int) (int, error) {
	if strOf(mapOf(r.Props[cmpFieldUpgrade]), "mode") == upgradeManual {
		return 0, nil
	}

	n := 0

	for _, in := range vmssWithout(existing, victims) {
		if in.ProtectActions {
			continue
		}

		if err := p.syncInstanceNICs(r, in); err != nil {
			return n, err
		}

		in.Model = model
		n++
	}

	return n, nil
}

func (p *ComputeProvider) vmssManualUpgrade(r *cmpRes, targets []*cmpInstance) error {
	for _, in := range targets {
		if in.ProtectActions {
			continue
		}

		if err := p.upgradeInstance(r, in); err != nil {
			return err
		}
	}

	return nil
}

func (p *ComputeProvider) upgradeInstance(r *cmpRes, in *cmpInstance) error {
	if err := p.syncInstanceNICs(r, in); err != nil {
		return err
	}

	in.Model = atoiOr(r.Aux[auxModel], 1)

	return nil
}

// syncInstanceNICs makes the instance's interfaces in Microsoft.Network match the scale set's current network
// profile, creating, updating and removing them as needed.
func (p *ComputeProvider) syncInstanceNICs(r *cmpRes, in *cmpInstance) error {
	cfgs := anyList(mapOf(mapOf(r.Props[cmpFieldVMProfile])[cmpFieldNetwork])[flexNICCfgKey])
	instARM := vmssInstanceARMID(r, in)
	want := make([]string, 0, len(cfgs))

	for _, c := range cfgs {
		cm := mapOf(c)
		props := mapOf(cm[fieldProperties])
		primary, _ := props[cmpFieldPrimary].(bool)
		body := map[string]any{fieldLocation: r.Location, fieldProperties: flexNICProps(props)}

		nid := ResourceID{
			SubscriptionID: r.Sub, ResourceGroup: r.RG, Namespace: namespaceMicrosoftNetwork,
			Types: []string{netNICs}, Names: []string{vmssNICName(r.Name, in.ID, strOf(cm, fieldName))},
		}

		if _, err := p.net.putInstanceNIC(nid, body, instARM, primary || len(cfgs) == 1); err != nil {
			return err
		}

		want = append(want, strOf(cm, fieldName))
	}

	var stale []string

	for _, name := range in.NICs {
		if !slices.Contains(want, name) {
			stale = append(stale, vmssNICName(r.Name, in.ID, name))
		}
	}

	p.net.dropInstanceNICs(r.RG, stale)
	in.NICs = want

	return nil
}

func (p *ComputeProvider) dropInstanceNICs(r *cmpRes, in *cmpInstance) {
	names := make([]string, 0, len(in.NICs))

	for _, n := range in.NICs {
		names = append(names, vmssNICName(r.Name, in.ID, n))
	}

	p.net.dropInstanceNICs(r.RG, names)
	in.NICs = nil
}

func isVMSSNICPath(id ResourceID) bool {
	n := len(id.Types)
	if n < 2 || !strings.EqualFold(id.Types[0], cmpVMSS) {
		return false
	}

	if strings.EqualFold(id.Types[1], cmpFieldNICs) {
		return n == vmssNICScopeLen
	}

	return n >= 3 && strings.EqualFold(id.Types[1], cmpVMs) && strings.EqualFold(id.Types[2], cmpFieldNICs)
}

func (p *ComputeProvider) instanceNICBodies(r *cmpRes, in *cmpInstance) []map[string]any {
	instARM := vmssInstanceARMID(r, in)
	out := make([]map[string]any, 0, len(in.NICs))

	for _, cfg := range in.NICs {
		display := instARM + "/" + cmpFieldNICs + "/" + cfg

		if b, ok := p.net.instanceNICBody(r.RG, vmssNICName(r.Name, in.ID, cfg), display, cfg); ok {
			out = append(out, b)
		}
	}

	return out
}

func (p *ComputeProvider) vmssNICs(r *cmpRes, instance string) ([]map[string]any, error) {
	if isFlexible(r) {
		return nil, ErrResourceNotFound
	}

	if instance == "" {
		var out []map[string]any
		for _, in := range r.Instances {
			out = append(out, p.instanceNICBodies(r, in)...)
		}

		return out, nil
	}

	in, _ := r.instance(instance)
	if in == nil {
		return nil, ErrResourceNotFound
	}

	return p.instanceNICBodies(r, in), nil
}

func findByName(list []map[string]any, name string) (map[string]any, bool) {
	for _, b := range list {
		if strings.EqualFold(strOf(b, fieldName), name) {
			return b, true
		}
	}

	return nil, false
}

// vmssNICListFor serves the scale-set, instance and IP configuration listings of Uniform instance interfaces.
func (p *ComputeProvider) vmssNICListFor(r *cmpRes, id ResourceID) ([]map[string]any, error) {
	switch len(id.Types) {
	case vmssNICScopeLen:
		return p.vmssNICs(r, "")
	case vmssNICInstanceLen:
		return p.vmssNICs(r, id.Names[1])
	}

	nic, err := p.vmssNICGetFor(r, ResourceID{Types: id.Types[:3], Names: id.Names[:3]})
	if err != nil {
		return nil, err
	}

	out := []map[string]any{}
	for _, c := range anyList(mapOf(nic[fieldProperties])[netIPConfigs]) {
		out = append(out, mapOf(c))
	}

	return out, nil
}

func (p *ComputeProvider) vmssNICGetFor(r *cmpRes, id ResourceID) (map[string]any, error) {
	list, err := p.vmssNICs(r, id.Names[1])
	if err != nil {
		return nil, err
	}

	nic, ok := findByName(list, id.Names[2])
	if !ok {
		return nil, ErrResourceNotFound
	}

	if len(id.Types) == vmssNICInstanceLen {
		return nic, nil
	}

	var ipcs []map[string]any
	for _, c := range anyList(mapOf(nic[fieldProperties])[netIPConfigs]) {
		ipcs = append(ipcs, mapOf(c))
	}

	if ip, found := findByName(ipcs, id.Names[3]); found {
		return ip, nil
	}

	return nil, ErrResourceNotFound
}

func (p *ComputeProvider) releaseUniformNICs(r *cmpRes) {
	if isFlexible(r) {
		return
	}

	p.dropInstances(r, r.Instances)
}
