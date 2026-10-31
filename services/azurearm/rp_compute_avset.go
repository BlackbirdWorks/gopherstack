package azurearm

import (
	"strings"
)

const (
	avSetFDDefault = 3
	avSetFDMax     = 3
	avSetUDDefault = 5
	avSetUDMax     = 20
	avSetAligned   = "Aligned"
	avSetClassic   = "Classic"
	cmpFieldFD     = "platformFaultDomainCount"
	cmpFieldUD     = "platformUpdateDomainCount"
)

func (p *ComputeProvider) avSetMembers(set *cmpRes) []*cmpRes {
	var out []*cmpRes

	id := set.armID()

	for _, vm := range p.ofType(cmpVMs) {
		if strings.EqualFold(strOf(mapOf(vm.Props[cmpFieldAvSet]), cmpFieldID), id) {
			out = append(out, vm)
		}
	}

	return out
}

func (p *ComputeProvider) putAvSet(id ResourceID, body map[string]any) (*cmpRes, error) {
	if err := validCmpName(id.Names[0], cmpMaxNameLen); err != nil {
		return nil, err
	}

	r, old, err := p.upsert(cmpAvSets, id, body)
	if err != nil {
		return nil, err
	}

	sku := firstNonEmpty(strOf(mapOf(body[cmpFieldSKU]), fieldName), avSetClassic)

	canon, ok := canonicalIn([]string{avSetAligned, avSetClassic}, sku)
	if !ok {
		return nil, cmpInvalid(
			"The availability set sku '%s' is not valid; allowed values are Aligned and Classic.",
			sku,
		)
	}

	r.SKU = map[string]any{fieldName: canon}
	props := cloneProps(propsOf(body))

	delete(props, "virtualMachines")
	delete(props, cmpKeyStatuses)

	fd, ud := countOr(props, cmpFieldFD, avSetFDDefault), countOr(props, cmpFieldUD, avSetUDDefault)

	if fd < 1 || fd > avSetFDMax {
		return nil, cmpInvalid("The specified fault domain count %d must fall in the range 1 to %d.", fd, avSetFDMax)
	}

	if ud < 1 || ud > avSetUDMax {
		return nil, cmpInvalid(
			"The specified platformUpdateDomainCount %d must fall in the range 1 to %d.",
			ud,
			avSetUDMax,
		)
	}

	if old != nil && len(p.avSetMembers(old)) > 0 &&
		(fd != intOf(old.Props[cmpFieldFD]) || ud != intOf(old.Props[cmpFieldUD])) {
		return nil, cmpChanged(cmpFieldFD)
	}

	props[cmpFieldFD], props[cmpFieldUD] = fd, ud
	r.Props = props
	p.items[cmpKey(cmpAvSets, r.RG, r.Name)] = r

	return r, nil
}

func countOr(props map[string]any, key string, def int) int {
	if v, ok := props[key]; ok {
		return intOf(v)
	}

	return def
}

func (p *ComputeProvider) avSetProps(r *cmpRes) map[string]any {
	props := cloneProps(r.Props)
	vms := p.avSetMembers(r)
	members := make([]any, 0, len(vms))

	for _, vm := range vms {
		members = append(members, map[string]any{cmpFieldID: vm.armID()})
	}

	props["virtualMachines"] = members

	return props
}

func (p *ComputeProvider) checkAvSetDelete(r *cmpRes) error {
	if m := p.avSetMembers(r); len(m) > 0 {
		return cmpNotAllowed(
			"Availability set %s cannot be deleted because it contains virtual machine %s.",
			r.Name,
			m[0].Name,
		)
	}

	return nil
}

// vmDomains reports the fault and update domain a virtual machine of an availability set was spread to.
func (p *ComputeProvider) vmDomains(vm *cmpRes) (int, int, bool) {
	set, ok := p.item(cmpAvSets, strOf(mapOf(vm.Props[cmpFieldAvSet]), cmpFieldID))
	if !ok {
		return 0, 0, false
	}

	fd, ud := intOf(set.Props[cmpFieldFD]), intOf(set.Props[cmpFieldUD])

	for i, m := range p.avSetMembers(set) {
		if m == vm {
			return i % max(fd, 1), i % max(ud, 1), true
		}
	}

	return 0, 0, false
}
