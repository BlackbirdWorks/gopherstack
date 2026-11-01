package azurearm

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

const (
	vmFieldVMSS      = "virtualMachineScaleSet"
	vmFieldFD        = "platformFaultDomain"
	auxVMSSManaged   = "vmssManaged"
	vmTrustedKey     = "x-gopherstack-trusted-profile"
	flexMaxRegionalF = 3
	flexMaxZonalFD   = 1
	flexDefaultFD    = 1
	flexNICCfgKey    = "networkInterfaceConfigurations"
)

func isFlexible(r *cmpRes) bool {
	return r.Type == cmpVMSS && strings.EqualFold(strOf(r.Props, cmpFieldOrchMode), modeFlexible)
}

func vmssModeOf(body map[string]any, old *cmpRes) (string, error) {
	mode := firstNonEmpty(strOf(propsOf(body), cmpFieldOrchMode), modeUniform)

	canon, ok := canonicalIn([]string{modeUniform, modeFlexible}, mode)
	if !ok {
		return "", cmpInvalid("The orchestrationMode '%s' is not valid; allowed values are Uniform and Flexible.", mode)
	}

	if old != nil && !strings.EqualFold(strOf(old.Props, cmpFieldOrchMode), canon) {
		return "", cmpChanged(cmpFieldOrchMode)
	}

	return canon, nil
}

func (p *ComputeProvider) flexNormalize(r *cmpRes, old *cmpRes, props map[string]any, capacity int) error {
	props[cmpFieldOrchMode] = modeFlexible

	fd := flexDefaultFD
	if v, set := props[cmpFieldFD]; set {
		fd = intOf(v)
	}

	maxFD := flexMaxRegionalF
	if len(r.Zones) > 0 {
		maxFD = flexMaxZonalFD
	}

	if fd < 1 || fd > maxFD {
		return cmpInvalid("The platformFaultDomainCount %d is not valid for a Flexible scale set; it must be between "+
			"1 and %d (1 for zone-spanning and zonal scale sets).", fd, maxFD)
	}

	props[cmpFieldFD] = fd

	if v, _ := props["overprovision"].(bool); v {
		return cmpInvalid("Overprovisioning is not supported with Flexible orchestration.")
	}

	profile := cloneProps(mapOf(props[cmpFieldVMProfile]))
	if len(profile) == 0 {
		if capacity > 0 {
			return cmpInvalid("sku.capacity %d requires virtualMachineProfile on a Flexible scale set.", capacity)
		}

		delete(props, cmpFieldVMProfile)

		return nil
	}

	if err := noPublicIPConfig(profile); err != nil {
		return err
	}

	if err := p.vmssProfile(r, old, profile); err != nil {
		return err
	}

	props[cmpFieldVMProfile] = profile

	return nil
}

func noPublicIPConfig(profile map[string]any) error {
	for _, c := range anyList(mapOf(profile[cmpFieldNetwork])[flexNICCfgKey]) {
		for _, ic := range anyList(mapOf(mapOf(c)[fieldProperties])["ipConfigurations"]) {
			if mapOf(mapOf(ic)[fieldProperties])["publicIPAddressConfiguration"] != nil {
				return cmpNotImplemented("publicIPAddressConfiguration on a Flexible scale set")
			}
		}
	}

	return nil
}

// vmssMembers returns every virtual machine that references the scale set, oldest first.
func (p *ComputeProvider) vmssMembers(r *cmpRes) []*cmpRes {
	var out []*cmpRes

	id := r.armID()

	for _, vm := range p.ofType(cmpVMs) {
		if strings.EqualFold(strOf(mapOf(vm.Props[vmFieldVMSS]), cmpFieldID), id) {
			out = append(out, vm)
		}
	}

	sort.SliceStable(out, func(i, j int) bool { return out[i].Created.Before(out[j].Created) })

	return out
}

func (p *ComputeProvider) vmssManaged(r *cmpRes) []*cmpRes {
	var out []*cmpRes

	for _, vm := range p.vmssMembers(r) {
		if vm.Aux[auxVMSSManaged] == cmpTrue {
			out = append(out, vm)
		}
	}

	return out
}

func (p *ComputeProvider) removeVM(vm *cmpRes) {
	p.deleteVM(vm)
	delete(p.items, cmpKey(cmpVMs, vm.RG, vm.Name))
}

func (p *ComputeProvider) deleteVMSSMembers(r *cmpRes) {
	if !isFlexible(r) {
		return
	}

	for _, vm := range p.vmssMembers(r) {
		if vm.Aux[auxVMSSManaged] == cmpTrue {
			p.removeVM(vm)

			continue
		}

		delete(vm.Props, vmFieldVMSS)
		vm.Etag = newEtag()
	}
}

// flexReconcile makes the number of scale-set-created virtual machines equal capacity.
func (p *ComputeProvider) flexReconcile(r *cmpRes, capacity int) error {
	managed := p.vmssManaged(r)

	for len(managed) > capacity {
		p.removeVM(managed[len(managed)-1])
		managed = managed[:len(managed)-1]
	}

	var created []*cmpRes

	for len(managed)+len(created) < capacity {
		vm, err := p.createFlexVM(r)
		if err != nil {
			for _, c := range created {
				p.removeVM(c)
			}

			return err
		}

		created = append(created, vm)
	}

	return nil
}

func (p *ComputeProvider) createFlexVM(r *cmpRes) (*cmpRes, error) {
	profile := mapOf(r.Props[cmpFieldVMProfile])
	next := atoiOr(r.Aux[auxNextInstance], 0)
	r.Aux[auxNextInstance] = strconv.Itoa(next + 1)
	name := fmt.Sprintf("%s_%08x", r.Name, next)

	nics, err := p.flexNICs(r, name, profile)
	if err != nil {
		return nil, err
	}

	body := flexVMBody(r, name, next, profile, nics)

	id := ResourceID{
		SubscriptionID: r.Sub, ResourceGroup: r.RG, Namespace: namespaceMicrosoftCompute,
		Types: []string{cmpVMs}, Names: []string{name},
	}

	vm, err := p.putVM(id, body)
	if err != nil {
		p.dropNICs(nics)

		return nil, err
	}

	vm.Aux[auxVMSSManaged] = cmpTrue

	if err = p.flexExtensions(r, vm, profile); err != nil {
		p.removeVM(vm)

		return nil, err
	}

	return vm, nil
}

type flexNIC struct {
	id      string
	del     string
	primary bool
}

func (p *ComputeProvider) dropNICs(nics []flexNIC) {
	for _, n := range nics {
		p.net.releaseNICs("", []string{n.id}, true)
	}
}

func (p *ComputeProvider) flexNICs(r *cmpRes, vmName string, profile map[string]any) ([]flexNIC, error) {
	cfgs := anyList(mapOf(profile[cmpFieldNetwork])[flexNICCfgKey])
	out := make([]flexNIC, 0, len(cfgs))

	for i, c := range cfgs {
		cm := mapOf(c)
		props := mapOf(cm[fieldProperties])
		primary, _ := props[cmpFieldPrimary].(bool)
		nicName := vmName + "-" + strOf(cm, fieldName)

		body := map[string]any{
			fieldLocation:   r.Location,
			fieldProperties: flexNICProps(props),
		}

		nid := ResourceID{
			SubscriptionID: r.Sub, ResourceGroup: r.RG, Namespace: namespaceMicrosoftNetwork,
			Types: []string{netNICs}, Names: []string{nicName},
		}

		got, err := p.net.Put(context.Background(), nid, body)
		if err != nil {
			p.dropNICs(out)

			return nil, err
		}

		out = append(out, flexNIC{
			id: strOf(got, cmpFieldID), primary: primary || (len(cfgs) == 1 && i == 0),
			del: firstNonEmpty(strOf(props, cmpFieldDelOpt), deleteOptionDelete),
		})
	}

	return out, nil
}

//nolint:gochecknoglobals // static read-only key lists
var (
	flexNICPassthrough = []string{
		"networkSecurityGroup",
		"enableAcceleratedNetworking",
		"enableIPForwarding",
		"dnsSettings",
	}
	flexIPCfgPassthrough = []string{
		netFieldSubnet, cmpFieldPrimary, "privateIPAddressVersion", "loadBalancerBackendAddressPools",
		"applicationSecurityGroups",
	}
)

func flexNICProps(props map[string]any) map[string]any {
	out := map[string]any{}

	for _, k := range flexNICPassthrough {
		if v, ok := props[k]; ok {
			out[k] = deepClone(v)
		}
	}

	raw := anyList(props["ipConfigurations"])
	ipcs := make([]any, 0, len(raw))

	for _, ic := range raw {
		im := mapOf(ic)
		ip := mapOf(im[fieldProperties])
		cfg := map[string]any{"privateIPAllocationMethod": methodDynamic}

		for _, k := range flexIPCfgPassthrough {
			if v, ok := ip[k]; ok {
				cfg[k] = deepClone(v)
			}
		}

		ipcs = append(ipcs, map[string]any{fieldName: strOf(im, fieldName), fieldProperties: cfg})
	}

	out["ipConfigurations"] = ipcs

	return out
}

func flexVMBody(r *cmpRes, name string, n int, profile map[string]any, nics []flexNIC) map[string]any {
	osp := mapOf(profile[cmpFieldOSProfile])
	sp := mapOf(profile[cmpFieldStorage])

	storage := map[string]any{
		cmpFieldOSDisk:    flexOSDisk(mapOf(sp[cmpFieldOSDisk])),
		cmpFieldDataDisks: flexDataDisks(name, sp),
	}
	if ref := mapOf(sp[cmpFieldImageRef]); len(ref) > 0 {
		storage[cmpFieldImageRef] = deepClone(ref)
	}

	osProfile := map[string]any{
		cmpFieldComputer:  vmssComputerPrefixName(r, n),
		cmpFieldAdminUser: strOf(osp, cmpFieldAdminUser),
	}

	for _, k := range []string{cmpFieldLinuxCfg, cmpFieldWinCfg, extFieldAllowOps} {
		if v, ok := osp[k]; ok {
			osProfile[k] = deepClone(v)
		}
	}

	refs := make([]any, 0, len(nics))
	for _, nic := range nics {
		refs = append(refs, map[string]any{
			cmpFieldID: nic.id, fieldProperties: map[string]any{cmpFieldPrimary: nic.primary, cmpFieldDelOpt: nic.del},
		})
	}

	props := map[string]any{
		cmpFieldHardware: map[string]any{"vmSize": strOf(r.SKU, fieldName)},
		cmpFieldStorage:  storage, cmpFieldOSProfile: osProfile,
		cmpFieldNetwork: map[string]any{"networkInterfaces": refs},
		vmFieldVMSS:     map[string]any{cmpFieldID: r.armID()},
	}

	body := map[string]any{fieldLocation: r.Location, fieldProperties: props, vmTrustedKey: true}

	if len(r.Zones) > 0 {
		body[cmpFieldZones] = []any{r.Zones[n%len(r.Zones)]}
	}

	return body
}

func vmssComputerPrefixName(r *cmpRes, n int) string {
	pfx := strOf(mapOf(mapOf(r.Props[cmpFieldVMProfile])[cmpFieldOSProfile]), "computerNamePrefix")
	suffix := strings.ToLower(strconv.FormatInt(int64(n), base36))

	return fmt.Sprintf("%s%0*s", pfx, instanceSuffixLen, suffix)
}

func flexOSDisk(osd map[string]any) map[string]any {
	out := cloneProps(osd)
	out[cmpFieldCreateOpt] = firstNonEmpty(strOf(osd, cmpFieldCreateOpt), createFromImage)
	setDefault(out, cmpFieldDelOpt, deleteOptionDelete)

	return out
}

func flexDataDisks(vm string, sp map[string]any) []any {
	raw := anyList(sp[cmpFieldDataDisks])
	out := make([]any, 0, len(raw))

	for _, e := range raw {
		d := cloneProps(mapOf(e))
		setDefault(d, cmpFieldCreateOpt, "Empty")
		setDefault(d, cmpFieldDelOpt, deleteOptionDelete)

		if strOf(d, fieldName) == "" {
			d[fieldName] = fmt.Sprintf("%s_disk%d", vm, intOf(d[cmpFieldLun]))
		}

		out = append(out, d)
	}

	return out
}

func (p *ComputeProvider) flexExtensions(r, vm *cmpRes, profile map[string]any) error {
	for _, e := range anyList(mapOf(profile["extensionProfile"])["extensions"]) {
		em := mapOf(e)
		id := ResourceID{
			SubscriptionID: vm.Sub, ResourceGroup: vm.RG, Namespace: namespaceMicrosoftCompute,
			Types: []string{cmpVMs, kidExtensions}, Names: []string{vm.Name, strOf(em, fieldName)},
		}

		if _, err := p.putKid(
			id,
			map[string]any{fieldLocation: r.Location, fieldProperties: em[fieldProperties]},
		); err != nil {
			return err
		}
	}

	return nil
}

// vmScaleSetRef validates and stores virtualMachineScaleSet on a virtual machine.
func (p *ComputeProvider) vmScaleSetRef(r *cmpRes, props, newProps map[string]any) error {
	ref := strOf(mapOf(props[vmFieldVMSS]), cmpFieldID)
	if ref == "" {
		delete(newProps, vmFieldVMSS)

		return nil
	}

	set, ok := p.item(cmpVMSS, ref)
	if !ok {
		return notFoundRes(ref)
	}

	switch {
	case !isFlexible(set):
		return cmpInvalid("The virtual machine scale set %s uses '%s' orchestration; only Flexible scale sets accept "+
			"virtual machines.", set.Name, strOf(set.Props, cmpFieldOrchMode))
	case set.Location != r.Location:
		return cmpInvalid("The virtual machine scale set %s is in location %s; the virtual machine must be in the "+
			"same location.", set.armID(), set.Location)
	case newProps[cmpFieldAvSet] != nil:
		return cmpInvalid("A virtual machine cannot be placed in both an availability set and a virtual machine " +
			"scale set.")
	case len(set.Zones) > 0 && len(r.Zones) > 0 && !containsFold(set.Zones, r.Zones[0]):
		return cmpInvalid(
			"The zone '%s' is not one of the zones of virtual machine scale set %s.",
			r.Zones[0],
			set.Name,
		)
	}

	if v, has := props[vmFieldFD]; has {
		if fd := intOf(v); fd < 0 || fd >= intOf(set.Props[cmpFieldFD]) {
			return cmpInvalid("The platformFaultDomain %d must be between 0 and %d for scale set %s.", fd,
				intOf(set.Props[cmpFieldFD])-1, set.Name)
		}
	}

	newProps[vmFieldVMSS] = map[string]any{cmpFieldID: set.armID()}

	return nil
}

func (p *ComputeProvider) flexInstanceBody(r, vm *cmpRes) map[string]any {
	id := r.id()
	id.Types = append(id.Types, cmpVMs)
	id.Names = append(id.Names, vm.Name)

	return map[string]any{
		cmpFieldID: id.ARMID(), fieldName: vm.Name, fieldType: namespaceMicrosoftCompute + "/" + cmpVMSS + "/" + cmpVMs,
		fieldLocation: vm.Location, cmpFieldInstance: vm.Name,
		fieldProperties: map[string]any{
			cmpFieldVMID: vm.UID, fieldProvisioningState: provisioningStateSucceeded, "latestModelApplied": true,
		},
	}
}

func (p *ComputeProvider) flexInstanceList(r *cmpRes) []map[string]any {
	members := p.vmssMembers(r)
	out := make([]map[string]any, 0, len(members))

	for _, vm := range members {
		out = append(out, p.flexInstanceBody(r, vm))
	}

	return out
}

// flexAction runs a batch action against the member virtual machines named by ids (all members when empty).
func (p *ComputeProvider) flexAction(r *cmpRes, action string, ids []string) error {
	var targets []*cmpRes

	for _, vm := range p.vmssMembers(r) {
		if len(ids) == 0 || containsFold(ids, vm.Name) {
			targets = append(targets, vm)
		}
	}

	for _, id := range ids {
		if !slicesContainsVM(targets, id) {
			return cmpInvalid("The instance id '%s' is not valid for scale set %s.", id, r.Name)
		}
	}

	if strings.EqualFold(action, "delete") {
		for _, vm := range targets {
			p.removeVM(vm)
		}

		return nil
	}

	for _, vm := range targets {
		if err := p.vmActionLocked(vm, action); err != nil {
			return err
		}
	}

	return nil
}

func slicesContainsVM(vms []*cmpRes, name string) bool {
	for _, vm := range vms {
		if strings.EqualFold(vm.Name, name) {
			return true
		}
	}

	return false
}
