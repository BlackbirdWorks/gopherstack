package azurearm

import (
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

const (
	upgradeManual    = "Manual"
	upgradeAutomatic = "Automatic"
	upgradeRolling   = "Rolling"
	modeUniform      = "Uniform"
	modeFlexible     = "Flexible"

	cmpFieldVMProfile = "virtualMachineProfile"
	cmpFieldUpgrade   = "upgradePolicy"
	cmpFieldCapacity  = "capacity"
	cmpFieldOrchMode  = "orchestrationMode"
	cmpFieldInstance  = "instanceId"
	auxModel          = "model"
	auxNextInstance   = "nextInstance"
	vmssMaxCapacity   = 1000
	vmssMaxZones      = 3
	vmssMaxLinuxPfx   = 58
	vmssMaxWinPfx     = 9
	base36            = 36
	instanceSuffixLen = 6
	vmssDefaultFD     = 5
	vmssInstanceNames = 2
)

// cmpInstance is one virtual machine of a uniform scale set.
type cmpInstance struct {
	ID    string `json:"id"`
	VMID  string `json:"vmId"`
	Power string `json:"power"`
	Model int    `json:"model"`
}

func (p *ComputeProvider) putVMSS(id ResourceID, body map[string]any) (*cmpRes, error) {
	if err := validCmpName(id.Names[0], cmpMaxNameLen); err != nil {
		return nil, err
	}

	r, old, err := p.upsert(cmpVMSS, id, body)
	if err != nil {
		return nil, err
	}

	if err = p.applyIdentity(r, old, body); err != nil {
		return nil, err
	}

	if r.Zones, err = parseZones(body, old, vmssMaxZones); err != nil {
		return nil, err
	}

	r.Extra = extraFields(body, "plan", "extendedLocation")

	mode, err := vmssModeOf(body, old)
	if err != nil {
		return nil, err
	}

	capacity, err := vmssSKU(r, body, old, mode == modeFlexible)
	if err != nil {
		return nil, err
	}

	props := cloneProps(propsOf(body))
	delete(props, "uniqueId")
	delete(props, fieldProvisioningState)
	delete(props, cmpFieldTimeMade)

	if mode == modeFlexible {
		return p.putFlexVMSS(r, old, props, capacity)
	}

	if err = p.vmssNormalize(r, old, props); err != nil {
		return nil, err
	}

	r.Props = props
	vmssReconcile(r, old, capacity)
	p.items[cmpKey(cmpVMSS, r.RG, r.Name)] = r

	return r, nil
}

func (p *ComputeProvider) putFlexVMSS(r, old *cmpRes, props map[string]any, capacity int) (*cmpRes, error) {
	if err := p.flexNormalize(r, old, props, capacity); err != nil {
		return nil, err
	}

	key := cmpKey(cmpVMSS, r.RG, r.Name)
	r.Props = props
	r.Aux[auxModel] = "1"
	p.items[key] = r

	if err := p.flexReconcile(r, capacity); err != nil {
		if old != nil {
			p.items[key] = old
		} else {
			delete(p.items, key)
		}

		return nil, err
	}

	return r, nil
}

func vmssSKU(r *cmpRes, body map[string]any, old *cmpRes, flexible bool) (int, error) {
	sku := mapOf(body[cmpFieldSKU])
	if len(sku) == 0 && old != nil {
		sku = old.SKU
	}

	if flexible && len(sku) == 0 {
		return 0, nil
	}

	size, ok := findVMSize(strOf(sku, fieldName))
	if !ok {
		return 0, cmpInvalid("The VM size '%s' is not available. Use the vmSizes or resourceSkus list for the sizes "+
			"offered in this location.", strOf(sku, fieldName))
	}

	capacity := intOf(sku[cmpFieldCapacity])
	if capacity < 0 || capacity > vmssMaxCapacity {
		return 0, cmpInvalid(
			"The sku.capacity %d is not valid; it must be between 0 and %d.",
			capacity,
			vmssMaxCapacity,
		)
	}

	r.SKU = map[string]any{
		fieldName:        size.name,
		cmpKeyTier:       firstNonEmpty(strOf(sku, cmpKeyTier), skuTierStandard),
		cmpFieldCapacity: capacity,
	}

	return capacity, nil
}

func (p *ComputeProvider) vmssNormalize(r, old *cmpRes, props map[string]any) error {
	props[cmpFieldOrchMode] = modeUniform

	up := cloneProps(mapOf(props[cmpFieldUpgrade]))

	pm, ok := canonicalIn(
		[]string{upgradeManual, upgradeAutomatic, upgradeRolling},
		firstNonEmpty(strOf(up, "mode"), upgradeManual),
	)
	if !ok {
		return cmpInvalid("The upgradePolicy.mode '%s' is not valid; allowed values are Automatic, Manual and Rolling.",
			strOf(up, "mode"))
	}

	up["mode"] = pm
	props[cmpFieldUpgrade] = up

	setDefault(props, "overprovision", true)
	setDefault(props, "singlePlacementGroup", true)
	setDefault(props, cmpFieldFD, vmssDefaultFD)

	profile := cloneProps(mapOf(props[cmpFieldVMProfile]))
	if len(profile) == 0 {
		return cmpInvalid("Required parameter 'virtualMachineProfile' is missing (null).")
	}

	if err := p.vmssProfile(r, old, profile); err != nil {
		return err
	}

	props[cmpFieldVMProfile] = profile

	return nil
}

func (p *ComputeProvider) vmssProfile(r, old *cmpRes, profile map[string]any) error {
	whole := map[string]any{
		cmpFieldOSProfile: profile[cmpFieldOSProfile], cmpFieldStorage: profile[cmpFieldStorage],
	}
	osType := vmOSType(whole)

	osp := cloneProps(mapOf(profile[cmpFieldOSProfile]))
	if len(osp) == 0 {
		return cmpInvalid("Required parameter 'virtualMachineProfile.osProfile' is missing (null).")
	}

	pfx := firstNonEmpty(strOf(osp, "computerNamePrefix"), strings.ToLower(r.Name))
	limit := vmssMaxLinuxPfx

	if osType == osWindows {
		limit = vmssMaxWinPfx
	}

	if len(pfx) > limit {
		return cmpInvalid(
			"The computerNamePrefix '%s' is too long; for %s it can be at most %d characters.",
			pfx,
			osType,
			limit,
		)
	}

	if err := validateAdminUser(osType, strOf(osp, cmpFieldAdminUser)); err != nil {
		return err
	}

	var existing map[string]any
	if old != nil {
		existing = mapOf(mapOf(old.Props[cmpFieldVMProfile])[cmpFieldOSProfile])
	}

	if err := checkOSAuth(osType, strOf(osp, cmpFieldAdminUser), osp, existing != nil); err != nil {
		return err
	}

	delete(osp, cmpFieldAdminPass)
	delete(osp, "customData")
	delete(osp, "secrets")
	osp["computerNamePrefix"] = pfx
	profile[cmpFieldOSProfile] = osp

	stripProfileSecrets(profile)

	sp := cloneProps(mapOf(profile[cmpFieldStorage]))
	if ref := mapOf(sp[cmpFieldImageRef]); len(ref) > 0 {
		if err := imageRefOK(ref); err != nil {
			return err
		}
	}

	profile[cmpFieldStorage] = sp

	return p.vmssNetwork(r, profile)
}

func (p *ComputeProvider) vmssNetwork(r *cmpRes, profile map[string]any) error {
	np := cloneProps(mapOf(profile[cmpFieldNetwork]))
	cfgs := anyList(np["networkInterfaceConfigurations"])

	if len(cfgs) == 0 {
		return cmpInvalid(
			"Required parameter 'virtualMachineProfile.networkProfile.networkInterfaceConfigurations' is " +
				"missing (null).",
		)
	}

	for _, c := range cfgs {
		cm := mapOf(c)
		if strOf(cm, fieldName) == "" {
			return cmpInvalid("Required parameter 'networkInterfaceConfigurations.name' is missing (null).")
		}

		ipcs := anyList(mapOf(cm[fieldProperties])["ipConfigurations"])
		if len(ipcs) == 0 {
			return cmpInvalid(
				"Network interface configuration '%s' needs at least one IP configuration.",
				strOf(cm, fieldName),
			)
		}

		for _, ic := range ipcs {
			subnet := subID(mapOf(mapOf(ic)[fieldProperties]), netFieldSubnet)
			if subnet == "" {
				return cmpInvalid("Required parameter 'ipConfigurations.properties.subnet.id' is missing (null).")
			}

			loc, ok := p.net.subnetLocation(subnet)
			if !ok {
				return netErr(
					http.StatusBadRequest,
					netCodeRef,
					"Resource %s referenced by the request was not found.",
					subnet,
				)
			}

			if loc != r.Location {
				return cmpInvalid(
					"The subnet %s is in location %s; the scale set is in location %s.",
					subnet,
					loc,
					r.Location,
				)
			}
		}
	}

	profile[cmpFieldNetwork] = np

	return nil
}

// vmssReconcile resizes the instance list to capacity and bumps the model version when the profile or size changed.
func vmssReconcile(r, old *cmpRes, capacity int) {
	model := 1
	if old != nil {
		model = atoiOr(old.Aux[auxModel], 1)

		if !reflect.DeepEqual(old.Props[cmpFieldVMProfile], r.Props[cmpFieldVMProfile]) ||
			strOf(old.SKU, fieldName) != strOf(r.SKU, fieldName) {
			model++
		}
	}

	r.Aux[auxModel] = strconv.Itoa(model)

	if mode := strOf(mapOf(r.Props[cmpFieldUpgrade]), "mode"); mode != upgradeManual {
		for _, in := range r.Instances {
			in.Model = model
		}
	}

	next := atoiOr(r.Aux[auxNextInstance], 0)

	for len(r.Instances) < capacity {
		r.Instances = append(r.Instances, &cmpInstance{
			ID: strconv.Itoa(next), VMID: uuid.NewString(), Power: powerRunning, Model: model,
		})
		next++
	}

	if len(r.Instances) > capacity {
		r.Instances = r.Instances[:capacity]
	}

	r.Aux[auxNextInstance] = strconv.Itoa(next)
}

func atoiOr(s string, def int) int {
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}

	return def
}

func (p *ComputeProvider) vmssProps(r *cmpRes) map[string]any {
	props := cloneProps(r.Props)
	props["uniqueId"] = r.UID
	props[fieldProvisioningState] = provisioningStateSucceeded
	props[cmpFieldTimeMade] = r.Created.Format(timeCreatedLayout)

	return props
}

func vmssComputerName(r *cmpRes, in *cmpInstance) string {
	pfx := strOf(mapOf(mapOf(r.Props[cmpFieldVMProfile])[cmpFieldOSProfile]), "computerNamePrefix")
	n, _ := strconv.Atoi(in.ID)
	suffix := strings.ToLower(strconv.FormatInt(int64(n), base36))

	return fmt.Sprintf("%s%0*s", pfx, instanceSuffixLen, suffix)
}

func (p *ComputeProvider) vmssInstanceBody(r *cmpRes, in *cmpInstance) map[string]any {
	cur := atoiOr(r.Aux[auxModel], 1)
	id := r.id()
	id.Types = append(id.Types, cmpVMs)
	id.Names = append(id.Names, in.ID)

	return map[string]any{
		cmpFieldID:       id.ARMID(),
		fieldName:        r.Name + "_" + in.ID,
		fieldType:        namespaceMicrosoftCompute + "/" + cmpVMSS + "/" + cmpVMs,
		fieldLocation:    r.Location,
		cmpFieldInstance: in.ID,
		cmpFieldSKU:      map[string]any{fieldName: strOf(r.SKU, fieldName), cmpKeyTier: strOf(r.SKU, cmpKeyTier)},
		fieldProperties: map[string]any{
			"latestModelApplied": in.Model == cur, "modelDefinitionApplied": "VirtualMachineScaleSet",
			cmpFieldVMID: in.VMID, fieldProvisioningState: provisioningStateSucceeded,
			cmpFieldOSProfile: map[string]any{cmpFieldComputer: vmssComputerName(r, in)},
			cmpFieldInstView: map[string]any{
				cmpKeyStatuses: []any{
					statusEntry("ProvisioningState/succeeded", "Provisioning succeeded", r.Created),
					statusEntry("PowerState/"+in.Power, powerDisplay(in.Power), r.Created),
				},
			},
		},
	}
}

func (r *cmpRes) instance(id string) (*cmpInstance, int) {
	for i, in := range r.Instances {
		if in.ID == id {
			return in, i
		}
	}

	return nil, -1
}

func (p *ComputeProvider) vmssInstanceGet(r *cmpRes, id ResourceID) (map[string]any, error) {
	if isFlexible(r) {
		for _, vm := range p.vmssMembers(r) {
			if len(id.Names) == vmssInstanceNames && strings.EqualFold(vm.Name, id.Names[1]) {
				return p.flexInstanceBody(r, vm), nil
			}
		}

		return nil, ErrResourceNotFound
	}

	in, _ := r.instance(id.Names[len(id.Names)-1])
	if in == nil || len(id.Names) != vmssInstanceNames {
		return nil, ErrResourceNotFound
	}

	return p.vmssInstanceBody(r, in), nil
}

func (p *ComputeProvider) vmssInstanceList(r *cmpRes, _ ResourceID) []map[string]any {
	if isFlexible(r) {
		return p.flexInstanceList(r)
	}

	out := make([]map[string]any, 0, len(r.Instances))
	for _, in := range r.Instances {
		out = append(out, p.vmssInstanceBody(r, in))
	}

	return out
}

func (p *ComputeProvider) vmssInstanceDelete(r *cmpRes, id ResourceID) error {
	if isFlexible(r) {
		return cmpNotAllowed("Instances of the Flexible scale set %s are virtual machines; delete the virtual "+
			"machine with the virtual machines API.", r.Name)
	}

	if len(id.Names) != vmssInstanceNames {
		return ErrResourceNotFound
	}

	_, idx := r.instance(id.Names[1])
	if idx < 0 {
		return ErrResourceNotFound
	}

	r.Instances = append(r.Instances[:idx], r.Instances[idx+1:]...)
	r.SKU[cmpFieldCapacity] = len(r.Instances)

	return nil
}

// VMSSAction runs manualupgrade, start, powerOff, restart, deallocate or delete against a scale set's instances;
// an empty ids list means every instance.
func (p *ComputeProvider) VMSSAction(id ResourceID, action string, ids []string) error {
	p.mu.Lock("VMSSAction")
	defer p.mu.Unlock()

	r, ok := p.top(cmpVMSS, id.ResourceGroup, id.Names[0])
	if !ok {
		return ErrResourceNotFound
	}

	if isFlexible(r) {
		return p.flexAction(r, action, ids)
	}

	targets := r.Instances

	if len(ids) > 0 {
		targets = nil

		for _, want := range ids {
			in, _ := r.instance(want)
			if in == nil {
				return cmpInvalid("The instance id '%s' is not valid for scale set %s.", want, r.Name)
			}

			targets = append(targets, in)
		}
	}

	return vmssApply(r, strings.ToLower(action), targets)
}

func vmssApply(r *cmpRes, action string, targets []*cmpInstance) error {
	switch action {
	case "manualupgrade":
		for _, in := range targets {
			in.Model = atoiOr(r.Aux[auxModel], 1)
		}
	case vmActionStart, vmActionRestart:
		for _, in := range targets {
			in.Power = powerRunning
		}
	case vmActionPowerOff:
		for _, in := range targets {
			in.Power = powerStopped
		}
	case vmActionDeallocate:
		for _, in := range targets {
			in.Power = powerDeallocated
		}
	case "delete":
		drop := map[string]bool{}
		for _, in := range targets {
			drop[in.ID] = true
		}

		kept := r.Instances[:0:0]

		for _, in := range r.Instances {
			if !drop[in.ID] {
				kept = append(kept, in)
			}
		}

		r.Instances = kept
		r.SKU[cmpFieldCapacity] = len(kept)
	default:
		return cmpNotImplemented("virtualMachineScaleSets/" + action)
	}

	r.Etag = newEtag()

	return nil
}
