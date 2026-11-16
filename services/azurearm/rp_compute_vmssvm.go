package azurearm

import (
	"bytes"
	"encoding/base64"
	"encoding/json/v2"
	"maps"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	cmpFieldProtect = "protectionPolicy"
	cmpFieldLicense = "licenseType"
	cmpFieldUserDat = "userData"
	vmssVMReimageAl = "reimageall"
)

//nolint:gochecknoglobals // static read-only key lists
var (
	vmssVMApplied = []string{
		cmpFieldProtect,
		cmpFieldLicense,
		cmpFieldUserDat,
		cmpFieldDiagProfile,
		"additionalCapabilities",
	}
	vmssVMEchoed = []string{
		cmpFieldLatestModel, "modelDefinitionApplied", cmpFieldVMID, fieldProvisioningState, cmpFieldInstView,
		cmpFieldOSProfile, cmpFieldNetwork, cmpFieldTimeMade,
	}
	vmssVMActions = []string{
		vmActionStart, vmActionRestart, vmActionPowerOff, vmActionDeallocate, vmActionRedeploy, vmActionReimage,
		vmssVMReimageAl,
	}
)

func jsonEqual(a, b any) bool {
	x, err := json.Marshal(a, jsonx.V1(), json.Deterministic(true))
	if err != nil {
		return false
	}

	y, err := json.Marshal(b, jsonx.V1(), json.Deterministic(true))

	return err == nil && bytes.Equal(x, y)
}

func (p *ComputeProvider) vmssInstanceRef(id ResourceID) (*cmpRes, *cmpInstance, error) {
	r, ok := p.top(cmpVMSS, id.ResourceGroup, id.Names[0])
	if !ok {
		return nil, nil, ErrResourceNotFound
	}

	if isFlexible(r) {
		return nil, nil, cmpNotAllowed("Instances of the Flexible scale set %s are virtual machines; use the "+
			"virtual machines API.", r.Name)
	}

	in, _ := r.instance(id.Names[1])
	if in == nil {
		return nil, nil, ErrResourceNotFound
	}

	return r, in, nil
}

// vmssInstancePut applies a PUT or PATCH to one Uniform instance: tags, protection policy and the other properties
// the Update operation documents; any other changed property is refused.
func (p *ComputeProvider) vmssInstancePut(id ResourceID, body map[string]any) (map[string]any, error) {
	r, in, err := p.vmssInstanceRef(id)
	if err != nil {
		return nil, err
	}

	cur := p.vmssInstanceBody(r, in)
	if err = vmssCheckReadOnly(cur, body); err != nil {
		return nil, err
	}

	props := mapOf(body[fieldProperties])
	next := *in

	if err = applyInstanceProps(&next, props); err != nil {
		return nil, err
	}

	if _, ok := body[fieldTags]; ok {
		next.Tags = stringTags(body[fieldTags])
	}

	*in = next
	r.Etag = newEtag()

	return p.vmssInstanceBody(r, in), nil
}

func vmssCheckReadOnly(cur, body map[string]any) error {
	if loc := normLocation(strOf(body, fieldLocation)); loc != "" && loc != normLocation(strOf(cur, fieldLocation)) {
		return cmpChanged(fieldLocation)
	}

	curProps := mapOf(cur[fieldProperties])

	for k, v := range mapOf(body[fieldProperties]) {
		switch {
		case containsFold(vmssVMApplied, k):
		case containsFold(vmssVMEchoed, k) && jsonEqual(curProps[k], v):
		default:
			return cmpInvalid("The property 'properties.%s' of a scale set instance cannot be changed with the "+
				"virtual machine scale set VM update; change the scale set model instead.", k)
		}
	}

	return nil
}

func applyInstanceProps(in *cmpInstance, props map[string]any) error {
	if pol, ok := props[cmpFieldProtect].(map[string]any); ok {
		si, ok1 := pol["protectFromScaleIn"].(bool)
		sa, ok2 := pol["protectFromScaleSetActions"].(bool)

		if (pol["protectFromScaleIn"] != nil && !ok1) || (pol["protectFromScaleSetActions"] != nil && !ok2) {
			return cmpInvalid("The protectionPolicy values must be booleans.")
		}

		in.ProtectScaleIn, in.ProtectActions = si, sa
	}

	next := maps.Clone(in.Props)
	if next == nil {
		next = map[string]any{}
	}

	if ud, ok := props[cmpFieldUserDat].(string); ok {
		if _, err := base64.StdEncoding.DecodeString(ud); err != nil {
			return cmpInvalid("The userData value is not valid base64.")
		}
	}

	for _, k := range []string{cmpFieldLicense, cmpFieldUserDat, cmpFieldDiagProfile, "additionalCapabilities"} {
		if v, ok := props[k]; ok {
			next[k] = deepClone(v)
		}
	}

	in.Props = next

	return nil
}

func (p *ComputeProvider) vmssInstancePatch(id ResourceID, body map[string]any) (map[string]any, error) {
	r, in, err := p.vmssInstanceRef(id)
	if err != nil {
		return nil, err
	}

	merged := p.vmssInstanceBody(r, in)

	if v, ok := body[fieldTags]; ok {
		merged[fieldTags] = v
	}

	maps.Copy(mapOf(merged[fieldProperties]), mapOf(body[fieldProperties]))

	return p.vmssInstancePut(id, merged)
}

// VMSSInstancePatch applies a PATCH to one Uniform scale set instance.
func (p *ComputeProvider) VMSSInstancePatch(id ResourceID, body map[string]any) (map[string]any, error) {
	p.mu.Lock("VMSSInstancePatch")
	defer p.mu.Unlock()

	return p.vmssInstancePatch(id, body)
}

// VMSSInstanceAction runs start, restart, powerOff, deallocate, redeploy, reimage or reimageall on one instance.
func (p *ComputeProvider) VMSSInstanceAction(id ResourceID, action string) error {
	if !containsFold(vmssVMActions, action) {
		return cmpNotImplemented("virtualMachineScaleSets/virtualMachines/" + action)
	}

	p.mu.Lock("VMSSInstanceAction")
	defer p.mu.Unlock()

	r, in, err := p.vmssInstanceRef(id)
	if err != nil {
		return err
	}

	switch strings.ToLower(action) {
	case vmActionReimage, vmssVMReimageAl:
		if err = p.upgradeInstance(r, in); err != nil {
			return err
		}

		in.Power = powerRunning
		r.Etag = newEtag()

		return nil
	case vmActionRedeploy:
		in.Power = powerRunning
		r.Etag = newEtag()

		return nil
	}

	return p.vmssApply(r, strings.ToLower(action), []*cmpInstance{in})
}
