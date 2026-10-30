package azurearm

import (
	"strings"
	"time"
)

const (
	vmActionStart      = "start"
	vmActionPowerOff   = "poweroff"
	vmActionRestart    = "restart"
	vmActionDeallocate = "deallocate"
	vmActionRedeploy   = "redeploy"
	vmActionGeneralize = "generalize"

	statusLevelInfo = "Info"
	auxPowerTime    = "powerTime"
)

// vmActionNames lists the actions VMAction implements, spelled as in the URL.
//
//nolint:gochecknoglobals // static read-only table
var vmActionNames = []string{
	vmActionStart, "powerOff", vmActionRestart, vmActionDeallocate, vmActionRedeploy, vmActionGeneralize,
}

func vmNotAllowed(action, vm, why string) error {
	return cmpNotAllowed("Operation '%s' is not allowed on VM '%s' since the VM is %s.", action, vm, why)
}

// VMAction runs start, powerOff, restart, deallocate, redeploy or generalize against a stored virtual machine.
func (p *ComputeProvider) VMAction(id ResourceID, action string) error {
	p.mu.Lock("VMAction")
	defer p.mu.Unlock()

	r, found := p.top(cmpVMs, id.ResourceGroup, id.Names[0])
	if !found {
		return ErrResourceNotFound
	}

	return p.vmActionLocked(r, action)
}

func (p *ComputeProvider) vmActionLocked(r *cmpRes, action string) error {
	name, ok := canonicalIn(vmActionNames, action)
	if !ok {
		return cmpNotImplemented("virtualMachines/" + action)
	}

	power := r.Aux[auxPower]
	generalized := r.Aux[auxGeneralized] == cmpTrue

	switch strings.ToLower(action) {
	case vmActionStart, vmActionRedeploy, vmActionRestart:
		if generalized {
			return vmNotAllowed(name, r.Name, "generalized")
		}
	}

	next, err := nextPower(strings.ToLower(action), name, r.Name, power)
	if err != nil {
		return err
	}

	if strings.ToLower(action) == vmActionGeneralize {
		r.Aux[auxGeneralized] = cmpTrue
	}

	if next != power {
		r.Aux[auxPower] = next
		r.Aux[auxPowerTime] = time.Now().UTC().Format(time.RFC3339Nano)
	}

	r.Etag = newEtag()

	return nil
}

func nextPower(action, name, vm, power string) (string, error) {
	switch action {
	case vmActionStart:
		return powerRunning, nil
	case vmActionPowerOff:
		if power == powerRunning {
			return powerStopped, nil
		}

		return power, nil
	case vmActionRestart:
		if power != powerRunning {
			return "", vmNotAllowed(name, vm, "not running")
		}

		return powerRunning, nil
	case vmActionDeallocate:
		return powerDeallocated, nil
	case vmActionRedeploy:
		if power == powerDeallocated {
			return "", vmNotAllowed(name, vm, "deallocated")
		}

		return powerRunning, nil
	}

	if power == powerRunning {
		return "", vmNotAllowed(name, vm, "running; stop the VM before generalizing it")
	}

	return power, nil
}

func powerDisplay(power string) string {
	switch power {
	case powerStopped:
		return "VM stopped"
	case powerDeallocated:
		return "VM deallocated"
	}

	return "VM running"
}

func statusEntry(code, display string, at time.Time) map[string]any {
	return map[string]any{
		"code": code, cmpKeyLevel: statusLevelInfo, cmpKeyDisplay: display, "time": at.Format(time.RFC3339Nano),
	}
}

func (p *ComputeProvider) vmInstanceView(r *cmpRes) map[string]any {
	powerAt := r.Created
	if t, err := time.Parse(time.RFC3339Nano, r.Aux[auxPowerTime]); err == nil {
		powerAt = t
	}

	statuses := []any{
		statusEntry("ProvisioningState/succeeded", "Provisioning succeeded", r.Created),
		statusEntry("PowerState/"+r.Aux[auxPower], powerDisplay(r.Aux[auxPower]), powerAt),
	}

	if r.Aux[auxGeneralized] == cmpTrue {
		statuses = append(statuses, statusEntry("OSState/generalized", "VM generalized", powerAt))
	}

	sp := mapOf(r.Props[cmpFieldStorage])
	disks := []any{}

	for _, e := range append([]any{sp[cmpFieldOSDisk]}, anyList(sp[cmpFieldDataDisks])...) {
		if name := strOf(mapOf(e), fieldName); name != "" {
			disks = append(disks, map[string]any{
				fieldName:      name,
				cmpKeyStatuses: []any{statusEntry("ProvisioningState/succeeded", "Provisioning succeeded", r.Created)},
			})
		}
	}

	out := map[string]any{"disks": disks, cmpKeyStatuses: statuses}

	if exts := vmExtensionViews(r); len(exts) > 0 {
		out["extensions"] = exts
	}

	if name := strOf(mapOf(r.Props[cmpFieldOSProfile]), cmpFieldComputer); name != "" {
		out[cmpFieldComputer] = name
	}

	if fd, ud, ok := p.vmDomains(r); ok {
		out["platformFaultDomain"], out["platformUpdateDomain"] = fd, ud
	}

	return out
}

// InstanceView returns the virtualMachines/{name}/instanceView document.
func (p *ComputeProvider) InstanceView(id ResourceID) (map[string]any, error) {
	p.mu.RLock("InstanceView")
	defer p.mu.RUnlock()

	r, ok := p.top(cmpVMs, id.ResourceGroup, id.Names[0])
	if !ok {
		return nil, ErrResourceNotFound
	}

	return p.vmInstanceView(r), nil
}

// VMSizes returns the sizes the location offers, as the vmSizes list endpoints do.
func VMSizes() map[string]any { return map[string]any{fieldValue: vmSizesBody()} }

// ResourceSKUs returns the skus listing for location.
func ResourceSKUs(location string) map[string]any {
	return map[string]any{fieldValue: resourceSKUsBody(location)}
}
