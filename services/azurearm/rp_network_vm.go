package azurearm

import (
	"net/http"
	"strings"
)

const (
	auxVM        = "vm"
	auxVMPrimary = "vmPrimary"
	nicCodeInUse = "NicInUse"
)

// nicAttachment is a virtual machine's claim on a network interface.
type nicAttachment struct {
	id      string
	primary bool
}

func nicInUse(nicID, user string) error {
	return netErr(http.StatusBadRequest, nicCodeInUse,
		"Network Interface %s is used by existing resource %s. In order to delete the network interface, it must be "+
			"dissociated from the resource. To learn more, see aka.ms/deletenic.", nicID, user)
}

// claimNICs attaches the interfaces to vmID atomically: either every interface is claimed or none is.
func (p *NetworkProvider) claimNICs(vmID, location string, refs []nicAttachment) error {
	p.mu.Lock("claimNICs")
	defer p.mu.Unlock()

	nics := make([]*netParent, 0, len(refs))

	for _, r := range refs {
		nic, ok := p.netRef(r.id, netNICs)
		if !ok {
			return netErr(http.StatusNotFound, netCodeNotFound,
				"The Resource '%s' was not found.", strings.TrimPrefix(r.id, "/"))
		}

		if nic.Obj.Location != normLocation(location) {
			return netErr(http.StatusBadRequest, "NetworkInterfaceAndVirtualMachineMustBeInSameLocation",
				"Network interface %s is in location %s but the virtual machine is in location %s; they must be in "+
					"the same location.", r.id, nic.Obj.Location, normLocation(location))
		}

		if user := nic.Obj.Aux[auxVM]; user != "" && !strings.EqualFold(user, vmID) {
			return nicInUse(r.id, user)
		}

		nics = append(nics, nic)
	}

	for i, nic := range nics {
		nic.Obj.Aux[auxVM] = vmID
		nic.Obj.Aux[auxVMPrimary] = boolStr(refs[i].primary)
		p.touch(nic)
	}

	return nil
}

// releaseNICs detaches the interfaces from vmID; with remove they are deleted as well.
func (p *NetworkProvider) releaseNICs(vmID string, ids []string, remove bool) {
	p.mu.Lock("releaseNICs")
	defer p.mu.Unlock()

	for _, id := range ids {
		nic, ok := p.netRef(id, netNICs)
		if !ok || !strings.EqualFold(nic.Obj.Aux[auxVM], vmID) {
			continue
		}

		delete(nic.Obj.Aux, auxVM)
		delete(nic.Obj.Aux, auxVMPrimary)
		p.touch(nic)

		if remove {
			delete(p.items, netKey(netNICs, nic.RG, nic.Obj.Name))
		}
	}

	p.reconcileLocked()
}

func boolStr(b bool) string {
	if b {
		return cmpTrue
	}

	return "false"
}

// nicVMBackRef adds the virtualMachine back-reference an attached interface reports.
func nicVMBackRef(props map[string]any, aux map[string]string) {
	vm := aux[auxVM]
	if vm == "" {
		return
	}

	props["virtualMachine"] = map[string]any{netFieldID: vm}
	props["primary"] = aux[auxVMPrimary] == cmpTrue
}

// subnetLocation reports the location of the virtual network owning subnetID and whether the subnet exists.
func (p *NetworkProvider) subnetLocation(subnetID string) (string, bool) {
	p.mu.RLock("subnetLocation")
	defer p.mu.RUnlock()

	v, _, ok := p.subnetByID(subnetID)
	if !ok {
		return "", false
	}

	return v.Obj.Location, true
}
