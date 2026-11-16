package azurearm

import (
	"strings"
)

const (
	auxVMSS        = "vmss"
	vmssNICPrefix  = "~vmss~"
	vmssNICSegment = "~"
)

func hiddenNIC(n *netParent) bool { return n.Type == netNICs && n.Obj.Aux[auxVMSS] != "" }

// vmssNICName is a name no user-created interface can carry; the interfaces of Uniform scale set instances live
// under it and are exposed only through the Microsoft.Compute scale set paths.
func vmssNICName(vmss, instance, cfg string) string {
	return vmssNICPrefix + strings.Join([]string{vmss, instance, cfg}, vmssNICSegment)
}

// putInstanceNIC creates or updates the interface of a Uniform scale set instance and returns its network id.
func (p *NetworkProvider) putInstanceNIC(
	id ResourceID, body map[string]any, instanceARMID string, primary bool,
) (string, error) {
	p.mu.Lock("putInstanceNIC")
	defer p.mu.Unlock()

	if _, _, err := p.putNIC(id, body); err != nil {
		return "", err
	}

	nic, _ := p.top(netNICs, id.ResourceGroup, id.Names[0])
	nic.Obj.Aux[auxVMSS] = instanceARMID
	nic.Obj.Aux[auxVM] = instanceARMID
	nic.Obj.Aux[auxVMPrimary] = boolStr(primary)
	p.reconcileLocked()

	return nic.armID(), nil
}

// dropInstanceNICs deletes interfaces created by putInstanceNIC.
func (p *NetworkProvider) dropInstanceNICs(rg string, names []string) {
	p.mu.Lock("dropInstanceNICs")
	defer p.mu.Unlock()

	for _, n := range names {
		if nic, ok := p.top(netNICs, rg, n); ok && hiddenNIC(nic) {
			delete(p.items, netKey(netNICs, rg, n))
		}
	}

	p.reconcileLocked()
}

// instanceNICBody returns the stored interface of a scale set instance with every id rewritten under display.
func (p *NetworkProvider) instanceNICBody(rg, name, display, leaf string) (map[string]any, bool) {
	p.mu.RLock("instanceNICBody")
	defer p.mu.RUnlock()

	nic, ok := p.top(netNICs, rg, name)
	if !ok || !hiddenNIC(nic) {
		return nil, false
	}

	out, _ := rewriteIDPrefix(p.bodyNIC(nic.id(nic.Sub), nic), nic.armID(), display).(map[string]any)
	out[fieldName] = leaf

	return out, true
}

// rewriteIDPrefix returns a copy of v in which every string starting with from starts with to instead.
func rewriteIDPrefix(v any, from, to string) any {
	switch t := v.(type) {
	case string:
		if rest, ok := strings.CutPrefix(t, from); ok {
			return to + rest
		}

		return t
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, e := range t {
			out[k] = rewriteIDPrefix(e, from, to)
		}

		return out
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = rewriteIDPrefix(e, from, to)
		}

		return out
	}

	return v
}
