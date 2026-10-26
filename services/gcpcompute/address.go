package gcpcompute

import (
	"fmt"
	"net/netip"
	"sort"
	"strings"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	addrExternal    = "EXTERNAL"
	addrInternal    = "INTERNAL"
	tierPremium     = "PREMIUM"
	poolFirstOctet  = 34
	poolRegionBase  = 100
	poolGlobalOctet = 120
	poolBlock       = 254
	poolMax         = poolBlock * poolBlock
)

func addrScopeOf(a *computeapi.Address) (string, string) {
	if a.Region == "" {
		return scopeGlobal, ""
	}

	return scopeRegion, lastSegment(a.Region)
}

func addrTarget(project string, a *computeapi.Address) target {
	scope, loc := addrScopeOf(a)

	return target{project: project, scope: scope, loc: loc, collection: "addresses", name: a.Name, id: a.Id}
}

func regionIndex(region string) int {
	for i, r := range regionCatalog {
		if r.name == region {
			return i
		}
	}

	return 0
}

func (b *InMemoryBackend) usedExternalLocked() map[string]bool {
	used := map[string]bool{}

	for _, a := range b.addresses {
		if a.AddressType != addrInternal && a.Address != "" {
			used[a.Address] = true
		}
	}

	b.markNatIPsLocked(used)

	for _, r := range b.routers {
		for _, ips := range r.AutoIPs {
			for _, ip := range ips {
				used[ip] = true
			}
		}
	}

	for _, r := range b.fwdRules {
		if r.IPAddress != "" && r.Subnetwork == "" {
			used[r.IPAddress] = true
		}
	}

	return used
}

func (b *InMemoryBackend) markNatIPsLocked(used map[string]bool) {
	for _, r := range b.instances {
		for _, nic := range r.Inst.NetworkInterfaces {
			for _, ac := range nic.AccessConfigs {
				if ac.NatIP != "" {
					used[ac.NatIP] = true
				}
			}
		}
	}
}

// allocExternalLocked takes the lowest free public address from the region's pool ("" is the global pool).
func (b *InMemoryBackend) allocExternalLocked(region string, extra ...string) (string, *APIError) {
	second := poolGlobalOctet
	if region != "" {
		second = poolRegionBase + regionIndex(region)
	}

	used := b.usedExternalLocked()
	for _, e := range extra {
		used[e] = true
	}

	for n := range poolMax {
		ip := fmt.Sprintf("%d.%d.%d.%d", poolFirstOctet, second, n/poolBlock, n%poolBlock+1)
		if !used[ip] {
			return ip, nil
		}
	}

	return "", errInvalid("No external IP addresses are available in this pool.")
}

func (b *InMemoryBackend) usedInternalLocked(subnetLink string) map[netip.Addr]bool {
	used := map[netip.Addr]bool{}

	for _, r := range b.instances {
		for _, nic := range r.Inst.NetworkInterfaces {
			if nic.Subnetwork != subnetLink {
				continue
			}

			if a, err := netip.ParseAddr(nic.NetworkIP); err == nil {
				used[a] = true
			}
		}
	}

	for _, a := range b.addresses {
		if a.Subnetwork != subnetLink || a.Address == "" {
			continue
		}

		if ip, err := netip.ParseAddr(a.Address); err == nil {
			used[ip] = true
		}
	}

	for _, r := range b.fwdRules {
		if ip, err := netip.ParseAddr(r.IPAddress); err == nil && r.Subnetwork == subnetLink {
			used[ip] = true
		}
	}

	return used
}

func (b *InMemoryBackend) addressUsersLocked(a *computeapi.Address) []string {
	var users []string

	for _, r := range b.instances {
		if addressUsedBy(a, r.Inst) {
			users = append(users, r.Inst.SelfLink)
		}
	}

	for _, r := range b.routers {
		for _, nat := range r.Router.Nats {
			for _, ip := range nat.NatIps {
				if ip == a.SelfLink {
					users = append(users, r.Router.SelfLink)
				}
			}
		}
	}

	for _, r := range b.fwdRules {
		if r.IPAddress == a.Address && (a.Region == r.Region) {
			users = append(users, r.SelfLink)
		}
	}

	sort.Strings(users)

	return users
}

func addressUsedBy(a *computeapi.Address, inst *computeapi.Instance) bool {
	for _, nic := range inst.NetworkInterfaces {
		if a.AddressType == addrInternal {
			if nic.Subnetwork == a.Subnetwork && nic.NetworkIP == a.Address {
				return true
			}

			continue
		}

		for _, ac := range nic.AccessConfigs {
			if ac.NatIP == a.Address {
				return true
			}
		}
	}

	return false
}

func (b *InMemoryBackend) addressViewLocked(a *computeapi.Address) *computeapi.Address {
	out := clone(a)
	out.Users = b.addressUsersLocked(a)
	out.Status = "RESERVED"

	if len(out.Users) > 0 {
		out.Status = "IN_USE"
	}

	return out
}

func (b *InMemoryBackend) buildAddressLocked(
	project, region string,
	in *computeapi.Address,
) (*computeapi.Address, *APIError) {
	if err := validName("name", in.Name); err != nil {
		return nil, err
	}

	if err := validLabels(in.Labels); err != nil {
		return nil, err
	}

	if in.IpVersion != "" && in.IpVersion != "IPV4" {
		return nil, errField("resource.ipVersion", in.IpVersion, "Only IPV4 addresses are supported.")
	}

	typ := in.AddressType
	if typ == "" {
		typ = addrExternal
	}

	a := &computeapi.Address{
		Kind: "compute#address", Id: newID(), CreationTimestamp: b.stamp(), Name: in.Name, Description: in.Description,
		AddressType: typ, Labels: in.Labels, LabelFingerprint: labelFP(in.Labels), IpVersion: "IPV4",
	}

	var err *APIError

	switch typ {
	case addrExternal:
		err = b.buildExternalLocked(region, in, a)
	case addrInternal:
		err = b.buildInternalLocked(project, region, in, a)
	default:
		err = errField("resource.addressType", typ, "Must be EXTERNAL or INTERNAL.")
	}

	if err != nil {
		return nil, err
	}

	if region == "" {
		a.SelfLink = globalLink(project, "addresses", a.Name)
	} else {
		a.Region = scopeURL(project, scopeRegion, region)
		a.SelfLink = regionLink(project, region, "addresses", a.Name)
	}

	return a, nil
}

func (b *InMemoryBackend) buildExternalLocked(region string, in, a *computeapi.Address) *APIError {
	if in.Subnetwork != "" || in.Network != "" {
		return errInvalid(
			"Invalid value for field 'resource.subnetwork': EXTERNAL addresses cannot specify a network or subnetwork.",
		)
	}

	a.NetworkTier = in.NetworkTier
	if a.NetworkTier == "" {
		a.NetworkTier = tierPremium
	}

	if a.NetworkTier != tierPremium && (region == "" || a.NetworkTier != tierStandard) {
		return errField("resource.networkTier", a.NetworkTier, "")
	}

	if in.Address == "" {
		ip, err := b.allocExternalLocked(region)
		if err != nil {
			return err
		}

		a.Address = ip

		return nil
	}

	ip, perr := netip.ParseAddr(in.Address)
	if perr != nil || !ip.Is4() {
		return errField("resource.address", in.Address, "Must be a valid IPv4 address.")
	}

	if b.usedExternalLocked()[in.Address] {
		return errField("resource.address", in.Address, "The IP address is already in use.")
	}

	a.Address = in.Address

	return nil
}

func (b *InMemoryBackend) buildInternalLocked(project, region string, in, a *computeapi.Address) *APIError {
	if region == "" {
		return errField(
			"resource.addressType",
			addrInternal,
			"Global internal addresses are not supported by this emulator.",
		)
	}

	if in.Subnetwork == "" {
		return errRequired("subnetwork")
	}

	s, err := b.resolveSubnetLocked(project, region, in.Subnetwork)
	if err != nil {
		return err
	}

	if lastSegment(s.Region) != region {
		return errField("resource.subnetwork", in.Subnetwork, "Subnetwork is in a different region than the address.")
	}

	p := netip.MustParsePrefix(s.IpCidrRange)
	used := b.usedInternalLocked(s.SelfLink)

	var ip netip.Addr

	if in.Address == "" {
		var ok bool
		if ip, ok = nextFree(p, used); !ok {
			return errInvalid("No free IP addresses are available in subnetwork '%s'.", s.Name)
		}
	} else {
		parsed, perr := netip.ParseAddr(in.Address)
		first, last := usableRange(p)

		if perr != nil || parsed.Compare(first) < 0 || parsed.Compare(last) > 0 {
			return errField(
				"resource.address",
				in.Address,
				"The IP address is not in the usable range of subnetwork '"+s.Name+"'.",
			)
		}

		if used[parsed] {
			return errField("resource.address", in.Address, "The IP address is already in use.")
		}

		ip = parsed
	}

	a.Address, a.Subnetwork, a.Purpose = ip.String(), s.SelfLink, in.Purpose

	if a.Purpose == "" {
		a.Purpose = "GCE_ENDPOINT"
	}

	return nil
}

func (b *InMemoryBackend) insertAddress(project, region string, in *computeapi.Address) (*computeapi.Operation, error) {
	b.mu.Lock("InsertAddress")
	defer b.mu.Unlock()

	if region != "" {
		if _, ok := findRegion(region); !ok {
			return nil, errNotFound(scopePath(project, scopeRegion, region))
		}
	}

	scope := scopeGlobal
	if region != "" {
		scope = scopeRegion
	}

	if _, dup := b.addresses[key(project, scope, region, in.Name)]; dup {
		return nil, errExists(relPath(project, scope, region, "addresses", in.Name))
	}

	a, err := b.buildAddressLocked(project, region, in)
	if err != nil {
		return nil, err
	}

	b.addresses[key(project, scope, region, a.Name)] = a

	return b.newOpLocked("insert", addrTarget(project, a), b.timing.Done), nil
}

func (b *InMemoryBackend) getAddressLocked(project, scope, region, name string) (*computeapi.Address, *APIError) {
	a, ok := b.addresses[key(project, scope, region, name)]
	if !ok {
		return nil, errNotFound(relPath(project, scope, region, "addresses", name))
	}

	return a, nil
}

func (b *InMemoryBackend) getAddress(project, scope, region, name string) (*computeapi.Address, error) {
	b.mu.RLock("GetAddress")
	defer b.mu.RUnlock()

	a, err := b.getAddressLocked(project, scope, region, name)
	if err != nil {
		return nil, err
	}

	return b.addressViewLocked(a), nil
}

func (b *InMemoryBackend) listAddresses(project, scope, loc string) []entry {
	b.mu.RLock("ListAddresses")
	defer b.mu.RUnlock()

	var out []entry

	for k, a := range b.addresses {
		aScope, aLoc := addrScopeOf(a)
		if !strings.HasPrefix(k, project+"/") || (scope != "" && (aScope != scope || aLoc != loc)) {
			continue
		}

		sk := scopeGlobal
		if aScope == scopeRegion {
			sk = scopeRegion + "/" + aLoc
		}

		out = append(out, entry{scope: sk, item: b.addressViewLocked(a)})
	}

	return out
}

func (b *InMemoryBackend) deleteAddress(project, scope, region, name string) (*computeapi.Operation, error) {
	b.mu.Lock("DeleteAddress")
	defer b.mu.Unlock()

	a, err := b.getAddressLocked(project, scope, region, name)
	if err != nil {
		return nil, err
	}

	if users := b.addressUsersLocked(a); len(users) > 0 {
		return nil, errInUse("address", relPath(project, scope, region, "addresses", name), trimURL(users[0]))
	}

	delete(b.addresses, key(project, scope, region, name))

	return b.newOpLocked("delete", addrTarget(project, a), b.timing.Done), nil
}

func (b *InMemoryBackend) setAddressLabels(
	project, scope, region, name string,
	req labelsRequest,
) (*computeapi.Operation, error) {
	b.mu.Lock("SetAddressLabels")
	defer b.mu.Unlock()

	a, err := b.getAddressLocked(project, scope, region, name)
	if err != nil {
		return nil, err
	}

	labels, fp, lerr := applyLabels(a.Labels, a.LabelFingerprint, req)
	if lerr != nil {
		return nil, lerr
	}

	a.Labels, a.LabelFingerprint = labels, fp

	return b.newOpLocked("setLabels", addrTarget(project, a), b.timing.Done), nil
}
