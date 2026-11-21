package azurearm

import (
	"maps"
	"net/http"
	"slices"
	"strings"

	"github.com/google/uuid"
)

const (
	auxDNSAuto    = "dnsAuto"
	auxVMStopped  = "vmStopped"
	dnsAutoTTL    = 10
	dnsRecordA    = "A"
	dnsLinkRegKey = "registrationEnabled"
)

// setVMPower records whether the virtual machine is running; a machine that is stopped or deallocated loses its
// auto-registered DNS records.
func (p *NetworkProvider) setVMPower(vmID string, running bool) {
	p.mu.Lock("setVMPower")
	defer p.mu.Unlock()

	for _, n := range p.ofType(netNICs) {
		if !strings.EqualFold(n.Obj.Aux[auxVM], vmID) {
			continue
		}

		if running {
			delete(n.Obj.Aux, auxVMStopped)
		} else {
			n.Obj.Aux[auxVMStopped] = cmpTrue
		}
	}

	p.reconcileLocked()
}

type autoRecord struct {
	name string
	addr string
}

// autoRegistrations lists the A records the virtual machines of vnetID get in a zone linked to it: one per running
// machine, from the private address of its primary interface (Learn: dns/private-dns-autoregistration).
func (p *NetworkProvider) autoRegistrations(vnetID string) []autoRecord {
	prefix := strings.ToLower(vnetID) + "/" + strings.ToLower(netSubnets) + "/"

	var out []autoRecord

	for _, n := range p.ofType(netNICs) {
		vm := n.Obj.Aux[auxVM]
		if vm == "" || hiddenNIC(n) || n.Obj.Aux[auxVMStopped] != "" || n.Obj.Aux[auxVMPrimary] != cmpTrue {
			continue
		}

		rid, err := ParseGenericResourcePath(vm)
		if err != nil || len(rid.Types) != 1 || !strings.EqualFold(rid.Types[0], "virtualMachines") {
			continue
		}

		for _, c := range anyList(n.Obj.Props[netIPConfigs]) {
			m, _ := c.(map[string]any)
			props := propsOf(m)

			if primary, _ := props["primary"].(bool); !primary ||
				!strings.HasPrefix(strings.ToLower(subID(props, netFieldSubnet)), prefix) {
				continue
			}

			if addr := strOf(props, "privateIPAddress"); addr != "" {
				out = append(out, autoRecord{name: rid.Names[0], addr: addr})
			}
		}
	}

	return out
}

// reconcileDNSAutoLocked rebuilds the auto-registered A records of every zone from its registration-enabled links.
func (p *NetworkProvider) reconcileDNSAutoLocked() {
	for _, z := range p.ofType(netZones) {
		want := map[string]string{}

		for _, l := range z.Kids[netLinks] {
			if on, _ := l.Props[dnsLinkRegKey].(bool); !on {
				continue
			}

			for _, r := range p.autoRegistrations(subID(l.Props, "virtualNetwork")) {
				if _, taken := want[strings.ToLower(r.name)]; !taken {
					want[strings.ToLower(r.name)] = r.addr
				}
			}
		}

		p.applyAutoRecords(z, want)
	}
}

func autoAddr(o *netObj) string {
	recs := anyList(o.Props[dnsFieldARecords])
	if len(recs) == 0 {
		return ""
	}

	m, _ := recs[0].(map[string]any)

	return strOf(m, "ipv4Address")
}

func (p *NetworkProvider) applyAutoRecords(z *netParent, want map[string]string) {
	var manual []string

	for _, o := range z.Kids[dnsRecordA] {
		if o.Aux[auxDNSAuto] == "" {
			manual = append(manual, strings.ToLower(o.Name))
		}
	}

	kept := make(map[string]bool, len(want))

	z.Kids[dnsRecordA] = slices.DeleteFunc(z.Kids[dnsRecordA], func(o *netObj) bool {
		if o.Aux[auxDNSAuto] == "" {
			return false
		}

		key := strings.ToLower(o.Name)
		drop := want[key] == "" || want[key] != autoAddr(o) || slices.Contains(manual, key)
		kept[key] = !drop

		return drop
	})

	for _, key := range slices.Sorted(maps.Keys(want)) {
		if kept[key] || slices.Contains(manual, key) {
			continue
		}

		z.Kids[dnsRecordA] = append(z.Kids[dnsRecordA], &netObj{
			Name: key,
			Etag: uuid.NewString(),
			GUID: uuid.NewString(),
			Aux:  map[string]string{auxDNSAuto: cmpTrue},
			Props: map[string]any{
				propTTL:          dnsAutoTTL,
				dnsFieldARecords: []any{map[string]any{"ipv4Address": want[key]}},
			},
		})
	}
}

// checkRegistrationLink refuses a second registration-enabled link to the same virtual network: a virtual network can
// be linked to only one private zone with auto-registration enabled.
func (p *NetworkProvider) checkRegistrationLink(zone *netParent, name, vnetID string) error {
	for _, z := range p.ofType(netZones) {
		for _, l := range z.Kids[netLinks] {
			if z == zone && strings.EqualFold(l.Name, name) {
				continue
			}

			if on, _ := l.Props[dnsLinkRegKey].(bool); on &&
				strings.EqualFold(subID(l.Props, "virtualNetwork"), vnetID) {
				return netErr(http.StatusConflict, dnsCodeConflict,
					"The virtual network %s is already linked to the private DNS zone %s with auto-registration "+
						"enabled; a virtual network can be linked to only one zone with auto-registration.",
					vnetID, z.Obj.Name)
			}
		}
	}

	return nil
}

// checkSingleLink refuses a second link, under another name, between the same zone and virtual network (Learn:
// dns/private-dns-virtual-network-links, "only one link between a private DNS zone and a virtual network").
func checkSingleLink(zone *netParent, name, vnetID string) error {
	for _, l := range zone.Kids[netLinks] {
		if !strings.EqualFold(l.Name, name) && strings.EqualFold(subID(l.Props, "virtualNetwork"), vnetID) {
			return netErr(http.StatusConflict, dnsCodeConflict,
				"The virtual network %s is already linked to the private DNS zone %s by the link %s; a zone can "+
					"have only one link to a virtual network.", vnetID, zone.Obj.Name, l.Name)
		}
	}

	return nil
}
