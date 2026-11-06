package gcpcompute

import (
	"fmt"
	"net/netip"
	"strings"
	"time"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	managedByUser   = "MANAGED_BY_USER"
	bgpEnabled      = "TRUE"
	bgpDisabled     = "FALSE"
	bgpUp           = "UP"
	bgpDown         = "DOWN"
	bgpEstablished  = "Established"
	bgpIdle         = "Idle"
	maxIPv6RangeLen = 126
)

var (
	linkLocalV4 = netip.MustParsePrefix("169.254.0.0/16") //nolint:gochecknoglobals // constant range
	ulaV6       = netip.MustParsePrefix("fdff:1::/64")    //nolint:gochecknoglobals // constant range
)

func validInterfaceRange(field, raw string) *APIError {
	p, err := netip.ParsePrefix(raw)
	if err != nil {
		return errField(field, raw, "Must be a CIDR range that keeps the interface address.")
	}

	if p.Addr().Is4() {
		if !linkLocalV4.Contains(p.Addr()) {
			return errField(field, raw, "IPv4 interface ranges must be in the RFC 3927 link-local space (169.254.0.0/16).")
		}

		return nil
	}

	if !ulaV6.Contains(p.Addr()) || p.Bits() > maxIPv6RangeLen {
		return errField(field, raw, "IPv6 interface ranges must be in fdff:1::/64 with a mask length of 126 or less.")
	}

	return nil
}

func (b *InMemoryBackend) normalizeRouterInterfacesLocked(project, region string, r *computeapi.Router) *APIError {
	seen := map[string]bool{}

	for _, ifc := range r.Interfaces {
		if err := validName("resource.interfaces.name", ifc.Name); err != nil {
			return err
		}

		if seen[ifc.Name] {
			return errField("resource.interfaces.name", ifc.Name, "Duplicate interface name.")
		}

		seen[ifc.Name] = true

		if ifc.LinkedInterconnectAttachment != "" {
			return errField("resource.interfaces.linkedInterconnectAttachment", ifc.LinkedInterconnectAttachment,
				"Interconnect attachments are not supported by this emulator.")
		}

		if ifc.IpRange != "" {
			if err := validInterfaceRange("resource.interfaces.ipRange", ifc.IpRange); err != nil {
				return err
			}
		}

		if ifc.LinkedVpnTunnel != "" {
			t, err := b.resolveRouterTunnelLocked(project, region, ifc.LinkedVpnTunnel)
			if err != nil {
				return err
			}

			ifc.LinkedVpnTunnel = t.SelfLink
		}

		if ifc.ManagementType == "" {
			ifc.ManagementType = managedByUser
		}
	}

	return nil
}

func (b *InMemoryBackend) resolveRouterTunnelLocked(project, region, raw string) (*computeapi.VpnTunnel, *APIError) {
	r, ok := parseRef(raw, collVPNTunnels, project, region)
	if !ok || r.loc != region {
		return nil, errField("resource.interfaces.linkedVpnTunnel", raw, "The tunnel must be in the router's region.")
	}

	t, found := b.vpnTunnels[key(r.project, region, r.name)]
	if !found {
		return nil, errNotFound(relPath(r.project, scopeRegion, region, collVPNTunnels, r.name))
	}

	return t, nil
}

func normalizeBgpPeers(r *computeapi.Router) *APIError {
	ifaces := map[string]bool{}
	for _, ifc := range r.Interfaces {
		ifaces[ifc.Name] = true
	}

	seen := map[string]bool{}

	for _, p := range r.BgpPeers {
		if err := validName("resource.bgpPeers.name", p.Name); err != nil {
			return err
		}

		if seen[p.Name] {
			return errField("resource.bgpPeers.name", p.Name, "Duplicate BGP peer name.")
		}

		seen[p.Name] = true

		if err := normalizeBgpPeer(p, ifaces); err != nil {
			return err
		}
	}

	return nil
}

func normalizeBgpPeer(p *computeapi.RouterBgpPeer, ifaces map[string]bool) *APIError {
	if p.InterfaceName == "" || !ifaces[p.InterfaceName] {
		return errField("resource.bgpPeers.interfaceName", p.InterfaceName, "The router has no such interface.")
	}

	if p.PeerAsn < 1 || p.PeerAsn > maxASN+1 {
		return errField("resource.bgpPeers.peerAsn", int64Str(p.PeerAsn), "ASN must be between 1 and 4294967295.")
	}

	for field, raw := range map[string]string{"ipAddress": p.IpAddress, "peerIpAddress": p.PeerIpAddress} {
		if raw == "" {
			continue
		}

		if _, err := netip.ParseAddr(raw); err != nil {
			return errField("resource.bgpPeers."+field, raw, "Not a valid IP address.")
		}
	}

	switch p.Enable {
	case "":
		p.Enable = bgpEnabled
	case bgpEnabled, bgpDisabled:
	default:
		return errField("resource.bgpPeers.enable", p.Enable, "Must be TRUE or FALSE.")
	}

	switch p.AdvertiseMode {
	case "":
		p.AdvertiseMode = "DEFAULT"
	case "DEFAULT", "CUSTOM":
	default:
		return errField("resource.bgpPeers.advertiseMode", p.AdvertiseMode, "")
	}

	if p.ManagementType == "" {
		p.ManagementType = managedByUser
	}

	return nil
}

func formatUptime(d time.Duration) string {
	secs := int64(d.Seconds())
	h, m, s := secs/3600, secs%3600/60, secs%60

	var parts []string

	if h > 0 {
		parts = append(parts, fmt.Sprintf("%d hours", h))
	}

	if m > 0 {
		parts = append(parts, fmt.Sprintf("%d minutes", m))
	}

	parts = append(parts, fmt.Sprintf("%d seconds", s))

	return strings.Join(parts, ", ")
}

// bgpPeerStatusesLocked reports each BGP peer from its linked tunnel: the session is up only while the tunnel
// is ESTABLISHED, and no routes are ever learned because no peer speaks BGP.
func (b *InMemoryBackend) bgpPeerStatusesLocked(r *computeapi.Router) []*computeapi.RouterStatusBgpPeerStatus {
	var out []*computeapi.RouterStatusBgpPeerStatus

	for _, p := range r.BgpPeers {
		st := &computeapi.RouterStatusBgpPeerStatus{
			Name: p.Name, IpAddress: p.IpAddress, PeerIpAddress: p.PeerIpAddress, State: bgpIdle, Status: bgpDown,
			EnableIpv4: p.EnableIpv4, EnableIpv6: p.EnableIpv6,
		}

		for _, ifc := range r.Interfaces {
			if ifc.Name != p.InterfaceName {
				continue
			}

			st.LinkedVpnTunnel = ifc.LinkedVpnTunnel

			if t := lbFind(b, vpnTunnelSpec(), ifc.LinkedVpnTunnel); t != nil && p.Enable != bgpDisabled {
				b.markBgpUpLocked(st, t)
			}
		}

		out = append(out, st)
	}

	return out
}

func (b *InMemoryBackend) markBgpUpLocked(st *computeapi.RouterStatusBgpPeerStatus, t *computeapi.VpnTunnel) {
	if state, _ := b.tunnelStateLocked(t); state != tunnelEstablished {
		return
	}

	st.State, st.Status = bgpEstablished, bgpUp

	if created, err := time.Parse(timeLayout, t.CreationTimestamp); err == nil {
		up := max(b.now().UTC().Sub(created.Add(b.timing.Done)), 0)
		st.Uptime, st.UptimeSeconds = formatUptime(up), int64Str(int64(up.Seconds()))
	}
}
