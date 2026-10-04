package container

import (
	"fmt"
	"net"
	"net/netip"
	"strings"
)

// LoopbackHost is the default bind address for engine-published ports.
const LoopbackHost = "127.0.0.1"

// AllInterfacesHost binds a published port on every host interface.
const AllInterfacesHost = "0.0.0.0"

const (
	hostContainerFields   = 2
	ipHostContainerFields = 3
)

// ParsePortSpec splits "[IP:]HOST:CONTAINER"; an absent IP yields the IPv4 unspecified address.
func ParsePortSpec(spec string) (netip.Addr, string, string, error) {
	parts := strings.Split(spec, ":")

	switch len(parts) {
	case hostContainerFields:
		return netip.IPv4Unspecified(), parts[0], parts[1], nil
	case ipHostContainerFields:
		ip, err := netip.ParseAddr(parts[0])
		if err != nil {
			return netip.Addr{}, "", "", fmt.Errorf("%w %q: bad host IP: %w", ErrInvalidPort, spec, err)
		}

		return ip, parts[1], parts[2], nil
	default:
		return netip.Addr{}, "", "", fmt.Errorf("%w %q: want [IP:]HOST:CONTAINER", ErrInvalidPort, spec)
	}
}

// BindHostFor picks the host IP to publish ports on: loopback unless the operator
// advertised a non-loopback host, in which case all interfaces.
func BindHostFor(advertiseHost string) string {
	if advertiseHost == "" || advertiseHost == "localhost" {
		return LoopbackHost
	}

	if ip := net.ParseIP(advertiseHost); ip != nil && ip.IsLoopback() {
		return LoopbackHost
	}

	return AllInterfacesHost
}

// PortSpec formats an "IP:HOST:CONTAINER" mapping.
func PortSpec(bindHost, hostPort, containerPort string) string {
	return bindHost + ":" + hostPort + ":" + containerPort
}
