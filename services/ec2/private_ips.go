package ec2

import (
	"encoding/binary"
	"net"
)

// reservedSubnetHosts is the count of leading addresses AWS reserves in every subnet
// (network, VPC router, DNS, future use).
const reservedSubnetHosts = 4

// subnetReservedHosts is the five addresses AWS withholds from every subnet (first four and last).
const subnetReservedHosts = reservedSubnetHosts + 1

// allocPrivateIPInSubnet returns the lowest free IPv4 address in the subnet's CIDR, or the
// legacy global allocation when the subnet is unknown, IPv6-only or exhausted. Must be called
// with mu held.
func (b *InMemoryBackend) allocPrivateIPInSubnet(subnetID string) string {
	sub, ok := b.subnets.Get(subnetID)
	if !ok || sub.CIDRBlock == "" {
		return b.allocPrivateIP()
	}

	_, ipNet, err := net.ParseCIDR(sub.CIDRBlock)
	if err != nil || ipNet.IP.To4() == nil {
		return b.allocPrivateIP()
	}

	used := b.usedPrivateIPsLocked(subnetID)
	base := binary.BigEndian.Uint32(ipNet.IP.To4())
	ones, bits := ipNet.Mask.Size()
	size := uint32(1) << uint(bits-ones)

	if size <= reservedSubnetHosts+1 {
		return b.allocPrivateIP()
	}

	for off := uint32(reservedSubnetHosts); off < size-1; off++ {
		var raw [4]byte

		binary.BigEndian.PutUint32(raw[:], base+off)

		ip := net.IP(raw[:]).String()
		if !used[ip] {
			return ip
		}
	}

	return b.allocPrivateIP()
}

func (b *InMemoryBackend) usedPrivateIPsLocked(subnetID string) map[string]bool {
	used := make(map[string]bool)

	for _, inst := range b.instances.All() {
		if inst.SubnetID == subnetID && inst.State != StateTerminated {
			used[inst.PrivateIP] = true
		}
	}

	for _, eni := range b.networkInterfaces.All() {
		if eni.SubnetID != subnetID {
			continue
		}

		used[eni.PrivateIP] = true

		for _, ip := range eni.SecondaryPrivateIPs {
			used[ip] = true
		}
	}

	for _, ngw := range b.natGateways.All() {
		if ngw.SubnetID != subnetID {
			continue
		}

		used[ngw.PrivateIP] = true

		for _, ip := range ngw.SecondaryPrivateIPs {
			used[ip] = true
		}

		for _, a := range ngw.SecondaryAddresses {
			used[a.PrivateIP] = true
		}
	}

	return used
}
