//nolint:staticcheck // legacy cluster fields are deprecated in the proto but still served
package gcpcontainer

import (
	"encoding/binary"
	"net/netip"
	"strconv"
	"strings"

	"cloud.google.com/go/container/apiv1/containerpb"
)

const (
	defaultPodBits = 14
	defaultSvcBits = 20
	ipv4Bits       = 32
)

type ranges struct {
	pod      netip.Prefix
	services netip.Prefix
	podName  string
	svcName  string
}

func addrToU32(a netip.Addr) uint32 {
	b := a.As4()

	return binary.BigEndian.Uint32(b[:])
}

func u32ToAddr(n uint32) netip.Addr {
	var b [4]byte

	binary.BigEndian.PutUint32(b[:], n)

	return netip.AddrFrom4(b)
}

// allocBlock returns the first bits-sized block in [from, to) that overlaps none of used.
func allocBlock(from, to string, bits int, used []netip.Prefix) (netip.Prefix, bool) {
	lo, hi := addrToU32(netip.MustParseAddr(from)), addrToU32(netip.MustParseAddr(to))
	size := uint32(1) << (ipv4Bits - bits)

	for a := lo; a+size-1 < hi && a >= lo; a += size {
		p := netip.PrefixFrom(u32ToAddr(a), bits)

		clash := false

		for _, u := range used {
			if u.Overlaps(p) {
				clash = true

				break
			}
		}

		if !clash {
			return p, true
		}
	}

	return netip.Prefix{}, false
}

// usedRangesLocked are the pod and service ranges of the project's clusters.
func (b *InMemoryBackend) usedRangesLocked(project string) []netip.Prefix {
	var out []netip.Prefix

	for _, c := range b.clusters {
		if c.Project != project {
			continue
		}

		ip := c.Cluster.GetIpAllocationPolicy()

		blocks := []string{ip.GetClusterIpv4CidrBlock(), ip.GetServicesIpv4CidrBlock(), c.Cluster.GetClusterIpv4Cidr()}

		for _, s := range blocks {
			if p, err := netip.ParsePrefix(s); err == nil {
				out = append(out, p)
			}
		}
	}

	return out
}

// wantBits reads a "/N" size request; ok is false for an explicit CIDR or an empty value.
func wantBits(s string) (int, bool) {
	rest, ok := strings.CutPrefix(s, "/")
	if !ok {
		return 0, false
	}

	n, err := strconv.Atoi(rest)

	return n, err == nil
}

func (b *InMemoryBackend) chooseRanges(project, region, subnet string, in *containerpb.Cluster) (ranges, error) {
	ip := in.GetIpAllocationPolicy()
	r := ranges{podName: ip.GetClusterSecondaryRangeName(), svcName: ip.GetServicesSecondaryRangeName()}
	pod := firstNonEmpty(ip.GetClusterIpv4CidrBlock(), ip.GetClusterIpv4Cidr(), in.GetClusterIpv4Cidr())
	svc := firstNonEmpty(ip.GetServicesIpv4CidrBlock(), ip.GetServicesIpv4Cidr())

	var err error

	if pod, err = b.secondary(project, region, subnet, r.podName, pod, "cluster_secondary_range_name"); err != nil {
		return r, err
	}

	if svc, err = b.secondary(project, region, subnet, r.svcName, svc, "services_secondary_range_name"); err != nil {
		return r, err
	}

	used := b.usedRangesLocked(project)

	if r.pod, err = pickRange(
		pod,
		defaultPodBits,
		minPodPrefix,
		maxPodPrefix,
		"10.0.0.0",
		"10.128.0.0",
		used,
		"cluster_ipv4_cidr_block",
	); err != nil {
		return r, err
	}

	used = append(used, r.pod)

	if r.services, err = pickRange(
		svc,
		defaultSvcBits,
		minSvcPrefix,
		maxSvcPrefix,
		"34.118.224.0",
		"34.120.0.0",
		used,
		"services_ipv4_cidr_block",
	); err != nil {
		return r, err
	}

	if r.pod.Overlaps(r.services) {
		return r, errInvalid(
			"ip_allocation_policy",
			"The pod range %s overlaps the services range %s.",
			r.pod,
			r.services,
		)
	}

	return r, nil
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}

	return ""
}

func (b *InMemoryBackend) secondary(project, region, subnet, rangeName, explicit, field string) (string, error) {
	if rangeName == "" || b.compute == nil {
		return explicit, nil
	}

	cidr, ok := b.compute.SecondaryRange(project, region, subnet, rangeName)
	if !ok {
		return "", errInvalid(field, "Secondary range %q does not exist in subnetwork %q.", rangeName, subnet)
	}

	return cidr, nil
}

func pickRange(
	want string,
	defBits, minBits, maxBits int,
	from, to string,
	used []netip.Prefix,
	field string,
) (netip.Prefix, error) {
	bits := defBits

	if n, ok := wantBits(want); ok {
		if n < minBits || n > maxBits {
			return netip.Prefix{}, errInvalid(field, "CIDR size /%d must be between /%d and /%d.", n, minBits, maxBits)
		}

		bits, want = n, ""
	}

	if want != "" {
		return parseCIDR(field, want, minBits, maxBits)
	}

	p, ok := allocBlock(from, to, bits, used)
	if !ok {
		return netip.Prefix{}, errInvalid(field, "No free /%d range is available.", bits)
	}

	return p, nil
}
