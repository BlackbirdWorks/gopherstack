package gcpcompute

import (
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/services/gcpcontainer"
)

// NetworkExists reports whether the VPC network exists in project.
func (b *InMemoryBackend) NetworkExists(project, network string) bool {
	b.mu.RLock("NetworkExists")
	defer b.mu.RUnlock()

	_, ok := b.networks[key(project, network)]

	return ok
}

// SubnetworkExists reports whether the subnetwork exists in the region of project.
func (b *InMemoryBackend) SubnetworkExists(project, region, subnetwork string) bool {
	b.mu.RLock("SubnetworkExists")
	defer b.mu.RUnlock()

	_, ok := b.subnets[key(project, region, subnetwork)]

	return ok
}

// SecondaryRange returns the CIDR of a subnetwork's named secondary range.
func (b *InMemoryBackend) SecondaryRange(project, region, subnetwork, rangeName string) (string, bool) {
	b.mu.RLock("SecondaryRange")
	defer b.mu.RUnlock()

	s, ok := b.subnets[key(project, region, subnetwork)]
	if !ok {
		return "", false
	}

	for _, r := range s.SecondaryIpRanges {
		if r.RangeName == rangeName {
			return r.IpCidrRange, true
		}
	}

	return "", false
}

// MachineTypeExists reports whether a predefined or custom machine type name is valid.
func (b *InMemoryBackend) MachineTypeExists(machineType string) bool {
	if _, ok := findMachine(machineType); ok {
		return true
	}

	_, tail, found := strings.Cut(machineType, "custom-")
	if !found {
		return false
	}

	family := strings.TrimSuffix(strings.TrimSuffix(machineType, "custom-"+tail), "-")
	switch family {
	case "", "n1", "n2", "n2d", "e2":
		_, _, ok := customMachine("custom-" + tail)

		return ok
	default:
		return false
	}
}

// EachSubnetwork calls fn for every subnetwork of project in a stable order.
func (b *InMemoryBackend) EachSubnetwork(project string, fn func(gcpcontainer.SubnetInfo)) {
	b.mu.RLock("EachSubnetwork")
	var infos []gcpcontainer.SubnetInfo

	for k, s := range b.subnets {
		if !strings.HasPrefix(k, project+"/") {
			continue
		}

		net := trimURL(s.Network)
		info := gcpcontainer.SubnetInfo{
			Network: net, Subnetwork: trimURL(s.SelfLink), CIDR: s.IpCidrRange, Region: lastSegment(s.Region),
			Secondary: map[string]string{},
		}

		if parts := strings.Split(net, "/"); len(parts) > 1 {
			info.NetworkProj = parts[1]
		}

		for _, r := range s.SecondaryIpRanges {
			info.Secondary[r.RangeName] = r.IpCidrRange
		}

		infos = append(infos, info)
	}
	b.mu.RUnlock()

	slices.SortFunc(
		infos,
		func(a, c gcpcontainer.SubnetInfo) int { return strings.Compare(a.Subnetwork, c.Subnetwork) },
	)

	for _, i := range infos {
		fn(i)
	}
}
