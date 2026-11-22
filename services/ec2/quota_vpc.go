package ec2

import (
	"errors"
	"strings"
)

const (
	vcpuHighMemory = "highmem"
	vcpuInf        = "inf"
	vcpuGVT        = "gvt"
	vcpuStandard   = "standard"
	vcpuP          = "p"
	vcpuF          = "f"
	vcpuX          = "x"
)

const (
	quotaSubnetsPerVPC      = "L-407747CB"
	quotaIGWs               = "L-A4707A72"
	quotaNATPerAZ           = "L-FE5A380F"
	quotaNetworkInterfaces  = "L-DF5E4CA3"
	quotaSecurityGroups     = "L-E79EC296"
	quotaRulesPerGroup      = "L-0EA8095F"
	quotaRouteTablesPerVPC  = "L-589F43AA"
	quotaRoutesPerTable     = "L-93826ACB"
	quotaOnDemandStandard   = "L-1216C47A"
	quotaOnDemandGVT        = "L-DB2E81BA"
	quotaOnDemandP          = "L-417A185B"
	quotaOnDemandF          = "L-74FC7D96"
	quotaOnDemandX          = "L-7295265B"
	quotaOnDemandInf        = "L-1945791B"
	quotaOnDemandHighMemory = "L-43DA4232"
	quotaSpotStandard       = "L-34B43A08"
	quotaSpotP              = "L-7212CCBC"
	quotaSpotGVT            = "L-3819A6DF"
)

// The limit errors CreateNatGateway, CreateNetworkInterface and CreateRoute document in the EC2 error code
// reference (errors-overview.html: NatGatewayLimitExceeded, NetworkInterfaceLimitExceeded,
// ResourceCountLimitExceeded).
var (
	ErrNatGatewayLimitExceeded       = errors.New("NatGatewayLimitExceeded")
	ErrNetworkInterfaceLimitExceeded = errors.New("NetworkInterfaceLimitExceeded")
	ErrResourceCountLimitExceeded    = errors.New("ResourceCountLimitExceeded")
)

// vcpuClass maps an instance type to the vCPU quota class the type is counted under.
func vcpuClass(instanceType string) string {
	family, _, _ := strings.Cut(instanceType, ".")

	switch {
	case strings.HasPrefix(family, "u-"):
		return vcpuHighMemory
	case strings.HasPrefix(family, vcpuInf):
		return vcpuInf
	case strings.HasPrefix(family, "vt"), strings.HasPrefix(family, "g"):
		return vcpuGVT
	case strings.HasPrefix(family, vcpuP):
		return vcpuP
	case strings.HasPrefix(family, vcpuF):
		return vcpuF
	case strings.HasPrefix(family, vcpuX):
		return vcpuX
	case family != "" && strings.ContainsRune("acdhimrtz", rune(family[0])):
		return vcpuStandard
	default:
		return ""
	}
}

func (b *InMemoryBackend) typeVCPUs(instanceType string) int {
	return int(instanceTypeCatalog[instanceType].vcpus)
}

func (b *InMemoryBackend) onDemandVCPUsLocked(class string) int {
	spotIDs := map[string]bool{}
	for _, r := range b.spotRequests.All() {
		if r.InstanceID != "" {
			spotIDs[r.InstanceID] = true
		}
	}

	n := 0

	for _, in := range b.instances.All() {
		live := in.State.Name == StateRunning.Name || in.State.Name == StatePending.Name
		if live && !spotIDs[in.ID] && vcpuClass(in.InstanceType) == class {
			n += b.typeVCPUs(in.InstanceType)
		}
	}

	return n
}

func (b *InMemoryBackend) spotVCPUsLocked(class string) int {
	n := 0

	for _, r := range b.spotRequests.All() {
		if (r.State == stateActive || r.State == "open") && vcpuClass(r.LaunchSpec.InstanceType) == class {
			n += b.typeVCPUs(r.LaunchSpec.InstanceType)
		}
	}

	return n
}

func (b *InMemoryBackend) maxNATPerAZLocked() int {
	perAZ := map[string]int{}
	most := 0

	for _, n := range b.natGateways.All() {
		if n.State == natGatewayStateDeleted {
			continue
		}

		perAZ[n.AvailabilityZone]++
		most = max(most, perAZ[n.AvailabilityZone])
	}

	return most
}

func maxGroupSize[T any](items []*T, key func(*T) string) int {
	per := map[string]int{}
	most := 0

	for _, it := range items {
		per[key(it)]++
		most = max(most, per[key(it)])
	}

	return most
}

// QuotaCount returns the live usage of an EC2 or VPC quota in the backend's Region; ok is false for a quota
// this backend does not derive from its resources.
func (b *InMemoryBackend) QuotaCount(code string) (int, bool) {
	b.mu.RLock("QuotaCount")
	defer b.mu.RUnlock()

	if n, ok := b.vpcQuotaCountLocked(code); ok {
		return n, true
	}

	classes := map[string]string{
		quotaOnDemandStandard: vcpuStandard, quotaOnDemandGVT: vcpuGVT, quotaOnDemandP: vcpuP, quotaOnDemandF: vcpuF,
		quotaOnDemandX: vcpuX, quotaOnDemandInf: vcpuInf, quotaOnDemandHighMemory: vcpuHighMemory,
	}
	if class, ok := classes[code]; ok {
		return b.onDemandVCPUsLocked(class), true
	}

	spot := map[string]string{quotaSpotStandard: vcpuStandard, quotaSpotP: vcpuP, quotaSpotGVT: vcpuGVT}
	if class, ok := spot[code]; ok {
		return b.spotVCPUsLocked(class), true
	}

	return 0, false
}

func (b *InMemoryBackend) vpcQuotaCountLocked(code string) (int, bool) {
	switch code {
	case quotaSubnetsPerVPC:
		return maxGroupSize(b.subnets.All(), func(s *Subnet) string { return s.VPCID }), true
	case quotaIGWs:
		return len(b.internetGateways.All()), true
	case quotaNATPerAZ:
		return b.maxNATPerAZLocked(), true
	case quotaNetworkInterfaces:
		return len(b.networkInterfaces.All()), true
	case quotaSecurityGroups:
		return len(b.securityGroups.All()), true
	case quotaRouteTablesPerVPC:
		return maxGroupSize(b.routeTables.All(), func(r *RouteTable) string { return r.VPCID }), true
	case quotaRoutesPerTable:
		most := 0
		for _, rt := range b.routeTables.All() {
			most = max(most, userRouteCount(rt))
		}

		return most, true
	case quotaRulesPerGroup:
		most := 0
		for _, sg := range b.securityGroups.All() {
			most = max(most, len(sg.IngressRules), len(sg.EgressRules))
		}

		return most, true
	default:
		return 0, false
	}
}

func (b *InMemoryBackend) natGatewaysInZoneLocked(zone string) int {
	n := 0

	for _, g := range b.natGateways.All() {
		if g.AvailabilityZone == zone && g.State != natGatewayStateDeleted {
			n++
		}
	}

	return n
}

func userRouteCount(rt *RouteTable) int {
	n := 0

	for _, r := range rt.Routes {
		if r.GatewayID != routeGatewayLocal {
			n++
		}
	}

	return n
}
