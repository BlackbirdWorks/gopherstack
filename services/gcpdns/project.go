package gcpdns

import (
	dnsapi "google.golang.org/api/dns/v1"
)

const projectNumberMod = 1_000_000_000_000

func projectNumber(project string) uint64 {
	var h uint64 = 14695981039346656037

	for i := range len(project) {
		h ^= uint64(project[i])
		h *= 1099511628211
	}

	return h%projectNumberMod + projectNumberMod/10
}

func defaultQuota() *dnsapi.Quota {
	return &dnsapi.Quota{
		Kind:                                 "dns#quota",
		ManagedZones:                         maxZones,
		ResourceRecordsPerRrset:              maxRRDatas,
		RrsetAdditionsPerChange:              maxChangeOps,
		RrsetDeletionsPerChange:              maxChangeOps,
		RrsetsPerManagedZone:                 maxRRSets,
		TotalRrdataSizePerChange:             maxChangeLen,
		ItemsPerRoutingPolicy:                maxRouting,
		NameserversPerDelegation:             delegations,
		TargetNameServersPerManagedZone:      maxTargets,
		TargetNameServersPerPolicy:           maxTargets,
		NetworksPerPolicy:                    maxNetworks,
		NetworksPerManagedZone:               maxNetworks,
		NetworksPerResponsePolicy:            maxNetworks,
		Policies:                             maxPolicies,
		ResponsePolicies:                     maxPolicies,
		ResponsePolicyRulesPerResponsePolicy: maxRules,
		PeeringZonesPerTargetNetwork:         peeringZones,
		ManagedZonesPerNetwork:               maxZones,
		DnsKeysPerManagedZone:                keysPerZone,
		GkeClustersPerManagedZone:            maxPolicies,
		GkeClustersPerPolicy:                 maxPolicies,
		GkeClustersPerResponsePolicy:         maxPolicies,
		ManagedZonesPerGkeCluster:            maxPolicies,
		InternetHealthChecksPerManagedZone:   healthChecks,
		WhitelistedKeySpecs:                  []*dnsapi.DnsKeySpec{},
	}
}

// GetProject returns the project's DNS quota block.
func (b *InMemoryBackend) GetProject(project string) *dnsapi.Project {
	return &dnsapi.Project{Kind: "dns#project", Id: project, Number: projectNumber(project), Quota: defaultQuota()}
}
