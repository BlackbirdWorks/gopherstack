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
		NameserversPerDelegation:             8,
		TargetNameServersPerManagedZone:      50,
		TargetNameServersPerPolicy:           50,
		NetworksPerPolicy:                    100,
		NetworksPerManagedZone:               100,
		NetworksPerResponsePolicy:            100,
		Policies:                             100,
		ResponsePolicies:                     100,
		ResponsePolicyRulesPerResponsePolicy: maxRules,
		PeeringZonesPerTargetNetwork:         1000,
		ManagedZonesPerNetwork:               10000,
		DnsKeysPerManagedZone:                6,
		GkeClustersPerManagedZone:            100,
		GkeClustersPerPolicy:                 100,
		GkeClustersPerResponsePolicy:         100,
		ManagedZonesPerGkeCluster:            100,
		InternetHealthChecksPerManagedZone:   10,
		WhitelistedKeySpecs:                  []*dnsapi.DnsKeySpec{},
	}
}

// GetProject returns the project's DNS quota block.
func (b *InMemoryBackend) GetProject(project string) *dnsapi.Project {
	return &dnsapi.Project{Kind: "dns#project", Id: project, Number: projectNumber(project), Quota: defaultQuota()}
}
