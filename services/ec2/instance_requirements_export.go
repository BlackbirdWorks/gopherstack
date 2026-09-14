package ec2

// InstanceRequirementsQuery is the exported form of instanceRequirementsQuery
// (handler_instance_types.go), letting other services -- currently
// autoscaling's MixedInstancesPolicy attribute-based instance selection,
// gopherstack-jgrn6 -- call ec2's real instance-type catalog matching engine
// directly, without going through the wire-form
// GetInstanceTypesFromInstanceRequirements HTTP parser. Field names and
// range/"*Set" semantics mirror instanceRequirementsQuery exactly; see its
// doc comment for wire-shape citations.
//
// NetworkBandwidthGbps and BaselineEbsBandwidthMbps are intentionally
// omitted: instanceTypeMatchesRequirements never filters on them (see
// instanceTypeMatchesNetworkRequirements's doc comment), so threading them
// through here could not change a match result.
type InstanceRequirementsQuery struct {
	BareMetal                       string
	BurstablePerformance            string
	LocalStorage                    string
	InstanceGenerations             []string
	AcceleratorManufacturers        []string
	VirtTypes                       []string
	ArchTypes                       []string
	AllowedInstanceTypes            []string
	ExcludedInstanceTypes           []string
	CPUManufacturers                []string
	AcceleratorNames                []string
	LocalStorageTypes               []string
	AcceleratorTypes                []string
	VCpuMin                         int64
	MemGiBPerVCpuMin                float64
	AcceleratorTotalMemoryMiBMin    int64
	AcceleratorTotalMemoryMiBMax    int64
	VCpuMax                         int64
	NetworkInterfaceCountMax        int64
	MemMax                          int64
	MemMin                          int64
	AcceleratorCountMin             int64
	AcceleratorCountMax             int64
	MemGiBPerVCpuMax                float64
	NetworkInterfaceCountMin        int64
	TotalLocalStorageGBMin          float64
	TotalLocalStorageGBMax          float64
	NetworkInterfaceCountMaxSet     bool
	AcceleratorTotalMemoryMiBMaxSet bool
	MemGiBPerVCpuMaxSet             bool
	MemGiBPerVCpuMinSet             bool
	VCpuMaxSet                      bool
	MemMaxSet                       bool
	TotalLocalStorageGBMinSet       bool
	TotalLocalStorageGBMaxSet       bool
	AcceleratorCountMaxSet          bool
	RequireHibernateSupport         bool
}

// toInternal converts q to the unexported instanceRequirementsQuery shape
// GetInstanceTypesFromInstanceRequirements/instanceTypeMatchesRequirements
// consume.
func (q InstanceRequirementsQuery) toInternal() *instanceRequirementsQuery {
	return &instanceRequirementsQuery{
		bareMetal:                       q.BareMetal,
		localStorage:                    q.LocalStorage,
		burstablePerformance:            q.BurstablePerformance,
		archTypes:                       q.ArchTypes,
		acceleratorTypes:                q.AcceleratorTypes,
		localStorageTypes:               q.LocalStorageTypes,
		acceleratorNames:                q.AcceleratorNames,
		acceleratorManufacturers:        q.AcceleratorManufacturers,
		virtTypes:                       q.VirtTypes,
		instanceGenerations:             q.InstanceGenerations,
		allowedTypes:                    q.AllowedInstanceTypes,
		excludedTypes:                   q.ExcludedInstanceTypes,
		cpuManufacturers:                q.CPUManufacturers,
		networkInterfaceCountMax:        q.NetworkInterfaceCountMax,
		networkInterfaceCountMin:        q.NetworkInterfaceCountMin,
		acceleratorTotalMemoryMiBMin:    q.AcceleratorTotalMemoryMiBMin,
		acceleratorTotalMemoryMiBMax:    q.AcceleratorTotalMemoryMiBMax,
		memGiBPerVCpuMax:                q.MemGiBPerVCpuMax,
		memGiBPerVCpuMin:                q.MemGiBPerVCpuMin,
		vcpuMax:                         q.VCpuMax,
		vcpuMin:                         q.VCpuMin,
		memMax:                          q.MemMax,
		memMin:                          q.MemMin,
		totalLocalStorageGBMin:          q.TotalLocalStorageGBMin,
		totalLocalStorageGBMax:          q.TotalLocalStorageGBMax,
		acceleratorCountMin:             q.AcceleratorCountMin,
		acceleratorCountMax:             q.AcceleratorCountMax,
		networkInterfaceCountMaxSet:     q.NetworkInterfaceCountMaxSet,
		acceleratorTotalMemoryMiBMaxSet: q.AcceleratorTotalMemoryMiBMaxSet,
		memGiBPerVCpuMaxSet:             q.MemGiBPerVCpuMaxSet,
		memGiBPerVCpuMinSet:             q.MemGiBPerVCpuMinSet,
		vcpuMaxSet:                      q.VCpuMaxSet,
		memMaxSet:                       q.MemMaxSet,
		totalLocalStorageGBMinSet:       q.TotalLocalStorageGBMinSet,
		totalLocalStorageGBMaxSet:       q.TotalLocalStorageGBMaxSet,
		acceleratorCountMaxSet:          q.AcceleratorCountMaxSet,
		requireHibernateSupport:         q.RequireHibernateSupport,
	}
}

// MatchInstanceTypes returns every cataloged EC2 instance type matching q,
// via the same instanceTypeMatchesRequirements engine
// GetInstanceTypesFromInstanceRequirements uses, in the same deterministic
// catalog order (sortedCatalogTypes). Reads only the package-level static
// instanceTypeCatalog, so -- unlike most InMemoryBackend methods -- it takes
// no lock: it is safe to call while holding another service's backend lock
// (see autoscaling's InstanceTypeResolver, wired in cli.go).
func (b *InMemoryBackend) MatchInstanceTypes(q InstanceRequirementsQuery) []string {
	return b.GetInstanceTypesFromInstanceRequirements(q.toInternal())
}
