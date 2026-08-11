package outposts

// catalogItemSeed and orderableInstanceTypeSeed are small, defensible static
// reference-data tables (a la services/grafana's grafanaVersions) standing
// in for AWS's own published Outposts hardware catalog -- real-world
// operational data (which rack/server SKUs and EC2 family/size combinations
// are currently orderable) that changes over time and is not encoded
// anywhere in the Go SDK module. Neither list is mutated after init and
// neither is part of Reset/Snapshot/Restore -- like grafanaVersions, this is
// read-only reference data, not customer-owned state. See PARITY.md's
// "Static reference-data operations" note.
//
// Every ID below matches CatalogItem.CatalogItemId's confirmed real pattern
// "OR-[A-Z0-9]{7}" (docs.aws.amazon.com/outposts/latest/APIReference/API_CatalogItem.html).
//
//nolint:gochecknoglobals,mnd // static seed data; every number below is a seeded hardware spec
var catalogItemSeed = []CatalogItem{
	{
		ID:                  "OR-RACKM05",
		ItemClass:           HardwareTypeRack,
		ItemStatus:          catalogItemStatusAvailable,
		PowerKva:            15,
		WeightLbs:           2010,
		SupportedStorage:    []string{supportedStorageEBS, supportedStorageS3},
		SupportedUplinkGbps: []int32{1, 10},
		EC2Capacities: []EC2Capacity{
			{Family: "m5", MaxSize: "m5.24xlarge", Quantity: "64"},
		},
	},
	{
		ID:                  "OR-RACKC05",
		ItemClass:           HardwareTypeRack,
		ItemStatus:          catalogItemStatusAvailable,
		PowerKva:            12,
		WeightLbs:           1980,
		SupportedStorage:    []string{supportedStorageEBS},
		SupportedUplinkGbps: []int32{10},
		EC2Capacities: []EC2Capacity{
			{Family: "c5", MaxSize: "c5.18xlarge", Quantity: "48"},
		},
	},
	{
		ID:                  "OR-SRVC6ID",
		ItemClass:           HardwareTypeServer,
		ItemStatus:          catalogItemStatusAvailable,
		PowerKva:            2,
		WeightLbs:           110,
		SupportedStorage:    []string{supportedStorageEBS},
		SupportedUplinkGbps: []int32{1, 10},
		EC2Capacities: []EC2Capacity{
			{Family: "c6id", MaxSize: "c6id.4xlarge", Quantity: "1"},
		},
	},
}

//nolint:gochecknoglobals,mnd // static seed data; every number below is a seeded hardware spec
var orderableInstanceTypeSeed = []OrderableInstanceType{
	{
		InstanceType:       "m5.xlarge",
		VCPUs:              4,
		MemoryInMib:        16384,
		NetworkPerformance: "Up to 10 Gigabit",
		FormFactor:         HardwareTypeRack,
		OutpostGeneration:  "GENERATION_1",
	},
	{
		InstanceType:       "m5.4xlarge",
		VCPUs:              16,
		MemoryInMib:        65536,
		NetworkPerformance: "10 Gigabit",
		FormFactor:         HardwareTypeRack,
		OutpostGeneration:  "GENERATION_2",
	},
	{
		InstanceType:       "c5.2xlarge",
		VCPUs:              8,
		MemoryInMib:        16384,
		NetworkPerformance: "Up to 10 Gigabit",
		FormFactor:         HardwareTypeRack,
		OutpostGeneration:  "GENERATION_1",
	},
	{
		InstanceType:       "c6id.xlarge",
		VCPUs:              4,
		MemoryInMib:        8192,
		NetworkPerformance: "Up to 12.5 Gigabit",
		FormFactor:         HardwareTypeServer,
	},
	{
		InstanceType:       "c6id.2xlarge",
		VCPUs:              8,
		MemoryInMib:        16384,
		NetworkPerformance: "Up to 12.5 Gigabit",
		FormFactor:         HardwareTypeServer,
	},
}

// findCatalogItem returns the seeded catalog item with the given ID.
func findCatalogItem(id string) (CatalogItem, bool) {
	for _, c := range catalogItemSeed {
		if c.ID == id {
			return c, true
		}
	}

	return CatalogItem{}, false
}

// findOrderableInstanceType returns the seeded orderable instance type with
// the given instance type name.
func findOrderableInstanceType(instanceType string) (OrderableInstanceType, bool) {
	for _, it := range orderableInstanceTypeSeed {
		if it.InstanceType == instanceType {
			return it, true
		}
	}

	return OrderableInstanceType{}, false
}

// orderableInstanceTypesForHardware returns the seeded instance types whose
// FormFactor matches supportedHardwareType ("RACK" or "SERVER"). An empty
// supportedHardwareType matches every seeded entry.
func orderableInstanceTypesForHardware(supportedHardwareType string) []OrderableInstanceType {
	if supportedHardwareType == "" {
		return append([]OrderableInstanceType(nil), orderableInstanceTypeSeed...)
	}

	out := make([]OrderableInstanceType, 0, len(orderableInstanceTypeSeed))

	for _, it := range orderableInstanceTypeSeed {
		if it.FormFactor == supportedHardwareType {
			out = append(out, it)
		}
	}

	return out
}
