package lightsail

// This file backs family A (9 ops: GetBlueprints, GetBundles,
// GetRelationalDatabaseBlueprints, GetRelationalDatabaseBundles,
// GetBucketBundles, GetDistributionBundles, GetContainerServicePowers,
// GetRegions, GetContainerAPIMetadata) -- static AWS-curated catalog data
// this emulator cannot derive from any user action (PARITY.md families A,
// 4.11). Every seed value below is an emulator decision, explicitly labeled
// as such, NOT a claim about AWS's real current catalog/pricing (which
// changes over time and is not published anywhere in this SDK module) --
// see PARITY.md's repeated warning against fabricating blueprint/bundle IDs
// or prices from memory. RegionName's 20 real values ARE SDK-derived
// (types/enums.go), so GetRegions is the one op in this family backed by an
// actual SDK enum rather than an invented seed.

import (
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// Blueprint mirrors types.Blueprint.
type Blueprint struct {
	BlueprintID string
	Name        string
	Group       string
	Type        string
	Platform    string
	Description string
	Version     string
	VersionCode string
	AppCategory string
	MinPower    int32
	IsActive    bool
}

// Bundle mirrors types.Bundle.
type Bundle struct {
	BundleID               string
	Name                   string
	InstanceType           string
	SupportedAppCategories []string
	SupportedPlatforms     []string
	CPUCount               int32
	Power                  int32
	DiskSizeInGb           int32
	PublicIPv4AddressCount int32
	RAMSizeInGb            float32
	Price                  float32
	TransferPerMonthInGb   int32
	IsActive               bool
}

// RelationalDatabaseBlueprint mirrors types.RelationalDatabaseBlueprint.
type RelationalDatabaseBlueprint struct {
	BlueprintID              string
	Engine                   string
	EngineDescription        string
	EngineVersion            string
	EngineVersionDescription string
	IsEngineDefault          bool
}

// RelationalDatabaseBundle mirrors types.RelationalDatabaseBundle.
type RelationalDatabaseBundle struct {
	BundleID             string
	Name                 string
	CPUCount             int32
	DiskSizeInGb         int32
	RAMSizeInGb          float32
	Price                float32
	TransferPerMonthInGb int32
	IsActive             bool
	IsEncrypted          bool
}

// BucketBundle mirrors types.BucketBundle.
type BucketBundle struct {
	BundleID             string
	Name                 string
	Price                float32
	StoragePerMonthInGb  int32
	TransferPerMonthInGb int32
	IsActive             bool
}

// DistributionBundle mirrors types.DistributionBundle.
type DistributionBundle struct {
	BundleID             string
	Name                 string
	Price                float32
	TransferPerMonthInGb int32
	IsActive             bool
}

// ContainerServicePowerSeed mirrors types.ContainerServicePower.
type ContainerServicePowerSeed struct {
	PowerID     string
	Name        string
	CPUCount    float32
	RAMSizeInGb float32
	Price       float32
	IsActive    bool
}

// RegionSeed mirrors types.Region.
type RegionSeed struct {
	Name              string
	DisplayName       string
	Description       string
	ContinentCode     string
	AvailabilityZones []string
}

// seedBlueprints is a small, defensible, CLEARLY SYNTHETIC stand-in for
// AWS's real published blueprint catalog -- not the authoritative list.
//
//nolint:gochecknoglobals // synthetic seed table, matches services/outposts' seed_data.go
var seedBlueprints = []Blueprint{
	{
		BlueprintID: "amazon_linux_2023", Name: "Amazon Linux 2023", Group: "amazon_linux",
		Type: "os", Platform: InstancePlatformLinuxUnix, Description: "Amazon Linux 2023 base OS image",
		Version: "1.0", VersionCode: "1", MinPower: 0, IsActive: true,
	},
	{
		BlueprintID: "ubuntu_22_04", Name: "Ubuntu 22.04 LTS", Group: blueprintGroupUbuntu,
		Type: "os", Platform: InstancePlatformLinuxUnix, Description: "Ubuntu Server 22.04 LTS base OS image",
		Version: "22.04", VersionCode: "1", MinPower: 0, IsActive: true,
	},
	{
		BlueprintID: "wordpress", Name: "WordPress", Group: "wordpress",
		Type: "app", Platform: InstancePlatformLinuxUnix, Description: "WordPress pre-configured application image",
		Version: "6.0", VersionCode: "1", MinPower: 0, IsActive: true,
	},
	{
		BlueprintID: "windows_server_2022", Name: "Windows Server 2022", Group: "windows_server",
		Type: "os", Platform: InstancePlatformWindows, Description: "Windows Server 2022 base OS image",
		Version: "2022", VersionCode: "1", MinPower: 0, IsActive: true,
	},
}

// seedBundles is a small, defensible, CLEARLY SYNTHETIC stand-in for AWS's
// real published instance-bundle catalog.
//
//nolint:gochecknoglobals,mnd // synthetic seed table, matches services/outposts' seed_data.go
var seedBundles = []Bundle{
	{
		BundleID: "nano_3_0", Name: "Nano", InstanceType: bundleInstanceTypeNano,
		SupportedPlatforms: []string{InstancePlatformLinuxUnix, InstancePlatformWindows},
		CPUCount:           1, Power: 400, DiskSizeInGb: 20, PublicIPv4AddressCount: 1,
		RAMSizeInGb: 0.5, Price: 3.5, TransferPerMonthInGb: 1024, IsActive: true,
	},
	{
		BundleID: "micro_3_0", Name: "Micro", InstanceType: bundleInstanceTypeMicro,
		SupportedPlatforms: []string{InstancePlatformLinuxUnix, InstancePlatformWindows},
		CPUCount:           1, Power: 500, DiskSizeInGb: 40, PublicIPv4AddressCount: 1,
		RAMSizeInGb: 1, Price: 5, TransferPerMonthInGb: 2048, IsActive: true,
	},
	{
		BundleID: "small_3_0", Name: "Small", InstanceType: bundleInstanceTypeSmall,
		SupportedPlatforms: []string{InstancePlatformLinuxUnix, InstancePlatformWindows},
		CPUCount:           2, Power: 1000, DiskSizeInGb: 60, PublicIPv4AddressCount: 1,
		RAMSizeInGb: 2, Price: 10, TransferPerMonthInGb: 3072, IsActive: true,
	},
	{
		BundleID: "medium_3_0", Name: "Medium", InstanceType: bundleInstanceTypeMedium,
		SupportedPlatforms: []string{InstancePlatformLinuxUnix, InstancePlatformWindows},
		CPUCount:           2, Power: 2000, DiskSizeInGb: 80, PublicIPv4AddressCount: 1,
		RAMSizeInGb: 4, Price: 20, TransferPerMonthInGb: 4096, IsActive: true,
	},
}

//nolint:gochecknoglobals // static reference table, read-only
var seedRDSBlueprints = []RelationalDatabaseBlueprint{
	{
		BlueprintID: "mysql_8_0", Engine: "mysql", EngineDescription: "MySQL",
		EngineVersion: "8.0", EngineVersionDescription: "MySQL 8.0", IsEngineDefault: true,
	},
}

//nolint:gochecknoglobals,mnd // static reference/pricing seed table (synthetic, documented)
var seedRDSBundles = []RelationalDatabaseBundle{
	{
		BundleID: "micro_2_0", Name: "Micro", CPUCount: 1, DiskSizeInGb: 40,
		RAMSizeInGb: 1, Price: 15, TransferPerMonthInGb: 100, IsActive: true, IsEncrypted: false,
	},
	{
		BundleID: "small_2_0", Name: "Small", CPUCount: 1, DiskSizeInGb: 80,
		RAMSizeInGb: 2, Price: 30, TransferPerMonthInGb: 200, IsActive: true, IsEncrypted: true,
	},
}

//nolint:gochecknoglobals,mnd // static reference/pricing seed table (synthetic, documented)
var seedBucketBundles = []BucketBundle{
	{
		BundleID:             "small_1_0",
		Name:                 "Small Bucket",
		Price:                1,
		StoragePerMonthInGb:  5,
		TransferPerMonthInGb: 5,
		IsActive:             true,
	},
	{
		BundleID:             "medium_1_0",
		Name:                 "Medium Bucket",
		Price:                3,
		StoragePerMonthInGb:  50,
		TransferPerMonthInGb: 50,
		IsActive:             true,
	},
}

//nolint:gochecknoglobals,mnd // static reference/pricing seed table (synthetic, documented)
var seedDistributionBundles = []DistributionBundle{
	{BundleID: "small_1_0", Name: "Small Distribution", Price: 2.5, TransferPerMonthInGb: 100, IsActive: true},
	{BundleID: "medium_1_0", Name: "Medium Distribution", Price: 10, TransferPerMonthInGb: 1024, IsActive: true},
}

// (ContainerServicePowerName's Values(): nano/micro/small/medium/large/...),
// only CPUCount/RAMSizeInGb/Price are this emulator's own synthetic seed.
//
//nolint:gochecknoglobals,mnd // synthetic seed table -- PowerID/Name values ARE SDK-confirmed
var seedContainerServicePowers = []ContainerServicePowerSeed{
	{PowerID: "nano", Name: "nano", CPUCount: 0.25, RAMSizeInGb: 0.5, Price: 7, IsActive: true},
	{PowerID: "micro", Name: "micro", CPUCount: 0.25, RAMSizeInGb: 1, Price: 10, IsActive: true},
	{PowerID: "small", Name: "small", CPUCount: 0.5, RAMSizeInGb: 2, Price: 20, IsActive: true},
	{PowerID: "medium", Name: "medium", CPUCount: 1, RAMSizeInGb: 4, Price: 40, IsActive: true},
	{PowerID: "large", Name: "large", CPUCount: 2, RAMSizeInGb: 8, Price: 80, IsActive: true},
}

// seedRegions is derived directly from types.RegionName's own Values()
// enumeration (20 real values, PARITY.md 4.11) -- the one genuinely
// SDK-sourced part of this file. DisplayName/Description/ContinentCode are
// this backend's own descriptive labels, not claimed as SDK-derived.
//
//nolint:gochecknoglobals // static reference table, read-only
var seedRegions = []RegionSeed{
	{
		Name:              "us-east-1",
		DisplayName:       "US East (N. Virginia)",
		Description:       "US East (N. Virginia)",
		ContinentCode:     "NA",
		AvailabilityZones: []string{"us-east-1a", "us-east-1b"},
	},
	{
		Name:              "us-east-2",
		DisplayName:       "US East (Ohio)",
		Description:       "US East (Ohio)",
		ContinentCode:     "NA",
		AvailabilityZones: []string{"us-east-2a"},
	},
	{
		Name:              "us-west-1",
		DisplayName:       "US West (N. California)",
		Description:       "US West (N. California)",
		ContinentCode:     "NA",
		AvailabilityZones: []string{"us-west-1a"},
	},
	{
		Name:              "us-west-2",
		DisplayName:       "US West (Oregon)",
		Description:       "US West (Oregon)",
		ContinentCode:     "NA",
		AvailabilityZones: []string{"us-west-2a"},
	},
	{
		Name:              "eu-west-1",
		DisplayName:       "EU (Ireland)",
		Description:       "EU (Ireland)",
		ContinentCode:     "EU",
		AvailabilityZones: []string{"eu-west-1a"},
	},
	{
		Name:              "eu-west-2",
		DisplayName:       "EU (London)",
		Description:       "EU (London)",
		ContinentCode:     "EU",
		AvailabilityZones: []string{"eu-west-2a"},
	},
	{
		Name:              "eu-west-3",
		DisplayName:       "EU (Paris)",
		Description:       "EU (Paris)",
		ContinentCode:     "EU",
		AvailabilityZones: []string{"eu-west-3a"},
	},
	{
		Name:              "eu-central-1",
		DisplayName:       "EU (Frankfurt)",
		Description:       "EU (Frankfurt)",
		ContinentCode:     "EU",
		AvailabilityZones: []string{"eu-central-1a"},
	},
	{
		Name:              "eu-north-1",
		DisplayName:       "EU (Stockholm)",
		Description:       "EU (Stockholm)",
		ContinentCode:     "EU",
		AvailabilityZones: []string{"eu-north-1a"},
	},
	{
		Name:              "eu-south-2",
		DisplayName:       "EU (Spain)",
		Description:       "EU (Spain)",
		ContinentCode:     "EU",
		AvailabilityZones: []string{"eu-south-2a"},
	},
	{
		Name:              "ca-central-1",
		DisplayName:       "Canada (Central)",
		Description:       "Canada (Central)",
		ContinentCode:     "NA",
		AvailabilityZones: []string{"ca-central-1a"},
	},
	{
		Name:              "ap-east-1",
		DisplayName:       "Asia Pacific (Hong Kong)",
		Description:       "Asia Pacific (Hong Kong)",
		ContinentCode:     "AS",
		AvailabilityZones: []string{"ap-east-1a"},
	},
	{
		Name:              "ap-south-1",
		DisplayName:       "Asia Pacific (Mumbai)",
		Description:       "Asia Pacific (Mumbai)",
		ContinentCode:     "AS",
		AvailabilityZones: []string{"ap-south-1a"},
	},
	{
		Name:              "ap-southeast-1",
		DisplayName:       "Asia Pacific (Singapore)",
		Description:       "Asia Pacific (Singapore)",
		ContinentCode:     "AS",
		AvailabilityZones: []string{"ap-southeast-1a"},
	},
	{
		Name:              "ap-southeast-2",
		DisplayName:       "Asia Pacific (Sydney)",
		Description:       "Asia Pacific (Sydney)",
		ContinentCode:     "OC",
		AvailabilityZones: []string{"ap-southeast-2a"},
	},
	{
		Name:              "ap-northeast-1",
		DisplayName:       "Asia Pacific (Tokyo)",
		Description:       "Asia Pacific (Tokyo)",
		ContinentCode:     "AS",
		AvailabilityZones: []string{"ap-northeast-1a"},
	},
	{
		Name:              "ap-northeast-2",
		DisplayName:       "Asia Pacific (Seoul)",
		Description:       "Asia Pacific (Seoul)",
		ContinentCode:     "AS",
		AvailabilityZones: []string{"ap-northeast-2a"},
	},
	{
		Name:              "ap-southeast-3",
		DisplayName:       "Asia Pacific (Jakarta)",
		Description:       "Asia Pacific (Jakarta)",
		ContinentCode:     "AS",
		AvailabilityZones: []string{"ap-southeast-3a"},
	},
	{
		Name:              "ap-southeast-5",
		DisplayName:       "Asia Pacific (Malaysia)",
		Description:       "Asia Pacific (Malaysia)",
		ContinentCode:     "AS",
		AvailabilityZones: []string{"ap-southeast-5a"},
	},
	{
		Name:              "sa-east-1",
		DisplayName:       "South America (Sao Paulo)",
		Description:       "South America (Sao Paulo)",
		ContinentCode:     "SA",
		AvailabilityZones: []string{"sa-east-1a"},
	},
}

// GetBlueprints returns the seed Blueprint catalog, optionally filtered by
// appCategory and IncludeInactive.
func (b *InMemoryBackend) GetBlueprints(appCategory string, includeInactive bool) []Blueprint {
	out := make([]Blueprint, 0, len(seedBlueprints))

	for _, bp := range seedBlueprints {
		if appCategory != "" && bp.AppCategory != appCategory {
			continue
		}

		if !includeInactive && !bp.IsActive {
			continue
		}

		out = append(out, bp)
	}

	return out
}

// GetBundles returns the seed Bundle catalog, optionally filtered by
// appCategory and IncludeInactive.
func (b *InMemoryBackend) GetBundles(appCategory string, includeInactive bool) []Bundle {
	out := make([]Bundle, 0, len(seedBundles))

	for _, bd := range seedBundles {
		if appCategory != "" && !slices.Contains(bd.SupportedAppCategories, appCategory) {
			continue
		}

		if !includeInactive && !bd.IsActive {
			continue
		}

		out = append(out, bd)
	}

	return out
}

// GetRelationalDatabaseBlueprints returns the seed RDS blueprint catalog.
func (b *InMemoryBackend) GetRelationalDatabaseBlueprints() []RelationalDatabaseBlueprint {
	return seedRDSBlueprints
}

// GetRelationalDatabaseBundles returns the seed RDS bundle catalog.
func (b *InMemoryBackend) GetRelationalDatabaseBundles(includeInactive bool) []RelationalDatabaseBundle {
	out := make([]RelationalDatabaseBundle, 0, len(seedRDSBundles))

	for _, bd := range seedRDSBundles {
		if !includeInactive && !bd.IsActive {
			continue
		}

		out = append(out, bd)
	}

	return out
}

// GetBucketBundles returns the seed bucket bundle catalog.
func (b *InMemoryBackend) GetBucketBundles(includeInactive bool) []BucketBundle {
	out := make([]BucketBundle, 0, len(seedBucketBundles))

	for _, bd := range seedBucketBundles {
		if !includeInactive && !bd.IsActive {
			continue
		}

		out = append(out, bd)
	}

	return out
}

// GetDistributionBundles returns the seed distribution bundle catalog.
func (b *InMemoryBackend) GetDistributionBundles() []DistributionBundle {
	return seedDistributionBundles
}

// GetContainerServicePowers returns the seed container-service power tiers.
func (b *InMemoryBackend) GetContainerServicePowers() []ContainerServicePowerSeed {
	return seedContainerServicePowers
}

// GetRegions returns the seed Region catalog, SDK-derived from
// types.RegionName's own enum values (see this file's doc comment).
func (b *InMemoryBackend) GetRegions() []RegionSeed {
	return seedRegions
}

// paginateGeneric applies pkgs/page's opaque-token pagination to items.
func paginateGeneric[T any](items []T, token string) (page.Page[T], error) {
	if err := page.ValidateToken(token); err != nil {
		return page.Page[T]{}, validationError(err.Error())
	}

	return page.New(items, token, 0, defaultPageLimit), nil
}
