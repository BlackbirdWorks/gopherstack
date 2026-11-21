package gcpcompute

import (
	"fmt"
	"hash/fnv"
	"slices"
	"sort"
	"strconv"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	catalogCreated     = "1969-12-31T16:00:00.000-08:00"
	maxPDs             = 128
	maxPDSizeGb        = 263168
	gbToMb             = 1024
	zonesPerRegionHint = 3
	zonesABC           = "abc"
	memStepMb          = 256
	gbStd              = 4
	gbHigh             = 8
	gbN1               = 3.75
)

const (
	regAsiaNE1  = "asia-northeast1"
	regEuropeW2 = "europe-west2"
	regEuropeW3 = "europe-west3"
	regUSEast4  = "us-east4"
	regUSWest2  = "us-west2"
)

type regionSpec struct {
	name      string
	autoRange string
	zones     string
	platform  string
}

// regionCatalog lists the supported regions with their documented auto-mode subnet ranges
// (docs.cloud.google.com/compute/docs/regions-zones, docs.cloud.google.com/vpc/docs/subnets). New regions are
// appended: the position of a region selects its external address pool.
var regionCatalog = []regionSpec{ //nolint:gochecknoglobals // read-only lookup table
	{"asia-east1", "10.140.0.0/20", zonesABC, platBroadwell},
	{regAsiaNE1, "10.146.0.0/20", zonesABC, platBroadwell},
	{"asia-south1", "10.160.0.0/20", zonesABC, platBroadwell},
	{"asia-southeast1", "10.148.0.0/20", zonesABC, platBroadwell},
	{"australia-southeast1", "10.152.0.0/20", zonesABC, platBroadwell},
	{"europe-west1", "10.132.0.0/20", "bcd", platBroadwell},
	{regEuropeW2, "10.154.0.0/20", zonesABC, platBroadwell},
	{regEuropeW3, "10.156.0.0/20", zonesABC, platBroadwell},
	{"europe-west4", "10.164.0.0/20", zonesABC, platBroadwell},
	{"southamerica-east1", "10.158.0.0/20", zonesABC, platBroadwell},
	{"us-central1", "10.128.0.0/20", "abcf", platBroadwell},
	{"us-east1", "10.142.0.0/20", "bcd", platBroadwell},
	{regUSEast4, "10.150.0.0/20", zonesABC, platBroadwell},
	{"us-west1", "10.138.0.0/20", zonesABC, platBroadwell},
	{regUSWest2, "10.168.0.0/20", zonesABC, platBroadwell},
	{"africa-south1", "10.218.0.0/20", zonesABC, platBroadwell},
	{"asia-east2", "10.170.0.0/20", zonesABC, platBroadwell},
	{"asia-northeast2", "10.174.0.0/20", zonesABC, platBroadwell},
	{"asia-northeast3", "10.178.0.0/20", zonesABC, platBroadwell},
	{"asia-south2", "10.190.0.0/20", zonesABC, platBroadwell},
	{"asia-southeast2", "10.184.0.0/20", zonesABC, platBroadwell},
	{"asia-southeast3", "10.232.0.0/20", zonesABC, platBroadwell},
	{"australia-southeast2", "10.192.0.0/20", zonesABC, platBroadwell},
	{"europe-central2", "10.186.0.0/20", zonesABC, platBroadwell},
	{"europe-north1", "10.166.0.0/20", zonesABC, platBroadwell},
	{"europe-north2", "10.226.0.0/20", zonesABC, platBroadwell},
	{"europe-southwest1", "10.204.0.0/20", zonesABC, platBroadwell},
	{"europe-west10", "10.214.0.0/20", zonesABC, platBroadwell},
	{"europe-west12", "10.210.0.0/20", zonesABC, platBroadwell},
	{"europe-west6", "10.172.0.0/20", zonesABC, platBroadwell},
	{"europe-west8", "10.198.0.0/20", zonesABC, platBroadwell},
	{"europe-west9", "10.200.0.0/20", zonesABC, platBroadwell},
	{"me-central1", "10.212.0.0/20", zonesABC, platBroadwell},
	{"me-central2", "10.216.0.0/20", zonesABC, platBroadwell},
	{"me-west1", "10.208.0.0/20", zonesABC, platBroadwell},
	{"northamerica-northeast1", "10.162.0.0/20", zonesABC, platBroadwell},
	{"northamerica-northeast2", "10.188.0.0/20", zonesABC, platBroadwell},
	{"northamerica-south1", "10.224.0.0/20", zonesABC, platBroadwell},
	{"southamerica-west1", "10.194.0.0/20", zonesABC, platBroadwell},
	{"us-east5", "10.202.0.0/20", zonesABC, platBroadwell},
	{"us-south1", "10.206.0.0/20", zonesABC, platBroadwell},
	{"us-west3", "10.180.0.0/20", zonesABC, platBroadwell},
	{"us-west4", "10.182.0.0/20", zonesABC, platBroadwell},
}

var cpuPlatforms = []string{ //nolint:gochecknoglobals // read-only lookup table
	platBroadwell,
	platCascade,
	"Intel Skylake",
	"AMD Rome",
}

func stableID(parts ...string) uint64 {
	h := fnv.New64a()

	for _, p := range parts {
		_, _ = h.Write([]byte(p + "\x00"))
	}

	return idFloor + h.Sum64()%idSpan
}

func findRegion(name string) (regionSpec, bool) {
	for _, r := range regionCatalog {
		if r.name == name {
			return r, true
		}
	}

	return regionSpec{}, false
}

func zoneNames(r regionSpec) []string {
	out := make([]string, 0, len(r.zones))
	for _, z := range r.zones {
		out = append(out, r.name+"-"+string(z))
	}

	return out
}

func allZones() []string {
	out := make([]string, 0, len(regionCatalog)*zonesPerRegionHint)
	for _, r := range regionCatalog {
		out = append(out, zoneNames(r)...)
	}

	sort.Strings(out)

	return out
}

func zoneExists(zone string) bool {
	r, ok := findRegion(regionOfZone(zone))
	if !ok {
		return false
	}

	return slices.Contains(zoneNames(r), zone)
}

func zoneResource(project, zone string) *computeapi.Zone {
	return &computeapi.Zone{
		Kind: "compute#zone", Id: stableID("zone", zone), CreationTimestamp: catalogCreated, Name: zone,
		Description: zone, Status: "UP", Region: scopeURL(project, scopeRegion, regionOfZone(zone)),
		SelfLink: scopeURL(project, scopeZone, zone), SupportsPzs: true,
		AvailableCpuPlatforms: cpuPlatforms,
	}
}

func regionResource(project string, r regionSpec) *computeapi.Region {
	zones := zoneNames(r)
	links := make([]string, len(zones))

	for i, z := range zones {
		links[i] = scopeURL(project, scopeZone, z)
	}

	return &computeapi.Region{
		Kind: "compute#region", Id: stableID("region", r.name), CreationTimestamp: catalogCreated, Name: r.name,
		Description: r.name, Status: "UP", SelfLink: scopeURL(project, scopeRegion, r.name), Zones: links,
		SupportsPzs: true,
	}
}

type machineSpec struct {
	name   string
	desc   string
	cpus   int64
	memMb  int64
	shared bool
	arm    bool
}

type machineFamily struct {
	totalGB  map[int64]float64
	family   string
	kind     string
	cpus     []int64
	gbPerCPU float64
	arm      bool
}

const (
	kindStandard = "standard"
	kindHighmem  = "highmem"
	kindHighcpu  = "highcpu"
	famN2D       = "n2d"
	gbN1High     = 6.5
	gbN1HighCPU  = 0.9
	gbHighCPU    = 2
	gbN2High128  = 864
	e2SharedCPUs = 2
	f1MicroMb    = 614
	g1SmallMb    = 1740
)

func family(name, kind string, gbPerCPU float64, cpus []int64) machineFamily {
	return machineFamily{family: name, kind: kind, cpus: cpus, gbPerCPU: gbPerCPU}
}

// machineFamilies are the predefined machine types of
// docs.cloud.google.com/compute/docs/general-purpose-machines: memory is vCPUs times gbPerCPU, except where the
// page lists a total (totalGB).
var machineFamilies = []machineFamily{ //nolint:gochecknoglobals // read-only catalogue
	family("e2", kindStandard, gbStd, []int64{2, 4, 8, 16, 32}),
	family("e2", kindHighmem, gbHigh, []int64{2, 4, 8, 16}),
	family("e2", kindHighcpu, 1, []int64{2, 4, 8, 16, 32}),
	family("n1", kindStandard, gbN1, []int64{1, 2, 4, 8, 16, 32, 64, 96}),
	family("n1", kindHighmem, gbN1High, []int64{2, 4, 8, 16, 32, 64, 96}),
	family("n1", kindHighcpu, gbN1HighCPU, []int64{2, 4, 8, 16, 32, 64, 96}),
	family("n2", kindStandard, gbStd, []int64{2, 4, 8, 16, 32, 48, 64, 80, 96, 128}),
	func() machineFamily {
		f := family("n2", kindHighmem, gbHigh, []int64{2, 4, 8, 16, 32, 48, 64, 80, 96, 128})
		f.totalGB = map[int64]float64{128: gbN2High128}

		return f
	}(),
	family("n2", kindHighcpu, 1, []int64{2, 4, 8, 16, 32, 48, 64, 80, 96}),
	family(famN2D, kindStandard, gbStd, []int64{2, 4, 8, 16, 32, 48, 64, 80, 96, 128, 224}),
	family(famN2D, kindHighmem, gbHigh, []int64{2, 4, 8, 16, 32, 48, 64, 80, 96}),
	family(famN2D, kindHighcpu, 1, []int64{2, 4, 8, 16, 32, 48, 64, 80, 96, 128, 224}),
	family("n4", kindStandard, gbStd, []int64{2, 4, 8, 16, 32, 48, 64, 80}),
	family("n4", kindHighmem, gbHigh, []int64{2, 4, 8, 16, 32, 48, 64, 80}),
	family("n4", kindHighcpu, gbHighCPU, []int64{2, 4, 8, 16, 32, 48, 64, 80}),
	family("c3", kindStandard, gbStd, []int64{4, 8, 22, 44, 88, 176}),
	family("c3", kindHighmem, gbHigh, []int64{4, 8, 22, 44, 88, 176}),
	family("c3", kindHighcpu, gbHighCPU, []int64{4, 8, 22, 44, 88, 176}),
	family("t2d", kindStandard, gbStd, []int64{1, 2, 4, 8, 16, 32, 48, 60}),
	func() machineFamily {
		f := family("t2a", kindStandard, gbStd, []int64{1, 2, 4, 8, 16, 32, 48})
		f.arm = true

		return f
	}(),
}

var sharedMachines = []machineSpec{ //nolint:gochecknoglobals // read-only catalogue
	{
		name: "e2-micro", desc: "Efficient Instance, 2 vCPU (1/4 shared physical core) and 1 GB RAM",
		cpus: e2SharedCPUs, memMb: 1 * gbToMb, shared: true,
	},
	{
		name: "e2-small", desc: "Efficient Instance, 2 vCPU (1/2 shared physical core) and 2 GB RAM",
		cpus: e2SharedCPUs, memMb: e2SharedCPUs * gbToMb, shared: true,
	},
	{
		name: "e2-medium", desc: "Efficient Instance, 2 vCPU (1/2 shared physical core) and 4 GB RAM",
		cpus: e2SharedCPUs, memMb: gbStd * gbToMb, shared: true,
	},
	{
		name: "f1-micro", desc: "Micro instance with 0.2 vCPU (shared physical core) and 0.6 GB RAM",
		cpus: 1, memMb: f1MicroMb, shared: true,
	},
	{
		name: "g1-small", desc: "Small instance with 0.5 vCPU (shared physical core) and 1.7 GB RAM",
		cpus: 1, memMb: g1SmallMb, shared: true,
	},
}

func buildMachineSpecs() []machineSpec {
	var out []machineSpec

	for _, f := range machineFamilies {
		for _, c := range f.cpus {
			gb := float64(c) * f.gbPerCPU
			if total, ok := f.totalGB[c]; ok {
				gb = total
			}

			out = append(out, machineSpec{
				name: fmt.Sprintf("%s-%s-%d", f.family, f.kind, c), cpus: c, memMb: int64(gb * gbToMb),
				desc: fmt.Sprintf("%d vCPUs %s GB RAM", c, trimFloat(gb)), arm: f.arm,
			})
		}
	}

	return append(out, sharedMachines...)
}

func trimFloat(f float64) string {
	if f == float64(int64(f)) {
		return strconv.FormatInt(int64(f), 10)
	}

	return fmt.Sprintf("%g", f)
}

var machineSpecs = buildMachineSpecs() //nolint:gochecknoglobals // read-only lookup table

func findMachine(name string) (machineSpec, bool) {
	for _, m := range machineSpecs {
		if m.name == name {
			return m, true
		}
	}

	return machineSpec{}, false
}

func machineArch(m machineSpec) string {
	if m.arm {
		return "ARM64"
	}

	return archX86
}

func machineResource(project, zone string, m machineSpec) *computeapi.MachineType {
	return &computeapi.MachineType{
		Kind: "compute#machineType", Id: stableID("machineType", zone, m.name), CreationTimestamp: catalogCreated,
		Name: m.name, Description: m.desc, GuestCpus: m.cpus, MemoryMb: m.memMb, ImageSpaceGb: 0,
		MaximumPersistentDisks: maxPDs, MaximumPersistentDisksSizeGb: maxPDSizeGb, IsSharedCpu: m.shared,
		Zone: zone, SelfLink: zoneLink(project, zone, "machineTypes", m.name), Architecture: machineArch(m),
	}
}

type diskTypeSpec struct {
	name, desc, valid string
	defaultGb         int64
}

var diskTypeCatalog = []diskTypeSpec{ //nolint:gochecknoglobals // read-only lookup table
	{"pd-balanced", "Balanced Persistent Disk", diskSizeRange, 100},
	{"pd-ssd", "SSD Persistent Disk", diskSizeRange, 100},
	{"pd-standard", "Standard Persistent Disk", diskSizeRange, 500},
}

func findDiskType(name string) (diskTypeSpec, bool) {
	for _, d := range diskTypeCatalog {
		if d.name == name {
			return d, true
		}
	}

	return diskTypeSpec{}, false
}

func diskTypeResource(project, zone string, d diskTypeSpec) *computeapi.DiskType {
	return &computeapi.DiskType{
		Kind: "compute#diskType", Id: stableID("diskType", zone, d.name), CreationTimestamp: catalogCreated,
		Name: d.name, Description: d.desc, ValidDiskSize: d.valid, Zone: scopeURL(project, scopeZone, zone),
		SelfLink: zoneLink(project, zone, "diskTypes", d.name), DefaultDiskSizeGb: d.defaultGb,
	}
}

func customMachine(name string) (int64, int64, bool) {
	var c, m int64
	if n, err := fmt.Sscanf(
		name,
		"custom-%d-%d",
		&c,
		&m,
	); err != nil || n != 2 ||
		name != fmt.Sprintf("custom-%d-%d", c, m) {
		return 0, 0, false
	}

	if c < 1 || (c != 1 && c%2 != 0) || m < gbToMb*c/2 || m%memStepMb != 0 {
		return 0, 0, false
	}

	return c, m, true
}
