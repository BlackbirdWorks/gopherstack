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

type regionSpec struct {
	name      string
	autoRange string
	zones     string
	platform  string
}

// regionCatalog lists the supported regions with their documented auto-mode subnet ranges.
var regionCatalog = []regionSpec{ //nolint:gochecknoglobals // read-only lookup table
	{"asia-east1", "10.140.0.0/20", zonesABC, platBroadwell},
	{"asia-northeast1", "10.146.0.0/20", zonesABC, platBroadwell},
	{"asia-south1", "10.160.0.0/20", zonesABC, platBroadwell},
	{"asia-southeast1", "10.148.0.0/20", zonesABC, platBroadwell},
	{"australia-southeast1", "10.152.0.0/20", zonesABC, platBroadwell},
	{"europe-west1", "10.132.0.0/20", "bcd", platBroadwell},
	{"europe-west2", "10.154.0.0/20", zonesABC, platBroadwell},
	{"europe-west3", "10.156.0.0/20", zonesABC, platBroadwell},
	{"europe-west4", "10.164.0.0/20", zonesABC, platBroadwell},
	{"southamerica-east1", "10.158.0.0/20", zonesABC, platBroadwell},
	{"us-central1", "10.128.0.0/20", "abcf", platBroadwell},
	{"us-east1", "10.142.0.0/20", "bcd", platBroadwell},
	{"us-east4", "10.150.0.0/20", zonesABC, platBroadwell},
	{"us-west1", "10.138.0.0/20", zonesABC, platBroadwell},
	{"us-west2", "10.168.0.0/20", zonesABC, platBroadwell},
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
}

func buildMachineSpecs() []machineSpec {
	var out []machineSpec

	add := func(family, kind string, cpus []int64, gbPerCPU float64) {
		for _, c := range cpus {
			out = append(out, machineSpec{
				name: fmt.Sprintf("%s-%s-%d", family, kind, c), cpus: c, memMb: int64(float64(c) * gbPerCPU * gbToMb),
				desc: fmt.Sprintf("%d vCPUs %s GB RAM", c, trimFloat(float64(c)*gbPerCPU)),
			})
		}
	}

	out = append(
		out,
		machineSpec{
			"e2-micro",
			"Efficient Instance, 2 vCPU (1/4 shared physical core) and 1 GB RAM",
			2,
			1 * gbToMb,
			true,
		},
		machineSpec{
			"e2-small",
			"Efficient Instance, 2 vCPU (1/2 shared physical core) and 2 GB RAM",
			2,
			2 * gbToMb,
			true,
		},
		machineSpec{
			"e2-medium",
			"Efficient Instance, 2 vCPU (1/2 shared physical core) and 4 GB RAM",
			2,
			4 * gbToMb,
			true,
		},
	)
	add("e2", "standard", []int64{2, 4, 8, 16, 32}, gbStd)
	add("e2", "highmem", []int64{2, 4, 8, 16}, gbHigh)
	add("e2", "highcpu", []int64{2, 4, 8, 16, 32}, 1)
	add("n2", "standard", []int64{2, 4, 8, 16, 32, 48, 64, 80, 96, 128}, gbStd)
	add("n2", "highmem", []int64{2, 4, 8, 16, 32, 48, 64, 80, 96, 128}, gbHigh)
	add("n2", "highcpu", []int64{2, 4, 8, 16, 32, 48, 64, 80, 96}, 1)
	add("n1", "standard", []int64{1, 2, 4, 8, 16, 32, 64, 96}, gbN1)

	return out
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

func machineResource(project, zone string, m machineSpec) *computeapi.MachineType {
	return &computeapi.MachineType{
		Kind: "compute#machineType", Id: stableID("machineType", zone, m.name), CreationTimestamp: catalogCreated,
		Name: m.name, Description: m.desc, GuestCpus: m.cpus, MemoryMb: m.memMb, ImageSpaceGb: 0,
		MaximumPersistentDisks: maxPDs, MaximumPersistentDisksSizeGb: maxPDSizeGb, IsSharedCpu: m.shared,
		Zone: zone, SelfLink: zoneLink(project, zone, "machineTypes", m.name), Architecture: archX86,
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
