package gcpcompute

import (
	"fmt"
	"hash/fnv"
	"sort"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	catalogCreated = "1969-12-31T16:00:00.000-08:00"
	maxPDs         = 128
	maxPDSizeGb    = 263168
	gbToMb         = 1024
)

type regionSpec struct {
	name      string
	autoRange string
	zones     string
	platform  string
}

// regionCatalog lists the supported regions with their documented auto-mode subnet ranges.
var regionCatalog = []regionSpec{
	{"asia-east1", "10.140.0.0/20", "abc", "Intel Broadwell"},
	{"asia-northeast1", "10.146.0.0/20", "abc", "Intel Broadwell"},
	{"asia-south1", "10.160.0.0/20", "abc", "Intel Broadwell"},
	{"asia-southeast1", "10.148.0.0/20", "abc", "Intel Broadwell"},
	{"australia-southeast1", "10.152.0.0/20", "abc", "Intel Broadwell"},
	{"europe-west1", "10.132.0.0/20", "bcd", "Intel Broadwell"},
	{"europe-west2", "10.154.0.0/20", "abc", "Intel Broadwell"},
	{"europe-west3", "10.156.0.0/20", "abc", "Intel Broadwell"},
	{"europe-west4", "10.164.0.0/20", "abc", "Intel Broadwell"},
	{"southamerica-east1", "10.158.0.0/20", "abc", "Intel Broadwell"},
	{"us-central1", "10.128.0.0/20", "abcf", "Intel Broadwell"},
	{"us-east1", "10.142.0.0/20", "bcd", "Intel Broadwell"},
	{"us-east4", "10.150.0.0/20", "abc", "Intel Broadwell"},
	{"us-west1", "10.138.0.0/20", "abc", "Intel Broadwell"},
	{"us-west2", "10.168.0.0/20", "abc", "Intel Broadwell"},
}

var cpuPlatforms = []string{"Intel Broadwell", "Intel Cascade Lake", "Intel Skylake", "AMD Rome"}

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
	var out []string
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

	for _, z := range zoneNames(r) {
		if z == zone {
			return true
		}
	}

	return false
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

	out = append(out,
		machineSpec{"e2-micro", "Efficient Instance, 2 vCPU (1/4 shared physical core) and 1 GB RAM", 2, 1 * gbToMb, true},
		machineSpec{"e2-small", "Efficient Instance, 2 vCPU (1/2 shared physical core) and 2 GB RAM", 2, 2 * gbToMb, true},
		machineSpec{"e2-medium", "Efficient Instance, 2 vCPU (1/2 shared physical core) and 4 GB RAM", 2, 4 * gbToMb, true},
	)
	add("e2", "standard", []int64{2, 4, 8, 16, 32}, 4)
	add("e2", "highmem", []int64{2, 4, 8, 16}, 8)
	add("e2", "highcpu", []int64{2, 4, 8, 16, 32}, 1)
	add("n2", "standard", []int64{2, 4, 8, 16, 32, 48, 64, 80, 96, 128}, 4)
	add("n2", "highmem", []int64{2, 4, 8, 16, 32, 48, 64, 80, 96, 128}, 8)
	add("n2", "highcpu", []int64{2, 4, 8, 16, 32, 48, 64, 80, 96}, 1)
	add("n1", "standard", []int64{1, 2, 4, 8, 16, 32, 64, 96}, 3.75)

	return out
}

func trimFloat(f float64) string {
	if f == float64(int64(f)) {
		return fmt.Sprintf("%d", int64(f))
	}

	return fmt.Sprintf("%g", f)
}

var machineSpecs = buildMachineSpecs()

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
		Zone: zone, SelfLink: zoneLink(project, zone, "machineTypes", m.name), Architecture: "X86_64",
	}
}

type diskTypeSpec struct {
	name, desc, valid string
	defaultGb         int64
}

var diskTypeCatalog = []diskTypeSpec{
	{"pd-balanced", "Balanced Persistent Disk", "10GB-65536GB", 100},
	{"pd-ssd", "SSD Persistent Disk", "10GB-65536GB", 100},
	{"pd-standard", "Standard Persistent Disk", "10GB-65536GB", 500},
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

func customMachine(name string) (cpus, memMb int64, ok bool) {
	var c, m int64
	if n, err := fmt.Sscanf(name, "custom-%d-%d", &c, &m); err != nil || n != 2 || name != fmt.Sprintf("custom-%d-%d", c, m) {
		return 0, 0, false
	}

	if c < 1 || (c != 1 && c%2 != 0) || m < gbToMb*c/2 || m%256 != 0 {
		return 0, 0, false
	}

	return c, m, true
}
