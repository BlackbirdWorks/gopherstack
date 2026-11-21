package gcpcompute

import (
	"slices"
	"strconv"
	"strings"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	collAccelTypes = "acceleratorTypes"
	gbPerMemGB     = 1024
)

type gpuLimit struct {
	maxVCPU  int64
	maxMemGB int64
}

type gpuModel struct {
	counts      map[int64]gpuLimit
	name        string
	desc        string
	zones       string
	noLocalSSD  string
	onlyLocalSS string
}

// gpuModels is the N1 GPU catalogue of docs.cloud.google.com/compute/docs/gpus (counts, vCPU and memory limits)
// and docs.cloud.google.com/compute/docs/gpus/gpu-regions-zones (zones, Local SSD notes). The accelerator type
// names are the Compute Engine ones and are not on those pages.
var gpuModels = []gpuModel{ //nolint:gochecknoglobals // read-only catalogue
	{
		name: "nvidia-tesla-t4", desc: "NVIDIA T4",
		counts: map[int64]gpuLimit{1: {48, 312}, 2: {48, 312}, 4: {96, 624}},
		zones: "asia-east1-a asia-east1-c asia-east2-a asia-east2-c asia-northeast1-a asia-northeast1-c " +
			"asia-northeast3-b asia-northeast3-c asia-south1-a asia-south1-b asia-southeast1-a asia-southeast1-b " +
			"asia-southeast1-c asia-southeast2-a asia-southeast2-b australia-southeast1-a australia-southeast1-c " +
			"europe-central2-b europe-central2-c europe-west1-b europe-west1-c europe-west1-d europe-west2-a " +
			"europe-west2-b europe-west3-b europe-west4-a europe-west4-b europe-west4-c me-west1-b me-west1-c " +
			"northamerica-northeast1-c southamerica-east1-a southamerica-east1-b southamerica-east1-c " +
			"us-central1-a us-central1-b us-central1-c us-central1-f us-east1-b us-east1-c us-east4-a us-east4-b " +
			"us-east4-c us-west1-a us-west1-b us-west2-b us-west2-c us-west3-b us-west4-a us-west4-b",
	},
	{
		name: "nvidia-tesla-p4", desc: "NVIDIA P4",
		counts: map[int64]gpuLimit{1: {24, 156}, 2: {48, 312}, 4: {96, 624}},
		zones: "asia-southeast1-a asia-southeast1-b australia-southeast1-a australia-southeast1-b europe-west4-b " +
			"europe-west4-c northamerica-northeast1-b northamerica-northeast1-c us-central1-a us-central1-c " +
			"us-east4-a us-east4-b us-east4-c us-west2-b us-west2-c",
		onlyLocalSS: "us-central1-c northamerica-northeast1-b",
	},
	{
		name: "nvidia-tesla-v100", desc: "NVIDIA V100",
		counts: map[int64]gpuLimit{1: {12, 78}, 2: {24, 156}, 4: {48, 312}, 8: {96, 624}},
		zones: "asia-east1-c europe-west4-a europe-west4-b europe-west4-c us-central1-a us-central1-b " +
			"us-central1-c us-central1-f us-east1-b us-west1-a us-west1-b",
		noLocalSSD: "us-east1-c",
	},
}

// legacyAcceleratorTypes are real accelerator types without a catalogue entry; they keep the earlier rules
// (any zone, N1 machine, TERMINATE) rather than becoming 404.
var legacyAcceleratorTypes = []string{ //nolint:gochecknoglobals // read-only lookup table
	"nvidia-tesla-t4-vws", "nvidia-tesla-p100", "nvidia-tesla-k80", "nvidia-tesla-a100", "nvidia-l4",
}

func findGPUModel(name string) (gpuModel, bool) {
	for _, m := range gpuModels {
		if m.name == name {
			return m, true
		}
	}

	return gpuModel{}, false
}

func (m gpuModel) inZone(zone string) bool {
	return zoneExists(zone) && slices.Contains(strings.Fields(m.zones), zone)
}

func (m gpuModel) maxCards() int64 {
	var most int64
	for c := range m.counts {
		most = max(most, c)
	}

	return most
}

func (m gpuModel) localSSDAllowed(zone string) bool {
	if m.onlyLocalSS != "" {
		return slices.Contains(strings.Fields(m.onlyLocalSS), zone)
	}

	return !slices.Contains(strings.Fields(m.noLocalSSD), zone)
}

func acceleratorTypeResource(project, zone string, m gpuModel) *computeapi.AcceleratorType {
	return &computeapi.AcceleratorType{
		Kind: "compute#acceleratorType", Id: stableID("acceleratorType", zone, m.name),
		CreationTimestamp: catalogCreated, Name: m.name, Description: m.desc,
		MaximumCardsPerInstance: m.maxCards(), Zone: scopeURL(project, scopeZone, zone),
		SelfLink: zoneLink(project, zone, collAccelTypes, m.name),
	}
}

func (b *InMemoryBackend) listAcceleratorTypes(project, scope, loc string) []entry {
	var out []entry

	for _, z := range allZones() {
		if scope == scopeZone && z != loc {
			continue
		}

		for _, m := range gpuModels {
			if m.inZone(z) {
				out = append(out, entry{scope: scopeZone + "/" + z, item: acceleratorTypeResource(project, z, m)})
			}
		}
	}

	return out
}

func getAcceleratorType(project, zone, name string) (*computeapi.AcceleratorType, error) {
	if !zoneExists(zone) {
		return nil, errNotFound(scopePath(project, scopeZone, zone))
	}

	if m, ok := findGPUModel(name); ok && m.inZone(zone) {
		return acceleratorTypeResource(project, zone, m), nil
	}

	return nil, errNotFound(relPath(project, scopeZone, zone, collAccelTypes, name))
}

// isN1Machine reports predefined N1 and custom machine types; unprefixed custom types are N1.
func isN1Machine(name string) bool {
	return strings.HasPrefix(name, "n1-") || strings.HasPrefix(name, "custom-")
}

func machineShape(machine string) (int64, int64) {
	name := lastSegment(machine)
	if m, ok := findMachine(name); ok {
		return m.cpus, m.memMb
	}

	c, mem, _ := customMachine(name)

	return c, mem
}

// validateAcceleratorsLocked checks guest accelerators against the catalogue and returns them with zonal URLs.
func (b *InMemoryBackend) validateAcceleratorsLocked(
	project, zone, machine string,
	sched *computeapi.Scheduling,
	disks []*computeapi.AttachedDisk,
	accels []*computeapi.AcceleratorConfig,
) ([]*computeapi.AcceleratorConfig, *APIError) {
	if len(accels) == 0 {
		return nil, nil
	}

	const field = "resource.guestAccelerators"

	if len(accels) > 1 {
		return nil, errField(field, "", "An instance can have only one type of accelerator.")
	}

	a := accels[0]

	if t := lastSegment(a.AcceleratorType); slices.Contains(legacyAcceleratorTypes, t) {
		return validateLegacyAccelerator(project, zone, machine, sched, t, a)
	}

	m, ok := findGPUModel(lastSegment(a.AcceleratorType))
	if !ok || !m.inZone(zone) {
		return nil, errNotFound(relPath(project, scopeZone, zone, collAccelTypes, lastSegment(a.AcceleratorType)))
	}

	limit, okCount := m.counts[a.AcceleratorCount]
	if !okCount {
		return nil, errField(field+".acceleratorCount", strconv.FormatInt(a.AcceleratorCount, 10),
			"The number of "+m.desc+" GPUs must be one of the supported counts, at most "+
				strconv.FormatInt(m.maxCards(), 10)+".")
	}

	name := lastSegment(machine)
	if !isN1Machine(name) {
		return nil, errInvalid("Invalid value for field '%s': GPUs can only be attached to N1 machine types, "+
			"not '%s'.", field, name)
	}

	cpus, memMb := machineShape(name)
	if cpus > limit.maxVCPU || memMb > limit.maxMemGB*gbPerMemGB {
		return nil, errInvalid("Invalid value for field '%s': %d %s GPUs support machine types with at most "+
			"%d vCPUs and %d GB of memory.", field, a.AcceleratorCount, m.desc, limit.maxVCPU, limit.maxMemGB)
	}

	if sched == nil || sched.OnHostMaintenance != maintTerminate {
		return nil, errInvalid("Invalid value for field 'resource.scheduling.onHostMaintenance': " +
			"instances with GPUs must use TERMINATE.")
	}

	for _, d := range disks {
		if d.Type == "SCRATCH" && !m.localSSDAllowed(zone) {
			return nil, errInvalid("Invalid value for field 'resource.disks': Local SSD is not supported with "+
				"%s GPUs in zone %s.", m.desc, zone)
		}
	}

	return []*computeapi.AcceleratorConfig{{
		AcceleratorType: zoneLink(project, zone, collAccelTypes, m.name), AcceleratorCount: a.AcceleratorCount,
	}}, nil
}

func validateLegacyAccelerator(
	project, zone, machine string,
	sched *computeapi.Scheduling,
	name string,
	a *computeapi.AcceleratorConfig,
) ([]*computeapi.AcceleratorConfig, *APIError) {
	if a.AcceleratorCount < 1 {
		return nil, errField("resource.guestAccelerators.acceleratorCount", "", "Must be at least 1.")
	}

	if m := lastSegment(machine); !isN1Machine(m) {
		return nil, errInvalid("Invalid value for field 'resource.guestAccelerators': GPUs cannot be attached to "+
			"machine type '%s'.", m)
	}

	if sched == nil || sched.OnHostMaintenance != maintTerminate {
		return nil, errInvalid("Invalid value for field 'resource.scheduling.onHostMaintenance': " +
			"instances with GPUs must use TERMINATE.")
	}

	return []*computeapi.AcceleratorConfig{{
		AcceleratorType: zoneLink(project, zone, collAccelTypes, name), AcceleratorCount: a.AcceleratorCount,
	}}, nil
}

func (h *Handler) registerAcceleratorOps() {
	h.scopedList(collAccelTypes, scopeZone, "compute#acceleratorTypeList", "compute#acceleratorTypeAggregatedList",
		collAccelTypes, zoneScopes(),
		func(b *InMemoryBackend, p, s, l string) []entry { return b.listAcceleratorTypes(p, s, l) })
	h.reg(collAccelTypes, scopeZone, verbGet, func(_ *Handler, rq *request) (any, error) {
		return getAcceleratorType(rq.rt.project, rq.rt.loc, rq.rt.name)
	})
}
