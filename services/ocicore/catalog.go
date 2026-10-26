package ocicore

import (
	"crypto/sha256"
	"encoding/hex"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	archX86      = "x86_64"
	archArm      = "aarch64"
	bytesGB      = 1024
	maxVnics     = 24
	procSkylake  = "2.0 GHz Intel Xeon Platinum 8167M"
	osOracle     = "Oracle Linux"
	osUbuntu     = "Canonical Ubuntu"
	listingNone  = "NONE"
	vcpusPerCore = 2
	smallVnics   = 2
	memPerOcpu   = 16
	armMemOcpu   = 6
)

type shapeDef struct {
	name       string
	processor  string
	arch       string
	ocpus      float64
	memGB      float64
	bandwidth  float64
	maxVnic    int
	minOcpus   float64
	maxOcpus   float64
	maxMemGB   float64
	maxBandGbs float64
	flexible   bool
	bareMetal  bool
}

//nolint:gochecknoglobals // immutable shape catalogue
var shapeDefs = []shapeDef{
	{"VM.Standard.E4.Flex", "2.55 GHz AMD EPYC 7J13 (Milan)", archX86, 0, 0, 0, 0, 1, 64, 1024, 40, true, false},
	{"VM.Standard.E5.Flex", "2.7 GHz AMD EPYC 9J14 (Genoa)", archX86, 0, 0, 0, 0, 1, 126, 1049, 40, true, false},
	{"VM.Standard.A1.Flex", "3.0 GHz Ampere Altra", archArm, 0, 0, 0, 0, 1, 76, 472, 40, true, false},
	{"VM.Standard3.Flex", "2.9 GHz Intel Xeon Platinum 8358", archX86, 0, 0, 0, 0, 1, 32, 512, 32, true, false},
	{"VM.Standard2.1", procSkylake, archX86, 1, 15, 1, 2, 0, 0, 0, 0, false, false},
	{"VM.Standard2.2", procSkylake, archX86, 2, 30, 2, 2, 0, 0, 0, 0, false, false},
	{"VM.Standard2.4", procSkylake, archX86, 4, 60, 4.1, 4, 0, 0, 0, 0, false, false},
	{"VM.Standard.E2.1.Micro", "2.0 GHz AMD EPYC 7551", archX86, 1, 1, 0.48, 1, 0, 0, 0, 0, false, false},
	{"BM.Standard.E4.128", "2.55 GHz AMD EPYC 7J13 (Milan)", archX86, 128, 2048, 100, 256, 0, 0, 0, 0, false, true},
}

func shapeByName(name string) (shapeDef, bool) {
	i := slices.IndexFunc(shapeDefs, func(s shapeDef) bool { return s.name == name })
	if i < 0 {
		return shapeDef{}, false
	}

	return shapeDefs[i], true
}

func flexMaxVnics(ocpus float64) int {
	if ocpus <= 1 {
		return smallVnics
	}

	return min(maxVnics, int(ocpus))
}

// ShapeConfigIn is the shapeConfig of LaunchInstance and UpdateInstance.
type ShapeConfigIn struct {
	Ocpus       *float64 `json:"ocpus"`
	MemoryInGBs *float64 `json:"memoryInGBs"`
	Vcpus       *int     `json:"vcpus"`
}

// resolve validates a requested shape configuration against the shape and returns the resolved one.
func (s shapeDef) resolve(in *ShapeConfigIn) (*InstanceShapeConfig, *APIError) {
	if !s.flexible {
		if in != nil && (in.Ocpus != nil || in.MemoryInGBs != nil || in.Vcpus != nil) {
			return nil, errParam("shapeConfig is only valid for flexible shapes, " + s.name + " is not flexible")
		}

		return &InstanceShapeConfig{
			Ocpus:                     s.ocpus,
			MemoryInGBs:               s.memGB,
			Vcpus:                     int(s.ocpus) * vcpusPerCore,
			NetworkingBandwidthInGbps: s.bandwidth,
			MaxVnicAttachments:        s.maxVnic,
			ProcessorDescription:      s.processor,
		}, nil
	}

	if in == nil || (in.Ocpus == nil && in.Vcpus == nil) {
		return nil, errMissing("shapeConfig.ocpus is required for the flexible shape " + s.name)
	}

	o := requestedOcpus(in)
	if o < s.minOcpus || o > s.maxOcpus || o != math.Trunc(o) {
		return nil, errParam("shapeConfig.ocpus must be between " + strconv.FormatFloat(s.minOcpus, 'f', -1, 64) +
			" and " + strconv.FormatFloat(s.maxOcpus, 'f', -1, 64) + " for " + s.name)
	}

	mem := cmpOrFloat(in.MemoryInGBs, o*s.defaultPerOcpu())

	if mem < max(1, o) || mem > min(s.maxMemGB, o*maxMemPerOcpu) {
		return nil, errParam(
			"shapeConfig.memoryInGBs is outside the range allowed for " + strconv.FormatFloat(o, 'f', -1, 64) +
				" OCPUs of " + s.name,
		)
	}

	return &InstanceShapeConfig{
		Ocpus: o, MemoryInGBs: mem, Vcpus: int(o) * vcpusPerCore, NetworkingBandwidthInGbps: min(o, s.maxBandGbs),
		MaxVnicAttachments: flexMaxVnics(o), ProcessorDescription: s.processor,
	}, nil
}

const maxMemPerOcpu = 64

func requestedOcpus(in *ShapeConfigIn) float64 {
	if in.Ocpus != nil {
		return *in.Ocpus
	}

	return float64(*in.Vcpus) / vcpusPerCore
}

func cmpOrFloat(p *float64, def float64) float64 {
	if p != nil {
		return *p
	}

	return def
}

func (s shapeDef) defaultPerOcpu() float64 {
	if s.arch == archArm {
		return armMemOcpu
	}

	return memPerOcpu
}

// Shape is one entry of ListShapes.
type Shape struct {
	OcpuOptions                *MinMax  `json:"ocpuOptions,omitempty"`
	MemoryOptions              *MemOpts `json:"memoryOptions,omitempty"`
	NetworkingBandwidthOptions *BwOpts  `json:"networkingBandwidthOptions,omitempty"`
	Shape                      string   `json:"shape"`
	ProcessorDescription       string   `json:"processorDescription"`
	BillingType                string   `json:"billingType"`
	ResizeCompatibleShapes     []string `json:"resizeCompatibleShapes,omitempty"`
	Ocpus                      float64  `json:"ocpus"`
	MemoryInGBs                float64  `json:"memoryInGBs"`
	NetworkingBandwidthInGbps  float64  `json:"networkingBandwidthInGbps"`
	MaxVnicAttachments         int      `json:"maxVnicAttachments"`
	IsFlexible                 bool     `json:"isFlexible"`
}

// MinMax is a numeric range.
type MinMax struct {
	Min float64 `json:"min"`
	Max float64 `json:"max"`
}

// MemOpts are the memory options of a flexible shape.
type MemOpts struct {
	MinInGBs            float64 `json:"minInGBs"`
	MaxInGBs            float64 `json:"maxInGBs"`
	DefaultPerOcpuInGBs float64 `json:"defaultPerOcpuInGBs"`
	MinPerOcpuInGBs     float64 `json:"minPerOcpuInGBs"`
	MaxPerOcpuInGBs     float64 `json:"maxPerOcpuInGBs"`
}

// BwOpts are the networking bandwidth options of a flexible shape.
type BwOpts struct {
	MinInGbps            float64 `json:"minInGbps"`
	MaxInGbps            float64 `json:"maxInGbps"`
	DefaultPerOcpuInGbps float64 `json:"defaultPerOcpuInGbps"`
}

func (s shapeDef) wire() Shape {
	out := Shape{
		Shape: s.name, ProcessorDescription: s.processor, IsFlexible: s.flexible, BillingType: "PAID",
		Ocpus: s.ocpus, MemoryInGBs: s.memGB, NetworkingBandwidthInGbps: s.bandwidth, MaxVnicAttachments: s.maxVnic,
	}

	if s.name == "VM.Standard.E2.1.Micro" {
		out.BillingType = "ALWAYS_FREE"
	}

	if s.flexible {
		out.Ocpus, out.MemoryInGBs = s.minOcpus, s.minOcpus*s.defaultPerOcpu()
		out.NetworkingBandwidthInGbps, out.MaxVnicAttachments = 1, smallVnics
		out.OcpuOptions = &MinMax{Min: s.minOcpus, Max: s.maxOcpus}
		out.MemoryOptions = &MemOpts{
			MinInGBs: 1, MaxInGBs: s.maxMemGB, DefaultPerOcpuInGBs: s.defaultPerOcpu(), MinPerOcpuInGBs: 1,
			MaxPerOcpuInGBs: maxMemPerOcpu,
		}
		out.NetworkingBandwidthOptions = &BwOpts{MinInGbps: 1, MaxInGbps: s.maxBandGbs, DefaultPerOcpuInGbps: 1}

		for _, o := range shapeDefs {
			if o.flexible && o.arch == s.arch && o.name != s.name {
				out.ResizeCompatibleShapes = append(out.ResizeCompatibleShapes, o.name)
			}
		}
	}

	return out
}

// ListShapes lists the compute shapes, optionally only those compatible with an image.
func (b *InMemoryBackend) ListShapes(
	compartment, imageID, name, pageToken string,
	limit int,
) (page.Page[Shape], *APIError) {
	b.mu.RLock("ListShapes")
	defer b.mu.RUnlock()

	if aerr := b.checkCompartment(compartment); aerr != nil {
		return page.Page[Shape]{}, aerr
	}

	arch := ""

	if imageID != "" {
		img, aerr := b.findImage(imageID)
		if aerr != nil {
			return page.Page[Shape]{}, aerr
		}

		arch = img.Internal.Arch
	}

	var out []Shape

	for _, s := range shapeDefs {
		if (name == "" || s.name == name) && (arch == "" || s.arch == arch) {
			out = append(out, s.wire())
		}
	}

	return page.New(out, pageToken, limit, defaultListLimit), nil
}

type platformImage struct {
	name, os, version, arch string
	sizeMB                  int64
}

//nolint:gochecknoglobals // immutable platform image catalogue
var platformImages = []platformImage{
	{"Oracle-Linux-8.10-2024.09.30-0", osOracle, "8", archX86, 47694},
	{"Oracle-Linux-8.10-aarch64-2024.09.30-0", osOracle, "8", archArm, 47694},
	{"Oracle-Linux-9.4-2024.09.30-0", osOracle, "9", archX86, 47694},
	{"Oracle-Linux-9.4-aarch64-2024.09.30-0", osOracle, "9", archArm, 47694},
	{"Canonical-Ubuntu-22.04-2024.09.30-0", osUbuntu, "22.04", archX86, 47694},
	{"Canonical-Ubuntu-22.04-aarch64-2024.09.30-0", osUbuntu, "22.04", archArm, 47694},
	{"Canonical-Ubuntu-24.04-2024.09.30-0", osUbuntu, "24.04", archX86, 47694},
	{"Canonical-Ubuntu-24.04-aarch64-2024.09.30-0", osUbuntu, "24.04", archArm, 47694},
	{"Windows-Server-2019-Standard-Edition-VM-2024.09.30-0", "Windows", "Server 2019 Standard", archX86, 51200},
	{"Windows-Server-2022-Standard-Edition-VM-2024.09.30-0", "Windows", "Server 2022 Standard", archX86, 51200},
}

func catalogID(kind, name string) string {
	sum := sha256.Sum256([]byte(kind + "/" + name))

	return "ocid1." + kind + ".oc1." + RegionKey + "." + hex.EncodeToString(sum[:])[:ocidTailLen]
}

func catalogCreated() time.Time { return time.Date(2024, time.September, 30, 0, 0, 0, 0, time.UTC) }

func catalogImages() []*Image {
	out := make([]*Image, 0, len(platformImages))

	for i, p := range platformImages {
		out = append(out, &Image{
			ID:              catalogID(kindImage, p.name),
			DisplayName:     p.name,
			LifecycleState:  statAvailable,
			TimeCreated:     catalogCreated(),
			FreeformTags:    map[string]string{},
			DefinedTags:     map[string]map[string]any{},
			Internal:        &Internal{Etag: catalogID("etag", p.name), Seq: int64(i), Arch: p.arch},
			OperatingSystem: p.os, OperatingSystemVersion: p.version, LaunchMode: "PARAVIRTUALIZED",
			ListingType: listingNone, SizeInMBs: p.sizeMB, BillableSizeInGBs: (p.sizeMB + bytesGB - 1) / bytesGB,
			CreateImageAllowed: true,
		})
	}

	return out
}

// findImage resolves a platform or custom image by OCID; callers hold the lock.
func (b *InMemoryBackend) findImage(id string) (*Image, *APIError) {
	if i := slices.IndexFunc(catalogImages(), func(m *Image) bool { return m.ID == id }); i >= 0 {
		return catalogImages()[i], nil
	}

	return get[*Image](b, id)
}

func bootVolumeGBs(sizeMB int64) int64 { return max(minVolumeGBs, (sizeMB+bytesGB-1)/bytesGB) }

// ImageListOpts are the ListImages filters.
type ImageListOpts struct {
	Compartment, Name, OS, OSVersion, Shape, State, SortBy, Order, Page string
	Limit                                                               int
}

func shapeArch(shape string) (string, *APIError) {
	if shape == "" {
		return "", nil
	}

	s, ok := shapeByName(shape)
	if !ok {
		return "", errParam("Unknown shape " + shape)
	}

	return s.arch, nil
}

func (o ImageListOpts) matches(m *Image, arch string) bool {
	return (o.Name == "" || m.DisplayName == o.Name) && (o.OS == "" || m.OperatingSystem == o.OS) &&
		(o.OSVersion == "" || m.OperatingSystemVersion == o.OSVersion) &&
		(arch == "" || m.Internal.Arch == arch) &&
		(o.State == "" || strings.EqualFold(m.LifecycleState, o.State))
}

// ListImages lists platform images plus the custom images of a compartment.
func (b *InMemoryBackend) ListImages(o ImageListOpts) (page.Page[resource], *APIError) {
	if aerr := validListOpts(ListOpts{SortBy: o.SortBy, Order: o.Order}); aerr != nil {
		return page.Page[resource]{}, aerr
	}

	b.mu.RLock("ListImages")
	defer b.mu.RUnlock()

	if aerr := b.checkCompartment(o.Compartment); aerr != nil {
		return page.Page[resource]{}, aerr
	}

	arch, aerr := shapeArch(o.Shape)
	if aerr != nil {
		return page.Page[resource]{}, aerr
	}

	all := catalogImages()

	for _, r := range b.res {
		if img, ok := r.(*Image); ok && img.CompartmentID == o.Compartment {
			all = append(all, img)
		}
	}

	all = slices.DeleteFunc(all, func(m *Image) bool { return !o.matches(m, arch) })
	sortRes(all, ListOpts{SortBy: o.SortBy, Order: o.Order})

	pg := page.New(all, o.Page, o.Limit, defaultListLimit)
	out := make([]resource, 0, len(pg.Data))

	for _, m := range pg.Data {
		out = append(out, m.view())
	}

	return page.Page[resource]{Data: out, Next: pg.Next}, nil
}

// GetImage returns a platform or custom image.
func (b *InMemoryBackend) GetImage(id string) (resource, string, *APIError) {
	b.mu.RLock("GetImage")
	defer b.mu.RUnlock()

	img, aerr := b.findImage(id)
	if aerr != nil {
		return nil, "", aerr
	}

	return img.view(), img.Internal.Etag, nil
}

func policySchedule(typ, period string, retention int) BackupSchedule {
	return BackupSchedule{
		BackupType: typ, Period: period, RetentionSeconds: retention, OffsetType: "STRUCTURED", HourOfDay: 0,
		DayOfMonth: 1, DayOfWeek: "MONDAY", Month: "JANUARY", TimeZone: "UTC",
	}
}

const (
	retWeek  = 7 * 24 * 3600
	retMonth = 4 * retWeek
	retYear  = 365 * 24 * 3600
	ret5Year = 5 * retYear
)

func predefinedPolicies() []*VolumeBackupPolicy {
	daily := policySchedule("INCREMENTAL", "ONE_DAY", retWeek)
	weekly := policySchedule("INCREMENTAL", "ONE_WEEK", retMonth)
	monthly := policySchedule("INCREMENTAL", "ONE_MONTH", retYear)
	yearly := policySchedule("FULL", "ONE_YEAR", ret5Year)
	defs := []struct {
		name string
		sch  []BackupSchedule
	}{
		{"bronze", []BackupSchedule{monthly, yearly}},
		{"silver", []BackupSchedule{weekly, monthly, yearly}},
		{"gold", []BackupSchedule{daily, weekly, monthly, yearly}},
	}
	out := make([]*VolumeBackupPolicy, 0, len(defs))

	for i, d := range defs {
		out = append(out, &VolumeBackupPolicy{
			ID:             catalogID(kindPolicy, d.name),
			DisplayName:    d.name,
			LifecycleState: statAvailable,
			TimeCreated:    catalogCreated(),
			FreeformTags:   map[string]string{},
			DefinedTags:    map[string]map[string]any{},
			Internal:       &Internal{Etag: catalogID("etag", d.name), Seq: int64(i)},
			Schedules:      d.sch,
		})
	}

	return out
}
