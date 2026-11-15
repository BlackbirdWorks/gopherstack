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
	osWindows    = "Windows"
	listingNone  = "NONE"
	vcpusPerCore = 2
	smallVnics   = 2
	memPerOcpu   = 16
	armMemOcpu   = 6
)

type shapeDef struct {
	name         string
	processor    string
	arch         string
	ocpuSteps    []float64
	ocpus        float64
	memGB        float64
	bandwidth    float64
	maxVnic      int
	minOcpus     float64
	maxOcpus     float64
	maxMemGB     float64
	maxBandGbs   float64
	bandPerOcpu  float64
	stepMemPerOc float64
	flexible     bool
	bareMetal    bool
}

func fixedVM(name, proc string, ocpus, mem, band float64, vnics int) shapeDef {
	return shapeDef{
		name: name, processor: proc, arch: archX86, ocpus: ocpus, memGB: mem, bandwidth: band, maxVnic: vnics,
	}
}

func fixedBM(name, proc string, ocpus, mem, band float64, vnics int) shapeDef {
	s := fixedVM(name, proc, ocpus, mem, band, vnics)
	s.bareMetal = true

	return s
}

func armBM(name, proc string, ocpus, mem, band float64) shapeDef {
	s := fixedBM(name, proc, ocpus, mem, band, bmVnics)
	s.arch = archArm

	return s
}

func flexVM(name, proc, arch string, maxO, maxMem, maxBand float64) shapeDef {
	return shapeDef{
		name: name, processor: proc, arch: arch, minOcpus: 1, maxOcpus: maxO, maxMemGB: maxMem, maxBandGbs: maxBand,
		flexible: true, bandPerOcpu: 1,
	}
}

func withBand(s shapeDef, perOcpu float64) shapeDef {
	s.bandPerOcpu = perOcpu

	return s
}

func denseFlex(name, proc string, memPerOcpu float64, steps ...float64) shapeDef {
	return shapeDef{
		name: name, processor: proc, arch: archX86, ocpuSteps: steps, stepMemPerOc: memPerOcpu, flexible: true,
		minOcpus: steps[0], maxOcpus: steps[len(steps)-1], maxMemGB: steps[len(steps)-1] * memPerOcpu,
		maxBandGbs: steps[len(steps)-1], bandPerOcpu: 1,
	}
}

const (
	procMilan   = "2.55 GHz AMD EPYC 7J13 (Milan)"
	procRome    = "2.25 GHz AMD EPYC 7742 (Rome)"
	procGenoa   = "2.4 GHz AMD EPYC 9J14 (Genoa)"
	procTurin   = "2.7 GHz AMD EPYC 9J45 (Turin)"
	procIcelake = "2.6 GHz Intel Xeon Platinum 8358 (Ice Lake)"
	procIcel6   = "3.0 GHz Intel Xeon 6354 (Ice Lake)"
	procSapph   = "2.0 GHz Intel Sapphire Rapids 8480+"
	procAmpere  = "3.0 GHz Ampere Altra"
	procEpyc7   = "2.0 GHz AMD EPYC 7551"
)

type shapeRow struct {
	name       string
	proc       string
	arch       string
	steps      []float64
	ocpus      float64
	mem        float64
	band       float64
	maxO       float64
	maxMem     float64
	maxBand    float64
	memPerOcpu float64
	vnics      int
	kind       int
}

const (
	kindFixedVM = iota
	kindFixedBM
	kindArmBM
	kindFlex
	kindFlexBand
	kindDense
	bmVnics         = 256
	bandPerOcpuHigh = 4
)

func (r shapeRow) def() shapeDef {
	switch r.kind {
	case kindFixedVM:
		return fixedVM(r.name, r.proc, r.ocpus, r.mem, r.band, r.vnics)
	case kindFixedBM:
		return fixedBM(r.name, r.proc, r.ocpus, r.mem, r.band, r.vnics)
	case kindArmBM:
		return armBM(r.name, r.proc, r.ocpus, r.mem, r.band)
	case kindFlexBand:
		return withBand(flexVM(r.name, r.proc, r.arch, r.maxO, r.maxMem, r.maxBand), bandPerOcpuHigh)
	case kindDense:
		return denseFlex(r.name, r.proc, r.memPerOcpu, r.steps...)
	}

	return flexVM(r.name, r.proc, r.arch, r.maxO, r.maxMem, r.maxBand)
}

func shapesFrom(rows []shapeRow) []shapeDef {
	out := make([]shapeDef, 0, len(rows))
	for _, r := range rows {
		out = append(out, r.def())
	}

	return out
}

// shapeDefs follows the compute shapes documentation (Compute/References/computeshapes.htm).
//
//nolint:gochecknoglobals // immutable shape catalogue
var shapeDefs = shapesFrom([]shapeRow{
	{"VM.Standard.E4.Flex", procMilan, archX86, nil, 0, 0, 0, 64, 1024, 40, 0, 0, kindFlex},
	{"VM.Standard.E5.Flex", "2.7 GHz AMD EPYC 9J14 (Genoa)", archX86, nil, 0, 0, 0, 126, 1049, 40, 0, 0, kindFlex},
	{"VM.Standard.E6.Flex", procTurin, archX86, nil, 0, 0, 0, 126, 1454, 99, 0, 0, kindFlex},
	{"VM.Standard.E6.Ax.Flex", procTurin, archX86, nil, 0, 0, 0, 94, 712, 99, 0, 0, kindFlex},
	{"VM.Standard.E3.Flex", procRome, archX86, nil, 0, 0, 0, 64, 1024, 40, 0, 0, kindFlex},
	{"VM.Standard.A1.Flex", procAmpere, archArm, nil, 0, 0, 0, 76, 472, 40, 0, 0, kindFlex},
	{"VM.Standard.A2.Flex", "3.0 GHz AmpereOne A160-30", archArm, nil, 0, 0, 0, 78, 946, 78, 0, 0, kindFlex},
	{"VM.Standard.A4.Flex", "3.6 GHz AmpereOne M A06-36M", archArm, nil, 0, 0, 0, 45, 700, 100, 0, 0, kindFlex},
	{"VM.Standard.A4.Ax.Flex", "3.6 GHz AmpereOne M 192-36M", archArm, nil, 0, 0, 0, 45, 720, 100, 0, 0, kindFlex},
	{"VM.Standard3.Flex", procIcelake, archX86, nil, 0, 0, 0, 32, 512, 32, 0, 0, kindFlex},
	{"VM.Standard4.Ax.Flex", "2.2 GHz Intel Xeon 6987P-C", archX86, nil, 0, 0, 0, 39, 360, 99, 0, 0, kindFlex},
	{"VM.Optimized3.Flex", procIcel6, archX86, nil, 0, 0, 0, 18, 256, 40, 0, 0, kindFlexBand},
	{"VM.DenseIO.E4.Flex", procMilan, "", []float64{8, 16, 32}, 0, 0, 0, 0, 0, 0, 16, 0, kindDense},
	{
		"VM.DenseIO.E5.Flex",
		"2.6 GHz AMD EPYC 9J14 (Genoa)",
		"",
		[]float64{8, 16, 24, 32, 40, 48},
		0,
		0,
		0,
		0,
		0,
		0,
		12,
		0,
		kindDense,
	},
	{"VM.DenseIO.E6.Ax.Flex", procTurin, "", []float64{8, 16, 24, 32, 40, 48}, 0, 0, 0, 0, 0, 0, 12, 0, kindDense},
	{"VM.Standard2.1", procSkylake, "", nil, 1, 15, 1, 0, 0, 0, 0, 2, kindFixedVM},
	{"VM.Standard2.2", procSkylake, "", nil, 2, 30, 2, 0, 0, 0, 0, 2, kindFixedVM},
	{"VM.Standard2.4", procSkylake, "", nil, 4, 60, 4.1, 0, 0, 0, 0, 4, kindFixedVM},
	{"VM.Standard2.8", procSkylake, "", nil, 8, 120, 8.2, 0, 0, 0, 0, 8, kindFixedVM},
	{"VM.Standard2.16", procSkylake, "", nil, 16, 240, 16.4, 0, 0, 0, 0, 16, kindFixedVM},
	{"VM.Standard2.24", procSkylake, "", nil, 24, 320, 24.6, 0, 0, 0, 0, 24, kindFixedVM},
	{"VM.Standard.E2.1", procEpyc7, "", nil, 1, 8, 0.7, 0, 0, 0, 0, 2, kindFixedVM},
	{"VM.Standard.E2.2", procEpyc7, "", nil, 2, 16, 1.4, 0, 0, 0, 0, 2, kindFixedVM},
	{"VM.Standard.E2.4", procEpyc7, "", nil, 4, 32, 2.8, 0, 0, 0, 0, 4, kindFixedVM},
	{"VM.Standard.E2.8", procEpyc7, "", nil, 8, 64, 5.6, 0, 0, 0, 0, 4, kindFixedVM},
	{"VM.Standard.E2.1.Micro", procEpyc7, "", nil, 1, 1, 0.48, 0, 0, 0, 0, 1, kindFixedVM},
	{"VM.DenseIO2.8", procSkylake, "", nil, 8, 120, 8.2, 0, 0, 0, 0, 8, kindFixedVM},
	{"VM.DenseIO2.16", procSkylake, "", nil, 16, 240, 16.4, 0, 0, 0, 0, 16, kindFixedVM},
	{"VM.DenseIO2.24", procSkylake, "", nil, 24, 320, 24.6, 0, 0, 0, 0, 24, kindFixedVM},
	{"VM.GPU2.1", procSkylake, "", nil, 12, 72, 8, 0, 0, 0, 0, 12, kindFixedVM},
	{"VM.GPU3.1", procSkylake, "", nil, 6, 90, 4, 0, 0, 0, 0, 6, kindFixedVM},
	{"VM.GPU3.2", procSkylake, "", nil, 12, 180, 8, 0, 0, 0, 0, 12, kindFixedVM},
	{"VM.GPU3.4", procSkylake, "", nil, 24, 360, 24.6, 0, 0, 0, 0, 24, kindFixedVM},
	{"VM.GPU.A10.1", procIcelake, "", nil, 15, 240, 24, 0, 0, 0, 0, 15, kindFixedVM},
	{"VM.GPU.A10.2", procIcelake, "", nil, 30, 480, 48, 0, 0, 0, 0, 24, kindFixedVM},
	{"BM.Standard3.64", procIcelake, "", nil, 64, 1024, 100, 0, 0, 0, 0, 256, kindFixedBM},
	{"BM.Standard4.Ax.120", "2.2 GHz Intel Xeon 6987P-C", "", nil, 120, 1152, 200, 0, 0, 0, 0, 256, kindFixedBM},
	{"BM.Standard.E4.128", procMilan, "", nil, 128, 2048, 100, 0, 0, 0, 0, 256, kindFixedBM},
	{"BM.Standard.E5.192", procGenoa, "", nil, 192, 2304, 100, 0, 0, 0, 0, 256, kindFixedBM},
	{"BM.Standard.E6.256", procTurin, "", nil, 256, 3072, 200, 0, 0, 0, 0, 256, kindFixedBM},
	{"BM.Standard.E6.Ax.192", procTurin, "", nil, 192, 1536, 200, 0, 0, 0, 0, 256, kindFixedBM},
	{"BM.Standard.E3.128", procRome, "", nil, 128, 2048, 100, 0, 0, 0, 0, 256, kindFixedBM},
	{"BM.Standard.E2.64", procEpyc7, "", nil, 64, 512, 50, 0, 0, 0, 0, 150, kindFixedBM},
	{"BM.Standard2.52", procSkylake, "", nil, 52, 768, 50, 0, 0, 0, 0, 200, kindFixedBM},
	{"BM.Standard1.36", "2.3 GHz Intel Xeon E5-2699 v3", "", nil, 36, 256, 10, 0, 0, 0, 0, 100, kindFixedBM},
	{"BM.Standard.B1.44", "2.2 GHz Intel Xeon E5-2699 v4", "", nil, 44, 512, 25, 0, 0, 0, 0, 44, kindFixedBM},
	{"BM.DenseIO.E4.128", procMilan, "", nil, 128, 2048, 100, 0, 0, 0, 0, 256, kindFixedBM},
	{"BM.DenseIO.E5.128", "2.6 GHz AMD EPYC 9J14 (Genoa)", "", nil, 128, 1536, 100, 0, 0, 0, 0, 256, kindFixedBM},
	{"BM.DenseIO.E6.Ax.192", procTurin, "", nil, 192, 2304, 200, 0, 0, 0, 0, 256, kindFixedBM},
	{"BM.DenseIO2.52", procSkylake, "", nil, 52, 768, 50, 0, 0, 0, 0, 52, kindFixedBM},
	{"BM.DenseIO1.36", "2.3 GHz Intel Xeon E5-2699 v3", "", nil, 36, 512, 10, 0, 0, 0, 0, 36, kindFixedBM},
	{"BM.Optimized3.36", procIcel6, "", nil, 36, 512, 100, 0, 0, 0, 0, 256, kindFixedBM},
	{"BM.HPC.E5.144", procGenoa, "", nil, 144, 768, 100, 0, 0, 0, 0, 256, kindFixedBM},
	{"BM.HPC2.36", "3.0 GHz Intel Xeon Gold 6154", "", nil, 36, 384, 25, 0, 0, 0, 0, 50, kindFixedBM},
	{"BM.GPU2.2", procSkylake, "", nil, 28, 192, 50, 0, 0, 0, 0, 28, kindFixedBM},
	{"BM.GPU3.8", procSkylake, "", nil, 52, 768, 50, 0, 0, 0, 0, 52, kindFixedBM},
	{"BM.GPU4.8", "2.9 GHz AMD EPYC 7542 (Rome)", "", nil, 64, 2048, 50, 0, 0, 0, 0, 64, kindFixedBM},
	{"BM.GPU.A10.4", procIcelake, "", nil, 64, 1024, 100, 0, 0, 0, 0, 256, kindFixedBM},
	{"BM.GPU.A100-v2.8", procMilan, "", nil, 128, 2048, 100, 0, 0, 0, 0, 256, kindFixedBM},
	{"BM.GPU.L40S.4", procSapph, "", nil, 112, 1024, 200, 0, 0, 0, 0, 256, kindFixedBM},
	{"BM.GPU.H100.8", procSapph, "", nil, 112, 2048, 100, 0, 0, 0, 0, 256, kindFixedBM},
	{"BM.GPU.H200.8", procSapph, "", nil, 112, 3072, 200, 0, 0, 0, 0, 256, kindFixedBM},
	{"BM.GPU.MI300X.8", procSapph, "", nil, 112, 2048, 100, 0, 0, 0, 0, 256, kindFixedBM},
	{"BM.GPU.B200.8", "Intel Emerald Rapids 8592+", "", nil, 128, 4096, 400, 0, 0, 0, 0, 256, kindFixedBM},
	{"BM.GPU.MI355X.8", "2 x 64-core AMD 5th Gen EPYC", "", nil, 128, 3072, 400, 0, 0, 0, 0, 256, kindFixedBM},
	{"BM.GPU.RTXPRO.8", "2 x 72-core Intel Xeon 6", "", nil, 144, 3072, 400, 0, 0, 0, 0, 256, kindFixedBM},
	{"BM.GPU.B300.8", "Intel Emerald Rapids 8592+", "", nil, 128, 4096, 400, 0, 0, 0, 0, 256, kindFixedBM},
	{"BM.GPU.GB200.4", "2 x 72-core NVIDIA Grace", "", nil, 144, 960, 400, 0, 0, 0, 0, 512, kindFixedBM},
	{"BM.GPU.GB300.4", "2 x 72-core NVIDIA Grace", "", nil, 144, 960, 400, 0, 0, 0, 0, 512, kindFixedBM},
	{"BM.Standard.A1.160", procAmpere, "", nil, 160, 1024, 100, 0, 0, 0, 0, 0, kindArmBM},
	{"BM.Standard.A4.48", "3.6 GHz AmpereOne M A06-36M", "", nil, 48, 768, 100, 0, 0, 0, 0, 0, kindArmBM},
	{"BM.Standard.A4.Ax.48", "3.6 GHz AmpereOne M 192-36M", "", nil, 48, 768, 100, 0, 0, 0, 0, 0, kindArmBM},
})

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

	if len(s.ocpuSteps) > 0 {
		return s.resolveSteps(in)
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
		Ocpus:                     o,
		MemoryInGBs:               mem,
		Vcpus:                     int(o) * vcpusPerCore,
		NetworkingBandwidthInGbps: min(o*s.bandPerOcpu, s.maxBandGbs),
		MaxVnicAttachments:        flexMaxVnics(o),
		ProcessorDescription:      s.processor,
	}, nil
}

const maxMemPerOcpu = 64

// resolveSteps handles the dense I/O flex shapes, which offer fixed OCPU counts with a fixed memory ratio.
func (s shapeDef) resolveSteps(in *ShapeConfigIn) (*InstanceShapeConfig, *APIError) {
	o := requestedOcpus(in)
	if !slices.Contains(s.ocpuSteps, o) {
		return nil, errParam("shapeConfig.ocpus must be one of the sizes offered by " + s.name)
	}

	mem := o * s.stepMemPerOc
	if in.MemoryInGBs != nil && *in.MemoryInGBs != mem {
		return nil, errParam("shapeConfig.memoryInGBs is fixed at " + strconv.FormatFloat(mem, 'f', -1, 64) +
			" GB for " + strconv.FormatFloat(o, 'f', -1, 64) + " OCPUs of " + s.name)
	}

	return &InstanceShapeConfig{
		Ocpus: o, MemoryInGBs: mem, Vcpus: int(o) * vcpusPerCore, NetworkingBandwidthInGbps: o,
		MaxVnicAttachments: min(maxVnics, int(o)), ProcessorDescription: s.processor,
	}, nil
}

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
		if len(s.ocpuSteps) > 0 {
			out.NetworkingBandwidthInGbps, out.MaxVnicAttachments = s.minOcpus, int(s.minOcpus)
			out.MemoryOptions = &MemOpts{
				MinInGBs: s.minOcpus * s.stepMemPerOc, MaxInGBs: s.maxMemGB, DefaultPerOcpuInGBs: s.stepMemPerOc,
				MinPerOcpuInGBs: s.stepMemPerOc, MaxPerOcpuInGBs: s.stepMemPerOc,
			}
		}

		out.NetworkingBandwidthOptions = &BwOpts{
			MinInGbps:            1,
			MaxInGbps:            s.maxBandGbs,
			DefaultPerOcpuInGbps: s.bandPerOcpu,
		}

		for _, o := range shapeDefs {
			if o.flexible && o.arch == s.arch && o.name != s.name && len(o.ocpuSteps) == 0 && len(s.ocpuSteps) == 0 {
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
	{"Windows-Server-2019-Standard-Edition-VM-2024.09.30-0", osWindows, "Server 2019 Standard", archX86, 51200},
	{"Windows-Server-2022-Standard-Edition-VM-2024.09.30-0", osWindows, "Server 2022 Standard", archX86, 51200},
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
func (b *InMemoryBackend) ListImages(o ImageListOpts) (page.Page[Resource], *APIError) {
	if aerr := validListOpts(ListOpts{SortBy: o.SortBy, Order: o.Order}); aerr != nil {
		return page.Page[Resource]{}, aerr
	}

	b.mu.RLock("ListImages")
	defer b.mu.RUnlock()

	if aerr := b.checkCompartment(o.Compartment); aerr != nil {
		return page.Page[Resource]{}, aerr
	}

	arch, aerr := shapeArch(o.Shape)
	if aerr != nil {
		return page.Page[Resource]{}, aerr
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
	out := make([]Resource, 0, len(pg.Data))

	for _, m := range pg.Data {
		out = append(out, m.view())
	}

	return page.Page[Resource]{Data: out, Next: pg.Next}, nil
}

// GetImage returns a platform or custom image.
func (b *InMemoryBackend) GetImage(id string) (Resource, string, *APIError) {
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
