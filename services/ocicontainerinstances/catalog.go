package ocicontainerinstances

import "slices"

type shapeDef struct {
	Name        string
	Processor   string
	MinOcpus    float32
	MaxOcpus    float32
	MaxMemory   float32
	DefPerOcpu  float32
	MaxPerOcpu  float32
	MaxBandwith float32
}

const (
	minMemoryGB    = 1
	minPerOcpuGB   = 1
	gbpsPerOcpu    = 1
	maxPerOcpuGB   = 64
	shapeE4        = "CI.Standard.E4.Flex"
	shapeE3        = "CI.Standard.E3.Flex"
	shapeA1        = "CI.Standard.A1.Flex"
	shapeE5        = "CI.Standard.E5.Flex"
	e4MaxMemory    = 1024
	a1MaxMemory    = 488
	e5MaxMemory    = 1504
	a1MaxOcpus     = 76
	e5MaxOcpus     = 94
	defaultMemE    = 16
	defaultMemA1   = 6
	defaultMemE5   = 12
	bandwidthLow   = 40
	bandwidthE5Max = 100
	e4MaxOcpus     = 64
)

// shapeCatalog: E4/A1/E5 limits are from the Oracle "Container Instance Shapes" page; E3 mirrors E4 (unverified).
func shapeCatalog() []shapeDef {
	return []shapeDef{
		{shapeA1, "3.0 GHz Ampere Altra", 1, a1MaxOcpus, a1MaxMemory, defaultMemA1, maxPerOcpuGB, bandwidthLow},
		{shapeE3, "2.25 GHz AMD EPYC 7742", 1, e4MaxOcpus, e4MaxMemory, defaultMemE, maxPerOcpuGB, bandwidthLow},
		{shapeE4, "2.55 GHz AMD EPYC 7J13", 1, e4MaxOcpus, e4MaxMemory, defaultMemE, maxPerOcpuGB, bandwidthLow},
		{shapeE5, "2.7 GHz AMD EPYC 9J14", 1, e5MaxOcpus, e5MaxMemory, defaultMemE5, maxPerOcpuGB, bandwidthE5Max},
	}
}

func lookupShape(name string) (shapeDef, bool) {
	i := slices.IndexFunc(shapeCatalog(), func(s shapeDef) bool { return s.Name == name })
	if i < 0 {
		return shapeDef{}, false
	}

	return shapeCatalog()[i], true
}

func (s shapeDef) bandwidth(ocpus float32) float32 { return min(ocpus*gbpsPerOcpu, s.MaxBandwith) }
