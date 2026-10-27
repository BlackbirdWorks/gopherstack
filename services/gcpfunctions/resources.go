package gcpfunctions

import (
	"strconv"
	"strings"

	"cloud.google.com/go/functions/apiv2/functionspb"
)

const (
	kiB = int64(1) << 10
	miB = kiB << 10
	giB = miB << 10

	minMemory   = 128 * miB
	maxMemory   = 32 * giB
	minCPU      = 0.08
	cpuTolerant = 0.0834
	cpuHalf     = 0.5
	defaultMem  = 256 * miB
)

// memoryBytes parses a ServiceConfig.available_memory quantity (k, M, G, T decimal; Ki, Mi, Gi, Ti binary).
func memoryBytes(q string) (int64, bool) {
	units := []struct {
		suffix string
		mult   float64
	}{
		{"Ki", float64(kiB)}, {"Mi", float64(miB)}, {"Gi", float64(giB)}, {"Ti", float64(giB) * 1024},
		{"k", 1e3}, {"M", 1e6}, {"G", 1e9}, {"T", 1e12},
	}

	mult := 1.0

	for _, u := range units {
		if num, ok := strings.CutSuffix(q, u.suffix); ok {
			q, mult = num, u.mult

			break
		}
	}

	v, err := strconv.ParseFloat(q, 64)
	if err != nil || v < 0 {
		return 0, false
	}

	return int64(v * mult), true
}

type cpuTier struct {
	cpu      float64
	minMem   int64
	maxMem   int64
	fraction bool
}

// cpuTiers are Cloud Run's documented memory ranges per CPU count, which 2nd gen functions share: up to 0.08 vCPU
// up to 512 MiB, up to 0.5 up to 1 GiB, up to 1 up to 4 GiB, 2 up to 8 GiB, 4 from 2 to 16 GiB, 6 from 4 to 24 GiB
// and 8 from 4 to 32 GiB. Fractional tiers cover every CPU count up to their value.
func cpuTiers() []cpuTier {
	return []cpuTier{
		{cpuTolerant, minMemory, 512 * miB, true},
		{cpuHalf, minMemory, giB, true},
		{1, minMemory, 4 * giB, true},
		{2, minMemory, 8 * giB, false},
		{4, 2 * giB, 16 * giB, false},
		{6, 4 * giB, 24 * giB, false},
		{8, 4 * giB, 32 * giB, false},
	}
}

func memoryRange(cpu float64) (int64, int64, bool) {
	if cpu < minCPU {
		return 0, 0, false
	}

	for _, t := range cpuTiers() {
		if (t.fraction && cpu <= t.cpu) || cpu == t.cpu {
			return t.minMem, t.maxMem, true
		}
	}

	return 0, 0, false
}

func validateResources(sc *functionspb.ServiceConfig) error {
	mem := int64(0)

	if q := sc.GetAvailableMemory(); q != "" {
		v, ok := memoryBytes(q)
		if !ok || v < minMemory || v > maxMemory {
			return errInvalid("service_config.available_memory %q must be between 128Mi and 32Gi.", q)
		}

		mem = v
	}

	q := sc.GetAvailableCpu()
	if q == "" {
		return nil
	}

	cpu, err := strconv.ParseFloat(q, 64)
	if err != nil {
		return errInvalid("service_config.available_cpu %q is not a number of CPUs (for example 1 or 0.5).", q)
	}

	lo, hi, ok := memoryRange(cpu)
	if !ok {
		return errInvalid(
			"service_config.available_cpu %q is not supported: use 0.08 to under 1, or 1, 2, 4, 6 or 8.",
			q,
		)
	}

	if mem == 0 {
		mem = defaultMem
	}

	if mem < lo || mem > hi {
		return errInvalid("service_config.available_cpu %q supports between %dMi and %dMi of available_memory.",
			q, lo/miB, hi/miB)
	}

	return nil
}

// defaultCPUFor derives the CPU count from the memory size, as the API documents for an unset available_cpu.
func defaultCPUFor(memory string) string {
	mem, ok := memoryBytes(memory)
	if !ok {
		mem = defaultMem
	}

	switch {
	case mem <= minMemory:
		return "0.083"
	case mem <= defaultMem:
		return "0.1666"
	case mem <= 512*miB:
		return "0.333"
	case mem <= giB:
		return "0.583"
	case mem <= 2*giB:
		return "1"
	case mem <= 8*giB:
		return "2"
	case mem <= 16*giB:
		return "4"
	}

	return "8"
}
