package gcpsql

import (
	"regexp"
	"runtime"
	"strconv"
)

const (
	standardMiB      = 3840
	highmemMiB       = 6656
	perfOptimizedMiB = 8192
	nanoPerCPU       = int64(1_000_000_000)
)

var (
	customTierRe = regexp.MustCompile(`^db-custom-(\d+)-(\d+)$`)
	sizedTierRe  = regexp.MustCompile(`^db-(n1-standard|n1-highmem|perf-optimized-N)-(\d+)$`)
)

// tierShape maps a documented machine type to vCPUs and memory in MiB; ok is false for unknown tiers.
func tierShape(tier string) (int64, int64, bool) {
	switch tier {
	case "db-f1-micro":
		return 1, microMiB, true
	case "db-g1-small":
		return 1, smallMiB, true
	}

	if m := customTierRe.FindStringSubmatch(tier); m != nil {
		cpu, _ := strconv.ParseInt(m[1], 10, 64)
		mem, _ := strconv.ParseInt(m[2], 10, 64)

		return cpu, mem, cpu > 0 && mem > 0
	}

	m := sizedTierRe.FindStringSubmatch(tier)
	if m == nil {
		return 0, 0, false
	}

	n, _ := strconv.ParseInt(m[2], 10, 64)
	perVCPU := map[string]int64{
		"n1-standard": standardMiB, "n1-highmem": highmemMiB, "perf-optimized-N": perfOptimizedMiB,
	}[m[1]]

	return n, n * perVCPU, n > 0
}

// tierLimits turns a tier into container CPU and memory caps; the CPU cap never exceeds the host's cores.
func tierLimits(tier string) (int64, int64) {
	cpu, memMiB, ok := tierShape(tier)
	if !ok {
		return 0, 0
	}

	cpu = min(cpu, int64(runtime.NumCPU()))

	return cpu * nanoPerCPU, memMiB * mib
}
