package gcprun

import (
	"math"
	"math/big"
	"strings"

	"cloud.google.com/go/run/apiv2/runpb"
)

const (
	limitCPU    = "cpu"
	limitMemory = "memory"
	nanoPerCPU  = 1_000_000_000
	milliPerCPU = 1000
)

const (
	kilo = 1000
	kibi = 1024
)

var memoryUnits = map[string]int64{ //nolint:gochecknoglobals // immutable unit table
	"K": kilo, "M": kilo * kilo, "G": kilo * kilo * kilo, "T": kilo * kilo * kilo * kilo,
	"Ki": kibi, "Mi": kibi * kibi, "Gi": kibi * kibi * kibi, "Ti": kibi * kibi * kibi * kibi,
}

// parseCPU turns a Cloud Run CPU limit ("1", "0.5", "1000m") into Docker NanoCPUs.
func parseCPU(s string) (int64, bool) {
	if base, ok := strings.CutSuffix(s, "m"); ok {
		return parseScaled(base, nanoPerCPU/milliPerCPU)
	}

	return parseScaled(s, nanoPerCPU)
}

func parseScaled(s string, mult int64) (int64, bool) {
	v, ok := new(big.Rat).SetString(s)
	if !ok || v.Sign() <= 0 {
		return 0, false
	}

	return ratToInt(v, mult)
}

// parseMemory turns a Cloud Run memory limit ("512Mi", "2G", "1073741824") into bytes.
func parseMemory(s string) (int64, bool) {
	digits := strings.TrimRight(s, "KMGTi")
	unit := s[len(digits):]
	mult := int64(1)

	if unit != "" {
		m, ok := memoryUnits[unit]
		if !ok {
			return 0, false
		}

		mult = m
	}

	return parseScaled(digits, mult)
}

func ratToInt(v *big.Rat, mult int64) (int64, bool) {
	v.Mul(v, new(big.Rat).SetInt64(mult))

	n := new(big.Int).Quo(v.Num(), v.Denom())
	if !n.IsInt64() || n.Int64() <= 0 || n.Int64() > math.MaxInt64/2 {
		return 0, false
	}

	return n.Int64(), true
}

// resourceLimits returns the memory bytes and NanoCPUs of a container's resources.limits; zero means unlimited.
func resourceLimits(c *runpb.Container) (int64, int64) {
	lim := c.GetResources().GetLimits()
	mem, _ := parseMemory(lim[limitMemory])
	cpu, _ := parseCPU(lim[limitCPU])

	return mem, cpu
}

func validateLimits(c *runpb.Container) error {
	for k, v := range c.GetResources().GetLimits() {
		var ok bool

		switch k {
		case limitCPU:
			_, ok = parseCPU(v)
		case limitMemory:
			_, ok = parseMemory(v)
		default:
			continue
		}

		if !ok {
			return errInvalid("Invalid %s limit %q.", k, v)
		}
	}

	return nil
}
