package ecs

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

const (
	maxPortRangesPerContainer = 100
	minPort                   = 1
	maxPort                   = 65535
)

type portRange struct{ start, end int }

func (r portRange) size() int { return r.end - r.start + 1 }

func (r portRange) String() string { return strconv.Itoa(r.start) + "-" + strconv.Itoa(r.end) }

// parsePortRange parses "first-last"; the first port must be below the last, both within 1-65535.
func parsePortRange(s string) (portRange, bool) {
	first, last, found := strings.Cut(s, "-")
	if !found {
		return portRange{}, false
	}

	start, err1 := strconv.Atoi(first)
	end, err2 := strconv.Atoi(last)

	if err1 != nil || err2 != nil || start < minPort || end > maxPort || start >= end {
		return portRange{}, false
	}

	return portRange{start: start, end: end}, true
}

// validatePortRanges enforces the containerPortRange rules in the PortMapping docs
// (ecs@v1.96.0 types.PortMapping.ContainerPortRange).
func validatePortRanges(def ContainerDefinition, networkMode string) error {
	var ranges []portRange

	for _, pm := range def.PortMappings {
		if pm.ContainerPortRange == "" {
			continue
		}

		if networkMode == networkModeHost || networkMode == networkModeNone {
			return fmt.Errorf(
				"%w: container %q: containerPortRange requires the bridge or awsvpc network mode", ErrClient, def.Name,
			)
		}

		r, ok := parsePortRange(pm.ContainerPortRange)
		if !ok {
			return fmt.Errorf(
				"%w: container %q: containerPortRange %q must be first-last within 1-65535 with first < last",
				ErrClient, def.Name, pm.ContainerPortRange,
			)
		}

		ranges = append(ranges, r)
	}

	if len(ranges) > maxPortRangesPerContainer {
		return fmt.Errorf(
			"%w: container %q: at most %d port ranges are allowed",
			ErrClient,
			def.Name,
			maxPortRangesPerContainer,
		)
	}

	return checkPortRangeOverlap(def, ranges)
}

func checkPortRangeOverlap(def ContainerDefinition, ranges []portRange) error {
	for i, a := range ranges {
		for _, b := range ranges[i+1:] {
			if a.start <= b.end && b.start <= a.end {
				return fmt.Errorf("%w: container %q: port ranges %s and %s overlap", ErrClient, def.Name, a, b)
			}
		}

		for _, pm := range def.PortMappings {
			if pm.ContainerPort >= a.start && pm.ContainerPort <= a.end {
				return fmt.Errorf(
					"%w: container %q: containerPort %d is already covered by port range %s",
					ErrClient, def.Name, pm.ContainerPort, a,
				)
			}
		}
	}

	return nil
}

// reserveHostPortRangeLocked reserves size contiguous free host ports from the ephemeral range on ci,
// returning the first. Must be called with the write lock held.
func reserveHostPortRangeLocked(ci *ContainerInstance, protocol string, size int) (portRange, bool) {
	lastStart := ephemeralPortRangeMax - size + 1
	if lastStart < ephemeralPortRangeMin {
		return portRange{}, false
	}

	span := big.NewInt(int64(lastStart - ephemeralPortRangeMin + 1))

	for range dynamicPortRandomAttempts {
		n, err := rand.Int(rand.Reader, span)
		if err != nil {
			break
		}

		if r, ok := tryReserveRangeLocked(ci, protocol, ephemeralPortRangeMin+int(n.Int64()), size); ok {
			return r, true
		}
	}

	for start := ephemeralPortRangeMin; start <= lastStart; start++ {
		if r, ok := tryReserveRangeLocked(ci, protocol, start, size); ok {
			return r, true
		}
	}

	return portRange{}, false
}

func tryReserveRangeLocked(ci *ContainerInstance, protocol string, start, size int) (portRange, bool) {
	for p := start; p < start+size; p++ {
		if ci.AllocatedPorts[hostPortKey(protocol, p)] {
			return portRange{}, false
		}
	}

	for p := start; p < start+size; p++ {
		ci.AllocatedPorts[hostPortKey(protocol, p)] = true
	}

	return portRange{start: start, end: start + size - 1}, true
}

func rangeHostPortKeys(protocol string, r portRange) []string {
	keys := make([]string, 0, r.size())
	for p := r.start; p <= r.end; p++ {
		keys = append(keys, hostPortKey(protocol, p))
	}

	return keys
}
