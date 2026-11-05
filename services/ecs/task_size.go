package ecs

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

const (
	mibPerGB = 1024

	minEphemeralStorageGiB = 21
	maxEphemeralStorageGiB = 200
)

// parseTaskCPU returns task-level cpu in CPU units; the task definition parameters accept "1024" and "1 vCPU".
func parseTaskCPU(s string) (int, bool) {
	s = strings.ToLower(strings.TrimSpace(s))

	if v, ok := strings.CutSuffix(s, "vcpus"); ok {
		return vcpuUnits(v)
	}

	if v, ok := strings.CutSuffix(s, "vcpu"); ok {
		return vcpuUnits(v)
	}

	n, err := strconv.Atoi(s)

	return n, err == nil && n > 0
}

func vcpuUnits(v string) (int, bool) {
	f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
	if err != nil || f <= 0 {
		return 0, false
	}

	return int(math.Round(f * cpuUnitsPerVCPU)), true
}

// parseTaskMemory returns task-level memory in MiB; the task definition parameters accept "2048" and "2 GB".
func parseTaskMemory(s string) (int, bool) {
	s = strings.ToLower(strings.TrimSpace(s))

	if v, ok := strings.CutSuffix(s, "gb"); ok {
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil || f <= 0 {
			return 0, false
		}

		return int(math.Round(f * mibPerGB)), true
	}

	n, err := strconv.Atoi(s)

	return n, err == nil && n > 0
}

// canonicalTaskSize returns s in its numeric form, or s unchanged when it does not parse.
func canonicalTaskSize(s string, parse func(string) (int, bool)) string {
	if n, ok := parse(s); ok {
		return strconv.Itoa(n)
	}

	return s
}

// validateEphemeralStorage enforces EphemeralStorage.SizeInGiB's documented 21 to 200 GiB range.
func validateEphemeralStorage(es *EphemeralStorage, kind error) error {
	if es == nil {
		return nil
	}

	if es.SizeInGiB < minEphemeralStorageGiB || es.SizeInGiB > maxEphemeralStorageGiB {
		return fmt.Errorf(
			"%w: ephemeralStorage.sizeInGiB %d is outside the supported range of %d to %d GiB",
			kind, es.SizeInGiB, minEphemeralStorageGiB, maxEphemeralStorageGiB,
		)
	}

	return nil
}

// validateContainerMemoryWithinTask rejects a container hard memory limit larger than the task's memory.
func validateContainerMemoryWithinTask(memory string, defs []ContainerDefinition, kind error) error {
	taskMiB, ok := parseTaskMemory(memory)
	if !ok {
		return nil
	}

	for _, cd := range defs {
		if cd.Memory > taskMiB {
			return fmt.Errorf(
				"%w: Invalid setting for container '%s'. The 'memory' setting for container is greater than for the task",
				kind, cd.Name,
			)
		}
	}

	return nil
}

// taskLimits returns the task-level CPU units and memory MiB, zero when unset.
func taskLimits(task *Task) (int, int) {
	cpu, _ := parseTaskCPU(task.CPU)
	mem, _ := parseTaskMemory(task.Memory)

	return cpu, mem
}
