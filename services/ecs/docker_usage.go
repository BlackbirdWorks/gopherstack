package ecs

import (
	"context"
	"encoding/json"
)

const cpuUnitsPerVCPU = 1024.0

type dockerCPUStats struct {
	CPUUsage struct {
		TotalUsage uint64 `json:"total_usage"`
	} `json:"cpu_usage"`
	SystemCPUUsage uint64 `json:"system_cpu_usage"`
	OnlineCPUs     uint32 `json:"online_cpus"`
}

type dockerStatsSample struct {
	MemoryStats struct {
		Stats map[string]uint64 `json:"stats"`
		Usage uint64            `json:"usage"`
	} `json:"memory_stats"`
	CPUStats    dockerCPUStats `json:"cpu_stats"`
	PreCPUStats dockerCPUStats `json:"precpu_stats"`
}

// usage returns the sample's CPU in vCPU units (1024 per vCPU) and working-set memory bytes.
func (s dockerStatsSample) usage() (float64, float64) {
	cpuDelta := float64(s.CPUStats.CPUUsage.TotalUsage) - float64(s.PreCPUStats.CPUUsage.TotalUsage)
	sysDelta := float64(s.CPUStats.SystemCPUUsage) - float64(s.PreCPUStats.SystemCPUUsage)

	var cpuUnits, memoryBytes float64

	if cpuDelta > 0 && sysDelta > 0 {
		cpus := float64(s.CPUStats.OnlineCPUs)
		if cpus == 0 {
			cpus = 1
		}

		cpuUnits = cpuDelta / sysDelta * cpus * cpuUnitsPerVCPU
	}

	cache := s.MemoryStats.Stats["inactive_file"]
	if cache == 0 {
		cache = s.MemoryStats.Stats["cache"]
	}

	if s.MemoryStats.Usage > cache {
		memoryBytes = float64(s.MemoryStats.Usage - cache)
	}

	return cpuUnits, memoryBytes
}

// TaskUsage sums the CPU and memory in use across the task's containers.
func (r *realDockerRunner) TaskUsage(ctx context.Context, taskArn string) (float64, float64, bool) {
	var cpuUnits, memoryBytes float64

	sampled := false

	for _, id := range r.ContainerIDs(taskArn) {
		body, err := r.cli.ContainerStats(ctx, id)
		if err != nil {
			continue
		}

		var sample dockerStatsSample

		decodeErr := json.NewDecoder(body).Decode(&sample)
		_ = body.Close()

		if decodeErr != nil {
			continue
		}

		c, m := sample.usage()
		cpuUnits += c
		memoryBytes += m
		sampled = true
	}

	return cpuUnits, memoryBytes, sampled
}
