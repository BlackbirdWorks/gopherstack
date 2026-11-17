package container

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"time"

	dockercontainer "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/container"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

// ErrStatsUnsupported is returned when the runtime client cannot report container stats.
var ErrStatsUnsupported = errors.New("container stats are not supported by this runtime client")

// StatsAPIClient is implemented by clients that can sample a container's resource usage.
type StatsAPIClient interface {
	ContainerStats(ctx context.Context, containerID string) (io.ReadCloser, error)
}

func (r *realDockerClient) ContainerStats(ctx context.Context, containerID string) (io.ReadCloser, error) {
	return r.c.ContainerStats(ctx, containerID)
}

// Stats is one sample of a container's resource usage.
type Stats struct {
	// CPUTimeNanos is the cumulative CPU time the container has consumed.
	CPUTimeNanos uint64
	// MemoryBytes is the memory in use, excluding reclaimable page cache.
	MemoryBytes uint64
	// MemoryPeakBytes is the peak memory the runtime reports, or zero when it does not report one.
	MemoryPeakBytes uint64
}

// StatsReader is implemented by runtimes that can sample a container's resource usage.
type StatsReader interface {
	Stats(ctx context.Context, containerID string) (Stats, error)
}

type statsSample struct {
	MemoryStats struct {
		Stats    map[string]uint64 `json:"stats"`
		Usage    uint64            `json:"usage"`
		MaxUsage uint64            `json:"max_usage"`
	} `json:"memory_stats"`
	CPUStats struct {
		CPUUsage struct {
			TotalUsage uint64 `json:"total_usage"`
		} `json:"cpu_usage"`
	} `json:"cpu_stats"`
}

// Stats samples the container's CPU time and memory use.
func (r *DockerRuntime) Stats(ctx context.Context, containerID string) (Stats, error) {
	api, ok := r.docker.(StatsAPIClient)
	if !ok {
		return Stats{}, ErrStatsUnsupported
	}

	body, err := api.ContainerStats(ctx, containerID)
	if err != nil {
		return Stats{}, fmt.Errorf("container stats %q: %w", containerID, err)
	}
	defer body.Close()

	var s statsSample
	if err = json.UnmarshalRead(body, &s, jsonx.V1()); err != nil {
		return Stats{}, fmt.Errorf("decode container stats %q: %w", containerID, err)
	}

	mem := s.MemoryStats.Usage
	for _, k := range []string{"inactive_file", "total_inactive_file"} {
		if cache, found := s.MemoryStats.Stats[k]; found && cache <= mem {
			mem -= cache

			break
		}
	}

	return Stats{
		CPUTimeNanos:    s.CPUStats.CPUUsage.TotalUsage,
		MemoryBytes:     mem,
		MemoryPeakBytes: s.MemoryStats.MaxUsage,
	}, nil
}

// GracefulStopper is implemented by runtimes that can stop a container with a caller-chosen SIGTERM grace period.
type GracefulStopper interface {
	// StopAndRemoveWithin sends SIGTERM, waits up to grace for the process to exit, then kills and removes the
	// container. killed reports that the grace period ran out.
	StopAndRemoveWithin(ctx context.Context, containerID string, grace time.Duration) (killed bool, err error)
}

// StopAndRemoveWithin stops the container after a SIGTERM grace period and removes it.
func (r *DockerRuntime) StopAndRemoveWithin(
	ctx context.Context, containerID string, grace time.Duration,
) (bool, error) {
	secs := max(int(grace.Seconds()), 1)
	start := time.Now()

	if err := r.docker.ContainerStop(ctx, containerID, dockercontainer.StopOptions{Timeout: &secs}); err != nil {
		return false, fmt.Errorf("container stop %q: %w", containerID, err)
	}

	killed := time.Since(start) >= time.Duration(secs)*time.Second

	if err := r.docker.ContainerRemove(
		ctx, containerID, dockercontainer.RemoveOptions{Force: true, RemoveVolumes: true},
	); err != nil {
		return killed, fmt.Errorf("container remove %q: %w", containerID, err)
	}

	r.forgetStop(containerID)

	return killed, nil
}
