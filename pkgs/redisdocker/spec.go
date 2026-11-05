package redisdocker

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

// Engine names.
const (
	EngineRedis  = "redis"
	EngineValkey = "valkey"
)

const (
	minModernMajor = 7
	versionParts   = 2

	maxReplicas = 5
)

// ErrSpec is returned when a launch request is inconsistent.
var ErrSpec = errors.New("invalid redis group spec")

// Runtime is the slice of the container runtime a group needs.
type Runtime interface {
	CreateAndStart(ctx context.Context, spec container.Spec) (string, error)
	StopAndRemove(ctx context.Context, containerID string) error
	Exec(ctx context.Context, containerID string, cmd []string) (container.ExecResult, error)
}

// ShardSpec describes one shard: its slots, replica count and optional RDB to start from.
type ShardSpec struct {
	// Seed is an RDB file the primary starts with; empty starts blank.
	Seed []byte
	// Slots are the hash slots served by the shard; ignored unless the group is in cluster mode.
	Slots []SlotRange
	// Replicas is the number of replicas of the shard's primary.
	Replicas int
}

// Spec describes a group to launch.
type Spec struct {
	// Engine is "redis" (default) or "valkey".
	Engine string
	// Version is the engine version ("7.1", "7.2.4"); only major.minor selects the image.
	Version string
	// TLSNames are extra certificate names beyond localhost and the advertised host.
	TLSNames []string
	Shards   []ShardSpec
	// ClusterMode runs Redis Cluster; without it the group is one primary and its replicas.
	ClusterMode bool
	// TLS serves TLS only (plaintext port disabled).
	TLS bool
}

func (s Spec) validate() error {
	if len(s.Shards) == 0 {
		return fmt.Errorf("%w: no shards", ErrSpec)
	}

	if !s.ClusterMode && len(s.Shards) != 1 {
		return fmt.Errorf("%w: a group without cluster mode has exactly one shard", ErrSpec)
	}

	for i, sh := range s.Shards {
		if sh.Replicas < 0 || sh.Replicas > maxReplicas {
			return fmt.Errorf("%w: shard %d replicas must be 0-%d", ErrSpec, i, maxReplicas)
		}
	}

	if !s.ClusterMode {
		return nil
	}

	slots := make([][]SlotRange, len(s.Shards))
	for i, sh := range s.Shards {
		slots[i] = sh.Slots
	}

	return ValidateSlotMap(slots)
}

// withDefaultSlots fills empty slot assignments of a cluster-mode spec with an even split.
func (s Spec) withDefaultSlots() Spec {
	if !s.ClusterMode {
		return s
	}

	for _, sh := range s.Shards {
		if len(sh.Slots) > 0 {
			return s
		}
	}

	even := EvenSlots(len(s.Shards))
	out := s
	out.Shards = make([]ShardSpec, len(s.Shards))

	for i, sh := range s.Shards {
		sh.Slots = even[i]
		out.Shards[i] = sh
	}

	return out
}

// ImageFor maps an engine and API version to a container image. Versions below 7 run on 7.0, whose cluster
// hostname support the group relies on; odd minors map to the next published image.
func ImageFor(engine, version string) (string, string) {
	major, minor := parseVersion(version)

	if engine == EngineValkey {
		if major < 1 {
			major, minor = 8, 0
		}

		return "valkey/valkey:" + strconv.Itoa(major) + "." + strconv.Itoa(minor) + "-alpine", "valkey-server"
	}

	if major < minModernMajor {
		major, minor = minModernMajor, 0
	}

	if minor%2 == 1 {
		minor++
	}

	return "redis:" + strconv.Itoa(major) + "." + strconv.Itoa(minor) + "-alpine", "redis-server"
}

func parseVersion(v string) (int, int) {
	parts := strings.SplitN(v, ".", versionParts+1)
	major, _ := strconv.Atoi(parts[0])

	minor := 0
	if len(parts) >= versionParts {
		minor, _ = strconv.Atoi(parts[1])
	}

	return major, minor
}
