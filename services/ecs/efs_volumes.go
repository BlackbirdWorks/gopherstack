package ecs

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// efsInitFailure is the documented ResourceInitializationError stoppedReason for EFS volume setup
// (stopped-task error codes: "failed to invoke EFS utils commands to set up EFS volumes").
const efsInitFailure = "ResourceInitializationError: failed to invoke EFS utils commands to set up EFS volumes"

var errEFSUnavailable = errors.New("EFS integration is not available")

// EFSVolumeMount is the host directory backing an efsVolumeConfiguration, plus the access point's POSIX identity.
type EFSVolumeMount struct {
	HostDir       string
	SecondaryGIDs []int64
	UID           int64
	GID           int64
	HasPosixUser  bool
}

// EFSResolver resolves task-definition EFS volumes against services/efs.
type EFSResolver interface {
	ResolveVolume(ctx context.Context, region, fsID, accessPointID, rootDirectory string) (EFSVolumeMount, error)
}

// efsVolumeRunner is implemented by runners that mount EFS volumes.
type efsVolumeRunner interface {
	SetEFSVolumeResolver(fn func(region string, cfg *EFSVolumeConfiguration) (EFSVolumeMount, error))
}

// SetEFSResolver wires EFS so efsVolumeConfiguration volumes mount at launch.
func (b *InMemoryBackend) SetEFSResolver(r EFSResolver) {
	b.mu.Lock("SetEFSResolver")
	defer b.mu.Unlock()

	b.efs = r
}

func (b *InMemoryBackend) efsResolver() EFSResolver {
	b.mu.RLock("efsResolver")
	defer b.mu.RUnlock()

	return b.efs
}

func (b *InMemoryBackend) resolveEFSVolume(region string, cfg *EFSVolumeConfiguration) (EFSVolumeMount, error) {
	r := b.efsResolver()
	if r == nil {
		return EFSVolumeMount{}, fmt.Errorf("%s: %w", efsInitFailure, errEFSUnavailable)
	}

	ap, root := "", cfg.RootDirectory
	if cfg.AuthorizationConfig != nil {
		ap = cfg.AuthorizationConfig.AccessPointID
	}

	m, err := r.ResolveVolume(context.Background(), region, cfg.FileSystemID, ap, root)
	if err != nil {
		return EFSVolumeMount{}, fmt.Errorf("%s: %w", efsInitFailure, err)
	}

	return m, nil
}

// efsSpec turns a resolved EFS mount into a bind volumeSpec; the access point's POSIX user becomes the
// default user of every container mounting the volume that sets none.
func efsSpec(m EFSVolumeMount) volumeSpec {
	s := volumeSpec{mount: bindMount(m.HostDir)}

	if m.HasPosixUser {
		s.user = strconv.FormatInt(m.UID, 10) + ":" + strconv.FormatInt(m.GID, 10)

		for _, g := range m.SecondaryGIDs {
			s.groups = append(s.groups, strconv.FormatInt(g, 10))
		}
	}

	return s
}

func regionOfARN(arn, def string) string {
	parts := strings.Split(arn, ":")
	const regionIdx = 3
	if len(parts) > regionIdx && parts[regionIdx] != "" {
		return parts[regionIdx]
	}

	return def
}
