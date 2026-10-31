package ecs

import (
	"context"
	"errors"
	"fmt"

	"github.com/moby/moby/api/types/mount"

	dockertypes "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/container"
)

const (
	volumeScopeTask   = "task"
	volumeScopeShared = "shared"
)

var (
	errUnsupportedVolume = errors.New("unsupported volume")
	errUndefinedVolume   = errors.New("mounts undefined volume")
)

// volumeSpec is a task-level volume resolved to its Docker mount source.
type volumeSpec struct {
	mount  mount.Mount
	user   string
	groups []string
}

func bindMount(src string) mount.Mount {
	return mount.Mount{Type: mount.TypeBind, Source: src}
}

// volumePlan holds a task's resolved volumes and the Docker volumes to delete when the task ends.
type volumePlan struct {
	specs    map[string]volumeSpec
	taskVols []string
}

func taskVolumeName(task *Task, volume string) string {
	return "gopherstack-ecs-" + taskIDFromARN(task.TaskArn) + "-" + volume
}

// resolveVolumes maps td.Volumes to Docker mounts: host.sourcePath is a bind, dockerVolumeConfiguration a
// named volume (task scope is deleted with the task), and an empty host an ephemeral task-scoped volume.
func (r *realDockerRunner) resolveVolumes(ctx context.Context, task *Task, td *TaskDefinition) (*volumePlan, error) {
	plan := &volumePlan{specs: make(map[string]volumeSpec, len(td.Volumes))}

	for _, v := range td.Volumes {
		switch {
		case v.EFSVolumeConfiguration != nil:
			spec, err := r.efsVolumeSpec(task, v)
			if err != nil {
				return nil, err
			}

			plan.specs[v.Name] = spec
		case v.ConfiguredAtLaunch:
			return nil, fmt.Errorf("%w: volume %q is configuredAtLaunch", errUnsupportedVolume, v.Name)
		case v.DockerVolumeConfig != nil:
			spec, err := r.dockerVolumeSpec(ctx, task, v, plan)
			if err != nil {
				return nil, err
			}

			plan.specs[v.Name] = spec
		case v.Host != nil && v.Host.SourcePath != "":
			plan.specs[v.Name] = volumeSpec{mount: mount.Mount{
				Type:        mount.TypeBind,
				Source:      v.Host.SourcePath,
				BindOptions: &mount.BindOptions{CreateMountpoint: true},
			}}
		default:
			name := taskVolumeName(task, v.Name)
			plan.taskVols = append(plan.taskVols, name)
			plan.specs[v.Name] = volumeSpec{mount: mount.Mount{Type: mount.TypeVolume, Source: name}}
		}
	}

	return plan, nil
}

func (r *realDockerRunner) efsVolumeSpec(task *Task, v Volume) (volumeSpec, error) {
	r.mu.Lock()
	resolve := r.efsLookup
	r.mu.Unlock()

	if resolve == nil {
		return volumeSpec{}, &taskLaunchError{
			Reason: fmt.Sprintf("%s: volume %q: %v", efsInitFailure, v.Name, errEFSUnavailable),
			Err:    errEFSUnavailable,
		}
	}

	m, err := resolve(regionOfARN(task.TaskArn, ""), v.EFSVolumeConfiguration)
	if err != nil {
		return volumeSpec{}, &taskLaunchError{Reason: fmt.Sprintf("volume %q: %v", v.Name, err), Err: err}
	}

	return efsSpec(m), nil
}

func (r *realDockerRunner) dockerVolumeSpec(
	ctx context.Context,
	task *Task,
	v Volume,
	plan *volumePlan,
) (volumeSpec, error) {
	cfg := v.DockerVolumeConfig
	name := taskVolumeName(task, v.Name)

	if cfg.Scope == volumeScopeShared {
		name = v.Name

		if err := r.requireSharedVolume(ctx, cfg, name); err != nil {
			return volumeSpec{}, err
		}
	} else {
		plan.taskVols = append(plan.taskVols, name)
	}

	vo := &mount.VolumeOptions{Labels: cfg.Labels}
	if cfg.Driver != "" || len(cfg.DriverOpts) > 0 {
		vo.DriverConfig = &mount.Driver{Name: cfg.Driver, Options: cfg.DriverOpts}
	}

	return volumeSpec{mount: mount.Mount{Type: mount.TypeVolume, Source: name, VolumeOptions: vo}}, nil
}

// requireSharedVolume fails unless a shared volume without autoprovision already exists.
func (r *realDockerRunner) requireSharedVolume(ctx context.Context, cfg *DockerVolumeConfig, name string) error {
	if cfg.Autoprovision != nil && *cfg.Autoprovision {
		return nil
	}

	exists, err := r.cli.VolumeExists(ctx, name)
	if err != nil {
		return fmt.Errorf("inspect docker volume %q: %w", name, err)
	}

	if !exists {
		return fmt.Errorf("%w: shared docker volume %q does not exist and autoprovision is false",
			errUnsupportedVolume, name)
	}

	return nil
}

// containerMounts builds cd's mounts from its mountPoints.
func containerMounts(cd ContainerDefinition, plan *volumePlan) ([]mount.Mount, error) {
	out := make([]mount.Mount, 0, len(cd.MountPoints))

	for _, mp := range cd.MountPoints {
		spec, ok := plan.specs[mp.SourceVolume]
		if !ok {
			return nil, fmt.Errorf("container %q %w %q", cd.Name, errUndefinedVolume, mp.SourceVolume)
		}

		m := spec.mount
		m.Target = mp.ContainerPath
		m.ReadOnly = mp.ReadOnly
		out = append(out, m)
	}

	return out, nil
}

// applyVolumeIdentity runs a container as an EFS access point's POSIX user unless the definition sets a user.
func applyVolumeIdentity(
	cfg *dockertypes.Config,
	hc *dockertypes.HostConfig,
	cd ContainerDefinition,
	plan *volumePlan,
) {
	if cd.User != "" {
		return
	}

	for _, mp := range cd.MountPoints {
		if spec := plan.specs[mp.SourceVolume]; spec.user != "" {
			cfg.User = spec.user
			hc.GroupAdd = append(hc.GroupAdd, spec.groups...)

			return
		}
	}
}
