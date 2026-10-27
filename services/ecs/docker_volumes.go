package ecs

import (
	"context"
	"errors"
	"fmt"

	"github.com/moby/moby/api/types/mount"
)

const (
	volumeScopeTask   = "task"
	volumeScopeShared = "shared"
)

var errUnsupportedVolume = errors.New("unsupported volume")

// volumeSpec is a task-level volume resolved to its Docker mount source.
type volumeSpec struct {
	mount mount.Mount
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
			return nil, fmt.Errorf("%w: volume %q uses efsVolumeConfiguration", errUnsupportedVolume, v.Name)
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

		if cfg.Autoprovision == nil || !*cfg.Autoprovision {
			exists, err := r.cli.VolumeExists(ctx, name)
			if err != nil {
				return volumeSpec{}, fmt.Errorf("inspect docker volume %q: %w", name, err)
			}

			if !exists {
				return volumeSpec{}, fmt.Errorf(
					"%w: shared docker volume %q does not exist and autoprovision is false", errUnsupportedVolume, name)
			}
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

// containerMounts builds cd's mounts from its mountPoints.
func containerMounts(cd ContainerDefinition, plan *volumePlan) ([]mount.Mount, error) {
	out := make([]mount.Mount, 0, len(cd.MountPoints))

	for _, mp := range cd.MountPoints {
		spec, ok := plan.specs[mp.SourceVolume]
		if !ok {
			return nil, fmt.Errorf("container %q mounts undefined volume %q", cd.Name, mp.SourceVolume)
		}

		m := spec.mount
		m.Target = mp.ContainerPath
		m.ReadOnly = mp.ReadOnly
		out = append(out, m)
	}

	return out, nil
}
