package ecs

import (
	"context"
	"strconv"
	"strings"

	dockertypes "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/container"
	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	labelTaskVolumes = "gopherstack.ecs.volumes"
	adoptStopSeconds = 2
)

type claimFunc func(service, resource string) (container.ManagedContainer, bool)

// taskAdopter is implemented by runners that can take over containers a crashed gopherstack left running.
type taskAdopter interface {
	AdoptTask(ctx context.Context, task *Task, td *TaskDefinition) bool
}

func ecsResourceKey(taskArn, containerName string) string { return taskArn + "|" + containerName }

// managedLabels identify a task container to a restarted gopherstack. Containers behind a security-group relay
// carry no resource key: the relay dies with the process, so they are never adopted.
func managedLabels(l *launch, cd ContainerDefinition, fronted bool) map[string]string {
	if fronted {
		return container.ManagedLabels("ecs", "")
	}

	out := container.ManagedLabels("ecs", ecsResourceKey(l.task.TaskArn, cd.Name))
	if l.plan != nil && len(l.plan.taskVols) > 0 {
		out[labelTaskVolumes] = strings.Join(l.plan.taskVols, ",")
	}

	return out
}

// AdoptTask takes over the running containers of task, one per container definition. It is all or nothing: when
// any container is missing, not running or not the expected image, every claimed container is removed and false
// is returned.
func (r *realDockerRunner) AdoptTask(ctx context.Context, task *Task, td *TaskDefinition) bool {
	claim := r.claim
	if claim == nil {
		claim = container.ClaimOrphan
	}

	return r.adoptTask(ctx, claim, task, td)
}

type adoptedContainer struct {
	id      string
	def     ContainerDefinition
	timeout int
}

func (r *realDockerRunner) adoptTask(ctx context.Context, claim claimFunc, task *Task, td *TaskDefinition) bool {
	order, err := orderContainers(td.ContainerDefinitions)
	if err != nil {
		return false
	}

	var (
		claimed []string
		found   = make([]adoptedContainer, len(td.ContainerDefinitions))
		vols    string
		ok      = true
	)

	for i, cd := range td.ContainerDefinitions {
		oc, hit := claim("ecs", ecsResourceKey(task.TaskArn, cd.Name))
		if !hit {
			ok = false

			continue
		}

		claimed = append(claimed, oc.ID)

		ac, usable := r.adoptable(ctx, task, i, cd, oc)
		if !usable {
			ok = false

			continue
		}

		found[i] = ac

		if v := oc.Labels[labelTaskVolumes]; v != "" {
			vols = v
		}
	}

	if !ok {
		r.discardClaimed(ctx, claimed)

		return false
	}

	r.resume(task, td, found, order, vols)

	logger.Load(ctx).InfoContext(ctx, "ecs: adopted task containers from an earlier run", "task", task.TaskArn)

	return true
}

func (r *realDockerRunner) adoptable(
	ctx context.Context, task *Task, idx int, cd ContainerDefinition, oc container.ManagedContainer,
) (adoptedContainer, bool) {
	if idx < len(task.Containers) {
		if want := task.Containers[idx].RuntimeID; want != "" && want != oc.ID {
			return adoptedContainer{}, false
		}
	}

	resp, err := r.cli.ContainerInspect(ctx, oc.ID)
	if err != nil || resp.State == nil || !resp.State.Running || resp.Config == nil || resp.Config.Image != cd.Image {
		return adoptedContainer{}, false
	}

	timeout := rollbackStopTimeoutSeconds
	if resp.Config.StopTimeout != nil {
		timeout = *resp.Config.StopTimeout
	}

	return adoptedContainer{def: cd, id: oc.ID, timeout: timeout}, true
}

func (r *realDockerRunner) discardClaimed(ctx context.Context, ids []string) {
	timeout := adoptStopSeconds

	for _, id := range ids {
		_ = r.cli.ContainerStop(ctx, id, dockertypes.StopOptions{Timeout: &timeout})
		_ = r.cli.ContainerRemove(ctx, id, dockertypes.RemoveOptions{Force: true, RemoveVolumes: true})
	}
}

// resume registers the adopted containers and restarts exit, health and log watching. Logs resume from now:
// replaying the container's whole history would duplicate what was already forwarded.
func (r *realDockerRunner) resume(
	task *Task, td *TaskDefinition, found []adoptedContainer, order []int, vols string,
) {
	defOrder := make([]string, len(found))
	startOrder := make([]string, 0, len(found))

	for i, f := range found {
		defOrder[i] = f.id
	}

	for _, i := range order {
		startOrder = append(startOrder, found[i].id)
	}

	r.mu.Lock()
	r.containers[task.TaskArn] = defOrder
	r.stopOrder[task.TaskArn] = startOrder

	if vols != "" {
		r.taskVolumes[task.TaskArn] = strings.Split(vols, ",")
	}

	for _, f := range found {
		r.stopTimeouts[f.id] = f.timeout
	}

	r.mu.Unlock()

	handler := r.taskCompletionHandler()
	needsExit := exitDependencies(td.ContainerDefinitions)
	since := strconv.FormatInt(r.now().Unix(), 10)
	taskID := taskIDFromARN(task.TaskArn)

	for _, f := range found {
		if cwLogs := r.cwLogsBackend(); cwLogs != nil {
			if group, stream, ok := awslogsContainerTarget(f.def, taskID, f.id); ok {
				_ = cwLogs.EnsureLogGroupAndStream(group, stream)
				r.forwardContainerLogs(cwLogs, f.id, group, stream, since)
			}
		}

		if handler != nil || needsExit[f.def.Name] || f.def.RestartPolicy != nil {
			r.watchContainerExit(handler, task.TaskArn, f.id, f.def)
		}

		r.monitorHealth(task.TaskArn, f.id, f.def)
	}
}
