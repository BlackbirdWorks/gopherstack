package ecs

import (
	"context"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const execNotEnabledMessage = "The execute command failed because execute command was not enabled when the task " +
	"was run or the execute command agent isn't running. Wait 30 seconds and try again or run a new task " +
	"using the enable-execute-command option."

type streamBaseKey struct{}

// WithStreamBase returns ctx carrying the websocket base URL ("wss://host") of the server handling the request.
func WithStreamBase(ctx context.Context, base string) context.Context {
	return context.WithValue(ctx, streamBaseKey{}, base)
}

// ExecSessionRequest asks a Session Manager data channel to run Command in a task container.
type ExecSessionRequest struct {
	Region      string
	Target      string
	ContainerID string
	Command     string
	StreamBase  string
}

// ExecSessionGrant is the stream a client attaches to.
type ExecSessionGrant struct {
	SessionID  string
	StreamURL  string
	TokenValue string
}

// ExecSessionStarter opens ECS Exec sessions; wired to the SSM Session Manager data channel.
type ExecSessionStarter interface {
	StartExecSession(ctx context.Context, req ExecSessionRequest) (ExecSessionGrant, error)
}

// SetExecSessionStarter wires the Session Manager backend ExecuteCommand streams through.
func (b *InMemoryBackend) SetExecSessionStarter(s ExecSessionStarter) {
	b.mu.Lock("SetExecSessionStarter")
	defer b.mu.Unlock()

	b.execSessions = s
}

// ExecuteCommand runs command in a container of a RUNNING task that has execute command enabled.
func (b *InMemoryBackend) ExecuteCommand(
	cluster, task, container, command string,
	interactive bool,
) (*ExecuteCommandOutput, error) {
	return b.ExecuteCommandContext(context.Background(), cluster, task, container, command, interactive)
}

// ExecuteCommandContext is ExecuteCommand with ctx carrying the request's stream base (see WithStreamBase).
func (b *InMemoryBackend) ExecuteCommandContext(
	ctx context.Context,
	cluster, task, container, command string,
	interactive bool,
) (*ExecuteCommandOutput, error) {
	if task == "" {
		return nil, fmt.Errorf("%w: task is required", ErrInvalidParameter)
	}

	if command == "" {
		return nil, fmt.Errorf("%w: command is required", ErrInvalidParameter)
	}

	if !interactive {
		return nil, fmt.Errorf(
			"%w: Amazon ECS only supports interactive sessions, so you must specify true for interactive",
			ErrInvalidParameter,
		)
	}

	clusterName := clusterKey(b.resolveCluster(cluster))

	target, out, starter, err := b.prepareExec(clusterName, cluster, task, container)
	if err != nil {
		return nil, err
	}

	if starter == nil {
		return nil, fmt.Errorf(
			"%w: the execute command agent for %s cannot be reached; ECS Exec needs the docker task runtime "+
				"and the SSM Session Manager service",
			ErrTargetNotConnected, container,
		)
	}

	base, _ := ctx.Value(streamBaseKey{}).(string)

	grant, err := starter.StartExecSession(ctx, ExecSessionRequest{
		Region: b.region, Target: target.name, ContainerID: target.runtimeID, Command: command, StreamBase: base,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrTargetNotConnected, err)
	}

	out.Interactive = true
	out.Session = Session(grant)

	return out, nil
}

type execTarget struct {
	name      string
	runtimeID string
}

// prepareExec validates the task and container and resolves the runtime container. starter is nil when the task
// is not on a runtime that can exec.
func (b *InMemoryBackend) prepareExec(
	clusterName, cluster, task, container string,
) (execTarget, *ExecuteCommandOutput, ExecSessionStarter, error) {
	b.mu.RLock("ExecuteCommand")
	defer b.mu.RUnlock()

	if !b.clusters.Has(clusterName) {
		return execTarget{}, nil, nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	taskArn := task
	if !strings.HasPrefix(task, "arn:") {
		taskArn = arn.Build("ecs", b.region, b.accountID, fmt.Sprintf("task/%s/%s", clusterName, task))
	}

	t, ok := b.tasks.Get(taskArn)
	if !ok || clusterKey(t.ClusterArn) != clusterName {
		return execTarget{}, nil, nil, fmt.Errorf("%w: task %s not found", ErrInvalidParameter, task)
	}

	if t.LastStatus != statusRunning {
		return execTarget{}, nil, nil, fmt.Errorf("%w: task %s is not in RUNNING state", ErrInvalidParameter, task)
	}

	if !t.EnableExecuteCommand {
		return execTarget{}, nil, nil, fmt.Errorf("%w: %s", ErrInvalidParameter, execNotEnabledMessage)
	}

	c, err := execContainer(t, container)
	if err != nil {
		return execTarget{}, nil, nil, err
	}

	clusterObj, _ := b.clusters.Get(clusterName)
	out := &ExecuteCommandOutput{
		ClusterArn: clusterObj.ClusterArn, ContainerArn: c.ContainerArn, ContainerName: c.Name, TaskArn: t.TaskArn,
	}

	_, dockerRuntime := b.runner.(interface{ ContainerIDs(taskArn string) []string })
	if !dockerRuntime || b.execSessions == nil || c.RuntimeID == "" {
		return execTarget{}, out, nil, nil
	}

	name := fmt.Sprintf("ecs:%s_%s_%s", clusterName, taskIDFromARN(t.TaskArn), c.RuntimeID)

	return execTarget{name: name, runtimeID: c.RuntimeID}, out, b.execSessions, nil
}

// execContainer picks the container to exec in: the named one, or the only one when the name is omitted.
func execContainer(t *Task, name string) (Container, error) {
	if name == "" {
		if len(t.Containers) != 1 {
			return Container{}, fmt.Errorf(
				"%w: the container name must be specified for a task with %d containers",
				ErrInvalidParameter, len(t.Containers),
			)
		}

		return t.Containers[0], nil
	}

	for _, c := range t.Containers {
		if c.Name == name {
			return c, nil
		}
	}

	return Container{}, fmt.Errorf("%w: container %s not found in task %s", ErrInvalidParameter, name, t.TaskArn)
}
