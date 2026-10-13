package ssm

import (
	"context"
	"time"
)

// ManagedContainer is a running, Compute-backed EC2 instance acting as a managed node.
type ManagedContainer struct {
	RegisteredAt time.Time
	Tags         map[string]string
	InstanceID   string
	ContainerID  string
}

// ContainerInstances lists the running Compute-backed EC2 instances of a region that have an
// instance profile (the SSM agent needs one to register). ids restricts the result when non-empty.
type ContainerInstances interface {
	ContainerInstances(region string, ids []string) []ManagedContainer
}

// ScriptFilePlaceholder in ScriptRequest.Interpreter stands for the path of the script file.
const ScriptFilePlaceholder = "{{file}}"

// ScriptRequest is one document step run in an instance container.
type ScriptRequest struct {
	WorkDir     string
	Script      string
	Interpreter []string
}

// ScriptResult is the outcome of a ScriptRequest.
type ScriptResult struct {
	Stdout   []byte
	Stderr   []byte
	ExitCode int
}

// ScriptExecutor runs a script in a container. A cancelled ctx must stop the script and return an error.
type ScriptExecutor interface {
	RunScript(ctx context.Context, containerID string, req ScriptRequest) (ScriptResult, error)
}

// CommandOutputSink stores Run Command output. Implementations resolve S3 and CloudWatch Logs per call.
type CommandOutputSink interface {
	PutObject(bucket, key string, body []byte) error
	PutLogLines(region, group, stream string, lines []string) error
}

// CommandRunner is the optional wiring that lets SendCommand execute in instance containers.
type CommandRunner struct {
	Instances ContainerInstances
	Executor  ScriptExecutor
	Sink      CommandOutputSink
}

// SetCommandRunner enables direct execution of SendCommand on Compute-backed instances.
func (b *InMemoryBackend) SetCommandRunner(r *CommandRunner) {
	b.mu.Lock("SetCommandRunner")
	defer b.mu.Unlock()

	b.runner = r
}

func (b *InMemoryBackend) commandRunner() *CommandRunner {
	b.mu.RLock("commandRunner")
	defer b.mu.RUnlock()

	return b.runner
}
