package ecs

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// ensureCall records one EnsureLogGroupAndStream invocation.
type ensureCall struct {
	group  string
	stream string
}

// mockECSCWLogsBackend is a test double for CWLogsBackend.
type mockECSCWLogsBackend struct {
	ensured []ensureCall
	mu      sync.Mutex
}

func (m *mockECSCWLogsBackend) EnsureLogGroupAndStream(groupName, streamName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensured = append(m.ensured, ensureCall{group: groupName, stream: streamName})

	return nil
}

func (m *mockECSCWLogsBackend) PutLogLines(_, _ string, _ []string) error {
	return nil
}

func (m *mockECSCWLogsBackend) calls() []ensureCall {
	m.mu.Lock()
	defer m.mu.Unlock()

	return append([]ensureCall(nil), m.ensured...)
}

func awslogsContainerDef(group, streamPrefix string) ContainerDefinition {
	opts := map[string]string{"awslogs-group": group}
	if streamPrefix != "" {
		opts["awslogs-stream-prefix"] = streamPrefix
	}

	return ContainerDefinition{
		Name:  "app",
		Image: "nginx",
		LogConfiguration: &LogConfiguration{
			LogDriver: logDriverAwslogs,
			Options:   opts,
		},
	}
}

// TestRunTask_AwslogsLogConfiguration_CreatesLogGroupAndStream proves that
// RunTask, once a CWLogsBackend is wired via SetCWLogsBackend, creates the
// log group/stream named by an awslogs-driver container's LogConfiguration
// (gopherstack-sv5q) -- instead of LogConfiguration being accepted, stored,
// and echoed with no effect. The stream name follows the SDK-documented
// "prefix-name/container-name/ecs-task-id" format for awslogs-stream-prefix.
func TestRunTask_AwslogsLogConfiguration_CreatesLogGroupAndStream(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	mock := &mockECSCWLogsBackend{}
	b.SetCWLogsBackend(mock)

	_, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:               "awslogs-family",
		ContainerDefinitions: []ContainerDefinition{awslogsContainerDef("/ecs/myapp", "ecs")},
	})
	require.NoError(t, err)

	tasks, err := b.RunTask(RunTaskInput{TaskDefinition: "awslogs-family"})
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	wantStream := "ecs/app/" + taskIDFromARN(tasks[0].TaskArn)

	calls := mock.calls()
	require.Len(t, calls, 1)
	require.Equal(t, "/ecs/myapp", calls[0].group)
	require.Equal(t, wantStream, calls[0].stream)
}

// TestRunTask_AwslogsLogConfiguration_NoStreamPrefix_FallsBackToTaskID proves
// the no-prefix approximation this fix uses (see logs.go's awslogsStreamName
// doc comment): real ECS names the stream after the Docker-assigned
// container ID in this case, which this backend has no access to at this
// layer, so the task ID alone is used instead.
func TestRunTask_AwslogsLogConfiguration_NoStreamPrefix_FallsBackToTaskID(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	mock := &mockECSCWLogsBackend{}
	b.SetCWLogsBackend(mock)

	_, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:               "awslogs-noprefix",
		ContainerDefinitions: []ContainerDefinition{awslogsContainerDef("/ecs/myapp", "")},
	})
	require.NoError(t, err)

	tasks, err := b.RunTask(RunTaskInput{TaskDefinition: "awslogs-noprefix"})
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	calls := mock.calls()
	require.Len(t, calls, 1)
	require.Equal(t, taskIDFromARN(tasks[0].TaskArn), calls[0].stream)
}

// TestRunTask_NonAwslogsDriver_NoLogStreamCreated proves only the awslogs
// driver triggers CloudWatch Logs stream creation -- a splunk/fluentd/etc.
// LogConfiguration must stay stored-and-echoed only, matching pre-fix
// behavior for every driver other than awslogs.
func TestRunTask_NonAwslogsDriver_NoLogStreamCreated(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	mock := &mockECSCWLogsBackend{}
	b.SetCWLogsBackend(mock)

	_, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family: "splunk-family",
		ContainerDefinitions: []ContainerDefinition{{
			Name:  "app",
			Image: "nginx",
			LogConfiguration: &LogConfiguration{
				LogDriver: "splunk",
				Options:   map[string]string{"splunk-url": "https://example.com"},
			},
		}},
	})
	require.NoError(t, err)

	_, err = b.RunTask(RunTaskInput{TaskDefinition: "splunk-family"})
	require.NoError(t, err)

	require.Empty(t, mock.calls())
}

// TestRunTask_AwslogsLogConfiguration_NoBackendWired_StaysPermissive proves
// the unwired path stays a silent no-op: with no CWLogsBackend set (the
// default for every one of the ~150 services that construct this backend in
// tests with no cross-service hooks), RunTask with an awslogs
// LogConfiguration must behave exactly as before this fix -- it must not
// reject, error, or panic.
func TestRunTask_AwslogsLogConfiguration_NoBackendWired_StaysPermissive(t *testing.T) {
	t.Parallel()

	b := newTestBackend() // no SetCWLogsBackend call: b.cwLogs stays nil

	_, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:               "awslogs-unwired",
		ContainerDefinitions: []ContainerDefinition{awslogsContainerDef("/ecs/myapp", "ecs")},
	})
	require.NoError(t, err)

	tasks, err := b.RunTask(RunTaskInput{TaskDefinition: "awslogs-unwired"})
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Equal(t, statusRunning, tasks[0].LastStatus)
}

// TestStartTask_AwslogsLogConfiguration_CreatesLogGroupAndStream proves the
// StartTask placement path (as opposed to RunTask's auto-placement) also
// wires the awslogs log group/stream creation.
func TestStartTask_AwslogsLogConfiguration_CreatesLogGroupAndStream(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	mock := &mockECSCWLogsBackend{}
	b.SetCWLogsBackend(mock)

	_, err := b.CreateCluster(CreateClusterInput{ClusterName: "start-task-cluster"})
	require.NoError(t, err)

	ci, err := b.RegisterContainerInstance("start-task-cluster", "i-abc123")
	require.NoError(t, err)

	_, err = b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:               "awslogs-start-task",
		ContainerDefinitions: []ContainerDefinition{awslogsContainerDef("/ecs/starttask", "ecs")},
	})
	require.NoError(t, err)

	tasks, failures, err := b.StartTask(StartTaskInput{
		Cluster:            "start-task-cluster",
		TaskDefinition:     "awslogs-start-task",
		ContainerInstances: []string{ci.ContainerInstanceArn},
	})
	require.NoError(t, err)
	require.Empty(t, failures)
	require.Len(t, tasks, 1)

	wantStream := "ecs/app/" + taskIDFromARN(tasks[0].TaskArn)

	calls := mock.calls()
	require.Len(t, calls, 1)
	require.Equal(t, "/ecs/starttask", calls[0].group)
	require.Equal(t, wantStream, calls[0].stream)
}
