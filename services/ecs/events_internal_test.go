package ecs

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
)

type publishedEvent struct {
	detail     map[string]any
	region     string
	source     string
	detailType string
	resources  []string
}

type recordingPublisher struct {
	events []publishedEvent
	mu     sync.Mutex
}

func (p *recordingPublisher) PublishEvent(
	ctx context.Context, source, detailType string, resources []string, detail string,
) {
	var d map[string]any
	if err := json.Unmarshal([]byte(detail), &d); err != nil {
		panic(err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.events = append(p.events, publishedEvent{
		region: awsmeta.Region(ctx), source: source, detailType: detailType, resources: resources, detail: d,
	})
}

func (p *recordingPublisher) ofType(detailType string) []publishedEvent {
	p.mu.Lock()
	defer p.mu.Unlock()

	var out []publishedEvent

	for _, e := range p.events {
		if e.detailType == detailType {
			out = append(out, e)
		}
	}

	return out
}

func (p *recordingPublisher) taskStatuses() []string {
	evs := p.ofType(detailTypeTaskState)
	out := make([]string, 0, len(evs))

	for _, e := range evs {
		out = append(out, e.detail["lastStatus"].(string))
	}

	return out
}

func newEventBackend(t *testing.T, runner TaskRunner) (*InMemoryBackend, *recordingPublisher, string) {
	t.Helper()

	b := NewInMemoryBackend("123456789012", "us-west-2", runner)
	pub := &recordingPublisher{}
	b.SetEventPublisher(pub)

	tdArn := registerSimpleTaskDef(t, b, "ev-app", "nginx")
	_, err := b.CreateCluster(CreateClusterInput{ClusterName: "ev"})
	require.NoError(t, err)

	return b, pub, tdArn
}

func TestTaskStateChangeEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		runner     TaskRunner
		run        func(t *testing.T, b *InMemoryBackend, taskArn string)
		name       string
		wantReason string
		delay      string
		wantStatus []string
	}{
		{
			name: "run_then_stop", runner: NewNoopRunner(),
			run: func(t *testing.T, b *InMemoryBackend, taskArn string) {
				t.Helper()
				_, err := b.StopTask("ev", taskArn, "because tests")
				require.NoError(t, err)
			},
			wantStatus: []string{"PROVISIONING", "PENDING", "RUNNING", "STOPPED"},
			wantReason: "because tests",
		},
		{
			name: "failed_launch", runner: failingRunner{},
			run:        func(*testing.T, *InMemoryBackend, string) {},
			wantStatus: []string{"PROVISIONING", "PENDING", "ACTIVATING", "STOPPED"},
			wantReason: "container start failed: simulated container start failure",
		},
		{
			name: "stop_pipeline", runner: NewNoopRunner(), delay: "stop",
			run: func(t *testing.T, b *InMemoryBackend, taskArn string) {
				t.Helper()
				_, err := b.StopTask("ev", taskArn, "drain")
				require.NoError(t, err)

				base := time.Now().Add(time.Hour)
				for i := 1; i <= 3; i++ {
					b.stepTaskLifecycle(base.Add(time.Duration(i) * time.Second))
				}
			},
			wantStatus: []string{
				"PROVISIONING", "PENDING", "RUNNING", "DEACTIVATING", "STOPPING", "DEPROVISIONING", "STOPPED",
			},
			wantReason: "drain",
		},
		{
			name: "start_pipeline", runner: NewNoopRunner(), delay: "start",
			run: func(_ *testing.T, b *InMemoryBackend, _ string) {
				base := time.Now().Add(time.Hour)
				b.stepTaskLifecycle(base.Add(time.Second))
				b.stepTaskLifecycle(base.Add(2 * time.Second))
			},
			wantStatus: []string{"PROVISIONING", "PENDING", "RUNNING"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, pub, tdArn := newEventBackend(t, tt.runner)

			switch tt.delay {
			case "stop":
				b.SetStopDelay(time.Millisecond)
			case "start":
				b.SetStartDelay(time.Millisecond)
			}

			tasks, _, err := b.RunTask(RunTaskInput{
				Cluster: "ev", TaskDefinition: tdArn, Group: "family:ev-app", StartedBy: "tester",
			})
			tasks = settleTasks(t, b, tasks)
			require.NoError(t, err)
			require.Len(t, tasks, 1)

			tt.run(t, b, tasks[0].TaskArn)

			assert.Equal(t, tt.wantStatus, pub.taskStatuses())

			evs := pub.ofType(detailTypeTaskState)
			last := evs[len(evs)-1]

			assert.Equal(t, "aws.ecs", last.source)
			assert.Equal(t, "us-west-2", last.region)
			assert.Equal(t, []string{tasks[0].TaskArn}, last.resources)
			assert.Equal(t, tasks[0].TaskArn, last.detail["taskArn"])
			assert.Equal(t, tasks[0].ClusterArn, last.detail["clusterArn"])
			assert.Equal(t, tasks[0].TaskDefinitionArn, last.detail["taskDefinitionArn"])
			assert.Equal(t, "family:ev-app", last.detail["group"])
			assert.Equal(t, "tester", last.detail["startedBy"])
			assert.InDelta(t, float64(len(evs)), last.detail["version"], 0)

			if tt.wantReason != "" {
				assert.Equal(t, tt.wantReason, last.detail["stoppedReason"])
				assert.Equal(t, "STOPPED", last.detail["desiredStatus"])
				assert.NotEmpty(t, last.detail["stoppedAt"])
			}

			containers, ok := last.detail["containers"].([]any)
			require.True(t, ok)
			require.Len(t, containers, 1)
			assert.Equal(t, "app", containers[0].(map[string]any)["name"])
			assert.Equal(t, last.detail["lastStatus"], containers[0].(map[string]any)["lastStatus"])
		})
	}
}

func TestTaskStateChangeEventStoppedExitCode(t *testing.T) {
	t.Parallel()

	b, pub, tdArn := newEventBackend(t, NewNoopRunner())

	tasks, _, err := b.RunTask(RunTaskInput{Cluster: "ev", TaskDefinition: tdArn})
	tasks = settleTasks(t, b, tasks)
	require.NoError(t, err)

	b.markTaskStoppedByContainerExit(tasks[0].TaskArn, "app", 3)

	evs := pub.ofType(detailTypeTaskState)
	last := evs[len(evs)-1]
	assert.Equal(t, "STOPPED", last.detail["lastStatus"])
	assert.Equal(t, "Essential container in task exited", last.detail["stoppedReason"])
	assert.InDelta(t, 3, last.detail["containers"].([]any)[0].(map[string]any)["exitCode"], 0)
}

func TestContainerInstanceStateChangeEvents(t *testing.T) {
	t.Parallel()

	b, pub, _ := newEventBackend(t, NewNoopRunner())

	ci, err := b.RegisterContainerInstance("ev", "i-0123")
	require.NoError(t, err)

	_, _, err = b.UpdateContainerInstancesState("ev", []string{ci.ContainerInstanceArn}, statusDraining)
	require.NoError(t, err)

	_, err = b.DeregisterContainerInstance("ev", ci.ContainerInstanceArn, true)
	require.NoError(t, err)

	evs := pub.ofType(detailTypeInstanceState)
	require.Len(t, evs, 3)

	for i, want := range []string{"ACTIVE", "DRAINING", "INACTIVE"} {
		assert.Equal(t, want, evs[i].detail["status"])
		assert.Equal(t, []string{ci.ContainerInstanceArn}, evs[i].resources)
		assert.Equal(t, ci.ContainerInstanceArn, evs[i].detail["containerInstanceArn"])
		assert.Equal(t, "i-0123", evs[i].detail["ec2InstanceId"])
		assert.Equal(t, ci.ClusterArn, evs[i].detail["clusterArn"])
		assert.InDelta(t, i+1, evs[i].detail["version"], 0)
	}
}

func eventNames(evs []publishedEvent) []string {
	out := make([]string, 0, len(evs))
	for _, e := range evs {
		out = append(out, e.detail["eventName"].(string))
	}

	return out
}

func TestDeploymentStateChangeEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		run            func(t *testing.T, b *InMemoryBackend, tdArn string)
		wantDeployment []string
		wantAction     []string
		wantTypes      []string
	}{
		{
			name: "create_reaches_steady_state",
			run: func(t *testing.T, b *InMemoryBackend, tdArn string) {
				t.Helper()
				_, err := b.CreateService(CreateServiceInput{
					ServiceName: "svc", Cluster: "ev", TaskDefinition: tdArn, DesiredCount: 1,
				})
				require.NoError(t, err)

				rec := NewReconciler(b)
				rec.RunOnce(t.Context())
				rec.RunOnce(t.Context())
			},
			wantDeployment: []string{eventDeploymentInProgress, eventDeploymentCompleted},
			wantAction:     []string{eventServiceSteadyState},
			wantTypes:      []string{eventTypeInfo, eventTypeInfo},
		},
		{
			name: "rolling_update_completes",
			run: func(t *testing.T, b *InMemoryBackend, tdArn string) {
				t.Helper()
				_, err := b.CreateService(CreateServiceInput{
					ServiceName: "svc", Cluster: "ev", TaskDefinition: tdArn, DesiredCount: 1,
				})
				require.NoError(t, err)

				rec := NewReconciler(b)
				rec.RunOnce(t.Context())
				rec.RunOnce(t.Context())

				_, err = b.UpdateService(UpdateServiceInput{Cluster: "ev", Service: "svc", ForceNewDeployment: true})
				require.NoError(t, err)

				for range 6 {
					rec.RunOnce(t.Context())
				}
			},
			wantDeployment: []string{
				eventDeploymentInProgress,
				eventDeploymentCompleted,
				eventDeploymentInProgress,
				eventDeploymentCompleted,
			},
			wantAction: []string{eventServiceSteadyState, eventServiceSteadyState},
			wantTypes:  []string{eventTypeInfo, eventTypeInfo, eventTypeInfo, eventTypeInfo},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, pub, tdArn := newEventBackend(t, NewNoopRunner())
			tt.run(t, b, tdArn)

			deployments := pub.ofType(detailTypeDeploymentState)
			assert.Equal(t, tt.wantDeployment, eventNames(deployments))

			for i, e := range deployments {
				assert.Equal(t, tt.wantTypes[i], e.detail["eventType"])
				assert.Contains(t, e.detail["deploymentId"], "ecs-svc/")
				assert.NotEmpty(t, e.detail["updatedAt"])
				require.Len(t, e.resources, 1)
				assert.Contains(t, e.resources[0], ":service/ev/svc")
			}

			actions := pub.ofType(detailTypeServiceAction)
			assert.Equal(t, tt.wantAction, eventNames(actions))
			assert.Contains(t, actions[0].detail["clusterArn"], ":cluster/ev")
		})
	}
}

func TestDeploymentFailedEvent(t *testing.T) {
	t.Parallel()

	b, pub, tdArn := newEventBackend(t, failingRunner{})

	_, err := b.CreateService(CreateServiceInput{
		ServiceName: "svc", Cluster: "ev", TaskDefinition: tdArn, DesiredCount: 3,
		DeploymentConfiguration: &DeploymentConfiguration{
			DeploymentCircuitBreaker: &DeploymentCircuitBreaker{Enable: true},
		},
	})
	require.NoError(t, err)

	_, _, err = b.RunTask(RunTaskInput{Cluster: "ev", TaskDefinition: tdArn, Group: "service:svc", Count: 3})
	require.NoError(t, err)
	b.awaitLaunches()

	evs := pub.ofType(detailTypeDeploymentState)
	assert.Equal(t, []string{eventDeploymentInProgress, eventDeploymentFailed}, eventNames(evs))
	assert.Equal(t, eventTypeError, evs[1].detail["eventType"])
	assert.Contains(t, evs[1].detail["reason"], "circuit breaker")
}

func TestEventsWithoutPublisher(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	tdArn := registerSimpleTaskDef(t, b, "np", "nginx")

	_, err := b.CreateCluster(CreateClusterInput{ClusterName: "np"})
	require.NoError(t, err)

	tasks, _, err := b.RunTask(RunTaskInput{Cluster: "np", TaskDefinition: tdArn})
	tasks = settleTasks(t, b, tasks)
	require.NoError(t, err)
	assert.Equal(t, statusRunning, tasks[0].LastStatus)
}
