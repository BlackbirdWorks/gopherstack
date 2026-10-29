package ecs

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dockertypes "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/container"
	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

// inspectingFake reports each container's image and running state, which the plain fake leaves empty.
type inspectingFake struct {
	*fakeDockerClient
	images  map[string]string
	stopped map[string]bool
}

func (f *inspectingFake) ContainerInspect(_ context.Context, id string) (dockertypes.InspectResponse, error) {
	timeout := 7

	return dockertypes.InspectResponse{
		ID:     id,
		State:  &dockertypes.State{Running: !f.stopped[id]},
		Config: &dockertypes.Config{Image: f.images[id], StopTimeout: &timeout},
	}, nil
}

func claimsFrom(held map[string]container.ManagedContainer) claimFunc {
	return func(service, resource string) (container.ManagedContainer, bool) {
		if service != "ecs" {
			return container.ManagedContainer{}, false
		}

		c, ok := held[resource]

		return c, ok
	}
}

const adoptTaskArn = "arn:aws:ecs:us-east-1:000000000000:task/c/abc"

func adoptTaskDef() *TaskDefinition {
	return &TaskDefinition{ContainerDefinitions: []ContainerDefinition{
		{Name: "web", Image: "nginx", Essential: true},
		{Name: "side", Image: "busybox", Essential: false},
	}}
}

func TestAdoptTask(t *testing.T) {
	t.Parallel()

	held := func(names ...string) map[string]container.ManagedContainer {
		out := map[string]container.ManagedContainer{}
		for _, n := range names {
			out[ecsResourceKey(adoptTaskArn, n)] = container.ManagedContainer{
				ID: "id-" + n, Running: true,
				Labels: map[string]string{labelTaskVolumes: "vol-a,vol-b"},
			}
		}

		return out
	}

	tests := []struct {
		held        map[string]container.ManagedContainer
		images      map[string]string
		stopped     map[string]bool
		name        string
		wantRemoved []string
		wantAdopt   bool
	}{
		{
			name: "all containers present", held: held("web", "side"),
			images:    map[string]string{"id-web": "nginx", "id-side": "busybox"},
			wantAdopt: true,
		},
		{
			name: "one container missing", held: held("web"),
			images:      map[string]string{"id-web": "nginx"},
			wantRemoved: []string{"id-web"},
		},
		{
			name: "image changed", held: held("web", "side"),
			images:      map[string]string{"id-web": "nginx", "id-side": "alpine"},
			wantRemoved: []string{"id-web", "id-side"},
		},
		{
			name: "container exited", held: held("web", "side"),
			images:      map[string]string{"id-web": "nginx", "id-side": "busybox"},
			stopped:     map[string]bool{"id-side": true},
			wantRemoved: []string{"id-web", "id-side"},
		},
		{name: "nothing held", held: held()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fake := &inspectingFake{
				fakeDockerClient: &fakeDockerClient{}, images: tt.images, stopped: tt.stopped,
			}
			r := newDockerRunnerWithClient(t.Context(), fake)
			r.claim = claimsFrom(tt.held)

			task := &Task{TaskArn: adoptTaskArn}
			got := r.AdoptTask(t.Context(), task, adoptTaskDef())

			assert.Equal(t, tt.wantAdopt, got)
			assert.ElementsMatch(t, tt.wantRemoved, fake.removed)

			if !tt.wantAdopt {
				assert.Empty(t, r.ContainerIDs(adoptTaskArn))

				return
			}

			assert.Equal(t, []string{"id-web", "id-side"}, r.ContainerIDs(adoptTaskArn))
			assert.Equal(t, []string{"vol-a", "vol-b"}, r.taskVolumes[adoptTaskArn])
			assert.Equal(t, 7, r.stopTimeouts["id-web"])
			assert.Empty(t, fake.removed)
		})
	}
}

func TestManagedLabelsForTaskContainer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		plan         *volumePlan
		name         string
		wantResource string
		wantVolumes  string
		fronted      bool
	}{
		{name: "adoptable", plan: &volumePlan{}, wantResource: ecsResourceKey(adoptTaskArn, "web")},
		{
			name: "task volumes", plan: &volumePlan{taskVols: []string{"v1", "v2"}},
			wantResource: ecsResourceKey(adoptTaskArn, "web"), wantVolumes: "v1,v2",
		},
		{name: "fronted never adopted", plan: &volumePlan{}, fronted: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			l := &launch{task: &Task{TaskArn: adoptTaskArn}, plan: tt.plan}
			got := managedLabels(l, ContainerDefinition{Name: "web"}, tt.fronted)

			assert.Equal(t, "true", got[container.LabelManaged])
			assert.Equal(t, container.CurrentIdentity().Owner, got[container.LabelOwner])
			assert.Equal(t, "ecs", got[container.LabelService])
			assert.Equal(t, tt.wantResource, got[container.LabelResource])
			assert.Equal(t, tt.wantVolumes, got[labelTaskVolumes])
		})
	}
}

func TestRunTaskStampsIdentityLabels(t *testing.T) {
	t.Parallel()

	fake := &fakeDockerClient{}
	runner := newDockerRunnerWithClient(t.Context(), fake)
	backend := NewInMemoryBackend("000000000000", "us-east-1", runner)

	_, err := backend.CreateCluster(CreateClusterInput{ClusterName: "test"})
	require.NoError(t, err)

	_, err = backend.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family: "fam", ContainerDefinitions: []ContainerDefinition{{Name: "app", Image: "busybox"}},
	})
	require.NoError(t, err)

	tasks, _, err := backend.RunTask(RunTaskInput{Cluster: "test", TaskDefinition: "fam"})
	require.NoError(t, err)

	tasks = settleTasks(t, backend, tasks)
	require.Len(t, tasks, 1)
	require.Len(t, fake.creates, 1)

	l := fake.creates[0].cfg.Labels
	assert.Equal(t, "true", l[container.LabelManaged])
	assert.Equal(t, "ecs", l[container.LabelService])
	assert.Equal(t, ecsResourceKey(tasks[0].TaskArn, "app"), l[container.LabelResource])
}

func TestRestoredTasksAdoptedOrStopped(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus string
		adopt      bool
	}{
		{name: "containers survived", adopt: true, wantStatus: statusRunning},
		{name: "containers lost", adopt: false, wantStatus: statusStopped},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fake1 := &fakeDockerClient{}
			b1 := NewInMemoryBackend("000000000000", "us-east-1", newDockerRunnerWithClient(t.Context(), fake1))

			_, err := b1.CreateCluster(CreateClusterInput{ClusterName: "test"})
			require.NoError(t, err)

			_, err = b1.RegisterTaskDefinition(RegisterTaskDefinitionInput{
				Family: "fam", ContainerDefinitions: []ContainerDefinition{{Name: "app", Image: "busybox"}},
			})
			require.NoError(t, err)

			tasks, _, err := b1.RunTask(RunTaskInput{Cluster: "test", TaskDefinition: "fam"})
			require.NoError(t, err)

			tasks = settleTasks(t, b1, tasks)
			require.Equal(t, statusRunning, tasks[0].LastStatus)

			arn, cid := tasks[0].TaskArn, tasks[0].Containers[0].RuntimeID
			require.NotEmpty(t, cid)

			snap := b1.Snapshot(t.Context())

			fake2 := &inspectingFake{
				fakeDockerClient: &fakeDockerClient{waitResult: make(chan dockertypes.WaitResponse, 1)},
				images:           map[string]string{cid: "busybox"},
			}
			r2 := newDockerRunnerWithClient(t.Context(), fake2)

			if tt.adopt {
				r2.claim = claimsFrom(map[string]container.ManagedContainer{
					ecsResourceKey(arn, "app"): {ID: cid, Running: true},
				})
			} else {
				r2.claim = claimsFrom(nil)
			}

			b2 := NewInMemoryBackend("000000000000", "us-east-1", r2)
			r2.SetTaskCompletionHandler(b2.markTaskStoppedByContainerExit)
			require.NoError(t, b2.Restore(t.Context(), snap))
			b2.adoptRestoredTasks(t.Context())

			got, _, err := b2.DescribeTasks("test", []string{arn})
			require.NoError(t, err)
			require.Len(t, got, 1)
			assert.Equal(t, tt.wantStatus, got[0].LastStatus)

			if !tt.adopt {
				assert.Equal(t, lostTaskReason, got[0].StoppedReason)

				return
			}

			assert.Equal(t, []string{cid}, r2.ContainerIDs(arn))

			require.Eventually(t, func() bool {
				fake2.mu.Lock()
				defer fake2.mu.Unlock()

				return len(fake2.waitRequestedOn) > 0
			}, 2*time.Second, 10*time.Millisecond, "exit watching must resume")

			fake2.waitResult <- dockertypes.WaitResponse{StatusCode: 4}

			require.Eventually(t, func() bool {
				d, _, descErr := b2.DescribeTasks("test", []string{arn})
				require.NoError(t, descErr)

				return d[0].LastStatus == statusStopped
			}, 2*time.Second, 10*time.Millisecond, "an adopted container exiting must stop the task")
		})
	}
}
