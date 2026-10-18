package ecs

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
)

const testASGArn = "arn:aws:autoscaling:us-east-1:123456789012:autoScalingGroup:uuid:autoScalingGroupName/g"

type fakeASG struct {
	protected map[string]bool
	ids       []string
	min, max  int
	desired   int
	mu        sync.Mutex
}

func (f *fakeASG) AutoScalingGroupExists(string) bool { return true }

func (f *fakeASG) DescribeGroup(string) (ASGInfo, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return ASGInfo{
		Name: "g", Min: f.min, Max: f.max, Desired: f.desired,
		InServiceInstanceIDs: slices.Clone(f.ids),
	}, true
}

func (f *fakeASG) SetDesiredCapacity(_ string, desired int) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for len(f.ids) < desired {
		f.ids = append(f.ids, fmt.Sprintf("i-%d", len(f.ids)+1))
	}

	for len(f.ids) > desired {
		last := f.ids[len(f.ids)-1]
		if f.protected[last] {
			break
		}

		f.ids = f.ids[:len(f.ids)-1]
	}

	f.desired = desired

	return nil
}

func (f *fakeASG) SetInstanceProtection(_ string, ids []string, protected bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, id := range ids {
		f.protected[id] = protected
	}

	return nil
}

func scalingBackend(
	t *testing.T,
	ms *ManagedScaling,
	protection string,
) (*InMemoryBackend, *fakeASG, *[]cwmetric.Point) {
	t.Helper()

	b := newTestBackend()
	asg := &fakeASG{min: 0, max: 10, protected: map[string]bool{}}
	b.SetAutoScalingGroupController(asg)
	b.SetInstanceResourceResolver(sizedResolver{cpu: 2048, mem: 4096})

	var (
		mu     sync.Mutex
		points []cwmetric.Point
	)

	b.SetMetricEmitter(cwmetric.EmitterFunc(func(p cwmetric.Point) error {
		mu.Lock()
		defer mu.Unlock()

		points = append(points, p)

		return nil
	}))

	_, err := b.CreateCluster(CreateClusterInput{ClusterName: "default"})
	require.NoError(t, err)

	_, err = b.CreateCapacityProvider(CreateCapacityProviderInput{
		Name: "cp",
		AutoScalingGroupProvider: &AutoScalingGroupProvider{
			AutoScalingGroupArn: testASGArn, ManagedScaling: ms, ManagedTerminationProtection: protection,
		},
	})
	require.NoError(t, err)

	_, err = b.PutClusterCapacityProviders("default", []string{"cp"}, nil)
	require.NoError(t, err)

	return b, asg, &points
}

func runOnProvider(t *testing.T, b *InMemoryBackend, td string, count int) []Task {
	t.Helper()

	tasks, failures, err := b.RunTask(RunTaskInput{
		TaskDefinition: td, Count: count,
		CapacityProviderStrategy: []CapacityProviderStrategyItem{{CapacityProvider: "cp", Weight: 1}},
	})
	require.NoError(t, err)
	require.Empty(t, failures)

	return tasks
}

func TestManagedScaling_ScaleOutPlacesWaitingTasks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ms          *ManagedScaling
		name        string
		tasks       int
		wantDesired int
	}{
		{name: "two_instances", ms: &ManagedScaling{Status: "ENABLED"}, tasks: 4, wantDesired: 2},
		{
			name:        "step_capped",
			ms:          &ManagedScaling{Status: "ENABLED", MaximumScalingStepSize: 1},
			tasks:       4,
			wantDesired: 1,
		},
		{
			name: "target_50_doubles", tasks: 2, wantDesired: 2,
			ms: &ManagedScaling{Status: "ENABLED", TargetCapacityPercent: 50},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, asg, points := scalingBackend(t, tt.ms, "DISABLED")
			td := ledgerTD(t, b, "web-"+tt.name, 1024, 1024)

			tasks := runOnProvider(t, b, td, tt.tasks)
			for _, task := range tasks {
				assert.Equal(t, statusProvisioning, task.LastStatus)
			}

			b.ReconcileCapacity(context.Background())

			assert.Equal(t, tt.wantDesired, asg.desired)

			running := 0

			for _, task := range b.tasks.All() {
				if task.LastStatus == statusRunning {
					running++
				}
			}

			assert.Positive(t, running)
			require.NotEmpty(t, *points)
			assert.InDelta(t, 200.0, (*points)[0].Value, 0.001)
			assert.Equal(t, managedScalingNamespace, (*points)[0].Namespace)
			assert.Equal(t, reservationMetric, (*points)[0].Name)
		})
	}
}

func TestManagedScaling_ScaleInAndProtection(t *testing.T) {
	t.Parallel()

	b, asg, _ := scalingBackend(t, &ManagedScaling{Status: "ENABLED"}, "ENABLED")
	td := ledgerTD(t, b, "svc", 1024, 1024)

	tasks := runOnProvider(t, b, td, 4)
	b.ReconcileCapacity(context.Background())
	require.Equal(t, 2, asg.desired)

	for _, task := range tasks[:2] {
		_, err := b.StopTask("default", task.TaskArn, "scale in")
		require.NoError(t, err)
	}

	b.ReconcileCapacity(context.Background())

	protectedCount := 0

	for _, p := range asg.protected {
		if p {
			protectedCount++
		}
	}

	assert.GreaterOrEqual(t, protectedCount, 1)
	assert.Less(t, asg.desired, 3)
	assert.GreaterOrEqual(t, asg.desired, protectedCount)
}

func TestManagedScaling_DisabledLeavesGroupAndFailsPlacement(t *testing.T) {
	t.Parallel()

	b, asg, _ := scalingBackend(t, &ManagedScaling{Status: "DISABLED"}, "DISABLED")
	td := ledgerTD(t, b, "off", 1024, 1024)

	tasks, failures, err := b.RunTask(RunTaskInput{
		TaskDefinition: td, Count: 1,
		CapacityProviderStrategy: []CapacityProviderStrategyItem{{CapacityProvider: "cp", Weight: 1}},
	})
	require.NoError(t, err)
	assert.Empty(t, tasks)
	require.Len(t, failures, 1)
	assert.Equal(t, failureReasonResource, failures[0].Reason)

	b.ReconcileCapacity(context.Background())
	assert.Zero(t, asg.desired)
}

func TestReservationMath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		m, n, target int
		want         float64
		wantTarget   int
	}{
		{name: "idle", m: 0, n: 0, target: 100, want: 100, wantTarget: 0},
		{name: "no_instances", m: 2, n: 0, target: 100, want: 200, wantTarget: 2},
		{name: "half", m: 1, n: 2, target: 100, want: 50, wantTarget: 1},
		{name: "headroom", m: 3, n: 3, target: 75, want: 100, wantTarget: 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.InDelta(t, tt.want, reservation(tt.m, tt.n), 0.001)
			assert.Equal(t, tt.wantTarget, targetInstances(tt.m, tt.target))
		})
	}
}
