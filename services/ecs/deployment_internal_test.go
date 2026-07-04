package ecs

import (
	"errors"
	"testing"
)

// errSimulatedStartFailure is returned by failingRunner to simulate a container
// that never starts (image pull error, crash-on-boot, etc.).
var errSimulatedStartFailure = errors.New("simulated container start failure")

// failingRunner is a TaskRunner whose RunTask always fails.
type failingRunner struct{}

func (failingRunner) RunTask(_ *Task, _ *TaskDefinition) error {
	return errSimulatedStartFailure
}
func (failingRunner) StopTask(_ *Task) error { return nil }

// registerSimpleTaskDef registers a minimal task definition and returns its ARN.
func registerSimpleTaskDef(t *testing.T, b *InMemoryBackend, family, image string) string {
	t.Helper()

	td, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:               family,
		ContainerDefinitions: []ContainerDefinition{{Name: "app", Image: image}},
	})
	if err != nil {
		t.Fatalf("RegisterTaskDefinition(%s): %v", family, err)
	}

	return td.TaskDefinitionArn
}

func TestCircuitBreakerThreshold(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		desired int
		want    int
	}{
		{name: "zero desired clamps to floor", desired: 0, want: 3},
		{name: "one desired clamps to floor", desired: 1, want: 3},
		{name: "small desired clamps to floor", desired: 4, want: 3},
		{name: "half rounds up above floor", desired: 10, want: 5},
		{name: "odd desired rounds up", desired: 7, want: 4},
		{name: "large desired below ceiling", desired: 300, want: 150},
		{name: "huge desired clamps to ceiling", desired: 1000, want: 200},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := circuitBreakerThreshold(tt.desired); got != tt.want {
				t.Errorf("circuitBreakerThreshold(%d) = %d, want %d", tt.desired, got, tt.want)
			}
		})
	}
}

func TestCircuitBreakerConfigHelpers(t *testing.T) {
	t.Parallel()

	enable := func(en, rb bool) *Service {
		return &Service{DeploymentConfiguration: &DeploymentConfiguration{
			DeploymentCircuitBreaker: &DeploymentCircuitBreaker{Enable: en, Rollback: rb},
		}}
	}

	tests := []struct {
		svc          *Service
		name         string
		wantEnabled  bool
		wantRollback bool
	}{
		{name: "nil config", svc: &Service{}, wantEnabled: false, wantRollback: false},
		{
			name: "config without breaker",
			svc:  &Service{DeploymentConfiguration: &DeploymentConfiguration{}},
		},
		{name: "enabled no rollback", svc: enable(true, false), wantEnabled: true},
		{name: "enabled with rollback", svc: enable(true, true), wantEnabled: true, wantRollback: true},
		{name: "disabled", svc: enable(false, true), wantEnabled: false, wantRollback: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := circuitBreakerEnabled(tt.svc); got != tt.wantEnabled {
				t.Errorf("circuitBreakerEnabled = %v, want %v", got, tt.wantEnabled)
			}
			if got := circuitBreakerRollback(tt.svc); got != tt.wantRollback {
				t.Errorf("circuitBreakerRollback = %v, want %v", got, tt.wantRollback)
			}
		})
	}
}

// Cluster and service names shared by the circuit-breaker tests.
const (
	cbCluster = "cb"
	cbService = "svc"
)

// launchFailingServiceTasks launches count tasks for the circuit-breaker service
// via RunTask with a failing runner so each attributed failure feeds the breaker.
func launchFailingServiceTasks(t *testing.T, b *InMemoryBackend, tdArn string, count int) {
	t.Helper()

	if _, err := b.RunTask(RunTaskInput{
		Cluster:        cbCluster,
		TaskDefinition: tdArn,
		Group:          "service:" + cbService,
		Count:          count,
	}); err != nil {
		t.Fatalf("RunTask: %v", err)
	}
}

func primaryDeployment(t *testing.T, b *InMemoryBackend) Deployment {
	t.Helper()

	svcs, _, err := b.DescribeServices(cbCluster, []string{cbService})
	if err != nil {
		t.Fatalf("DescribeServices: %v", err)
	}
	if len(svcs) != 1 {
		t.Fatalf("DescribeServices returned %d services, want 1", len(svcs))
	}

	for _, d := range svcs[0].Deployments {
		if d.Status == deploymentStatusPrimary {
			return d
		}
	}

	t.Fatalf("no PRIMARY deployment found on service %s", cbService)

	return Deployment{}
}

func TestCircuitBreaker_TripsToFailed_NoRollback(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("123456789012", "us-east-1", failingRunner{})
	tdArn := registerSimpleTaskDef(t, b, "cb-app", "nginx:good")

	if _, err := b.CreateCluster(CreateClusterInput{ClusterName: "cb"}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	if _, err := b.CreateService(CreateServiceInput{
		ServiceName:    "svc",
		Cluster:        "cb",
		TaskDefinition: tdArn,
		DesiredCount:   3,
		DeploymentConfiguration: &DeploymentConfiguration{
			DeploymentCircuitBreaker: &DeploymentCircuitBreaker{Enable: true, Rollback: false},
		},
	}); err != nil {
		t.Fatalf("CreateService: %v", err)
	}

	// Three failed launches meet the threshold for desiredCount=3.
	launchFailingServiceTasks(t, b, tdArn, 3)

	dep := primaryDeployment(t, b)
	if dep.RolloutState != deploymentRolloutStateFailed {
		t.Fatalf("RolloutState = %q, want FAILED (FailedTasks=%d)", dep.RolloutState, dep.FailedTasks)
	}
	if dep.FailedTasks < 3 {
		t.Errorf("FailedTasks = %d, want >= 3", dep.FailedTasks)
	}

	// The reconciler must halt: it should NOT launch replacement tasks for a
	// FAILED, non-rolled-back deployment.
	r := NewReconciler(b)
	r.RunOnce(t.Context())

	arns, err := b.ListTasksFiltered(ListTasksInput{Cluster: "cb", ServiceName: "svc"})
	if err != nil {
		t.Fatalf("ListTasksFiltered: %v", err)
	}
	if len(arns) != 3 {
		t.Errorf("task count after halt = %d, want 3 (no relaunch of failing deployment)", len(arns))
	}
}

func TestCircuitBreaker_BelowThreshold_StaysInProgress(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("123456789012", "us-east-1", failingRunner{})
	tdArn := registerSimpleTaskDef(t, b, "cb-app", "nginx:good")

	if _, err := b.CreateCluster(CreateClusterInput{ClusterName: "cb"}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	if _, err := b.CreateService(CreateServiceInput{
		ServiceName:    "svc",
		Cluster:        "cb",
		TaskDefinition: tdArn,
		DesiredCount:   10,
		DeploymentConfiguration: &DeploymentConfiguration{
			DeploymentCircuitBreaker: &DeploymentCircuitBreaker{Enable: true},
		},
	}); err != nil {
		t.Fatalf("CreateService: %v", err)
	}

	// Threshold for desiredCount=10 is 5; only 4 failures should not trip.
	launchFailingServiceTasks(t, b, tdArn, 4)

	dep := primaryDeployment(t, b)
	if dep.RolloutState != deploymentRolloutStateInProgress {
		t.Errorf("RolloutState = %q, want IN_PROGRESS (FailedTasks=%d)", dep.RolloutState, dep.FailedTasks)
	}
}

func TestCircuitBreaker_Disabled_NeverTrips(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("123456789012", "us-east-1", failingRunner{})
	tdArn := registerSimpleTaskDef(t, b, "cb-app", "nginx:good")

	if _, err := b.CreateCluster(CreateClusterInput{ClusterName: "cb"}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	if _, err := b.CreateService(CreateServiceInput{
		ServiceName:    "svc",
		Cluster:        "cb",
		TaskDefinition: tdArn,
		DesiredCount:   2,
		// No DeploymentConfiguration → circuit breaker off.
	}); err != nil {
		t.Fatalf("CreateService: %v", err)
	}

	launchFailingServiceTasks(t, b, tdArn, 10)

	dep := primaryDeployment(t, b)
	if dep.RolloutState != deploymentRolloutStateInProgress {
		t.Errorf("RolloutState = %q, want IN_PROGRESS (breaker disabled)", dep.RolloutState)
	}
}

func TestCircuitBreaker_RollsBackToStableTaskDef(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("123456789012", "us-east-1", failingRunner{})
	v1 := registerSimpleTaskDef(t, b, "cb-app", "nginx:v1-good")

	if _, err := b.CreateCluster(CreateClusterInput{ClusterName: "cb"}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	if _, err := b.CreateService(CreateServiceInput{
		ServiceName:    "svc",
		Cluster:        "cb",
		TaskDefinition: v1,
		DesiredCount:   2,
		DeploymentConfiguration: &DeploymentConfiguration{
			DeploymentCircuitBreaker: &DeploymentCircuitBreaker{Enable: true, Rollback: true},
		},
	}); err != nil {
		t.Fatalf("CreateService: %v", err)
	}

	// Roll the service forward to a bad revision; this demotes v1 to ACTIVE and
	// makes v2 the PRIMARY deployment.
	v2 := registerSimpleTaskDef(t, b, "cb-app", "nginx:v2-bad")

	if _, err := b.UpdateService(UpdateServiceInput{
		Cluster:        "cb",
		Service:        "svc",
		TaskDefinition: v2,
	}); err != nil {
		t.Fatalf("UpdateService: %v", err)
	}

	// The new (v2) deployment fails to start enough tasks to trip the breaker.
	launchFailingServiceTasks(t, b, v2, 3)

	svcs, _, err := b.DescribeServices("cb", []string{"svc"})
	if err != nil {
		t.Fatalf("DescribeServices: %v", err)
	}

	got := svcs[0]
	if got.TaskDefinition != v1 {
		t.Errorf("service TaskDefinition = %q, want rollback to %q", got.TaskDefinition, v1)
	}

	// The new PRIMARY (rollback) deployment should target v1 and be IN_PROGRESS.
	var primary, failed *Deployment

	for i := range got.Deployments {
		d := &got.Deployments[i]
		switch {
		case d.Status == deploymentStatusPrimary:
			primary = d
		case d.RolloutState == deploymentRolloutStateFailed:
			failed = d
		}
	}

	if primary == nil {
		t.Fatal("no PRIMARY deployment after rollback")
	}
	if primary.TaskDefinition != v1 {
		t.Errorf("rollback PRIMARY TaskDefinition = %q, want %q", primary.TaskDefinition, v1)
	}
	if primary.RolloutState != deploymentRolloutStateInProgress {
		t.Errorf("rollback PRIMARY RolloutState = %q, want IN_PROGRESS", primary.RolloutState)
	}
	if failed == nil {
		t.Error("expected a demoted FAILED deployment to remain visible")
	} else if failed.TaskDefinition != v2 {
		t.Errorf("FAILED deployment TaskDefinition = %q, want %q", failed.TaskDefinition, v2)
	}
}

func TestCircuitBreaker_InitialDeployment_NoRollbackTarget(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("123456789012", "us-east-1", failingRunner{})
	v1 := registerSimpleTaskDef(t, b, "cb-app", "nginx:v1-bad")

	if _, err := b.CreateCluster(CreateClusterInput{ClusterName: "cb"}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	if _, err := b.CreateService(CreateServiceInput{
		ServiceName:    "svc",
		Cluster:        "cb",
		TaskDefinition: v1,
		DesiredCount:   2,
		DeploymentConfiguration: &DeploymentConfiguration{
			DeploymentCircuitBreaker: &DeploymentCircuitBreaker{Enable: true, Rollback: true},
		},
	}); err != nil {
		t.Fatalf("CreateService: %v", err)
	}

	launchFailingServiceTasks(t, b, v1, 3)

	// With no prior stable deployment, the initial deployment stays FAILED and
	// the service is not reverted anywhere.
	dep := primaryDeployment(t, b)
	if dep.RolloutState != deploymentRolloutStateFailed {
		t.Errorf("RolloutState = %q, want FAILED (no rollback target)", dep.RolloutState)
	}

	svcs, _, err := b.DescribeServices("cb", []string{"svc"})
	if err != nil {
		t.Fatalf("DescribeServices: %v", err)
	}
	if svcs[0].TaskDefinition != v1 {
		t.Errorf("TaskDefinition = %q, want unchanged %q", svcs[0].TaskDefinition, v1)
	}
}
