package ecs

import (
	"context"
	"sync"
	"testing"
)

// launchTypeEC2Test is the EC2 launch-type string, factored into a constant to
// avoid repeating the literal across the tests below.
const launchTypeEC2Test = "EC2"

// fakeELBv2Registrar is a minimal ELBv2TargetRegistrar fake that records
// every Register/Deregister call so tests can assert on ELBv2 side effects
// without a real elbv2 backend.
type fakeELBv2Registrar struct {
	registered   []registrarCall
	deregistered []registrarCall
	mu           sync.Mutex
}

type registrarCall struct {
	targetGroupARN string
	targets        []ELBTarget
}

func (f *fakeELBv2Registrar) RegisterTargets(_ context.Context, tgArn string, targets []ELBTarget) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.registered = append(f.registered, registrarCall{targetGroupARN: tgArn, targets: targets})

	return nil
}

func (f *fakeELBv2Registrar) DeregisterTargets(_ context.Context, tgArn string, targets []ELBTarget) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.deregistered = append(f.deregistered, registrarCall{targetGroupARN: tgArn, targets: targets})

	return nil
}

func (f *fakeELBv2Registrar) registeredCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.registered)
}

func (f *fakeELBv2Registrar) deregisteredCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.deregistered)
}

const testTGArn = "arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/tg1/abc123"

// fargateLBService bundles the identifiers of a service created by
// newFargateServiceWithLB so tests can pass them around without named returns.
type fargateLBService struct {
	cluster string
	service string
	tdArn   string
}

// newFargateServiceWithLB creates a cluster, a FARGATE-compatible task
// definition, and a service (with LoadBalancers pointing at testTGArn) on b.
func newFargateServiceWithLB(t *testing.T, b *InMemoryBackend, name string) fargateLBService {
	t.Helper()

	const cluster = "cl-elbv2test"

	if _, err := b.CreateCluster(CreateClusterInput{ClusterName: cluster}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	td, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:                  name,
		RequiresCompatibilities: []string{"FARGATE"},
		NetworkMode:             networkModeAwsvpc,
		CPU:                     "256",
		Memory:                  "512",
		ContainerDefinitions:    []ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	if err != nil {
		t.Fatalf("RegisterTaskDefinition: %v", err)
	}

	if _, svcErr := b.CreateService(CreateServiceInput{
		Cluster:        cluster,
		ServiceName:    name,
		TaskDefinition: td.TaskDefinitionArn,
		LaunchType:     launchTypeFargate,
		DesiredCount:   0, // launched explicitly via StartTaskForService below
		LoadBalancers: []LoadBalancer{
			{TargetGroupArn: testTGArn, ContainerName: "app", ContainerPort: 8080},
		},
	}); svcErr != nil {
		t.Fatalf("CreateService: %v", svcErr)
	}

	return fargateLBService{cluster: cluster, service: name, tdArn: td.TaskDefinitionArn}
}

func TestInMemoryBackend_ELBv2Registrar_NoRegistrar_NoEffect(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("123456789012", "us-east-1", NewNoopRunner())
	s := newFargateServiceWithLB(t, b, "svc-no-registrar")

	// No SetELBv2Registrar call: must behave exactly as before this feature,
	// i.e. not panic.
	if err := b.StartTaskForService(s.cluster, s.service, s.tdArn); err != nil {
		t.Fatalf("StartTaskForService: %v", err)
	}

	if err := b.StopOldestServiceTask(s.cluster, s.service); err != nil {
		t.Fatalf("StopOldestServiceTask: %v", err)
	}
}

func TestInMemoryBackend_ELBv2Registrar_RunningTaskRegisters(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("123456789012", "us-east-1", NewNoopRunner())
	reg := &fakeELBv2Registrar{}
	b.SetELBv2Registrar(reg)

	s := newFargateServiceWithLB(t, b, "svc-register")

	if err := b.StartTaskForService(s.cluster, s.service, s.tdArn); err != nil {
		t.Fatalf("StartTaskForService: %v", err)
	}

	if got := reg.registeredCount(); got != 1 {
		t.Fatalf("registeredCount = %d, want 1", got)
	}

	call := reg.registered[0]
	if call.targetGroupARN != testTGArn {
		t.Errorf("targetGroupARN = %q, want %q", call.targetGroupARN, testTGArn)
	}

	if len(call.targets) != 1 {
		t.Fatalf("targets = %v, want 1 entry", call.targets)
	}

	if call.targets[0].Port != 8080 {
		t.Errorf("target port = %d, want 8080", call.targets[0].Port)
	}

	if call.targets[0].ID == "" {
		t.Error("target ID (private IP) is empty")
	}
}

func TestInMemoryBackend_ELBv2Registrar_StopTaskDeregisters(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("123456789012", "us-east-1", NewNoopRunner())
	reg := &fakeELBv2Registrar{}
	b.SetELBv2Registrar(reg)

	s := newFargateServiceWithLB(t, b, "svc-deregister")

	if err := b.StartTaskForService(s.cluster, s.service, s.tdArn); err != nil {
		t.Fatalf("StartTaskForService: %v", err)
	}

	if reg.registeredCount() != 1 {
		t.Fatalf("expected 1 registration before stop, got %d", reg.registeredCount())
	}

	tasks, _, describeErr := b.DescribeTasks(s.cluster, nil)
	if describeErr != nil {
		t.Fatalf("DescribeTasks: %v", describeErr)
	}

	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasks))
	}

	if _, stopErr := b.StopTask(s.cluster, tasks[0].TaskArn, "test stop"); stopErr != nil {
		t.Fatalf("StopTask: %v", stopErr)
	}

	if got := reg.deregisteredCount(); got != 1 {
		t.Fatalf("deregisteredCount = %d, want 1", got)
	}

	call := reg.deregistered[0]
	if call.targetGroupARN != testTGArn || call.targets[0].Port != 8080 {
		t.Errorf("unexpected deregister call: %+v", call)
	}
}

func TestInMemoryBackend_ELBv2Registrar_StopOldestServiceTaskDeregisters(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("123456789012", "us-east-1", NewNoopRunner())
	reg := &fakeELBv2Registrar{}
	b.SetELBv2Registrar(reg)

	s := newFargateServiceWithLB(t, b, "svc-scale-in")

	if err := b.StartTaskForService(s.cluster, s.service, s.tdArn); err != nil {
		t.Fatalf("StartTaskForService: %v", err)
	}

	if err := b.StopOldestServiceTask(s.cluster, s.service); err != nil {
		t.Fatalf("StopOldestServiceTask: %v", err)
	}

	if got := reg.deregisteredCount(); got != 1 {
		t.Fatalf("deregisteredCount = %d, want 1", got)
	}
}

func TestInMemoryBackend_ELBv2Registrar_ServiceWithoutLoadBalancers_NoOp(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("123456789012", "us-east-1", NewNoopRunner())
	reg := &fakeELBv2Registrar{}
	b.SetELBv2Registrar(reg)

	if _, err := b.CreateCluster(CreateClusterInput{ClusterName: "cl-noLB"}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	td, tdErr := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:                  "svc-no-lb",
		RequiresCompatibilities: []string{"FARGATE"},
		NetworkMode:             networkModeAwsvpc,
		CPU:                     "256",
		Memory:                  "512",
		ContainerDefinitions:    []ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	if tdErr != nil {
		t.Fatalf("RegisterTaskDefinition: %v", tdErr)
	}

	if _, svcErr := b.CreateService(CreateServiceInput{
		Cluster:        "cl-noLB",
		ServiceName:    "svc-no-lb",
		TaskDefinition: td.TaskDefinitionArn,
		LaunchType:     launchTypeFargate,
	}); svcErr != nil {
		t.Fatalf("CreateService: %v", svcErr)
	}

	if err := b.StartTaskForService("cl-noLB", "svc-no-lb", td.TaskDefinitionArn); err != nil {
		t.Fatalf("StartTaskForService: %v", err)
	}

	if got := reg.registeredCount(); got != 0 {
		t.Fatalf("registeredCount = %d, want 0 (service has no LoadBalancers)", got)
	}
}

// TestInMemoryBackend_ELBv2Registrar_EC2LaunchType_NoUsableIdentity documents
// the known limitation: EC2-launch-type tasks in this backend have no ENI/
// private-IP modeling (see privateIPFromAttachments), so there is no usable
// ELBv2 target identity to register for them — registration is skipped
// rather than fabricating an identity.
func TestInMemoryBackend_ELBv2Registrar_EC2LaunchType_NoUsableIdentity(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("123456789012", "us-east-1", NewNoopRunner())
	reg := &fakeELBv2Registrar{}
	b.SetELBv2Registrar(reg)

	if _, err := b.CreateCluster(CreateClusterInput{ClusterName: "cl-ec2"}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	td, tdErr := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family:               "svc-ec2",
		ContainerDefinitions: []ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	if tdErr != nil {
		t.Fatalf("RegisterTaskDefinition: %v", tdErr)
	}

	if _, svcErr := b.CreateService(CreateServiceInput{
		Cluster:        "cl-ec2",
		ServiceName:    "svc-ec2",
		TaskDefinition: td.TaskDefinitionArn,
		LaunchType:     launchTypeEC2Test,
		LoadBalancers: []LoadBalancer{
			{TargetGroupArn: testTGArn, ContainerName: "app", ContainerPort: 8080},
		},
	}); svcErr != nil {
		t.Fatalf("CreateService: %v", svcErr)
	}

	if err := b.StartTaskForService("cl-ec2", "svc-ec2", td.TaskDefinitionArn); err != nil {
		t.Fatalf("StartTaskForService: %v", err)
	}

	if got := reg.registeredCount(); got != 0 {
		t.Fatalf("registeredCount = %d, want 0 (EC2-launch-type tasks have no ENI private IP)", got)
	}
}
