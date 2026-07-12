package autoscaling_test

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

// fakeELBv2Registrar is a minimal autoscaling.ELBv2TargetRegistrar fake that
// records every Register/Deregister call so tests can assert on ELBv2 side
// effects without a real elbv2 backend.
type fakeELBv2Registrar struct {
	registerErr  error
	registered   []registrarCall
	deregistered []registrarCall
	mu           sync.Mutex
}

type registrarCall struct {
	targetGroupARN string
	ids            []string
}

func (f *fakeELBv2Registrar) RegisterTargets(
	_ context.Context, tgArn string, targets []autoscaling.ELBTarget,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.registered = append(f.registered, registrarCall{targetGroupARN: tgArn, ids: elbTargetIDs(targets)})

	return f.registerErr
}

func (f *fakeELBv2Registrar) DeregisterTargets(
	_ context.Context, tgArn string, targets []autoscaling.ELBTarget,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.deregistered = append(f.deregistered, registrarCall{targetGroupARN: tgArn, ids: elbTargetIDs(targets)})

	return nil
}

func elbTargetIDs(targets []autoscaling.ELBTarget) []string {
	ids := make([]string, len(targets))
	for i, t := range targets {
		ids[i] = t.ID
	}

	return ids
}

// registeredIDsFor returns the union of instance IDs registered against tgArn
// across all RegisterTargets calls observed so far.
func (f *fakeELBv2Registrar) registeredIDsFor(tgArn string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	var ids []string

	for _, c := range f.registered {
		if c.targetGroupARN == tgArn {
			ids = append(ids, c.ids...)
		}
	}

	return ids
}

func (f *fakeELBv2Registrar) deregisteredIDsFor(tgArn string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	var ids []string

	for _, c := range f.deregistered {
		if c.targetGroupARN == tgArn {
			ids = append(ids, c.ids...)
		}
	}

	return ids
}

const testTGArn = "arn:aws:elasticloadbalancing:us-east-1:000000000000:targetgroup/tg1/abc123"

// newTGGroup creates an ASG (with a launch configuration and no EC2Launcher,
// so instances are fabricated) whose TargetGroupARNs includes testTGArn.
func newTGGroup(t *testing.T, b *autoscaling.InMemoryBackend, name string, desired int32) {
	t.Helper()

	_, err := b.CreateLaunchConfiguration(autoscaling.CreateLaunchConfigurationInput{
		LaunchConfigurationName: name + "-lc",
		ImageID:                 "ami-12345678",
		InstanceType:            "t3.micro",
	})
	require.NoError(t, err)

	_, err = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
		AutoScalingGroupName:    name,
		LaunchConfigurationName: name + "-lc",
		MinSize:                 0,
		MaxSize:                 10,
		DesiredCapacity:         desired,
		AvailabilityZones:       []string{"us-east-1a"},
		TargetGroupARNs:         []string{testTGArn},
	})
	require.NoError(t, err)
}

func TestInMemoryBackend_ELBv2Registrar_NoRegistrar_NoEffect(t *testing.T) {
	t.Parallel()

	b := autoscaling.NewInMemoryBackend()
	t.Cleanup(b.Close)

	// No SetELBv2Registrar call: must behave exactly as before this feature,
	// i.e. not panic and not attempt any registration.
	newTGGroup(t, b, "asg-no-registrar", 2)

	require.NoError(t, b.SetDesiredCapacity("asg-no-registrar", 4))
	_, err := b.TerminateInstanceInAutoScalingGroup(mustFirstInstanceID(t, b, "asg-no-registrar"), false)
	require.NoError(t, err)
}

func mustFirstInstanceID(t *testing.T, b *autoscaling.InMemoryBackend, groupName string) string {
	t.Helper()

	groups, err := b.DescribeAutoScalingGroups([]string{groupName})
	require.NoError(t, err)
	require.NotEmpty(t, groups)
	require.NotEmpty(t, groups[0].Instances)

	return groups[0].Instances[0].InstanceID
}

func TestInMemoryBackend_ELBv2Registrar_CreateGroupRegisters(t *testing.T) {
	t.Parallel()

	b := autoscaling.NewInMemoryBackend()
	t.Cleanup(b.Close)

	reg := &fakeELBv2Registrar{}
	b.SetELBv2Registrar(reg)

	newTGGroup(t, b, "asg-create", 2)

	assert.Len(t, reg.registeredIDsFor(testTGArn), 2)
}

func TestInMemoryBackend_ELBv2Registrar_ScaleOutRegistersNewInstances(t *testing.T) {
	t.Parallel()

	b := autoscaling.NewInMemoryBackend()
	t.Cleanup(b.Close)

	reg := &fakeELBv2Registrar{}
	b.SetELBv2Registrar(reg)

	newTGGroup(t, b, "asg-scale-out", 1)
	require.Len(t, reg.registeredIDsFor(testTGArn), 1)

	require.NoError(t, b.SetDesiredCapacity("asg-scale-out", 3))

	assert.Len(t, reg.registeredIDsFor(testTGArn), 3)
}

func TestInMemoryBackend_ELBv2Registrar_ScaleInDeregisters(t *testing.T) {
	t.Parallel()

	b := autoscaling.NewInMemoryBackend()
	t.Cleanup(b.Close)

	reg := &fakeELBv2Registrar{}
	b.SetELBv2Registrar(reg)

	newTGGroup(t, b, "asg-scale-in", 3)
	require.NoError(t, b.SetDesiredCapacity("asg-scale-in", 1))

	assert.Len(t, reg.deregisteredIDsFor(testTGArn), 2)
}

func TestInMemoryBackend_ELBv2Registrar_TerminateInstanceDeregisters(t *testing.T) {
	t.Parallel()

	b := autoscaling.NewInMemoryBackend()
	t.Cleanup(b.Close)

	reg := &fakeELBv2Registrar{}
	b.SetELBv2Registrar(reg)

	newTGGroup(t, b, "asg-terminate", 2)
	id := mustFirstInstanceID(t, b, "asg-terminate")

	_, err := b.TerminateInstanceInAutoScalingGroup(id, false)
	require.NoError(t, err)

	assert.Contains(t, reg.deregisteredIDsFor(testTGArn), id)
}

func TestInMemoryBackend_ELBv2Registrar_AttachDetachInstances(t *testing.T) {
	t.Parallel()

	b := autoscaling.NewInMemoryBackend()
	t.Cleanup(b.Close)

	reg := &fakeELBv2Registrar{}
	b.SetELBv2Registrar(reg)

	newTGGroup(t, b, "asg-attach", 0)

	require.NoError(t, b.AttachInstances("asg-attach", []string{"i-manual1", "i-manual2"}))
	assert.ElementsMatch(t, []string{"i-manual1", "i-manual2"}, reg.registeredIDsFor(testTGArn))

	_, err := b.DetachInstances("asg-attach", []string{"i-manual1"}, false)
	require.NoError(t, err)
	assert.Contains(t, reg.deregisteredIDsFor(testTGArn), "i-manual1")
	assert.NotContains(t, reg.deregisteredIDsFor(testTGArn), "i-manual2")
}

func TestInMemoryBackend_ELBv2Registrar_AttachDetachTargetGroups(t *testing.T) {
	t.Parallel()

	const secondTG = "arn:aws:elasticloadbalancing:us-east-1:000000000000:targetgroup/tg2/def456"

	b := autoscaling.NewInMemoryBackend()
	t.Cleanup(b.Close)

	reg := &fakeELBv2Registrar{}
	b.SetELBv2Registrar(reg)

	newTGGroup(t, b, "asg-tg-attach", 2)
	reg.registered = nil // reset from CreateAutoScalingGroup

	// Attaching a second target group registers the group's existing instances.
	require.NoError(t, b.AttachLoadBalancerTargetGroups("asg-tg-attach", []string{secondTG}))
	assert.Len(t, reg.registeredIDsFor(secondTG), 2)
	assert.Empty(t, reg.registeredIDsFor(testTGArn)) // untouched, already-attached TG is not re-registered

	// Detaching the original target group deregisters the group's instances from it.
	require.NoError(t, b.DetachLoadBalancerTargetGroups("asg-tg-attach", []string{testTGArn}))
	assert.Len(t, reg.deregisteredIDsFor(testTGArn), 2)
	assert.Empty(t, reg.deregisteredIDsFor(secondTG))
}

func TestInMemoryBackend_ELBv2Registrar_RegisterErrorDoesNotFailCall(t *testing.T) {
	t.Parallel()

	b := autoscaling.NewInMemoryBackend()
	t.Cleanup(b.Close)

	reg := &fakeELBv2Registrar{registerErr: assert.AnError}
	b.SetELBv2Registrar(reg)

	// Best-effort registration: a registrar error must not fail the ASG-side
	// operation or leave the group instance list inconsistent.
	newTGGroup(t, b, "asg-reg-err", 2)

	groups, err := b.DescribeAutoScalingGroups([]string{"asg-reg-err"})
	require.NoError(t, err)
	require.Len(t, groups, 1)
	assert.Len(t, groups[0].Instances, 2)
}
