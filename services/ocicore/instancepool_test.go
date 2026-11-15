package ocicore_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (e *env) mkConfig(
	t *testing.T,
	mod func(*core.InstanceConfigurationLaunchInstanceDetails),
) core.InstanceConfiguration {
	t.Helper()

	l := &core.InstanceConfigurationLaunchInstanceDetails{
		Shape:       new("VM.Standard.E4.Flex"),
		ShapeConfig: &core.InstanceConfigurationLaunchInstanceShapeConfigDetails{Ocpus: new(float32(1))},
		SourceDetails: core.InstanceConfigurationInstanceSourceViaImageDetails{
			ImageId: e.imageID(t, "Oracle Linux", "9", false),
		},
	}
	if mod != nil {
		mod(l)
	}

	r, err := e.mgmt.CreateInstanceConfiguration(t.Context(), core.CreateInstanceConfigurationRequest{
		CreateInstanceConfiguration: core.CreateInstanceConfigurationDetails{
			CompartmentId: new(testComp), DisplayName: new("cfg"),
			InstanceDetails: core.ComputeInstanceDetails{LaunchDetails: l},
		},
	})
	require.NoError(t, err)

	return r.InstanceConfiguration
}

func TestInstanceConfigurationValidation(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)

	tests := []struct {
		mod  func(*core.InstanceConfigurationLaunchInstanceDetails)
		name string
	}{
		{
			name: "unknown shape",
			mod:  func(l *core.InstanceConfigurationLaunchInstanceDetails) { l.Shape = new("VM.Nope") },
		},
		{
			name: "flex without ocpus",
			mod:  func(l *core.InstanceConfigurationLaunchInstanceDetails) { l.ShapeConfig = nil },
		},
		{name: "unknown image", mod: func(l *core.InstanceConfigurationLaunchInstanceDetails) {
			l.SourceDetails = core.InstanceConfigurationInstanceSourceViaImageDetails{
				ImageId: new("ocid1.image.oc1.iad.nope"),
			}
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			l := &core.InstanceConfigurationLaunchInstanceDetails{
				Shape:       new("VM.Standard.E4.Flex"),
				ShapeConfig: &core.InstanceConfigurationLaunchInstanceShapeConfigDetails{Ocpus: new(float32(1))},
				SourceDetails: core.InstanceConfigurationInstanceSourceViaImageDetails{
					ImageId: e.imageID(t, "Oracle Linux", "9", false),
				},
			}
			tc.mod(l)

			_, err := e.mgmt.CreateInstanceConfiguration(t.Context(), core.CreateInstanceConfigurationRequest{
				CreateInstanceConfiguration: core.CreateInstanceConfigurationDetails{
					CompartmentId: new(testComp), InstanceDetails: core.ComputeInstanceDetails{LaunchDetails: l},
				},
			})
			assert.GreaterOrEqual(t, svcErr(t, err).GetHTTPStatusCode(), http.StatusBadRequest)
		})
	}
}

func TestInstancePoolScaling(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	f := e.fixture(t)
	cfg := e.mkConfig(t, nil)

	place := []core.CreateInstancePoolPlacementConfigurationDetails{
		{AvailabilityDomain: new(testAD), PrimarySubnetId: f.pub.Id},
		{AvailabilityDomain: new(testAD2), PrimarySubnetId: f.pub.Id},
	}

	_, err := e.mgmt.CreateInstancePool(
		ctx,
		core.CreateInstancePoolRequest{
			CompartmentId: new(testComp), InstanceConfigurationId: cfg.Id, Size: new(1), PlacementConfigurations: place,
			InstanceDisplayNameFormatter: new("web"),
		},
	)
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(), "the formatter needs ${launchCount}")

	pool, err := e.mgmt.CreateInstancePool(
		ctx,
		core.CreateInstancePoolRequest{
			CompartmentId: new(testComp), InstanceConfigurationId: cfg.Id, Size: new(3), PlacementConfigurations: place,
			DisplayName: new("pool"), InstanceDisplayNameFormatter: new("web-${launchCount}"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, core.InstancePoolLifecycleStateProvisioning, pool.LifecycleState)
	assert.Equal(t, 3, *pool.CurrentSize)
	e.settle()

	members, err := e.mgmt.ListInstancePoolInstances(ctx, core.ListInstancePoolInstancesRequest{
		CompartmentId: new(testComp), InstancePoolId: pool.Id,
	})
	require.NoError(t, err)
	require.Len(t, members.Items, 3)

	ads := map[string]int{}
	names := map[string]bool{}

	for _, m := range members.Items {
		ads[*m.AvailabilityDomain]++
		names[*m.DisplayName] = true
	}

	assert.Equal(t, map[string]int{testAD: 2, testAD2: 1}, ads, "instances spread over the placement configurations")
	assert.Equal(t, map[string]bool{"web-1": true, "web-2": true, "web-3": true}, names)

	_, err = e.mgmt.UpdateInstancePool(ctx, core.UpdateInstancePoolRequest{
		InstancePoolId: pool.Id,
		Size:           new(1),
	})
	require.NoError(t, err)
	e.settle()

	got, err := e.mgmt.GetInstancePool(ctx, core.GetInstancePoolRequest{InstancePoolId: pool.Id})
	require.NoError(t, err)
	assert.Equal(t, 1, *got.CurrentSize)
	assert.Equal(t, core.InstancePoolLifecycleStateRunning, got.LifecycleState)

	_, err = e.mgmt.DeleteInstanceConfiguration(ctx, core.DeleteInstanceConfigurationRequest{
		InstanceConfigurationId: cfg.Id,
	})
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode(), "a pool uses the configuration")

	remaining, err := e.mgmt.ListInstancePoolInstances(ctx, core.ListInstancePoolInstancesRequest{
		CompartmentId: new(testComp), InstancePoolId: pool.Id,
	})
	require.NoError(t, err)
	require.Len(t, remaining.Items, 1)

	_, err = e.compute.TerminateInstance(ctx, core.TerminateInstanceRequest{InstanceId: remaining.Items[0].Id})
	require.NoError(t, err)
	e.settle()

	healed, err := e.mgmt.GetInstancePool(ctx, core.GetInstancePoolRequest{InstancePoolId: pool.Id})
	require.NoError(t, err)
	assert.Equal(t, 1, *healed.CurrentSize, "the pool replaces a terminated instance")

	_, err = e.mgmt.StopInstancePool(ctx, core.StopInstancePoolRequest{InstancePoolId: pool.Id})
	require.NoError(t, err)
	e.settle()

	stopped, err := e.mgmt.GetInstancePool(ctx, core.GetInstancePoolRequest{InstancePoolId: pool.Id})
	require.NoError(t, err)
	assert.Equal(t, core.InstancePoolLifecycleStateStopped, stopped.LifecycleState)

	_, err = e.mgmt.TerminateInstancePool(ctx, core.TerminateInstancePoolRequest{InstancePoolId: pool.Id})
	require.NoError(t, err)
	e.settle()

	gone, err := e.mgmt.GetInstancePool(ctx, core.GetInstancePoolRequest{InstancePoolId: pool.Id})
	require.NoError(t, err)
	assert.Equal(t, core.InstancePoolLifecycleStateTerminated, gone.LifecycleState)

	left, err := e.compute.ListInstances(ctx, core.ListInstancesRequest{
		CompartmentId: new(testComp), LifecycleState: core.InstanceLifecycleStateStopped,
	})
	require.NoError(t, err)
	assert.Empty(t, left.Items)
}

func TestInstancePoolAttachDetach(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	f := e.fixture(t)
	cfg := e.mkConfig(t, nil)

	pool, err := e.mgmt.CreateInstancePool(
		ctx,
		core.CreateInstancePoolRequest{
			CompartmentId: new(testComp), InstanceConfigurationId: cfg.Id, Size: new(1),
			PlacementConfigurations: []core.CreateInstancePoolPlacementConfigurationDetails{
				{AvailabilityDomain: new(testAD), PrimarySubnetId: f.pub.Id},
			},
		},
	)
	require.NoError(t, err)
	e.settle()

	extra := e.mustLaunch(t, f.pub)
	e.settle()

	att, err := e.mgmt.AttachInstancePoolInstance(ctx, core.AttachInstancePoolInstanceRequest{
		InstancePoolId: pool.Id,
		InstanceId:     extra.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, core.InstancePoolInstanceLifecycleStateActive, att.LifecycleState)

	_, err = e.mgmt.AttachInstancePoolInstance(ctx, core.AttachInstancePoolInstanceRequest{
		InstancePoolId: pool.Id,
		InstanceId:     extra.Id,
	})
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode())

	got, err := e.mgmt.GetInstancePool(ctx, core.GetInstancePoolRequest{InstancePoolId: pool.Id})
	require.NoError(t, err)
	assert.Equal(t, 2, *got.Size)

	_, err = e.mgmt.DetachInstancePoolInstance(ctx, core.DetachInstancePoolInstanceRequest{
		InstancePoolId: pool.Id, InstanceId: extra.Id, IsDecrementSize: new(true), IsAutoTerminate: new(true),
	})
	require.NoError(t, err)
	e.settle()

	got, err = e.mgmt.GetInstancePool(ctx, core.GetInstancePoolRequest{InstancePoolId: pool.Id})
	require.NoError(t, err)
	assert.Equal(t, 1, *got.Size)
	assert.Equal(t, 1, *got.CurrentSize)

	gone, err := e.compute.GetInstance(ctx, core.GetInstanceRequest{InstanceId: extra.Id})
	require.NoError(t, err)
	assert.Equal(t, core.InstanceLifecycleStateTerminated, gone.LifecycleState)
}

func TestLaunchInstanceConfigurationAndFromInstance(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	f := e.fixture(t)

	cfg := e.mkConfig(t, func(l *core.InstanceConfigurationLaunchInstanceDetails) {
		l.CreateVnicDetails = &core.InstanceConfigurationCreateVnicDetails{SubnetId: f.pub.Id}
		l.DisplayName = new("tmpl")
	})

	inst, err := e.mgmt.LaunchInstanceConfiguration(ctx, core.LaunchInstanceConfigurationRequest{
		InstanceConfigurationId: cfg.Id,
		InstanceConfiguration: core.ComputeInstanceDetails{
			LaunchDetails: &core.InstanceConfigurationLaunchInstanceDetails{
				AvailabilityDomain: new(testAD), CompartmentId: new(testComp),
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "VM.Standard.E4.Flex", *inst.Shape)
	assert.Equal(t, "tmpl", *inst.DisplayName)

	from, err := e.mgmt.CreateInstanceConfiguration(ctx, core.CreateInstanceConfigurationRequest{
		CreateInstanceConfiguration: core.CreateInstanceConfigurationFromInstanceDetails{
			CompartmentId: new(testComp), InstanceId: inst.Id,
		},
	})
	require.NoError(t, err)

	cd, ok := from.InstanceDetails.(core.ComputeInstanceDetails)
	require.True(t, ok)
	assert.Equal(t, "VM.Standard.E4.Flex", *cd.LaunchDetails.Shape)
}
