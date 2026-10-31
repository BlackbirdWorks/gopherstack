package gcpcompute_test

import (
	"net/http"
	"testing"
	"time"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func (e *env) runningVM(t *testing.T) {
	t.Helper()
	e.baseNet(t)
	e.vm(t, vmSpec("vm1"))
}

func (e *env) stopVM(t *testing.T) {
	t.Helper()

	op, err := e.instances(t).
		Stop(t.Context(), &computepb.StopInstanceRequest{Project: project, Zone: zone, Instance: "vm1"})
	e.done(t, op, err)
	require.Equal(t, stTerm, e.getVM(t, "vm1").GetStatus())
}

func (e *env) update(t *testing.T, name string, res *computepb.Instance, minimal, most string) error {
	t.Helper()

	req := &computepb.UpdateInstanceRequest{Project: project, Zone: zone, Instance: name, InstanceResource: res}
	if minimal != "" {
		req.MinimalAction = &minimal
	}

	if most != "" {
		req.MostDisruptiveAllowedAction = &most
	}

	_, err := e.instances(t).Update(t.Context(), req)

	return err
}

func TestInstanceSuspendResume(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.runningVM(t)
	timing := gcpcomputeTiming()

	assertAPIError(t, e.resumeErr(t, "vm1"), http.StatusBadRequest, "resourceNotReady")

	op, err := e.instances(t).
		Suspend(t.Context(), &computepb.SuspendInstanceRequest{Project: project, Zone: zone, Instance: "vm1"})
	require.NoError(t, err)
	assert.Equal(t, "suspend", op.Proto().GetOperationType())
	assert.Equal(t, "SUSPENDING", e.getVM(t, "vm1").GetStatus())
	assertAPIError(t, e.resumeErr(t, "vm1"), http.StatusBadRequest, "resourceNotReady")

	e.clock.Advance(timing.Stop)
	assert.Equal(t, "SUSPENDED", e.getVM(t, "vm1").GetStatus())
	assert.NotEmpty(t, e.getVM(t, "vm1").GetLastSuspendedTimestamp())

	_, err = e.instances(t).
		Start(t.Context(), &computepb.StartInstanceRequest{Project: project, Zone: zone, Instance: "vm1"})
	assertAPIError(t, err, http.StatusBadRequest, "resourceNotReady")
	_, err = e.instances(t).
		Suspend(t.Context(), &computepb.SuspendInstanceRequest{Project: project, Zone: zone, Instance: "vm1"})
	assertAPIError(t, err, http.StatusBadRequest, "resourceNotReady")

	require.NoError(t, e.resumeErr(t, "vm1"))

	for _, step := range []struct {
		status string
		after  time.Duration
	}{
		{"PROVISIONING", 0},
		{"STAGING", timing.Provision},
		{"RUNNING", timing.Stage - timing.Provision},
	} {
		e.clock.Advance(step.after)
		assert.Equal(t, step.status, e.getVM(t, "vm1").GetStatus())
	}
}

func (e *env) resumeErr(t *testing.T, name string) error {
	t.Helper()

	op, err := e.instances(t).
		Resume(t.Context(), &computepb.ResumeInstanceRequest{Project: project, Zone: zone, Instance: name})
	if err != nil {
		return err
	}

	_ = op

	return nil
}

func TestInstanceSuspendPreconditions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		prep func(t *testing.T, e *env)
		name string
		spot bool
	}{
		{name: "terminated", prep: func(t *testing.T, e *env) { t.Helper(); e.stopVM(t) }},
		{name: "spot", spot: true, prep: func(*testing.T, *env) {}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.baseNet(t)

			spec := vmSpec("vm1")
			if tt.spot {
				spec.Scheduling = &computepb.Scheduling{
					ProvisioningModel:         s("SPOT"),
					InstanceTerminationAction: s("STOP"),
				}
			}

			e.vm(t, spec)
			tt.prep(t, e)

			_, err := e.instances(t).
				Suspend(t.Context(), &computepb.SuspendInstanceRequest{Project: project, Zone: zone, Instance: "vm1"})
			assertAPIError(t, err, http.StatusBadRequest, "")
		})
	}
}

func TestInstanceStoppedOnlyMutations(t *testing.T) {
	t.Parallel()

	type call func(t *testing.T, e *env) error

	do := func(t *testing.T, e *env, mk func(c *compute.InstancesClient) (*compute.Operation, error)) error {
		t.Helper()

		op, err := mk(e.instances(t))

		return e.finish(t, op, err)
	}

	tests := []struct {
		call   call
		verify func(t *testing.T, i *computepb.Instance)
		name   string
	}{
		{
			name: "service account",
			call: func(t *testing.T, e *env) error {
				t.Helper()

				return do(t, e, func(c *compute.InstancesClient) (*compute.Operation, error) {
					return c.SetServiceAccount(t.Context(), &computepb.SetServiceAccountInstanceRequest{
						Project: project, Zone: zone, Instance: "vm1",
						InstancesSetServiceAccountRequestResource: &computepb.InstancesSetServiceAccountRequest{
							Email: s(
								"sa@proj.iam.gserviceaccount.com",
							), Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"},
						},
					})
				})
			},
			verify: func(t *testing.T, i *computepb.Instance) {
				t.Helper()
				require.Len(t, i.GetServiceAccounts(), 1)
				assert.Equal(t, "sa@proj.iam.gserviceaccount.com", i.GetServiceAccounts()[0].GetEmail())
			},
		},
		{
			name: "min cpu platform",
			call: func(t *testing.T, e *env) error {
				t.Helper()

				return do(t, e, func(c *compute.InstancesClient) (*compute.Operation, error) {
					return c.SetMinCpuPlatform(t.Context(), &computepb.SetMinCpuPlatformInstanceRequest{
						Project: project, Zone: zone, Instance: "vm1",
						InstancesSetMinCpuPlatformRequestResource: &computepb.InstancesSetMinCpuPlatformRequest{
							MinCpuPlatform: s("Intel Skylake"),
						},
					})
				})
			},
			verify: func(t *testing.T, i *computepb.Instance) {
				t.Helper()
				assert.Equal(t, "Intel Skylake", i.GetMinCpuPlatform())
			},
		},
		{
			name: "shielded config",
			call: func(t *testing.T, e *env) error {
				t.Helper()

				return do(t, e, func(c *compute.InstancesClient) (*compute.Operation, error) {
					return c.UpdateShieldedInstanceConfig(
						t.Context(),
						&computepb.UpdateShieldedInstanceConfigInstanceRequest{
							Project: project, Zone: zone, Instance: "vm1",
							ShieldedInstanceConfigResource: &computepb.ShieldedInstanceConfig{
								EnableSecureBoot: new(true),
								EnableVtpm:       new(true),
							},
						},
					)
				})
			},
			verify: func(t *testing.T, i *computepb.Instance) {
				t.Helper()
				assert.True(t, i.GetShieldedInstanceConfig().GetEnableSecureBoot())
				assert.True(
					t,
					i.GetShieldedInstanceConfig().GetEnableIntegrityMonitoring(),
					"PATCH keeps unspecified fields",
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.runningVM(t)

			assertAPIError(t, tt.call(t, e), http.StatusBadRequest, "resourceNotReady")

			e.stopVM(t)
			require.NoError(t, tt.call(t, e))
			tt.verify(t, e.getVM(t, "vm1"))
		})
	}
}

func TestInstanceStoppedOnlyValidation(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.runningVM(t)
	e.stopVM(t)

	c := e.instances(t)

	_, err := c.SetServiceAccount(t.Context(), &computepb.SetServiceAccountInstanceRequest{
		Project: project, Zone: zone, Instance: "vm1",
		InstancesSetServiceAccountRequestResource: &computepb.InstancesSetServiceAccountRequest{
			Email: s("not-an-email"),
		},
	})
	assertAPIError(t, err, http.StatusBadRequest, "invalid")

	_, err = c.SetMinCpuPlatform(t.Context(), &computepb.SetMinCpuPlatformInstanceRequest{
		Project: project, Zone: zone, Instance: "vm1",
		InstancesSetMinCpuPlatformRequestResource: &computepb.InstancesSetMinCpuPlatformRequest{
			MinCpuPlatform: s("Quantum 9000"),
		},
	})
	assertAPIError(t, err, http.StatusBadRequest, "invalid")

	_, err = c.SetServiceAccount(t.Context(), &computepb.SetServiceAccountInstanceRequest{
		Project: project, Zone: zone, Instance: "ghost",
		InstancesSetServiceAccountRequestResource: &computepb.InstancesSetServiceAccountRequest{},
	})
	assertAPIError(t, err, http.StatusNotFound, "notFound")
}

func TestInstanceSetScheduling(t *testing.T) {
	t.Parallel()

	set := func(t *testing.T, e *env, sc *computepb.Scheduling) error {
		t.Helper()

		op, err := e.instances(t).SetScheduling(t.Context(), &computepb.SetSchedulingInstanceRequest{
			Project: project, Zone: zone, Instance: "vm1", SchedulingResource: sc,
		})

		return e.finish(t, op, err)
	}

	e := newEnv(t)
	e.runningVM(t)

	spot := &computepb.Scheduling{ProvisioningModel: s("SPOT"), InstanceTerminationAction: s("STOP")}
	assertAPIError(t, set(t, e, spot), http.StatusBadRequest, "resourceNotReady")
	assertAPIError(
		t,
		set(t, e, &computepb.Scheduling{AutomaticRestart: new(false)}),
		http.StatusBadRequest,
		"resourceNotReady",
	)

	e.stopVM(t)
	require.NoError(
		t,
		set(t, e, &computepb.Scheduling{AutomaticRestart: new(false), OnHostMaintenance: s("TERMINATE")}),
	)

	sc := e.getVM(t, "vm1").GetScheduling()
	assert.False(t, sc.GetAutomaticRestart())
	assert.Equal(t, "TERMINATE", sc.GetOnHostMaintenance())

	require.NoError(t, set(t, e, spot))
	assert.Equal(t, "SPOT", e.getVM(t, "vm1").GetScheduling().GetProvisioningModel())
	assertAPIError(
		t,
		set(t, e, &computepb.Scheduling{ProvisioningModel: s("SPOT"), AutomaticRestart: new(true)}),
		http.StatusBadRequest,
		"invalid",
	)
}

func TestInstanceUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		res      *computepb.Instance
		verify   func(t *testing.T, i *computepb.Instance)
		name     string
		minimal  string
		most     string
		status   string
		errToken string
		errCode  int
		stopped  bool
	}{
		{
			name: "description no effect", res: &computepb.Instance{Description: s("new")}, status: stRunning,
			verify: func(t *testing.T, i *computepb.Instance) {
				t.Helper()
				assert.Equal(t, "new", i.GetDescription())
			},
		},
		{
			name: "deletion protection", res: &computepb.Instance{DeletionProtection: new(true)}, status: stRunning,
			verify: func(t *testing.T, i *computepb.Instance) { t.Helper(); assert.True(t, i.GetDeletionProtection()) },
		},
		{
			name: "can ip forward restarts", res: &computepb.Instance{CanIpForward: new(true)}, status: "PROVISIONING",
			verify: func(t *testing.T, i *computepb.Instance) { t.Helper(); assert.True(t, i.GetCanIpForward()) },
		},
		{
			name:    "minimal restart on description",
			res:     &computepb.Instance{Description: s("d")},
			minimal: "RESTART",
			status:  "PROVISIONING",
		},
		{
			name:    "minimal refresh keeps running",
			res:     &computepb.Instance{Description: s("d")},
			minimal: "REFRESH",
			status:  stRunning,
		},
		{
			name:   "most disruptive allows",
			res:    &computepb.Instance{CanIpForward: new(true)},
			most:   "RESTART",
			status: "PROVISIONING",
		},
		{
			name: "most disruptive too low", res: &computepb.Instance{CanIpForward: new(true)}, most: "NO_EFFECT",
			status: stRunning, errCode: http.StatusBadRequest, errToken: "invalid",
		},
		{
			name: "minimal above most", res: &computepb.Instance{Description: s("d")}, minimal: "RESTART", most: "REFRESH",
			status: stRunning, errCode: http.StatusBadRequest, errToken: "invalid",
		},
		{
			name:     "bad minimal",
			res:      &computepb.Instance{},
			minimal:  "REBOOT",
			status:   stRunning,
			errCode:  http.StatusBadRequest,
			errToken: "invalid",
		},
		{
			name:     "bad most",
			res:      &computepb.Instance{},
			most:     "REBOOT",
			status:   stRunning,
			errCode:  http.StatusBadRequest,
			errToken: "invalid",
		},
		{
			name:   "immutable machine type",
			res:    &computepb.Instance{MachineType: s("zones/" + zone + "/machineTypes/e2-standard-4")},
			status: stRunning, errCode: http.StatusBadRequest, errToken: "invalid",
		},
		{
			name:   "same machine type is fine",
			res:    &computepb.Instance{MachineType: s(e2Medium), Description: s("x")},
			status: stRunning,
		},
		{
			name:     "stale fingerprint",
			res:      &computepb.Instance{Fingerprint: s("stale")},
			status:   stRunning,
			errCode:  http.StatusPreconditionFailed,
			errToken: "conditionNotMet",
		},
		{
			name:   "stale label fingerprint",
			res:    &computepb.Instance{Labels: map[string]string{"a": "b"}, LabelFingerprint: s("stale")},
			status: stRunning, errCode: http.StatusPreconditionFailed, errToken: "conditionNotMet",
		},
		{
			name: "labels", res: &computepb.Instance{Labels: map[string]string{"env": "x"}}, status: stRunning,
			verify: func(t *testing.T, i *computepb.Instance) {
				t.Helper()
				assert.Equal(t, map[string]string{"env": "x"}, i.GetLabels())
			},
		},
		{
			name:    "restart change while stopped does not start",
			res:     &computepb.Instance{CanIpForward: new(true)},
			stopped: true,
			status:  stTerm,
			verify:  func(t *testing.T, i *computepb.Instance) { t.Helper(); assert.True(t, i.GetCanIpForward()) },
		},
		{
			name: "bad platform", res: &computepb.Instance{MinCpuPlatform: s("Nope")}, status: stRunning,
			errCode: http.StatusBadRequest, errToken: "invalid",
		},
		{
			name: "scheduling restart", status: "PROVISIONING",
			res: &computepb.Instance{
				Scheduling: &computepb.Scheduling{ProvisioningModel: s("SPOT"), InstanceTerminationAction: s("STOP")},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.runningVM(t)

			if tt.stopped {
				e.stopVM(t)
			}

			before := e.getVM(t, "vm1")
			err := e.update(t, "vm1", tt.res, tt.minimal, tt.most)

			if tt.errCode != 0 {
				assertAPIError(t, err, tt.errCode, tt.errToken)
				assert.Equal(t, before.GetFingerprint(), e.getVM(t, "vm1").GetFingerprint())
			} else {
				require.NoError(t, err)
				assert.NotEqual(t, before.GetFingerprint(), e.getVM(t, "vm1").GetFingerprint())
			}

			got := e.getVM(t, "vm1")
			assert.Equal(t, tt.status, got.GetStatus())

			if tt.verify != nil && tt.errCode == 0 {
				tt.verify(t, got)
			}
		})
	}
}

func TestInstanceUpdateRestartCompletes(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.runningVM(t)
	require.NoError(t, e.update(t, "vm1", &computepb.Instance{CanIpForward: new(true)}, "", ""))
	assert.Equal(t, "PROVISIONING", e.getVM(t, "vm1").GetStatus())

	e.settle()
	got := e.getVM(t, "vm1")
	assert.Equal(t, stRunning, got.GetStatus())
	assert.NotEmpty(t, got.GetLastStopTimestamp())
}

func TestInstanceIntegrityPolicy(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.runningVM(t)

	set := func() error {
		op, err := e.instances(t).
			SetShieldedInstanceIntegrityPolicy(t.Context(), &computepb.SetShieldedInstanceIntegrityPolicyInstanceRequest{
				Project: project, Zone: zone, Instance: "vm1",
				ShieldedInstanceIntegrityPolicyResource: &computepb.ShieldedInstanceIntegrityPolicy{
					UpdateAutoLearnPolicy: new(true),
				},
			})

		return e.finish(t, op, err)
	}

	require.NoError(t, set())
	assert.True(t, e.getVM(t, "vm1").GetShieldedInstanceIntegrityPolicy().GetUpdateAutoLearnPolicy())

	e.stopVM(t)
	assertAPIError(t, set(), http.StatusBadRequest, "resourceNotReady")
}

func TestInstanceSerialPortAndGuestAttributes(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.runningVM(t)

	tests := []struct {
		port    *int32
		name    string
		errCode int
	}{
		{name: "default port"},
		{name: "port 4", port: proto.Int32(4)},
		{name: "port 0", port: proto.Int32(0), errCode: http.StatusBadRequest},
		{name: "port 5", port: proto.Int32(5), errCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := e.instances(t).GetSerialPortOutput(t.Context(), &computepb.GetSerialPortOutputInstanceRequest{
				Project: project, Zone: zone, Instance: "vm1", Port: tt.port,
			})

			if tt.errCode != 0 {
				assertAPIError(t, err, tt.errCode, "invalid")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "compute#serialPortOutput", out.GetKind())
			assert.Empty(t, out.GetContents())
			assert.Zero(t, out.GetStart())
			assert.Zero(t, out.GetNext())
			assert.Contains(t, out.GetSelfLink(), "/instances/vm1/serialPort")
		})
	}

	t.Run("missing instance", func(t *testing.T) {
		t.Parallel()

		_, err := e.instances(t).GetSerialPortOutput(t.Context(), &computepb.GetSerialPortOutputInstanceRequest{
			Project: project, Zone: zone, Instance: "ghost",
		})
		assertAPIError(t, err, http.StatusNotFound, "notFound")
	})

	t.Run("guest attributes", func(t *testing.T) {
		t.Parallel()

		_, err := e.instances(t).GetGuestAttributes(t.Context(), &computepb.GetGuestAttributesInstanceRequest{
			Project: project, Zone: zone, Instance: "vm1", QueryPath: s("hello/"),
		})
		assertAPIError(t, err, http.StatusNotFound, "notFound")

		_, err = e.instances(t).GetGuestAttributes(t.Context(), &computepb.GetGuestAttributesInstanceRequest{
			Project: project, Zone: zone, Instance: "ghost",
		})
		assertAPIError(t, err, http.StatusNotFound, "notFound")
	})
}
