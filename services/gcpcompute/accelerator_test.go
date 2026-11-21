package gcpcompute_test

import (
	"net/http"
	"testing"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/proto"
)

func gpuVM(machine, accel string, count int32) *computepb.Instance {
	spec := vmSpec("gpu")
	spec.MachineType = s(machine)
	spec.Scheduling = &computepb.Scheduling{OnHostMaintenance: s("TERMINATE")}
	spec.GuestAccelerators = []*computepb.AcceleratorConfig{
		{AcceleratorType: s(accel), AcceleratorCount: new(count)},
	}

	return spec
}

func TestInstanceGuestAcceleratorsAtInsert(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mod    func(i *computepb.Instance)
		name   string
		reason string
		status int
	}{
		{name: "t4", mod: func(*computepb.Instance) {}},
		{name: "t4 zone qualified", mod: func(i *computepb.Instance) {
			i.GuestAccelerators[0].AcceleratorType = s("zones/" + zone + "/acceleratorTypes/nvidia-tesla-t4")
		}},
		{name: "v100 x8", mod: func(i *computepb.Instance) {
			i.MachineType = s("n1-standard-96")
			i.GuestAccelerators[0] = &computepb.AcceleratorConfig{
				AcceleratorType: s("nvidia-tesla-v100"), AcceleratorCount: proto.Int32(8),
			}
		}},
		{name: "unsupported count", status: http.StatusBadRequest, reason: "invalid", mod: func(i *computepb.Instance) {
			i.GuestAccelerators[0].AcceleratorCount = proto.Int32(3)
		}},
		{
			name:   "v100 count 8 but t4 type",
			status: http.StatusBadRequest,
			reason: "invalid",
			mod: func(i *computepb.Instance) {
				i.GuestAccelerators[0].AcceleratorCount = proto.Int32(8)
			},
		},
		{name: "zero count", status: http.StatusBadRequest, reason: "invalid", mod: func(i *computepb.Instance) {
			i.GuestAccelerators[0].AcceleratorCount = proto.Int32(0)
		}},
		{name: "unknown type", status: http.StatusNotFound, reason: "notFound", mod: func(i *computepb.Instance) {
			i.GuestAccelerators[0].AcceleratorType = s("nvidia-nope")
		}},
		{name: "e2 machine", status: http.StatusBadRequest, reason: "invalid", mod: func(i *computepb.Instance) {
			i.MachineType = s(e2Medium)
		}},
		{name: "needs terminate", status: http.StatusBadRequest, reason: "invalid", mod: func(i *computepb.Instance) {
			i.Scheduling = nil
		}},
		{
			name:   "too many vcpus for one t4",
			status: http.StatusBadRequest,
			reason: "invalid",
			mod: func(i *computepb.Instance) {
				i.MachineType = s("n1-standard-96")
			},
		},
		{
			name:   "two accelerator types",
			status: http.StatusBadRequest,
			reason: "invalid",
			mod: func(i *computepb.Instance) {
				i.GuestAccelerators = append(
					i.GuestAccelerators,
					&computepb.AcceleratorConfig{
						AcceleratorType:  s("nvidia-tesla-p4"),
						AcceleratorCount: proto.Int32(1),
					},
				)
			},
		},
		{
			name:   "p4 local ssd outside its zones",
			status: http.StatusBadRequest,
			reason: "invalid",
			mod: func(i *computepb.Instance) {
				i.GuestAccelerators[0].AcceleratorType = s("nvidia-tesla-p4")
				i.Disks = append(i.Disks, &computepb.AttachedDisk{Type: s("SCRATCH")})
			},
		},
		{name: "custom n1 at the t4 vcpu ceiling", mod: func(i *computepb.Instance) {
			i.MachineType = s("custom-48-49152")
		}},
		{name: "custom n1 over the t4 vcpu ceiling", status: http.StatusBadRequest, reason: "invalid",
			mod: func(i *computepb.Instance) { i.MachineType = s("custom-50-51200") }},
		{name: "custom n1 at the v100 memory ceiling", mod: func(i *computepb.Instance) {
			i.MachineType = s("custom-12-79872")
			i.GuestAccelerators[0].AcceleratorType = s("nvidia-tesla-v100")
		}},
		{name: "custom n1 over the v100 memory ceiling", status: http.StatusBadRequest, reason: "invalid",
			mod: func(i *computepb.Instance) {
				i.MachineType = s("custom-12-80128")
				i.GuestAccelerators[0].AcceleratorType = s("nvidia-tesla-v100")
			}},
		{name: "legacy p100 in any zone", mod: func(i *computepb.Instance) {
			i.GuestAccelerators[0].AcceleratorType = s("nvidia-tesla-p100")
		}},
		{name: "legacy t4 vws", mod: func(i *computepb.Instance) {
			i.GuestAccelerators[0].AcceleratorType = s("nvidia-tesla-t4-vws")
			i.GuestAccelerators[0].AcceleratorCount = proto.Int32(2)
		}},
		{name: "legacy needs n1", status: http.StatusBadRequest, reason: "invalid", mod: func(i *computepb.Instance) {
			i.GuestAccelerators[0].AcceleratorType = s("nvidia-l4")
			i.MachineType = s(e2Medium)
		}},
		{name: "legacy zero count", status: http.StatusBadRequest, reason: "invalid", mod: func(i *computepb.Instance) {
			i.GuestAccelerators[0].AcceleratorType = s("nvidia-tesla-k80")
			i.GuestAccelerators[0].AcceleratorCount = proto.Int32(0)
		}},
		{name: "t4 local ssd", mod: func(i *computepb.Instance) {
			i.Disks = append(i.Disks, &computepb.AttachedDisk{Type: s("SCRATCH")})
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.baseNet(t)

			spec := gpuVM("n1-standard-1", "nvidia-tesla-t4", 1)
			tt.mod(spec)

			op, err := e.instances(t).Insert(t.Context(),
				&computepb.InsertInstanceRequest{Project: project, Zone: zone, InstanceResource: spec})
			if tt.status != 0 {
				assertAPIError(t, err, tt.status, tt.reason)

				return
			}

			e.done(t, op, err)

			got := e.getVM(t, "gpu").GetGuestAccelerators()
			require.Len(t, got, 1)
			assert.Contains(t, got[0].GetAcceleratorType(), "/zones/"+zone+"/acceleratorTypes/")
			assert.Equal(t, spec.GetGuestAccelerators()[0].GetAcceleratorCount(), got[0].GetAcceleratorCount())
		})
	}
}

func TestAcceleratorTypes(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	c := client(t, e, compute.NewAcceleratorTypesRESTClient)

	got, err := c.Get(t.Context(), &computepb.GetAcceleratorTypeRequest{
		Project: project, Zone: zone, AcceleratorType: "nvidia-tesla-v100",
	})
	require.NoError(t, err)
	assert.Equal(t, int32(8), got.GetMaximumCardsPerInstance())
	assert.Contains(t, got.GetSelfLink(), "/zones/"+zone+"/acceleratorTypes/nvidia-tesla-v100")

	for _, tt := range []struct{ zone, name string }{
		{"us-east1-d", "nvidia-tesla-t4"},
		{zone, "nvidia-nope"},
		{"nowhere-a", "nvidia-tesla-t4"},
	} {
		_, err = c.Get(t.Context(), &computepb.GetAcceleratorTypeRequest{
			Project: project, Zone: tt.zone, AcceleratorType: tt.name,
		})
		assertAPIError(t, err, http.StatusNotFound, "notFound")
	}

	names := map[string]bool{}
	it := c.List(t.Context(), &computepb.ListAcceleratorTypesRequest{Project: project, Zone: zone})

	for {
		a, nerr := it.Next()
		if nerr != nil {
			require.ErrorIs(t, nerr, iterator.Done)

			break
		}

		names[a.GetName()] = true
	}

	assert.Equal(t, map[string]bool{"nvidia-tesla-t4": true, "nvidia-tesla-p4": true, "nvidia-tesla-v100": true}, names)

	scopes := map[string]int{}
	agg := c.AggregatedList(t.Context(), &computepb.AggregatedListAcceleratorTypesRequest{Project: project})

	for {
		kv, nerr := agg.Next()
		if nerr != nil {
			require.ErrorIs(t, nerr, iterator.Done)

			break
		}

		scopes[kv.Key] = len(kv.Value.GetAcceleratorTypes())
	}

	assert.Equal(t, 3, scopes["zones/"+zone])
}
