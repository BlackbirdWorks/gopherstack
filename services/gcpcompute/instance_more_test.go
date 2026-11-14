package gcpcompute_test

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/proto"
)

func (e *env) stopVM1(t *testing.T) {
	t.Helper()

	op, err := e.instances(t).
		Stop(t.Context(), &computepb.StopInstanceRequest{Project: project, Zone: zone, Instance: "vm1"})
	e.done(t, op, err)
}

func TestSetName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		current string
		newName string
		reason  string
		status  int
		stop    bool
	}{
		{name: "renames stopped", stop: true, current: "vm1", newName: "renamed"},
		{
			name:    "running is not ready",
			current: "vm1",
			newName: "renamed",
			status:  http.StatusBadRequest,
			reason:  "resourceNotReady",
		},
		{
			name:    "wrong current name",
			stop:    true,
			current: "other",
			newName: "renamed",
			status:  http.StatusBadRequest,
			reason:  "invalid",
		},
		{
			name:    "bad new name",
			stop:    true,
			current: "vm1",
			newName: "Bad_Name",
			status:  http.StatusBadRequest,
			reason:  "invalid",
		},
		{
			name:    "taken",
			stop:    true,
			current: "vm1",
			newName: "vm2",
			status:  http.StatusConflict,
			reason:  "alreadyExists",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.baseNet(t)
			e.vm(t, vmSpec("vm1"))
			e.vm(t, vmSpec("vm2"))

			if tt.stop {
				e.stopVM1(t)
			}

			op, err := e.instances(t).SetName(t.Context(), &computepb.SetNameInstanceRequest{
				Project: project, Zone: zone, Instance: "vm1",
				InstancesSetNameRequestResource: &computepb.InstancesSetNameRequest{
					CurrentName: s(tt.current), Name: s(tt.newName),
				},
			})
			if tt.status != 0 {
				assertAPIError(t, err, tt.status, tt.reason)

				return
			}

			e.done(t, op, err)

			got := e.getVM(t, tt.newName)
			assert.Contains(t, got.GetSelfLink(), "/instances/"+tt.newName)

			_, err = e.instances(t).
				Get(t.Context(), &computepb.GetInstanceRequest{Project: project, Zone: zone, Instance: "vm1"})
			assertAPIError(t, err, http.StatusNotFound, "notFound")
		})
	}
}

func TestSetDiskAutoDeleteAndDisplayDevice(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)

	spec := vmSpec("vm1")
	spec.Disks[0].DeviceName = s("boot")
	e.vm(t, spec)

	op, err := e.instances(t).SetDiskAutoDelete(t.Context(), &computepb.SetDiskAutoDeleteInstanceRequest{
		Project: project, Zone: zone, Instance: "vm1", DeviceName: "boot", AutoDelete: false,
	})
	e.done(t, op, err)
	assert.False(t, e.getVM(t, "vm1").GetDisks()[0].GetAutoDelete())

	_, err = e.instances(t).SetDiskAutoDelete(t.Context(), &computepb.SetDiskAutoDeleteInstanceRequest{
		Project: project, Zone: zone, Instance: "vm1", DeviceName: "nope", AutoDelete: true,
	})
	assertAPIError(t, err, http.StatusBadRequest, "invalid")

	_, err = e.instances(t).UpdateDisplayDevice(t.Context(), &computepb.UpdateDisplayDeviceInstanceRequest{
		Project:               project,
		Zone:                  zone,
		Instance:              "vm1",
		DisplayDeviceResource: &computepb.DisplayDevice{EnableDisplay: new(true)},
	})
	assertAPIError(t, err, http.StatusBadRequest, "resourceNotReady")

	e.stopVM1(t)

	op, err = e.instances(t).UpdateDisplayDevice(t.Context(), &computepb.UpdateDisplayDeviceInstanceRequest{
		Project:               project,
		Zone:                  zone,
		Instance:              "vm1",
		DisplayDeviceResource: &computepb.DisplayDevice{EnableDisplay: new(true)},
	})
	e.done(t, op, err)
	assert.True(t, e.getVM(t, "vm1").GetDisplayDevice().GetEnableDisplay())
}

func TestSetMachineResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		machine string
		accel   string
		count   int32
		status  int
	}{
		{name: "n1 with gpu", machine: "n1-standard-1", accel: "nvidia-tesla-t4", count: 1},
		{name: "e2 rejects gpu", machine: e2Medium, accel: "nvidia-tesla-t4", count: 1, status: http.StatusBadRequest},
		{
			name:    "unknown accelerator",
			machine: "n1-standard-1",
			accel:   "nvidia-nope",
			count:   1,
			status:  http.StatusBadRequest,
		},
		{
			name:    "zero count",
			machine: "n1-standard-1",
			accel:   "nvidia-tesla-t4",
			count:   0,
			status:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.baseNet(t)

			spec := vmSpec("vm1")
			spec.MachineType = s(tt.machine)
			spec.Scheduling = &computepb.Scheduling{OnHostMaintenance: s("TERMINATE")}
			e.vm(t, spec)

			_, err := e.instances(t).SetMachineResources(t.Context(), &computepb.SetMachineResourcesInstanceRequest{
				Project: project, Zone: zone, Instance: "vm1",
				InstancesSetMachineResourcesRequestResource: &computepb.InstancesSetMachineResourcesRequest{
					GuestAccelerators: []*computepb.AcceleratorConfig{
						{AcceleratorType: s(tt.accel), AcceleratorCount: new(tt.count)},
					},
				},
			})
			assertAPIError(t, err, http.StatusBadRequest, "resourceNotReady")

			e.stopVM1(t)

			op, err := e.instances(t).SetMachineResources(t.Context(), &computepb.SetMachineResourcesInstanceRequest{
				Project: project, Zone: zone, Instance: "vm1",
				InstancesSetMachineResourcesRequestResource: &computepb.InstancesSetMachineResourcesRequest{
					GuestAccelerators: []*computepb.AcceleratorConfig{
						{AcceleratorType: s(tt.accel), AcceleratorCount: new(tt.count)},
					},
				},
			})
			if tt.status != 0 {
				assertAPIError(t, err, tt.status, "invalid")

				return
			}

			e.done(t, op, err)

			accs := e.getVM(t, "vm1").GetGuestAccelerators()
			require.Len(t, accs, 1)
			assert.Contains(t, accs[0].GetAcceleratorType(), "/acceleratorTypes/"+tt.accel)
			assert.Equal(t, tt.count, accs[0].GetAcceleratorCount())
		})
	}
}

func TestUpdateAccessConfigAndNetworkInterface(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)
	e.vm(t, vmSpec("vm1"))

	addr, err := e.addresses(t).Insert(t.Context(), &computepb.InsertAddressRequest{
		Project: project, Region: region, AddressResource: &computepb.Address{Name: s("static")},
	})
	e.done(t, addr, err)

	reserved, err := e.addresses(t).
		Get(t.Context(), &computepb.GetAddressRequest{Project: project, Region: region, Address: "static"})
	require.NoError(t, err)

	vm := e.getVM(t, "vm1")
	oldIP := vm.GetNetworkInterfaces()[0].GetAccessConfigs()[0].GetNatIP()
	require.NotEqual(t, reserved.GetAddress(), oldIP)

	op, err := e.instances(t).UpdateAccessConfig(t.Context(), &computepb.UpdateAccessConfigInstanceRequest{
		Project:          project,
		Zone:             zone,
		Instance:         "vm1",
		NetworkInterface: "nic0",
		AccessConfigResource: &computepb.AccessConfig{
			Name:  s("External NAT"),
			NatIP: s(reserved.GetAddress()),
			Type:  s("ONE_TO_ONE_NAT"),
		},
	})
	e.done(t, op, err)
	assert.Equal(t, reserved.GetAddress(), e.getVM(t, "vm1").GetNetworkInterfaces()[0].GetAccessConfigs()[0].GetNatIP())

	got, err := e.addresses(t).
		Get(t.Context(), &computepb.GetAddressRequest{Project: project, Region: region, Address: "static"})
	require.NoError(t, err)
	assert.Equal(t, "IN_USE", got.GetStatus())

	_, err = e.instances(t).UpdateAccessConfig(t.Context(), &computepb.UpdateAccessConfigInstanceRequest{
		Project: project, Zone: zone, Instance: "vm1", NetworkInterface: "nic0",
		AccessConfigResource: &computepb.AccessConfig{Name: s("missing")},
	})
	assertAPIError(t, err, http.StatusBadRequest, "invalid")

	nic := e.getVM(t, "vm1").GetNetworkInterfaces()[0]

	_, err = e.instances(t).UpdateNetworkInterface(t.Context(), &computepb.UpdateNetworkInterfaceInstanceRequest{
		Project: project, Zone: zone, Instance: "vm1", NetworkInterface: "nic0",
		NetworkInterfaceResource: &computepb.NetworkInterface{Fingerprint: s("stale")},
	})
	assertAPIError(t, err, http.StatusPreconditionFailed, "conditionNotMet")

	op, err = e.instances(t).UpdateNetworkInterface(t.Context(), &computepb.UpdateNetworkInterfaceInstanceRequest{
		Project: project, Zone: zone, Instance: "vm1", NetworkInterface: "nic0",
		NetworkInterfaceResource: &computepb.NetworkInterface{
			Fingerprint:   nic.Fingerprint,
			AliasIpRanges: []*computepb.AliasIpRange{{IpCidrRange: s("10.10.0.64/28")}},
		},
	})
	e.done(t, op, err)

	updated := e.getVM(t, "vm1").GetNetworkInterfaces()[0]
	require.Len(t, updated.GetAliasIpRanges(), 1)
	assert.Equal(t, "10.10.0.64/28", updated.GetAliasIpRanges()[0].GetIpCidrRange())
	assert.NotEqual(t, nic.GetFingerprint(), updated.GetFingerprint())

	_, err = e.instances(t).UpdateNetworkInterface(t.Context(), &computepb.UpdateNetworkInterfaceInstanceRequest{
		Project: project, Zone: zone, Instance: "vm1", NetworkInterface: "nic0",
		NetworkInterfaceResource: &computepb.NetworkInterface{NetworkIP: s("10.10.0.50")},
	})
	assertAPIError(t, err, http.StatusBadRequest, "resourceNotReady")

	e.stopVM1(t)

	op, err = e.instances(t).UpdateNetworkInterface(t.Context(), &computepb.UpdateNetworkInterfaceInstanceRequest{
		Project: project, Zone: zone, Instance: "vm1", NetworkInterface: "nic0",
		NetworkInterfaceResource: &computepb.NetworkInterface{NetworkIP: s("10.10.0.50")},
	})
	e.done(t, op, err)
	assert.Equal(t, "10.10.0.50", e.getVM(t, "vm1").GetNetworkInterfaces()[0].GetNetworkIP())
}

func TestSimulateMaintenanceEvent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		scheduling *computepb.Scheduling
		want       string
	}{
		{
			name:       "migrate keeps running",
			scheduling: &computepb.Scheduling{OnHostMaintenance: s("MIGRATE")},
			want:       "RUNNING",
		},
		{
			name: "terminate restarts", want: "PROVISIONING",
			scheduling: &computepb.Scheduling{OnHostMaintenance: s("TERMINATE"), AutomaticRestart: new(true)},
		},
		{
			name: "terminate without restart stops", want: "STOPPING",
			scheduling: &computepb.Scheduling{OnHostMaintenance: s("TERMINATE"), AutomaticRestart: new(false)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.baseNet(t)

			spec := vmSpec("vm1")
			spec.Scheduling = tt.scheduling
			e.vm(t, spec)

			op, err := e.instances(t).
				SimulateMaintenanceEvent(t.Context(), &computepb.SimulateMaintenanceEventInstanceRequest{
					Project: project, Zone: zone, Instance: "vm1",
				})
			require.NoError(t, err)
			require.NotNil(t, op)
			assert.Equal(t, tt.want, e.getVM(t, "vm1").GetStatus())
		})
	}

	t.Run("stopped is not ready", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		e.baseNet(t)
		e.vm(t, vmSpec("vm1"))
		e.stopVM1(t)

		_, err := e.instances(t).
			SimulateMaintenanceEvent(t.Context(), &computepb.SimulateMaintenanceEventInstanceRequest{
				Project: project, Zone: zone, Instance: "vm1",
			})
		assertAPIError(t, err, http.StatusBadRequest, "resourceNotReady")
	})
}

func TestListReferrers(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)
	e.vm(t, vmSpec("vm1"))

	op, err := e.groups(t).Insert(t.Context(), &computepb.InsertInstanceGroupRequest{
		Project: project, Zone: zone, InstanceGroupResource: &computepb.InstanceGroup{Name: s("ug")},
	})
	e.done(t, op, err)

	op, err = e.groups(t).AddInstances(t.Context(), &computepb.AddInstancesInstanceGroupRequest{
		Project: project, Zone: zone, InstanceGroup: "ug",
		InstanceGroupsAddInstancesRequestResource: &computepb.InstanceGroupsAddInstancesRequest{
			Instances: []*computepb.InstanceReference{{Instance: s("zones/" + zone + "/instances/vm1")}},
		},
	})
	e.done(t, op, err)

	it := e.instances(t).ListReferrers(t.Context(), &computepb.ListReferrersInstancesRequest{
		Project: project, Zone: zone, Instance: "vm1",
	})

	var refs []*computepb.Reference

	for {
		r, nerr := it.Next()
		if errors.Is(nerr, iterator.Done) {
			break
		}

		require.NoError(t, nerr)

		refs = append(refs, r)
	}

	require.Len(t, refs, 1)
	assert.Equal(t, "MEMBER_OF", refs[0].GetReferenceType())
	assert.Contains(t, refs[0].GetReferrer(), "/instanceGroups/ug")
	assert.Contains(t, refs[0].GetTarget(), "/instances/vm1")
}

func TestBulkInsert(t *testing.T) {
	t.Parallel()

	props := func() *computepb.InstanceProperties { return tmplSpec("x").GetProperties() }

	tests := []struct {
		name      string
		res       *computepb.BulkInsertInstanceResource
		existing  []string
		wantNames []string
		status    int
	}{
		{
			name: "name pattern", res: &computepb.BulkInsertInstanceResource{
				Count: proto.Int64(3), NamePattern: s("node-##"), InstanceProperties: props(),
			},
			wantNames: []string{"node-01", "node-02", "node-03"},
		},
		{
			name:     "pattern continues after highest",
			existing: []string{"node-07"},
			res: &computepb.BulkInsertInstanceResource{
				Count: proto.Int64(2), NamePattern: s("node-##"), InstanceProperties: props(),
			},
			wantNames: []string{"node-07", "node-08", "node-09"},
		},
		{
			name: "per instance names", res: &computepb.BulkInsertInstanceResource{
				InstanceProperties: props(),
				PerInstanceProperties: map[string]*computepb.BulkInsertInstanceResourcePerInstanceProperties{
					"alpha": {Hostname: s("alpha.example.com")}, "beta": {},
				},
			},
			wantNames: []string{"alpha", "beta"},
		},
		{
			name:   "needs name source",
			res:    &computepb.BulkInsertInstanceResource{Count: proto.Int64(1), InstanceProperties: props()},
			status: http.StatusBadRequest,
		},
		{
			name: "bad pattern", res: &computepb.BulkInsertInstanceResource{
				Count: proto.Int64(1), NamePattern: s("a#b#c"), InstanceProperties: props(),
			},
			status: http.StatusBadRequest,
		},
		{
			name:   "needs properties",
			res:    &computepb.BulkInsertInstanceResource{Count: proto.Int64(1), NamePattern: s("a-#")},
			status: http.StatusBadRequest,
		},
		{
			name: "min count rolls back", existing: []string{"dup"}, res: &computepb.BulkInsertInstanceResource{
				InstanceProperties: props(),
				PerInstanceProperties: map[string]*computepb.BulkInsertInstanceResourcePerInstanceProperties{
					"dup": {}, "fresh": {},
				},
			},
			wantNames: []string{"dup"}, status: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.baseNet(t)

			for _, n := range tt.existing {
				e.vm(t, vmSpec(n))
			}

			op, err := e.instances(t).BulkInsert(t.Context(), &computepb.BulkInsertInstanceRequest{
				Project: project, Zone: zone, BulkInsertInstanceResourceResource: tt.res,
			})
			if tt.status != 0 {
				assertAPIError(t, err, tt.status, "")
				assert.ElementsMatch(t, tt.wantNames, e.vmNames(t), "a failed bulk insert leaves nothing behind")

				return
			}

			e.done(t, op, err)
			assert.ElementsMatch(t, append(tt.existing[:0:0], tt.wantNames...), e.vmNames(t))
			assert.Equal(t, "bulkInsert", op.Proto().GetOperationType())
		})
	}
}

func TestBulkInsertRegionalAndTemplate(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)
	tmpl := e.template(t, "tpl")

	regInst := client(t, e, compute.NewRegionInstancesRESTClient)

	op, err := regInst.BulkInsert(t.Context(), &computepb.BulkInsertRegionInstanceRequest{
		Project: project, Region: region, BulkInsertInstanceResourceResource: &computepb.BulkInsertInstanceResource{
			Count: proto.Int64(4), NamePattern: s("r-#"), SourceInstanceTemplate: s(tmpl),
			LocationPolicy: &computepb.LocationPolicy{
				TargetShape: s("BALANCED"),
				Locations: map[string]*computepb.LocationPolicyLocation{
					"zones/us-central1-c": {Preference: s("DENY")},
				},
			},
		},
	})
	e.done(t, op, err)

	zones := map[string]int{}

	it := e.instances(t).AggregatedList(t.Context(), &computepb.AggregatedListInstancesRequest{Project: project})
	for {
		pair, nerr := it.Next()
		if errors.Is(nerr, iterator.Done) {
			break
		}

		require.NoError(t, nerr)

		for _, vm := range pair.Value.GetInstances() {
			zones[trimZone(vm.GetZone())]++
			assert.Equal(t, "test", vm.GetLabels()["env"], "template properties apply")
		}
	}

	total := 0

	for z, n := range zones {
		total += n

		assert.LessOrEqual(t, n, 2, "BALANCED spreads across the allowed zones: %s", z)
	}

	assert.Equal(t, 4, total)
	assert.Zero(t, zones["us-central1-c"])
	assert.GreaterOrEqual(t, len(zones), 2)
}

func trimZone(link string) string { return link[strings.LastIndex(link, "/")+1:] }
