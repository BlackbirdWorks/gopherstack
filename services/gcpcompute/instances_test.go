package gcpcompute_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

const (
	subnetRef   = "regions/" + region + "/subnetworks/sub"
	debianFam   = "projects/debian-cloud/global/images/family/debian-12"
	e2Medium    = "zones/" + zone + "/machineTypes/e2-medium"
	stRunning   = "RUNNING"
	stTerm      = "TERMINATED"
	otherZone   = "us-east1-b"
	otherRegion = "us-east1"
)

func (e *env) baseNet(t *testing.T) {
	t.Helper()
	e.network(t, "vpc", false)
	e.subnet(t, "sub", "10.10.0.0/24")

	op, err := e.subnets(t).Insert(t.Context(), &computepb.InsertSubnetworkRequest{
		Project: project, Region: otherRegion, SubnetworkResource: &computepb.Subnetwork{
			Name: s("east"), Network: s("global/networks/vpc"), IpCidrRange: s("10.20.0.0/24"),
		},
	})
	e.done(t, op, err)
}

func vmSpec(name string) *computepb.Instance {
	return &computepb.Instance{
		Name:        s(name),
		MachineType: s(e2Medium),
		Disks: []*computepb.AttachedDisk{
			{
				Boot:       new(true),
				AutoDelete: new(true),
				InitializeParams: &computepb.AttachedDiskInitializeParams{
					SourceImage: s(debianFam),
					DiskSizeGb:  proto.Int64(20),
				},
			},
		},
		NetworkInterfaces: []*computepb.NetworkInterface{{
			Subnetwork:    s(subnetRef),
			AccessConfigs: []*computepb.AccessConfig{{Name: s("External NAT"), Type: s("ONE_TO_ONE_NAT")}},
		}},
	}
}

func (e *env) vm(t *testing.T, spec *computepb.Instance) *computepb.Instance {
	t.Helper()

	op, err := e.instances(t).
		Insert(t.Context(), &computepb.InsertInstanceRequest{Project: project, Zone: zone, InstanceResource: spec})
	e.done(t, op, err)

	return e.getVM(t, spec.GetName())
}

func (e *env) getVM(t *testing.T, name string) *computepb.Instance {
	t.Helper()

	inst, err := e.instances(t).
		Get(t.Context(), &computepb.GetInstanceRequest{Project: project, Zone: zone, Instance: name})
	require.NoError(t, err)

	return inst
}

func TestInstanceStatusTransitions(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)

	op, err := e.instances(t).
		Insert(
			t.Context(),
			&computepb.InsertInstanceRequest{Project: project, Zone: zone, InstanceResource: vmSpec("vm1")},
		)
	require.NoError(t, err)
	assert.Equal(t, computepb.Operation_PENDING, op.Proto().GetStatus())
	assert.Contains(t, op.Proto().GetZone(), "/zones/"+zone)
	assert.Contains(t, op.Proto().GetTargetLink(), "/zones/"+zone+"/instances/vm1")

	timing := gcpcomputeTiming()

	tests := []struct {
		name   string
		status string
		at     time.Duration
		opStat computepb.Operation_Status
	}{
		{name: "provisioning", at: 0, status: "PROVISIONING", opStat: computepb.Operation_PENDING},
		{
			name: "still provisioning", at: timing.Provision - time.Millisecond,
			status: "PROVISIONING", opStat: computepb.Operation_RUNNING,
		},
		{name: "staging", at: timing.Provision, status: "STAGING", opStat: computepb.Operation_RUNNING},
		{name: "running", at: timing.Stage, status: "RUNNING", opStat: computepb.Operation_DONE},
	}

	var elapsed time.Duration

	for _, tt := range tests {
		e.clock.Advance(tt.at - elapsed)
		elapsed = tt.at

		inst := e.getVM(t, "vm1")
		assert.Equal(t, tt.status, inst.GetStatus(), tt.name)

		got, gerr := e.zoneOps(t).
			Get(
				t.Context(),
				&computepb.GetZoneOperationRequest{Project: project, Zone: zone, Operation: op.Name()},
			)
		require.NoError(t, gerr)

		if tt.name == "still provisioning" {
			assert.Equal(t, computepb.Operation_RUNNING, got.GetStatus())
			assert.NotEmpty(t, got.GetStartTime())
		}

		if tt.name == "running" {
			assert.Equal(t, computepb.Operation_DONE, got.GetStatus())
			assert.Equal(t, int32(100), got.GetProgress())
			assert.NotEmpty(t, got.GetEndTime())
		}
	}
}

func TestInstanceInsertShape(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)

	spec := vmSpec("vm1")
	spec.Labels = map[string]string{"env": "dev"}
	spec.Metadata = &computepb.Metadata{Items: []*computepb.Items{{Key: s("startup-script"), Value: s("echo hi")}}}
	spec.Tags = &computepb.Tags{Items: []string{"web"}}
	inst := e.vm(t, spec)

	assert.Equal(t, "compute#instance", inst.GetKind())
	assert.Equal(t, stRunning, inst.GetStatus())
	assert.Equal(
		t,
		"https://www.googleapis.com/compute/v1/projects/"+project+"/zones/"+zone+"/instances/vm1",
		inst.GetSelfLink(),
	)
	assert.Equal(
		t,
		"https://www.googleapis.com/compute/v1/projects/"+project+"/zones/"+zone+"/machineTypes/e2-medium",
		inst.GetMachineType(),
	)
	assert.NotZero(t, inst.GetId())
	assert.NotEmpty(t, inst.GetCreationTimestamp())
	assert.NotEmpty(t, inst.GetFingerprint())
	assert.NotEmpty(t, inst.GetCpuPlatform())
	assert.Equal(t, map[string]string{"env": "dev"}, inst.GetLabels())
	assert.NotEmpty(t, inst.GetLabelFingerprint())
	assert.NotEmpty(t, inst.GetMetadata().GetFingerprint())
	assert.Equal(t, "echo hi", inst.GetMetadata().GetItems()[0].GetValue())
	assert.Equal(t, []string{"web"}, inst.GetTags().GetItems())
	assert.NotEmpty(t, inst.GetTags().GetFingerprint())

	sched := inst.GetScheduling()
	assert.Equal(t, "MIGRATE", sched.GetOnHostMaintenance())
	assert.True(t, sched.GetAutomaticRestart())
	assert.Equal(t, "STANDARD", sched.GetProvisioningModel())

	require.Len(t, inst.GetNetworkInterfaces(), 1)

	nic := inst.GetNetworkInterfaces()[0]
	assert.Equal(t, "nic0", nic.GetName())
	assert.Equal(t, "10.10.0.2", nic.GetNetworkIP())
	assert.Contains(t, nic.GetNetwork(), "/global/networks/vpc")
	assert.Contains(t, nic.GetSubnetwork(), "/regions/"+region+"/subnetworks/sub")
	require.Len(t, nic.GetAccessConfigs(), 1)
	assert.True(t, strings.HasPrefix(nic.GetAccessConfigs()[0].GetNatIP(), "34."))
	assert.Equal(t, "ONE_TO_ONE_NAT", nic.GetAccessConfigs()[0].GetType())
	assert.Equal(t, "PREMIUM", nic.GetAccessConfigs()[0].GetNetworkTier())

	require.Len(t, inst.GetDisks(), 1)

	ad := inst.GetDisks()[0]
	assert.True(t, ad.GetBoot())
	assert.True(t, ad.GetAutoDelete())
	assert.Equal(t, "persistent-disk-0", ad.GetDeviceName())
	assert.Equal(t, "READ_WRITE", ad.GetMode())
	assert.Equal(t, int64(20), ad.GetDiskSizeGb())
	assert.Contains(t, ad.GetSource(), "/zones/"+zone+"/disks/vm1")

	disk, err := e.disks(t).Get(t.Context(), &computepb.GetDiskRequest{Project: project, Zone: zone, Disk: "vm1"})
	require.NoError(t, err)
	assert.Equal(t, int64(20), disk.GetSizeGb())
	assert.Contains(t, disk.GetSourceImage(), "/projects/debian-cloud/global/images/debian-12-bookworm-v")
	assert.Equal(t, []string{inst.GetSelfLink()}, disk.GetUsers())
}

func TestInstanceInsertErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mut    func(i *computepb.Instance)
		name   string
		reason string
		status int
	}{
		{func(i *computepb.Instance) { i.Name = nil }, "no name", "required", 400},
		{func(i *computepb.Instance) { i.Name = s("Bad_Name") }, "bad name", "invalid", 400},
		{func(i *computepb.Instance) { i.Name = s("exists") }, "duplicate", "alreadyExists", 409},
		{func(i *computepb.Instance) { i.MachineType = nil }, "no machine type", "required", 400},
		{
			func(i *computepb.Instance) { i.MachineType = s("zones/" + zone + "/machineTypes/z9-huge") },
			"unknown machine type",
			"notFound",
			404,
		},
		{
			func(i *computepb.Instance) { i.MachineType = s("zones/" + otherZone + "/machineTypes/e2-medium") },
			"machine type other zone",
			"invalid",
			400,
		},
		{func(i *computepb.Instance) { i.Disks = nil }, "no disks", "invalid", 400},
		{func(i *computepb.Instance) { i.Disks[0].Boot = new(false) }, "first disk not boot", "invalid", 400},
		{func(i *computepb.Instance) {
			i.Disks[0].InitializeParams.SourceImage = s("projects/debian-cloud/global/images/family/nope")
		}, "unknown image", "notFound", 404},
		{func(i *computepb.Instance) { i.Disks[0].InitializeParams = nil }, "disk without source", "invalid", 400},
		{func(i *computepb.Instance) { i.Disks[0].Type = s("SCRATCH") }, "scratch disk", "invalid", 400},
		{func(i *computepb.Instance) { i.NetworkInterfaces = nil }, "no nic", "invalid", 400},
		{func(i *computepb.Instance) {
			i.NetworkInterfaces[0].Subnetwork = nil
			i.NetworkInterfaces[0].Network = s("global/networks/vpc")
		}, "custom network without subnet", "invalid", 400},
		{func(i *computepb.Instance) {
			i.NetworkInterfaces[0].Subnetwork = s("regions/" + otherRegion + "/subnetworks/east")
		}, "subnet other region", "invalid", 400},
		{func(i *computepb.Instance) {
			i.NetworkInterfaces[0].Subnetwork = s("regions/" + region + "/subnetworks/ghost")
		}, "unknown subnet", "notFound", 404},
		{
			func(i *computepb.Instance) { i.NetworkInterfaces[0].NetworkIP = s("10.99.0.4") },
			"ip outside subnet",
			"invalid",
			400,
		},
		{
			func(i *computepb.Instance) { i.NetworkInterfaces[0].NetworkIP = s("10.10.0.1") },
			"gateway ip",
			"invalid",
			400,
		},
		{
			func(i *computepb.Instance) { i.NetworkInterfaces[0].AccessConfigs[0].NatIP = s("34.1.2.3") },
			"unreserved nat ip",
			"invalid",
			400,
		},
		{func(i *computepb.Instance) {
			i.Scheduling = &computepb.Scheduling{Preemptible: new(true), AutomaticRestart: new(true)}
		}, "preemptible auto restart", "invalid", 400},
		{func(i *computepb.Instance) { i.Labels = map[string]string{"Bad": "x"} }, "bad label", "invalid", 400},
		{func(i *computepb.Instance) {
			i.Metadata = &computepb.Metadata{Items: []*computepb.Items{{Key: s("a b")}}}
		}, "bad metadata key", "invalid", 400},
		{
			func(i *computepb.Instance) { i.Tags = &computepb.Tags{Items: []string{"Bad Tag"}} },
			"bad tag",
			"invalid",
			400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.baseNet(t)
			e.vm(t, vmSpec("exists"))

			disksBefore := countDisks(t, e)

			spec := vmSpec("fresh")
			tt.mut(spec)

			_, err := e.instances(t).
				Insert(
					t.Context(),
					&computepb.InsertInstanceRequest{Project: project, Zone: zone, InstanceResource: spec},
				)
			assertAPIError(t, err, tt.status, tt.reason)
			assert.Equal(t, disksBefore, countDisks(t, e), "failed insert must not leak disks")
		})
	}
}

func countDisks(t *testing.T, e *env) int {
	t.Helper()

	n := 0
	it := e.disks(t).List(t.Context(), &computepb.ListDisksRequest{Project: project, Zone: zone})

	for {
		_, err := it.Next()
		if err != nil {
			return n
		}

		n++
	}
}

func TestInstanceIPAllocation(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)

	ips := func(name string) (string, string) {
		nic := e.getVM(t, name).GetNetworkInterfaces()[0]

		return nic.GetNetworkIP(), nic.GetAccessConfigs()[0].GetNatIP()
	}

	e.vm(t, vmSpec("a"))
	e.vm(t, vmSpec("b"))
	e.vm(t, vmSpec("c"))

	ia, ea := ips("a")
	ib, eb := ips("b")
	ic, ec := ips("c")

	assert.Equal(t, []string{"10.10.0.2", "10.10.0.3", "10.10.0.4"}, []string{ia, ib, ic})
	assert.Len(t, map[string]bool{ea: true, eb: true, ec: true}, 3)

	op, err := e.instances(t).
		Delete(t.Context(), &computepb.DeleteInstanceRequest{Project: project, Zone: zone, Instance: "a"})
	e.done(t, op, err)

	e.vm(t, vmSpec("d"))

	id, _ := ips("d")
	assert.Equal(t, "10.10.0.2", id, "the lowest free address is reused")

	explicit := vmSpec("e")
	explicit.NetworkInterfaces[0].NetworkIP = s("10.10.0.100")
	e.vm(t, explicit)

	ie, _ := ips("e")
	assert.Equal(t, "10.10.0.100", ie)

	clash := vmSpec("f")
	clash.NetworkInterfaces[0].NetworkIP = s("10.10.0.100")

	_, err = e.instances(t).
		Insert(
			t.Context(),
			&computepb.InsertInstanceRequest{Project: project, Zone: zone, InstanceResource: clash},
		)
	assertAPIError(t, err, http.StatusBadRequest, "invalid")
}

func TestInstanceAutoModeNetworkNeedsNoSubnet(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.network(t, "auto", true)

	spec := vmSpec("auto-vm")
	spec.NetworkInterfaces = []*computepb.NetworkInterface{{Network: s("global/networks/auto")}}

	inst := e.vm(t, spec)
	nic := inst.GetNetworkInterfaces()[0]
	assert.Equal(t, "10.128.0.2", nic.GetNetworkIP())
	assert.Contains(t, nic.GetSubnetwork(), "/regions/"+region+"/subnetworks/auto")
	assert.Empty(t, nic.GetAccessConfigs())
}

func TestInstanceStopStart(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)
	insts := e.instances(t)
	inst := e.vm(t, vmSpec("vm1"))
	firstIP := inst.GetNetworkInterfaces()[0].GetAccessConfigs()[0].GetNatIP()
	require.NotEmpty(t, firstIP)

	op, err := insts.Stop(t.Context(), &computepb.StopInstanceRequest{Project: project, Zone: zone, Instance: "vm1"})
	require.NoError(t, err)
	assert.Equal(t, "stop", op.Proto().GetOperationType())
	assert.Equal(t, "STOPPING", e.getVM(t, "vm1").GetStatus())

	e.settle()
	require.NoError(t, op.Wait(t.Context()))

	stopped := e.getVM(t, "vm1")
	assert.Equal(t, stTerm, stopped.GetStatus())
	assert.NotEmpty(t, stopped.GetLastStopTimestamp())
	assert.Empty(t, stopped.GetNetworkInterfaces()[0].GetAccessConfigs()[0].GetNatIP(), "ephemeral IP is released")
	assert.Equal(t, "10.10.0.2", stopped.GetNetworkInterfaces()[0].GetNetworkIP(), "internal IP is kept")

	_, err = insts.Reset(t.Context(), &computepb.ResetInstanceRequest{Project: project, Zone: zone, Instance: "vm1"})
	assertAPIError(t, err, http.StatusBadRequest, "resourceNotReady")

	op, err = insts.Start(t.Context(), &computepb.StartInstanceRequest{Project: project, Zone: zone, Instance: "vm1"})
	require.NoError(t, err)
	assert.Equal(t, "PROVISIONING", e.getVM(t, "vm1").GetStatus())
	e.settle()
	require.NoError(t, op.Wait(t.Context()))

	started := e.getVM(t, "vm1")
	assert.Equal(t, stRunning, started.GetStatus())
	assert.NotEmpty(t, started.GetNetworkInterfaces()[0].GetAccessConfigs()[0].GetNatIP())

	op, err = insts.Start(t.Context(), &computepb.StartInstanceRequest{Project: project, Zone: zone, Instance: "vm1"})
	e.done(t, op, err)
	assert.Equal(t, stRunning, e.getVM(t, "vm1").GetStatus(), "starting a running instance is a no-op")

	op, err = insts.Reset(t.Context(), &computepb.ResetInstanceRequest{Project: project, Zone: zone, Instance: "vm1"})
	e.done(t, op, err)
	assert.Equal(t, stRunning, e.getVM(t, "vm1").GetStatus())

	_, err = insts.Stop(t.Context(), &computepb.StopInstanceRequest{Project: project, Zone: zone, Instance: "ghost"})
	assertAPIError(t, err, http.StatusNotFound, "notFound")
}

func TestInstanceMetadataLabelsTags(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)
	insts := e.instances(t)
	inst := e.vm(t, vmSpec("vm1"))

	md := inst.GetMetadata()
	item := func(k, v string) *computepb.Items { return &computepb.Items{Key: s(k), Value: s(v)} }

	metaTests := []struct {
		meta   *computepb.Metadata
		name   string
		reason string
		status int
	}{
		{
			&computepb.Metadata{Fingerprint: s("stale"), Items: []*computepb.Items{item("a", "1")}},
			"stale fingerprint",
			"conditionNotMet",
			412,
		},
		{&computepb.Metadata{Items: []*computepb.Items{item("a", "1")}}, "missing fingerprint", "conditionNotMet", 412},
		{
			&computepb.Metadata{Fingerprint: md.Fingerprint, Items: []*computepb.Items{item("a", "1"), item("a", "2")}},
			"duplicate keys",
			"invalid",
			400,
		},
		{
			&computepb.Metadata{Fingerprint: md.Fingerprint, Items: []*computepb.Items{item("bad key", "1")}},
			"bad key",
			"invalid",
			400,
		},
		{&computepb.Metadata{Fingerprint: md.Fingerprint, Items: []*computepb.Items{item("a", "1")}}, "ok", "", 200},
	}

	for _, tt := range metaTests {
		op, err := insts.SetMetadata(
			t.Context(),
			&computepb.SetMetadataInstanceRequest{
				Project:          project,
				Zone:             zone,
				Instance:         "vm1",
				MetadataResource: tt.meta,
			},
		)
		if tt.status == http.StatusOK {
			e.done(t, op, err)

			continue
		}

		assertAPIError(t, err, tt.status, tt.reason)
	}

	after := e.getVM(t, "vm1")
	assert.Equal(t, "1", after.GetMetadata().GetItems()[0].GetValue())
	assert.NotEqual(t, md.GetFingerprint(), after.GetMetadata().GetFingerprint())
	assert.NotEqual(t, inst.GetFingerprint(), after.GetFingerprint())

	_, err := insts.SetMetadata(t.Context(), &computepb.SetMetadataInstanceRequest{
		Project:          project,
		Zone:             zone,
		Instance:         "vm1",
		MetadataResource: &computepb.Metadata{Fingerprint: md.Fingerprint},
	})
	assertAPIError(t, err, http.StatusPreconditionFailed, "conditionNotMet")

	_, err = insts.SetLabels(t.Context(), &computepb.SetLabelsInstanceRequest{
		Project:  project,
		Zone:     zone,
		Instance: "vm1",
		InstancesSetLabelsRequestResource: &computepb.InstancesSetLabelsRequest{
			Labels:           map[string]string{"a": "b"},
			LabelFingerprint: s("stale"),
		},
	})
	assertAPIError(t, err, http.StatusPreconditionFailed, "conditionNotMet")

	_, err = insts.SetLabels(t.Context(), &computepb.SetLabelsInstanceRequest{
		Project:  project,
		Zone:     zone,
		Instance: "vm1",
		InstancesSetLabelsRequestResource: &computepb.InstancesSetLabelsRequest{
			Labels:           map[string]string{"A": "b"},
			LabelFingerprint: after.LabelFingerprint,
		},
	})
	assertAPIError(t, err, http.StatusBadRequest, "invalid")

	op, err := insts.SetLabels(t.Context(), &computepb.SetLabelsInstanceRequest{
		Project:  project,
		Zone:     zone,
		Instance: "vm1",
		InstancesSetLabelsRequestResource: &computepb.InstancesSetLabelsRequest{
			Labels:           map[string]string{"team": "core"},
			LabelFingerprint: after.LabelFingerprint,
		},
	})
	e.done(t, op, err)

	labelled := e.getVM(t, "vm1")
	assert.Equal(t, map[string]string{"team": "core"}, labelled.GetLabels())
	assert.NotEqual(t, after.GetLabelFingerprint(), labelled.GetLabelFingerprint())

	_, err = insts.SetTags(t.Context(), &computepb.SetTagsInstanceRequest{
		Project:      project,
		Zone:         zone,
		Instance:     "vm1",
		TagsResource: &computepb.Tags{Items: []string{"x"}, Fingerprint: s("stale")},
	})
	assertAPIError(t, err, http.StatusPreconditionFailed, "conditionNotMet")

	op, err = insts.SetTags(t.Context(), &computepb.SetTagsInstanceRequest{
		Project:      project,
		Zone:         zone,
		Instance:     "vm1",
		TagsResource: &computepb.Tags{Items: []string{"web", "db"}, Fingerprint: labelled.GetTags().Fingerprint},
	})
	e.done(t, op, err)

	assert.ElementsMatch(t, []string{"web", "db"}, e.getVM(t, "vm1").GetTags().GetItems())
}

func TestInstanceSetMachineType(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)
	insts := e.instances(t)
	e.vm(t, vmSpec("vm1"))

	set := func(mt string) error {
		op, err := insts.SetMachineType(t.Context(), &computepb.SetMachineTypeInstanceRequest{
			Project: project, Zone: zone, Instance: "vm1",
			InstancesSetMachineTypeRequestResource: &computepb.InstancesSetMachineTypeRequest{MachineType: s(mt)},
		})
		if err == nil {
			e.done(t, op, err)
		}

		return err
	}

	assertAPIError(t, set("zones/"+zone+"/machineTypes/n2-standard-4"), http.StatusBadRequest, "resourceNotReady")

	op, err := insts.Stop(t.Context(), &computepb.StopInstanceRequest{Project: project, Zone: zone, Instance: "vm1"})
	e.done(t, op, err)

	assertAPIError(t, set("zones/"+zone+"/machineTypes/q1-nope"), http.StatusNotFound, "notFound")
	require.NoError(t, set("zones/"+zone+"/machineTypes/n2-standard-4"))
	assert.Contains(t, e.getVM(t, "vm1").GetMachineType(), "/machineTypes/n2-standard-4")
	require.NoError(t, set("zones/"+zone+"/machineTypes/custom-4-8192"))
	assert.Contains(t, e.getVM(t, "vm1").GetMachineType(), "/machineTypes/custom-4-8192")
	assertAPIError(t, set("zones/"+zone+"/machineTypes/custom-3-8192"), http.StatusNotFound, "notFound")
}

func TestInstanceDisksAttachDetach(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)
	insts := e.instances(t)
	e.vm(t, vmSpec("vm1"))
	e.vm(t, vmSpec("vm2"))

	op, err := e.disks(t).
		Insert(
			t.Context(),
			&computepb.InsertDiskRequest{
				Project:      project,
				Zone:         zone,
				DiskResource: &computepb.Disk{Name: s("data"), SizeGb: proto.Int64(10)},
			},
		)
	e.done(t, op, err)

	attach := func(inst string, ad *computepb.AttachedDisk) error {
		aop, aerr := insts.AttachDisk(
			t.Context(),
			&computepb.AttachDiskInstanceRequest{
				Project:              project,
				Zone:                 zone,
				Instance:             inst,
				AttachedDiskResource: ad,
			},
		)
		if aerr == nil {
			e.done(t, aop, aerr)
		}

		return aerr
	}

	assertAPIError(
		t,
		attach("vm1", &computepb.AttachedDisk{Source: s("zones/" + zone + "/disks/ghost")}),
		http.StatusNotFound,
		"notFound",
	)
	assertAPIError(t, attach("vm1", &computepb.AttachedDisk{}), http.StatusBadRequest, "invalid")
	require.NoError(
		t,
		attach("vm1", &computepb.AttachedDisk{Source: s("zones/" + zone + "/disks/data"), DeviceName: s("data-disk")}),
	)
	assertAPIError(
		t,
		attach("vm1", &computepb.AttachedDisk{Source: s("zones/" + zone + "/disks/data")}),
		http.StatusBadRequest,
		"invalid",
	)
	assertAPIError(
		t,
		attach("vm2", &computepb.AttachedDisk{Source: s("zones/" + zone + "/disks/data")}),
		http.StatusBadRequest,
		"resourceInUseByAnotherResource",
	)

	vm1 := e.getVM(t, "vm1")
	require.Len(t, vm1.GetDisks(), 2)
	assert.Equal(t, "data-disk", vm1.GetDisks()[1].GetDeviceName())
	assert.Equal(t, int32(1), vm1.GetDisks()[1].GetIndex())
	assert.False(t, vm1.GetDisks()[1].GetBoot())

	disk, err := e.disks(t).Get(t.Context(), &computepb.GetDiskRequest{Project: project, Zone: zone, Disk: "data"})
	require.NoError(t, err)
	assert.Equal(t, []string{vm1.GetSelfLink()}, disk.GetUsers())
	assert.NotEmpty(t, disk.GetLastAttachTimestamp())

	_, err = e.disks(t).Delete(t.Context(), &computepb.DeleteDiskRequest{Project: project, Zone: zone, Disk: "data"})
	assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")

	_, err = insts.DetachDisk(
		t.Context(),
		&computepb.DetachDiskInstanceRequest{
			Project:    project,
			Zone:       zone,
			Instance:   "vm1",
			DeviceName: "persistent-disk-0",
		},
	)
	assertAPIError(t, err, http.StatusBadRequest, "resourceNotReady")

	_, err = insts.DetachDisk(
		t.Context(),
		&computepb.DetachDiskInstanceRequest{Project: project, Zone: zone, Instance: "vm1", DeviceName: "nope"},
	)
	assertAPIError(t, err, http.StatusBadRequest, "invalid")

	op, err = insts.DetachDisk(
		t.Context(),
		&computepb.DetachDiskInstanceRequest{Project: project, Zone: zone, Instance: "vm1", DeviceName: "data-disk"},
	)
	e.done(t, op, err)

	disk, err = e.disks(t).Get(t.Context(), &computepb.GetDiskRequest{Project: project, Zone: zone, Disk: "data"})
	require.NoError(t, err)
	assert.Empty(t, disk.GetUsers())
	assert.NotEmpty(t, disk.GetLastDetachTimestamp())

	require.NoError(
		t,
		attach("vm2", &computepb.AttachedDisk{Source: s("zones/" + zone + "/disks/data"), Mode: s("READ_ONLY")}),
	)
	require.NoError(
		t,
		attach("vm1", &computepb.AttachedDisk{Source: s("zones/" + zone + "/disks/data"), Mode: s("READ_ONLY")}),
	)
}

func TestInstanceDelete(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)
	insts := e.instances(t)

	spec := vmSpec("vm1")
	spec.DeletionProtection = new(true)
	e.vm(t, spec)

	_, err := insts.Delete(t.Context(), &computepb.DeleteInstanceRequest{Project: project, Zone: zone, Instance: "vm1"})
	assertAPIError(t, err, http.StatusBadRequest, "invalid")

	op, err := insts.SetDeletionProtection(t.Context(), &computepb.SetDeletionProtectionInstanceRequest{
		Project: project, Zone: zone, Resource: "vm1", DeletionProtection: new(false),
	})
	e.done(t, op, err)

	_, err = e.subnets(t).
		Delete(
			t.Context(),
			&computepb.DeleteSubnetworkRequest{Project: project, Region: region, Subnetwork: "sub"},
		)
	assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")

	op, err = insts.Delete(t.Context(), &computepb.DeleteInstanceRequest{Project: project, Zone: zone, Instance: "vm1"})
	e.done(t, op, err)

	_, err = insts.Get(t.Context(), &computepb.GetInstanceRequest{Project: project, Zone: zone, Instance: "vm1"})
	assertAPIError(t, err, http.StatusNotFound, "notFound")

	_, err = e.disks(t).Get(t.Context(), &computepb.GetDiskRequest{Project: project, Zone: zone, Disk: "vm1"})
	assertAPIError(t, err, http.StatusNotFound, "notFound")

	op, err = e.subnets(t).
		Delete(
			t.Context(),
			&computepb.DeleteSubnetworkRequest{Project: project, Region: region, Subnetwork: "sub"},
		)
	e.done(t, op, err)
}

func TestInstanceStaticAddresses(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)
	addrs := e.addresses(t)

	for _, a := range []*computepb.Address{
		{Name: s("ext")},
		{Name: s("int"), AddressType: s("INTERNAL"), Subnetwork: s(subnetRef)},
	} {
		op, err := addrs.Insert(
			t.Context(),
			&computepb.InsertAddressRequest{Project: project, Region: region, AddressResource: a},
		)
		e.done(t, op, err)
	}

	ext, err := addrs.Get(t.Context(), &computepb.GetAddressRequest{Project: project, Region: region, Address: "ext"})
	require.NoError(t, err)

	internal, err := addrs.Get(
		t.Context(),
		&computepb.GetAddressRequest{Project: project, Region: region, Address: "int"},
	)
	require.NoError(t, err)

	spec := vmSpec("vm1")
	spec.NetworkInterfaces[0].NetworkIP = s(internal.GetAddress())
	spec.NetworkInterfaces[0].AccessConfigs[0].NatIP = s(ext.GetAddress())
	inst := e.vm(t, spec)

	nic := inst.GetNetworkInterfaces()[0]
	assert.Equal(t, internal.GetAddress(), nic.GetNetworkIP())
	assert.Equal(t, ext.GetAddress(), nic.GetAccessConfigs()[0].GetNatIP())

	for _, name := range []string{"ext", "int"} {
		got, gerr := addrs.Get(
			t.Context(),
			&computepb.GetAddressRequest{Project: project, Region: region, Address: name},
		)
		require.NoError(t, gerr)
		assert.Equal(t, "IN_USE", got.GetStatus(), name)
		assert.Equal(t, []string{inst.GetSelfLink()}, got.GetUsers(), name)

		_, gerr = addrs.Delete(
			t.Context(),
			&computepb.DeleteAddressRequest{Project: project, Region: region, Address: name},
		)
		assertAPIError(t, gerr, http.StatusBadRequest, "resourceInUseByAnotherResource")
	}

	other := vmSpec("vm2")
	other.NetworkInterfaces[0].AccessConfigs[0].NatIP = s(ext.GetAddress())
	_, err = e.instances(t).
		Insert(
			t.Context(),
			&computepb.InsertInstanceRequest{Project: project, Zone: zone, InstanceResource: other},
		)
	assertAPIError(t, err, http.StatusBadRequest, "invalid")

	op, err := e.instances(t).
		Stop(t.Context(), &computepb.StopInstanceRequest{Project: project, Zone: zone, Instance: "vm1"})
	e.done(t, op, err)
	assert.Equal(
		t,
		ext.GetAddress(),
		e.getVM(t, "vm1").GetNetworkInterfaces()[0].GetAccessConfigs()[0].GetNatIP(),
		"static IP survives a stop",
	)
}

func TestInstanceAccessConfigs(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)
	insts := e.instances(t)

	spec := vmSpec("vm1")
	spec.NetworkInterfaces[0].AccessConfigs = nil
	inst := e.vm(t, spec)
	assert.Empty(t, inst.GetNetworkInterfaces()[0].GetAccessConfigs())

	op, err := insts.AddAccessConfig(t.Context(), &computepb.AddAccessConfigInstanceRequest{
		Project: project, Zone: zone, Instance: "vm1", NetworkInterface: "nic0",
		AccessConfigResource: &computepb.AccessConfig{Name: s("External NAT"), Type: s("ONE_TO_ONE_NAT")},
	})
	e.done(t, op, err)

	got := e.getVM(t, "vm1").GetNetworkInterfaces()[0]
	require.Len(t, got.GetAccessConfigs(), 1)
	assert.NotEmpty(t, got.GetAccessConfigs()[0].GetNatIP())
	assert.NotEqual(t, inst.GetNetworkInterfaces()[0].GetFingerprint(), got.GetFingerprint())

	_, err = insts.AddAccessConfig(t.Context(), &computepb.AddAccessConfigInstanceRequest{
		Project: project, Zone: zone, Instance: "vm1", NetworkInterface: "nic0",
		AccessConfigResource: &computepb.AccessConfig{Name: s("External NAT")},
	})
	assertAPIError(t, err, http.StatusBadRequest, "invalid")

	_, err = insts.AddAccessConfig(t.Context(), &computepb.AddAccessConfigInstanceRequest{
		Project:              project,
		Zone:                 zone,
		Instance:             "vm1",
		NetworkInterface:     "nic9",
		AccessConfigResource: &computepb.AccessConfig{Name: s("x")},
	})
	assertAPIError(t, err, http.StatusBadRequest, "invalid")

	op, err = insts.DeleteAccessConfig(t.Context(), &computepb.DeleteAccessConfigInstanceRequest{
		Project: project, Zone: zone, Instance: "vm1", NetworkInterface: "nic0", AccessConfig: "External NAT",
	})
	e.done(t, op, err)
	assert.Empty(t, e.getVM(t, "vm1").GetNetworkInterfaces()[0].GetAccessConfigs())

	_, err = insts.DeleteAccessConfig(t.Context(), &computepb.DeleteAccessConfigInstanceRequest{
		Project: project, Zone: zone, Instance: "vm1", NetworkInterface: "nic0", AccessConfig: "External NAT",
	})
	assertAPIError(t, err, http.StatusBadRequest, "invalid")
}
