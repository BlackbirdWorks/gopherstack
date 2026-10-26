package ocicore_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testAD  = "Abcd:US-ASHBURN-AD-1"
	testAD2 = "Abcd:US-ASHBURN-AD-2"
)

type netFix struct {
	vcn     core.Vcn
	pub     core.Subnet
	private core.Subnet
}

func (e *env) fixture(t *testing.T) netFix {
	t.Helper()

	v := e.mkVcn(t, "10.0.0.0/16", "cmp")
	pub, err := e.client.CreateSubnet(
		t.Context(),
		core.CreateSubnetRequest{
			CompartmentId: new(testComp), VcnId: v.Id, CidrBlock: new("10.0.1.0/24"), DnsLabel: new("pub")},
	)
	require.NoError(t, err)

	priv, err := e.client.CreateSubnet(
		t.Context(),
		core.CreateSubnetRequest{
			CompartmentId: new(
				testComp,
			),
			VcnId:                  v.Id,
			CidrBlock:              new("10.0.2.0/24"),
			ProhibitPublicIpOnVnic: new(true),
		},
	)
	require.NoError(t, err)

	return netFix{vcn: v, pub: pub.Subnet, private: priv.Subnet}
}

func (e *env) imageID(t *testing.T, os, version string, arm bool) *string {
	t.Helper()

	shape := "VM.Standard.E4.Flex"
	if arm {
		shape = "VM.Standard.A1.Flex"
	}

	r, err := e.compute.ListImages(t.Context(), core.ListImagesRequest{
		CompartmentId: new(testComp), OperatingSystem: new(os), OperatingSystemVersion: new(version), Shape: new(shape),
	})
	require.NoError(t, err)

	for _, it := range r.Items {
		if it.BaseImageId == nil {
			return it.Id
		}
	}

	require.FailNow(t, "no platform image")

	return nil
}

func (e *env) settle() { e.clock.Advance(3 * time.Second) }

func (e *env) launch(t *testing.T, subnet core.Subnet, mod func(*core.LaunchInstanceDetails)) (core.Instance, error) {
	t.Helper()

	d := core.LaunchInstanceDetails{
		CompartmentId: new(testComp), AvailabilityDomain: new(testAD), Shape: new("VM.Standard.E4.Flex"),
		DisplayName: new("web"), ShapeConfig: &core.LaunchInstanceShapeConfigDetails{Ocpus: new(float32(2))},
		SourceDetails:     core.InstanceSourceViaImageDetails{ImageId: e.imageID(t, "Oracle Linux", "9", false)},
		CreateVnicDetails: &core.CreateVnicDetails{SubnetId: subnet.Id},
	}
	if mod != nil {
		mod(&d)
	}

	r, err := e.compute.LaunchInstance(t.Context(), core.LaunchInstanceRequest{LaunchInstanceDetails: d})

	return r.Instance, err
}

func (e *env) mustLaunch(t *testing.T, subnet core.Subnet) core.Instance {
	t.Helper()

	i, err := e.launch(t, subnet, nil)
	require.NoError(t, err)

	return i
}

func (e *env) getInstance(t *testing.T, id *string) core.Instance {
	t.Helper()

	r, err := e.compute.GetInstance(t.Context(), core.GetInstanceRequest{InstanceId: id})
	require.NoError(t, err)

	return r.Instance
}

func TestShapesAndImages(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()

	shapes, err := e.compute.ListShapes(ctx, core.ListShapesRequest{CompartmentId: new(testComp)})
	require.NoError(t, err)

	byName := map[string]core.Shape{}
	for _, s := range shapes.Items {
		byName[*s.Shape] = s
	}

	for _, n := range []string{
		"VM.Standard.E4.Flex", "VM.Standard.E5.Flex", "VM.Standard.A1.Flex", "VM.Standard3.Flex", "VM.Standard2.1",
	} {
		assert.Contains(t, byName, n)
	}

	assert.InDelta(t, 64, *byName["VM.Standard.E4.Flex"].OcpuOptions.Max, 0)
	assert.True(t, *byName["VM.Standard.E4.Flex"].IsFlexible)
	assert.False(t, *byName["VM.Standard2.1"].IsFlexible)

	tests := []struct {
		req   core.ListImagesRequest
		name  string
		count int
	}{
		{core.ListImagesRequest{OperatingSystem: new("Oracle Linux")}, "oracle linux", 4},
		{
			core.ListImagesRequest{OperatingSystem: new("Canonical Ubuntu"), OperatingSystemVersion: new("24.04")},
			"ubuntu 24",
			2,
		},
		{core.ListImagesRequest{OperatingSystem: new("Windows")}, "windows", 2},
		{core.ListImagesRequest{Shape: new("VM.Standard.A1.Flex")}, "arm shape", 4},
		{core.ListImagesRequest{OperatingSystem: new("Plan 9")}, "none", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.req.CompartmentId = new(testComp)
			r, lerr := e.compute.ListImages(ctx, tt.req)
			require.NoError(t, lerr)
			assert.Len(t, r.Items, tt.count)
		})
	}

	img := e.imageID(t, "Windows", "Server 2022 Standard", false)
	g, err := e.compute.GetImage(ctx, core.GetImageRequest{ImageId: img})
	require.NoError(t, err)
	assert.Equal(t, core.ImageLifecycleStateAvailable, g.LifecycleState)
	assert.Positive(t, *g.SizeInMBs)

	_, err = e.compute.GetImage(ctx, core.GetImageRequest{ImageId: new("ocid1.image.oc1.iad.nope")})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestLaunchInstanceValidation(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	f := e.fixture(t)
	other := e.fixture(t)
	_, err := e.launch(t, f.pub, func(d *core.LaunchInstanceDetails) {
		d.CreateVnicDetails.HostnameLabel = new("taken")
	})
	require.NoError(t, err)

	armImg := e.imageID(t, "Oracle Linux", "9", true)
	adSubnet, err := e.client.CreateSubnet(
		t.Context(),
		core.CreateSubnetRequest{
			CompartmentId: new(
				testComp,
			),
			VcnId:              f.vcn.Id,
			CidrBlock:          new("10.0.3.0/24"),
			AvailabilityDomain: new(testAD2)},
	)
	require.NoError(t, err)

	tests := []struct {
		mod    func(*core.LaunchInstanceDetails)
		name   string
		code   string
		status int
	}{
		{func(d *core.LaunchInstanceDetails) { d.AvailabilityDomain = nil }, "missing ad", "MissingParameter", 400},
		{func(d *core.LaunchInstanceDetails) { d.AvailabilityDomain = new("AD-1") }, "bad ad", "InvalidParameter", 400},
		{func(d *core.LaunchInstanceDetails) { d.Shape = nil }, "missing shape", "MissingParameter", 400},
		{func(d *core.LaunchInstanceDetails) { d.Shape = new("VM.Nope.1") }, "unknown shape", "InvalidParameter", 400},
		{func(d *core.LaunchInstanceDetails) { d.ShapeConfig = nil }, "flex needs config", "MissingParameter", 400},
		{
			func(d *core.LaunchInstanceDetails) { d.ShapeConfig.Ocpus = new(float32(65)) },
			"too many ocpus",
			"InvalidParameter",
			400,
		},
		{
			func(d *core.LaunchInstanceDetails) { d.ShapeConfig.MemoryInGBs = new(float32(2000)) },
			"too much memory",
			"InvalidParameter",
			400,
		},
		{
			func(d *core.LaunchInstanceDetails) { d.Shape = new("VM.Standard2.1") },
			"fixed shape with config",
			"InvalidParameter",
			400,
		},
		{func(d *core.LaunchInstanceDetails) { d.SourceDetails = nil }, "no source", "MissingParameter", 400},
		{func(d *core.LaunchInstanceDetails) {
			d.SourceDetails = core.InstanceSourceViaImageDetails{ImageId: new("ocid1.image.oc1.iad.nope")}
		}, "unknown image", "NotAuthorizedOrNotFound", 404},
		{func(d *core.LaunchInstanceDetails) {
			d.SourceDetails = core.InstanceSourceViaImageDetails{ImageId: armImg}
		}, "arch mismatch", "InvalidParameter", 400},
		{func(d *core.LaunchInstanceDetails) {
			d.SourceDetails = core.InstanceSourceViaImageDetails{
				ImageId:             d.SourceDetails.(core.InstanceSourceViaImageDetails).ImageId,
				BootVolumeSizeInGBs: new(int64(10)),
			}
		}, "boot volume too small", "InvalidParameter", 400},
		{func(d *core.LaunchInstanceDetails) { d.CreateVnicDetails = nil }, "no subnet", "MissingParameter", 400},
		{func(d *core.LaunchInstanceDetails) {
			d.CreateVnicDetails.SubnetId = other.pub.Id
			d.CreateVnicDetails.NsgIds = []string{"ocid1.networksecuritygroup.oc1.iad.x"}
		}, "unknown nsg", "NotAuthorizedOrNotFound", 404},
		{func(d *core.LaunchInstanceDetails) {
			d.CreateVnicDetails.SubnetId = f.private.Id
			d.CreateVnicDetails.AssignPublicIp = new(true)
		}, "public on private subnet", "InvalidParameter", 400},
		{
			func(d *core.LaunchInstanceDetails) { d.CreateVnicDetails.SubnetId = adSubnet.Id },
			"subnet ad mismatch",
			"InvalidParameter",
			400,
		},
		{
			func(d *core.LaunchInstanceDetails) { d.CreateVnicDetails.PrivateIp = new("10.0.1.1") },
			"reserved ip",
			"InvalidParameter",
			400,
		},
		{
			func(d *core.LaunchInstanceDetails) { d.CreateVnicDetails.PrivateIp = new("192.168.0.5") },
			"ip outside subnet",
			"InvalidParameter",
			400,
		},
		{
			func(d *core.LaunchInstanceDetails) { d.CreateVnicDetails.HostnameLabel = new("TAKEN") },
			"dup hostname",
			"Conflict",
			409,
		},
		{
			func(d *core.LaunchInstanceDetails) { d.CreateVnicDetails.HostnameLabel = new("-bad-") },
			"bad hostname",
			"InvalidParameter",
			400,
		},
		{func(d *core.LaunchInstanceDetails) {
			d.CreateVnicDetails.SubnetId = f.private.Id
			d.CreateVnicDetails.HostnameLabel = new("nodns")
		}, "hostname without subnet dns", "InvalidParameter", 400},
		{
			func(d *core.LaunchInstanceDetails) { d.Metadata = map[string]string{"user_data": "!!notbase64"} },
			"bad user data",
			"InvalidParameter",
			400,
		},
		{
			func(d *core.LaunchInstanceDetails) { d.FaultDomain = new("FD-9") },
			"bad fault domain",
			"InvalidParameter",
			400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, lerr := e.launch(t, f.pub, tt.mod)
			se := svcErr(t, lerr)
			assert.Equal(t, tt.status, se.GetHTTPStatusCode())
			assert.Equal(t, tt.code, se.GetCode())
		})
	}
}

func TestLaunchInstanceState(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	f := e.fixture(t)

	i, err := e.launch(t, f.pub, func(d *core.LaunchInstanceDetails) {
		d.CreateVnicDetails.HostnameLabel = new("app1")
		d.CreateVnicDetails.DisplayName = new("primary")
		d.Metadata = map[string]string{"ssh_authorized_keys": "ssh-rsa AAAA test", "user_data": "ZWNobyBoaQ=="}
		d.FreeformTags = map[string]string{"env": "dev"}
	})
	require.NoError(t, err)
	assert.Equal(t, core.InstanceLifecycleStateProvisioning, i.LifecycleState)
	assert.Equal(t, "VM.Standard.E4.Flex", *i.Shape)
	assert.InDelta(t, 2, *i.ShapeConfig.Ocpus, 0)
	assert.InDelta(t, 32, *i.ShapeConfig.MemoryInGBs, 0)
	assert.Equal(t, "ZWNobyBoaQ==", i.Metadata["user_data"])
	assert.Equal(t, "dev", i.FreeformTags["env"])
	assert.Equal(t, "us-ashburn-1", *i.Region)
	assert.Regexp(t, `^FAULT-DOMAIN-[1-3]$`, *i.FaultDomain)

	e.settle()
	assert.Equal(t, core.InstanceLifecycleStateRunning, e.getInstance(t, i.Id).LifecycleState)

	atts, err := e.compute.ListVnicAttachments(
		ctx,
		core.ListVnicAttachmentsRequest{CompartmentId: new(testComp), InstanceId: i.Id},
	)
	require.NoError(t, err)
	require.Len(t, atts.Items, 1)
	assert.Equal(t, core.VnicAttachmentLifecycleStateAttached, atts.Items[0].LifecycleState)
	assert.Equal(t, 0, *atts.Items[0].NicIndex)

	vn, err := e.client.GetVnic(ctx, core.GetVnicRequest{VnicId: atts.Items[0].VnicId})
	require.NoError(t, err)
	assert.True(t, *vn.IsPrimary)
	assert.Equal(t, "10.0.1.2", *vn.PrivateIp)
	assert.NotEmpty(t, *vn.PublicIp)
	assert.Equal(t, "app1", *vn.HostnameLabel)
	assert.Equal(t, core.VnicLifecycleStateAvailable, vn.LifecycleState)
	assert.Regexp(t, `^02:00:17:`, *vn.MacAddress)

	ips, err := e.client.ListPrivateIps(ctx, core.ListPrivateIpsRequest{VnicId: vn.Id})
	require.NoError(t, err)
	require.Len(t, ips.Items, 1)
	assert.True(t, *ips.Items[0].IsPrimary)

	pub, err := e.client.GetPublicIpByIpAddress(ctx, core.GetPublicIpByIpAddressRequest{
		IpAddress: vn.PublicIp})
	require.NoError(t, err)
	assert.Equal(t, core.PublicIpLifetimeEphemeral, pub.Lifetime)
	assert.Equal(t, ips.Items[0].Id, pub.AssignedEntityId)

	bvs, err := e.block.ListBootVolumes(
		ctx,
		core.ListBootVolumesRequest{CompartmentId: new(testComp), AvailabilityDomain: new(testAD)},
	)
	require.NoError(t, err)
	require.Len(t, bvs.Items, 1)
	assert.Equal(t, core.BootVolumeLifecycleStateAvailable, bvs.Items[0].LifecycleState)
	assert.EqualValues(t, 50, *bvs.Items[0].SizeInGBs)

	bas, err := e.compute.ListBootVolumeAttachments(ctx, core.ListBootVolumeAttachmentsRequest{
		CompartmentId: new(testComp), AvailabilityDomain: new(testAD), InstanceId: i.Id})
	require.NoError(t, err)
	require.Len(t, bas.Items, 1)
	assert.Equal(t, bvs.Items[0].Id, bas.Items[0].BootVolumeId)

	private, err := e.launch(t, f.private, nil)
	require.NoError(t, err)

	e.settle()

	pvn := e.vnicOf(t, private.Id)
	assert.Nil(t, pvn.PublicIp)
	assert.Equal(t, "10.0.2.2", *pvn.PrivateIp)

	n, err := e.compute.ListInstances(
		ctx,
		core.ListInstancesRequest{CompartmentId: new(testComp), DisplayName: new("web")},
	)
	require.NoError(t, err)
	assert.Len(t, n.Items, 2)

	n, err = e.compute.ListInstances(ctx, core.ListInstancesRequest{
		CompartmentId: new(testComp), AvailabilityDomain: new(testAD2)})
	require.NoError(t, err)
	assert.Empty(t, n.Items)
}

func (e *env) vnicOf(t *testing.T, instance *string) core.Vnic {
	t.Helper()

	atts, err := e.compute.ListVnicAttachments(
		t.Context(),
		core.ListVnicAttachmentsRequest{CompartmentId: new(testComp), InstanceId: instance},
	)
	require.NoError(t, err)
	require.NotEmpty(t, atts.Items)

	vn, err := e.client.GetVnic(t.Context(), core.GetVnicRequest{VnicId: atts.Items[0].VnicId})
	require.NoError(t, err)

	return vn.Vnic
}

func TestInstanceActions(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	tests := []struct {
		name      string
		action    core.InstanceActionActionEnum
		prepare   core.InstanceActionActionEnum
		wantState core.InstanceLifecycleStateEnum
		settled   core.InstanceLifecycleStateEnum
		status    int
	}{
		{
			"stop",
			core.InstanceActionActionStop,
			"",
			core.InstanceLifecycleStateStopping,
			core.InstanceLifecycleStateStopped,
			0,
		},
		{
			"softstop",
			core.InstanceActionActionSoftstop,
			"",
			core.InstanceLifecycleStateStopping,
			core.InstanceLifecycleStateStopped,
			0,
		},
		{"start while running", core.InstanceActionActionStart, "", "", "", http.StatusConflict},
		{
			"start after stop",
			core.InstanceActionActionStart,
			core.InstanceActionActionStop,
			core.InstanceLifecycleStateStarting,
			core.InstanceLifecycleStateRunning,
			0,
		},
		{
			"reset",
			core.InstanceActionActionReset,
			"",
			core.InstanceLifecycleStateRunning,
			core.InstanceLifecycleStateRunning,
			0,
		},
		{
			"softreset",
			core.InstanceActionActionSoftreset,
			"",
			core.InstanceLifecycleStateRunning,
			core.InstanceLifecycleStateRunning,
			0,
		},
		{
			"reset while stopped",
			core.InstanceActionActionReset,
			core.InstanceActionActionStop,
			"",
			"",
			http.StatusConflict,
		},
		{"unsupported", core.InstanceActionActionRebootmigrate, "", "", "", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			f := e.fixture(t)
			i := e.mustLaunch(t, f.pub)
			e.settle()

			if tt.prepare != "" {
				_, err := e.compute.InstanceAction(
					ctx,
					core.InstanceActionRequest{InstanceId: i.Id, Action: tt.prepare},
				)
				require.NoError(t, err)
				e.settle()
			}

			r, err := e.compute.InstanceAction(ctx, core.InstanceActionRequest{InstanceId: i.Id, Action: tt.action})
			if tt.status != 0 {
				assert.Equal(t, tt.status, svcErr(t, err).GetHTTPStatusCode())

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantState, r.LifecycleState)
			e.settle()
			assert.Equal(t, tt.settled, e.getInstance(t, i.Id).LifecycleState)
		})
	}
}

func TestUpdateInstance(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	f := e.fixture(t)
	i := e.mustLaunch(t, f.pub)
	e.settle()

	r, err := e.compute.UpdateInstance(ctx, core.UpdateInstanceRequest{
		InstanceId:  i.Id,
		DisplayName: new("renamed"),
		Metadata:    map[string]string{"k": "v"},
	})
	require.NoError(t, err)
	assert.Equal(t, "renamed", *r.DisplayName)
	assert.Equal(t, "v", r.Metadata["k"])

	_, err = e.compute.UpdateInstance(ctx, core.UpdateInstanceRequest{InstanceId: i.Id, IfMatch: new("bogus"),
		DisplayName: new("x")})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	resize := core.UpdateInstanceRequest{InstanceId: i.Id,
		Shape: new("VM.Standard3.Flex"), ShapeConfig: &core.UpdateInstanceShapeConfigDetails{Ocpus: new(float32(4))}}
	_, err = e.compute.UpdateInstance(ctx, resize)
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "running instance cannot change shape")

	_, err = e.compute.InstanceAction(
		ctx,
		core.InstanceActionRequest{InstanceId: i.Id, Action: core.InstanceActionActionStop},
	)
	require.NoError(t, err)
	e.settle()

	r, err = e.compute.UpdateInstance(ctx, resize)
	require.NoError(t, err)
	assert.Equal(t, "VM.Standard3.Flex", *r.Shape)
	assert.InDelta(t, 4, *r.ShapeConfig.Ocpus, 0)

	resize.Shape = new("VM.Standard.A1.Flex")
	_, err = e.compute.UpdateInstance(ctx, resize)
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(), "x86 image on an arm shape")

	resize.Shape = new("VM.Nope")
	_, err = e.compute.UpdateInstance(ctx, resize)
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
}

func TestTerminateInstance(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	tests := []struct {
		name     string
		preserve bool
	}{{"delete boot volume", false}, {"preserve boot volume", true}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			f := e.fixture(t)
			i := e.mustLaunch(t, f.pub)
			e.settle()

			vn := e.vnicOf(t, i.Id)
			bas, err := e.compute.ListBootVolumeAttachments(ctx, core.ListBootVolumeAttachmentsRequest{
				CompartmentId: new(testComp), AvailabilityDomain: new(testAD), InstanceId: i.Id})
			require.NoError(t, err)

			_, err = e.client.DeleteSubnet(ctx, core.DeleteSubnetRequest{SubnetId: f.pub.Id})
			assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "subnet holds a VNIC")

			_, err = e.compute.TerminateInstance(
				ctx,
				core.TerminateInstanceRequest{InstanceId: i.Id, PreserveBootVolume: new(tt.preserve)},
			)
			require.NoError(t, err)
			assert.Equal(t, core.InstanceLifecycleStateTerminating, e.getInstance(t, i.Id).LifecycleState)

			e.settle()
			assert.Equal(t, core.InstanceLifecycleStateTerminated, e.getInstance(t, i.Id).LifecycleState)

			g, err := e.client.GetVnic(ctx, core.GetVnicRequest{VnicId: vn.Id})
			require.NoError(t, err)
			assert.Equal(t, core.VnicLifecycleStateTerminated, g.LifecycleState)

			bv, err := e.block.GetBootVolume(ctx, core.GetBootVolumeRequest{BootVolumeId: bas.Items[0].BootVolumeId})
			require.NoError(t, err)

			if tt.preserve {
				assert.Equal(t, core.BootVolumeLifecycleStateAvailable, bv.LifecycleState)
			} else {
				assert.Equal(t, core.BootVolumeLifecycleStateTerminated, bv.LifecycleState)
			}

			ba, err := e.compute.GetBootVolumeAttachment(
				ctx,
				core.GetBootVolumeAttachmentRequest{BootVolumeAttachmentId: bas.Items[0].Id},
			)
			require.NoError(t, err)
			assert.Equal(t, core.BootVolumeAttachmentLifecycleStateDetached, ba.LifecycleState)

			_, err = e.compute.InstanceAction(
				ctx,
				core.InstanceActionRequest{InstanceId: i.Id, Action: core.InstanceActionActionStart},
			)
			assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}

func TestLaunchFromPreservedBootVolume(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	f := e.fixture(t)
	i := e.mustLaunch(t, f.pub)
	e.settle()

	bas, err := e.compute.ListBootVolumeAttachments(ctx, core.ListBootVolumeAttachmentsRequest{
		CompartmentId: new(testComp), AvailabilityDomain: new(testAD), InstanceId: i.Id})
	require.NoError(t, err)

	bvID := bas.Items[0].BootVolumeId
	src := func(d *core.LaunchInstanceDetails) {
		d.SourceDetails = core.InstanceSourceViaBootVolumeDetails{BootVolumeId: bvID}
	}

	_, err = e.launch(t, f.pub, src)
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "boot volume still attached")

	_, err = e.compute.TerminateInstance(
		ctx,
		core.TerminateInstanceRequest{InstanceId: i.Id, PreserveBootVolume: new(true)},
	)
	require.NoError(t, err)
	e.settle()

	n, err := e.launch(t, f.pub, src)
	require.NoError(t, err)

	e.settle()
	assert.Equal(t, core.InstanceLifecycleStateRunning, e.getInstance(t, n.Id).LifecycleState)
}

func TestCreateImageFromInstance(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	f := e.fixture(t)
	i := e.mustLaunch(t, f.pub)
	e.settle()

	r, err := e.compute.CreateImage(ctx, core.CreateImageRequest{
		CompartmentId: new(testComp), InstanceId: i.Id, DisplayName: new("golden")})
	require.NoError(t, err)
	assert.Equal(t, "Oracle Linux", *r.OperatingSystem)
	assert.Equal(t, core.ImageLifecycleStateProvisioning, r.LifecycleState)

	e.settle()

	l, err := e.compute.ListImages(
		ctx,
		core.ListImagesRequest{CompartmentId: new(testComp), DisplayName: new("golden")},
	)
	require.NoError(t, err)
	require.Len(t, l.Items, 1)
	assert.Equal(t, core.ImageLifecycleStateAvailable, l.Items[0].LifecycleState)

	n, err := e.launch(t, f.pub, func(d *core.LaunchInstanceDetails) {
		d.SourceDetails = core.InstanceSourceViaImageDetails{ImageId: r.Id}
	})
	require.NoError(t, err)
	assert.Equal(t, r.Id, n.ImageId)

	_, err = e.compute.DeleteImage(ctx, core.DeleteImageRequest{ImageId: r.Id})
	require.NoError(t, err)

	g, err := e.compute.GetImage(ctx, core.GetImageRequest{ImageId: r.Id})
	require.NoError(t, err)
	assert.Equal(t, core.ImageLifecycleStateDeleted, g.LifecycleState)

	_, err = e.compute.DeleteImage(ctx, core.DeleteImageRequest{ImageId: e.imageID(t, "Oracle Linux", "9", false)})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.compute.CreateImage(ctx, core.CreateImageRequest{
		CompartmentId: new(testComp), InstanceId: new("ocid1.instance.oc1.iad.nope")})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestVnicsAndPrivateIps(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	f := e.fixture(t)
	i := e.mustLaunch(t, f.pub)
	e.settle()

	nsg, err := e.client.CreateNetworkSecurityGroup(ctx, core.CreateNetworkSecurityGroupRequest{
		CompartmentId: new(testComp),
		VcnId:         f.vcn.Id,
	})
	require.NoError(t, err)

	att, err := e.compute.AttachVnic(ctx, core.AttachVnicRequest{
		InstanceId:        i.Id,
		CreateVnicDetails: &core.CreateVnicDetails{SubnetId: f.private.Id, NsgIds: []string{*nsg.Id}}})
	require.NoError(t, err)
	assert.Equal(t, core.VnicAttachmentLifecycleStateAttaching, att.LifecycleState)
	assert.Equal(t, 1, *att.NicIndex)

	e.settle()

	members, err := e.client.ListNetworkSecurityGroupVnics(
		ctx,
		core.ListNetworkSecurityGroupVnicsRequest{NetworkSecurityGroupId: nsg.Id},
	)
	require.NoError(t, err)
	require.Len(t, members.Items, 1)
	assert.Equal(t, att.VnicId, members.Items[0].VnicId)
	assert.Equal(t, i.Id, members.Items[0].ResourceId)

	_, err = e.client.DeleteNetworkSecurityGroup(
		ctx,
		core.DeleteNetworkSecurityGroupRequest{NetworkSecurityGroupId: nsg.Id},
	)
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.UpdateVnic(
		ctx,
		core.UpdateVnicRequest{VnicId: att.VnicId,
			DisplayName: new("second"), NsgIds: []string{}, SkipSourceDestCheck: new(true)},
	)
	require.NoError(t, err)

	v, err := e.client.GetVnic(ctx, core.GetVnicRequest{VnicId: att.VnicId})
	require.NoError(t, err)
	assert.Equal(t, "second", *v.DisplayName)
	assert.True(t, *v.SkipSourceDestCheck)
	assert.Empty(t, v.NsgIds)
	assert.False(t, *v.IsPrimary)

	sec, err := e.client.CreatePrivateIp(
		ctx,
		core.CreatePrivateIpRequest{
			VnicId: att.VnicId, DisplayName: new("sec")},
	)
	require.NoError(t, err)
	assert.Equal(t, "10.0.2.3", *sec.IpAddress)
	assert.False(t, *sec.IsPrimary)

	_, err = e.client.CreatePrivateIp(
		ctx,
		core.CreatePrivateIpRequest{
			VnicId: att.VnicId, IpAddress: new("10.0.2.3")},
	)
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	l, err := e.client.ListPrivateIps(
		ctx,
		core.ListPrivateIpsRequest{SubnetId: f.private.Id, IpAddress: new("10.0.2.3")},
	)
	require.NoError(t, err)
	require.Len(t, l.Items, 1)

	_, err = e.client.ListPrivateIps(ctx, core.ListPrivateIpsRequest{})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

	// route rule targets resolve to real private IPs
	rtReq := func(target *string, vcn *string) core.CreateRouteTableRequest {
		return core.CreateRouteTableRequest{
			CompartmentId: new(testComp), VcnId: vcn,
			RouteRules: []core.RouteRule{{Destination: new("172.16.0.0/12"), NetworkEntityId: target}}}
	}

	_, err = e.client.CreateRouteTable(ctx, rtReq(sec.Id, f.vcn.Id))
	require.NoError(t, err)

	_, err = e.client.CreateRouteTable(ctx, rtReq(new("ocid1.privateip.oc1.iad.nope"), f.vcn.Id))
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	otherVcn := e.mkVcn(t, "10.9.0.0/16", "o")
	_, err = e.client.CreateRouteTable(ctx, rtReq(sec.Id, otherVcn.Id))
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.DeletePrivateIp(ctx, core.DeletePrivateIpRequest{PrivateIpId: sec.Id})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "route target")

	prim, err := e.client.ListPrivateIps(ctx, core.ListPrivateIpsRequest{VnicId: att.VnicId})
	require.NoError(t, err)
	require.Len(t, prim.Items, 2)

	for _, p := range prim.Items {
		if *p.IsPrimary {
			_, err = e.client.DeletePrivateIp(ctx, core.DeletePrivateIpRequest{PrivateIpId: p.Id})
			assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())
		}
	}

	// detaching terminates the VNIC and drops its private IPs
	_, err = e.compute.DetachVnic(ctx, core.DetachVnicRequest{VnicAttachmentId: att.Id})
	require.NoError(t, err)
	e.settle()

	a, err := e.compute.GetVnicAttachment(ctx, core.GetVnicAttachmentRequest{VnicAttachmentId: att.Id})
	require.NoError(t, err)
	assert.Equal(t, core.VnicAttachmentLifecycleStateDetached, a.LifecycleState)

	_, err = e.client.GetPrivateIp(ctx, core.GetPrivateIpRequest{PrivateIpId: sec.Id})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	pa, err := e.compute.ListVnicAttachments(
		ctx,
		core.ListVnicAttachmentsRequest{CompartmentId: new(testComp), InstanceId: i.Id},
	)
	require.NoError(t, err)

	for _, x := range pa.Items {
		if *x.NicIndex == 0 {
			_, err = e.compute.DetachVnic(ctx, core.DetachVnicRequest{VnicAttachmentId: x.Id})
			assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "primary VNIC")
		}
	}
}

func TestReservedPrivateIpMove(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	f := e.fixture(t)
	a, b := e.mustLaunch(t, f.pub), e.mustLaunch(t, f.pub)
	e.settle()

	va, vb := e.vnicOf(t, a.Id), e.vnicOf(t, b.Id)

	r, err := e.client.CreatePrivateIp(
		ctx,
		core.CreatePrivateIpRequest{
			SubnetId: f.pub.Id, Lifetime: core.CreatePrivateIpDetailsLifetimeReserved},
	)
	require.NoError(t, err)
	assert.Equal(t, core.PrivateIpIpStateAvailable, r.IpState)
	assert.Nil(t, r.VnicId)

	for _, vn := range []core.Vnic{va, vb} {
		u, uerr := e.client.UpdatePrivateIp(ctx, core.UpdatePrivateIpRequest{PrivateIpId: r.Id,
			VnicId: vn.Id})
		require.NoError(t, uerr)
		assert.Equal(t, vn.Id, u.VnicId)
	}

	_, err = e.client.CreatePrivateIp(
		ctx,
		core.CreatePrivateIpRequest{
			SubnetId: f.pub.Id, Lifetime: core.CreatePrivateIpDetailsLifetimeEphemeral},
	)
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.UpdatePrivateIp(ctx, core.UpdatePrivateIpRequest{PrivateIpId: r.Id,
		VnicId: new("ocid1.vnic.oc1.iad.nope")})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.compute.TerminateInstance(ctx, core.TerminateInstanceRequest{InstanceId: b.Id})
	require.NoError(t, err)
	e.settle()

	g, err := e.client.GetPrivateIp(ctx, core.GetPrivateIpRequest{PrivateIpId: r.Id})
	require.NoError(t, err)
	assert.Equal(t, core.PrivateIpIpStateAvailable, g.IpState, "reserved IP survives its VNIC")
	assert.Nil(t, g.VnicId)
}

func TestPublicIPAssignment(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	f := e.fixture(t)
	i, err := e.launch(t, f.private, nil)
	require.NoError(t, err)

	e.settle()

	vn := e.vnicOf(t, i.Id)
	prim, err := e.client.ListPrivateIps(ctx, core.ListPrivateIpsRequest{VnicId: vn.Id})
	require.NoError(t, err)

	pid := prim.Items[0].Id

	res, err := e.client.CreatePublicIp(
		ctx,
		core.CreatePublicIpRequest{
			CompartmentId: new(testComp), Lifetime: core.CreatePublicIpDetailsLifetimeReserved},
	)
	require.NoError(t, err)

	u, err := e.client.UpdatePublicIp(ctx, core.UpdatePublicIpRequest{PublicIpId: res.Id,
		PrivateIpId: pid})
	require.NoError(t, err)
	assert.Equal(t, pid, u.AssignedEntityId)
	assert.Equal(t, core.PublicIpAssignedEntityTypePrivateIp, u.AssignedEntityType)

	v, err := e.client.GetVnic(ctx, core.GetVnicRequest{VnicId: vn.Id})
	require.NoError(t, err)
	assert.Equal(t, res.IpAddress, v.PublicIp)

	other, err := e.client.CreatePublicIp(
		ctx,
		core.CreatePublicIpRequest{
			CompartmentId: new(testComp), Lifetime: core.CreatePublicIpDetailsLifetimeEphemeral, PrivateIpId: pid},
	)
	assert.Nil(t, other.Id)
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.UpdatePublicIp(ctx, core.UpdatePublicIpRequest{PublicIpId: res.Id,
		PrivateIpId: new("")})
	require.NoError(t, err)

	v, err = e.client.GetVnic(ctx, core.GetVnicRequest{VnicId: vn.Id})
	require.NoError(t, err)
	assert.Nil(t, v.PublicIp)

	eph, err := e.client.CreatePublicIp(
		ctx,
		core.CreatePublicIpRequest{
			CompartmentId: new(testComp), Lifetime: core.CreatePublicIpDetailsLifetimeEphemeral, PrivateIpId: pid},
	)
	require.NoError(t, err)

	_, err = e.client.UpdatePublicIp(ctx, core.UpdatePublicIpRequest{PublicIpId: eph.Id,
		PrivateIpId: new("")})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.DeletePublicIp(ctx, core.DeletePublicIpRequest{PublicIpId: eph.Id})
	require.NoError(t, err)

	v, err = e.client.GetVnic(ctx, core.GetVnicRequest{VnicId: vn.Id})
	require.NoError(t, err)
	assert.Nil(t, v.PublicIp)
}

func TestVolumeLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()

	mk := func(d core.CreateVolumeDetails) (core.Volume, error) {
		d.CompartmentId = new(testComp)
		r, err := e.block.CreateVolume(ctx, core.CreateVolumeRequest{CreateVolumeDetails: d})

		return r.Volume, err
	}

	tests := []struct {
		d      core.CreateVolumeDetails
		name   string
		code   string
		status int
	}{
		{
			core.CreateVolumeDetails{
				AvailabilityDomain: new(testAD),
				SizeInGBs:          new(int64(100)),
				DisplayName:        new("v"),
			},
			"ok",
			"",
			0,
		},
		{core.CreateVolumeDetails{AvailabilityDomain: new(testAD)}, "default size", "", 0},
		{
			core.CreateVolumeDetails{
				AvailabilityDomain: new(testAD),
				SizeInMBs:          new(int64(51200)),
				VpusPerGB:          new(int64(20)),
			},
			"mbs",
			"",
			0,
		},
		{core.CreateVolumeDetails{SizeInGBs: new(int64(100))}, "missing ad", "MissingParameter", 400},
		{
			core.CreateVolumeDetails{AvailabilityDomain: new(testAD), SizeInGBs: new(int64(10))},
			"too small",
			"InvalidParameter",
			400,
		},
		{
			core.CreateVolumeDetails{AvailabilityDomain: new(testAD), SizeInGBs: new(int64(40000))},
			"too large",
			"InvalidParameter",
			400,
		},
		{
			core.CreateVolumeDetails{AvailabilityDomain: new(testAD), VpusPerGB: new(int64(15))},
			"bad vpus",
			"InvalidParameter",
			400,
		},
		{core.CreateVolumeDetails{AvailabilityDomain: new("zzz")}, "bad ad", "InvalidParameter", 400},
		{
			core.CreateVolumeDetails{
				AvailabilityDomain: new(testAD),
				BackupPolicyId:     new("ocid1.volumebackuppolicy.oc1..nope"),
			},
			"unknown policy",
			"NotAuthorizedOrNotFound",
			404,
		},
		{
			core.CreateVolumeDetails{
				AvailabilityDomain: new(testAD),
				VolumeBackupId:     new("ocid1.volumebackup.oc1.iad.nope"),
			},
			"unknown backup",
			"NotAuthorizedOrNotFound",
			404,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			v, err := mk(tt.d)
			if tt.status != 0 {
				se := svcErr(t, err)
				assert.Equal(t, tt.status, se.GetHTTPStatusCode())
				assert.Equal(t, tt.code, se.GetCode())

				return
			}

			require.NoError(t, err)
			assert.Equal(t, core.VolumeLifecycleStateProvisioning, v.LifecycleState)
			assert.Equal(t, *v.SizeInGBs*1024, *v.SizeInMBs)
		})
	}

	v, err := mk(
		core.CreateVolumeDetails{AvailabilityDomain: new(testAD), SizeInGBs: new(int64(60)), DisplayName: new("grow")},
	)
	require.NoError(t, err)

	e.settle()

	g, err := e.block.GetVolume(ctx, core.GetVolumeRequest{VolumeId: v.Id})
	require.NoError(t, err)
	assert.Equal(t, core.VolumeLifecycleStateAvailable, g.LifecycleState)
	assert.EqualValues(t, 10, *g.VpusPerGB)

	u, err := e.block.UpdateVolume(
		ctx,
		core.UpdateVolumeRequest{VolumeId: v.Id,
			SizeInGBs: new(int64(80)), VpusPerGB: new(int64(30)), DisplayName: new("grown")},
	)
	require.NoError(t, err)
	assert.EqualValues(t, 80, *u.SizeInGBs)
	assert.EqualValues(t, 30, *u.VpusPerGB)

	_, err = e.block.UpdateVolume(
		ctx,
		core.UpdateVolumeRequest{
			VolumeId:  v.Id,
			SizeInGBs: new(int64(70)),
		},
	)
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(), "no shrinking")

	clone, err := mk(
		core.CreateVolumeDetails{
			AvailabilityDomain: new(testAD),
			SourceDetails:      core.VolumeSourceFromVolumeDetails{Id: v.Id},
		},
	)
	require.NoError(t, err)
	assert.EqualValues(t, 80, *clone.SizeInGBs)

	_, err = mk(
		core.CreateVolumeDetails{
			AvailabilityDomain: new(testAD2),
			SourceDetails:      core.VolumeSourceFromVolumeDetails{Id: v.Id},
		},
	)
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(), "clone across ADs")

	l, err := e.block.ListVolumes(ctx, core.ListVolumesRequest{CompartmentId: new(testComp), DisplayName: new("grown")})
	require.NoError(t, err)
	assert.Len(t, l.Items, 1)

	_, err = e.block.DeleteVolume(ctx, core.DeleteVolumeRequest{VolumeId: v.Id})
	require.NoError(t, err)

	e.settle()

	g, err = e.block.GetVolume(ctx, core.GetVolumeRequest{VolumeId: v.Id})
	require.NoError(t, err)
	assert.Equal(t, core.VolumeLifecycleStateTerminated, g.LifecycleState)
}

func TestVolumeAttachments(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	f := e.fixture(t)
	i := e.mustLaunch(t, f.pub)
	e.settle()

	vol := func(ad string) core.Volume {
		r, err := e.block.CreateVolume(ctx, core.CreateVolumeRequest{
			CompartmentId: new(testComp), AvailabilityDomain: new(ad)})
		require.NoError(t, err)

		return r.Volume
	}

	v1, v2, far := vol(testAD), vol(testAD), vol(testAD2)
	e.settle()

	iscsi, err := e.compute.AttachVolume(
		ctx,
		core.AttachVolumeRequest{AttachVolumeDetails: core.AttachIScsiVolumeDetails{
			InstanceId: i.Id, VolumeId: v1.Id, UseChap: new(true), Device: new("/dev/oracleoci/oraclevdb")}},
	)
	require.NoError(t, err)

	ia, ok := iscsi.VolumeAttachment.(core.IScsiVolumeAttachment)
	require.True(t, ok)
	assert.Equal(t, core.VolumeAttachmentLifecycleStateAttaching, ia.LifecycleState)
	assert.Equal(t, 3260, *ia.Port)
	assert.Contains(t, *ia.Iqn, "iqn.")
	assert.NotEmpty(t, *ia.ChapSecret)

	para, err := e.compute.AttachVolume(
		ctx,
		core.AttachVolumeRequest{AttachVolumeDetails: core.AttachParavirtualizedVolumeDetails{
			InstanceId: i.Id, VolumeId: v2.Id, IsReadOnly: new(true)}},
	)
	require.NoError(t, err)

	pa, ok := para.VolumeAttachment.(core.ParavirtualizedVolumeAttachment)
	require.True(t, ok)
	assert.True(t, *pa.IsReadOnly)

	e.settle()

	got, err := e.compute.GetVolumeAttachment(ctx, core.GetVolumeAttachmentRequest{VolumeAttachmentId: ia.Id})
	require.NoError(t, err)
	assert.Equal(t, core.VolumeAttachmentLifecycleStateAttached, got.GetLifecycleState())

	l, err := e.compute.ListVolumeAttachments(
		ctx,
		core.ListVolumeAttachmentsRequest{CompartmentId: new(testComp), InstanceId: i.Id},
	)
	require.NoError(t, err)
	assert.Len(t, l.Items, 2)

	tests := []struct {
		d      core.AttachVolumeDetails
		name   string
		status int
	}{
		{core.AttachIScsiVolumeDetails{InstanceId: i.Id, VolumeId: v1.Id}, "already attached", http.StatusConflict},
		{core.AttachIScsiVolumeDetails{InstanceId: i.Id, VolumeId: far.Id}, "other ad", http.StatusBadRequest},
		{
			core.AttachIScsiVolumeDetails{InstanceId: i.Id, VolumeId: new("ocid1.volume.oc1.iad.nope")},
			"unknown volume",
			http.StatusNotFound,
		},
		{
			core.AttachIScsiVolumeDetails{InstanceId: new("ocid1.instance.oc1.iad.nope"), VolumeId: v1.Id},
			"unknown instance",
			http.StatusNotFound,
		},
		{
			core.AttachIScsiVolumeDetails{InstanceId: i.Id, VolumeId: v1.Id, Device: new("/dev/sdb")},
			"bad device",
			http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		_, aerr := e.compute.AttachVolume(ctx, core.AttachVolumeRequest{AttachVolumeDetails: tt.d})
		assert.Equal(t, tt.status, svcErr(t, aerr).GetHTTPStatusCode(), tt.name)
	}

	_, err = e.block.DeleteVolume(ctx, core.DeleteVolumeRequest{VolumeId: v1.Id})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "attached volume")

	other := e.mustLaunch(t, f.pub)
	e.settle()

	_, err = e.compute.AttachVolume(ctx, core.AttachVolumeRequest{AttachVolumeDetails: core.AttachIScsiVolumeDetails{
		InstanceId: other.Id, VolumeId: v1.Id}})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "not shareable")

	_, err = e.compute.DetachVolume(ctx, core.DetachVolumeRequest{VolumeAttachmentId: ia.Id})
	require.NoError(t, err)

	d, err := e.compute.GetVolumeAttachment(ctx, core.GetVolumeAttachmentRequest{VolumeAttachmentId: ia.Id})
	require.NoError(t, err)
	assert.Equal(t, core.VolumeAttachmentLifecycleStateDetaching, d.GetLifecycleState())

	e.settle()

	d, err = e.compute.GetVolumeAttachment(ctx, core.GetVolumeAttachmentRequest{VolumeAttachmentId: ia.Id})
	require.NoError(t, err)
	assert.Equal(t, core.VolumeAttachmentLifecycleStateDetached, d.GetLifecycleState())

	_, err = e.block.DeleteVolume(ctx, core.DeleteVolumeRequest{VolumeId: v1.Id})
	require.NoError(t, err)

	// terminating the instance detaches what is left
	_, err = e.compute.TerminateInstance(ctx, core.TerminateInstanceRequest{InstanceId: i.Id})
	require.NoError(t, err)
	e.settle()

	d, err = e.compute.GetVolumeAttachment(ctx, core.GetVolumeAttachmentRequest{VolumeAttachmentId: pa.Id})
	require.NoError(t, err)
	assert.Equal(t, core.VolumeAttachmentLifecycleStateDetached, d.GetLifecycleState())
}

func TestVolumeBackupsAndPolicies(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()

	vr, err := e.block.CreateVolume(ctx, core.CreateVolumeRequest{
		CompartmentId: new(testComp), AvailabilityDomain: new(testAD), SizeInGBs: new(int64(75))})
	require.NoError(t, err)

	vol := vr.Volume

	_, err = e.block.CreateVolumeBackup(
		ctx,
		core.CreateVolumeBackupRequest{VolumeId: vol.Id},
	)
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "volume still provisioning")

	e.settle()

	first, err := e.block.CreateVolumeBackup(
		ctx,
		core.CreateVolumeBackupRequest{
			VolumeId: vol.Id, DisplayName: new("b1")},
	)
	require.NoError(t, err)
	assert.Equal(t, core.VolumeBackupTypeFull, first.Type, "first backup is a full one")
	assert.Equal(t, core.VolumeBackupLifecycleStateCreating, first.LifecycleState)
	assert.EqualValues(t, 75, *first.SizeInGBs)

	second, err := e.block.CreateVolumeBackup(
		ctx,
		core.CreateVolumeBackupRequest{VolumeId: vol.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, core.VolumeBackupTypeIncremental, second.Type)

	_, err = e.block.CreateVolumeBackup(
		ctx,
		core.CreateVolumeBackupRequest{
			VolumeId: new("ocid1.volume.oc1.iad.nope"),
		},
	)
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	e.settle()

	l, err := e.block.ListVolumeBackups(
		ctx,
		core.ListVolumeBackupsRequest{CompartmentId: new(testComp), VolumeId: vol.Id},
	)
	require.NoError(t, err)
	assert.Len(t, l.Items, 2)

	_, err = e.block.UpdateVolumeBackup(ctx, core.UpdateVolumeBackupRequest{VolumeBackupId: first.Id,
		DisplayName: new("renamed")})
	require.NoError(t, err)

	restored, err := e.block.CreateVolume(ctx, core.CreateVolumeRequest{
		CompartmentId: new(
			testComp,
		),
		AvailabilityDomain: new(testAD2),
		SourceDetails:      core.VolumeSourceFromVolumeBackupDetails{Id: first.Id}})
	require.NoError(t, err)
	assert.Equal(t, core.VolumeLifecycleStateRestoring, restored.LifecycleState)
	assert.EqualValues(t, 75, *restored.SizeInGBs)

	e.settle()

	rg, err := e.block.GetVolume(ctx, core.GetVolumeRequest{VolumeId: restored.Id})
	require.NoError(t, err)
	assert.Equal(t, core.VolumeLifecycleStateAvailable, rg.LifecycleState)

	_, err = e.block.DeleteVolumeBackup(ctx, core.DeleteVolumeBackupRequest{VolumeBackupId: second.Id})
	require.NoError(t, err)

	// predefined and custom policies
	pols, err := e.block.ListVolumeBackupPolicies(ctx, core.ListVolumeBackupPoliciesRequest{})
	require.NoError(t, err)

	byName := map[string]core.VolumeBackupPolicy{}
	for _, p := range pols.Items {
		byName[*p.DisplayName] = p
	}

	require.Contains(t, byName, "gold")
	assert.Len(t, byName["gold"].Schedules, 4)
	assert.Len(t, byName["bronze"].Schedules, 2)

	custom, err := e.block.CreateVolumeBackupPolicy(
		ctx,
		core.CreateVolumeBackupPolicyRequest{
			CompartmentId: new(testComp), DisplayName: new("nightly"), Schedules: []core.VolumeBackupSchedule{{
				BackupType: core.VolumeBackupScheduleBackupTypeIncremental, Period: core.VolumeBackupSchedulePeriodDay,
				RetentionSeconds: new(86400 * 7)}}},
	)
	require.NoError(t, err)

	_, err = e.block.CreateVolumeBackupPolicy(
		ctx,
		core.CreateVolumeBackupPolicyRequest{
			CompartmentId: new(
				testComp,
			),
			Schedules: []core.VolumeBackupSchedule{
				{BackupType: "NOPE", Period: core.VolumeBackupSchedulePeriodDay, RetentionSeconds: new(1)},
			}},
	)
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

	lc, err := e.block.ListVolumeBackupPolicies(ctx, core.ListVolumeBackupPoliciesRequest{CompartmentId: new(testComp)})
	require.NoError(t, err)
	assert.Len(t, lc.Items, 4)

	asg, err := e.block.CreateVolumeBackupPolicyAssignment(ctx, core.CreateVolumeBackupPolicyAssignmentRequest{
		AssetId:  vol.Id,
		PolicyId: custom.Id,
	})
	require.NoError(t, err)

	_, err = e.block.CreateVolumeBackupPolicyAssignment(ctx, core.CreateVolumeBackupPolicyAssignmentRequest{
		AssetId:  vol.Id,
		PolicyId: byName["gold"].Id,
	})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "one policy per asset")

	_, err = e.block.DeleteVolumeBackupPolicy(ctx, core.DeleteVolumeBackupPolicyRequest{PolicyId: custom.Id})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "assigned policy")

	ga, err := e.block.GetVolumeBackupPolicyAssetAssignment(
		ctx,
		core.GetVolumeBackupPolicyAssetAssignmentRequest{AssetId: vol.Id},
	)
	require.NoError(t, err)
	require.Len(t, ga.Items, 1)
	assert.Equal(t, custom.Id, ga.Items[0].PolicyId)

	gone, err := e.block.GetVolumeBackupPolicyAssignment(
		ctx,
		core.GetVolumeBackupPolicyAssignmentRequest{PolicyAssignmentId: asg.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, vol.Id, gone.AssetId)

	_, err = e.block.DeleteVolumeBackupPolicyAssignment(
		ctx,
		core.DeleteVolumeBackupPolicyAssignmentRequest{PolicyAssignmentId: asg.Id},
	)
	require.NoError(t, err)

	_, err = e.block.UpdateVolumeBackupPolicy(ctx, core.UpdateVolumeBackupPolicyRequest{
		PolicyId:    custom.Id,
		DisplayName: new("weekly"),
		Schedules:   []core.VolumeBackupSchedule{},
	})
	require.NoError(t, err)

	_, err = e.block.DeleteVolumeBackupPolicy(ctx, core.DeleteVolumeBackupPolicyRequest{PolicyId: custom.Id})
	require.NoError(t, err)

	// a policy given at volume creation is assigned
	pv, err := e.block.CreateVolume(ctx, core.CreateVolumeRequest{
		CompartmentId: new(testComp), AvailabilityDomain: new(testAD), BackupPolicyId: byName["silver"].Id})
	require.NoError(t, err)

	ga, err = e.block.GetVolumeBackupPolicyAssetAssignment(
		ctx,
		core.GetVolumeBackupPolicyAssetAssignmentRequest{AssetId: pv.Id},
	)
	require.NoError(t, err)
	assert.Len(t, ga.Items, 1)
}

func TestComputePersistence(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	f := e.fixture(t)
	i := e.mustLaunch(t, f.pub)
	e.settle()

	_, err := e.block.CreateVolume(ctx, core.CreateVolumeRequest{
		CompartmentId: new(testComp), AvailabilityDomain: new(testAD)})
	require.NoError(t, err)

	snap := e.backend.Snapshot(ctx)

	e2 := newEnv(t, false)
	require.NoError(t, e2.backend.Restore(ctx, snap))

	got := e2.getInstance(t, i.Id)
	assert.Equal(t, core.InstanceLifecycleStateRunning, got.LifecycleState)

	vn := e2.vnicOf(t, i.Id)
	assert.Equal(t, "10.0.1.2", *vn.PrivateIp)
	assert.NotEmpty(t, *vn.PublicIp)

	bvs, err := e2.block.ListBootVolumes(
		ctx,
		core.ListBootVolumesRequest{CompartmentId: new(testComp), AvailabilityDomain: new(testAD)},
	)
	require.NoError(t, err)
	assert.Len(t, bvs.Items, 1)

	vols, err := e2.block.ListVolumes(ctx, core.ListVolumesRequest{CompartmentId: new(testComp)})
	require.NoError(t, err)
	assert.Len(t, vols.Items, 1)
}
