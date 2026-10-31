package ec2

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func bdmVals(devices ...string) url.Values {
	v := url.Values{}

	for i, d := range devices {
		p := "BlockDeviceMapping." + string(rune('1'+i)) + "."
		v.Set(p+"DeviceName", d)
		v.Set(p+"Ebs.VolumeSize", "4")
		v.Set(p+"Ebs.DeleteOnTermination", map[bool]string{true: "true", false: "false"}[i == 0])
	}

	return v
}

func volumeOnDevice(t *testing.T, b *InMemoryBackend, device string) *Volume {
	t.Helper()

	for _, v := range b.DescribeVolumes(nil) {
		if v.Attachment != nil && v.Attachment.Device == device {
			return v
		}
	}

	require.FailNow(t, "no volume on device "+device)

	return nil
}

func TestEBSMountPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		device  string
		want    string
		wantErr bool
	}{
		{device: "/dev/sdf", want: "/mnt/ebs/sdf"},
		{device: "/dev/xvdb", want: "/mnt/ebs/xvdb"},
		{device: "sdg", want: "/mnt/ebs/sdg"},
		{device: "/dev/nvme1n1", want: "/mnt/ebs/nvme1n1"},
		{device: "/dev/../etc", want: "/mnt/ebs/_etc"},
		{device: "/dev/", wantErr: true},
		{device: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.device, func(t *testing.T) {
			t.Parallel()

			got, err := ebsMountPath(tt.device)
			if tt.wantErr {
				require.ErrorIs(t, err, errEBSMountInvalid)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLaunchMountsBlockDeviceMappings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		enabled   bool
		wantMount bool
	}{
		{name: "opt_in_mounts_named_volumes", enabled: true, wantMount: true},
		{name: "default_off_mounts_nothing", enabled: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b, api := newDockerHandler(t, DockerComputeConfig{EBSVolumes: tt.enabled})

			_, err := h.handleRunInstances(runVals(amiAL2, bdmVals("/dev/sdf", "/dev/sdg")), "r")
			require.NoError(t, err)
			require.Len(t, api.created, 1)

			mounts := api.created[0].HostConfig.Mounts
			if !tt.wantMount {
				assert.Empty(t, mounts)
				assert.Empty(t, api.volumes)

				return
			}

			sdf, sdg := volumeOnDevice(t, b, "/dev/sdf"), volumeOnDevice(t, b, "/dev/sdg")
			require.Len(t, mounts, 2, "root volume must not be mounted")

			got := map[string]string{}
			for _, m := range mounts {
				got[m.Target] = m.Source
			}

			assert.Equal(t, map[string]string{
				"/mnt/ebs/sdf": "gopherstack-ebs-" + sdf.ID,
				"/mnt/ebs/sdg": "gopherstack-ebs-" + sdg.ID,
			}, got)

			vol := api.volumes["gopherstack-ebs-"+sdf.ID]
			assert.Equal(t, sdf.ID, vol.Labels[labelEBSVolume])
			assert.Equal(t, "000000000000/us-east-1", vol.Labels[labelEBSOwner])
		})
	}
}

func TestDockerVolumesRemovedWithEBSVolume(t *testing.T) {
	t.Parallel()

	tests := []struct {
		act             func(t *testing.T, h *Handler, b *InMemoryBackend, keep *Volume)
		name            string
		wantKeepRemoved bool
	}{
		{
			name: "terminate_honours_delete_on_termination",
			act: func(t *testing.T, h *Handler, b *InMemoryBackend, _ *Volume) {
				t.Helper()

				_, err := h.handleTerminateInstances(
					url.Values{"InstanceId.1": {b.DescribeInstances(nil, "")[0].ID}},
					"r",
				)
				require.NoError(t, err)
			},
		},
		{
			name:            "delete_volume",
			wantKeepRemoved: true,
			act: func(t *testing.T, h *Handler, b *InMemoryBackend, keep *Volume) {
				t.Helper()

				_, err := h.handleTerminateInstances(
					url.Values{"InstanceId.1": {b.DescribeInstances(nil, "")[0].ID}},
					"r",
				)
				require.NoError(t, err)

				_, err = h.handleDeleteVolume(url.Values{"VolumeId": {keep.ID}}, "r")
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b, api := newDockerHandler(t, DockerComputeConfig{EBSVolumes: true})

			_, err := h.handleRunInstances(runVals(amiAL2, bdmVals("/dev/sdf", "/dev/sdg")), "r")
			require.NoError(t, err)

			dot, keep := volumeOnDevice(t, b, "/dev/sdf"), volumeOnDevice(t, b, "/dev/sdg")
			require.Contains(t, api.volumes, "gopherstack-ebs-"+dot.ID)
			require.Contains(t, api.volumes, "gopherstack-ebs-"+keep.ID)

			tt.act(t, h, b, keep)

			assert.NotContains(t, api.volumes, "gopherstack-ebs-"+dot.ID, "DeleteOnTermination volume is removed")

			if tt.wantKeepRemoved {
				assert.NotContains(t, api.volumes, "gopherstack-ebs-"+keep.ID, "DeleteVolume removes the docker volume")
			} else {
				assert.Contains(t, api.volumes, "gopherstack-ebs-"+keep.ID, "retained EBS volume keeps its data")
			}
		})
	}
}

func TestRuntimeAttachDetachMovesDataBetweenInstances(t *testing.T) {
	t.Parallel()

	h, b, api := newDockerHandler(t, DockerComputeConfig{EBSVolumes: true})

	_, err := h.handleRunInstances(runVals(amiAL2, nil), "r")
	require.NoError(t, err)

	api.fakeDockerAPI.containerID = "ctr-2"

	_, err = h.handleRunInstances(runVals(amiAL2, nil), "r")
	require.NoError(t, err)

	instances := b.DescribeInstances(nil, "")
	require.Len(t, instances, 2)

	byProvider := map[string]string{}
	for _, inst := range instances {
		byProvider[inst.ProviderID] = inst.ID
	}

	vol, err := b.CreateVolume("us-east-1a", "gp3", 1, "")
	require.NoError(t, err)

	volName := "gopherstack-ebs-" + vol.ID
	api.writeFile(fakeVolPrefix+vol.ID, "/seed.txt", []byte("seed"))

	attach := func(inst string) {
		t.Helper()

		_, aerr := h.handleAttachVolume(
			url.Values{"VolumeId": {vol.ID}, "InstanceId": {inst}, "Device": {"/dev/sdf"}},
			"r",
		)
		require.NoError(t, aerr)
	}
	detach := func() {
		t.Helper()

		_, derr := h.handleDetachVolume(url.Values{"VolumeId": {vol.ID}}, "r")
		require.NoError(t, derr)
	}

	attach(byProvider[richProvider])
	got, ok := api.ctrFile(richProvider, "/mnt/ebs/sdf/seed.txt")
	require.True(t, ok, "volume contents are visible at the device path")
	assert.Equal(t, "seed", string(got))
	assert.Contains(t, api.volumes, volName)

	api.writeFile(richProvider, "/mnt/ebs/sdf/written.txt", []byte("persisted"))
	detach()
	assert.Contains(t, api.execs, []string{"rm", "-rf", "--", "/mnt/ebs/sdf"}, "container path is cleared on detach")

	stored, ok := api.files[fakeVolPrefix+vol.ID]["/written.txt"]
	require.True(t, ok, "data written while attached is stored in the docker volume")
	assert.Equal(t, "persisted", string(stored))

	attach(byProvider["ctr-2"])
	got, ok = api.ctrFile("ctr-2", "/mnt/ebs/sdf/written.txt")
	require.True(t, ok, "second instance sees the first instance's file")
	assert.Equal(t, "persisted", string(got))
}

func TestTerminateSyncsRuntimeAttachedVolumeBack(t *testing.T) {
	t.Parallel()

	h, b, api := newDockerHandler(t, DockerComputeConfig{EBSVolumes: true})

	_, err := h.handleRunInstances(runVals(amiAL2, nil), "r")
	require.NoError(t, err)

	inst := b.DescribeInstances(nil, "")[0]
	vol, err := b.CreateVolume("us-east-1a", "gp3", 1, "")
	require.NoError(t, err)

	_, err = h.handleAttachVolume(
		url.Values{"VolumeId": {vol.ID}, "InstanceId": {inst.ID}, "Device": {"/dev/sdh"}},
		"r",
	)
	require.NoError(t, err)

	api.writeFile(richProvider, "/mnt/ebs/sdh/db.dat", []byte("rows"))

	_, err = h.handleTerminateInstances(url.Values{"InstanceId.1": {inst.ID}}, "r")
	require.NoError(t, err)

	stored, ok := api.files[fakeVolPrefix+vol.ID]["/db.dat"]
	require.True(t, ok)
	assert.Equal(t, "rows", string(stored))
	assert.Contains(t, api.volumes, "gopherstack-ebs-"+vol.ID, "the surviving EBS volume keeps its docker volume")
}

func TestDetachFailureKeepsVolumeAttached(t *testing.T) {
	t.Parallel()

	h, b, api := newDockerHandler(t, DockerComputeConfig{EBSVolumes: true})

	_, err := h.handleRunInstances(runVals(amiAL2, nil), "r")
	require.NoError(t, err)

	inst := b.DescribeInstances(nil, "")[0]
	vol, err := b.CreateVolume("us-east-1a", "gp3", 1, "")
	require.NoError(t, err)

	_, err = h.handleAttachVolume(
		url.Values{"VolumeId": {vol.ID}, "InstanceId": {inst.ID}, "Device": {"/dev/sdf"}},
		"r",
	)
	require.NoError(t, err)

	api.copyFromErr = errFakeBoom

	_, err = h.handleDetachVolume(url.Values{"VolumeId": {vol.ID}}, "r")
	require.ErrorIs(t, err, errFakeBoom)

	after := b.DescribeVolumes([]string{vol.ID})
	require.Len(t, after, 1)
	assert.NotNil(t, after[0].Attachment, "a failed sync must not detach the EBS volume")
}

func TestAttachVolumeDisabledLeavesContainerAlone(t *testing.T) {
	t.Parallel()

	h, b, api := newDockerHandler(t, DockerComputeConfig{})

	_, err := h.handleRunInstances(runVals(amiAL2, nil), "r")
	require.NoError(t, err)

	vol, err := b.CreateVolume("us-east-1a", "gp3", 1, "")
	require.NoError(t, err)

	_, err = h.handleAttachVolume(url.Values{
		"VolumeId": {vol.ID}, "InstanceId": {b.DescribeInstances(nil, "")[0].ID}, "Device": {"/dev/sdf"},
	}, "r")
	require.NoError(t, err)
	assert.Empty(t, api.volumes)
	assert.Empty(t, api.files)
}
