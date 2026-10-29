package ec2

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"testing"

	mobyclient "github.com/moby/moby/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func randHex(t *testing.T, n int) string {
	t.Helper()

	b := make([]byte, n)
	_, err := rand.Read(b)
	require.NoError(t, err)

	return hex.EncodeToString(b)
}

// execExit runs a shell command in a container and returns its exit code.
func execExit(t *testing.T, cli *mobyclient.Client, containerID, script string) int {
	t.Helper()

	ex, err := cli.ExecCreate(t.Context(), containerID, mobyclient.ExecCreateOptions{Cmd: []string{"sh", "-c", script}})
	require.NoError(t, err)

	_, err = cli.ExecStart(t.Context(), ex.ID, mobyclient.ExecStartOptions{Detach: true})
	require.NoError(t, err)

	code := -1

	require.Eventually(t, func() bool {
		res, inspectErr := cli.ExecInspect(t.Context(), ex.ID, mobyclient.ExecInspectOptions{})
		if inspectErr != nil || res.Running {
			return false
		}

		code = res.ExitCode

		return true
	}, liveTimeout, liveTick)

	return code
}

func liveEBSCompute(t *testing.T, instances ...string) (*DockerCompute, *mobyclient.Client, string) {
	t.Helper()

	cmds := map[string][]string{}
	for _, id := range instances {
		cmds["gopherstack-"+id] = []string{"sleep", "600"}
	}

	dc, cli := newLiveCompute(t, cmds)
	dc.cfg.EBSVolumes = true
	owner := "live/" + randHex(t, 4)

	t.Cleanup(func() {
		dc.ReapVolumes(context.Background(), owner, map[string]struct{}{})
	})

	return dc, cli, owner
}

func TestDockerComputeLiveEBSVolumePersistsAcrossInstances(t *testing.T) {
	t.Parallel()

	suffix := randHex(t, 4)
	instA, instB := "i-ebsa"+suffix, "i-ebsb"+suffix
	dc, cli, owner := liveEBSCompute(t, instA, instB)
	vol := VolumeAttachSpec{VolumeID: "vol-live" + suffix, Device: "/dev/sdf", Owner: owner}

	a := launchLive(t, dc, LaunchRequest{InstanceID: instA})
	b := launchLive(t, dc, LaunchRequest{InstanceID: instB})

	require.NoError(t, dc.AttachEBSVolume(t.Context(), a.ProviderID, vol))
	require.Zero(t, execExit(t, cli, a.ProviderID, "echo persisted > /mnt/ebs/sdf/data.txt"))

	require.NoError(t, dc.DetachEBSVolume(t.Context(), a.ProviderID, vol))
	assert.Zero(t, execExit(t, cli, a.ProviderID, "test ! -e /mnt/ebs/sdf"), "detach clears the device path")

	require.NoError(t, dc.AttachEBSVolume(t.Context(), b.ProviderID, vol))
	assert.Zero(t, execExit(t, cli, b.ProviderID, "grep -q persisted /mnt/ebs/sdf/data.txt"),
		"file written on instance A is on the volume attached to instance B")
}

func TestDockerComputeLiveEBSLaunchMountPersistsAcrossTerminate(t *testing.T) {
	t.Parallel()

	suffix := randHex(t, 4)
	instA, instB := "i-ebsc"+suffix, "i-ebsd"+suffix
	dc, cli, owner := liveEBSCompute(t, instA, instB)
	vol := VolumeAttachSpec{VolumeID: "vol-mnt" + suffix, Device: "/dev/sdg", Owner: owner}

	a, err := dc.Launch(t.Context(), LaunchRequest{InstanceID: instA, Volumes: []VolumeAttachSpec{vol}})
	require.NoError(t, err)
	require.Zero(t, execExit(t, cli, a.ProviderID, "echo kept > /mnt/ebs/sdg/k.txt"))
	require.NoError(t, dc.Terminate(t.Context(), instA, a.ProviderID))

	b := launchLive(t, dc, LaunchRequest{InstanceID: instB, Volumes: []VolumeAttachSpec{vol}})
	assert.Zero(t, execExit(t, cli, b.ProviderID, "grep -q kept /mnt/ebs/sdg/k.txt"),
		"a retained volume's data survives termination and relaunch")
}

func TestDockerComputeLiveCommitAndLaunchFromLocalImage(t *testing.T) {
	t.Parallel()

	suffix := randHex(t, 4)
	instA, instB := "i-cmta"+suffix, "i-cmtb"+suffix
	dc, cli, _ := liveEBSCompute(t, instA, instB)
	amiID := "ami-" + randHex(t, 8)

	a := launchLive(t, dc, LaunchRequest{InstanceID: instA})
	require.Zero(t, execExit(t, cli, a.ProviderID, "echo golden > /marker"))

	ref, err := dc.CommitInstance(t.Context(), a.ProviderID, "live-"+suffix, amiID)
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = cli.ImageRemove(context.Background(), ref, mobyclient.ImageRemoveOptions{Force: true})
	})

	found, err := dc.DiscoverImages(t.Context())
	require.NoError(t, err)
	idx := indexOfAMI(found, amiID)
	require.Less(t, idx, len(found))
	assert.Equal(t, "live-"+suffix, found[idx].Name)
	assert.Equal(t, ref, found[idx].DockerRef)
	assert.Equal(t, amiID, found[idx].AMIID, "the committed image is discovered by its AMI tag")

	b := launchLive(t, dc, LaunchRequest{InstanceID: instB, DockerImage: ref})
	assert.Zero(t, execExit(t, cli, b.ProviderID, "grep -q golden /marker"), "instance runs the committed image")
}

func indexOfAMI(list []DiscoveredImage, amiID string) int {
	for i, d := range list {
		if d.AMIID == amiID {
			return i
		}
	}

	return len(list)
}
