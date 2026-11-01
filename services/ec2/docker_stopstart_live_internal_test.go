package ec2

import (
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDockerComputeLiveStopStartKeepsFilesystem(t *testing.T) {
	t.Parallel()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not found")
	}

	dc, _ := newLiveCompute(t, nil)
	res := launchLive(t, dc, LaunchRequest{InstanceID: "i-stopstart01"})

	_, err := dockerExec(t.Context(), res.ProviderID, "echo kept > /root/root-volume.txt")
	require.NoError(t, err)

	require.NoError(t, dc.Stop(t.Context(), "i-stopstart01", res.ProviderID))
	require.NoError(t, dc.Start(t.Context(), "i-stopstart01", res.ProviderID))

	got, err := dockerExec(t.Context(), res.ProviderID, "cat /root/root-volume.txt")
	require.NoError(t, err)
	assert.Equal(t, "kept", got, "the root filesystem must survive stop/start")
}
