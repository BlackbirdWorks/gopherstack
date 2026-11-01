package ec2

import (
	"context"
	"os/exec"
	"strings"
	"testing"

	mobyclient "github.com/moby/moby/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

// onlyResource lists just the managed containers whose resource key is id, so a live test never reconciles
// containers it did not create.
type onlyResource struct {
	container.ManagedLister
	id string
}

func (o onlyResource) ListManaged(ctx context.Context) ([]container.ManagedContainer, error) {
	all, err := o.ManagedLister.ListManaged(ctx)
	if err != nil {
		return nil, err
	}

	var out []container.ManagedContainer

	for _, c := range all {
		if strings.EqualFold(c.Labels[container.LabelResource], o.id) {
			out = append(out, c)
		}
	}

	return out, nil
}

func TestDockerComputeLiveCrashedInstanceAdopted(t *testing.T) {
	t.Parallel()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not found")
	}

	rt, err := container.NewRuntime(container.Config{})
	require.NoError(t, err)

	t.Cleanup(func() { _ = rt.Close() })

	lister, ok := rt.(container.ManagedLister)
	require.True(t, ok)

	dc1, cli := newLiveCompute(t, nil)

	b1 := NewInMemoryBackend("000000000000", "us-east-1")
	insts, err := b1.RunInstances("ami-12345678", "t3.micro", "", 1)
	require.NoError(t, err)

	id := insts[0].ID
	res := launchLive(t, dc1, LaunchRequest{InstanceID: id, ImageID: insts[0].ImageID})
	stale := res
	stale.PrivateIP = "10.9.9.9"
	require.NoError(t, b1.SetComputeResult(id, stale))

	snap := b1.Snapshot(t.Context())

	orphans, err := container.Reconcile(t.Context(), onlyResource{lister, id}, container.CurrentIdentity(), nil)
	require.NoError(t, err)

	dc2 := newDockerComputeWithAPI(dc1.api, DockerComputeConfig{Image: liveImage})
	dc2.claim = orphans.Claim

	b2 := NewInMemoryBackend("000000000000", "us-east-1")
	b2.WithCompute(dc2)

	h := NewHandler(b2)
	require.NoError(t, h.Restore(t.Context(), snap))

	assert.Equal(t, res.ProviderID, b2.LookupInstanceProviderID(id), "same container, not a replacement")

	got, found := b2.instanceCopy(id)
	require.True(t, found)
	assert.Equal(t, res.PrivateIP, got.PrivateIP, "the recorded address is refreshed from the adopted container")

	insp, err := cli.ContainerInspect(t.Context(), res.ProviderID, mobyclient.ContainerInspectOptions{})
	require.NoError(t, err)
	require.NotNil(t, insp.Container.State)
	assert.True(t, insp.Container.State.Running)

	assert.Zero(t, orphans.SweepUnclaimed(t.Context()), "nothing may be left for the sweep")

	list, err := cli.ContainerList(t.Context(), mobyclient.ContainerListOptions{
		All: true, Filters: mobyclient.Filters{}.Add("label", dockerLabelPrefix+"instance="+id),
	})
	require.NoError(t, err)
	assert.Len(t, list.Items, 1, "exactly one container backs the instance")
}
