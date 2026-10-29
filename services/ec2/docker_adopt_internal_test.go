package ec2

import (
	"context"
	"testing"

	mobycontainer "github.com/moby/moby/api/types/container"
	mobyclient "github.com/moby/moby/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/sgfilter"
)

// runningFake reports the fake container as running (or not) on inspect.
type runningFake struct {
	*fakeDockerAPI
	stopped bool
}

func (f *runningFake) ContainerInspect(
	ctx context.Context, id string, opts mobyclient.ContainerInspectOptions,
) (mobyclient.ContainerInspectResult, error) {
	res, err := f.fakeDockerAPI.ContainerInspect(ctx, id, opts)
	res.Container.State = &mobycontainer.State{Running: !f.stopped}

	return res, err
}

func claimOne(c container.ManagedContainer, ok bool) func(string, string) (container.ManagedContainer, bool) {
	return func(service, resource string) (container.ManagedContainer, bool) {
		if service != "ec2" || resource != "i-adopt" {
			return container.ManagedContainer{}, false
		}

		return c, ok
	}
}

func TestLaunchStampsIdentityLabels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		admit        func(string, int) sgfilter.Admitter
		name         string
		wantResource string
	}{
		{name: "adoptable", wantResource: "i-adopt"},
		{name: "fronted never adopted", admit: func(string, int) sgfilter.Admitter { return nil }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			api := &fakeDockerAPI{containerID: "cid", containerIP: "172.17.0.9"}
			dc := newDockerComputeWithAPI(api, DockerComputeConfig{Image: defaultDockerImage})

			_, err := dc.Launch(t.Context(), LaunchRequest{InstanceID: "i-adopt", ImageID: "ami-1", Admit: tt.admit})
			require.NoError(t, err)
			require.Len(t, api.created, 1)

			l := api.created[0].Config.Labels
			assert.Equal(t, "true", l[container.LabelManaged])
			assert.Equal(t, container.CurrentIdentity().Owner, l[container.LabelOwner])
			assert.NotEmpty(t, l[container.LabelPID])
			assert.Equal(t, "ec2", l[container.LabelService])
			assert.Equal(t, tt.wantResource, l[container.LabelResource])
			assert.Equal(t, "i-adopt", l[dockerLabelPrefix+"instance"])
		})
	}
}

func TestAdoptInstance(t *testing.T) {
	t.Parallel()

	good := container.ManagedContainer{
		ID: "cid", Running: true, Labels: map[string]string{dockerLabelPrefix + "image": "ami-1"},
	}
	spec := AdoptSpec{InstanceID: "i-adopt", ProviderID: "cid", ImageID: "ami-1"}

	tests := []struct {
		spec        AdoptSpec
		name        string
		claimed     container.ManagedContainer
		wantRemoved []string
		cfg         DockerComputeConfig
		notClaimed  bool
		stopped     bool
		wantAdopt   bool
	}{
		{name: "matching container", claimed: good, spec: spec, wantAdopt: true},
		{name: "nothing held", spec: spec, notClaimed: true},
		{
			name: "different container id", claimed: good,
			spec:        AdoptSpec{InstanceID: "i-adopt", ProviderID: "other", ImageID: "ami-1"},
			wantRemoved: []string{"cid"},
		},
		{
			name: "different ami", claimed: good,
			spec:        AdoptSpec{InstanceID: "i-adopt", ProviderID: "cid", ImageID: "ami-2"},
			wantRemoved: []string{"cid"},
		},
		{name: "not running", claimed: good, spec: spec, stopped: true, wantRemoved: []string{"cid"}},
		{
			name: "ebs mapping cannot be rebuilt", claimed: good, spec: spec,
			cfg: DockerComputeConfig{EBSVolumes: true}, wantRemoved: []string{"cid"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fake := &fakeDockerAPI{containerID: "cid", containerIP: "172.17.0.9"}
			tt.cfg.Image = defaultDockerImage

			var api dockerAPI = &runningFake{fakeDockerAPI: fake, stopped: tt.stopped}
			if tt.cfg.EBSVolumes {
				api = newRichDockerAPI(fake)
			}

			dc := newDockerComputeWithAPI(api, tt.cfg)
			dc.claim = claimOne(tt.claimed, !tt.notClaimed)

			res, ok := dc.AdoptInstance(t.Context(), tt.spec)

			assert.Equal(t, tt.wantAdopt, ok)
			assert.ElementsMatch(t, tt.wantRemoved, fake.removed)

			if tt.wantAdopt {
				assert.Equal(t, "cid", res.ProviderID)
				assert.Equal(t, "172.17.0.9", res.PrivateIP)
			}
		})
	}
}

func TestRestoredInstanceReattachedToSurvivingContainer(t *testing.T) {
	t.Parallel()

	b1 := NewInMemoryBackend("000000000000", "us-east-1")
	insts, err := b1.RunInstances("ami-12345678", "t3.micro", "", 1)
	require.NoError(t, err)

	id := insts[0].ID
	require.NoError(t, b1.SetComputeResult(id, LaunchResult{ProviderID: "cid", PrivateIP: "10.9.9.9"}))

	snap := b1.Snapshot(t.Context())

	fake := &fakeDockerAPI{containerID: "cid", containerIP: "172.17.0.9"}
	dc := newDockerComputeWithAPI(&runningFake{fakeDockerAPI: fake}, DockerComputeConfig{Image: defaultDockerImage})
	dc.claim = func(service, resource string) (container.ManagedContainer, bool) {
		if service == "ec2" && resource == id {
			return container.ManagedContainer{
				ID: "cid", Running: true, Labels: map[string]string{dockerLabelPrefix + "image": "ami-12345678"},
			}, true
		}

		return container.ManagedContainer{}, false
	}

	b2 := NewInMemoryBackend("000000000000", "us-east-1")
	b2.WithCompute(dc)

	h := NewHandler(b2)
	require.NoError(t, h.Restore(t.Context(), snap))

	assert.Equal(t, "cid", b2.LookupInstanceProviderID(id), "the instance keeps its container")
	got, ok := b2.instanceCopy(id)
	require.True(t, ok)
	assert.Equal(t, "172.17.0.9", got.PrivateIP, "the address is the container's, as it still is")
	assert.Empty(t, fake.removed)
	assert.Empty(t, fake.created, "adoption must not launch a second container")
}
