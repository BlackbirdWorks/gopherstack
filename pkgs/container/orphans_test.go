package container_test

import (
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/image"
	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

type fakeLister struct {
	all     []container.ManagedContainer
	removed []string
}

func (f *fakeLister) ListManaged(context.Context) ([]container.ManagedContainer, error) {
	return f.all, nil
}

func (f *fakeLister) StopAndRemove(_ context.Context, id string) error {
	f.removed = append(f.removed, id)

	return nil
}

const deadPID = "999999999:1"

func mc(running bool, kv ...string) container.ManagedContainer {
	l := map[string]string{container.LabelManaged: "true"}
	for i := 0; i+1 < len(kv); i += 2 {
		l[kv[i]] = kv[i+1]
	}

	return container.ManagedContainer{ID: "c1", Running: running, Labels: l}
}

func TestReconcile(t *testing.T) {
	t.Parallel()

	me := container.Identity{Owner: "dir-me", PID: os.Getpid()}
	parent := strconv.Itoa(os.Getppid())

	tests := []struct {
		name        string
		all         []container.ManagedContainer
		wantRemoved []string
		wantClaim   string
		wantSwept   []string
	}{
		{
			name: "adopts same owner resource",
			all: []container.ManagedContainer{mc(true, container.LabelOwner, "dir-me",
				container.LabelPID, deadPID, container.LabelService, "gcpsql", container.LabelResource, "p/i")},
			wantClaim: "c1",
		},
		{
			name: "unclaimed same owner swept",
			all: []container.ManagedContainer{mc(true, container.LabelOwner, "dir-me",
				container.LabelPID, deadPID, container.LabelService, "gcpsql", container.LabelResource, "p/other")},
			wantSwept: []string{"c1"},
		},
		{
			name: "same owner without resource removed",
			all: []container.ManagedContainer{
				mc(true, container.LabelOwner, "dir-me", container.LabelPID, deadPID),
			},
			wantRemoved: []string{"c1"},
		},
		{
			name: "same owner live other process untouched",
			all: []container.ManagedContainer{mc(true, container.LabelOwner, "dir-me",
				container.LabelPID, parent, container.LabelService, "gcpsql", container.LabelResource, "p/i")},
		},
		{
			name: "foreign owner untouched",
			all: []container.ManagedContainer{
				mc(true, container.LabelOwner, "dir-other", container.LabelPID, deadPID),
			},
		},
		{
			name: "unlabelled untouched",
			all:  []container.ManagedContainer{{ID: "c1", Running: true, Labels: map[string]string{"x": "y"}}},
		},
		{
			name: "dead ephemeral removed",
			all: []container.ManagedContainer{mc(true, container.LabelOwner, "run-old",
				container.LabelEphemeral, "true", container.LabelPID, deadPID)},
			wantRemoved: []string{"c1"},
		},
		{
			name: "live ephemeral untouched",
			all: []container.ManagedContainer{mc(true, container.LabelOwner, "run-old",
				container.LabelEphemeral, "true", container.LabelPID, parent)},
		},
		{
			name: "stopped same owner swept not claimed",
			all: []container.ManagedContainer{mc(false, container.LabelOwner, "dir-me",
				container.LabelPID, deadPID, container.LabelService, "gcpsql", container.LabelResource, "p/i")},
			wantSwept: []string{"c1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := &fakeLister{all: tt.all}
			o, err := container.Reconcile(t.Context(), f, me, nil)
			require.NoError(t, err)
			assert.ElementsMatch(t, tt.wantRemoved, f.removed)

			got, ok := o.Claim("gcpsql", "p/i")
			assert.Equal(t, tt.wantClaim != "", ok)
			assert.Equal(t, tt.wantClaim, got.ID)

			f.removed = nil
			assert.Len(t, tt.wantSwept, o.SweepUnclaimed(t.Context()))
			assert.ElementsMatch(t, tt.wantSwept, f.removed)
		})
	}
}

func TestOwnProcessContainersAdopted(t *testing.T) {
	t.Parallel()

	id := container.CurrentIdentity()
	l := id.Labels()
	l[container.LabelService], l[container.LabelResource] = "gcpsql", "p/i"

	f := &fakeLister{all: []container.ManagedContainer{{ID: "c1", Running: true, Labels: l}}}
	o, err := container.Reconcile(t.Context(), f, id, nil)
	require.NoError(t, err)

	got, ok := o.Claim("gcpsql", "p/i")
	assert.True(t, ok)
	assert.Equal(t, "c1", got.ID)
}

func TestSetPersistentOwnerStable(t *testing.T) {
	t.Parallel()

	a := container.SetPersistentOwner("/some/dir")
	b := container.SetPersistentOwner("/some/dir")
	c := container.SetPersistentOwner("/other/dir")
	t.Cleanup(func() { container.SetEphemeralOwner() })

	assert.Equal(t, a.Owner, b.Owner)
	assert.NotEqual(t, a.Owner, c.Owner)
	assert.False(t, a.Ephemeral)
	assert.True(t, container.SetEphemeralOwner().Ephemeral)
}

type listAPI struct {
	*mockAPI
	items []container.ManagedContainer
}

func (l *listAPI) ContainerListByLabel(context.Context, string) ([]container.ManagedContainer, error) {
	return l.items, nil
}

func TestDockerRuntimeListManaged(t *testing.T) {
	t.Parallel()

	rt := newRuntime(&listAPI{mockAPI: &mockAPI{}, items: []container.ManagedContainer{{ID: "a"}}})
	got, err := rt.ListManaged(t.Context())
	require.NoError(t, err)
	assert.Len(t, got, 1)

	_, err = newRuntime(&mockAPI{}).ListManaged(t.Context())
	require.ErrorIs(t, err, container.ErrListUnsupported)
}

func TestCreateAndStartLabels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantKeys  []string
		name      string
		spec      container.Spec
		unmanaged bool
	}{
		{
			name: "identity and resource labels", spec: container.Spec{
				Image: "img", Labels: container.ResourceLabels("gcpsql", "p/i"),
			},
			wantKeys: []string{
				container.LabelManaged, container.LabelOwner, container.LabelPID,
				container.LabelService, container.LabelResource,
			},
		},
		{name: "unmanaged", spec: container.Spec{Image: "img", Unmanaged: true}, unmanaged: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			api := &mockAPI{images: []image.Summary{{}}}
			_, err := newRuntime(api).CreateAndStart(t.Context(), tt.spec)
			require.NoError(t, err)

			cfg := api.lastCfg
			if tt.unmanaged {
				assert.NotContains(t, cfg.Labels, container.LabelManaged)

				return
			}

			for _, k := range tt.wantKeys {
				assert.Contains(t, cfg.Labels, k)
			}

			assert.Equal(t, "true", cfg.Labels[container.LabelManaged])
		})
	}
}
