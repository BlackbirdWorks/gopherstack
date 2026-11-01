package gcpsql

import (
	"context"
	"maps"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

type removeRecorder struct {
	removed []string
	mu      sync.Mutex
}

func (r *removeRecorder) CreateAndStart(context.Context, container.Spec) (string, error) {
	return "", nil
}
func (r *removeRecorder) StopContainer(context.Context, string) error  { return nil }
func (r *removeRecorder) StartContainer(context.Context, string) error { return nil }

func (r *removeRecorder) Exec(context.Context, string, []string) (container.ExecResult, error) {
	return container.ExecResult{}, nil
}

func (r *removeRecorder) StopAndRemove(_ context.Context, id string) error {
	r.mu.Lock()
	r.removed = append(r.removed, id)
	r.mu.Unlock()

	return nil
}

type orphanLister struct{ all []container.ManagedContainer }

func (o orphanLister) ListManaged(context.Context) ([]container.ManagedContainer, error) {
	return o.all, nil
}
func (orphanLister) StopAndRemove(context.Context, string) error { return nil }

func TestAdoptFrom(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate     func(*dockerEngine, *container.ManagedContainer)
		name       string
		wantClaim  bool
		wantRemove bool
	}{
		{
			name:   "fresh instance leaves orphan held",
			mutate: func(e *dockerEngine, _ *container.ManagedContainer) { e.spec.Fresh = true },
		},
		{
			name:   "replica leaves orphan held",
			mutate: func(e *dockerEngine, _ *container.ManagedContainer) { e.spec.Replica = &ReplicaSource{} },
		},
		{
			name:   "recovery leaves orphan held",
			mutate: func(e *dockerEngine, _ *container.ManagedContainer) { e.spec.Recovery = &Recovery{} },
		},
		{
			name:      "shape mismatch removed",
			wantClaim: true, wantRemove: true,
			mutate: func(_ *dockerEngine, c *container.ManagedContainer) { c.Labels[shapeLabel] = "other" },
		},
		{
			name:      "missing dump dir removed",
			wantClaim: true, wantRemove: true,
			mutate: func(_ *dockerEngine, c *container.ManagedContainer) { c.Labels[dumpDirLabel] = "/nonexistent/x" },
		},
		{
			name:      "unreachable database removed",
			wantClaim: true, wantRemove: true,
			mutate: func(*dockerEngine, *container.ManagedContainer) {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := &removeRecorder{}
			orig := t.TempDir()
			dumpDir := t.TempDir()
			e := &dockerEngine{
				rt: rec, mu: lockmetrics.New("t"), id: "i", image: "postgres:16", dir: orig,
				spec: LaunchSpec{Project: "p"},
			}

			id := container.CurrentIdentity()
			labels := id.Labels()
			labels[container.LabelPID] = "999999999:1"

			maps.Copy(labels, e.ownerLabels())

			labels[dumpDirLabel] = dumpDir

			oc := container.ManagedContainer{
				ID: "ctr1", Running: true, Labels: labels, Ports: map[int]int{5432: 1},
			}
			tt.mutate(e, &oc)

			lister := orphanLister{all: []container.ManagedContainer{oc}}
			orphans, err := container.Reconcile(t.Context(), lister, id, nil)
			require.NoError(t, err)

			ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
			defer cancel()

			assert.False(t, e.adoptFrom(ctx, orphans.Claim))
			assert.Equal(t, tt.wantRemove, len(rec.removed) == 1)
			assert.Equal(t, orig, e.dir)
			assert.Empty(t, e.ctrID)

			_, stillHeld := orphans.Claim("gcpsql", e.resourceKey())
			assert.Equal(t, !tt.wantClaim, stillHeld)

			_, err = os.Stat(orig)
			require.NoError(t, err)
		})
	}
}
