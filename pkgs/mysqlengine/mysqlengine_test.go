package mysqlengine_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/mysqlengine"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

var errBoom = errors.New("boom")

type failingRuntime struct{ created []container.Spec }

func (r *failingRuntime) CreateAndStart(_ context.Context, spec container.Spec) (string, error) {
	r.created = append(r.created, spec)

	return "", errBoom
}

func (*failingRuntime) StopAndRemove(context.Context, string) error  { return nil }
func (*failingRuntime) StopContainer(context.Context, string) error  { return nil }
func (*failingRuntime) StartContainer(context.Context, string) error { return nil }

func (*failingRuntime) Exec(context.Context, string, []string) (container.ExecResult, error) {
	return container.ExecResult{}, nil
}

func TestNewRequiresRuntime(t *testing.T) {
	t.Parallel()

	_, err := mysqlengine.New(mysqlengine.Config{})
	require.ErrorIs(t, err, mysqlengine.ErrRuntimeUnavailable)
}

func TestVolumeName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		prefix string
		id     string
		want   string
	}{
		{"ocid is sanitised", "p", "ocid1.mysqldbsystem.oc1.iad.AbC", "p-data-ocid1-mysqldbsystem-oc1-iad-abc"},
		{"default prefix", "", "x_y", "mysql-data-x-y"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, err := mysqlengine.New(mysqlengine.Config{Runtime: &failingRuntime{}, Prefix: tt.prefix})
			require.NoError(t, err)
			assert.Equal(t, tt.want, e.VolumeName(tt.id))
		})
	}
}

func TestLaunchFailureReleasesPorts(t *testing.T) {
	t.Parallel()

	alloc, err := portalloc.New(40100, 40110)
	require.NoError(t, err)

	rt := &failingRuntime{}
	e, err := mysqlengine.New(mysqlengine.Config{Runtime: rt, Ports: alloc, Prefix: "t"})
	require.NoError(t, err)

	_, err = e.Launch(t.Context(), mysqlengine.Spec{ID: "a", Image: "mysql:8.4", RootPassword: "pw"})
	require.ErrorIs(t, err, errBoom)

	require.NotEmpty(t, rt.created)
	assert.Contains(t, rt.created[0].Env, "MYSQL_ROOT_PASSWORD=pw")
	assert.Equal(t, "mysql:8.4", rt.created[0].Image)
	assert.Empty(t, alloc.Allocated(), "a failed launch gives its ports back")
}
