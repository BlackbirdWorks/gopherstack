package mysqlengine_test

import (
	"context"
	"errors"
	"strings"
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

func TestDatabaseSQL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, db, charset, collation string
		want                         string
		bad                          bool
	}{
		{name: "ok", db: "app_1", charset: "utf8mb4", collation: "utf8mb4_0900_ai_ci",
			want: "CREATE DATABASE IF NOT EXISTS `app_1` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"},
		{name: "backtick name", db: "a`; DROP DATABASE x; --", charset: "utf8mb4", collation: "utf8mb4_bin", bad: true},
		{name: "empty name", db: "", charset: "utf8mb4", collation: "utf8mb4_bin", bad: true},
		{name: "long name", db: strings.Repeat("a", 65), charset: "utf8mb4", collation: "utf8mb4_bin", bad: true},
		{name: "charset injection", db: "a", charset: "utf8; DROP DATABASE x", collation: "utf8mb4_bin", bad: true},
		{name: "collation injection", db: "a", charset: "utf8mb4", collation: "x; DROP DATABASE y", bad: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := mysqlengine.CreateDatabaseSQL(tt.db, tt.charset, tt.collation)
			if tt.bad {
				require.ErrorIs(t, err, mysqlengine.ErrInvalidIdentifier)

				if tt.db != "a" {
					_, err = mysqlengine.DropDatabaseSQL(tt.db)
					require.ErrorIs(t, err, mysqlengine.ErrInvalidIdentifier)
				}

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)

			drop, err := mysqlengine.DropDatabaseSQL(tt.db)
			require.NoError(t, err)
			assert.Equal(t, "DROP DATABASE IF EXISTS `"+tt.db+"`", drop)
		})
	}
}

func TestDatabaseSQLRoundTrip(t *testing.T) {
	t.Parallel()

	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ012345678_$-"

	tests := []struct {
		name  string
		input string
		bad   bool
	}{
		{name: "alphabet", input: alphabet},
		{name: "single", input: "a"},
		{name: "max len", input: strings.Repeat("Z", 64)},
		{name: "dollar dash", input: "my-db$1"},
		{name: "space", input: "a b", bad: true},
		{name: "backtick", input: "a`b", bad: true},
		{name: "quote", input: "a'b", bad: true},
		{name: "unicode", input: "café", bad: true},
		{name: "nul", input: "a\x00b", bad: true},
		{name: "too long", input: strings.Repeat("a", 65), bad: true},
		{name: "empty", input: "", bad: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			drop, err := mysqlengine.DropDatabaseSQL(tt.input)
			if tt.bad {
				require.ErrorIs(t, err, mysqlengine.ErrInvalidIdentifier)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "DROP DATABASE IF EXISTS `"+tt.input+"`", drop)
		})
	}
}
