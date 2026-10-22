package azuresql_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azuresql"
)

func TestBackendLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		launcher func() azuresql.Launcher
		name     string
		wantPort bool
	}{
		{name: "engine", launcher: func() azuresql.Launcher { return &fakeLauncher{} }, wantPort: true},
		{
			name:     "state only without runtime",
			launcher: func() azuresql.Launcher { return &fakeLauncher{err: azuresql.ErrRuntimeUnavailable} },
		},
		{name: "state only without launcher", launcher: func() azuresql.Launcher { return nil }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t, tt.launcher())
			ep := mustEnsure(
				t,
				b,
				"/Subs/S/Servers/One",
				azuresql.Spec{AdminLogin: "sqladmin", AdminPassword: "Passw0rd!x"},
			)
			assert.Equal(t, tt.wantPort, ep.Port != 0)

			got, ok := b.Endpoint("/subs/s/servers/one")
			require.True(t, ok, "ids are case-insensitive")
			assert.Equal(t, ep, got)

			require.NoError(t, b.EnsureDatabase("/subs/s/servers/one", "app", "SQL_Latin1_General_CP1_CI_AS"))
			require.NoError(t, b.EnsureDatabase("/subs/s/servers/one", "app", ""), "idempotent")
			require.ErrorIs(t, b.EnsureDatabase("/subs/s/servers/none", "x", ""), azuresql.ErrNotFound)

			names, err := b.Databases("/subs/s/servers/one")
			require.NoError(t, err)
			assert.Equal(t, []string{"app"}, names)

			require.NoError(t, b.DropDatabase("/subs/s/servers/one", "app"))
			names, err = b.Databases("/subs/s/servers/one")
			require.NoError(t, err)
			assert.Empty(t, names)

			require.NoError(t, b.Delete("/subs/s/servers/one"))
			require.ErrorIs(t, b.Delete("/subs/s/servers/one"), azuresql.ErrNotFound)

			_, ok = b.Endpoint("/subs/s/servers/one")
			assert.False(t, ok)
		})
	}
}

func TestBackendEngineErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(b *azuresql.InMemoryBackend) error
		name string
	}{
		{name: "launch failure", call: func(b *azuresql.InMemoryBackend) error {
			_, err := b.Ensure("s", azuresql.Spec{})

			return err
		}},
		{name: "database failure", call: func(b *azuresql.InMemoryBackend) error {
			return b.EnsureDatabase("s", "failing", "")
		}},
		{name: "control character", call: func(b *azuresql.InMemoryBackend) error {
			return b.EnsureDatabase("s", "bad\nname", "")
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			l := &fakeLauncher{}
			if tt.name == "launch failure" {
				l.err = errBoom
			}

			b := newBackend(t, l)
			if tt.name != "launch failure" {
				mustEnsure(t, b, "s", azuresql.Spec{})
			}

			require.Error(t, tt.call(b))
		})
	}
}

func TestBackendUpdateAppliesNewPassword(t *testing.T) {
	t.Parallel()

	l := &fakeLauncher{}
	b := newBackend(t, l)
	mustEnsure(t, b, "s", azuresql.Spec{AdminLogin: "adm", AdminPassword: "Passw0rd!x"})
	mustEnsure(t, b, "s", azuresql.Spec{AdminLogin: "adm"})
	require.Len(t, l.engines, 1)
	assert.Equal(t, "Passw0rd!x", l.engines[0].logins["adm"], "empty password keeps the current one")

	mustEnsure(t, b, "s", azuresql.Spec{AdminLogin: "adm", AdminPassword: "N3wPassw0rd!"})
	assert.Equal(t, "N3wPassw0rd!", l.engines[0].logins["adm"])
}

func TestBackendCloseStopsEngines(t *testing.T) {
	t.Parallel()

	l := &fakeLauncher{}
	b := newBackend(t, l)
	mustEnsure(t, b, "a", azuresql.Spec{})
	mustEnsure(t, b, "b", azuresql.Spec{})
	require.NoError(t, b.Delete("a"))
	assert.True(t, l.engines[0].closed)

	b.Reset()
	assert.True(t, l.engines[1].closed)
}

func TestBackendSnapshotRestore(t *testing.T) {
	t.Parallel()

	src := newBackend(t, &fakeLauncher{})
	mustEnsure(t, src, "s", azuresql.Spec{AdminLogin: "adm", AdminPassword: "Passw0rd!x"})
	require.NoError(t, src.EnsureDatabase("s", "app", "Latin1_General_CI_AS"))

	l := &fakeLauncher{}
	dst := newBackend(t, l)
	require.NoError(t, dst.Restore(t.Context(), src.Snapshot(t.Context())))

	names, err := dst.Databases("s")
	require.NoError(t, err)
	assert.Equal(t, []string{"app"}, names)
	assert.Equal(t, "Passw0rd!x", l.engines[0].logins["adm"])
	assert.Equal(t, "Latin1_General_CI_AS", l.engines[0].dbs["app"])
}
