package redshift_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

func newWorkgroupEngine(t *testing.T, rt *fakeEngineRuntime, probe *gatedProbe) *redshift.InMemoryBackend {
	t.Helper()

	cfg := redshift.EngineConfig{}
	if probe != nil {
		cfg.Probe = probe.probe
	}

	b := engineBackend(t, rt, cfg)

	_, err := b.CreateNamespace(redshift.CreateNamespaceParams{
		NamespaceName: "ns-eng", AdminUsername: "root", AdminUserPassword: "Passw0rdXyz", DBName: "sales",
	})
	require.NoError(t, err)

	return b
}

func workgroupStatus(b *redshift.InMemoryBackend, name string) string {
	st, _ := b.WorkgroupStatus(name)

	return st
}

func TestWorkgroupEngine_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *redshift.InMemoryBackend, rt *fakeEngineRuntime, probe *gatedProbe)
		name string
	}{
		{
			name: "creating_until_engine_answers",
			run: func(t *testing.T, b *redshift.InMemoryBackend, rt *fakeEngineRuntime, probe *gatedProbe) {
				t.Helper()

				wg, err := b.CreateWorkgroup("wg-eng", "ns-eng", redshift.WorkgroupParams{}, nil)
				require.NoError(t, err)
				assert.Equal(t, "CREATING", wg.Status)

				_, ok := b.WorkgroupEngineLogin("wg-eng")
				assert.False(t, ok)
				assert.Equal(t, "CREATING", workgroupStatus(b, "wg-eng"))

				close(probe.gate)
				require.Eventually(t, func() bool { return workgroupStatus(b, "wg-eng") == "AVAILABLE" },
					10*time.Second, 5*time.Millisecond)

				login, ok := b.WorkgroupEngineLogin("wg-eng")
				require.True(t, ok)
				assert.Equal(t, "root", login.User)
				assert.Equal(t, "Passw0rdXyz", login.Password)
				assert.Equal(t, "sales", login.DBName)

				got, err := b.GetWorkgroup("wg-eng")
				require.NoError(t, err)
				assert.Equal(t, "127.0.0.1", got.Endpoint.Address)
				assert.Equal(t, got.Port, got.Endpoint.Port)

				specs := rt.createdSpecs()
				require.Len(t, specs, 1)
				assert.Contains(t, specs[0].Env, "POSTGRES_USER=root")
				assert.Contains(t, specs[0].Env, "POSTGRES_DB=sales")
			},
		},
		{
			name: "delete_removes_container",
			run: func(t *testing.T, b *redshift.InMemoryBackend, rt *fakeEngineRuntime, probe *gatedProbe) {
				t.Helper()

				close(probe.gate)

				_, err := b.CreateWorkgroup("wg-eng", "ns-eng", redshift.WorkgroupParams{}, nil)
				require.NoError(t, err)
				require.Eventually(t, func() bool { return workgroupStatus(b, "wg-eng") == "AVAILABLE" },
					10*time.Second, 5*time.Millisecond)

				_, err = b.DeleteWorkgroup("wg-eng")
				require.NoError(t, err)

				require.Eventually(t, func() bool { return len(rt.removedIDs()) == 1 },
					10*time.Second, 5*time.Millisecond)

				_, ok := b.WorkgroupEngineLogin("wg-eng")
				assert.False(t, ok)
			},
		},
		{
			name: "start_failure_drops_workgroup",
			run: func(t *testing.T, b *redshift.InMemoryBackend, rt *fakeEngineRuntime, _ *gatedProbe) {
				t.Helper()

				rt.mu.Lock()
				rt.createErr = errRuntimeDown
				rt.mu.Unlock()

				_, err := b.CreateWorkgroup("wg-eng", "ns-eng", redshift.WorkgroupParams{}, nil)
				require.NoError(t, err)

				require.Eventually(t, func() bool {
					_, found := b.WorkgroupStatus("wg-eng")

					return !found
				}, 10*time.Second, 5*time.Millisecond)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := &fakeEngineRuntime{}
			probe := newGatedProbe()
			b := newWorkgroupEngine(t, rt, probe)

			tt.run(t, b, rt, probe)
		})
	}
}

func TestWorkgroupEngine_NamespacePasswordUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setErr       error
		name         string
		wantPassword string
		wantErr      bool
	}{
		{name: "applied", wantPassword: "N3wPassword"},
		{name: "failure", setErr: errRuntimeDown, wantErr: true, wantPassword: "Passw0rdXyz"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var mu sync.Mutex

			var got redshift.PasswordChange

			rt := &fakeEngineRuntime{}
			b := engineBackend(t, rt, redshift.EngineConfig{
				Probe: readyProbe,
				SetPassword: func(_ context.Context, c redshift.PasswordChange) error {
					mu.Lock()
					defer mu.Unlock()

					got = c

					return tt.setErr
				},
			})

			_, err := b.CreateNamespace(redshift.CreateNamespaceParams{
				NamespaceName: "ns-eng", AdminUsername: "root", AdminUserPassword: "Passw0rdXyz", DBName: "sales",
			})
			require.NoError(t, err)

			_, err = b.CreateWorkgroup("wg-eng", "ns-eng", redshift.WorkgroupParams{}, nil)
			require.NoError(t, err)
			require.Eventually(t, func() bool { return workgroupStatus(b, "wg-eng") == "AVAILABLE" },
				10*time.Second, 5*time.Millisecond)

			_, err = b.UpdateNamespace("ns-eng", redshift.UpdateNamespaceParams{AdminUserPassword: "N3wPassword"})
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			mu.Lock()
			assert.Equal(t, "N3wPassword", got.NewPassword)
			mu.Unlock()

			login, ok := b.WorkgroupEngineLogin("wg-eng")
			require.True(t, ok)
			assert.Equal(t, tt.wantPassword, login.Password)
		})
	}
}

func TestWorkgroupEngine_RelaunchSkipsDeleting(t *testing.T) {
	t.Parallel()

	rt := &fakeEngineRuntime{}
	b := engineBackend(t, rt, redshift.EngineConfig{Probe: readyProbe})
	_, err := b.CreateNamespace(redshift.CreateNamespaceParams{NamespaceName: "ns-eng", AdminUsername: "root"})
	require.NoError(t, err)

	_, err = b.CreateWorkgroup("wg-eng", "ns-eng", redshift.WorkgroupParams{}, nil)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return workgroupStatus(b, "wg-eng") == "AVAILABLE" },
		10*time.Second, 5*time.Millisecond)

	b.SetServerlessLifecycleDelay(time.Hour)

	_, err = b.DeleteWorkgroup("wg-eng")
	require.NoError(t, err)
	require.Equal(t, "DELETING", workgroupStatus(b, "wg-eng"))

	before := len(rt.createdSpecs())
	require.NoError(t, b.Restore(t.Context(), b.Snapshot(t.Context())))

	assert.Equal(t, "DELETING", workgroupStatus(b, "wg-eng"))
	assert.Len(t, rt.createdSpecs(), before)
}
