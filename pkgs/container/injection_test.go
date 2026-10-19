package container_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

func TestInjection_AppliedOnCreate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		inj       *container.Injection
		name      string
		wantBinds []string
		wantDNS   int
		wantEnv   bool
	}{
		{nil, "none", []string{"/a:/b"}, 0, false},
		{&container.Injection{DNS: []string{"172.17.0.1", "bogus"}}, "dns only", []string{"/a:/b"}, 1, false},
		{
			&container.Injection{DNS: []string{"172.17.0.1"}, CAFile: "/host/ca.pem"},
			"dns and ca",
			[]string{"/a:/b", "/host/ca.pem:" + container.InjectedCAPath + ":ro"}, 1, true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			api := &mockAPI{}
			rt := container.NewDockerRuntimeWithAPI(api, container.Config{
				PoolSize: 1, IdleTimeout: time.Minute, Injection: tt.inj,
			})

			mounts := []string{"/a:/b"}
			env := []string{"X=1", "AWS_CA_BUNDLE=/custom"}

			_, err := rt.CreateAndStart(t.Context(), container.Spec{
				Image: "img", Mounts: mounts, Env: env,
			})
			require.NoError(t, err)

			assert.Len(t, api.lastHost.DNS, tt.wantDNS)
			assert.Equal(t, tt.wantBinds, api.lastHost.Binds)
			assert.Equal(t, []string{"/a:/b"}, mounts)
			assert.Len(t, env, 2)

			if tt.wantEnv {
				assert.Contains(t, api.lastCfg.Env, "AWS_CA_BUNDLE=/custom")
				assert.Contains(t, api.lastCfg.Env, "NODE_EXTRA_CA_CERTS="+container.InjectedCAPath)
				assert.NotContains(t, api.lastCfg.Env, "AWS_CA_BUNDLE="+container.InjectedCAPath)
			}
		})
	}
}

func TestInjectionFromEnv(t *testing.T) {
	t.Setenv(container.EnvInjectDNS, "10.0.0.1, 10.0.0.2")
	t.Setenv(container.EnvInjectCAFile, "/ca.pem")

	inj := container.InjectionFromEnv()
	require.NotNil(t, inj)
	assert.Equal(t, []string{"10.0.0.1", "10.0.0.2"}, inj.DNS)
	assert.Equal(t, "/ca.pem", inj.CAFile)
}
