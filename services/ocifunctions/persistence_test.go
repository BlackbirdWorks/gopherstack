package ocifunctions_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/functions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	oci "github.com/blackbirdworks/gopherstack/services/ocifunctions"
)

func TestSnapshotRestore(t *testing.T) {
	t.Parallel()

	src := newEnv(t, envOpts{register: true})
	app := src.mkApp(t, "app", func(d *functions.CreateApplicationDetails) {
		d.Config = map[string]string{"K": "v"}
		d.ImagePolicyConfig = &functions.ImagePolicyConfig{IsPolicyEnabled: new(true)}
	})
	fn := src.mkFn(t, app, "fn", func(d *functions.CreateFunctionDetails) {
		d.ProvisionedConcurrencyConfig = functions.ConstantProvisionedConcurrencyConfig{Count: new(7)}
		d.Config = map[string]string{"F": "1"}
	})

	snap := src.handler.Snapshot(t.Context())

	dst := newEnv(t, envOpts{})
	require.NoError(t, dst.handler.Restore(t.Context(), snap))

	gotApp, err := dst.mgmt.GetApplication(t.Context(), functions.GetApplicationRequest{ApplicationId: app.Id})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"K": "v"}, gotApp.Config)
	assert.True(t, *gotApp.ImagePolicyConfig.IsPolicyEnabled)

	gotFn, err := dst.mgmt.GetFunction(t.Context(), functions.GetFunctionRequest{FunctionId: fn.Id})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"F": "1"}, gotFn.Config)

	pc, ok := gotFn.ProvisionedConcurrencyConfig.(functions.ConstantProvisionedConcurrencyConfig)
	require.True(t, ok)
	assert.Equal(t, 7, *pc.Count)
	assert.Len(t, dst.backend.Auth.Records(), 1)
}

func TestRestoreRejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate  func(map[string]any)
		name    string
		wantErr bool
		wantApp bool
	}{
		{
			name:    "null application",
			mutate:  func(m map[string]any) { m["applications"] = map[string]any{"x": nil} },
			wantErr: true,
			wantApp: false,
		},
		{
			name:    "null function",
			mutate:  func(m map[string]any) { m["functions"] = map[string]any{"x": nil} },
			wantErr: true,
			wantApp: false,
		},
		{
			name:    "old version starts empty",
			mutate:  func(m map[string]any) { m["version"] = 0 },
			wantErr: false,
			wantApp: false,
		},
		{name: "unchanged keeps state", mutate: func(map[string]any) {}, wantErr: false, wantApp: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := newEnv(t, envOpts{})
			app := src.mkApp(t, "app", nil)

			body := map[string]any{}
			require.NoError(t, json.Unmarshal(src.handler.Snapshot(t.Context()), &body))
			tt.mutate(body)

			out, err := json.Marshal(body)
			require.NoError(t, err)

			dst := newEnv(t, envOpts{})
			err = dst.handler.Restore(t.Context(), out)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			_, err = dst.mgmt.GetApplication(t.Context(), functions.GetApplicationRequest{ApplicationId: app.Id})
			if tt.wantApp {
				require.NoError(t, err)

				return
			}

			assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}

type cfgProvider struct{ s oci.Settings }

func (c cfgProvider) GetOCIFunctionsSettings() oci.Settings { return c.s }

func TestProvider(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ctx      *service.AppContext
		name     string
		wantPort int
		wantErr  bool
	}{
		{ctx: nil, name: "nil context", wantPort: 0, wantErr: true},
		{ctx: &service.AppContext{}, name: "defaults", wantPort: oci.DefaultPort},
		{ctx: &service.AppContext{Config: cfgProvider{oci.Settings{Port: 9999, DisableEngine: true}}},
			name: "from config", wantPort: 9999},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &oci.Provider{}
			assert.Equal(t, "OCIFunctions", p.Name())

			reg, err := p.Init(tt.ctx)
			if tt.wantErr {
				require.ErrorIs(t, err, oci.ErrNilAppContext)

				return
			}

			require.NoError(t, err)

			h, ok := reg.(*oci.Handler)
			require.True(t, ok)
			t.Cleanup(func() { h.Shutdown(t.Context()) })

			assert.Equal(t, tt.wantPort, h.Port)
		})
	}
}
