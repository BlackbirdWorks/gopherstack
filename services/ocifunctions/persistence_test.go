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
		name    string
		mutate  func(map[string]any)
		wantErr bool
		wantApp bool
	}{
		{"null application", func(m map[string]any) { m["applications"] = map[string]any{"x": nil} }, true, false},
		{"null function", func(m map[string]any) { m["functions"] = map[string]any{"x": nil} }, true, false},
		{"old version starts empty", func(m map[string]any) { m["version"] = 0 }, false, false},
		{"unchanged keeps state", func(map[string]any) {}, false, true},
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
		{nil, "nil context", 0, true},
		{&service.AppContext{}, "defaults", oci.DefaultPort, false},
		{&service.AppContext{Config: cfgProvider{oci.Settings{Port: 9999, Engine: oci.EngineNone}}}, "from config",
			9999, false},
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
