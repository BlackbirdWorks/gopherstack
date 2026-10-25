package azurefunctions_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurefunctions"
)

func TestFunctions_List(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		runtime     bool
		wantNames   []string
		wantURL     map[string]string
		wantDisable map[string]bool
	}{
		{
			name:        "from the running host",
			runtime:     true,
			wantNames:   []string{"hello", "tick"},
			wantURL:     map[string]string{"hello": "http://app." + testDomain + ":10012/api/hello", "tick": ""},
			wantDisable: map[string]bool{"hello": false, "tick": true},
		},
		{
			name:        "from function.json without a runtime",
			runtime:     false,
			wantNames:   []string{"hello", "tick"},
			wantURL:     map[string]string{"hello": "http://app." + testDomain + ":10012/api/greet", "tick": ""},
			wantDisable: map[string]bool{"hello": false, "tick": true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, tt.runtime)
			e.backend.PutApp("app", azurefunctions.AppConfig{LinuxFx: "NODE|20"})
			_, err := e.backend.Deploy("app", helloZip(t), "t", "m")
			require.NoError(t, err)

			fns, err := e.backend.Functions(t.Context(), "app")
			require.NoError(t, err)
			require.Len(t, fns, len(tt.wantNames))

			for i, f := range fns {
				assert.Equal(t, tt.wantNames[i], f.Name)
				assert.Equal(t, tt.wantURL[f.Name], f.InvokeURL)
				assert.Equal(t, tt.wantDisable[f.Name], f.IsDisabled)
				assert.NotEmpty(t, f.Config["bindings"])
			}

			one, err := e.backend.Function(t.Context(), "app", "HELLO")
			require.NoError(t, err)
			assert.Equal(t, "hello", one.Name)

			_, err = e.backend.Function(t.Context(), "app", "missing")
			require.ErrorIs(t, err, azurefunctions.ErrFunctionNotFound)
		})
	}
}

func TestFunctions_EmptyAndMissing(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	e.backend.PutApp("app", azurefunctions.AppConfig{LinuxFx: "NODE|20"})

	fns, err := e.backend.Functions(t.Context(), "app")
	require.NoError(t, err)
	assert.Empty(t, fns, "an app without content has no functions")
	assert.Zero(t, e.rt.running(), "and needs no host to say so")

	_, err = e.backend.Functions(t.Context(), "ghost")
	require.ErrorIs(t, err, azurefunctions.ErrAppNotFound)
}

func TestFunctions_StoppedAppFallsBackToPackage(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	e.backend.PutApp("app", azurefunctions.AppConfig{LinuxFx: "NODE|20"})
	_, err := e.backend.Deploy("app", helloZip(t), "t", "m")
	require.NoError(t, err)
	require.NoError(t, e.backend.SetStopped("app", true))

	fns, err := e.backend.Functions(t.Context(), "app")
	require.NoError(t, err)
	assert.Len(t, fns, 2)
	assert.Zero(t, e.rt.running())
}
