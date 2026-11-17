package azurecontainerregistry_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	acr "github.com/blackbirdworks/gopherstack/services/azurecontainerregistry"
)

func TestUsedBytesTracksStoredBlobs(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	creds := e.registry(t, "usagereg", true)
	c := e.client(t, "usagereg", basic(creds.Username, creds.Password1))

	empty, err := e.h.Backend.UsedBytes("usagereg")
	require.NoError(t, err)
	assert.Zero(t, empty)

	img := c.PushImage("app", "v1")

	used, err := e.h.Backend.UsedBytes("usagereg")
	require.NoError(t, err)
	assert.Greater(t, used, int64(len(img.Layer)), "layer, config and manifest are counted")

	require.Equal(t, http.StatusAccepted, c.Do(http.MethodDelete, "/v2/app/manifests/"+img.Digest, nil, nil).Status)

	after, err := e.h.Backend.UsedBytes("usagereg")
	require.NoError(t, err)
	assert.Zero(t, after, "deleting the only image frees every blob")

	_, err = e.h.Backend.UsedBytes("missing")
	require.ErrorIs(t, err, acr.ErrRegistryNotFound)
}

func TestImportRefusedFromRegistryWithExportDisabled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		allowed bool
		uri     bool
		wantErr bool
	}{
		{name: "allowed by default", allowed: true},
		{name: "disabled by resource id", allowed: false, wantErr: true},
		{name: "disabled by login server", allowed: false, uri: true, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, src, dst := newImportEnv(t)
			require.NoError(t, e.h.Backend.SetExportAllowed("sourcereg", tt.allowed))

			source := acr.ImportSource{Registry: "sourcereg", Image: "team/app:v1"}
			if tt.uri {
				source = acr.ImportSource{
					RegistryURI: "sourcereg.azurecr.localhost:10011", Image: "team/app:v1",
					Username: src.Username, Password: src.Password1,
				}
			}

			err := e.h.Backend.ImportImage(t.Context(), "targetreg", acr.ImportRequest{Source: source})
			if tt.wantErr {
				require.ErrorIs(t, err, acr.ErrImportExportOff)
				assert.NotEqual(t, http.StatusOK, pullStatus(t, e, dst, "team/app", "v1"))

				return
			}

			require.NoError(t, err)
			assert.Equal(t, http.StatusOK, pullStatus(t, e, dst, "team/app", "v1"))
		})
	}
}

func TestExportPolicySurvivesRestore(t *testing.T) {
	t.Parallel()

	first := newEnv(t, "")
	first.registry(t, "exportsnap", true)
	require.NoError(t, first.h.Backend.SetExportAllowed("exportsnap", false))

	snap := first.h.Backend.Snapshot(t.Context())

	fresh := newEnv(t, "")
	require.NoError(t, fresh.h.Backend.Restore(t.Context(), snap))
	src := fresh.registry(t, "exportsnap", true)
	fresh.registry(t, "other", true)

	err := fresh.h.Backend.ImportImage(t.Context(), "other", acr.ImportRequest{
		Source: acr.ImportSource{Registry: "exportsnap", Image: "x/y:1", Username: src.Username},
	})
	require.ErrorIs(t, err, acr.ErrImportExportOff)
}
