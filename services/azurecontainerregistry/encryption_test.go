package azurecontainerregistry_test

import (
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	acr "github.com/blackbirdworks/gopherstack/services/azurecontainerregistry"
)

func TestEncryptionKeyFollowsKeyState(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	creds := e.registry(t, "keyreg", true)

	usable := true
	e.h.Backend.SetKeyChecker(func(string) error {
		if !usable {
			return errors.New("key disabled")
		}

		return nil
	})

	c := e.client(t, "keyreg", basic(creds.Username, creds.Password1))

	require.NoError(t, e.h.Backend.SetEncryptionKey("keyreg", "https://kv.vault.azure.net/keys/k"))
	assert.Equal(t, http.StatusOK, c.Do(http.MethodGet, "/v2/", nil, nil).Status)

	usable = false
	assert.Equal(t, http.StatusForbidden, c.Do(http.MethodGet, "/v2/", nil, nil).Status)

	usable = true
	assert.Equal(t, http.StatusOK, c.Do(http.MethodGet, "/v2/", nil, nil).Status)

	usable = false

	require.NoError(t, e.h.Backend.SetEncryptionKey("keyreg", ""))
	assert.Equal(t, http.StatusOK, c.Do(http.MethodGet, "/v2/", nil, nil).Status)
	require.ErrorIs(t, e.h.Backend.SetEncryptionKey("ghostreg", "x"), acr.ErrRegistryNotFound)
}
