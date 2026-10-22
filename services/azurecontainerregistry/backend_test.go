package azurecontainerregistry_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	acr "github.com/blackbirdworks/gopherstack/services/azurecontainerregistry"
)

func TestPutRegistryNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		reg     string
	}{
		{name: "valid", reg: "myregistry"},
		{name: "digits", reg: "Reg12345"},
		{name: "too short", reg: "abcd", wantErr: acr.ErrInvalidName},
		{name: "hyphen", reg: "my-registry", wantErr: acr.ErrInvalidName},
		{name: "underscore", reg: "my_registry", wantErr: acr.ErrInvalidName},
		{name: "too long", reg: "a123456789a123456789a123456789a123456789a1234567890", wantErr: acr.ErrInvalidName},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := acr.NewInMemoryBackend().PutRegistry(tt.reg, true)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestCredentials(t *testing.T) {
	t.Parallel()

	b := acr.NewInMemoryBackend()
	_, err := b.PutRegistry("credtest", false)
	require.NoError(t, err)

	_, err = b.Credentials("credtest")
	require.ErrorIs(t, err, acr.ErrAdminDisabled)

	_, err = b.Credentials("missing")
	require.ErrorIs(t, err, acr.ErrRegistryNotFound)

	info, err := b.PutRegistry("CredTest", true)
	require.NoError(t, err)
	assert.True(t, info.AdminEnabled)

	got, err := b.Credentials("credtest")
	require.NoError(t, err)
	assert.Equal(t, "credtest", got.Username)
	assert.Len(t, got.Password1, 32)
	assert.NotEqual(t, got.Password1, got.Password2)

	_, err = b.RegenerateCredential("credtest", "bogus")
	require.ErrorIs(t, err, acr.ErrInvalidCred)

	_, err = b.RegenerateCredential("missing", "password")
	require.ErrorIs(t, err, acr.ErrRegistryNotFound)

	next, err := b.RegenerateCredential("credtest", "password2")
	require.NoError(t, err)
	assert.Equal(t, got.Password1, next.Password1)
	assert.NotEqual(t, got.Password2, next.Password2)

	require.NoError(t, b.DeleteRegistry("credtest"))
	require.ErrorIs(t, b.DeleteRegistry("credtest"), acr.ErrRegistryNotFound)
}

func TestSnapshotRestore(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	before := newEnv(t, dir)
	creds := before.registry(t, "persisted", true)
	c := before.client(t, "persisted", basic(creds.Username, creds.Password1))
	img := c.PushImage("keep/me", "v1")

	snap := before.h.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	after := newEnv(t, dir)
	require.NoError(t, after.h.Restore(t.Context(), snap))

	c2 := after.client(t, "persisted", basic(creds.Username, creds.Password1))

	t.Run("blobs survive on disk", func(t *testing.T) {
		t.Parallel()

		got := c2.Do(http.MethodGet, "/v2/keep/me/manifests/v1", nil, nil)
		require.Equal(t, http.StatusOK, got.Status)
		assert.Equal(t, img.Body, got.Body)
	})

	t.Run("metadata survives", func(t *testing.T) {
		t.Parallel()

		got := c2.Do(http.MethodGet, "/acr/v1/keep/me/_tags/v1", nil, nil)
		require.Equal(t, http.StatusOK, got.Status)
	})

	t.Run("credentials survive", func(t *testing.T) {
		t.Parallel()

		restored, err := after.h.Backend.Credentials("persisted")
		require.NoError(t, err)
		assert.Equal(t, creds, restored)
	})

	t.Run("garbage rejected", func(t *testing.T) {
		t.Parallel()

		require.Error(t, newEnv(t, "").h.Restore(t.Context(), []byte("not json")))
	})
}

func TestReset(t *testing.T) {
	t.Parallel()

	e := newEnv(t, t.TempDir())
	e.registry(t, "resetreg", true)

	e.h.Reset()

	_, found := e.h.Backend.Registry("resetreg")
	assert.False(t, found)
}
