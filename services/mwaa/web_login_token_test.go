package mwaa_test

import (
	"context"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/mwaa"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateWebLoginToken_RequiresAvailable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		status  string
		wantErr bool
	}{
		{name: "creating_rejected", status: "CREATING", wantErr: true},
		{name: "updating_rejected", status: "UPDATING", wantErr: true},
		{name: "deleting_rejected", status: "DELETING", wantErr: true},
		{name: "available_ok", status: "AVAILABLE", wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
			env := b.AddEnvironmentInternal("web-state-env-" + tt.name)
			env.Status = tt.status

			_, _, err := b.CreateWebLoginToken(context.Background(), "web-state-env-"+tt.name)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, mwaa.ErrEnvironmentNotFound,
					"non-AVAILABLE env must return not-found sentinel")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCreateWebLoginToken_NotFound(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, _, err := b.CreateWebLoginToken(context.Background(), "missing-env")

	require.Error(t, err)
	require.ErrorIs(t, err, mwaa.ErrEnvironmentNotFound)
}

func TestCreateWebLoginToken_HappyPath(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	seedEnv(t, b, "web-env")

	token, _, err := b.CreateWebLoginToken(context.Background(), "web-env")

	require.NoError(t, err)
	assert.NotEmpty(t, token)
	// Token is JWT-shaped: three dot-separated base64url segments.
	parts := strings.Split(token, ".")
	assert.Len(t, parts, 3, "expected JWT-shaped token with 3 dot-separated parts")
}

func TestCreateWebLoginToken_HostnameMatchesWebserverURL(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	envName := "web-hostname-env"
	_, err := b.CreateEnvironment(context.Background(), envName, newCreateReq())
	require.NoError(t, err)
	env, _ := b.GetEnvironment(context.Background(), envName)

	_, hostname, err := b.CreateWebLoginToken(context.Background(), envName)
	require.NoError(t, err)

	wantHostname := strings.TrimPrefix(env.WebserverURL, "https://")
	assert.Equal(t, wantHostname, hostname)
}

func TestWebLoginToken_JWTShaped(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "jwt-web-env", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "jwt-web-env") // promote CREATING → AVAILABLE

	token, _, err := b.CreateWebLoginToken(context.Background(), "jwt-web-env")
	require.NoError(t, err)

	parts := strings.Split(token, ".")
	assert.Len(t, parts, 3, "web login token must be three dot-separated JWT segments")
	assert.NotEmpty(t, parts[0])
	assert.NotEmpty(t, parts[1])
	assert.NotEmpty(t, parts[2])
}
