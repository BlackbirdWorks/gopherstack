package mwaa_test

import (
	"context"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/mwaa"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateCliToken_RequiresAvailable(t *testing.T) {
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
			env := b.AddEnvironmentInternal("cli-state-env-" + tt.name)
			env.Status = tt.status

			_, _, err := b.CreateCliToken(context.Background(), "cli-state-env-"+tt.name)
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

func TestCreateCliToken_NotFound(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, _, err := b.CreateCliToken(context.Background(), "missing-env")

	require.Error(t, err)
	require.ErrorIs(t, err, mwaa.ErrEnvironmentNotFound)
}

func TestCreateCliToken_HappyPath(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	seedEnv(t, b, "cli-env")

	token, _, err := b.CreateCliToken(context.Background(), "cli-env")

	require.NoError(t, err)
	assert.NotEmpty(t, token)
	// Token is JWT-shaped: three dot-separated base64url segments.
	parts := strings.Split(token, ".")
	assert.Len(t, parts, 3, "expected JWT-shaped token with 3 dot-separated parts")
}

func TestCreateCliToken_HostnameMatchesWebserverURL(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	envName := "cli-hostname-env"
	_, err := b.CreateEnvironment(context.Background(), envName, newCreateReq())
	require.NoError(t, err)
	env, _ := b.GetEnvironment(context.Background(), envName) // promote + capture URL

	_, hostname, err := b.CreateCliToken(context.Background(), envName)
	require.NoError(t, err)

	wantHostname := strings.TrimPrefix(env.WebserverURL, "https://")
	assert.Equal(t, wantHostname, hostname)
}

func TestTokenHostname_ContainsAirflowRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		region    string
		wantInfix string
	}{
		{name: "us_east_1", region: "us-east-1", wantInfix: ".airflow.us-east-1.amazonaws.com"},
		{name: "eu_west_1", region: "eu-west-1", wantInfix: ".airflow.eu-west-1.amazonaws.com"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mwaa.NewInMemoryBackend(tt.region, testAccountID)
			_, err := b.CreateEnvironment(context.Background(), "tok-region-env", newCreateReq())
			require.NoError(t, err)
			_, _ = b.GetEnvironment(context.Background(), "tok-region-env")

			_, hostname, err := b.CreateCliToken(context.Background(), "tok-region-env")
			require.NoError(t, err)
			assert.Contains(t, hostname, tt.wantInfix)
		})
	}
}

func TestTokenHostname_NotSameAcrossEnvironments(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)

	for _, name := range []string{"env-tok-a", "env-tok-b"} {
		_, err := b.CreateEnvironment(context.Background(), name, newCreateReq())
		require.NoError(t, err)
		_, _ = b.GetEnvironment(context.Background(), name)
	}

	_, hostA, err := b.CreateCliToken(context.Background(), "env-tok-a")
	require.NoError(t, err)
	_, hostB, err := b.CreateCliToken(context.Background(), "env-tok-b")
	require.NoError(t, err)

	assert.NotEqual(t, hostA, hostB, "different environments should have different hostnames")
}

func TestCliToken_JWTShaped(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "jwt-cli-env", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "jwt-cli-env") // promote CREATING → AVAILABLE

	token, _, err := b.CreateCliToken(context.Background(), "jwt-cli-env")
	require.NoError(t, err)

	parts := strings.Split(token, ".")
	assert.Len(t, parts, 3, "CLI token must be three dot-separated JWT segments")
	assert.NotEmpty(t, parts[0], "header segment must not be empty")
	assert.NotEmpty(t, parts[1], "payload segment must not be empty")
	assert.NotEmpty(t, parts[2], "signature segment must not be empty")
}

func TestCliToken_DifferentFromWebToken(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "token-diff-env", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "token-diff-env") // promote CREATING → AVAILABLE

	cli, _, err := b.CreateCliToken(context.Background(), "token-diff-env")
	require.NoError(t, err)

	web, _, err := b.CreateWebLoginToken(context.Background(), "token-diff-env")
	require.NoError(t, err)

	assert.NotEqual(t, cli, web, "CLI token and web login token must differ")
}

func TestToken_DifferentPerEnvironment(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	_, err := b.CreateEnvironment(context.Background(), "env-token-a", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "env-token-a") // promote CREATING → AVAILABLE
	_, err = b.CreateEnvironment(context.Background(), "env-token-b", newCreateReq())
	require.NoError(t, err)
	_, _ = b.GetEnvironment(context.Background(), "env-token-b") // promote CREATING → AVAILABLE

	tokenA, _, err := b.CreateCliToken(context.Background(), "env-token-a")
	require.NoError(t, err)

	tokenB, _, err := b.CreateCliToken(context.Background(), "env-token-b")
	require.NoError(t, err)

	assert.NotEqual(t, tokenA, tokenB, "tokens for different environments must differ")
}
