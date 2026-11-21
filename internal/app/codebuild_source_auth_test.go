package app

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	codebuildbackend "github.com/blackbirdworks/gopherstack/services/codebuild"
	codeconnectionsbackend "github.com/blackbirdworks/gopherstack/services/codeconnections"
	secretsmanagerbackend "github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

func TestCodeBuildSourceAuthResolvesEmulatedServices(t *testing.T) {
	t.Parallel()

	sm := secretsmanagerbackend.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	smH := secretsmanagerbackend.NewHandler(sm)

	secret, err := sm.CreateSecret(t.Context(), &secretsmanagerbackend.CreateSecretInput{
		Name:         "git",
		SecretString: `{"ServerType":"GITHUB","AuthType":"PERSONAL_ACCESS_TOKEN","Token":"tok"}`,
	})
	require.NoError(t, err)

	cc := codeconnectionsbackend.NewInMemoryBackend("000000000000", "us-east-1")
	conn, err := cc.CreateConnection(t.Context(), "gh", "GitHub", "", nil)
	require.NoError(t, err)

	values := codebuildValues{sm: smH}

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "secret value", run: func(t *testing.T) {
			t.Helper()

			got, gerr := values.SecretValue(t.Context(), "us-east-1", secret.ARN, "", "")
			require.NoError(t, gerr)
			assert.Contains(t, got, `"Token":"tok"`)
		}},
		{name: "secret in another region", run: func(t *testing.T) {
			t.Helper()

			_, gerr := values.SecretValue(t.Context(), "eu-west-1", secret.ARN, "", "")
			require.Error(t, gerr)
		}},
		{name: "connection status", run: func(t *testing.T) {
			t.Helper()

			var res codebuildbackend.ConnectionResolver = cc

			status, serr := res.ConnectionStatus(t.Context(), "us-east-1", conn.ConnectionArn)
			require.NoError(t, serr)
			assert.Equal(t, "AVAILABLE", status)

			_, serr = res.ConnectionStatus(t.Context(), "eu-west-1", conn.ConnectionArn)
			require.Error(t, serr)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t)
		})
	}
}
