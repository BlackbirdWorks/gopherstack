package app

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
)

func TestCodeBuildCredentialsIssuedFromSTS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		byName  map[string]service.Registerable
		name    string
		wantErr bool
	}{
		{
			name: "sts wired",
			byName: map[string]service.Registerable{
				"STS": stsbackend.NewHandler(stsbackend.NewInMemoryBackend()),
			},
		},
		{name: "sts missing", byName: map[string]service.Registerable{}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := &codebuildCredentials{byName: tt.byName}
			creds, err := c.IssueBuildCredentials(t.Context(), "arn:aws:iam::000000000000:role/svc", "AWSCodeBuild-x")

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, creds.AccessKeyID)
			assert.NotEmpty(t, creds.SecretAccessKey)
			assert.NotEmpty(t, creds.SessionToken)
			assert.False(t, creds.Expires.IsZero())
		})
	}
}
