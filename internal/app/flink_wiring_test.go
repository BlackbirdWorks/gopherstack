package app

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
)

const flinkEngineTrust = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
	`"Principal":{"Service":"kinesisanalytics.amazonaws.com"},"Action":"sts:AssumeRole"}]}`

func TestFlinkDataDirPersistence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		want    string
		persist bool
	}{
		{name: "persist", persist: true, want: "flink"},
		{name: "memory"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			got := (&CLI{DataDir: dir, Persist: tt.persist}).GetFlinkDataDir()

			if tt.want == "" {
				assert.Empty(t, got)

				return
			}

			assert.Equal(t, dir+"/"+tt.want, got)
		})
	}
}

func TestFlinkRoleCredentialsFromSTS(t *testing.T) {
	t.Parallel()

	cli := &CLI{AccountID: "000000000000", Region: "us-east-1", Port: ":8125"}
	byName := newWiredServices(t, cli)

	iamBk := byName["IAM"].(*iambackend.Handler).Backend.(*iambackend.InMemoryBackend)
	_, err := iamBk.CreateRole("flink-exec", "/", flinkEngineTrust, "")
	require.NoError(t, err)

	_, err = iamBk.CreateRole("lambda-only", "/", lambdaEnvTrust, "")
	require.NoError(t, err)

	issuer := &flinkRoleCredentials{byName: byName}

	tests := []struct {
		name    string
		role    string
		wantErr bool
	}{
		{name: "trusted role", role: "arn:aws:iam::000000000000:role/flink-exec"},
		{name: "untrusted role", role: "arn:aws:iam::000000000000:role/lambda-only", wantErr: true},
		{name: "missing role", role: "arn:aws:iam::000000000000:role/nope", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c, ierr := issuer.IssueApplicationCredentials(tt.role, "kinesisanalytics-app")
			if tt.wantErr {
				require.Error(t, ierr)

				return
			}

			require.NoError(t, ierr)
			assert.NotEmpty(t, c.AccessKeyID)
			assert.NotEmpty(t, c.SessionToken)
			assert.False(t, c.Expires.IsZero())
		})
	}
}
