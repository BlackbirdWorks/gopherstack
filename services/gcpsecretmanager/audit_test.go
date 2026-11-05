package gcpsecretmanager_test

import (
	"testing"

	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

const smService = "google.cloud.secretmanager.v1.SecretManagerService."

func TestAuditLogs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		act          func(t *testing.T, e *env)
		name         string
		wantMethod   string
		wantResource string
		wantLog      string
		wantCode     int32
	}{
		{
			name: "create secret is admin activity", wantMethod: smService + "CreateSecret",
			wantResource: proj + "/secrets/audited", wantLog: "activity",
			act: func(t *testing.T, e *env) { t.Helper(); e.secret(t, "audited") },
		},
		{
			name: "access secret version is a data access read", wantMethod: smService + "AccessSecretVersion",
			wantResource: proj + "/secrets/audited/versions/1", wantLog: "data_access",
			act: func(t *testing.T, e *env) {
				t.Helper()
				s := e.secret(t, "audited")
				v := e.version(t, s.GetName(), "hunter2")
				_, err := e.client.AccessSecretVersion(
					t.Context(),
					&secretmanagerpb.AccessSecretVersionRequest{Name: v.GetName()},
				)
				require.NoError(t, err)
			},
		},
		{
			name: "a failed call records its status", wantMethod: smService + "CreateSecret",
			wantResource: proj, wantLog: "activity", wantCode: int32(codes.AlreadyExists),
			act: func(t *testing.T, e *env) {
				t.Helper()
				e.secret(t, "audited")
				_, err := e.client.CreateSecret(t.Context(), &secretmanagerpb.CreateSecretRequest{
					Parent: proj, SecretId: "audited", Secret: &secretmanagerpb.Secret{Replication: auto()},
				})
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			sink := &auditSink{}
			e.handler.Backend.Audit.SetSink(sink)

			tt.act(t, e)

			logs := sink.logs(t)
			require.NotEmpty(t, logs)

			last := logs[len(logs)-1]
			assert.Equal(t, tt.wantMethod, last.GetMethodName())
			assert.Equal(t, "secretmanager.googleapis.com", last.GetServiceName())
			assert.Equal(t, tt.wantCode, last.GetStatus().GetCode())
			assert.NotEmpty(t, last.GetAuthenticationInfo().GetPrincipalEmail())
			assert.Contains(t, sink.logNames()[len(logs)-1], "cloudaudit.googleapis.com%2F"+tt.wantLog)

			if tt.wantCode == 0 {
				assert.Equal(t, tt.wantResource, last.GetResourceName())
			}

			assert.Nil(t, last.GetRequest(), "request bodies, which can hold secret payloads, are never logged")
		})
	}
}

func TestAuditLogsSkipReadsAndWithoutSink(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	s := e.secret(t, "quiet")

	sink := &auditSink{}
	e.handler.Backend.Audit.SetSink(sink)

	_, err := e.client.GetSecret(t.Context(), &secretmanagerpb.GetSecretRequest{Name: s.GetName()})
	require.NoError(t, err)
	assert.Empty(t, sink.logs(t), "Get is neither admin activity nor audited data access")
}
