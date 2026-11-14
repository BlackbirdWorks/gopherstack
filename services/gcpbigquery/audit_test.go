package gcpbigquery_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuditLogs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		act          func(t *testing.T, s *sdkEnv)
		name         string
		wantMethod   string
		wantResource string
	}{
		{
			name: "dataset create", wantMethod: "google.cloud.bigquery.v2.DatasetService.InsertDataset",
			wantResource: "projects/" + sdkProject + "/datasets/audit_ds",
			act: func(t *testing.T, s *sdkEnv) {
				t.Helper()
				require.NoError(t, s.client.Dataset("audit_ds").Create(t.Context(), nil))
			},
		},
		{
			name: "dataset delete", wantMethod: "google.cloud.bigquery.v2.DatasetService.DeleteDataset",
			wantResource: "projects/" + sdkProject + "/datasets/audit_ds",
			act: func(t *testing.T, s *sdkEnv) {
				t.Helper()
				require.NoError(t, s.client.Dataset("audit_ds").Create(t.Context(), nil))
				require.NoError(t, s.client.Dataset("audit_ds").Delete(t.Context()))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newSDK(t)
			sink := &auditSink{}
			s.backend.Audit.SetSink(sink)

			tt.act(t, s)

			logs := sink.logs(t)
			require.NotEmpty(t, logs)

			last := logs[len(logs)-1]
			assert.Equal(t, tt.wantMethod, last.GetMethodName())
			assert.Equal(t, "bigquery.googleapis.com", last.GetServiceName())
			assert.Equal(t, tt.wantResource, last.GetResourceName())
		})
	}
}
