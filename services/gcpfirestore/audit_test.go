package gcpfirestore_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuditLogs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		act          func(t *testing.T, e *env)
		name         string
		wantMethod   string
		wantResource string
	}{
		{
			name: "set via the client is a Commit", wantMethod: "google.firestore.v1.Firestore.Commit",
			wantResource: docRoot + "/users/alice",
			act: func(t *testing.T, e *env) {
				t.Helper()
				_, err := e.client.Collection("users").Doc("alice").Set(t.Context(), map[string]any{"a": 1})
				require.NoError(t, err)
			},
		},
		{
			name: "delete via the client", wantMethod: "google.firestore.v1.Firestore.Commit",
			wantResource: docRoot + "/users/bob",
			act: func(t *testing.T, e *env) {
				t.Helper()
				doc := e.client.Collection("users").Doc("bob")
				_, err := doc.Set(t.Context(), map[string]any{"a": 1})
				require.NoError(t, err)
				_, err = doc.Delete(t.Context())
				require.NoError(t, err)
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
			assert.Equal(t, "firestore.googleapis.com", last.GetServiceName())
			assert.Equal(t, tt.wantResource, last.GetResourceName())
			assert.Contains(t, sink.logNames()[len(logs)-1], "cloudaudit.googleapis.com%2Fdata_access")
		})
	}
}
