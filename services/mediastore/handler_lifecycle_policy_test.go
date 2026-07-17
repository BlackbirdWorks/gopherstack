package mediastore_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_LifecyclePolicy(t *testing.T) {
	t.Parallel()

	const lcPolicy = `{"rules":[]}`

	tests := []struct {
		name       string
		op         string
		wantStatus int
		withPolicy bool
	}{
		{
			name:       "put lifecycle policy",
			op:         "PutLifecyclePolicy",
			wantStatus: http.StatusOK,
		},
		{
			name:       "get lifecycle policy",
			op:         "GetLifecyclePolicy",
			withPolicy: true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete lifecycle policy",
			op:         "DeleteLifecyclePolicy",
			withPolicy: true,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			setupRec := doRequest(t, h, "CreateContainer", map[string]any{"ContainerName": "lifecycle-test"})
			require.Equal(t, http.StatusOK, setupRec.Code)

			if tt.withPolicy {
				putRec := doRequest(t, h, "PutLifecyclePolicy",
					map[string]any{"ContainerName": "lifecycle-test", "LifecyclePolicy": lcPolicy})
				require.Equal(t, http.StatusOK, putRec.Code)
			}

			body := map[string]any{"ContainerName": "lifecycle-test"}
			if tt.op == "PutLifecyclePolicy" {
				body["LifecyclePolicy"] = lcPolicy
			}

			result := doRequest(t, h, tt.op, body)
			assert.Equal(t, tt.wantStatus, result.Code)
		})
	}
}
