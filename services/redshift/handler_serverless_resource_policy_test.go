package redshift_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServerless_ResourcePolicy_CRUD(t *testing.T) {
	t.Parallel()

	h := newServerlessHandler()
	resourceArn := "arn:aws:redshift-serverless:us-east-1:000000000000:snapshot/rp-snap"
	policy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow"}]}`

	rec := doServerlessOp(t, h, "PutResourcePolicy", map[string]any{
		"resourceArn": resourceArn,
		"policy":      policy,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var putResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&putResp))
	rp, _ := putResp["resourcePolicy"].(map[string]any)
	require.NotNil(t, rp)
	assert.Equal(t, resourceArn, rp["resourceArn"])
	assert.Equal(t, policy, rp["policy"])

	rec = doServerlessOp(t, h, "GetResourcePolicy", map[string]any{"resourceArn": resourceArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&getResp))
	rp, _ = getResp["resourcePolicy"].(map[string]any)
	require.NotNil(t, rp)
	assert.Equal(t, policy, rp["policy"])

	rec = doServerlessOp(t, h, "DeleteResourcePolicy", map[string]any{"resourceArn": resourceArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var deleteResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&deleteResp))
	assert.Empty(t, deleteResp, "DeleteResourcePolicyResponse has zero members on the real wire")

	rec = doServerlessOp(t, h, "GetResourcePolicy", map[string]any{"resourceArn": resourceArn})
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestServerless_ResourcePolicy_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		op         string
		wantType   string
		wantStatus int
	}{
		{
			name:       "get missing resourceArn",
			op:         "GetResourcePolicy",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
			wantType:   "ValidationException",
		},
		{
			name: "get unknown policy",
			op:   "GetResourcePolicy",
			body: map[string]any{
				"resourceArn": "arn:aws:redshift-serverless:us-east-1:000000000000:snapshot/nope",
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "ResourceNotFoundException",
		},
		{
			name: "put missing policy",
			op:   "PutResourcePolicy",
			body: map[string]any{
				"resourceArn": "arn:aws:redshift-serverless:us-east-1:000000000000:snapshot/x",
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "ValidationException",
		},
		{
			name: "delete unknown policy",
			op:   "DeleteResourcePolicy",
			body: map[string]any{
				"resourceArn": "arn:aws:redshift-serverless:us-east-1:000000000000:snapshot/nope",
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newServerlessHandler()

			rec := doServerlessOp(t, h, tt.op, tt.body)
			require.Equal(t, tt.wantStatus, rec.Code)

			var errResp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
			assert.Equal(t, tt.wantType, errResp["__type"])
		})
	}
}
