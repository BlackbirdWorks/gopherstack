package omics_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOmics_Configuration exercises the Configuration CRUD family.
func TestOmics_Configuration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		check    func(t *testing.T, body []byte)
		body     any
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "CreateConfiguration returns 201",
			method:   http.MethodPost,
			path:     "/configuration",
			body:     map[string]any{"name": "cfg1", "description": "desc"},
			wantCode: http.StatusCreated,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "cfg1", resp["name"])
			},
		},
		{
			name:     "GetConfiguration unknown returns 404",
			method:   http.MethodGet,
			path:     "/configuration/doesnotexist",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

// TestS3AccessPolicy_WireShape verifies the S3AccessPolicy family's wire
// shape, field-diffed against PutS3AccessPolicyInput/Output and
// GetS3AccessPolicyOutput: PutS3AccessPolicy's response includes the
// s3AccessPointArn it was called with, and GetS3AccessPolicy returns the
// policy document under the real key "s3AccessPolicy" (previously the
// invented key "policy", which real SDK clients never populate).
func TestS3AccessPolicy_WireShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	apArn := "arn:aws:s3:us-east-1:000000000000:accesspoint/my-ap"

	putRec := doRequest(t, h, http.MethodPut, "/s3accesspolicy/"+apArn, map[string]any{
		"s3AccessPolicy": `{"Version":"2012-10-17"}`,
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	var putResp map[string]any
	require.NoError(t, json.Unmarshal(putRec.Body.Bytes(), &putResp))
	assert.Equal(t, apArn, putResp["s3AccessPointArn"])

	getRec := doRequest(t, h, http.MethodGet, "/s3accesspolicy/"+apArn, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	policy, _ := getResp["s3AccessPolicy"].(string)
	assert.JSONEq(t, `{"Version":"2012-10-17"}`, policy, "real wire key is s3AccessPolicy")
	assert.Nil(t, getResp["policy"], "policy is not a real wire key")
	assert.Equal(t, apArn, getResp["s3AccessPointArn"])
}
