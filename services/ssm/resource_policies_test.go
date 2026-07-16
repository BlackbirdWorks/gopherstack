package ssm_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResourcePolicies(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)
	arn := `"arn:aws:ssm:us-east-1:000000000000:parameter/my-param"`

	// Get empty
	rec := doRequest(t, h, "GetResourcePolicies", `{"ResourceArn":`+arn+`}`)
	require.Equal(t, http.StatusOK, rec.Code)

	// Put
	rec = doRequest(
		t,
		h,
		"PutResourcePolicy",
		`{"ResourceArn":`+arn+`,"Policy":"{\"Version\":\"2012-10-17\"}"}`,
	)
	require.Equal(t, http.StatusOK, rec.Code)
	assertBodyContains(t, rec, "PolicyId")

	// Get shows policy
	rec = doRequest(t, h, "GetResourcePolicies", `{"ResourceArn":`+arn+`}`)
	assertBodyContains(t, rec, "Policies")
}
func TestResourcePolicies_FullCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		resourceArn string
		policy      string
	}{
		{
			name:        "parameter_policy",
			resourceArn: "arn:aws:ssm:us-east-1:123456789012:parameter/my-param",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
				`"Principal":{"AWS":"arn:aws:iam::111111111111:root"},` +
				`"Action":"ssm:GetParameter","Resource":"*"}]}`,
		},
		{
			name:        "opsmetadata_policy",
			resourceArn: "arn:aws:ssm:us-east-1:123456789012:opsmetadata/app/prod",
			policy:      `{"Version":"2012-10-17","Statement":[{"Effect":"Allow"}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)

			// Put policy.
			body, _ := json.Marshal(map[string]any{
				"ResourceArn": tt.resourceArn,
				"Policy":      tt.policy,
			})
			rec := doRequest(t, h, "PutResourcePolicy", string(body))
			require.Equal(t, http.StatusOK, rec.Code)

			var putResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &putResp))
			policyID := putResp["PolicyId"].(string)
			assert.NotEmpty(t, policyID)

			// Get policies.
			body, _ = json.Marshal(map[string]any{"ResourceArn": tt.resourceArn})
			rec = doRequest(t, h, "GetResourcePolicies", string(body))
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), policyID)

			// Delete policy.
			body, _ = json.Marshal(map[string]any{
				"ResourceArn": tt.resourceArn,
				"PolicyId":    policyID,
			})
			rec = doRequest(t, h, "DeleteResourcePolicy", string(body))
			require.Equal(t, http.StatusOK, rec.Code)

			// Get policies — should be empty.
			body, _ = json.Marshal(map[string]any{"ResourceArn": tt.resourceArn})
			rec = doRequest(t, h, "GetResourcePolicies", string(body))
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), `"Policies":[]`)
		})
	}
}
