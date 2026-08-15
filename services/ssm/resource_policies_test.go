package ssm_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
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
			policyHash := putResp["PolicyHash"].(string)
			assert.NotEmpty(t, policyID)
			assert.NotEmpty(t, policyHash)

			// Get policies.
			body, _ = json.Marshal(map[string]any{"ResourceArn": tt.resourceArn})
			rec = doRequest(t, h, "GetResourcePolicies", string(body))
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), policyID)

			// Delete policy.
			body, _ = json.Marshal(map[string]any{
				"ResourceArn": tt.resourceArn,
				"PolicyId":    policyID,
				"PolicyHash":  policyHash,
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

// TestResourcePolicies_RequiredFields covers the required members
// gopherstack's DeleteResourcePolicyInput/PutResourcePolicyInput previously
// lacked entirely (ResourceArn/PolicyId/PolicyHash and ResourceArn/Policy
// respectively, api_op_DeleteResourcePolicy.go/api_op_PutResourcePolicy.go)
// and silently discarded rather than rejecting an incomplete request.
func TestResourcePolicies_RequiredFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		op     string
		body   string
		wantIn string
	}{
		{"put_missing_resourcearn", "PutResourcePolicy", `{"Policy":"{}"}`, "ResourceArn"},
		{
			"put_missing_policy", "PutResourcePolicy",
			`{"ResourceArn":"arn:aws:ssm:us-east-1:000000000000:parameter/p"}`, "Policy",
		},
		{"delete_missing_resourcearn", "DeleteResourcePolicy", `{"PolicyId":"id","PolicyHash":"h"}`, "ResourceArn"},
		{
			"delete_missing_policyid", "DeleteResourcePolicy",
			`{"ResourceArn":"arn:aws:ssm:us-east-1:000000000000:parameter/p","PolicyHash":"h"}`, "PolicyId",
		},
		{
			"delete_missing_policyhash", "DeleteResourcePolicy",
			`{"ResourceArn":"arn:aws:ssm:us-east-1:000000000000:parameter/p","PolicyId":"id"}`, "PolicyHash",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			rec := doRequest(t, h, tt.op, tt.body)
			require.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), "ValidationException")
			assert.Contains(t, rec.Body.String(), tt.wantIn)
		})
	}
}

// TestResourcePolicies_UpdateInPlace drives the real SDK client through the
// create/update/read cycle the SDK doc comment describes: supplying an
// existing PolicyId (with the current PolicyHash) to PutResourcePolicy
// updates that policy in place rather than appending a duplicate. Before the
// fix, PolicyId/PolicyHash had no Go struct members at all, so every
// PutResourcePolicy call unconditionally appended a new policy.
func TestResourcePolicies_UpdateInPlace(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	arn := "arn:aws:ssm:us-east-1:000000000000:parameter/update-me"

	created, err := client.PutResourcePolicy(t.Context(), &ssmsdk.PutResourcePolicyInput{
		ResourceArn: aws.String(arn),
		Policy:      aws.String(`{"Version":"2012-10-17","Statement":[{"Sid":"v1"}]}`),
	})
	require.NoError(t, err)

	updated, err := client.PutResourcePolicy(t.Context(), &ssmsdk.PutResourcePolicyInput{
		ResourceArn: aws.String(arn),
		Policy:      aws.String(`{"Version":"2012-10-17","Statement":[{"Sid":"v2"}]}`),
		PolicyId:    created.PolicyId,
		PolicyHash:  created.PolicyHash,
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(created.PolicyId), aws.ToString(updated.PolicyId))
	assert.NotEqual(t, aws.ToString(created.PolicyHash), aws.ToString(updated.PolicyHash))

	got, err := client.GetResourcePolicies(t.Context(), &ssmsdk.GetResourcePoliciesInput{
		ResourceArn: aws.String(arn),
	})
	require.NoError(t, err)
	require.Len(t, got.Policies, 1, "update must replace the existing policy in place, not append a duplicate")
	assert.Contains(t, aws.ToString(got.Policies[0].Policy), "v2")
}

// TestResourcePolicies_UpdateConflict asserts a stale PolicyHash is rejected
// with ResourcePolicyConflictException rather than silently applied --
// PolicyHash exists specifically "to prevent multiple calls from attempting
// to overwrite a policy" (api_op_PutResourcePolicy.go doc comment).
func TestResourcePolicies_UpdateConflict(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	arn := "arn:aws:ssm:us-east-1:000000000000:parameter/conflict-me"

	created, err := client.PutResourcePolicy(t.Context(), &ssmsdk.PutResourcePolicyInput{
		ResourceArn: aws.String(arn),
		Policy:      aws.String(`{"Sid":"v1"}`),
	})
	require.NoError(t, err)

	_, err = client.PutResourcePolicy(t.Context(), &ssmsdk.PutResourcePolicyInput{
		ResourceArn: aws.String(arn),
		Policy:      aws.String(`{"Sid":"v2"}`),
		PolicyId:    created.PolicyId,
		PolicyHash:  aws.String("stale-hash"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ResourcePolicyConflictException")

	_, err = client.DeleteResourcePolicy(t.Context(), &ssmsdk.DeleteResourcePolicyInput{
		ResourceArn: aws.String(arn),
		PolicyId:    created.PolicyId,
		PolicyHash:  aws.String("stale-hash"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ResourcePolicyConflictException")
}

// TestResourcePolicies_UnknownPolicyID asserts an unknown PolicyId on an
// update/delete is a real ResourcePolicyNotFoundException, not a fabricated
// success or an uncaught 500 -- the sentinel this repo already declared
// (ErrResourcePolicyNotFound) previously had no case in
// classifySSMErrorExtended and was never actually returned by any code path.
func TestResourcePolicies_UnknownPolicyID(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	arn := "arn:aws:ssm:us-east-1:000000000000:parameter/missing-policy"

	_, err := client.PutResourcePolicy(t.Context(), &ssmsdk.PutResourcePolicyInput{
		ResourceArn: aws.String(arn),
		Policy:      aws.String(`{"Sid":"v1"}`),
		PolicyId:    aws.String("does-not-exist"),
		PolicyHash:  aws.String("does-not-matter"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ResourcePolicyNotFoundException")

	_, err = client.DeleteResourcePolicy(t.Context(), &ssmsdk.DeleteResourcePolicyInput{
		ResourceArn: aws.String(arn),
		PolicyId:    aws.String("does-not-exist"),
		PolicyHash:  aws.String("does-not-matter"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ResourcePolicyNotFoundException")
}

// TestResourcePolicies_Pagination asserts GetResourcePolicies' MaxResults/
// NextToken (present on the real GetResourcePoliciesInput but previously
// absent from gopherstack's Go struct entirely) actually paginate.
func TestResourcePolicies_Pagination(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	arn := "arn:aws:ssm:us-east-1:000000000000:opsmetadata/paged"

	for i := range 3 {
		_, err := client.PutResourcePolicy(t.Context(), &ssmsdk.PutResourcePolicyInput{
			ResourceArn: aws.String(arn),
			Policy:      aws.String(`{"Sid":"p` + string(rune('a'+i)) + `"}`),
		})
		require.NoError(t, err)
	}

	first, err := client.GetResourcePolicies(t.Context(), &ssmsdk.GetResourcePoliciesInput{
		ResourceArn: aws.String(arn),
		MaxResults:  aws.Int32(2),
	})
	require.NoError(t, err)
	require.Len(t, first.Policies, 2)
	require.NotNil(t, first.NextToken)

	second, err := client.GetResourcePolicies(t.Context(), &ssmsdk.GetResourcePoliciesInput{
		ResourceArn: aws.String(arn),
		MaxResults:  aws.Int32(2),
		NextToken:   first.NextToken,
	})
	require.NoError(t, err)
	assert.Len(t, second.Policies, 1)
}
