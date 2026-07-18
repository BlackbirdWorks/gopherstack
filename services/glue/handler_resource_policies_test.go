package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_PutResourcePolicy_ExistsConditions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// MUST_EXIST against a policy that doesn't exist yet -> EntityNotFoundException.
	rec := doGlueRequest(t, h, "PutResourcePolicy", map[string]any{
		"PolicyInJson":          `{"Version":"2012-10-17"}`,
		"PolicyExistsCondition": "MUST_EXIST",
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "EntityNotFoundException")

	// NOT_EXIST creates it fine.
	rec = doGlueRequest(t, h, "PutResourcePolicy", map[string]any{
		"PolicyInJson":          `{"Version":"2012-10-17"}`,
		"PolicyExistsCondition": "NOT_EXIST",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// NOT_EXIST again, now that it exists -> ConditionCheckFailureException.
	rec = doGlueRequest(t, h, "PutResourcePolicy", map[string]any{
		"PolicyInJson":          `{"Version":"2012-10-17"}`,
		"PolicyExistsCondition": "NOT_EXIST",
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ConditionCheckFailureException")

	// MUST_EXIST now succeeds.
	rec = doGlueRequest(t, h, "PutResourcePolicy", map[string]any{
		"PolicyInJson":          `{"Version":"2012-10-17","Id":"2"}`,
		"PolicyExistsCondition": "MUST_EXIST",
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func Test_PutResourcePolicy_HashCondition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "PutResourcePolicy", map[string]any{
		"PolicyInJson": `{"Version":"2012-10-17"}`,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		PolicyHash string `json:"PolicyHash"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotEmpty(t, out.PolicyHash)

	// Stale hash -> ConditionCheckFailureException, and the policy is untouched.
	rec = doGlueRequest(t, h, "PutResourcePolicy", map[string]any{
		"PolicyInJson":        `{"Version":"stale-write"}`,
		"PolicyHashCondition": "not-the-real-hash",
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ConditionCheckFailureException")

	// Correct hash succeeds.
	rec = doGlueRequest(t, h, "PutResourcePolicy", map[string]any{
		"PolicyInJson":        `{"Version":"2012-10-17","Id":"2"}`,
		"PolicyHashCondition": out.PolicyHash,
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func Test_DeleteResourcePolicy_HashCondition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "PutResourcePolicy", map[string]any{
		"PolicyInJson": `{"Version":"2012-10-17"}`,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		PolicyHash string `json:"PolicyHash"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	// Wrong hash -> rejected, policy stays.
	rec = doGlueRequest(t, h, "DeleteResourcePolicy", map[string]any{
		"PolicyHashCondition": "wrong-hash",
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ConditionCheckFailureException")

	rec = doGlueRequest(t, h, "GetResourcePolicy", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	// Correct hash deletes it.
	rec = doGlueRequest(t, h, "DeleteResourcePolicy", map[string]any{
		"PolicyHashCondition": out.PolicyHash,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "GetResourcePolicy", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestGetResourcePolicies_ReturnsStoredPolicies verifies GetResourcePolicies
// aggregates the account-level and per-resource policies set via
// PutResourcePolicy.
func TestGetResourcePolicies_ReturnsStoredPolicies(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "GetResourcePolicies", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var empty struct {
		GetResourcePoliciesResponseList []map[string]any `json:"GetResourcePoliciesResponseList"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &empty))
	assert.Empty(t, empty.GetResourcePoliciesResponseList)

	doGlueRequest(t, h, "PutResourcePolicy", map[string]any{
		"PolicyInJson": `{"Version":"2012-10-17","Statement":[]}`,
	})
	doGlueRequest(t, h, "PutResourcePolicy", map[string]any{
		"PolicyInJson": `{"Version":"2012-10-17","Statement":[{"Sid":"x"}]}`,
		"ResourceArn":  "arn:aws:glue:us-east-1:123456789012:catalog",
	})

	rec = doGlueRequest(t, h, "GetResourcePolicies", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		GetResourcePoliciesResponseList []map[string]any `json:"GetResourcePoliciesResponseList"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.GetResourcePoliciesResponseList, 2)

	for _, p := range out.GetResourcePoliciesResponseList {
		assert.NotEmpty(t, p["PolicyInJson"])
		assert.NotEmpty(t, p["PolicyHash"])
	}
}

// TestResourcePolicy tests resource policy CRUD.
func TestResourcePolicy(t *testing.T) {
	t.Parallel()
	h := newGlueHandler(t)

	// PutResourcePolicy
	out := dispatchNewOp(t, h, "PutResourcePolicy", map[string]any{
		"PolicyInJson": `{"Version":"2012-10-17","Statement":[]}`,
	})
	hash, _ := out["PolicyHash"].(string)
	if hash == "" {
		t.Error("expected non-empty PolicyHash")
	}

	// GetResourcePolicy
	out2 := dispatchNewOp(t, h, "GetResourcePolicy", map[string]any{})
	if out2["PolicyInJson"] == "" {
		t.Errorf("expected non-empty PolicyInJson: %v", out2)
	}

	// DeleteResourcePolicy
	dispatchNewOp(t, h, "DeleteResourcePolicy", map[string]any{})

	// Verify deletion
	dispatchNewOpExpectError(t, h, "GetResourcePolicy", map[string]any{})
}
