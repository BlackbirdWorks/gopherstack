package iot_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

// TestBatch2_PolicyPrincipalOps tests policy/principal listing.
func TestPolicyPrincipalOps(t *testing.T) {
	t.Parallel()
	b := iot.NewInMemoryBackend()
	h := iot.NewHandler(b, nil)

	// Seed: policy + attachment
	b.AddPolicyInternal(iot.Policy{
		PolicyName:     "my-policy",
		PolicyDocument: `{"Version":"2012-10-17"}`,
		ARN:            "arn:aws:iot:us-east-1:000000000000:policy/my-policy",
	})
	_ = b.AttachPolicy(&iot.AttachPolicyInput{
		PolicyName: "my-policy",
		Target:     "arn:aws:iot:us-east-1:000000000000:cert/abc123",
	})

	// ListPolicyPrincipals (via header)
	out := iotOK(t, h, http.MethodGet, "/policy-principals", nil)
	// No header set so principals is empty — just verify response structure
	if out["principals"] == nil {
		t.Error("expected principals key in response")
	}

	// ListTargetsForPolicy
	out2 := iotOK(t, h, http.MethodGet, "/policy-targets/my-policy", nil)
	targets, _ := out2["targets"].([]any)
	if len(targets) != 1 {
		t.Errorf("expected 1 target, got %d", len(targets))
	}
}

// TestRefinement1_AttachPrincipalPolicy_Handler verifies AttachPrincipalPolicy via HTTP.
func TestAttachPrincipalPolicy_Handler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "attach_policy_to_principal", wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newRefHandler()
			rec := doRefRequest(t, h, http.MethodPut, "/principal-policies/my-policy", nil,
				map[string]string{"x-amzn-iot-principal": "arn:aws:iot:us-east-1:123456789012:cert/abc123"})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestGetPolicy(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	backend.AddPolicyInternal(iot.Policy{PolicyName: "my-policy", PolicyDocument: `{"Version":"2012-10-17"}`})

	out, err := backend.GetPolicy("my-policy")
	require.NoError(t, err)
	assert.Equal(t, "my-policy", out.PolicyName)
	assert.NotEmpty(t, out.PolicyARN)
	assert.JSONEq(t, `{"Version":"2012-10-17"}`, out.PolicyDocument)
}

func TestGetPolicy_NotFound(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	_, err := backend.GetPolicy("nonexistent")
	require.Error(t, err)
	assert.ErrorIs(t, err, iot.ErrPolicyNotFound)
}

func TestDeletePolicy(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	backend.AddPolicyInternal(iot.Policy{PolicyName: "del-policy"})
	require.Equal(t, 1, backend.PolicyCount())

	require.NoError(t, backend.DeletePolicy("del-policy"))
	assert.Equal(t, 0, backend.PolicyCount())
}

func TestDeletePolicy_NotFound(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	err := backend.DeletePolicy("nonexistent")
	require.Error(t, err)
	assert.ErrorIs(t, err, iot.ErrPolicyNotFound)
}

func TestListPolicies_Sorted(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	backend.AddPolicyInternal(iot.Policy{PolicyName: "z-policy"})
	backend.AddPolicyInternal(iot.Policy{PolicyName: "a-policy"})
	backend.AddPolicyInternal(iot.Policy{PolicyName: "m-policy"})

	policies := backend.ListPolicies()
	require.Len(t, policies, 3)
	assert.Equal(t, "a-policy", policies[0].PolicyName)
	assert.Equal(t, "m-policy", policies[1].PolicyName)
	assert.Equal(t, "z-policy", policies[2].PolicyName)
}

func TestHandler_ListPolicies_Response(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		setupPolicies []string
		wantCount     int
	}{
		{
			name:          "empty_list",
			setupPolicies: nil,
			wantCount:     0,
		},
		{
			name:          "two_policies",
			setupPolicies: []string{"pol-a", "pol-b"},
			wantCount:     2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := iot.NewInMemoryBackend()
			h := iot.NewHandler(backend, nil)

			for _, name := range tt.setupPolicies {
				backend.AddPolicyInternal(iot.Policy{PolicyName: name})
			}

			resp := doRequest(t, h, http.MethodGet, "/policies", nil)
			require.Equal(t, http.StatusOK, resp.Code)

			var out map[string]any
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
			policies, ok := out["policies"]
			require.True(t, ok, "response must have 'policies' key")

			policiesSlice, ok := policies.([]any)
			require.True(t, ok)
			assert.Len(t, policiesSlice, tt.wantCount)
		})
	}
}

func TestHandler_GetPolicy_HTTP(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	h := iot.NewHandler(backend, nil)
	backend.AddPolicyInternal(iot.Policy{PolicyName: "http-policy", PolicyDocument: `{}`})

	resp := doRequest(t, h, http.MethodGet, "/policies/http-policy", nil)
	require.Equal(t, http.StatusOK, resp.Code)

	// creationDate/lastModifiedDate are JSON numbers (epoch seconds), not
	// RFC3339 strings, so this must decode into map[string]any.
	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Equal(t, "http-policy", out["policyName"])
	assert.NotEmpty(t, out["policyArn"])

	for _, field := range []string{"creationDate", "lastModifiedDate"} {
		_, isNumber := out[field].(float64)
		assert.True(t, isNumber, "%s must be a JSON number (epoch seconds), got %T: %v", field, out[field], out[field])
	}
}

func TestHandler_DeletePolicy_HTTP(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	h := iot.NewHandler(backend, nil)
	backend.AddPolicyInternal(iot.Policy{PolicyName: "delete-http-policy"})

	resp := doRequest(t, h, http.MethodDelete, "/policies/delete-http-policy", nil)
	require.Equal(t, http.StatusNoContent, resp.Code)

	assert.Equal(t, 0, backend.PolicyCount())
}

func TestHandler_GetPolicy_NotFound_Returns404(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	h := iot.NewHandler(backend, nil)

	resp := doRequest(t, h, http.MethodGet, "/policies/nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, resp.Code)
}

func TestPolicyTargetCount_Helper(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	backend.AddPolicyInternal(iot.Policy{PolicyName: "attach-test-policy"})
	assert.Equal(t, 0, backend.PolicyTargetCount("attach-test-policy"))

	require.NoError(t, backend.AttachPolicy(&iot.AttachPolicyInput{
		PolicyName: "attach-test-policy",
		Target:     "arn:aws:iot:us-east-1:000000000000:cert/abc",
	}))
	assert.Equal(t, 1, backend.PolicyTargetCount("attach-test-policy"))
}

func TestCreatePolicy_ReturnsPolicyVersionID(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	var out map[string]string
	code := r3JSON(t, h, http.MethodPost, "/policies/test-policy", map[string]any{
		"policyDocument": `{"Version":"2012-10-17"}`,
	}, &out)
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, "1", out["policyVersionId"])
}

func TestCreatePolicy_AutoCreatesVersion1(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreatePolicy(&iot.CreatePolicyInput{
		PolicyName:     "auto-version-policy",
		PolicyDocument: `{"Version":"2012-10-17"}`,
	})
	require.NoError(t, err)

	versions, err := b.ListPolicyVersions("auto-version-policy")
	require.NoError(t, err)
	require.Len(t, versions, 1)
	assert.Equal(t, "1", versions[0].VersionID)
	assert.True(t, versions[0].IsDefaultVersion)
}

func TestCreatePolicy_Version1IsDefault(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreatePolicy(&iot.CreatePolicyInput{
		PolicyName:     "default-v1-policy",
		PolicyDocument: `{"Version":"2012-10-17","Statement":[]}`,
	})
	require.NoError(t, err)

	pv, err := b.GetPolicyVersion("default-v1-policy", "1")
	require.NoError(t, err)
	assert.True(t, pv.IsDefaultVersion)
}

func TestGetPolicy_ReturnsDefaultVersionID(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	r3Req(t, h, http.MethodPost, "/policies/versioned-policy", map[string]any{
		"policyDocument": `{"Version":"2012-10-17"}`,
	})

	var out map[string]any
	code := r3JSON(t, h, http.MethodGet, "/policies/versioned-policy", nil, &out)
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, "1", out["defaultVersionId"])
}

func TestGetPolicy_ReturnsCreationDate(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	r3Req(t, h, http.MethodPost, "/policies/dated-policy", map[string]any{
		"policyDocument": `{"Version":"2012-10-17"}`,
	})

	var out map[string]any
	code := r3JSON(t, h, http.MethodGet, "/policies/dated-policy", nil, &out)
	require.Equal(t, http.StatusOK, code)
	assert.NotEmpty(t, out["creationDate"])
	assert.NotEmpty(t, out["lastModifiedDate"])
}

func TestGetPolicy_DefaultVersionUpdatesAfterSetDefault(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreatePolicy(&iot.CreatePolicyInput{
		PolicyName:     "multi-ver-policy",
		PolicyDocument: `{"Version":"2012-10-17"}`,
	})
	require.NoError(t, err)

	_, err = b.CreatePolicyVersion(&iot.CreatePolicyVersionInput{
		PolicyName:     "multi-ver-policy",
		PolicyDocument: `{"Version":"2012-10-17","Statement":[]}`,
		SetAsDefault:   true,
	})
	require.NoError(t, err)

	out, err := b.GetPolicy("multi-ver-policy")
	require.NoError(t, err)
	assert.Equal(t, "2", out.DefaultVersionID)
}

func TestPolicy_CreationDateNotZero(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreatePolicy(&iot.CreatePolicyInput{
		PolicyName:     "ts-policy",
		PolicyDocument: `{}`,
	})
	require.NoError(t, err)

	out, err := b.GetPolicy("ts-policy")
	require.NoError(t, err)
	assert.False(t, out.CreatedAt.IsZero(), "CreatedAt should not be zero")
	assert.False(t, out.LastModifiedAt.IsZero(), "LastModifiedAt should not be zero")
}

func TestPolicy_LastModifiedUpdatesOnNewVersion(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreatePolicy(&iot.CreatePolicyInput{
		PolicyName:     "lm-policy",
		PolicyDocument: `{}`,
	})
	require.NoError(t, err)

	before, err := b.GetPolicy("lm-policy")
	require.NoError(t, err)

	_, err = b.CreatePolicyVersion(&iot.CreatePolicyVersionInput{
		PolicyName:     "lm-policy",
		PolicyDocument: `{"updated": true}`,
		SetAsDefault:   false,
	})
	require.NoError(t, err)

	after, err := b.GetPolicy("lm-policy")
	require.NoError(t, err)

	assert.False(t, after.LastModifiedAt.Before(before.LastModifiedAt),
		"LastModifiedAt should be >= initial value after CreatePolicyVersion")
}

func TestDeletePolicyVersion_DefaultVersionBlocked(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreatePolicy(&iot.CreatePolicyInput{
		PolicyName:     "del-default-policy",
		PolicyDocument: `{}`,
	})
	require.NoError(t, err)

	err = b.DeletePolicyVersion("del-default-policy", "1")
	require.ErrorIs(t, err, iot.ErrDeleteConflict)
}

func TestDeletePolicyVersion_NonDefaultCanBeDeleted(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreatePolicy(&iot.CreatePolicyInput{
		PolicyName:     "del-v2-policy",
		PolicyDocument: `{}`,
	})
	require.NoError(t, err)

	_, err = b.CreatePolicyVersion(&iot.CreatePolicyVersionInput{
		PolicyName:     "del-v2-policy",
		PolicyDocument: `{"updated": true}`,
		SetAsDefault:   false,
	})
	require.NoError(t, err)

	err = b.DeletePolicyVersion("del-v2-policy", "2")
	require.NoError(t, err)
}

func TestDeletePolicyVersion_AfterSetDefault_OldDefaultCanBeDeleted(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreatePolicy(&iot.CreatePolicyInput{
		PolicyName:     "swap-default-policy",
		PolicyDocument: `{}`,
	})
	require.NoError(t, err)

	_, err = b.CreatePolicyVersion(&iot.CreatePolicyVersionInput{
		PolicyName:     "swap-default-policy",
		PolicyDocument: `{"v2": true}`,
		SetAsDefault:   true,
	})
	require.NoError(t, err)

	err = b.DeletePolicyVersion("swap-default-policy", "1")
	require.NoError(t, err)
}

func TestDeletePolicyVersion_DefaultVersion_DeleteConflict(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreatePolicy(&iot.CreatePolicyInput{
		PolicyName:     "del-def-conflict",
		PolicyDocument: `{}`,
	})
	require.NoError(t, err)

	// Delete version 1 (the default) — should fail with ErrDeleteConflict
	err = b.DeletePolicyVersion("del-def-conflict", "1")
	require.ErrorIs(t, err, iot.ErrDeleteConflict)
}

func TestErrorFormat_PolicyNotFound_UsesAWSFormat(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	var out map[string]string
	code := r3JSON(t, h, http.MethodGet, "/policies/missing-policy", nil, &out)
	assert.Equal(t, http.StatusNotFound, code)
	assert.Equal(t, "ResourceNotFoundException", out["__type"])
}

func TestPolicyVersion_CreateAndList(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreatePolicy(&iot.CreatePolicyInput{
		PolicyName:     "vlist-policy",
		PolicyDocument: `{}`,
	})
	require.NoError(t, err)

	_, err = b.CreatePolicyVersion(&iot.CreatePolicyVersionInput{
		PolicyName:     "vlist-policy",
		PolicyDocument: `{"v2": true}`,
		SetAsDefault:   false,
	})
	require.NoError(t, err)

	versions, err := b.ListPolicyVersions("vlist-policy")
	require.NoError(t, err)
	assert.Len(t, versions, 2, "should have version 1 (auto-created) and version 2")
}

func TestPolicyVersion_SetDefault_ClearsOldDefault(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreatePolicy(&iot.CreatePolicyInput{
		PolicyName:     "setdef-policy",
		PolicyDocument: `{}`,
	})
	require.NoError(t, err)

	_, err = b.CreatePolicyVersion(&iot.CreatePolicyVersionInput{
		PolicyName:     "setdef-policy",
		PolicyDocument: `{"v2": true}`,
		SetAsDefault:   false,
	})
	require.NoError(t, err)

	err = b.SetDefaultPolicyVersion("setdef-policy", "2")
	require.NoError(t, err)

	v1, err := b.GetPolicyVersion("setdef-policy", "1")
	require.NoError(t, err)
	assert.False(t, v1.IsDefaultVersion, "version 1 should no longer be default")

	v2, err := b.GetPolicyVersion("setdef-policy", "2")
	require.NoError(t, err)
	assert.True(t, v2.IsDefaultVersion, "version 2 should now be default")
}

func TestCreatePolicy_DocumentMatchesVersion1(t *testing.T) {
	t.Parallel()

	const doc = `{"Version":"2012-10-17","Statement":[]}`
	_, b := newR3Handler()
	_, err := b.CreatePolicy(&iot.CreatePolicyInput{
		PolicyName:     "docmatch-policy",
		PolicyDocument: doc,
	})
	require.NoError(t, err)

	pv, err := b.GetPolicyVersion("docmatch-policy", "1")
	require.NoError(t, err)
	assert.JSONEq(t, doc, pv.PolicyDocument)
}

func TestGetPolicyVersion_NotFound_AfterDelete(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreatePolicy(&iot.CreatePolicyInput{
		PolicyName:     "del-get-policy",
		PolicyDocument: `{}`,
	})
	require.NoError(t, err)

	_, err = b.CreatePolicyVersion(&iot.CreatePolicyVersionInput{
		PolicyName:     "del-get-policy",
		PolicyDocument: `{"v2": true}`,
		SetAsDefault:   true,
	})
	require.NoError(t, err)

	err = b.DeletePolicyVersion("del-get-policy", "1")
	require.NoError(t, err)

	_, err = b.GetPolicyVersion("del-get-policy", "1")
	require.ErrorIs(t, err, iot.ErrPolicyVersionNotFound)
}

func TestReset_ClearsPoliciesAndVersions(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreatePolicy(&iot.CreatePolicyInput{
		PolicyName:     "reset-ver-policy",
		PolicyDocument: `{}`,
	})
	require.NoError(t, err)

	b.Reset()

	_, err = b.ListPolicyVersions("reset-ver-policy")
	require.ErrorIs(t, err, iot.ErrPolicyNotFound)
}

func TestGetPolicy_ReturnsSameDocument(t *testing.T) {
	t.Parallel()

	const doc = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow"}]}`
	h, _ := newR3Handler()
	r3Req(t, h, http.MethodPost, "/policies/doc-policy", map[string]any{
		"policyDocument": doc,
	})

	var out map[string]any
	code := r3JSON(t, h, http.MethodGet, "/policies/doc-policy", nil, &out)
	require.Equal(t, http.StatusOK, code)
	assert.JSONEq(t, doc, out["policyDocument"].(string))
}

func TestListPolicies_SortedByName(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	names := []string{"zoo-policy", "ant-policy", "mid-policy"}
	for _, n := range names {
		_, err := b.CreatePolicy(&iot.CreatePolicyInput{
			PolicyName:     n,
			PolicyDocument: `{}`,
		})
		require.NoError(t, err)
	}

	policies := b.ListPolicies()
	require.Len(t, policies, 3)
	assert.Equal(t, "ant-policy", policies[0].PolicyName)
	assert.Equal(t, "mid-policy", policies[1].PolicyName)
	assert.Equal(t, "zoo-policy", policies[2].PolicyName)
}

func TestDeletePolicy_RemovesFromList(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreatePolicy(&iot.CreatePolicyInput{
		PolicyName:     "to-delete-policy",
		PolicyDocument: `{}`,
	})
	require.NoError(t, err)

	err = b.DeletePolicy("to-delete-policy")
	require.NoError(t, err)

	_, err = b.GetPolicy("to-delete-policy")
	require.ErrorIs(t, err, iot.ErrPolicyNotFound)
}

func TestCreatePolicy_ARNFormat(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackendWithConfig("555566667777", "ca-central-1")
	out, err := b.CreatePolicy(&iot.CreatePolicyInput{
		PolicyName:     "arn-test-policy",
		PolicyDocument: `{}`,
	})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iot:ca-central-1:555566667777:policy/arn-test-policy", out.PolicyARN)
}

func TestGetPolicy_DefaultVersionID_AfterMultipleVersions(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreatePolicy(&iot.CreatePolicyInput{
		PolicyName:     "multi-default-policy",
		PolicyDocument: `{"v": 1}`,
	})
	require.NoError(t, err)

	// Add versions 2 and 3 without setting as default
	for range 2 {
		_, err = b.CreatePolicyVersion(&iot.CreatePolicyVersionInput{
			PolicyName:     "multi-default-policy",
			PolicyDocument: `{}`,
			SetAsDefault:   false,
		})
		require.NoError(t, err)
	}

	// Version 1 should still be default
	out, err := b.GetPolicy("multi-default-policy")
	require.NoError(t, err)
	assert.Equal(t, "1", out.DefaultVersionID)

	// Add version 4 and set as default
	_, err = b.CreatePolicyVersion(&iot.CreatePolicyVersionInput{
		PolicyName:     "multi-default-policy",
		PolicyDocument: `{"v": 4}`,
		SetAsDefault:   true,
	})
	require.NoError(t, err)

	out, err = b.GetPolicy("multi-default-policy")
	require.NoError(t, err)
	assert.Equal(t, "4", out.DefaultVersionID)
}

// TestParityB_PolicyVersionsPath verifies CreatePolicyVersion uses /versions (plural).
func TestPolicyVersionsPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		wantStatus int
	}{
		{
			name:       "create_version_plural_path",
			path:       "/policies/parb-pol/versions",
			wantStatus: http.StatusOK,
		},
		{
			name:       "list_versions_plural_path",
			path:       "/policies/parb-pol/versions",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			doRequest(t, h, http.MethodPost, "/policies/parb-pol",
				map[string]string{"policyDocument": `{"Version":"2012-10-17"}`})

			var rec *httptest.ResponseRecorder
			if tt.name == "create_version_plural_path" {
				rec = doRequest(t, h, http.MethodPost, tt.path,
					map[string]string{"policyDocument": `{"Version":"2012-10-17","v2":true}`})
			} else {
				rec = doRequest(t, h, http.MethodGet, tt.path, nil)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestParityB_GetPolicyVersion_ReturnsPolicyArnAndName verifies GetPolicyVersion returns policyArn and policyName.
func TestGetPolicyVersion_ReturnsPolicyArnAndName(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	doRequest(t, h, http.MethodPost, "/policies/parb-pol2",
		map[string]string{"policyDocument": `{"Version":"2012-10-17"}`})
	doRequest(t, h, http.MethodPost, "/policies/parb-pol2/versions?setAsDefault=true",
		map[string]string{"policyDocument": `{"Version":"2012-10-17","v2":true}`})

	rec := doRequest(t, h, http.MethodGet, "/policies/parb-pol2/versions/2", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "parb-pol2", resp["policyName"], "policyName must be in GetPolicyVersion response")
	policyARN, _ := resp["policyArn"].(string)
	assert.NotEmpty(t, policyARN, "policyArn must be in GetPolicyVersion response")
}

// TestParityB_DeletePolicy_WithTargets_Blocked verifies policies with attached targets
// cannot be deleted.
func TestDeletePolicy_WithTargets_Blocked(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantDeleteErr error
		name          string
		attachTarget  bool
	}{
		{
			name:          "with_target_blocked",
			attachTarget:  true,
			wantDeleteErr: iot.ErrDeleteConflict,
		},
		{
			name:          "no_target_allowed",
			attachTarget:  false,
			wantDeleteErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iot.NewInMemoryBackend()
			b.AddPolicyInternal(iot.Policy{PolicyName: "del-pol-" + tt.name, PolicyDocument: `{}`})

			if tt.attachTarget {
				err := b.AttachPolicy(&iot.AttachPolicyInput{
					PolicyName: "del-pol-" + tt.name,
					Target:     "arn:aws:iot:us-east-1:000000000000:cert/abc",
				})
				require.NoError(t, err)
			}

			err := b.DeletePolicy("del-pol-" + tt.name)
			if tt.wantDeleteErr != nil {
				require.ErrorIs(t, err, tt.wantDeleteErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestParityB_PolicyVersionLimit verifies a policy cannot have more than 5 versions.
func TestPolicyVersionLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		extraCounts int
		wantErr     bool
	}{
		{name: "four_versions_ok", extraCounts: 4, wantErr: false},
		{name: "five_versions_at_limit", extraCounts: 5, wantErr: false},
		{name: "six_versions_exceeds_limit", extraCounts: 6, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iot.NewInMemoryBackend()
			b.AddPolicyInternal(iot.Policy{PolicyName: "limit-pol", PolicyDocument: `{}`})

			var lastErr error
			for range tt.extraCounts {
				_, lastErr = b.CreatePolicyVersion(&iot.CreatePolicyVersionInput{
					PolicyName:     "limit-pol",
					PolicyDocument: `{}`,
				})
			}

			if tt.wantErr {
				require.ErrorIs(t, lastErr, iot.ErrVersionsLimitExceeded)
			} else {
				require.NoError(t, lastErr)
			}
		})
	}
}
