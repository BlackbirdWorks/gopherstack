package bedrock_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateAutomatedReasoningPolicy(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		setup      func(*testing.T, *bedrock.Handler)
		input      map[string]any
		name       string
		wantStatus int
		wantFields bool
	}{
		{
			name:       "valid policy",
			input:      map[string]any{"name": "my-policy", "description": "test"},
			wantStatus: http.StatusCreated,
			wantFields: true,
		},
		{
			name:       "missing name",
			input:      map[string]any{"name": ""},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "duplicate name",
			setup: func(t *testing.T, h *bedrock.Handler) {
				t.Helper()
				_, err := h.Backend.CreateAutomatedReasoningPolicy("dup-policy", "", nil)
				require.NoError(t, err)
			},
			input:      map[string]any{"name": "dup-policy"},
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests { //nolint:paralleltest // existing issue.
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doRequest(t, h, http.MethodPost, "/automated-reasoning-policies", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantFields {
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				assert.NotEmpty(t, out["policyArn"])
				assert.Equal(t, "my-policy", out["name"])
				assert.Equal(t, "ACTIVE", out["status"])
			}
		})
	}
}

func TestHandler_CancelAutomatedReasoningPolicyBuildWorkflow( //nolint:paralleltest // existing issue.
	t *testing.T,
) {
	tests := []struct {
		name        string
		policyARN   string
		workflowID  string
		setupPolicy bool
		setupWF     bool
		wantStatus  int
	}{
		{
			name:        "cancel existing workflow",
			setupPolicy: true,
			setupWF:     true,
			wantStatus:  http.StatusOK,
		},
		{
			name:        "policy not found",
			policyARN:   "arn:aws:bedrock:us-east-1:000000000000:automated-reasoning-policy/nonexistent",
			workflowID:  "wf-001",
			setupPolicy: false,
			setupWF:     false,
			wantStatus:  http.StatusNotFound,
		},
		{
			name:        "workflow not found",
			setupPolicy: true,
			setupWF:     false,
			workflowID:  "nonexistent-wf",
			wantStatus:  http.StatusNotFound,
		},
	}

	for _, tt := range tests { //nolint:paralleltest // existing issue.
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler(t)

			policyARN := tt.policyARN
			workflowID := tt.workflowID

			if tt.setupPolicy {
				policy, err := h.Backend.CreateAutomatedReasoningPolicy("test-cancel-policy", "", nil)
				require.NoError(t, err)
				policyARN = policy.PolicyArn
			}

			if tt.setupWF {
				wf := h.Backend.AddBuildWorkflowForTest(policyARN)
				workflowID = wf.BuildWorkflowID
			}

			path := fmt.Sprintf("/automated-reasoning-policies/%s/build-workflows/%s/cancel",
				url.PathEscape(policyARN), workflowID)
			rec := doRequest(t, h, http.MethodPost, path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CreateAutomatedReasoningPolicyTestCase( //nolint:paralleltest // existing issue.
	t *testing.T,
) {
	tests := []struct {
		name        string
		policyARN   string
		setupPolicy bool
		wantStatus  int
	}{
		{
			name:        "valid test case",
			setupPolicy: true,
			wantStatus:  http.StatusCreated,
		},
		{
			name:        "policy not found",
			policyARN:   "arn:aws:bedrock:us-east-1:000000000000:automated-reasoning-policy/nonexistent",
			setupPolicy: false,
			wantStatus:  http.StatusNotFound,
		},
	}

	for _, tt := range tests { //nolint:paralleltest // existing issue.
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler(t)

			policyARN := tt.policyARN
			if tt.setupPolicy {
				policy, err := h.Backend.CreateAutomatedReasoningPolicy("tc-policy", "", nil)
				require.NoError(t, err)
				policyARN = policy.PolicyArn
			}

			path := fmt.Sprintf("/automated-reasoning-policies/%s/test-cases", url.PathEscape(policyARN))
			rec := doRequest(t, h, http.MethodPost, path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusCreated {
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				assert.NotEmpty(t, out["testCaseId"])
				assert.Equal(t, policyARN, out["policyArn"])
			}
		})
	}
}

func TestHandler_CreateAutomatedReasoningPolicyVersion(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		name        string
		policyARN   string
		setupPolicy bool
		wantStatus  int
	}{
		{
			name:        "valid version",
			setupPolicy: true,
			wantStatus:  http.StatusCreated,
		},
		{
			name:        "policy not found",
			policyARN:   "arn:aws:bedrock:us-east-1:000000000000:automated-reasoning-policy/nonexistent",
			setupPolicy: false,
			wantStatus:  http.StatusNotFound,
		},
	}

	for _, tt := range tests { //nolint:paralleltest // existing issue.
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler(t)

			policyARN := tt.policyARN
			if tt.setupPolicy {
				policy, err := h.Backend.CreateAutomatedReasoningPolicy("ver-policy", "", nil)
				require.NoError(t, err)
				policyARN = policy.PolicyArn
			}

			path := fmt.Sprintf("/automated-reasoning-policies/%s/versions", url.PathEscape(policyARN))
			rec := doRequest(t, h, http.MethodPost, path, map[string]any{
				"lastUpdatedDefinitionHash": "abc123hash",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusCreated {
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				assert.NotEmpty(t, out["policyArn"])
				assert.NotEmpty(t, out["version"])
				assert.Equal(t, "abc123hash", out["definitionHash"])
			}
		})
	}
}

func TestHandler_CancelWorkflowWrongPolicy(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	policy1, err := h.Backend.CreateAutomatedReasoningPolicy("policy-1", "", nil)
	require.NoError(t, err)

	policy2, err := h.Backend.CreateAutomatedReasoningPolicy("policy-2", "", nil)
	require.NoError(t, err)

	wf := h.Backend.AddBuildWorkflowForTest(policy1.PolicyArn)

	// Cancel using wrong policy ARN should return 404.
	path := fmt.Sprintf("/automated-reasoning-policies/%s/build-workflows/%s/cancel",
		url.PathEscape(policy2.PolicyArn), wf.BuildWorkflowID)
	rec := doRequest(t, h, http.MethodPost, path, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_TagsOnAutomatedReasoningPolicy(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/automated-reasoning-policies", map[string]any{
		"name": "tagged-policy",
		"tags": []map[string]string{
			{"key": "team", "value": "platform"},
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	policyARN := out["policyArn"].(string)

	rec2 := doRequest(t, h, http.MethodPost, "/listTagsForResource", map[string]any{
		"resourceARN": policyARN,
	})
	assert.Equal(t, http.StatusOK, rec2.Code)

	var tagsOut map[string]any
	mustUnmarshal(t, rec2, &tagsOut)
	tags := tagsOut["tags"].([]any)
	assert.Len(t, tags, 1)
	assert.Equal(t, "team", tags[0].(map[string]any)["key"])
}

func TestHandler_PerPolicyVersionNumbering(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	p1, err := h.Backend.CreateAutomatedReasoningPolicy("policy-ver-1", "", nil)
	require.NoError(t, err)

	p2, err := h.Backend.CreateAutomatedReasoningPolicy("policy-ver-2", "", nil)
	require.NoError(t, err)

	// Create versions for each policy independently.
	rec1 := doRequest(t, h, http.MethodPost,
		"/automated-reasoning-policies/"+url.PathEscape(p1.PolicyArn)+"/versions",
		map[string]any{"lastUpdatedDefinitionHash": "hash1"})
	require.Equal(t, http.StatusCreated, rec1.Code)

	rec2 := doRequest(t, h, http.MethodPost,
		"/automated-reasoning-policies/"+url.PathEscape(p2.PolicyArn)+"/versions",
		map[string]any{"lastUpdatedDefinitionHash": "hash2"})
	require.Equal(t, http.StatusCreated, rec2.Code)

	// Both should be version "1" (per-policy counter).
	var v1Out map[string]any
	mustUnmarshal(t, rec1, &v1Out)
	assert.Equal(t, "1", v1Out["version"])

	var v2Out map[string]any
	mustUnmarshal(t, rec2, &v2Out)
	assert.Equal(t, "1", v2Out["version"])

	// Second version for p1 should be "2".
	rec3 := doRequest(t, h, http.MethodPost,
		"/automated-reasoning-policies/"+url.PathEscape(p1.PolicyArn)+"/versions",
		map[string]any{"lastUpdatedDefinitionHash": "hash3"})
	require.Equal(t, http.StatusCreated, rec3.Code)

	var v3Out map[string]any
	mustUnmarshal(t, rec3, &v3Out)
	assert.Equal(t, "2", v3Out["version"])
}

func TestAccuracy_ARPVersion_PerPolicyNumberingIsolated(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")

	p1, err := b.CreateAutomatedReasoningPolicy("policy-alpha", "", nil)
	require.NoError(t, err)

	p2, err := b.CreateAutomatedReasoningPolicy("policy-beta", "", nil)
	require.NoError(t, err)

	// Each policy starts its own counter.
	v1a, err := b.CreateAutomatedReasoningPolicyVersion(p1.PolicyArn, "notes")
	require.NoError(t, err)
	assert.Equal(t, "1", v1a.Version)

	v1b, err := b.CreateAutomatedReasoningPolicyVersion(p2.PolicyArn, "notes")
	require.NoError(t, err)
	assert.Equal(t, "1", v1b.Version, "per-policy counter — beta starts at 1")

	v2a, err := b.CreateAutomatedReasoningPolicyVersion(p1.PolicyArn, "notes")
	require.NoError(t, err)
	assert.Equal(t, "2", v2a.Version)

	v2b, err := b.CreateAutomatedReasoningPolicyVersion(p2.PolicyArn, "notes")
	require.NoError(t, err)
	assert.Equal(t, "2", v2b.Version)
}

func TestAccuracy_ARPVersion_ViaHTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		wantLastVersion string
		versionCount    int
	}{
		{name: "single version", versionCount: 1, wantLastVersion: "1"},
		{name: "three versions", versionCount: 3, wantLastVersion: "3"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)
			policy, err := b.CreateAutomatedReasoningPolicy("versioned-policy-"+tt.name, "", nil)
			require.NoError(t, err)

			var lastVersion map[string]any
			for i := range tt.versionCount {
				rec := doRequest(t, h, http.MethodPost,
					"/automated-reasoning-policies/"+url.PathEscape(policy.PolicyArn)+"/versions",
					map[string]any{"notes": fmt.Sprintf("version %d", i+1)})
				require.Equal(t, http.StatusCreated, rec.Code)
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lastVersion))
			}

			assert.Equal(t, tt.wantLastVersion, lastVersion["version"])
			assert.NotEmpty(t, lastVersion["policyArn"])
		})
	}
}

func TestAccuracy_ARP_DescriptionPreservedOnCreate(t *testing.T) {
	t.Parallel()

	const desc = "reasons about financial transactions"

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/automated-reasoning-policies", map[string]any{
		"name":        "finance-arp",
		"description": desc,
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	policyARN := out["policyArn"].(string)

	getRec := doRequest(t, h, http.MethodGet, "/automated-reasoning-policies/"+policyARN, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	assert.Equal(t, desc, getOut["description"])
	assert.Equal(t, "finance-arp", getOut["name"])
}

func TestAccuracy_ARP_UpdateDescriptionReflected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/automated-reasoning-policies", map[string]any{
		"name": "updatable-arp",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	policyARN := out["policyArn"].(string)

	// Update description (real AWS: UpdateAutomatedReasoningPolicy uses PATCH)
	updateRec := doRequest(
		t, h, http.MethodPatch,
		"/automated-reasoning-policies/"+policyARN,
		map[string]any{"description": "updated description"},
	)
	require.Equal(t, http.StatusOK, updateRec.Code)

	getRec := doRequest(t, h, http.MethodGet, "/automated-reasoning-policies/"+policyARN, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	assert.Equal(t, "updated description", getOut["description"])
}

func TestAccuracy_ARP_VersionNumberingIsolatedPerPolicy(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)

	policy0, err := b.CreateAutomatedReasoningPolicy("iso-arp-0", "", nil)
	require.NoError(t, err)

	policy1, err := b.CreateAutomatedReasoningPolicy("iso-arp-1", "", nil)
	require.NoError(t, err)

	// Create 2 versions for policy 0 via HTTP
	for i := range 2 {
		rec := doRequest(t, h, http.MethodPost,
			"/automated-reasoning-policies/"+url.PathEscape(policy0.PolicyArn)+"/versions",
			map[string]any{"definitionHash": fmt.Sprintf("hash-0-%d", i)})
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	// Create 1 version for policy 1 via HTTP
	rec := doRequest(t, h, http.MethodPost,
		"/automated-reasoning-policies/"+url.PathEscape(policy1.PolicyArn)+"/versions",
		map[string]any{"definitionHash": "hash-1-0"})
	require.Equal(t, http.StatusCreated, rec.Code)

	// Verify second version for policy0 has version "2"
	var verOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &verOut))

	// policy1's first version is "1" (isolated counter)
	assert.Equal(t, "1", verOut["version"], "policy1 version counter starts from 1 independent of policy0")
}

func TestAccuracy_ARP_BuildWorkflowStatusTransitions(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)

	// Create a policy
	rec := doRequest(t, h, http.MethodPost, "/automated-reasoning-policies", map[string]any{
		"name": "workflow-test-arp",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	policyARN := out["policyArn"].(string)

	// Add a build workflow via test helper
	wf := b.AddBuildWorkflowForTest(policyARN)
	assert.Equal(t, "Running", wf.Status)

	// Get the workflow
	getRec := doRequest(t, h, http.MethodGet,
		"/automated-reasoning-policies/"+policyARN+"/build-workflows/"+wf.BuildWorkflowID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	assert.Equal(t, "Running", getOut["status"])
}

func TestAccuracy_ARP_TestCaseRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create policy
	rec := doRequest(t, h, http.MethodPost, "/automated-reasoning-policies", map[string]any{
		"name": "tc-arp",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	policyARN := out["policyArn"].(string)

	// Create test case (needs URL-encoded ARN in path)
	tcRec := doRequest(
		t, h, http.MethodPost,
		"/automated-reasoning-policies/"+url.PathEscape(policyARN)+"/test-cases",
		map[string]any{"input": "Is 2 + 2 = 4?"},
	)
	require.Equal(t, http.StatusCreated, tcRec.Code)

	var tcOut map[string]any
	require.NoError(t, json.Unmarshal(tcRec.Body.Bytes(), &tcOut))
	tcID := tcOut["testCaseId"].(string)
	assert.NotEmpty(t, tcID)

	// Get test case (needs URL-encoded ARN in path)
	getTestRec := doRequest(t, h, http.MethodGet,
		"/automated-reasoning-policies/"+url.PathEscape(policyARN)+"/test-cases/"+tcID, nil)
	require.Equal(t, http.StatusOK, getTestRec.Code)

	var getTestOut map[string]any
	require.NoError(t, json.Unmarshal(getTestRec.Body.Bytes(), &getTestOut))
	assert.Equal(t, tcID, getTestOut["testCaseId"])
}

func TestAccuracy_ARP_AnnotationsGetAfterUpdate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create policy
	rec := doRequest(t, h, http.MethodPost, "/automated-reasoning-policies", map[string]any{
		"name": "ann-arp",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	policyARN := out["policyArn"].(string)

	// Update annotations (needs URL-encoded ARN)
	updateRec := doRequest(
		t, h, http.MethodPatch,
		"/automated-reasoning-policies/"+url.PathEscape(policyARN)+"/annotations",
		map[string]any{"annotations": []map[string]any{
			{"key": "env", "value": "prod"},
		}},
	)
	require.Equal(t, http.StatusOK, updateRec.Code)

	// Get annotations - should not be empty
	getAnnRec := doRequest(t, h, http.MethodGet,
		"/automated-reasoning-policies/"+url.PathEscape(policyARN)+"/annotations", nil)
	require.Equal(t, http.StatusOK, getAnnRec.Code)

	var annOut map[string]any
	require.NoError(t, json.Unmarshal(getAnnRec.Body.Bytes(), &annOut))
	assert.Contains(t, annOut, "annotations")
}

func TestHandler_GetAutomatedReasoningPolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/automated-reasoning-policies", map[string]any{"name": "my-arp"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var created map[string]any
	mustUnmarshal(t, rec, &created)
	policyARN := created["policyArn"].(string)

	rec2 := doRequest(t, h, http.MethodGet, "/automated-reasoning-policies/"+url.PathEscape(policyARN), nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	mustUnmarshal(t, rec2, &out)
	assert.Equal(t, policyARN, out["policyArn"])
	assert.Equal(t, "my-arp", out["name"])
}

func TestHandler_GetAutomatedReasoningPolicy_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	path := "/automated-reasoning-policies/" + url.PathEscape("arn:aws:bedrock:us-east-1::arp/nope")
	rec := doRequest(t, h, http.MethodGet, path, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_UpdateAutomatedReasoningPolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/automated-reasoning-policies", map[string]any{"name": "upd-pol"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var created map[string]any
	mustUnmarshal(t, rec, &created)
	policyARN := created["policyArn"].(string)

	rec2 := doRequest(t, h, http.MethodPatch, "/automated-reasoning-policies/"+url.PathEscape(policyARN),
		map[string]any{"description": "new-desc"})
	assert.Equal(t, http.StatusOK, rec2.Code)
}

func TestHandler_DeleteAutomatedReasoningPolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/automated-reasoning-policies", map[string]any{"name": "del-pol"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var created map[string]any
	mustUnmarshal(t, rec, &created)
	policyARN := created["policyArn"].(string)

	rec2 := doRequest(t, h, http.MethodDelete, "/automated-reasoning-policies/"+url.PathEscape(policyARN), nil)
	assert.Equal(t, http.StatusNoContent, rec2.Code)

	rec3 := doRequest(t, h, http.MethodGet, "/automated-reasoning-policies/"+url.PathEscape(policyARN), nil)
	assert.Equal(t, http.StatusNotFound, rec3.Code)
}

func TestHandler_StartARPBuildWorkflow(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/automated-reasoning-policies", map[string]any{"name": "wf-pol"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var created map[string]any
	mustUnmarshal(t, rec, &created)
	policyARN := created["policyArn"].(string)

	rec2 := doRequest(t, h, http.MethodPost,
		"/automated-reasoning-policies/"+url.PathEscape(policyARN)+"/build-workflows", nil)
	assert.Equal(t, http.StatusCreated, rec2.Code)

	var wf map[string]any
	mustUnmarshal(t, rec2, &wf)
	assert.NotEmpty(t, wf["buildWorkflowId"])
}

func TestHandler_GetListDeleteARPBuildWorkflow(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/automated-reasoning-policies", map[string]any{"name": "wf2-pol"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var created map[string]any
	mustUnmarshal(t, rec, &created)
	policyARN := created["policyArn"].(string)

	recWF := doRequest(t, h, http.MethodPost,
		"/automated-reasoning-policies/"+url.PathEscape(policyARN)+"/build-workflows", nil)
	require.Equal(t, http.StatusCreated, recWF.Code)

	var wf map[string]any
	mustUnmarshal(t, recWF, &wf)
	wfID := wf["buildWorkflowId"].(string)

	// Get
	recGet := doRequest(t, h, http.MethodGet,
		"/automated-reasoning-policies/"+url.PathEscape(policyARN)+"/build-workflows/"+wfID, nil)
	assert.Equal(t, http.StatusOK, recGet.Code)

	// List
	recList := doRequest(t, h, http.MethodGet,
		"/automated-reasoning-policies/"+url.PathEscape(policyARN)+"/build-workflows", nil)
	require.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	mustUnmarshal(t, recList, &listOut)
	assert.Len(t, listOut["buildWorkflows"], 1)

	// Delete
	recDel := doRequest(t, h, http.MethodDelete,
		"/automated-reasoning-policies/"+url.PathEscape(policyARN)+"/build-workflows/"+wfID, nil)
	assert.Equal(t, http.StatusNoContent, recDel.Code)

	// List after delete
	recList2 := doRequest(t, h, http.MethodGet,
		"/automated-reasoning-policies/"+url.PathEscape(policyARN)+"/build-workflows", nil)
	var listOut2 map[string]any
	mustUnmarshal(t, recList2, &listOut2)
	assert.Empty(t, listOut2["buildWorkflows"])
}

func TestHandler_GetListDeleteARPTestCase(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/automated-reasoning-policies", map[string]any{"name": "tc-pol"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var created map[string]any
	mustUnmarshal(t, rec, &created)
	policyARN := created["policyArn"].(string)

	recTC := doRequest(t, h, http.MethodPost,
		"/automated-reasoning-policies/"+url.PathEscape(policyARN)+"/test-cases", nil)
	require.Equal(t, http.StatusCreated, recTC.Code)

	var tc map[string]any
	mustUnmarshal(t, recTC, &tc)
	tcID := tc["testCaseId"].(string)

	// Get
	recGet := doRequest(t, h, http.MethodGet,
		"/automated-reasoning-policies/"+url.PathEscape(policyARN)+"/test-cases/"+tcID, nil)
	assert.Equal(t, http.StatusOK, recGet.Code)

	// List
	recList := doRequest(t, h, http.MethodGet,
		"/automated-reasoning-policies/"+url.PathEscape(policyARN)+"/test-cases", nil)
	require.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	mustUnmarshal(t, recList, &listOut)
	assert.Len(t, listOut["testCases"], 1)

	// Update
	recUpd := doRequest(t, h, http.MethodPatch,
		"/automated-reasoning-policies/"+url.PathEscape(policyARN)+"/test-cases/"+tcID,
		map[string]any{
			"guardContent":                     "the model said X",
			"expectedAggregatedFindingsResult": "VALID",
		})
	assert.Equal(t, http.StatusOK, recUpd.Code)

	// Delete
	recDel := doRequest(t, h, http.MethodDelete,
		"/automated-reasoning-policies/"+url.PathEscape(policyARN)+"/test-cases/"+tcID, nil)
	assert.Equal(t, http.StatusNoContent, recDel.Code)

	// Get after delete
	recGet2 := doRequest(t, h, http.MethodGet,
		"/automated-reasoning-policies/"+url.PathEscape(policyARN)+"/test-cases/"+tcID, nil)
	assert.Equal(t, http.StatusNotFound, recGet2.Code)
}

// TestAccuracy_ARPTestCase_UpdateAppliesContent locks in that
// UpdateAutomatedReasoningPolicyTestCase actually parses and stores its request
// body (aws-sdk-go-v2 api_op_UpdateAutomatedReasoningPolicyTestCase.go:29-71) --
// gopherstack previously accepted PATCH bodies but never read them, only echoing
// testCaseId/policyArn back.
func TestAccuracy_ARPTestCase_UpdateAppliesContent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/automated-reasoning-policies", map[string]any{"name": "upd-content-pol"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var created map[string]any
	mustUnmarshal(t, rec, &created)
	policyARN := created["policyArn"].(string)

	recTC := doRequest(t, h, http.MethodPost,
		"/automated-reasoning-policies/"+url.PathEscape(policyARN)+"/test-cases", nil)
	require.Equal(t, http.StatusCreated, recTC.Code)

	var tc map[string]any
	mustUnmarshal(t, recTC, &tc)
	tcID := tc["testCaseId"].(string)

	confidence := 0.85
	updRec := doRequest(t, h, http.MethodPatch,
		"/automated-reasoning-policies/"+url.PathEscape(policyARN)+"/test-cases/"+tcID,
		map[string]any{
			"guardContent":                     "the model said the sky is blue",
			"queryContent":                     "what color is the sky?",
			"expectedAggregatedFindingsResult": "VALID",
			"confidenceThreshold":              confidence,
		})
	require.Equal(t, http.StatusOK, updRec.Code)

	getRec := doRequest(t, h, http.MethodGet,
		"/automated-reasoning-policies/"+url.PathEscape(policyARN)+"/test-cases/"+tcID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut map[string]any
	mustUnmarshal(t, getRec, &getOut)
	assert.Equal(t, "the model said the sky is blue", getOut["guardContent"])
	assert.Equal(t, "what color is the sky?", getOut["queryContent"])
	assert.Equal(t, "VALID", getOut["expectedAggregatedFindingsResult"])
	assert.InEpsilon(t, confidence, getOut["confidenceThreshold"], 0)
}

// TestAccuracy_ARPTestCase_UpdateRequiresGuardContent locks in that
// UpdateAutomatedReasoningPolicyTestCase's required guardContent and
// expectedAggregatedFindingsResult fields are enforced, not silently accepted
// as absent.
func TestAccuracy_ARPTestCase_UpdateRequiresGuardContent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/automated-reasoning-policies", map[string]any{"name": "req-content-pol"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var created map[string]any
	mustUnmarshal(t, rec, &created)
	policyARN := created["policyArn"].(string)

	recTC := doRequest(t, h, http.MethodPost,
		"/automated-reasoning-policies/"+url.PathEscape(policyARN)+"/test-cases", nil)
	require.Equal(t, http.StatusCreated, recTC.Code)

	var tc map[string]any
	mustUnmarshal(t, recTC, &tc)
	tcID := tc["testCaseId"].(string)

	updRec := doRequest(t, h, http.MethodPatch,
		"/automated-reasoning-policies/"+url.PathEscape(policyARN)+"/test-cases/"+tcID, nil)
	assert.Equal(t, http.StatusBadRequest, updRec.Code)

	getRec := doRequest(t, h, http.MethodGet,
		"/automated-reasoning-policies/"+url.PathEscape(policyARN)+"/test-cases/"+tcID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut map[string]any
	mustUnmarshal(t, getRec, &getOut)
	assert.Empty(t, getOut["guardContent"], "a rejected update must not mutate the test case")
}

// TestAccuracy_ARP_UpdateOpsRejectOldPUTMethod locks in the parity fix for
// three ARP update ops that real AWS routes on PATCH: UpdateAutomatedReasoningPolicy,
// UpdateAutomatedReasoningPolicyTestCase, and UpdateAutomatedReasoningPolicyAnnotations.
// gopherstack previously routed all three on PUT, making them 100% unreachable
// by real SDK clients (which always send PATCH for these ops).
func TestAccuracy_ARP_UpdateOpsRejectOldPUTMethod(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/automated-reasoning-policies",
		map[string]any{"name": "put-rejected-policy"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	policyARN := out["policyArn"].(string)

	tcRec := doRequest(t, h, http.MethodPost,
		"/automated-reasoning-policies/"+url.PathEscape(policyARN)+"/test-cases", nil)
	require.Equal(t, http.StatusCreated, tcRec.Code)

	var tcOut map[string]any
	require.NoError(t, json.Unmarshal(tcRec.Body.Bytes(), &tcOut))
	tcID := tcOut["testCaseId"].(string)

	tests := []struct {
		name string
		path string
	}{
		{name: "UpdateAutomatedReasoningPolicy", path: "/automated-reasoning-policies/" + url.PathEscape(policyARN)},
		{
			name: "UpdateAutomatedReasoningPolicyTestCase",
			path: "/automated-reasoning-policies/" + url.PathEscape(policyARN) + "/test-cases/" + tcID,
		},
		{
			name: "UpdateAutomatedReasoningPolicyAnnotations",
			path: "/automated-reasoning-policies/" + url.PathEscape(policyARN) + "/annotations",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			putRec := doRequest(t, h, http.MethodPut, tt.path, map[string]any{"description": "x"})
			assert.Equal(t, http.StatusNotFound, putRec.Code)
		})
	}
}
