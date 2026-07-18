package batch_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/batch"
)

func TestHandler_SchedulingPolicy_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *batch.Handler)
		name       string
		wantStatus int
		wantARN    bool
	}{
		{
			name:       "create_success",
			wantStatus: http.StatusOK,
			wantARN:    true,
		},
		{
			name: "create_duplicate",
			setup: func(t *testing.T, h *batch.Handler) {
				t.Helper()
				rec := post(t, h, "/v1/createschedulingpolicy", map[string]any{
					"name": "my-policy",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := post(t, h, "/v1/createschedulingpolicy", map[string]any{
				"name": "my-policy",
				"tags": map[string]string{"env": "test"},
			})

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantARN {
				var out map[string]string
				mustUnmarshal(t, rec, &out)
				assert.Contains(t, out["arn"], "my-policy")
				assert.Equal(t, "my-policy", out["name"])
			}
		})
	}
}

func TestHandler_SchedulingPolicy_Delete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		useName    bool
	}{
		{name: "delete_success", wantStatus: http.StatusOK, useName: true},
		{name: "delete_not_found", wantStatus: http.StatusBadRequest, useName: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			policyARN := "nonexistent-arn"

			if tt.useName {
				rec := post(t, h, "/v1/createschedulingpolicy", map[string]any{
					"name": "del-policy",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var out map[string]string
				mustUnmarshal(t, rec, &out)
				policyARN = out["arn"]
			}

			delRec := post(t, h, "/v1/deleteschedulingpolicy", map[string]any{
				"arn": policyARN,
			})
			assert.Equal(t, tt.wantStatus, delRec.Code)
		})
	}
}

// --- ServiceEnvironment tests ---

func TestBatch_DescribeSchedulingPolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		arns      []string
		wantCount int
	}{
		{
			name:      "all_policies",
			arns:      nil,
			wantCount: 2,
		},
		{
			name:      "unknown_arn_omitted",
			arns:      []string{"arn:aws:batch:us-east-1:000000000000:scheduling-policy/nonexistent"},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for _, n := range []string{"policy-a", "policy-b"} {
				rec := post(t, h, "/v1/createschedulingpolicy", map[string]any{"name": n})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			body := map[string]any{}
			if tt.arns != nil {
				body["arns"] = tt.arns
			}

			rec := post(t, h, "/v1/describeschedulingpolicies", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			mustUnmarshal(t, rec, &out)

			items, _ := out["schedulingPolicies"].([]any)
			assert.Len(t, items, tt.wantCount)
		})
	}
}

// --- ListSchedulingPolicies tests ---

func TestBatch_ListSchedulingPolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		policies  []string
		wantCount int
	}{
		{
			name:      "empty",
			policies:  nil,
			wantCount: 0,
		},
		{
			name:      "populated",
			policies:  []string{"sp-one", "sp-two"},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			for _, n := range tt.policies {
				rec := post(t, h, "/v1/createschedulingpolicy", map[string]any{"name": n})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := post(t, h, "/v1/listschedulingpolicies", map[string]any{})
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			mustUnmarshal(t, rec, &out)

			items, _ := out["schedulingPolicies"].([]any)
			assert.Len(t, items, tt.wantCount)
		})
	}
}

// --- UpdateSchedulingPolicy tests ---

func TestBatch_UpdateSchedulingPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		arn        string
		createName string
		wantStatus int
	}{
		{
			name:       "success",
			createName: "up-policy",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			arn:        "arn:aws:batch:us-east-1:000000000000:scheduling-policy/nonexistent",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_arn",
			arn:        "",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			policyARN := tt.arn

			if tt.createName != "" {
				rec := post(t, h, "/v1/createschedulingpolicy", map[string]any{"name": tt.createName})
				require.Equal(t, http.StatusOK, rec.Code)

				var out map[string]string
				mustUnmarshal(t, rec, &out)
				policyARN = out["arn"]
			}

			rec := post(t, h, "/v1/updateschedulingpolicy", map[string]any{"arn": policyARN})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- DescribeServiceEnvironments tests ---

func TestBatch_SchedulingPolicyNameIndex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		policyName string
		wantStatus int
		createDupe bool
	}{
		{
			name:       "unique_policy_created",
			policyName: "unique-policy",
			wantStatus: http.StatusOK,
			createDupe: false,
		},
		{
			name:       "duplicate_name_rejected",
			policyName: "dupe-policy",
			wantStatus: http.StatusBadRequest,
			createDupe: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// First creation always succeeds.
			rec := post(t, h, "/v1/createschedulingpolicy", map[string]any{"name": tt.policyName})
			require.Equal(t, http.StatusOK, rec.Code)

			if tt.createDupe {
				rec = post(t, h, "/v1/createschedulingpolicy", map[string]any{"name": tt.policyName})
				assert.Equal(t, tt.wantStatus, rec.Code)
			}
		})
	}
}

// --- ServiceJob tests ---
