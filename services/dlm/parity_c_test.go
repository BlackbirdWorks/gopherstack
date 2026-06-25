package dlm_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dlm"
)

func TestParityC_GetLifecyclePolicies_DeterministicOrder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		count int
	}{
		{name: "five_policies", count: 5},
		{name: "three_policies", count: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for range tt.count {
				createPolicy(t, h)
			}

			firstRec := doRequest(t, h, http.MethodGet, "/policies", nil)
			require.Equal(t, http.StatusOK, firstRec.Code)

			secondRec := doRequest(t, h, http.MethodGet, "/policies", nil)
			require.Equal(t, http.StatusOK, secondRec.Code)

			assert.JSONEq(t, firstRec.Body.String(), secondRec.Body.String())

			var resp map[string]any
			require.NoError(t, json.Unmarshal(firstRec.Body.Bytes(), &resp))
			policies, ok := resp["Policies"].([]any)
			require.True(t, ok)
			require.Len(t, policies, tt.count)

			// Verify ascending PolicyID order.
			ids := make([]string, len(policies))
			for i, p := range policies {
				pm := p.(map[string]any)
				ids[i] = pm["PolicyId"].(string)
			}

			for i := 1; i < len(ids); i++ {
				assert.LessOrEqual(t, ids[i-1], ids[i], "policies not sorted ascending by PolicyID")
			}
		})
	}
}

func TestParityC_UpdateLifecyclePolicy_PolicyDetails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		createDetails map[string]any
		updateDetails map[string]any
		wantType      string
	}{
		{
			name:          "update_policy_type",
			createDetails: map[string]any{"PolicyType": "EBS_SNAPSHOT_MANAGEMENT"},
			updateDetails: map[string]any{"PolicyType": "IMAGE_MANAGEMENT"},
			wantType:      "IMAGE_MANAGEMENT",
		},
		{
			name:          "update_with_schedules",
			createDetails: map[string]any{"PolicyType": "EBS_SNAPSHOT_MANAGEMENT"},
			updateDetails: map[string]any{
				"PolicyType": "EBS_SNAPSHOT_MANAGEMENT",
				"Schedules":  []any{map[string]any{"Name": "daily"}},
			},
			wantType: "EBS_SNAPSHOT_MANAGEMENT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create with initial PolicyDetails.
			createRec := doRequest(t, h, http.MethodPost, "/policies", map[string]any{
				"Description":      "test",
				"ExecutionRoleArn": "arn:aws:iam::000000000000:role/dlm-role",
				"State":            "ENABLED",
				"PolicyDetails":    tt.createDetails,
			})
			require.Equal(t, http.StatusCreated, createRec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
			policyID := createResp["PolicyId"].(string)

			// Update with new PolicyDetails.
			patchRec := doRequest(t, h, http.MethodPatch, fmt.Sprintf("/policies/%s", policyID), map[string]any{
				"PolicyDetails": tt.updateDetails,
			})
			assert.Equal(t, http.StatusOK, patchRec.Code)

			// GET and verify PolicyDetails updated.
			getRec := doRequest(t, h, http.MethodGet, fmt.Sprintf("/policies/%s", policyID), nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
			policy := getResp["Policy"].(map[string]any)
			details := policy["PolicyDetails"].(map[string]any)
			assert.Equal(t, tt.wantType, details["PolicyType"])
		})
	}
}

func TestParityC_CounterSerialization_NoCollision(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "counter_survives_restore"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b1 := dlm.NewInMemoryBackend("000000000000", "us-east-1")

			// Create 2 policies.
			p1, err := b1.CreateLifecyclePolicy("p1", "arn:aws:iam::000000000000:role/r", "ENABLED", nil, nil)
			require.NoError(t, err)
			p2, err := b1.CreateLifecyclePolicy("p2", "arn:aws:iam::000000000000:role/r", "ENABLED", nil, nil)
			require.NoError(t, err)

			existingIDs := map[string]struct{}{
				p1.PolicyID: {},
				p2.PolicyID: {},
			}

			// Snapshot and restore into fresh backend.
			snap := b1.Snapshot(context.Background())
			b2 := dlm.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, b2.Restore(context.Background(), snap))

			// Create a new policy in restored backend — must not collide.
			p3, err := b2.CreateLifecyclePolicy("p3", "arn:aws:iam::000000000000:role/r", "ENABLED", nil, nil)
			require.NoError(t, err)

			_, collision := existingIDs[p3.PolicyID]
			assert.False(t, collision, "new policyID %s collides with existing IDs", p3.PolicyID)

			// New ID must be strictly greater (lexicographically) than existing ones.
			assert.Greater(t, p3.PolicyID, p2.PolicyID)
		})
	}
}

func TestParityC_Tags_SurviveSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "tags_via_tagresource"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b1 := dlm.NewInMemoryBackend("000000000000", "us-east-1")

			p, err := b1.CreateLifecyclePolicy("test", "arn:aws:iam::000000000000:role/r", "ENABLED", nil, nil)
			require.NoError(t, err)

			// Tag via TagResource.
			require.NoError(t, b1.TagResource(p.PolicyArn, map[string]string{"env": "prod", "team": "infra"}))

			// Verify tags appear via GetLifecyclePolicy (reads p.Tags).
			got, err := b1.GetLifecyclePolicy(p.PolicyID)
			require.NoError(t, err)
			assert.Equal(t, "prod", got.Tags["env"])
			assert.Equal(t, "infra", got.Tags["team"])

			// Snapshot + restore.
			snap := b1.Snapshot(context.Background())
			b2 := dlm.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, b2.Restore(context.Background(), snap))

			// Tags must survive restore.
			got2, err := b2.GetLifecyclePolicy(p.PolicyID)
			require.NoError(t, err)
			assert.Equal(t, "prod", got2.Tags["env"])

			// TagResource on restored backend must also affect GetLifecyclePolicy.
			require.NoError(t, b2.TagResource(p.PolicyArn, map[string]string{"new": "tag"}))

			got3, err := b2.GetLifecyclePolicy(p.PolicyID)
			require.NoError(t, err)
			assert.Equal(t, "tag", got3.Tags["new"])
			assert.Equal(t, "prod", got3.Tags["env"])
		})
	}
}

func TestParityC_Tags_UntagAndList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantTags map[string]string
		tagKeys  []string
	}{
		{
			name:     "untag_one_key",
			tagKeys:  []string{"env"},
			wantTags: map[string]string{"team": "infra"},
		},
		{
			name:     "untag_all_keys",
			tagKeys:  []string{"env", "team"},
			wantTags: map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := dlm.NewInMemoryBackend("000000000000", "us-east-1")

			p, err := b.CreateLifecyclePolicy("t", "arn:aws:iam::000000000000:role/r", "ENABLED",
				map[string]string{"env": "prod", "team": "infra"}, nil)
			require.NoError(t, err)

			require.NoError(t, b.UntagResource(p.PolicyArn, tt.tagKeys))

			tags, err := b.ListTagsForResource(p.PolicyArn)
			require.NoError(t, err)
			assert.Equal(t, tt.wantTags, tags)
		})
	}
}

func TestParityC_UpdateLifecyclePolicy_NilDetailsPreserved(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "nil_details_does_not_clear"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := dlm.NewInMemoryBackend("000000000000", "us-east-1")

			details := map[string]any{"PolicyType": "EBS_SNAPSHOT_MANAGEMENT"}
			p, err := b.CreateLifecyclePolicy("t", "arn:aws:iam::000000000000:role/r", "ENABLED", nil, details)
			require.NoError(t, err)

			// Update with nil PolicyDetails — must not clear existing details.
			require.NoError(t, b.UpdateLifecyclePolicy(p.PolicyID, "new desc", "", "", nil))

			got, err := b.GetLifecyclePolicy(p.PolicyID)
			require.NoError(t, err)
			assert.Equal(t, "new desc", got.Description)
			assert.Equal(t, "EBS_SNAPSHOT_MANAGEMENT", got.PolicyDetails["PolicyType"])
		})
	}
}
