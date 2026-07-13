package dlm_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dlm"
)

// ---------------------------------------------------------------------------
// CreateLifecyclePolicy: required-field validation
// ---------------------------------------------------------------------------

// TestBackend_CreateLifecyclePolicy_RequiredFields verifies that
// Description and ExecutionRoleArn -- both "This member is required" on the
// real CreateLifecyclePolicyInput shape -- are rejected with
// InvalidRequestException when missing, instead of silently creating a
// policy no real AWS account could ever produce.
func TestBackend_CreateLifecyclePolicy_RequiredFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		description string
		roleARN     string
		wantErr     bool
	}{
		{
			name:        "missing description is rejected",
			description: "",
			roleARN:     "arn:aws:iam::000000000000:role/r",
			wantErr:     true,
		},
		{
			name:        "missing executionRoleArn is rejected",
			description: "desc",
			roleARN:     "",
			wantErr:     true,
		},
		{
			name:        "both present succeeds",
			description: "desc",
			roleARN:     "arn:aws:iam::000000000000:role/r",
			wantErr:     false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := dlm.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.CreateLifecyclePolicy(tc.description, tc.roleARN, "ENABLED", nil, nil)

			if tc.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, dlm.ErrInvalidRequest)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// GetLifecyclePolicies: PolicyType summary field
// ---------------------------------------------------------------------------

// TestBackend_GetLifecyclePolicies_PolicyType verifies the LifecyclePolicySummary
// wire shape's PolicyType field (present on every element of the real
// GetLifecyclePolicies response, previously missing from this backend's
// PolicySummary), including the EBS_SNAPSHOT_MANAGEMENT default AWS applies
// when PolicyDetails omits PolicyType.
func TestBackend_GetLifecyclePolicies_PolicyType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		policyDetails map[string]any
		name          string
		wantType      string
	}{
		{
			name:          "explicit PolicyType is echoed",
			policyDetails: map[string]any{"PolicyType": "IMAGE_MANAGEMENT"},
			wantType:      "IMAGE_MANAGEMENT",
		},
		{
			name:          "nil PolicyDetails defaults to EBS_SNAPSHOT_MANAGEMENT",
			policyDetails: nil,
			wantType:      "EBS_SNAPSHOT_MANAGEMENT",
		},
		{
			name:          "PolicyDetails without PolicyType defaults to EBS_SNAPSHOT_MANAGEMENT",
			policyDetails: map[string]any{"ResourceTypes": []any{"VOLUME"}},
			wantType:      "EBS_SNAPSHOT_MANAGEMENT",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := dlm.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.CreateLifecyclePolicy(
				"d", "arn:aws:iam::000000000000:role/r", "ENABLED", nil, tc.policyDetails,
			)
			require.NoError(t, err)

			summaries, err := b.GetLifecyclePolicies(dlm.PolicyFilter{})
			require.NoError(t, err)
			require.Len(t, summaries, 1)
			assert.Equal(t, tc.wantType, summaries[0].PolicyType)
		})
	}
}

// ---------------------------------------------------------------------------
// GetLifecyclePolicies: ResourceTypes / TargetTags / TagsToAdd filters
// ---------------------------------------------------------------------------

// TestBackend_GetLifecyclePolicies_ResourceTypesFilter verifies the
// resourceTypes query filter, previously accepted on the wire but silently
// ignored by the backend (a disguised no-op).
func TestBackend_GetLifecyclePolicies_ResourceTypesFilter(t *testing.T) {
	t.Parallel()

	b := dlm.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateLifecyclePolicy(
		"volume-policy", "arn:aws:iam::000000000000:role/r", "ENABLED", nil,
		map[string]any{"ResourceTypes": []any{"VOLUME"}},
	)
	require.NoError(t, err)

	_, err = b.CreateLifecyclePolicy(
		"instance-policy", "arn:aws:iam::000000000000:role/r", "ENABLED", nil,
		map[string]any{"ResourceTypes": []any{"INSTANCE"}},
	)
	require.NoError(t, err)

	tests := []struct {
		name          string
		wantDesc      string
		resourceTypes []string
		wantCount     int
	}{
		{name: "no filter returns all", resourceTypes: nil, wantCount: 2},
		{name: "VOLUME filter returns one", resourceTypes: []string{"VOLUME"}, wantCount: 1, wantDesc: "volume-policy"},
		{
			name:          "case-insensitive match",
			resourceTypes: []string{"volume"},
			wantCount:     1,
			wantDesc:      "volume-policy",
		},
		{
			name:          "unmatched resource type returns none",
			resourceTypes: []string{"BOGUS"},
			wantCount:     0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result, listErr := b.GetLifecyclePolicies(dlm.PolicyFilter{ResourceTypes: tc.resourceTypes})
			require.NoError(t, listErr)
			require.Len(t, result, tc.wantCount)

			if tc.wantDesc != "" {
				assert.Equal(t, tc.wantDesc, result[0].Description)
			}
		})
	}
}

// TestBackend_GetLifecyclePolicies_TargetTagsFilter verifies the targetTags
// query filter against PolicyDetails.TargetTags ({Key,Value} pairs).
func TestBackend_GetLifecyclePolicies_TargetTagsFilter(t *testing.T) {
	t.Parallel()

	b := dlm.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateLifecyclePolicy(
		"prod-policy", "arn:aws:iam::000000000000:role/r", "ENABLED", nil,
		map[string]any{
			"TargetTags": []any{map[string]any{"Key": "env", "Value": "prod"}},
		},
	)
	require.NoError(t, err)

	_, err = b.CreateLifecyclePolicy(
		"dev-policy", "arn:aws:iam::000000000000:role/r", "ENABLED", nil,
		map[string]any{
			"TargetTags": []any{map[string]any{"Key": "env", "Value": "dev"}},
		},
	)
	require.NoError(t, err)

	tests := []struct {
		name       string
		wantDesc   string
		targetTags []string
		wantCount  int
	}{
		{name: "no filter returns all", targetTags: nil, wantCount: 2},
		{
			name:       "matching key=value returns one",
			targetTags: []string{"env=prod"},
			wantCount:  1,
			wantDesc:   "prod-policy",
		},
		{name: "non-matching value returns none", targetTags: []string{"env=staging"}, wantCount: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result, listErr := b.GetLifecyclePolicies(dlm.PolicyFilter{TargetTags: tc.targetTags})
			require.NoError(t, listErr)
			require.Len(t, result, tc.wantCount)

			if tc.wantDesc != "" {
				assert.Equal(t, tc.wantDesc, result[0].Description)
			}
		})
	}
}

// TestBackend_GetLifecyclePolicies_TagsToAddFilter verifies the tagsToAdd
// query filter, which matches against the TagsToAdd of any schedule nested
// under PolicyDetails.Schedules.
func TestBackend_GetLifecyclePolicies_TagsToAddFilter(t *testing.T) {
	t.Parallel()

	b := dlm.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateLifecyclePolicy(
		"tagged-policy", "arn:aws:iam::000000000000:role/r", "ENABLED", nil,
		map[string]any{
			"Schedules": []any{
				map[string]any{
					"Name":      "daily",
					"TagsToAdd": []any{map[string]any{"Key": "team", "Value": "infra"}},
				},
			},
		},
	)
	require.NoError(t, err)

	_, err = b.CreateLifecyclePolicy(
		"untagged-policy", "arn:aws:iam::000000000000:role/r", "ENABLED", nil,
		map[string]any{
			"Schedules": []any{map[string]any{"Name": "daily"}},
		},
	)
	require.NoError(t, err)

	result, err := b.GetLifecyclePolicies(dlm.PolicyFilter{TagsToAdd: []string{"team=infra"}})
	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Equal(t, "tagged-policy", result[0].Description)

	result, err = b.GetLifecyclePolicies(dlm.PolicyFilter{TagsToAdd: []string{"team=nomatch"}})
	require.NoError(t, err)
	assert.Empty(t, result)
}

// TestBackend_GetLifecyclePolicies_CombinedFilters verifies that the
// filter dimensions (PolicyIDs, State, ResourceTypes, TargetTags, TagsToAdd)
// are ANDed together, not just applied independently.
func TestBackend_GetLifecyclePolicies_CombinedFilters(t *testing.T) {
	t.Parallel()

	b := dlm.NewInMemoryBackend("000000000000", "us-east-1")

	match, err := b.CreateLifecyclePolicy(
		"match", "arn:aws:iam::000000000000:role/r", "ENABLED", nil,
		map[string]any{
			"ResourceTypes": []any{"VOLUME"},
			"TargetTags":    []any{map[string]any{"Key": "env", "Value": "prod"}},
		},
	)
	require.NoError(t, err)

	_, err = b.CreateLifecyclePolicy(
		"wrong-state", "arn:aws:iam::000000000000:role/r", "DISABLED", nil,
		map[string]any{
			"ResourceTypes": []any{"VOLUME"},
			"TargetTags":    []any{map[string]any{"Key": "env", "Value": "prod"}},
		},
	)
	require.NoError(t, err)

	result, err := b.GetLifecyclePolicies(dlm.PolicyFilter{
		State:         "ENABLED",
		ResourceTypes: []string{"VOLUME"},
		TargetTags:    []string{"env=prod"},
	})
	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Equal(t, match.PolicyID, result[0].PolicyID)
}
