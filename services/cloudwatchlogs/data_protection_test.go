package cloudwatchlogs_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

func TestCloudWatchLogsBackend_PutAndDescribeAccountPolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr    error
		setup      func(b *cloudwatchlogs.InMemoryBackend)
		name       string
		policyName string
		policyType string
		policyDoc  string
		wantLen    int
		callPut    bool
	}{
		{
			name:       "create_and_describe_all",
			policyName: "my-policy",
			policyType: "DATA_PROTECTION_POLICY",
			policyDoc:  `{"version":"2021-06-01"}`,
			callPut:    true,
			wantLen:    1,
		},
		{
			name: "describe_filtered_by_type",
			setup: func(b *cloudwatchlogs.InMemoryBackend) {
				_, _ = b.PutAccountPolicy("p1", "DATA_PROTECTION_POLICY", "{}", "", "")
				_, _ = b.PutAccountPolicy("p2", "SUBSCRIPTION_FILTER_POLICY", "{}", "", "")
			},
			policyType: "DATA_PROTECTION_POLICY",
			wantLen:    1,
		},
		{
			name:       "invalid_policy_type",
			policyName: "p",
			policyType: "INVALID_TYPE",
			callPut:    true,
			wantErr:    cloudwatchlogs.ErrValidation,
		},
		{
			name:       "missing_name",
			policyName: "",
			policyType: "DATA_PROTECTION_POLICY",
			callPut:    true,
			wantErr:    cloudwatchlogs.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			if tt.callPut {
				policy, err := b.PutAccountPolicy(tt.policyName, tt.policyType, tt.policyDoc, "", "")
				if tt.wantErr != nil {
					require.ErrorIs(t, err, tt.wantErr)

					return
				}
				require.NoError(t, err)
				// Field-diffed against types.AccountPolicy: accountId and
				// lastUpdatedTime must be populated, not left as the zero value
				// a previous revision always returned.
				assert.NotEmpty(t, policy.AccountID)
				assert.NotZero(t, policy.LastUpdatedTime)
			}

			policies, _, err := b.DescribeAccountPolicies(tt.policyType, "", nil, 0, "")
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Len(t, policies, tt.wantLen)

			for _, p := range policies {
				assert.NotEmpty(t, p.AccountID)
				assert.NotZero(t, p.LastUpdatedTime)
			}
		})
	}
}

func TestCloudWatchLogsBackend_PutAccountPolicy_ExtendedTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr           error
		name              string
		policyType        string
		scope             string
		selectionCriteria string
		wantScope         string
	}{
		{
			name:       "data_protection_policy",
			policyType: "DATA_PROTECTION_POLICY",
			wantScope:  "ALL",
		},
		{
			name:       "subscription_filter_policy",
			policyType: "SUBSCRIPTION_FILTER_POLICY",
			wantScope:  "ALL",
		},
		{
			name:       "field_index_policy",
			policyType: "FIELD_INDEX_POLICY",
			wantScope:  "ALL",
		},
		{
			name:       "transformer_policy",
			policyType: "TRANSFORMER_POLICY",
			wantScope:  "ALL",
		},
		{
			name:       "invalid_type",
			policyType: "INVALID_TYPE",
			wantErr:    cloudwatchlogs.ErrValidation,
		},
		{
			name:              "selection_criteria_scope",
			policyType:        "DATA_PROTECTION_POLICY",
			scope:             "SELECTION_CRITERIA",
			selectionCriteria: "logGroupName LIKE '/aws/lambda/%'",
			wantScope:         "SELECTION_CRITERIA",
		},
		{
			name:       "selection_criteria_scope_missing_criteria",
			policyType: "DATA_PROTECTION_POLICY",
			scope:      "SELECTION_CRITERIA",
			wantErr:    cloudwatchlogs.ErrValidation,
		},
		{
			name:       "invalid_scope",
			policyType: "DATA_PROTECTION_POLICY",
			scope:      "INVALID_SCOPE",
			wantErr:    cloudwatchlogs.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			policy, err := b.PutAccountPolicy(
				"p1",
				tt.policyType,
				"{}",
				tt.scope,
				tt.selectionCriteria,
			)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, policy)
			assert.Equal(t, tt.wantScope, policy.Scope)
			if tt.selectionCriteria != "" {
				assert.Equal(t, tt.selectionCriteria, policy.SelectionCriteria)
			}
		})
	}
}

func TestCloudWatchLogsBackend_DescribeAccountPolicies_Pagination(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackend()

	// Create 5 policies.
	for i := range 5 {
		_, err := b.PutAccountPolicy(
			fmt.Sprintf("policy-%02d", i),
			"DATA_PROTECTION_POLICY",
			"{}",
			"", "",
		)
		require.NoError(t, err)
	}

	// Page 1.
	page1, token1, err := b.DescribeAccountPolicies("", "", nil, 2, "")
	require.NoError(t, err)
	assert.Len(t, page1, 2)
	assert.NotEmpty(t, token1)
	assert.Equal(t, "policy-00", page1[0].PolicyName)
	assert.Equal(t, "policy-01", page1[1].PolicyName)

	// Page 2.
	page2, token2, err := b.DescribeAccountPolicies("", "", nil, 2, token1)
	require.NoError(t, err)
	assert.Len(t, page2, 2)
	assert.NotEmpty(t, token2)

	// Page 3 (last).
	page3, token3, err := b.DescribeAccountPolicies("", "", nil, 2, token2)
	require.NoError(t, err)
	assert.Len(t, page3, 1)
	assert.Empty(t, token3)
}

func TestCloudWatchLogsBackend_DescribeAccountPolicies_FilterByType(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackend()
	_, err := b.PutAccountPolicy("p1", "DATA_PROTECTION_POLICY", "{}", "", "")
	require.NoError(t, err)
	_, err = b.PutAccountPolicy("p2", "FIELD_INDEX_POLICY", "{}", "", "")
	require.NoError(t, err)
	_, err = b.PutAccountPolicy("p3", "TRANSFORMER_POLICY", "{}", "", "")
	require.NoError(t, err)

	tests := []struct {
		name       string
		policyType string
		wantLen    int
	}{
		{
			name:       "filter_data_protection",
			policyType: "DATA_PROTECTION_POLICY",
			wantLen:    1,
		},
		{
			name:       "filter_field_index",
			policyType: "FIELD_INDEX_POLICY",
			wantLen:    1,
		},
		{
			name:       "filter_transformer",
			policyType: "TRANSFORMER_POLICY",
			wantLen:    1,
		},
		{
			name:    "no_filter_all",
			wantLen: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			policies, _, descErr := b.DescribeAccountPolicies(tt.policyType, "", nil, 0, "")
			require.NoError(t, descErr)
			assert.Len(t, policies, tt.wantLen)
		})
	}
}
