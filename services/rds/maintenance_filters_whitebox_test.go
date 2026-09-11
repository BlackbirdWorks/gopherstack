package rds

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMatchesAllPendingMaintenanceActionFilters exercises
// applyPendingMaintenanceActionFilters' narrowing logic directly against
// literal PendingMaintenanceAction values, since DescribePendingMaintenanceActions
// (maintenance.go) has no code path that ever populates real data for an
// end-to-end test through the real client (see
// TestDescribePendingMaintenanceActions_UnknownFilterErrors in
// describe_filters_batch1_test.go for the part of this contract that is
// observable that way).
func TestMatchesAllPendingMaintenanceActionFilters(t *testing.T) {
	t.Parallel()

	instARN := "arn:aws:rds:us-east-1:123456789012:db:flt-inst-a"
	cluARN := "arn:aws:rds:us-east-1:123456789012:cluster:flt-clu-b"

	actions := []PendingMaintenanceAction{
		{ResourceIdentifier: instARN, Action: "system-update"},
		{ResourceIdentifier: cluARN, Action: "engine-upgrade"},
	}

	tests := []struct {
		name       string
		query      string
		wantResIDs []string
	}{
		{
			name:       "db-instance-id narrows to matching resource",
			query:      "Filters.Filter.1.Name=db-instance-id&Filters.Filter.1.Values.Value.1=flt-inst-a",
			wantResIDs: []string{instARN},
		},
		{
			name:       "db-cluster-id narrows to matching resource",
			query:      "Filters.Filter.1.Name=db-cluster-id&Filters.Filter.1.Values.Value.1=flt-clu-b",
			wantResIDs: []string{cluARN},
		},
		{
			name:       "no filters returns everything",
			query:      "",
			wantResIDs: []string{instARN, cluARN},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			vals, err := url.ParseQuery(tc.query)
			require.NoError(t, err)

			got, err := applyPendingMaintenanceActionFilters(vals, actions)
			require.NoError(t, err)

			gotIDs := make([]string, 0, len(got))
			for _, a := range got {
				gotIDs = append(gotIDs, a.ResourceIdentifier)
			}
			assert.ElementsMatch(t, tc.wantResIDs, gotIDs)
		})
	}

	t.Run("unknown filter name errors", func(t *testing.T) {
		t.Parallel()

		vals, err := url.ParseQuery("Filters.Filter.1.Name=bogus&Filters.Filter.1.Values.Value.1=x")
		require.NoError(t, err)

		_, err = applyPendingMaintenanceActionFilters(vals, actions)
		require.ErrorIs(t, err, ErrInvalidParameter)
	})
}
