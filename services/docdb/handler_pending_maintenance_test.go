package docdb_test

import (
	"context"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/docdb"
)

func TestHandler_ApplyPendingMaintenanceAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "apply_action_success",
			vals: url.Values{
				"Action":             {"ApplyPendingMaintenanceAction"},
				"Version":            {"2014-10-31"},
				"ResourceIdentifier": {"arn:aws:rds:us-east-1:000000000000:cluster:my-cluster"},
				"ApplyAction":        {"system-update"},
				"OptInType":          {"immediate"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "ApplyPendingMaintenanceActionResponse",
		},
		{
			name: "apply_action_missing_resource",
			vals: url.Values{
				"Action":      {"ApplyPendingMaintenanceAction"},
				"Version":     {"2014-10-31"},
				"ApplyAction": {"system-update"},
				"OptInType":   {"immediate"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestOptInTypeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		optInType string
		wantErr   bool
	}{
		{name: "immediate_valid", optInType: "immediate", wantErr: false},
		{name: "next_maintenance_valid", optInType: "next-maintenance", wantErr: false},
		{name: "undo_opt_in_valid", optInType: "undo-opt-in", wantErr: false},
		{name: "invalid_opt_in_type", optInType: "bad-value", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.ApplyPendingMaintenanceAction(
				context.Background(),
				"arn:aws:rds:us-east-1:000000000000:cluster:c1",
				"system-update",
				tt.optInType,
			)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestHandler_DescribePendingMaintenanceActions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "describe_pending_actions",
			vals: url.Values{
				"Action":  {"DescribePendingMaintenanceActions"},
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DescribePendingMaintenanceActionsResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

// TestPendingMaintenanceAction_RealQueue locks in the real
// pending-maintenance-action queue behavior added this pass: previously
// ApplyPendingMaintenanceAction validated its inputs and
// DescribePendingMaintenanceActions always returned an empty list, so
// nothing was ever really "pending" regardless of what a caller asked for
// (see PARITY.md). AddPendingMaintenanceActionInternal seeds the queue the
// way real AWS's own system-side upgrade/patch-availability data would,
// after which Describe must reflect real queued state and Apply must
// genuinely mutate it.
func TestPendingMaintenanceAction_RealQueue(t *testing.T) {
	t.Parallel()

	const resourceARN = "arn:aws:rds:us-east-1:000000000000:cluster:queued-cluster"

	b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddPendingMaintenanceActionInternal(resourceARN, "system-update", "a system update is available")

	// Describe with no filter must surface the seeded action for real.
	all := b.DescribePendingMaintenanceActions(context.Background(), "")
	require.Len(t, all, 1)
	assert.Equal(t, resourceARN, all[0].ResourceIdentifier)
	require.Len(t, all[0].Actions, 1)
	assert.Equal(t, "system-update", all[0].Actions[0].Action)
	assert.Empty(t, all[0].Actions[0].OptInStatus, "seeded action starts with no opt-in status")

	// Describe filtered by a different resource ARN must exclude it.
	filtered := b.DescribePendingMaintenanceActions(
		context.Background(), "arn:aws:rds:us-east-1:000000000000:cluster:other",
	)
	assert.Empty(t, filtered)

	// Apply(immediate) must genuinely mutate the queued action's OptInStatus/CurrentApplyDate.
	result, err := b.ApplyPendingMaintenanceAction(context.Background(), resourceARN, "system-update", "immediate")
	require.NoError(t, err)
	require.Len(t, result.Actions, 1)
	assert.Equal(t, "immediate", result.Actions[0].OptInStatus)
	assert.NotEmpty(t, result.Actions[0].CurrentApplyDate)

	// The mutation must persist: a subsequent Describe reflects it too.
	after := b.DescribePendingMaintenanceActions(context.Background(), resourceARN)
	require.Len(t, after, 1)
	require.Len(t, after[0].Actions, 1)
	assert.Equal(t, "immediate", after[0].Actions[0].OptInStatus)

	// Apply(undo-opt-in) must clear the opt-in status back out.
	undone, err := b.ApplyPendingMaintenanceAction(context.Background(), resourceARN, "system-update", "undo-opt-in")
	require.NoError(t, err)
	require.Len(t, undone.Actions, 1)
	assert.Empty(t, undone.Actions[0].OptInStatus)
	assert.Empty(t, undone.Actions[0].CurrentApplyDate)
}

// TestPendingMaintenanceAction_ApplyUnqueuedIsNoop locks in that applying an
// action never seeded for a resource is a harmless no-op (matching AWS's own
// opt-in semantics for an action that doesn't currently apply), not an
// error and not a fabricated queue entry.
func TestPendingMaintenanceAction_ApplyUnqueuedIsNoop(t *testing.T) {
	t.Parallel()

	b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
	result, err := b.ApplyPendingMaintenanceAction(
		context.Background(), "arn:aws:rds:us-east-1:000000000000:cluster:never-queued", "db-upgrade", "immediate",
	)
	require.NoError(t, err)
	assert.Empty(t, result.Actions)

	all := b.DescribePendingMaintenanceActions(context.Background(), "")
	assert.Empty(t, all, "Apply on a never-queued action must not fabricate a queue entry")
}

// TestHandler_DescribePendingMaintenanceActions_RealData drives the XML
// response end-to-end through the handler, locking in that a seeded action's
// fields (Action/OptInStatus/CurrentApplyDate) actually round-trip onto the
// wire instead of the previous hardcoded-empty PendingMaintenanceActionDetails.
func TestHandler_DescribePendingMaintenanceActions_RealData(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	const resourceARN = "arn:aws:rds:us-east-1:000000000000:cluster:wire-cluster"
	h.Backend.AddPendingMaintenanceActionInternal(resourceARN, "db-upgrade", "a new engine version is available")

	rr := doRequest(t, h, url.Values{
		"Action":             {"ApplyPendingMaintenanceAction"},
		"Version":            {"2014-10-31"},
		"ResourceIdentifier": {resourceARN},
		"ApplyAction":        {"db-upgrade"},
		"OptInType":          {"next-maintenance"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "<Action>db-upgrade</Action>")
	assert.Contains(t, rr.Body.String(), "<OptInStatus>next-maintenance</OptInStatus>")

	describeRR := doRequest(t, h, url.Values{
		"Action":             {"DescribePendingMaintenanceActions"},
		"Version":            {"2014-10-31"},
		"ResourceIdentifier": {resourceARN},
	})
	require.Equal(t, http.StatusOK, describeRR.Code)
	assert.Contains(t, describeRR.Body.String(), resourceARN)
	assert.Contains(t, describeRR.Body.String(), "<Action>db-upgrade</Action>")
	assert.Contains(t, describeRR.Body.String(), "<OptInStatus>next-maintenance</OptInStatus>")
}
