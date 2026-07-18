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
			err := b.ApplyPendingMaintenanceAction(
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
