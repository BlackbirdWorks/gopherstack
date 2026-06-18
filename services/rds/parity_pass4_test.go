package rds_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestCreateDBInstance_AllocatedStorageBound verifies that CreateDBInstance
// rejects an out-of-range AllocatedStorage (AWS bound: 20–65536 GiB) and
// accepts in-range values.
func TestCreateDBInstance_AllocatedStorageBound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		id         string
		storage    string
		wantStatus int
	}{
		{name: "below min", id: "as-below-min", storage: "10", wantStatus: http.StatusBadRequest},
		{name: "at min", id: "as-at-min", storage: "20", wantStatus: http.StatusOK},
		{name: "mid range", id: "as-mid-range", storage: "100", wantStatus: http.StatusOK},
		{name: "at max", id: "as-at-max", storage: "65536", wantStatus: http.StatusOK},
		{name: "above max", id: "as-above-max", storage: "65537", wantStatus: http.StatusBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newAccuracyRDSHandler()
			rec := doAccuracyRDS(t, h, url.Values{
				"Action":               {"CreateDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {tc.id},
				"DBInstanceClass":      {"db.t3.micro"},
				"Engine":               {"postgres"},
				"MasterUsername":       {"admin"},
				"AllocatedStorage":     {tc.storage},
			})
			assert.Equal(t, tc.wantStatus, rec.Code, "AllocatedStorage=%s", tc.storage)
		})
	}
}
