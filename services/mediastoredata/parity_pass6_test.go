package mediastoredata_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestParity_ListItems_MaxResultsBound verifies ListItems rejects a MaxResults
// outside the AWS 1-1000 range with a ValidationException.
func TestParity_ListItems_MaxResultsBound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		query      string
		wantStatus int
	}{
		{name: "valid", query: "/?MaxResults=10", wantStatus: http.StatusOK},
		{name: "at_upper_bound", query: "/?MaxResults=1000", wantStatus: http.StatusOK},
		{name: "over_upper_bound", query: "/?MaxResults=1001", wantStatus: http.StatusBadRequest},
		{name: "zero", query: "/?MaxResults=0", wantStatus: http.StatusBadRequest},
		{name: "non_numeric", query: "/?MaxResults=lots", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodGet, tt.query, nil, nil)
			assert.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body.String())
		})
	}
}
