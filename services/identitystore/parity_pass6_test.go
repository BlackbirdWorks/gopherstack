package identitystore_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestParity_ListUsers_MaxResultsBound verifies ListUsers rejects a MaxResults
// outside the AWS 1-100 range with a ValidationException, while an unset or
// in-range value is accepted.
func TestParity_ListUsers_MaxResultsBound(t *testing.T) {
	t.Parallel()

	const storeID = "d-1234567890"

	tests := []struct {
		name       string
		maxResults any
		wantStatus int
	}{
		{name: "unset_ok", maxResults: nil, wantStatus: http.StatusOK},
		{name: "in_range_ok", maxResults: 50, wantStatus: http.StatusOK},
		{name: "at_upper_bound_ok", maxResults: 100, wantStatus: http.StatusOK},
		{name: "over_bound_rejected", maxResults: 101, wantStatus: http.StatusBadRequest},
		{name: "negative_rejected", maxResults: -1, wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			body := map[string]any{"IdentityStoreId": storeID}
			if tt.maxResults != nil {
				body["MaxResults"] = tt.maxResults
			}

			rec := doRequest(t, h, "ListUsers", body)
			assert.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body.String())
		})
	}
}
