package verifiedpermissions_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestParity_CreatePolicyStore_DescriptionBound verifies a description longer
// than the AWS 150-character bound is rejected with a validation error.
func TestParity_CreatePolicyStore_DescriptionBound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		descLen  int
		wantCode int
	}{
		{name: "at_bound_ok", descLen: 150, wantCode: http.StatusOK},
		{name: "over_bound_rejected", descLen: 151, wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestVPHandler(t)
			rec := doVPRequest(t, h, "CreatePolicyStore", map[string]any{
				"validationSettings": map[string]any{"mode": "OFF"},
				"description":        strings.Repeat("d", tt.descLen),
			})

			assert.Equal(t, tt.wantCode, rec.Code, "body: %s", rec.Body.String())
		})
	}
}
