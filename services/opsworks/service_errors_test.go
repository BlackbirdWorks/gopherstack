package opsworks_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDescribeServiceDiagnostics verifies the always-empty diagnostic
// describe-only operations (DescribeServiceErrors, DescribeRaidArrays).
func TestDescribeServiceDiagnostics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		operation string
		body      map[string]any
		checkKey  string
	}{
		{
			name:      "DescribeServiceErrors returns empty list",
			operation: "DescribeServiceErrors",
			body:      map[string]any{},
			checkKey:  "ServiceErrors",
		},
		{
			name:      "DescribeRaidArrays returns empty list",
			operation: "DescribeRaidArrays",
			body:      map[string]any{},
			checkKey:  "RaidArrays",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doTarget(t, h, tt.operation, tt.body)
			require.Equal(t, http.StatusOK, rec.Code)
			resp := parseJSON(t, rec.Body.Bytes())
			_, ok := resp[tt.checkKey]
			assert.True(t, ok, "response should contain key %q", tt.checkKey)
		})
	}
}
