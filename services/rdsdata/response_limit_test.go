package rdsdata_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecuteStatementResponseSizeCap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		size     int
		wantCode int
	}{
		{name: "under_cap", size: 100 << 10, wantCode: http.StatusOK},
		{name: "over_cap", size: 2 << 20, wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			payload := strings.Repeat("x", tt.size)

			rec := doRDSDataRequest(t, h, "/Execute", map[string]any{
				"resourceArn": columnOriginResourceARN,
				"secretArn":   columnOriginSecretARN,
				"sql":         "SELECT '" + payload + "' AS big",
			})

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				require.Contains(t, rec.Body.String(), "BadRequestException")
				assert.Contains(t, rec.Body.String(), "size limit")
			}
		})
	}
}
