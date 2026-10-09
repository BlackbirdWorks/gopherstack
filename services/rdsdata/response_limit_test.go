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
		json     bool
	}{
		{name: "under_cap", size: 100 << 10, wantCode: http.StatusOK},
		{name: "over_cap", size: 2 << 20, wantCode: http.StatusBadRequest},
		{name: "json_over_binary_cap", size: 2 << 20, wantCode: http.StatusOK, json: true},
		{name: "json_over_cap", size: 11 << 20, wantCode: http.StatusBadRequest, json: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			payload := strings.Repeat("x", tt.size)

			body := map[string]any{
				"resourceArn": columnOriginResourceARN,
				"secretArn":   columnOriginSecretARN,
				"sql":         "SELECT '" + payload + "' AS big",
			}
			if tt.json {
				body["formatRecordsAs"] = "JSON"
			}

			rec := doRDSDataRequest(t, h, "/Execute", body)

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				require.Contains(t, rec.Body.String(), "UnsupportedResultException")
				assert.Contains(t, rec.Body.String(), "size limit")
			}
		})
	}
}
