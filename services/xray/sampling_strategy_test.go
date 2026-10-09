package xray_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetTraceSummaries_SamplingStrategyName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		strategy   map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "partial scan",
			strategy:   map[string]any{"Name": "PartialScan", "Value": 0.5},
			wantStatus: http.StatusOK,
		},
		{name: "fixed rate", strategy: map[string]any{"Name": "FixedRate", "Value": 0.1}, wantStatus: http.StatusOK},
		{name: "unknown", strategy: map[string]any{"Name": "Random", "Value": 1}, wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doXrayRequest(t, newTestHandler(t), "/TraceSummaries", map[string]any{
				"StartTime": 1, "EndTime": 2, "SamplingStrategy": tt.strategy,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
