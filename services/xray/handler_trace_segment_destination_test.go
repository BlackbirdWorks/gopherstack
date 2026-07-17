package xray_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTraceSegmentDestination_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		destination string
		wantStatus  int
	}{
		{name: "XRay destination", destination: "XRay", wantStatus: http.StatusOK},
		{name: "CloudWatchLogs destination", destination: "CloudWatchLogs", wantStatus: http.StatusOK},
		{name: "empty rejected", destination: "", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.destination != "" {
				updRec := doXrayRequest(t, h, "/UpdateTraceSegmentDestination", map[string]any{
					"Destination": tt.destination,
				})
				assert.Equal(t, tt.wantStatus, updRec.Code)
			} else {
				updRec := doXrayRequest(t, h, "/UpdateTraceSegmentDestination", map[string]any{})
				assert.Equal(t, tt.wantStatus, updRec.Code)
			}
		})
	}
}
