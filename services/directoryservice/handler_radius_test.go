package directoryservice_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRadius(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "enable update disable cycle"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mustCreateSimpleAD(t, h, "corp.example.com")

			radiusSettings := map[string]any{
				"RadiusServers":   []any{"10.0.0.1"},
				"RadiusPort":      1812,
				"RadiusRetries":   3,
				"RadiusTimeout":   30,
				"SharedSecret":    "secret",
				"UseSameUsername": false,
			}

			// Enable
			rec1 := doRequest(t, h, "EnableRadius", map[string]any{
				"DirectoryId":    dirID,
				"RadiusSettings": radiusSettings,
			})
			assert.Equal(t, http.StatusOK, rec1.Code)

			// Update
			rec2 := doRequest(t, h, "UpdateRadius", map[string]any{
				"DirectoryId":    dirID,
				"RadiusSettings": radiusSettings,
			})
			assert.Equal(t, http.StatusOK, rec2.Code)

			// Disable
			rec3 := doRequest(t, h, "DisableRadius", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec3.Code)

			_ = tc
		})
	}
}
