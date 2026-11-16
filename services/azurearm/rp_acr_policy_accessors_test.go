package azurearm_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContainerRegistryRetention(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		patch   string
		days    int
		premium bool
		enabled bool
	}{
		{name: "default", premium: true, days: 7, enabled: false},
		{name: "enabled", premium: true, days: 30, enabled: true,
			patch: `{"properties":{"policies":{"retentionPolicy":{"status":"enabled","days":30}}}}`},
		{name: "disabled", premium: true, days: 30, enabled: false,
			patch: `{"properties":{"policies":{"retentionPolicy":{"status":"disabled","days":30}}}}`},
		{name: "basic ignored", premium: false, days: 0, enabled: false,
			patch: `{"properties":{"policies":{"retentionPolicy":{"status":"enabled","days":30}}}}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACRHandler(t, tt.premium)

			if tt.patch != "" {
				st, got := doRequest(t, h, http.MethodPatch, acrBase+acrVersion, []byte(tt.patch))
				require.Equal(t, http.StatusOK, st, "%v", got)
			}

			days, enabled := h.Registry.ContainerRegistryRetention("GopherReg")
			assert.Equal(t, tt.enabled, enabled)

			if tt.premium {
				assert.Equal(t, tt.days, days)
			}
		})
	}

	t.Run("unknown", func(t *testing.T) {
		t.Parallel()

		_, enabled := newACRHandler(t, true).Registry.ContainerRegistryRetention("missing")
		assert.False(t, enabled)
	})
}
