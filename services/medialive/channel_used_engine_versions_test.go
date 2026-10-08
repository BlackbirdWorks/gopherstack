package medialive_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListChannels_UsedChannelEngineVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantVersion string
		start       bool
	}{
		{name: "idle_omitted"},
		{name: "running_default", start: true, wantVersion: "AVCHD-1.0.0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			channelID := createTestChannel(t, h)

			if tt.start {
				rec := doRequest(t, h, http.MethodPost, "/prod/channels/"+channelID+"/start", nil)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, http.MethodGet, "/prod/channels", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			items, _ := decodeBody(t, rec.Body.Bytes())["channels"].([]any)
			require.Len(t, items, 1)
			item, _ := items[0].(map[string]any)

			used, ok := item["usedChannelEngineVersions"].([]any)
			if tt.wantVersion == "" {
				assert.False(t, ok)

				return
			}

			require.Len(t, used, 1)
			first, _ := used[0].(map[string]any)
			assert.Equal(t, tt.wantVersion, first["version"])
		})
	}
}
