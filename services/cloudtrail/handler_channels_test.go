package cloudtrail_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudtrail"
)

// TestCloudTrailChannel exercises CreateChannel and DeleteChannel.
func TestCloudTrailChannel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *cloudtrail.Handler)
		name string
	}{
		{
			name: "create_channel_success",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "CreateChannel", map[string]any{
					"Name":   "my-channel",
					"Source": "custom-source",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				assert.NotEmpty(t, resp["ChannelArn"])
				assert.Equal(t, "my-channel", resp["Name"])
				assert.Equal(t, "custom-source", resp["Source"])
			},
		},
		{
			name: "create_channel_missing_name",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "CreateChannel", map[string]any{
					"Source": "custom-source",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "delete_channel_success",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				createRec := doCloudTrailOp(t, h, "CreateChannel", map[string]any{
					"Name":   "del-channel",
					"Source": "src",
				})
				createResp := parseCloudTrailResp(t, createRec)
				channelARN := createResp["ChannelArn"].(string)
				rec := doCloudTrailOp(t, h, "DeleteChannel", map[string]any{
					"Channel": channelARN,
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "delete_channel_not_found",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "DeleteChannel", map[string]any{
					"Channel": "channel-missing",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestCloudTrailHandler()
			tt.ops(t, h)
		})
	}
}

// TestCloudTrailUpdateChannelRename verifies UpdateChannel applies the Name
// parameter (renaming the channel), not just Destinations.
func TestCloudTrailUpdateChannelRename(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	createRec := doCloudTrailOp(t, h, "CreateChannel", map[string]any{
		"Name":   "orig-channel",
		"Source": "Custom",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	channelARN, _ := parseCloudTrailResp(t, createRec)["ChannelArn"].(string)
	require.NotEmpty(t, channelARN)

	rec := doCloudTrailOp(t, h, "UpdateChannel", map[string]any{
		"Channel": channelARN,
		"Name":    "renamed-channel",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseCloudTrailResp(t, rec)
	assert.Equal(t, "renamed-channel", resp["Name"])

	getRec := doCloudTrailOp(t, h, "GetChannel", map[string]any{"Channel": channelARN})
	require.Equal(t, http.StatusOK, getRec.Code)
	getResp := parseCloudTrailResp(t, getRec)
	assert.Equal(t, "renamed-channel", getResp["Name"], "rename must persist and be visible via GetChannel")
}

// TestCloudTrailChannelLifecycle covers CreateChannel, GetChannel, ListChannels,
// UpdateChannel, DeleteChannel.
func TestCloudTrailChannelLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	// CreateEventDataStore (needed for channel source).
	rec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
		"Name": "ch-eds",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseCloudTrailResp(t, rec)
	edsARN, _ := resp["EventDataStoreArn"].(string)

	// CreateChannel.
	rec = doCloudTrailOp(t, h, "CreateChannel", map[string]any{
		"Name":   "test-channel",
		"Source": "Custom",
		"Destinations": []map[string]any{
			{
				"Type":     "EVENT_DATA_STORE",
				"Location": edsARN,
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	channelResp := parseCloudTrailResp(t, rec)
	channelARN, _ := channelResp["ChannelArn"].(string)
	require.NotEmpty(t, channelARN)

	// GetChannel.
	rec = doCloudTrailOp(t, h, "GetChannel", map[string]any{"Channel": channelARN})
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListChannels.
	rec = doCloudTrailOp(t, h, "ListChannels", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	// UpdateChannel.
	rec = doCloudTrailOp(t, h, "UpdateChannel", map[string]any{
		"Channel": channelARN,
		"Name":    "updated-channel",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// DeleteChannel.
	rec = doCloudTrailOp(t, h, "DeleteChannel", map[string]any{"Channel": channelARN})
	assert.Equal(t, http.StatusOK, rec.Code)
}
