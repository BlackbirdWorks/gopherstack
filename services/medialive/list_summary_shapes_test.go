package medialive_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	medialivesdk "github.com/aws/aws-sdk-go-v2/service/medialive"
	"github.com/aws/aws-sdk-go-v2/service/medialive/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListChannels_SummaryTagsRoundTrip proves ChannelSummary.Tags (over-wide
// List-summary sweep, gopherstack-dv4s) now round-trips through the real
// aws-sdk-go-v2 client -- it was sourced on storedChannel all along but
// never copied onto ChannelSummary or emitted by channelSummaryToWire.
func TestListChannels_SummaryTagsRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestChannelClient(t, h)

	_, err := client.CreateChannel(t.Context(), &medialivesdk.CreateChannelInput{
		Name:         aws.String("rt-tags"),
		ChannelClass: types.ChannelClassStandard,
		Tags:         map[string]string{"team": "video"},
	})
	require.NoError(t, err)

	listed, err := client.ListChannels(t.Context(), &medialivesdk.ListChannelsInput{})
	require.NoError(t, err)
	require.Len(t, listed.Channels, 1)
	assert.Equal(t, map[string]string{"team": "video"}, listed.Channels[0].Tags)
}

// TestListNetworks_RawBodyOmitsTags proves the "tags" key -- present on
// neither DescribeNetworkOutput nor the DescribeNetworkSummary type List
// reuses (over-wide List-summary sweep, gopherstack-dv4s) -- is absent from
// the raw response body. Checked at the raw-body level since a typed client
// silently discards unknown keys and so can't see a leak.
func TestListNetworks_RawBodyOmitsTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, http.MethodPost, "/prod/networks", map[string]any{
		"name": "net-leak",
		"tags": map[string]string{"k": "v"},
	})
	require.Equal(t, http.StatusCreated, createRec.Code)

	listRec := doRequest(t, h, http.MethodGet, "/prod/networks", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp struct {
		Networks []map[string]any `json:"networks"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	require.Len(t, listResp.Networks, 1)
	assert.NotContains(t, listResp.Networks[0], "tags", "Network has no tags field on any CRUD/List shape")
	assert.Equal(t, "net-leak", listResp.Networks[0]["name"])
}
