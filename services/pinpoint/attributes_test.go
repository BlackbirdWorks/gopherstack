package pinpoint_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAudit6_RemoveAttributes_RemovesBlacklistedNamesOnly verifies
// RemoveAttributes parses the UpdateAttributesRequest.Blacklist from the
// request body and only deletes the named custom attributes -- not the
// whole endpoint-custom-attributes bucket, and not unrelated attributes.
func TestRemoveAttributes_RemovesBlacklistedNamesOnly(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "remove-attrs-app")

	rec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/endpoints/ep-1",
		map[string]any{
			"ChannelType": "EMAIL",
			"Address":     "user@example.com",
			"Attributes": map[string]any{
				"Interests": []string{"Music"},
				"Plan":      []string{"Gold"},
			},
		})
	require.Equal(t, http.StatusAccepted, rec.Code)

	rec = doPinpointRequest(t, h, http.MethodPut,
		"/v1/apps/"+appID+"/attributes/endpoint-custom-attributes",
		map[string]any{"Blacklist": []string{"Interests"}})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "endpoint-custom-attributes", resp["AttributeType"])

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/endpoints/ep-1", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var ep map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &ep))

	attrs, _ := ep["Attributes"].(map[string]any)
	_, hasInterests := attrs["Interests"]
	assert.False(t, hasInterests, "blacklisted attribute must be removed")

	_, hasPlan := attrs["Plan"]
	assert.True(t, hasPlan, "non-blacklisted attribute must survive")
}

// ──────────────────────────────────────────────────
// Parity Phase 4: VoiceTemplate tagging via ARN
// ──────────────────────────────────────────────────
