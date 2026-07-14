package rolesanywhere_test

// handler_test.go covers wire-shape parity fixes made to handler.go:
//   - CreateTrustAnchor honoring the request's enabled field
//   - DeleteTrustAnchor/DeleteProfile returning the deleted resource
//   - UntagResource reading resourceArn/tagKeys from the JSON body (matching
//     the real aws-sdk-go-v2 wire shape) instead of query parameters
//   - notificationSettings/attributeMappings surfacing on every trust
//     anchor/profile read, not only the dedicated Put/Reset/Delete-mapping ops

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateTrustAnchor_EnabledFalse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        map[string]any
		name        string
		wantEnabled bool
	}{
		{
			name: "enabled false is honored",
			body: map[string]any{
				"name":    "disabled-anchor",
				"source":  map[string]any{"sourceType": "CERTIFICATE_BUNDLE"},
				"enabled": false,
			},
			wantEnabled: false,
		},
		{
			name: "enabled true is honored",
			body: map[string]any{
				"name":    "enabled-anchor",
				"source":  map[string]any{"sourceType": "CERTIFICATE_BUNDLE"},
				"enabled": true,
			},
			wantEnabled: true,
		},
		{
			name: "omitted enabled defaults to true",
			body: map[string]any{
				"name":   "default-anchor",
				"source": map[string]any{"sourceType": "CERTIFICATE_BUNDLE"},
			},
			wantEnabled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doREST(t, h, http.MethodPost, "/trustanchors", tt.body)
			require.Equal(t, http.StatusCreated, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			ta := resp["trustAnchor"].(map[string]any)
			assert.Equal(t, tt.wantEnabled, ta["enabled"])
		})
	}
}

func TestHandler_DeleteTrustAnchor_ReturnsResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	recCreate := doREST(t, h, http.MethodPost, "/trustanchors", map[string]any{
		"name":   "del-return-anchor",
		"source": map[string]any{"sourceType": "CERTIFICATE_BUNDLE"},
	})
	require.Equal(t, http.StatusCreated, recCreate.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(recCreate.Body.Bytes(), &createResp))
	id := createResp["trustAnchor"].(map[string]any)["trustAnchorId"].(string)

	recDelete := doREST(t, h, http.MethodDelete, "/trustanchor/"+id, nil)
	require.Equal(t, http.StatusOK, recDelete.Code)

	var deleteResp map[string]any
	require.NoError(t, json.Unmarshal(recDelete.Body.Bytes(), &deleteResp))
	ta, ok := deleteResp["trustAnchor"].(map[string]any)
	require.True(t, ok,
		"DeleteTrustAnchor response must carry the deleted trustAnchor, got: %s", recDelete.Body.String())
	assert.Equal(t, id, ta["trustAnchorId"])
}

func TestHandler_DeleteProfile_ReturnsResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	recCreate := doREST(t, h, http.MethodPost, "/profiles", map[string]any{
		"name":     "del-return-profile",
		"roleArns": []string{"arn:aws:iam::000000000000:role/Test"},
	})
	require.Equal(t, http.StatusCreated, recCreate.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(recCreate.Body.Bytes(), &createResp))
	id := createResp["profile"].(map[string]any)["profileId"].(string)

	recDelete := doREST(t, h, http.MethodDelete, "/profile/"+id, nil)
	require.Equal(t, http.StatusOK, recDelete.Code)

	var deleteResp map[string]any
	require.NoError(t, json.Unmarshal(recDelete.Body.Bytes(), &deleteResp))
	p, ok := deleteResp["profile"].(map[string]any)
	require.True(t, ok, "DeleteProfile response must carry the deleted profile, got: %s", recDelete.Body.String())
	assert.Equal(t, id, p["profileId"])
}

func TestHandler_UntagResource_InvalidJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/UntagResource", bytes.NewReader([]byte(`{invalid`)))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetTrustAnchor_ShowsNotificationSettings(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	recCreate := doREST(t, h, http.MethodPost, "/trustanchors", map[string]any{
		"name":   "notif-visible-anchor",
		"source": map[string]any{"sourceType": "CERTIFICATE_BUNDLE"},
	})
	require.Equal(t, http.StatusCreated, recCreate.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(recCreate.Body.Bytes(), &createResp))
	id := createResp["trustAnchor"].(map[string]any)["trustAnchorId"].(string)

	recPut := doREST(t, h, http.MethodPatch, "/put-notifications-settings", map[string]any{
		"trustAnchorId": id,
		"notificationSettings": []map[string]any{
			{"event": "CA_CERTIFICATE_EXPIRY", "channel": "ALL", "enabled": true, "threshold": 45},
		},
	})
	require.Equal(t, http.StatusOK, recPut.Code)

	// GetTrustAnchor (not just the PutNotificationSettings response itself)
	// must also surface the settings, since AWS's TrustAnchorDetail carries
	// notificationSettings on every read.
	recGet := doREST(t, h, http.MethodGet, "/trustanchor/"+id, nil)
	require.Equal(t, http.StatusOK, recGet.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(recGet.Body.Bytes(), &getResp))
	ta := getResp["trustAnchor"].(map[string]any)
	settings, ok := ta["notificationSettings"].([]any)
	require.True(t, ok, "GetTrustAnchor response must carry notificationSettings, got: %s", recGet.Body.String())
	require.Len(t, settings, 1)

	// ListTrustAnchors must show it too.
	recList := doREST(t, h, http.MethodGet, "/trustanchors", nil)
	require.Equal(t, http.StatusOK, recList.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listResp))
	items := listResp["trustAnchors"].([]any)
	require.Len(t, items, 1)
	listedTA := items[0].(map[string]any)
	assert.NotEmpty(t, listedTA["notificationSettings"])
}

func TestHandler_GetProfile_ShowsAttributeMappings(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	recCreate := doREST(t, h, http.MethodPost, "/profiles", map[string]any{
		"name":     "mapping-visible-profile",
		"roleArns": []string{"arn:aws:iam::000000000000:role/Test"},
	})
	require.Equal(t, http.StatusCreated, recCreate.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(recCreate.Body.Bytes(), &createResp))
	id := createResp["profile"].(map[string]any)["profileId"].(string)

	recPut := doREST(t, h, http.MethodPut, "/profiles/"+id+"/mappings", map[string]any{
		"certificateField": "x509Subject",
		"mappingRules":     []map[string]any{{"specifier": "CN"}},
	})
	require.Equal(t, http.StatusOK, recPut.Code)

	// GetProfile (not just the PutAttributeMapping response itself) must
	// also surface the mapping, since AWS's ProfileDetail carries
	// attributeMappings on every read.
	recGet := doREST(t, h, http.MethodGet, "/profile/"+id, nil)
	require.Equal(t, http.StatusOK, recGet.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(recGet.Body.Bytes(), &getResp))
	p := getResp["profile"].(map[string]any)
	mappings, ok := p["attributeMappings"].([]any)
	require.True(t, ok, "GetProfile response must carry attributeMappings, got: %s", recGet.Body.String())
	require.Len(t, mappings, 1)

	// ListProfiles must show it too.
	recList := doREST(t, h, http.MethodGet, "/profiles", nil)
	require.Equal(t, http.StatusOK, recList.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listResp))
	items := listResp["profiles"].([]any)
	require.Len(t, items, 1)
	listedProfile := items[0].(map[string]any)
	assert.NotEmpty(t, listedProfile["attributeMappings"])
}
