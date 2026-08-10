package mediapackage_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediapackage"
)

func TestOriginEndpoint_Create(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		check    func(t *testing.T, body []byte)
		name     string
		wantCode int
	}{
		{
			name: "create returns endpoint with ARN and URL",
			body: map[string]any{
				"channelId":    "ch1",
				"id":           "ep1",
				"description":  "HLS endpoint",
				"manifestName": "index",
				"origination":  "ALLOW",
			},
			wantCode: http.StatusCreated,
			check: func(t *testing.T, body []byte) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp["arn"], "arn:aws:mediapackage:us-east-1:000000000000:origin_endpoints/ep1")
				assert.Equal(t, "ep1", resp["id"])
				assert.Equal(t, "ch1", resp["channelId"])
				assert.Equal(t, "ALLOW", resp["origination"])
				assert.NotEmpty(t, resp["url"])
			},
		},
		{
			name:     "create missing ChannelId returns 422",
			body:     map[string]any{"id": "ep1"},
			wantCode: http.StatusUnprocessableEntity,
		},
		{
			name:     "create missing Id returns 422",
			body:     map[string]any{"channelId": "ch1"},
			wantCode: http.StatusUnprocessableEntity,
		},
		{
			name:     "create channel not found returns 404",
			body:     map[string]any{"channelId": "nonexistent", "id": "ep1"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.wantCode == http.StatusCreated {
				// Pre-create the channel
				doRequest(t, h, http.MethodPost, "/channels", map[string]any{"id": "ch1"})
			}
			rec := doRequest(t, h, http.MethodPost, "/origin_endpoints", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestOriginEndpoint_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	channelID := createTestChannel(t, h)
	epID := createTestOriginEndpoint(t, h, channelID)

	assert.Equal(t, 1, mediapackage.OriginEndpointCount(h.Backend.(*mediapackage.InMemoryBackend)))

	// Describe
	rec := doRequest(t, h, http.MethodGet, "/origin_endpoints/"+epID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, epID, descResp["id"])
	assert.Equal(t, channelID, descResp["channelId"])

	// Update
	rec = doRequest(t, h, http.MethodPut, "/origin_endpoints/"+epID, map[string]any{
		"description": "updated endpoint",
		"origination": "DENY",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
	assert.Equal(t, "updated endpoint", updateResp["description"])
	assert.Equal(t, "DENY", updateResp["origination"])

	// List
	rec = doRequest(t, h, http.MethodGet, "/origin_endpoints", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp["originEndpoints"], 1)

	// Delete
	rec = doRequest(t, h, http.MethodDelete, "/origin_endpoints/"+epID, nil)
	assert.Equal(t, http.StatusAccepted, rec.Code)
	assert.Equal(t, 0, mediapackage.OriginEndpointCount(h.Backend.(*mediapackage.InMemoryBackend)))

	// Describe deleted returns 404
	rec = doRequest(t, h, http.MethodGet, "/origin_endpoints/"+epID, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestOriginEndpoint_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{"describe unknown returns 404", http.MethodGet, "/origin_endpoints/notexist"},
		{"update unknown returns 404", http.MethodPut, "/origin_endpoints/notexist"},
		{"delete unknown returns 404", http.MethodDelete, "/origin_endpoints/notexist"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, tc.method, tc.path, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestOriginEndpoint_DefaultOrigination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestChannel(t, h)

	rec := doRequest(t, h, http.MethodPost, "/origin_endpoints", map[string]any{
		"channelId": "test-channel",
		"id":        "ep-defaults",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ALLOW", resp["origination"])
	assert.Equal(t, "ep-defaults", resp["manifestName"])
}

func TestOriginEndpoint_ListEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/origin_endpoints", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["originEndpoints"])
}

// TestOriginEndpoint_DeleteReturns202 verifies delete endpoint returns 202.
func TestOriginEndpoint_DeleteReturns202(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "delete origin endpoint returns 202 Accepted", wantCode: http.StatusAccepted},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			chID, _ := createChannelWithTags(t, h, "ch-ep-del", nil)
			createEndpointWithTags(t, h, chID, "ep-del-202", nil)

			code, _ := doRequestJSON(t, h, http.MethodDelete, "/origin_endpoints/ep-del-202", nil)
			assert.Equal(t, tc.wantCode, code)
		})
	}
}

// TestOriginEndpoint_DefaultFields verifies default field values on create.
func TestOriginEndpoint_DefaultFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		wantOrigination  string
		wantManifestName string
		epID             string
	}{
		{
			name:             "origination defaults to ALLOW",
			wantOrigination:  "ALLOW",
			wantManifestName: "ep-defaults",
			epID:             "ep-defaults",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			chID, _ := createChannelWithTags(t, h, "ch-defaults", nil)

			code, resp := doRequestJSON(t, h, http.MethodPost, "/origin_endpoints", map[string]any{
				"channelId": chID,
				"id":        tc.epID,
			})
			require.Equal(t, http.StatusCreated, code)

			assert.Equal(t, tc.wantOrigination, resp["origination"])
			assert.Equal(t, tc.wantManifestName, resp["manifestName"])
		})
	}
}

// TestHandler_OriginEndpoint_CreatedAtPresent verifies the createdAt field --
// present on every real MediaPackage OriginEndpoint response -- is populated
// rather than absent from the JSON entirely.
func TestHandler_OriginEndpoint_CreatedAtPresent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	channelID := createTestChannel(t, h)
	epID := createTestOriginEndpoint(t, h, channelID)

	rec := doRequest(t, h, http.MethodGet, "/origin_endpoints/"+epID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["createdAt"])
}

// TestHandler_OriginEndpoint_PackagingConfigRoundTrip verifies the
// authorization/hlsPackage/dashPackage/cmafPackage/mssPackage blocks accepted
// by CreateOriginEndpoint are returned by Describe and List -- previously
// these request fields were parsed by no one and vanished entirely.
func TestHandler_OriginEndpoint_PackagingConfigRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	channelID := createTestChannel(t, h)

	rec := doRequest(t, h, http.MethodPost, "/origin_endpoints", map[string]any{
		"channelId": channelID,
		"id":        "ep-pkg",
		"authorization": map[string]any{
			"cdnIdentifierSecret": "arn:aws:secretsmanager:us-east-1:000000000000:secret:s",
			"secretsRoleArn":      "arn:aws:iam::000000000000:role/r",
		},
		"hlsPackage": map[string]any{"segmentDurationSeconds": float64(6)},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	require.Contains(t, created, "authorization")
	require.Contains(t, created, "hlsPackage")
	assert.NotContains(t, created, "dashPackage", "unset packaging blocks should be omitted, not null-filled")

	rec = doRequest(t, h, http.MethodGet, "/origin_endpoints/ep-pkg", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var described map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &described))
	auth, ok := described["authorization"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "arn:aws:iam::000000000000:role/r", auth["secretsRoleArn"])

	hls, ok := described["hlsPackage"].(map[string]any)
	require.True(t, ok)
	assert.InEpsilon(t, float64(6), hls["segmentDurationSeconds"], 0)

	// List should reflect the same configuration.
	rec = doRequest(t, h, http.MethodGet, "/origin_endpoints", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	endpoints, ok := listResp["originEndpoints"].([]any)
	require.True(t, ok)
	require.Len(t, endpoints, 1)

	listed := endpoints[0].(map[string]any)
	assert.Contains(t, listed, "hlsPackage")
}

// TestHandler_OriginEndpoint_Authorization_RequiredFields verifies a
// partially-populated authorization block is rejected with 422 over the
// wire, not silently accepted with a missing secret.
func TestHandler_OriginEndpoint_Authorization_RequiredFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	channelID := createTestChannel(t, h)

	code, resp := doRequestJSON(t, h, http.MethodPost, "/origin_endpoints", map[string]any{
		"channelId": channelID,
		"id":        "ep-bad-auth",
		"authorization": map[string]any{
			"cdnIdentifierSecret": "arn:aws:secretsmanager:1",
		},
	})
	require.Equal(t, http.StatusUnprocessableEntity, code)
	assert.Contains(t, resp["Message"], "SecretsRoleArn")
}

// TestHandler_OriginEndpoint_MssPackage_FullDepthRoundTrip verifies the
// nested SPEKE/StreamSelection fields inside mssPackage survive a Create
// then Describe over the wire.
func TestHandler_OriginEndpoint_MssPackage_FullDepthRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	channelID := createTestChannel(t, h)

	rec := doRequest(t, h, http.MethodPost, "/origin_endpoints", map[string]any{
		"channelId": channelID,
		"id":        "ep-mss",
		"mssPackage": map[string]any{
			"manifestWindowSeconds": float64(60),
			"streamSelection": map[string]any{
				"streamOrder":           "ORIGINAL",
				"maxVideoBitsPerSecond": float64(5000000),
				"minVideoBitsPerSecond": float64(100000),
			},
			"encryption": map[string]any{
				"spekeKeyProvider": map[string]any{
					"resourceId": "r1",
					"roleArn":    "arn:aws:iam:1",
					"url":        "https://speke.example.com",
					"systemIds":  []any{"81376844-f976-481e-a695-0e6108b45a58"},
				},
			},
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/origin_endpoints/ep-mss", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var described map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &described))

	mss, ok := described["mssPackage"].(map[string]any)
	require.True(t, ok)
	assert.InEpsilon(t, float64(60), mss["manifestWindowSeconds"], 0)

	speke := mss["encryption"].(map[string]any)["spekeKeyProvider"].(map[string]any)
	assert.Equal(t, "r1", speke["resourceId"])
	assert.Equal(t, []any{"81376844-f976-481e-a695-0e6108b45a58"}, speke["systemIds"])
}
