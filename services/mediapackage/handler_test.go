package mediapackage_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_RotateChannelCredentials_RealPath verifies RotateChannelCredentials
// is routed at PUT /channels/{id}/credentials, matching the real
// aws-sdk-go-v2/service/mediapackage wire shape. The route was previously
// wired to POST /channels/{id}/ingest_endpoints/credentials, a path the real
// SDK never sends, so a genuine client call would have 404'd as "unknown
// operation" -- exactly the disguised-route-matcher-bug class unit tests
// (calling h.Handler() with a made-up path) can hide.
func TestHandler_RotateChannelCredentials_RealPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	channelID := createTestChannel(t, h)

	rec := doRequest(t, h, http.MethodPut, "/channels/"+channelID+"/credentials", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, channelID, resp["id"])

	// The old (fictional) path must no longer be recognized as this operation.
	rec = doRequest(t, h, http.MethodPost, "/channels/"+channelID+"/ingest_endpoints/credentials", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestHandler_ConfigureLogs_ReflectsInDescribe verifies that log groups set via
// ConfigureLogs appear in a subsequent DescribeChannel response. Previously
// ConfigureLogs accepted egressAccessLogs/ingressAccessLogs, discarded them,
// and channelOutput never even had fields to carry them.
func TestHandler_ConfigureLogs_ReflectsInDescribe(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	channelID := createTestChannel(t, h)

	rec := doRequest(t, h, http.MethodPut, "/channels/"+channelID+"/configure_logs", map[string]any{
		"egressAccessLogs":  map[string]any{"logGroupName": "/aws/MediaPackage/Egress"},
		"ingressAccessLogs": map[string]any{"logGroupName": "/aws/MediaPackage/Ingress"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/channels/"+channelID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	egress, ok := resp["egressAccessLogs"].(map[string]any)
	require.True(t, ok, "egressAccessLogs should be present after ConfigureLogs")
	assert.Equal(t, "/aws/MediaPackage/Egress", egress["logGroupName"])

	ingress, ok := resp["ingressAccessLogs"].(map[string]any)
	require.True(t, ok, "ingressAccessLogs should be present after ConfigureLogs")
	assert.Equal(t, "/aws/MediaPackage/Ingress", ingress["logGroupName"])
}

// TestHandler_Channel_CreatedAtPresent and
// TestHandler_OriginEndpoint_CreatedAtPresent verify the createdAt field --
// present on every real MediaPackage Channel/OriginEndpoint response -- is
// now populated rather than absent from the JSON entirely.
func TestHandler_Channel_CreatedAtPresent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	channelID := createTestChannel(t, h)

	rec := doRequest(t, h, http.MethodGet, "/channels/"+channelID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["createdAt"])
}

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
