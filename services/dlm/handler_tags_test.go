package dlm_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dlm"
)

// ---------------------------------------------------------------------------
// handleTagResource: invalid body
// ---------------------------------------------------------------------------

func TestHandler_TagResource_InvalidBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     []byte
		wantCode int
	}{
		{
			name:     "malformed JSON on POST tags returns 400",
			body:     []byte(`bad json`),
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			policyID := createPolicy(t, h)

			// Get the ARN.
			rec := doRequest(t, h, http.MethodGet, fmt.Sprintf("/policies/%s", policyID), nil)
			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			arn := resp["Policy"].(map[string]any)["PolicyArn"].(string)

			req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/tags/%s", arn), bytes.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			rec2 := httptest.NewRecorder()
			e := echo.New()
			c := e.NewContext(req, rec2)
			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tc.wantCode, rec2.Code)
		})
	}
}

// ---------------------------------------------------------------------------
// HandleREST via HTTP: UntagResource with multiple tagKeys query params
// ---------------------------------------------------------------------------

func TestHandler_UntagResource_MultipleQueryKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tagKeys  []string
		wantCode int
	}{
		{
			name:     "untag multiple keys via repeated query params",
			tagKeys:  []string{"env", "team"},
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			policyID := createPolicy(t, h)

			rec := doRequest(t, h, http.MethodGet, fmt.Sprintf("/policies/%s", policyID), nil)
			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			arn := resp["Policy"].(map[string]any)["PolicyArn"].(string)

			// Tag first.
			doRequest(t, h, http.MethodPost, fmt.Sprintf("/tags/%s", arn), map[string]any{
				"Tags": map[string]string{"env": "prod", "team": "ops"},
			})

			// Build query string with multiple tagKeys.
			var parts []string
			for _, k := range tc.tagKeys {
				parts = append(parts, "tagKeys="+k)
			}
			path := fmt.Sprintf("/tags/%s?%s", arn, strings.Join(parts, "&"))

			req := httptest.NewRequest(http.MethodDelete, path, nil)
			rec2 := httptest.NewRecorder()
			e := echo.New()
			c := e.NewContext(req, rec2)
			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tc.wantCode, rec2.Code)
		})
	}
}

// ---------------------------------------------------------------------------
// RouteMatcher + Handler: tag ops on a non-standard-partition ARN
// ---------------------------------------------------------------------------

// TestHandler_TagResource_GovCloudPartitionMatcherRouted proves a full,
// matcher-routed round trip (mirroring how a real dispatcher decides whether
// DLM should even handle the request) for a backend running in a GovCloud
// region. pkgs/arn.Build derives the ARN partition from the region, so this
// backend's own CreateLifecyclePolicy produces a PolicyArn with partition
// "aws-us-gov", not "aws". Before the RouteMatcher fix, its hardcoded
// "arn:aws:dlm:" prefix check would reject that very ARN on the /tags/{arn}
// path, so a GovCloud-region deployment could never tag/untag/list-tags its
// own policies even though CreateLifecyclePolicy/GetLifecyclePolicy worked
// fine.
func TestHandler_TagResource_GovCloudPartitionMatcherRouted(t *testing.T) {
	t.Parallel()

	backend := dlm.NewInMemoryBackend("000000000000", "us-gov-west-1")
	h := dlm.NewHandler(backend)

	policyID := createPolicy(t, h)

	getRec := doRequest(t, h, http.MethodGet, fmt.Sprintf("/policies/%s", policyID), nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	policyARN := getResp["Policy"].(map[string]any)["PolicyArn"].(string)
	require.Contains(t, policyARN, "arn:aws-us-gov:dlm:", "sanity: PolicyArn must carry the GovCloud partition")

	tagPath := fmt.Sprintf("/tags/%s", policyARN)
	body, err := json.Marshal(map[string]any{"Tags": map[string]string{"env": "prod"}})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, tagPath, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)

	require.True(t, h.RouteMatcher()(c), "RouteMatcher must accept a GovCloud-partition dlm tag ARN")
	require.Equal(t, "TagResource", h.ExtractOperation(c))
	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusOK, rec.Code)

	listRec := doRequest(t, h, http.MethodGet, tagPath, nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	tags, ok := listResp["Tags"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "prod", tags["env"])
}

// ---------------------------------------------------------------------------
// HandleREST via HTTP: UntagResource tagKeys percent-encoding
// ---------------------------------------------------------------------------

// TestHandler_UntagResource_URLEncodedTagKey verifies tagKeys query values
// are decoded the same way the real SDK's httpbinding encoder produces them
// (url.Values.Encode() -- standard percent/plus escaping), not read as a
// literal query-string substring. A tag key containing characters that must
// be percent-escaped on the wire (space, '=', '&', '%') would otherwise
// never match the stored (decoded) key and UntagResource would silently
// no-op.
func TestHandler_UntagResource_URLEncodedTagKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		tagKey string
	}{
		{name: "tag key containing a space", tagKey: "team name"},
		{name: "tag key containing an equals sign", tagKey: "a=b"},
		{name: "tag key containing an ampersand", tagKey: "a&b"},
		{name: "tag key containing a percent sign", tagKey: "50%off"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			policyID := createPolicy(t, h)

			rec := doRequest(t, h, http.MethodGet, fmt.Sprintf("/policies/%s", policyID), nil)
			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			arn := resp["Policy"].(map[string]any)["PolicyArn"].(string)

			doRequest(t, h, http.MethodPost, fmt.Sprintf("/tags/%s", arn), map[string]any{
				"Tags": map[string]string{tc.tagKey: "value", "keep": "me"},
			})

			// Build the query string the same way the real SDK's
			// httpbinding encoder does (encoder.AddQuery("tagKeys") then
			// url.Values.Encode()), not by naively concatenating the raw
			// key into the query string.
			q := url.Values{"tagKeys": {tc.tagKey}}
			path := fmt.Sprintf("/tags/%s?%s", arn, q.Encode())

			req := httptest.NewRequest(http.MethodDelete, path, nil)
			rec2 := httptest.NewRecorder()
			e := echo.New()
			c := e.NewContext(req, rec2)
			require.NoError(t, h.Handler()(c))
			require.Equal(t, http.StatusOK, rec2.Code)

			listRec := doRequest(t, h, http.MethodGet, fmt.Sprintf("/tags/%s", arn), nil)
			require.Equal(t, http.StatusOK, listRec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
			tags, ok := listResp["Tags"].(map[string]any)
			require.True(t, ok)
			assert.NotContains(t, tags, tc.tagKey, "percent-decoded tag key must be removed")
			assert.Contains(t, tags, "keep")
		})
	}
}
