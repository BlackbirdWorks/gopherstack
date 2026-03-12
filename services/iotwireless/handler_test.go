package iotwireless_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotwireless"
)

func newTestHandlerHTTP() *iotwireless.Handler {
	bk := iotwireless.NewInMemoryBackend()
	h := iotwireless.NewHandler(bk)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	return h
}

func doIoTWRequest(t *testing.T, h *iotwireless.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()

	var req *http.Request

	if body != "" {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, http.NoBody)
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_CreateGetListDeleteWirelessDevice(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		deviceName string
		devType    string
		wantStatus int
	}{
		{
			name:       "full_lifecycle",
			deviceName: "my-device",
			devType:    "LoRaWAN",
			wantStatus: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			body := `{"Name":"` + tt.deviceName + `","Type":"` + tt.devType + `","DestinationName":"d1"}`

			// Create
			rec := doIoTWRequest(t, h, http.MethodPost, "/wireless-devices", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			id, ok := createResp["Id"].(string)
			require.True(t, ok)
			assert.NotEmpty(t, id)

			// Get
			rec = doIoTWRequest(t, h, http.MethodGet, "/wireless-devices/"+id, "")
			assert.Equal(t, http.StatusOK, rec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
			assert.Equal(t, tt.deviceName, getResp["Name"])

			// List
			rec = doIoTWRequest(t, h, http.MethodGet, "/wireless-devices", "")
			assert.Equal(t, http.StatusOK, rec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			devices, ok := listResp["WirelessDeviceList"].([]any)
			require.True(t, ok)
			assert.Len(t, devices, 1)

			// Delete
			rec = doIoTWRequest(t, h, http.MethodDelete, "/wireless-devices/"+id, "")
			assert.Equal(t, http.StatusNoContent, rec.Code)

			// Get after delete returns 404
			rec = doIoTWRequest(t, h, http.MethodGet, "/wireless-devices/"+id, "")
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestHandler_GetWirelessDevice_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		id   string
	}{
		{name: "no_such_device", id: "does-not-exist"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()
			rec := doIoTWRequest(t, h, http.MethodGet, "/wireless-devices/"+tt.id, "")
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestHandler_CreateGetListDeleteServiceProfile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		profileName string
		wantStatus  int
	}{
		{
			name:        "full_lifecycle",
			profileName: "my-profile",
			wantStatus:  http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			body := `{"Name":"` + tt.profileName + `"}`

			// Create
			rec := doIoTWRequest(t, h, http.MethodPost, "/service-profiles", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			id, ok := createResp["Id"].(string)
			require.True(t, ok)
			assert.NotEmpty(t, id)

			// Get
			rec = doIoTWRequest(t, h, http.MethodGet, "/service-profiles/"+id, "")
			assert.Equal(t, http.StatusOK, rec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
			assert.Equal(t, tt.profileName, getResp["Name"])

			// List
			rec = doIoTWRequest(t, h, http.MethodGet, "/service-profiles", "")
			assert.Equal(t, http.StatusOK, rec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			profiles, ok := listResp["ServiceProfileList"].([]any)
			require.True(t, ok)
			assert.Len(t, profiles, 1)

			// Delete
			rec = doIoTWRequest(t, h, http.MethodDelete, "/service-profiles/"+id, "")
			assert.Equal(t, http.StatusNoContent, rec.Code)

			// Get after delete returns 404
			rec = doIoTWRequest(t, h, http.MethodGet, "/service-profiles/"+id, "")
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestHandler_CreateGetListDeleteDestination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		destName   string
		expression string
		wantStatus int
	}{
		{
			name:       "full_lifecycle",
			destName:   "my-dest",
			expression: "my-iot-rule",
			wantStatus: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			body := `{"Name":"` + tt.destName + `","Expression":"` + tt.expression +
				`","ExpressionType":"RuleName","RoleArn":"arn:aws:iam::000000000000:role/r"}`

			// Create
			rec := doIoTWRequest(t, h, http.MethodPost, "/destinations", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			assert.Equal(t, tt.destName, createResp["Name"])

			// Get
			rec = doIoTWRequest(t, h, http.MethodGet, "/destinations/"+tt.destName, "")
			assert.Equal(t, http.StatusOK, rec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
			assert.Equal(t, tt.destName, getResp["Name"])
			assert.Equal(t, tt.expression, getResp["Expression"])

			// List
			rec = doIoTWRequest(t, h, http.MethodGet, "/destinations", "")
			assert.Equal(t, http.StatusOK, rec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			dests, ok := listResp["DestinationList"].([]any)
			require.True(t, ok)
			assert.Len(t, dests, 1)

			// Delete
			rec = doIoTWRequest(t, h, http.MethodDelete, "/destinations/"+tt.destName, "")
			assert.Equal(t, http.StatusNoContent, rec.Code)

			// Get after delete returns 404
			rec = doIoTWRequest(t, h, http.MethodGet, "/destinations/"+tt.destName, "")
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestHandler_ListTagsTagUntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		addTags    string
		wantKey    string
		wantValue  string
		wantGone   string
		removeTags []string
	}{
		{
			name:      "tag_and_list",
			addTags:   `{"Tags":{"env":"prod","team":"platform"}}`,
			wantKey:   "env",
			wantValue: "prod",
		},
		{
			name:       "tag_then_untag",
			addTags:    `{"Tags":{"env":"staging","team":"infra"}}`,
			removeTags: []string{"team"},
			wantKey:    "env",
			wantValue:  "staging",
			wantGone:   "team",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			// Create a service profile to get a real ARN.
			rec := doIoTWRequest(t, h, http.MethodPost, "/service-profiles", `{"Name":"tag-test-profile"}`)
			require.Equal(t, http.StatusCreated, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			arn := createResp["Arn"].(string)
			require.NotEmpty(t, arn)

			// Encode the ARN for URL path.
			encodedARN := strings.ReplaceAll(arn, ":", "%3A")
			encodedARN = strings.ReplaceAll(encodedARN, "/", "%2F")

			// TagResource
			rec = doIoTWRequest(t, h, http.MethodPost, "/tags/"+encodedARN, tt.addTags)
			assert.Equal(t, http.StatusNoContent, rec.Code)

			// Untag if specified.
			if len(tt.removeTags) > 0 {
				queryStr := ""
				for _, k := range tt.removeTags {
					if queryStr != "" {
						queryStr += "&"
					}

					queryStr += "tagKeys=" + k
				}

				e := echo.New()
				req := httptest.NewRequest(http.MethodDelete, "/tags/"+encodedARN+"?"+queryStr, http.NoBody)
				recDel := httptest.NewRecorder()
				c := e.NewContext(req, recDel)
				require.NoError(t, h.Handler()(c))
				assert.Equal(t, http.StatusNoContent, recDel.Code)
			}

			// ListTags
			rec = doIoTWRequest(t, h, http.MethodGet, "/tags/"+encodedARN, "")
			assert.Equal(t, http.StatusOK, rec.Code)

			var tagsResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tagsResp))
			tags, ok := tagsResp["Tags"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.wantValue, tags[tt.wantKey])

			if tt.wantGone != "" {
				_, present := tags[tt.wantGone]
				assert.False(t, present, "tag %q should be removed", tt.wantGone)
			}
		})
	}
}

func TestHandler_UnknownPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
	}{
		{name: "unknown_base", path: "/unknown-resource"},
		{name: "root", path: "/"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()
			rec := doIoTWRequest(t, h, http.MethodGet, tt.path, "")
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}
