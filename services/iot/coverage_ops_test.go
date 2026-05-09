package iot_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

func newIoTHandler(t *testing.T) *iot.Handler {
	t.Helper()

	return iot.NewHandler(iot.NewInMemoryBackend(), nil)
}

func doIoTRequest(t *testing.T, h *iot.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

func TestIoT_ThingTypes(t *testing.T) {
	t.Parallel()

	h := newIoTHandler(t)

	// CreateThingType
	rec := doIoTRequest(t, h, http.MethodPost, "/thing-types/my-type", map[string]any{
		"thingTypeProperties": map[string]any{
			"thingTypeDescription": "sensor type",
		},
	})
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	// DescribeThingType
	rec = doIoTRequest(t, h, http.MethodGet, "/thing-types/my-type", nil)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	// ListThingTypes
	rec = doIoTRequest(t, h, http.MethodGet, "/thing-types", nil)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	// DeprecateThingType
	rec = doIoTRequest(t, h, http.MethodPost, "/thing-types/my-type/deprecate", map[string]any{
		"undoDeprecate": false,
	})
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	// DeleteThingType
	rec = doIoTRequest(t, h, http.MethodDelete, "/thing-types/my-type", nil)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)
}

func TestIoT_ThingGroups(t *testing.T) {
	t.Parallel()

	h := newIoTHandler(t)

	// CreateThingGroup
	rec := doIoTRequest(t, h, http.MethodPost, "/thing-groups/my-group", map[string]any{
		"thingGroupProperties": map[string]any{
			"thingGroupDescription": "test group",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// ListThingGroups
	rec = doIoTRequest(t, h, http.MethodGet, "/thing-groups", nil)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	// CreateThing and add to group
	doIoTRequest(t, h, http.MethodPost, "/things/my-thing", nil)
	rec = doIoTRequest(t, h, http.MethodPut, "/thing-groups/addThingToThingGroup", map[string]any{
		"thingName":      "my-thing",
		"thingGroupName": "my-group",
	})
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	// ListThingsInThingGroup
	rec = doIoTRequest(t, h, http.MethodGet, "/thing-groups/my-group/things", nil)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	// UpdateThingGroup
	rec = doIoTRequest(t, h, http.MethodPatch, "/thing-groups/my-group", map[string]any{
		"thingGroupProperties": map[string]any{
			"thingGroupDescription": "updated",
		},
	})
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	// RemoveThingFromThingGroup
	rec = doIoTRequest(t, h, http.MethodPut, "/thing-groups/removeThingFromThingGroup", map[string]any{
		"thingName":      "my-thing",
		"thingGroupName": "my-group",
	})
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	// DeleteThingGroup
	rec = doIoTRequest(t, h, http.MethodDelete, "/thing-groups/my-group", nil)
	assert.True(t, rec.Code >= 200 && rec.Code < 300)
}
