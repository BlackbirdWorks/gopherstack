package iot_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

func TestThingTypes(t *testing.T) {
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

func TestBackend_UpdateThingType(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackend()
	_, err := b.CreateThingType(&iot.CreateThingTypeInput{
		ThingTypeName:        "Sensor",
		Description:          "original",
		SearchableAttributes: []string{"model"},
	})
	require.NoError(t, err)

	err = b.UpdateThingType(&iot.UpdateThingTypeInput{
		ThingTypeName:        "Sensor",
		Description:          "updated",
		SearchableAttributes: []string{"model", "firmware"},
	})
	require.NoError(t, err)

	tt, err := b.DescribeThingType("Sensor")
	require.NoError(t, err)
	assert.Equal(t, "updated", tt.Description)
	assert.ElementsMatch(t, []string{"model", "firmware"}, tt.SearchableAttributes)
}

func TestBackend_UpdateThingType_NotFound(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackend()
	err := b.UpdateThingType(&iot.UpdateThingTypeInput{ThingTypeName: "missing", Description: "x"})
	require.ErrorIs(t, err, iot.ErrThingTypeNotFound)
}

func TestHandler_UpdateThingType(t *testing.T) {
	t.Parallel()
	h := newIoTHandler(t)

	iotOK(t, h, http.MethodPost, "/thing-types/Sensor", map[string]any{
		"thingTypeProperties": map[string]any{
			"thingTypeDescription": "original",
		},
	})

	iotOK(t, h, http.MethodPatch, "/thing-types/Sensor", map[string]any{
		"thingTypeProperties": map[string]any{
			"thingTypeDescription": "updated",
			"searchableAttributes": []string{"a", "b"},
		},
	})

	out := iotOK(t, h, http.MethodGet, "/thing-types/Sensor", nil)
	props, _ := out["thingTypeProperties"].(map[string]any)
	assert.Equal(t, "updated", props["thingTypeDescription"])

	iotExpectError(t, h, "/thing-types/does-not-exist")
}

func TestThingTypeName_Stored(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	_, err := backend.CreateThing(&iot.CreateThingInput{
		ThingName:     "typed-thing",
		ThingTypeName: "SensorType",
	})
	require.NoError(t, err)

	th, err := backend.DescribeThing("typed-thing")
	require.NoError(t, err)
	assert.Equal(t, "SensorType", th.ThingTypeName)
}

func TestUpdateThing_RemoveThingType(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	_, err := backend.CreateThing(&iot.CreateThingInput{
		ThingName:     "typed-remove",
		ThingTypeName: "OldType",
	})
	require.NoError(t, err)

	err = backend.UpdateThing(&iot.UpdateThingInput{
		ThingName:       "typed-remove",
		RemoveThingType: true,
	})
	require.NoError(t, err)

	th, err := backend.DescribeThing("typed-remove")
	require.NoError(t, err)
	assert.Empty(t, th.ThingTypeName)
}

func TestHandler_DescribeThing_IncludesThingTypeName(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	h := iot.NewHandler(backend, nil)
	backend.AddThingInternal(iot.Thing{ThingName: "sensor-1", ThingTypeName: "SensorType"})

	resp := doRequest(t, h, http.MethodGet, "/things/sensor-1", nil)
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Equal(t, "SensorType", out["thingTypeName"])
}

func TestCreateThingType_ReturnsThingTypeID(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	var out map[string]string
	code := r3JSON(t, h, http.MethodPost, "/thing-types/SensorV3", nil, &out)
	require.Equal(t, http.StatusOK, code)
	assert.NotEmpty(t, out["thingTypeId"])
}

func TestThingType_IDIsUUID(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	tt, err := b.CreateThingType(&iot.CreateThingTypeInput{ThingTypeName: "uuid-type"})
	require.NoError(t, err)

	assert.NotEmpty(t, tt.ThingTypeID)
	assert.Len(t, tt.ThingTypeID, 36)
	assert.Contains(t, tt.ThingTypeID, "-", "ThingTypeID should be UUID-formatted")
}

func TestThingType_IDsAreUnique(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	seen := make(map[string]bool)

	for _, name := range []string{"unique-type-a", "unique-type-b", "unique-type-c", "unique-type-d", "unique-type-e"} {
		tt, err := b.CreateThingType(&iot.CreateThingTypeInput{ThingTypeName: name})
		require.NoError(t, err)
		assert.False(t, seen[tt.ThingTypeID], "duplicate ThingTypeID: %s", tt.ThingTypeID)
		seen[tt.ThingTypeID] = true
	}
}

func TestThingType_IDIsConsistentBetweenCreateAndDescribe(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	created, err := b.CreateThingType(&iot.CreateThingTypeInput{ThingTypeName: "consistent-type"})
	require.NoError(t, err)

	described, err := b.DescribeThingType("consistent-type")
	require.NoError(t, err)
	assert.Equal(t, created.ThingTypeID, described.ThingTypeID)
}

func TestDescribeThingType_IncludesThingTypeID(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	var createOut map[string]string
	r3JSON(t, h, http.MethodPost, "/thing-types/describe-id-type", nil, &createOut)
	createID := createOut["thingTypeId"]

	var out map[string]any
	code := r3JSON(t, h, http.MethodGet, "/thing-types/describe-id-type", nil, &out)
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, createID, out["thingTypeId"])
}

func TestListThingTypes_IncludesMetadata(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	r3Req(t, h, http.MethodPost, "/thing-types/meta-type", nil)

	var out struct {
		ThingTypes []map[string]any `json:"thingTypes"`
	}
	code := r3JSON(t, h, http.MethodGet, "/thing-types", nil, &out)
	require.Equal(t, http.StatusOK, code)
	require.Len(t, out.ThingTypes, 1)

	item := out.ThingTypes[0]
	assert.Contains(t, item, "thingTypeMetadata")
	meta, ok := item["thingTypeMetadata"].(map[string]any)
	assert.True(t, ok)
	_, hasDeprecated := meta["deprecated"]
	assert.True(t, hasDeprecated, "thingTypeMetadata should include 'deprecated' field")
	_, hasCreationDate := meta["creationDate"]
	assert.True(t, hasCreationDate, "thingTypeMetadata should include 'creationDate' field")
}

func TestListThingTypes_DeprecatedFlagInMetadata(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	r3Req(t, h, http.MethodPost, "/thing-types/dep-list-type", nil)
	r3Req(t, h, http.MethodPost, "/thing-types/dep-list-type/deprecate", map[string]any{
		"undoDeprecate": false,
	})

	var out struct {
		ThingTypes []map[string]any `json:"thingTypes"`
	}
	r3JSON(t, h, http.MethodGet, "/thing-types", nil, &out)
	require.Len(t, out.ThingTypes, 1)

	meta := out.ThingTypes[0]["thingTypeMetadata"].(map[string]any)
	assert.Equal(t, true, meta["deprecated"])
}

func TestListThingTypes_IsSorted(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	names := []string{"zebra-type", "alpha-type", "middle-type"}
	for _, n := range names {
		_, err := b.CreateThingType(&iot.CreateThingTypeInput{ThingTypeName: n})
		require.NoError(t, err)
	}

	types := b.ListThingTypes()
	require.Len(t, types, 3)
	assert.Equal(t, "alpha-type", types[0].ThingTypeName)
	assert.Equal(t, "middle-type", types[1].ThingTypeName)
	assert.Equal(t, "zebra-type", types[2].ThingTypeName)
}

func TestDeprecateThingType_SetsDeprecated(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateThingType(&iot.CreateThingTypeInput{ThingTypeName: "dep-type"})
	require.NoError(t, err)

	err = b.DeprecateThingType(&iot.DeprecateThingTypeInput{ThingTypeName: "dep-type"})
	require.NoError(t, err)

	tt, err := b.DescribeThingType("dep-type")
	require.NoError(t, err)
	assert.True(t, tt.Deprecated)
	assert.False(t, tt.DeprecationDate.IsZero())
}

func TestDeprecateThingType_UndoDeprecate(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateThingType(&iot.CreateThingTypeInput{ThingTypeName: "undep-type"})
	require.NoError(t, err)

	err = b.DeprecateThingType(&iot.DeprecateThingTypeInput{ThingTypeName: "undep-type"})
	require.NoError(t, err)

	err = b.DeprecateThingType(&iot.DeprecateThingTypeInput{
		ThingTypeName: "undep-type",
		UndoDeprecate: true,
	})
	require.NoError(t, err)

	tt, err := b.DescribeThingType("undep-type")
	require.NoError(t, err)
	assert.False(t, tt.Deprecated)
	assert.True(t, tt.DeprecationDate.IsZero(), "DeprecationDate should be cleared on undoDeprecate")
}

func TestDeprecateThingType_HTTP_UndoDeprecate(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	r3Req(t, h, http.MethodPost, "/thing-types/http-undep-type", nil)
	r3Req(t, h, http.MethodPost, "/thing-types/http-undep-type/deprecate", map[string]any{
		"undoDeprecate": false,
	})

	code, _ := r3Req(t, h, http.MethodPost, "/thing-types/http-undep-type/deprecate", map[string]any{
		"undoDeprecate": true,
	})
	require.Equal(t, http.StatusOK, code)

	var out map[string]any
	r3JSON(t, h, http.MethodGet, "/thing-types/http-undep-type", nil, &out)
	meta := out["thingTypeMetadata"].(map[string]any)
	assert.Equal(t, false, meta["deprecated"])
}

func TestDescribeThingType_DeprecationDate_SetOnDeprecate(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	r3Req(t, h, http.MethodPost, "/thing-types/dated-dep-type", nil)
	r3Req(t, h, http.MethodPost, "/thing-types/dated-dep-type/deprecate", map[string]any{
		"undoDeprecate": false,
	})

	var out map[string]any
	r3JSON(t, h, http.MethodGet, "/thing-types/dated-dep-type", nil, &out)
	meta := out["thingTypeMetadata"].(map[string]any)
	assert.Equal(t, true, meta["deprecated"])
	assert.NotNil(t, meta["deprecationDate"], "deprecationDate should be set after deprecation")
}

func TestDescribeThingType_DeprecationDate_ClearedOnUndoDeprecate(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	r3Req(t, h, http.MethodPost, "/thing-types/undep-dated-type", nil)
	r3Req(t, h, http.MethodPost, "/thing-types/undep-dated-type/deprecate", map[string]any{
		"undoDeprecate": false,
	})
	r3Req(t, h, http.MethodPost, "/thing-types/undep-dated-type/deprecate", map[string]any{
		"undoDeprecate": true,
	})

	var out map[string]any
	r3JSON(t, h, http.MethodGet, "/thing-types/undep-dated-type", nil, &out)
	meta := out["thingTypeMetadata"].(map[string]any)
	assert.Equal(t, false, meta["deprecated"])
	assert.Nil(t, meta["deprecationDate"], "deprecationDate should be nil after undoDeprecate")
}

func TestDeprecateThingType_NotFound_UsesAWSFormat(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	var out map[string]string
	code := r3JSON(t, h, http.MethodPost, "/thing-types/no-such-type/deprecate", map[string]any{
		"undoDeprecate": false,
	}, &out)
	assert.Equal(t, http.StatusNotFound, code)
	assert.Equal(t, "ResourceNotFoundException", out["__type"])
}

func TestDeleteThingType_RequiresDeprecation(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateThingType(&iot.CreateThingTypeInput{ThingTypeName: "nodelete-type"})
	require.NoError(t, err)

	err = b.DeleteThingType("nodelete-type")
	require.ErrorIs(t, err, iot.ErrValidation)
}

func TestDeleteThingType_SucceedsAfterDeprecation(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateThingType(&iot.CreateThingTypeInput{ThingTypeName: "del-dep-type"})
	require.NoError(t, err)

	err = b.DeprecateThingType(&iot.DeprecateThingTypeInput{ThingTypeName: "del-dep-type"})
	require.NoError(t, err)

	err = b.DeleteThingType("del-dep-type")
	require.NoError(t, err)

	_, err = b.DescribeThingType("del-dep-type")
	require.ErrorIs(t, err, iot.ErrThingTypeNotFound)
}

func TestDeleteThingType_HTTP_MissingDeprecation_Returns400(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	r3Req(t, h, http.MethodPost, "/thing-types/no-dep-http-type", nil)

	var out map[string]string
	code := r3JSON(t, h, http.MethodDelete, "/thing-types/no-dep-http-type", nil, &out)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "InvalidRequestException", out["__type"])
}

func TestErrorFormat_ThingTypeNotFound_UsesAWSFormat(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	var out map[string]string
	code := r3JSON(t, h, http.MethodGet, "/thing-types/missing-type", nil, &out)
	assert.Equal(t, http.StatusNotFound, code)
	assert.Equal(t, "ResourceNotFoundException", out["__type"])
}

func TestReset_ClearsThingTypes(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateThingType(&iot.CreateThingTypeInput{ThingTypeName: "reset-type"})
	require.NoError(t, err)

	b.Reset()
	assert.Empty(t, b.ListThingTypes())
}

func TestThingType_SearchableAttributesPreserved(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	searchable := []string{"color", "model", "firmware"}
	_, err := b.CreateThingType(&iot.CreateThingTypeInput{
		ThingTypeName:        "searchable-type",
		SearchableAttributes: searchable,
	})
	require.NoError(t, err)

	tt, err := b.DescribeThingType("searchable-type")
	require.NoError(t, err)
	assert.Equal(t, searchable, tt.SearchableAttributes)
}

func TestCreateThingType_DuplicateName_Conflict(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateThingType(&iot.CreateThingTypeInput{ThingTypeName: "dup-thing-type"})
	require.NoError(t, err)

	_, err = b.CreateThingType(&iot.CreateThingTypeInput{ThingTypeName: "dup-thing-type"})
	require.ErrorIs(t, err, iot.ErrAlreadyExists)
}

func TestCreateThingType_HTTP_DuplicateName_Returns409(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	r3Req(t, h, http.MethodPost, "/thing-types/http-dup-type", nil)

	var out map[string]string
	code := r3JSON(t, h, http.MethodPost, "/thing-types/http-dup-type", nil, &out)
	assert.Equal(t, http.StatusConflict, code)
	assert.Equal(t, "ResourceAlreadyExistsException", out["__type"])
}

func TestDeprecateThingType_AlreadyDeprecated_SetsAgain(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateThingType(&iot.CreateThingTypeInput{ThingTypeName: "redep-type"})
	require.NoError(t, err)

	err = b.DeprecateThingType(&iot.DeprecateThingTypeInput{ThingTypeName: "redep-type"})
	require.NoError(t, err)
	first, err := b.DescribeThingType("redep-type")
	require.NoError(t, err)

	err = b.DeprecateThingType(&iot.DeprecateThingTypeInput{ThingTypeName: "redep-type"})
	require.NoError(t, err)
	second, err := b.DescribeThingType("redep-type")
	require.NoError(t, err)

	assert.True(t, second.Deprecated)
	assert.False(t, second.DeprecationDate.Before(first.DeprecationDate),
		"re-deprecating should update DeprecationDate")
}

func TestUpdateThing_RemoveThingType_Clears(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateThing(&iot.CreateThingInput{
		ThingName:     "typed-clear-thing",
		ThingTypeName: "SomeType",
	})
	require.NoError(t, err)

	err = b.UpdateThing(&iot.UpdateThingInput{
		ThingName:       "typed-clear-thing",
		RemoveThingType: true,
	})
	require.NoError(t, err)

	th, err := b.DescribeThing("typed-clear-thing")
	require.NoError(t, err)
	assert.Empty(t, th.ThingTypeName, "ThingTypeName should be cleared after RemoveThingType")
}

func TestUpdateThing_ChangeThingType(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateThing(&iot.CreateThingInput{
		ThingName:     "changetype-thing",
		ThingTypeName: "TypeA",
	})
	require.NoError(t, err)

	err = b.UpdateThing(&iot.UpdateThingInput{
		ThingName:     "changetype-thing",
		ThingTypeName: "TypeB",
	})
	require.NoError(t, err)

	th, err := b.DescribeThing("changetype-thing")
	require.NoError(t, err)
	assert.Equal(t, "TypeB", th.ThingTypeName)
}

func TestDescribeThingType_NotFound_Error(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.DescribeThingType("ghost-type")
	require.ErrorIs(t, err, iot.ErrThingTypeNotFound)
}
