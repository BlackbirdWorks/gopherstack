package iot_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

// newR3Handler returns a fresh Handler backed by a configured InMemoryBackend.
func newR3Handler() (*iot.Handler, *iot.InMemoryBackend) {
	b := iot.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

	return iot.NewHandler(b, nil), b
}

// r3Req issues a request against the handler using doRefRequest and returns status + body bytes.
func r3Req(t *testing.T, h *iot.Handler, method, path string, body any) (int, []byte) {
	t.Helper()
	rec := doRefRequest(t, h, method, path, body, nil)

	return rec.Code, rec.Body.Bytes()
}

// r3JSON is like r3Req but also decodes the body into out.
func r3JSON(t *testing.T, h *iot.Handler, method, path string, body any, out any) int {
	t.Helper()
	code, raw := r3Req(t, h, method, path, body)
	if out != nil {
		require.NoError(t, json.Unmarshal(raw, out))
	}

	return code
}

// -----------------------------------------------------------------------
// Issue #1: randomHex — certificate IDs must be unique
// -----------------------------------------------------------------------

func TestRefinement3_CertificateIDs_AreUnique(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	seen := make(map[string]bool)

	for range 10 {
		var out map[string]string
		code := r3JSON(t, h, http.MethodPost, "/certificates/create-from-csr", map[string]any{
			"certificateSigningRequest": "csr-data",
			"setAsActive":               true,
		}, &out)
		require.Equal(t, http.StatusOK, code)
		id := out["certificateId"]
		assert.NotEmpty(t, id)
		assert.False(t, seen[id], "duplicate certificate ID: %s", id)
		seen[id] = true
	}
}

func TestRefinement3_RegisterCertificate_MultipleHaveUniqueIDs(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	seen := make(map[string]bool)

	for range 20 {
		cert, err := b.RegisterCertificate(&iot.RegisterCertificateInput{
			CertificatePem: "PEM",
			Status:         "ACTIVE",
		})
		require.NoError(t, err)
		assert.False(t, seen[cert.CertificateID], "duplicate cert ID: %s", cert.CertificateID)
		seen[cert.CertificateID] = true
	}
}

// -----------------------------------------------------------------------
// Issue #2 & #3: CreatePolicy returns policyVersionId and auto-creates version "1"
// -----------------------------------------------------------------------

func TestRefinement3_CreatePolicy_ReturnsPolicyVersionID(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	var out map[string]string
	code := r3JSON(t, h, http.MethodPost, "/policies/test-policy", map[string]any{
		"policyDocument": `{"Version":"2012-10-17"}`,
	}, &out)
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, "1", out["policyVersionId"])
}

func TestRefinement3_CreatePolicy_AutoCreatesVersion1(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreatePolicy(&iot.CreatePolicyInput{
		PolicyName:     "auto-version-policy",
		PolicyDocument: `{"Version":"2012-10-17"}`,
	})
	require.NoError(t, err)

	versions, err := b.ListPolicyVersions("auto-version-policy")
	require.NoError(t, err)
	require.Len(t, versions, 1)
	assert.Equal(t, "1", versions[0].VersionID)
	assert.True(t, versions[0].IsDefaultVersion)
}

func TestRefinement3_CreatePolicy_Version1IsDefault(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreatePolicy(&iot.CreatePolicyInput{
		PolicyName:     "default-v1-policy",
		PolicyDocument: `{"Version":"2012-10-17","Statement":[]}`,
	})
	require.NoError(t, err)

	pv, err := b.GetPolicyVersion("default-v1-policy", "1")
	require.NoError(t, err)
	assert.True(t, pv.IsDefaultVersion)
}

// -----------------------------------------------------------------------
// Issue #4: GetPolicy returns defaultVersionId, creationDate, lastModifiedDate
// -----------------------------------------------------------------------

func TestRefinement3_GetPolicy_ReturnsDefaultVersionID(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	r3Req(t, h, http.MethodPost, "/policies/versioned-policy", map[string]any{
		"policyDocument": `{"Version":"2012-10-17"}`,
	})

	var out map[string]any
	code := r3JSON(t, h, http.MethodGet, "/policies/versioned-policy", nil, &out)
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, "1", out["defaultVersionId"])
}

func TestRefinement3_GetPolicy_ReturnsCreationDate(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	r3Req(t, h, http.MethodPost, "/policies/dated-policy", map[string]any{
		"policyDocument": `{"Version":"2012-10-17"}`,
	})

	var out map[string]any
	code := r3JSON(t, h, http.MethodGet, "/policies/dated-policy", nil, &out)
	require.Equal(t, http.StatusOK, code)
	assert.NotEmpty(t, out["creationDate"])
	assert.NotEmpty(t, out["lastModifiedDate"])
}

func TestRefinement3_GetPolicy_DefaultVersionUpdatesAfterSetDefault(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreatePolicy(&iot.CreatePolicyInput{
		PolicyName:     "multi-ver-policy",
		PolicyDocument: `{"Version":"2012-10-17"}`,
	})
	require.NoError(t, err)

	_, err = b.CreatePolicyVersion(&iot.CreatePolicyVersionInput{
		PolicyName:     "multi-ver-policy",
		PolicyDocument: `{"Version":"2012-10-17","Statement":[]}`,
		SetAsDefault:   true,
	})
	require.NoError(t, err)

	out, err := b.GetPolicy("multi-ver-policy")
	require.NoError(t, err)
	assert.Equal(t, "2", out.DefaultVersionID)
}

// -----------------------------------------------------------------------
// Issue #5: Policy stores CreatedAt / LastModifiedAt
// -----------------------------------------------------------------------

func TestRefinement3_Policy_CreationDateNotZero(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreatePolicy(&iot.CreatePolicyInput{
		PolicyName:     "ts-policy",
		PolicyDocument: `{}`,
	})
	require.NoError(t, err)

	out, err := b.GetPolicy("ts-policy")
	require.NoError(t, err)
	assert.False(t, out.CreatedAt.IsZero(), "CreatedAt should not be zero")
	assert.False(t, out.LastModifiedAt.IsZero(), "LastModifiedAt should not be zero")
}

func TestRefinement3_Policy_LastModifiedUpdatesOnNewVersion(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreatePolicy(&iot.CreatePolicyInput{
		PolicyName:     "lm-policy",
		PolicyDocument: `{}`,
	})
	require.NoError(t, err)

	before, err := b.GetPolicy("lm-policy")
	require.NoError(t, err)

	_, err = b.CreatePolicyVersion(&iot.CreatePolicyVersionInput{
		PolicyName:     "lm-policy",
		PolicyDocument: `{"updated": true}`,
		SetAsDefault:   false,
	})
	require.NoError(t, err)

	after, err := b.GetPolicy("lm-policy")
	require.NoError(t, err)

	assert.False(t, after.LastModifiedAt.Before(before.LastModifiedAt),
		"LastModifiedAt should be >= initial value after CreatePolicyVersion")
}

// -----------------------------------------------------------------------
// Issue #6: UpdateThing validates expectedVersion
// -----------------------------------------------------------------------

func TestRefinement3_UpdateThing_ExpectedVersionMatch(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateThing(&iot.CreateThingInput{ThingName: "ver-thing"})
	require.NoError(t, err)

	err = b.UpdateThing(&iot.UpdateThingInput{
		ThingName:       "ver-thing",
		ExpectedVersion: 1,
		AttributePayload: &iot.AttributePayload{
			Attributes: map[string]string{"env": "test"},
		},
	})
	require.NoError(t, err)

	t2, err := b.DescribeThing("ver-thing")
	require.NoError(t, err)
	assert.Equal(t, int64(2), t2.Version)
}

func TestRefinement3_UpdateThing_ExpectedVersionMismatch_ReturnsVersionConflict(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateThing(&iot.CreateThingInput{ThingName: "conflict-thing"})
	require.NoError(t, err)

	err = b.UpdateThing(&iot.UpdateThingInput{
		ThingName:       "conflict-thing",
		ExpectedVersion: 99,
	})
	require.ErrorIs(t, err, iot.ErrVersionConflict)
}

func TestRefinement3_UpdateThing_ZeroExpectedVersion_Ignored(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateThing(&iot.CreateThingInput{ThingName: "nocheck-thing"})
	require.NoError(t, err)

	err = b.UpdateThing(&iot.UpdateThingInput{
		ThingName:       "nocheck-thing",
		ExpectedVersion: 0,
	})
	require.NoError(t, err)
}

func TestRefinement3_UpdateThing_VersionConflict_HTTP409(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	r3Req(t, h, http.MethodPost, "/things/http-conflict-thing", nil)

	var out map[string]string
	code := r3JSON(t, h, http.MethodPatch, "/things/http-conflict-thing", map[string]any{
		"expectedVersion": 99,
	}, &out)
	assert.Equal(t, http.StatusConflict, code)
	assert.Equal(t, "VersionConflictException", out["__type"])
}

func TestRefinement3_UpdateThing_VersionConflict_AfterSuccessfulUpdate(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateThing(&iot.CreateThingInput{ThingName: "seq-ver-thing"})
	require.NoError(t, err)

	err = b.UpdateThing(&iot.UpdateThingInput{
		ThingName:       "seq-ver-thing",
		ExpectedVersion: 1,
	})
	require.NoError(t, err)

	err = b.UpdateThing(&iot.UpdateThingInput{
		ThingName:       "seq-ver-thing",
		ExpectedVersion: 1,
	})
	require.ErrorIs(t, err, iot.ErrVersionConflict)
}

func TestRefinement3_UpdateThing_EmptyPayload_IncrementsVersion(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateThing(&iot.CreateThingInput{ThingName: "empty-update-thing"})
	require.NoError(t, err)

	err = b.UpdateThing(&iot.UpdateThingInput{ThingName: "empty-update-thing"})
	require.NoError(t, err)

	th, err := b.DescribeThing("empty-update-thing")
	require.NoError(t, err)
	assert.Equal(t, int64(2), th.Version)
}

// -----------------------------------------------------------------------
// Issue #7: UpdateThingGroup returns {"version": N}
// -----------------------------------------------------------------------

func TestRefinement3_UpdateThingGroup_ReturnsVersion(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	r3Req(t, h, http.MethodPost, "/thing-groups/ver-group", map[string]any{
		"thingGroupProperties": map[string]any{
			"thingGroupDescription": "original",
		},
	})

	var out map[string]any
	code := r3JSON(t, h, http.MethodPatch, "/thing-groups/ver-group", map[string]any{
		"thingGroupProperties": map[string]any{
			"thingGroupDescription": "updated",
		},
	}, &out)
	require.Equal(t, http.StatusOK, code)
	v, ok := out["version"]
	assert.True(t, ok, "response should contain 'version' key")
	assert.InEpsilon(t, float64(2), v, 0.001)
}

func TestRefinement3_UpdateThingGroup_VersionIncrements(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateThingGroup(&iot.CreateThingGroupInput{ThingGroupName: "inc-group"})
	require.NoError(t, err)

	ver1, err := b.UpdateThingGroup(&iot.UpdateThingGroupInput{
		ThingGroupName: "inc-group",
		Description:    "first update",
	})
	require.NoError(t, err)
	assert.Equal(t, int64(2), ver1)

	ver2, err := b.UpdateThingGroup(&iot.UpdateThingGroupInput{
		ThingGroupName: "inc-group",
		Description:    "second update",
	})
	require.NoError(t, err)
	assert.Equal(t, int64(3), ver2)
}

func TestRefinement3_UpdateThingGroup_ExpectedVersionMismatch(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateThingGroup(&iot.CreateThingGroupInput{ThingGroupName: "ev-group"})
	require.NoError(t, err)

	_, err = b.UpdateThingGroup(&iot.UpdateThingGroupInput{
		ThingGroupName:  "ev-group",
		Description:     "upd",
		ExpectedVersion: 99,
	})
	require.ErrorIs(t, err, iot.ErrVersionConflict)
}

func TestRefinement3_UpdateThingGroup_ZeroExpectedVersion_Ignored(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateThingGroup(&iot.CreateThingGroupInput{ThingGroupName: "nocheck-group"})
	require.NoError(t, err)

	_, err = b.UpdateThingGroup(&iot.UpdateThingGroupInput{
		ThingGroupName:  "nocheck-group",
		Description:     "updated",
		ExpectedVersion: 0,
	})
	require.NoError(t, err)
}

func TestRefinement3_UpdateThingGroup_MultipleUpdates_VersionTrackingCorrect(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	r3Req(t, h, http.MethodPost, "/thing-groups/multi-ver-group", nil)

	for expectedVersion := float64(2); expectedVersion <= 5; expectedVersion++ {
		var out map[string]any
		code := r3JSON(t, h, http.MethodPatch, "/thing-groups/multi-ver-group", map[string]any{
			"thingGroupProperties": map[string]any{
				"thingGroupDescription": "update",
			},
		}, &out)
		require.Equal(t, http.StatusOK, code)
		assert.InEpsilon(t, expectedVersion, out["version"], 0.001)
	}
}

// -----------------------------------------------------------------------
// Issue #8–10: ThingType gets a ThingTypeID
// -----------------------------------------------------------------------

func TestRefinement3_CreateThingType_ReturnsThingTypeID(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	var out map[string]string
	code := r3JSON(t, h, http.MethodPost, "/thing-types/SensorV3", nil, &out)
	require.Equal(t, http.StatusOK, code)
	assert.NotEmpty(t, out["thingTypeId"])
}

func TestRefinement3_ThingType_IDIsUUID(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	tt, err := b.CreateThingType(&iot.CreateThingTypeInput{ThingTypeName: "uuid-type"})
	require.NoError(t, err)

	assert.NotEmpty(t, tt.ThingTypeID)
	assert.Len(t, tt.ThingTypeID, 36)
	assert.Contains(t, tt.ThingTypeID, "-", "ThingTypeID should be UUID-formatted")
}

func TestRefinement3_ThingType_IDsAreUnique(t *testing.T) {
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

func TestRefinement3_ThingType_IDIsConsistentBetweenCreateAndDescribe(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	created, err := b.CreateThingType(&iot.CreateThingTypeInput{ThingTypeName: "consistent-type"})
	require.NoError(t, err)

	described, err := b.DescribeThingType("consistent-type")
	require.NoError(t, err)
	assert.Equal(t, created.ThingTypeID, described.ThingTypeID)
}

func TestRefinement3_DescribeThingType_IncludesThingTypeID(t *testing.T) {
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

// -----------------------------------------------------------------------
// Issue #11: ListThingTypes includes thingTypeMetadata per item
// -----------------------------------------------------------------------

func TestRefinement3_ListThingTypes_IncludesMetadata(t *testing.T) {
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

func TestRefinement3_ListThingTypes_DeprecatedFlagInMetadata(t *testing.T) {
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

func TestRefinement3_ListThingTypes_IsSorted(t *testing.T) {
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

// -----------------------------------------------------------------------
// Issue #12: DeprecateThingType supports undoDeprecate
// -----------------------------------------------------------------------

func TestRefinement3_DeprecateThingType_SetsDeprecated(t *testing.T) {
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

func TestRefinement3_DeprecateThingType_UndoDeprecate(t *testing.T) {
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

func TestRefinement3_DeprecateThingType_HTTP_UndoDeprecate(t *testing.T) {
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

func TestRefinement3_DescribeThingType_DeprecationDate_SetOnDeprecate(t *testing.T) {
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

func TestRefinement3_DescribeThingType_DeprecationDate_ClearedOnUndoDeprecate(t *testing.T) {
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

func TestRefinement3_DeprecateThingType_NotFound_UsesAWSFormat(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	var out map[string]string
	code := r3JSON(t, h, http.MethodPost, "/thing-types/no-such-type/deprecate", map[string]any{
		"undoDeprecate": false,
	}, &out)
	assert.Equal(t, http.StatusNotFound, code)
	assert.Equal(t, "ResourceNotFoundException", out["__type"])
}

// -----------------------------------------------------------------------
// Issue #13: DeleteThingType requires deprecation first
// -----------------------------------------------------------------------

func TestRefinement3_DeleteThingType_RequiresDeprecation(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateThingType(&iot.CreateThingTypeInput{ThingTypeName: "nodelete-type"})
	require.NoError(t, err)

	err = b.DeleteThingType("nodelete-type")
	require.ErrorIs(t, err, iot.ErrValidation)
}

func TestRefinement3_DeleteThingType_SucceedsAfterDeprecation(t *testing.T) {
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

func TestRefinement3_DeleteThingType_HTTP_MissingDeprecation_Returns400(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	r3Req(t, h, http.MethodPost, "/thing-types/no-dep-http-type", nil)

	var out map[string]string
	code := r3JSON(t, h, http.MethodDelete, "/thing-types/no-dep-http-type", nil, &out)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "InvalidRequestException", out["__type"])
}

// -----------------------------------------------------------------------
// Issue #14: DescribeThingGroup includes parentGroupName in thingGroupMetadata
// -----------------------------------------------------------------------

func TestRefinement3_DescribeThingGroup_IncludesParentGroupName(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	r3Req(t, h, http.MethodPost, "/thing-groups/parent-group", nil)
	r3Req(t, h, http.MethodPost, "/thing-groups/child-group", map[string]any{
		"parentGroupName": "parent-group",
	})

	var out map[string]any
	code := r3JSON(t, h, http.MethodGet, "/thing-groups/child-group", nil, &out)
	require.Equal(t, http.StatusOK, code)
	meta, ok := out["thingGroupMetadata"].(map[string]any)
	require.True(t, ok, "response should contain thingGroupMetadata")
	assert.Equal(t, "parent-group", meta["parentGroupName"])
}

func TestRefinement3_DescribeThingGroup_MetadataHasCreationDate(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	r3Req(t, h, http.MethodPost, "/thing-groups/cd-group", nil)

	var out map[string]any
	r3JSON(t, h, http.MethodGet, "/thing-groups/cd-group", nil, &out)
	meta := out["thingGroupMetadata"].(map[string]any)
	assert.NotEmpty(t, meta["creationDate"])
}

func TestRefinement3_CreateThingGroup_ParentGroupNameStored(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateThingGroup(&iot.CreateThingGroupInput{ThingGroupName: "root"})
	require.NoError(t, err)

	child, err := b.CreateThingGroup(&iot.CreateThingGroupInput{
		ThingGroupName:  "child",
		ParentGroupName: "root",
	})
	require.NoError(t, err)
	assert.Equal(t, "root", child.ParentGroupName)
}

// -----------------------------------------------------------------------
// Issue #15: CreateCertificateFromCsr response includes status
// -----------------------------------------------------------------------

func TestRefinement3_CreateCertFromCsr_ResponseIncludesStatus_Active(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	var out map[string]string
	code := r3JSON(t, h, http.MethodPost, "/certificates/create-from-csr", map[string]any{
		"certificateSigningRequest": "csr-data",
		"setAsActive":               true,
	}, &out)
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, "ACTIVE", out["status"])
}

func TestRefinement3_CreateCertFromCsr_ResponseIncludesStatus_Inactive(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	var out map[string]string
	code := r3JSON(t, h, http.MethodPost, "/certificates/create-from-csr", map[string]any{
		"certificateSigningRequest": "csr-data",
		"setAsActive":               false,
	}, &out)
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, "INACTIVE", out["status"])
}

// -----------------------------------------------------------------------
// Issue #16 & #17: Certificate has lastModifiedDate; DescribeCertificate returns it
// -----------------------------------------------------------------------

func TestRefinement3_Certificate_LastModifiedDate_SetOnCreate(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	cert, err := b.CreateCertificateFromCsr(&iot.CreateCertificateFromCsrInput{SetAsActive: true})
	require.NoError(t, err)
	assert.False(t, cert.LastModifiedAt.IsZero(), "LastModifiedAt should be set on certificate creation")
}

func TestRefinement3_DescribeCertificate_ReturnsLastModifiedDate(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	var createOut map[string]string
	r3JSON(t, h, http.MethodPost, "/certificates/create-from-csr", map[string]any{
		"certificateSigningRequest": "csr",
		"setAsActive":               true,
	}, &createOut)
	certID := createOut["certificateId"]

	var out map[string]any
	code := r3JSON(t, h, http.MethodGet, "/certificates/"+certID, nil, &out)
	require.Equal(t, http.StatusOK, code)
	desc, ok := out["certificateDescription"].(map[string]any)
	require.True(t, ok)
	assert.NotEmpty(t, desc["lastModifiedDate"])
}

// -----------------------------------------------------------------------
// Issue #18: ListCertificates includes lastModifiedDate
// -----------------------------------------------------------------------

func TestRefinement3_ListCertificates_IncludesLastModifiedDate(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	r3Req(t, h, http.MethodPost, "/certificates/create-from-csr", map[string]any{
		"certificateSigningRequest": "csr",
		"setAsActive":               true,
	})

	var out struct {
		Certificates []map[string]any `json:"certificates"`
	}
	code := r3JSON(t, h, http.MethodGet, "/certificates", nil, &out)
	require.Equal(t, http.StatusOK, code)
	require.Len(t, out.Certificates, 1)
	assert.NotEmpty(t, out.Certificates[0]["lastModifiedDate"])
}

// -----------------------------------------------------------------------
// Issue #19: UpdateCertificate validates status values
// -----------------------------------------------------------------------

func TestRefinement3_UpdateCertificate_ValidStatus_ACTIVE(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	cert, err := b.CreateCertificateFromCsr(&iot.CreateCertificateFromCsrInput{})
	require.NoError(t, err)

	err = b.UpdateCertificate(&iot.UpdateCertificateInput{
		CertificateID: cert.CertificateID,
		NewStatus:     "ACTIVE",
	})
	require.NoError(t, err)
}

func TestRefinement3_UpdateCertificate_ValidStatus_REVOKED(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	cert, err := b.CreateCertificateFromCsr(&iot.CreateCertificateFromCsrInput{SetAsActive: true})
	require.NoError(t, err)

	err = b.UpdateCertificate(&iot.UpdateCertificateInput{
		CertificateID: cert.CertificateID,
		NewStatus:     "REVOKED",
	})
	require.NoError(t, err)
}

func TestRefinement3_UpdateCertificate_ValidStatus_PENDING_TRANSFER(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	cert, err := b.CreateCertificateFromCsr(&iot.CreateCertificateFromCsrInput{})
	require.NoError(t, err)

	err = b.UpdateCertificate(&iot.UpdateCertificateInput{
		CertificateID: cert.CertificateID,
		NewStatus:     "PENDING_TRANSFER",
	})
	require.ErrorIs(t, err, iot.ErrValidation, "PENDING_TRANSFER cannot be set via UpdateCertificate")
}

func TestRefinement3_UpdateCertificate_ValidStatus_PENDING_ACTIVATION(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	cert, err := b.CreateCertificateFromCsr(&iot.CreateCertificateFromCsrInput{})
	require.NoError(t, err)

	err = b.UpdateCertificate(&iot.UpdateCertificateInput{
		CertificateID: cert.CertificateID,
		NewStatus:     "PENDING_ACTIVATION",
	})
	require.ErrorIs(t, err, iot.ErrValidation, "PENDING_ACTIVATION cannot be set via UpdateCertificate")
}

func TestRefinement3_UpdateCertificate_ValidStatus_INACTIVE(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	cert, err := b.CreateCertificateFromCsr(&iot.CreateCertificateFromCsrInput{SetAsActive: true})
	require.NoError(t, err)

	err = b.UpdateCertificate(&iot.UpdateCertificateInput{
		CertificateID: cert.CertificateID,
		NewStatus:     "INACTIVE",
	})
	require.NoError(t, err)
}

func TestRefinement3_UpdateCertificate_InvalidStatus_ReturnsError(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	cert, err := b.CreateCertificateFromCsr(&iot.CreateCertificateFromCsrInput{})
	require.NoError(t, err)

	err = b.UpdateCertificate(&iot.UpdateCertificateInput{
		CertificateID: cert.CertificateID,
		NewStatus:     "BOGUS_STATUS",
	})
	require.ErrorIs(t, err, iot.ErrValidation)
}

func TestRefinement3_UpdateCertificate_UpdatesLastModifiedDate(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	cert, err := b.CreateCertificateFromCsr(&iot.CreateCertificateFromCsrInput{})
	require.NoError(t, err)
	initial := cert.LastModifiedAt

	err = b.UpdateCertificate(&iot.UpdateCertificateInput{
		CertificateID: cert.CertificateID,
		NewStatus:     "ACTIVE",
	})
	require.NoError(t, err)

	updated, err := b.DescribeCertificate(cert.CertificateID)
	require.NoError(t, err)
	assert.False(t, updated.LastModifiedAt.Before(initial),
		"LastModifiedAt should be updated after UpdateCertificate")
}

func TestRefinement3_UpdateCertificate_InvalidStatus_HTTP400(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	var createOut map[string]string
	r3JSON(t, h, http.MethodPost, "/certificates/create-from-csr", map[string]any{
		"certificateSigningRequest": "csr",
	}, &createOut)
	certID := createOut["certificateId"]

	var out map[string]string
	code := r3JSON(t, h, http.MethodPut, "/certificates/"+certID+"?newStatus=INVALID_STATUS", nil, &out)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "InvalidRequestException", out["__type"])
}

// -----------------------------------------------------------------------
// Issue #20: DeletePolicyVersion cannot delete the default version
// -----------------------------------------------------------------------

func TestRefinement3_DeletePolicyVersion_DefaultVersionBlocked(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreatePolicy(&iot.CreatePolicyInput{
		PolicyName:     "del-default-policy",
		PolicyDocument: `{}`,
	})
	require.NoError(t, err)

	err = b.DeletePolicyVersion("del-default-policy", "1")
	require.ErrorIs(t, err, iot.ErrDeleteConflict)
}

func TestRefinement3_DeletePolicyVersion_NonDefaultCanBeDeleted(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreatePolicy(&iot.CreatePolicyInput{
		PolicyName:     "del-v2-policy",
		PolicyDocument: `{}`,
	})
	require.NoError(t, err)

	_, err = b.CreatePolicyVersion(&iot.CreatePolicyVersionInput{
		PolicyName:     "del-v2-policy",
		PolicyDocument: `{"updated": true}`,
		SetAsDefault:   false,
	})
	require.NoError(t, err)

	err = b.DeletePolicyVersion("del-v2-policy", "2")
	require.NoError(t, err)
}

func TestRefinement3_DeletePolicyVersion_AfterSetDefault_OldDefaultCanBeDeleted(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreatePolicy(&iot.CreatePolicyInput{
		PolicyName:     "swap-default-policy",
		PolicyDocument: `{}`,
	})
	require.NoError(t, err)

	_, err = b.CreatePolicyVersion(&iot.CreatePolicyVersionInput{
		PolicyName:     "swap-default-policy",
		PolicyDocument: `{"v2": true}`,
		SetAsDefault:   true,
	})
	require.NoError(t, err)

	err = b.DeletePolicyVersion("swap-default-policy", "1")
	require.NoError(t, err)
}

func TestRefinement3_DeletePolicyVersion_DefaultVersion_DeleteConflict(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreatePolicy(&iot.CreatePolicyInput{
		PolicyName:     "del-def-conflict",
		PolicyDocument: `{}`,
	})
	require.NoError(t, err)

	// Delete version 1 (the default) — should fail with ErrDeleteConflict
	err = b.DeletePolicyVersion("del-def-conflict", "1")
	require.ErrorIs(t, err, iot.ErrDeleteConflict)
}

// -----------------------------------------------------------------------
// Issue #21: Error response format uses __type + message
// -----------------------------------------------------------------------

func TestRefinement3_ErrorFormat_NotFound_UsesAWSFormat(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	var out map[string]string
	code := r3JSON(t, h, http.MethodGet, "/things/nonexistent-thing", nil, &out)
	assert.Equal(t, http.StatusNotFound, code)
	assert.Equal(t, "ResourceNotFoundException", out["__type"])
	assert.NotEmpty(t, out["message"])
}

func TestRefinement3_ErrorFormat_AlreadyExists_UsesAWSFormat(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	r3Req(t, h, http.MethodPost, "/things/dup-thing", nil)
	var out map[string]string
	code := r3JSON(t, h, http.MethodPost, "/things/dup-thing", nil, &out)
	assert.Equal(t, http.StatusConflict, code)
	assert.Equal(t, "ResourceAlreadyExistsException", out["__type"])
	assert.NotEmpty(t, out["message"])
}

func TestRefinement3_ErrorFormat_PolicyNotFound_UsesAWSFormat(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	var out map[string]string
	code := r3JSON(t, h, http.MethodGet, "/policies/missing-policy", nil, &out)
	assert.Equal(t, http.StatusNotFound, code)
	assert.Equal(t, "ResourceNotFoundException", out["__type"])
}

func TestRefinement3_ErrorFormat_ThingGroupNotFound_UsesAWSFormat(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	var out map[string]string
	code := r3JSON(t, h, http.MethodGet, "/thing-groups/missing-group", nil, &out)
	assert.Equal(t, http.StatusNotFound, code)
	assert.Equal(t, "ResourceNotFoundException", out["__type"])
}

func TestRefinement3_ErrorFormat_ThingTypeNotFound_UsesAWSFormat(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	var out map[string]string
	code := r3JSON(t, h, http.MethodGet, "/thing-types/missing-type", nil, &out)
	assert.Equal(t, http.StatusNotFound, code)
	assert.Equal(t, "ResourceNotFoundException", out["__type"])
}

func TestRefinement3_ErrorFormat_CertNotFound_UsesAWSFormat(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	certID := strings.Repeat("a", 64)
	var out map[string]string
	code := r3JSON(t, h, http.MethodGet, "/certificates/"+certID, nil, &out)
	assert.Equal(t, http.StatusNotFound, code)
	assert.Equal(t, "ResourceNotFoundException", out["__type"])
}

func TestRefinement3_ErrorFormat_RuleNotFound_UsesAWSFormat(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	var out map[string]string
	code := r3JSON(t, h, http.MethodGet, "/rules/missing-rule", nil, &out)
	assert.Equal(t, http.StatusNotFound, code)
	assert.Equal(t, "ResourceNotFoundException", out["__type"])
}

// -----------------------------------------------------------------------
// Additional accuracy: error types are distinct
// -----------------------------------------------------------------------

func TestRefinement3_ErrVersionConflict_IsDistinctFromOtherErrors(t *testing.T) {
	t.Parallel()

	assert.NotEqual(t, iot.ErrVersionConflict, iot.ErrValidation)
	assert.NotEqual(t, iot.ErrVersionConflict, iot.ErrAlreadyExists)
	assert.NotEqual(t, iot.ErrVersionConflict, iot.ErrDeleteConflict)
}

func TestRefinement3_ErrDeleteConflict_IsDistinctFromOtherErrors(t *testing.T) {
	t.Parallel()

	assert.NotEqual(t, iot.ErrDeleteConflict, iot.ErrValidation)
	assert.NotEqual(t, iot.ErrDeleteConflict, iot.ErrVersionConflict)
	assert.NotEqual(t, iot.ErrDeleteConflict, iot.ErrAlreadyExists)
}

// -----------------------------------------------------------------------
// Additional coverage: Certificate ID format
// -----------------------------------------------------------------------

func TestRefinement3_Certificate_IDIs64HexChars(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	cert, err := b.CreateCertificateFromCsr(&iot.CreateCertificateFromCsrInput{SetAsActive: true})
	require.NoError(t, err)

	assert.Len(t, cert.CertificateID, 64, "certificate ID should be 64 hex characters")

	for _, ch := range cert.CertificateID {
		assert.True(t, (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f'),
			"certificate ID should be lowercase hex, got char: %c", ch)
	}
}

func TestRefinement3_Certificate_ARNContainsRegionAndAccount(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackendWithConfig("111122223333", "eu-west-1")
	cert, err := b.CreateCertificateFromCsr(&iot.CreateCertificateFromCsrInput{SetAsActive: true})
	require.NoError(t, err)

	assert.True(t, strings.HasPrefix(cert.ARN, "arn:aws:iot:eu-west-1:111122223333:cert/"),
		"cert ARN should contain region+account, got: %s", cert.ARN)
}

// -----------------------------------------------------------------------
// Additional coverage: Policy version lifecycle
// -----------------------------------------------------------------------

func TestRefinement3_PolicyVersion_CreateAndList(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreatePolicy(&iot.CreatePolicyInput{
		PolicyName:     "vlist-policy",
		PolicyDocument: `{}`,
	})
	require.NoError(t, err)

	_, err = b.CreatePolicyVersion(&iot.CreatePolicyVersionInput{
		PolicyName:     "vlist-policy",
		PolicyDocument: `{"v2": true}`,
		SetAsDefault:   false,
	})
	require.NoError(t, err)

	versions, err := b.ListPolicyVersions("vlist-policy")
	require.NoError(t, err)
	assert.Len(t, versions, 2, "should have version 1 (auto-created) and version 2")
}

func TestRefinement3_PolicyVersion_SetDefault_ClearsOldDefault(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreatePolicy(&iot.CreatePolicyInput{
		PolicyName:     "setdef-policy",
		PolicyDocument: `{}`,
	})
	require.NoError(t, err)

	_, err = b.CreatePolicyVersion(&iot.CreatePolicyVersionInput{
		PolicyName:     "setdef-policy",
		PolicyDocument: `{"v2": true}`,
		SetAsDefault:   false,
	})
	require.NoError(t, err)

	err = b.SetDefaultPolicyVersion("setdef-policy", "2")
	require.NoError(t, err)

	v1, err := b.GetPolicyVersion("setdef-policy", "1")
	require.NoError(t, err)
	assert.False(t, v1.IsDefaultVersion, "version 1 should no longer be default")

	v2, err := b.GetPolicyVersion("setdef-policy", "2")
	require.NoError(t, err)
	assert.True(t, v2.IsDefaultVersion, "version 2 should now be default")
}

func TestRefinement3_CreatePolicy_DocumentMatchesVersion1(t *testing.T) {
	t.Parallel()

	const doc = `{"Version":"2012-10-17","Statement":[]}`
	_, b := newR3Handler()
	_, err := b.CreatePolicy(&iot.CreatePolicyInput{
		PolicyName:     "docmatch-policy",
		PolicyDocument: doc,
	})
	require.NoError(t, err)

	pv, err := b.GetPolicyVersion("docmatch-policy", "1")
	require.NoError(t, err)
	assert.JSONEq(t, doc, pv.PolicyDocument)
}

func TestRefinement3_GetPolicyVersion_NotFound_AfterDelete(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreatePolicy(&iot.CreatePolicyInput{
		PolicyName:     "del-get-policy",
		PolicyDocument: `{}`,
	})
	require.NoError(t, err)

	_, err = b.CreatePolicyVersion(&iot.CreatePolicyVersionInput{
		PolicyName:     "del-get-policy",
		PolicyDocument: `{"v2": true}`,
		SetAsDefault:   true,
	})
	require.NoError(t, err)

	err = b.DeletePolicyVersion("del-get-policy", "1")
	require.NoError(t, err)

	_, err = b.GetPolicyVersion("del-get-policy", "1")
	require.ErrorIs(t, err, iot.ErrPolicyVersionNotFound)
}

// -----------------------------------------------------------------------
// Additional coverage: RegisterCertificate variants have lastModifiedDate
// -----------------------------------------------------------------------

func TestRefinement3_RegisterCertificate_HasLastModifiedDate(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	cert, err := b.RegisterCertificate(&iot.RegisterCertificateInput{
		CertificatePem: "PEM",
		Status:         "ACTIVE",
	})
	require.NoError(t, err)
	assert.False(t, cert.LastModifiedAt.IsZero())
}

func TestRefinement3_RegisterCertificateWithoutCA_HasLastModifiedDate(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	cert, err := b.RegisterCertificateWithoutCA(&iot.RegisterCertificateInput{
		CertificatePem: "PEM",
		Status:         "INACTIVE",
	})
	require.NoError(t, err)
	assert.False(t, cert.LastModifiedAt.IsZero())
}

// -----------------------------------------------------------------------
// Additional coverage: Reset clears all new state
// -----------------------------------------------------------------------

func TestRefinement3_Reset_ClearsThingTypes(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateThingType(&iot.CreateThingTypeInput{ThingTypeName: "reset-type"})
	require.NoError(t, err)

	b.Reset()
	assert.Empty(t, b.ListThingTypes())
}

func TestRefinement3_Reset_ClearsCertificates(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateCertificateFromCsr(&iot.CreateCertificateFromCsrInput{SetAsActive: true})
	require.NoError(t, err)

	b.Reset()
	assert.Empty(t, b.ListCertificates())
}

func TestRefinement3_Reset_ClearsPoliciesAndVersions(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreatePolicy(&iot.CreatePolicyInput{
		PolicyName:     "reset-ver-policy",
		PolicyDocument: `{}`,
	})
	require.NoError(t, err)

	b.Reset()

	_, err = b.ListPolicyVersions("reset-ver-policy")
	require.ErrorIs(t, err, iot.ErrPolicyNotFound)
}

// -----------------------------------------------------------------------
// Additional coverage: ThingType searchableAttributes preserved
// -----------------------------------------------------------------------

func TestRefinement3_ThingType_SearchableAttributesPreserved(t *testing.T) {
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

// -----------------------------------------------------------------------
// Additional coverage: GetPolicy returns correct policyDocument
// -----------------------------------------------------------------------

func TestRefinement3_GetPolicy_ReturnsSameDocument(t *testing.T) {
	t.Parallel()

	const doc = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow"}]}`
	h, _ := newR3Handler()
	r3Req(t, h, http.MethodPost, "/policies/doc-policy", map[string]any{
		"policyDocument": doc,
	})

	var out map[string]any
	code := r3JSON(t, h, http.MethodGet, "/policies/doc-policy", nil, &out)
	require.Equal(t, http.StatusOK, code)
	assert.JSONEq(t, doc, out["policyDocument"].(string))
}

// -----------------------------------------------------------------------
// Additional coverage: ThingGroup parent is stored via DescribeThingGroup
// -----------------------------------------------------------------------

func TestRefinement3_DescribeThingGroup_StoresAndReturnsParent(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateThingGroup(&iot.CreateThingGroupInput{ThingGroupName: "parent"})
	require.NoError(t, err)

	_, err = b.CreateThingGroup(&iot.CreateThingGroupInput{
		ThingGroupName:  "child",
		ParentGroupName: "parent",
	})
	require.NoError(t, err)

	child, err := b.DescribeThingGroup("child")
	require.NoError(t, err)
	assert.Equal(t, "parent", child.ParentGroupName)
}

// -----------------------------------------------------------------------
// Additional coverage: CreateThingType idempotency and error cases
// -----------------------------------------------------------------------

func TestRefinement3_CreateThingType_DuplicateName_Conflict(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateThingType(&iot.CreateThingTypeInput{ThingTypeName: "dup-thing-type"})
	require.NoError(t, err)

	_, err = b.CreateThingType(&iot.CreateThingTypeInput{ThingTypeName: "dup-thing-type"})
	require.ErrorIs(t, err, iot.ErrAlreadyExists)
}

func TestRefinement3_CreateThingType_HTTP_DuplicateName_Returns409(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	r3Req(t, h, http.MethodPost, "/thing-types/http-dup-type", nil)

	var out map[string]string
	code := r3JSON(t, h, http.MethodPost, "/thing-types/http-dup-type", nil, &out)
	assert.Equal(t, http.StatusConflict, code)
	assert.Equal(t, "ResourceAlreadyExistsException", out["__type"])
}

func TestRefinement3_DeprecateThingType_AlreadyDeprecated_SetsAgain(t *testing.T) {
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

// -----------------------------------------------------------------------
// Additional coverage: CreateThingGroup uniqueness and errors
// -----------------------------------------------------------------------

func TestRefinement3_CreateThingGroup_DuplicateName_Conflict(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateThingGroup(&iot.CreateThingGroupInput{ThingGroupName: "dup-group"})
	require.NoError(t, err)

	_, err = b.CreateThingGroup(&iot.CreateThingGroupInput{ThingGroupName: "dup-group"})
	require.ErrorIs(t, err, iot.ErrAlreadyExists)
}

func TestRefinement3_DeleteThingGroup_NotFound_Error(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	err := b.DeleteThingGroup("nonexistent-group")
	require.ErrorIs(t, err, iot.ErrThingGroupNotFound)
}

func TestRefinement3_CreateThingGroup_VersionStartsAt1(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	tg, err := b.CreateThingGroup(&iot.CreateThingGroupInput{ThingGroupName: "ver1-group"})
	require.NoError(t, err)
	assert.Equal(t, int64(1), tg.Version)
}

func TestRefinement3_UpdateThingGroup_NotFound_Error(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.UpdateThingGroup(&iot.UpdateThingGroupInput{ThingGroupName: "ghost-group"})
	require.ErrorIs(t, err, iot.ErrThingGroupNotFound)
}

// -----------------------------------------------------------------------
// Additional coverage: Thing lifecycle
// -----------------------------------------------------------------------

func TestRefinement3_CreateThing_DuplicateName_Conflict(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateThing(&iot.CreateThingInput{ThingName: "dup-thing"})
	require.NoError(t, err)

	_, err = b.CreateThing(&iot.CreateThingInput{ThingName: "dup-thing"})
	require.ErrorIs(t, err, iot.ErrAlreadyExists)
}

func TestRefinement3_DeleteThing_NotFound_Error(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	err := b.DeleteThing("ghost-thing")
	require.ErrorIs(t, err, iot.ErrThingNotFound)
}

func TestRefinement3_UpdateThing_NotFound_Error(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	err := b.UpdateThing(&iot.UpdateThingInput{ThingName: "ghost-thing"})
	require.ErrorIs(t, err, iot.ErrThingNotFound)
}

func TestRefinement3_ListThings_SortedByName(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	names := []string{"zebra", "alpha", "mango"}
	for _, n := range names {
		_, err := b.CreateThing(&iot.CreateThingInput{ThingName: n})
		require.NoError(t, err)
	}

	things := b.ListThings()
	require.Len(t, things, 3)
	assert.Equal(t, "alpha", things[0].ThingName)
	assert.Equal(t, "mango", things[1].ThingName)
	assert.Equal(t, "zebra", things[2].ThingName)
}

func TestRefinement3_DescribeThing_ReturnsThingID(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	out, err := b.CreateThing(&iot.CreateThingInput{ThingName: "id-thing"})
	require.NoError(t, err)

	th, err := b.DescribeThing("id-thing")
	require.NoError(t, err)
	assert.Equal(t, out.ThingID, th.ThingID)
	assert.NotEmpty(t, th.ThingID)
}

func TestRefinement3_Thing_ARNFormat(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackendWithConfig("999988887777", "ap-southeast-1")
	_, err := b.CreateThing(&iot.CreateThingInput{ThingName: "arn-test-thing"})
	require.NoError(t, err)

	th, err := b.DescribeThing("arn-test-thing")
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(th.ARN, "arn:aws:iot:ap-southeast-1:999988887777:thing/"),
		"thing ARN should contain region+account, got: %s", th.ARN)
}

func TestRefinement3_UpdateThing_RemoveThingType_Clears(t *testing.T) {
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

func TestRefinement3_UpdateThing_ChangeThingType(t *testing.T) {
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

// -----------------------------------------------------------------------
// Additional coverage: Certificate status lifecycle
// -----------------------------------------------------------------------

func TestRefinement3_Certificate_StatusTransitions(t *testing.T) {
	t.Parallel()

	statuses := []string{"ACTIVE", "INACTIVE", "REVOKED"}

	for _, status := range statuses {
		t.Run(status, func(t *testing.T) {
			t.Parallel()

			_, b := newR3Handler()
			cert, err := b.CreateCertificateFromCsr(&iot.CreateCertificateFromCsrInput{})
			require.NoError(t, err)

			err = b.UpdateCertificate(&iot.UpdateCertificateInput{
				CertificateID: cert.CertificateID,
				NewStatus:     status,
			})
			require.NoError(t, err)

			updated, err := b.DescribeCertificate(cert.CertificateID)
			require.NoError(t, err)
			assert.Equal(t, status, updated.Status)
		})
	}
}

func TestRefinement3_DeleteCertificate_NotFound_Error(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	err := b.DeleteCertificate(strings.Repeat("0", 64))
	require.ErrorIs(t, err, iot.ErrCertificateNotFound)
}

func TestRefinement3_ListCertificates_SortedByID(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()

	for range 3 {
		_, err := b.RegisterCertificate(&iot.RegisterCertificateInput{
			CertificatePem: "PEM",
			Status:         "ACTIVE",
		})
		require.NoError(t, err)
	}

	certs := b.ListCertificates()
	require.Len(t, certs, 3)

	for idx := 1; idx < len(certs); idx++ {
		assert.LessOrEqual(t, certs[idx-1].CertificateID, certs[idx].CertificateID,
			"certificates should be sorted by ID")
	}
}

// -----------------------------------------------------------------------
// Additional coverage: Policy CRUD accuracy
// -----------------------------------------------------------------------

func TestRefinement3_ListPolicies_SortedByName(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	names := []string{"zoo-policy", "ant-policy", "mid-policy"}
	for _, n := range names {
		_, err := b.CreatePolicy(&iot.CreatePolicyInput{
			PolicyName:     n,
			PolicyDocument: `{}`,
		})
		require.NoError(t, err)
	}

	policies := b.ListPolicies()
	require.Len(t, policies, 3)
	assert.Equal(t, "ant-policy", policies[0].PolicyName)
	assert.Equal(t, "mid-policy", policies[1].PolicyName)
	assert.Equal(t, "zoo-policy", policies[2].PolicyName)
}

func TestRefinement3_DeletePolicy_RemovesFromList(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreatePolicy(&iot.CreatePolicyInput{
		PolicyName:     "to-delete-policy",
		PolicyDocument: `{}`,
	})
	require.NoError(t, err)

	err = b.DeletePolicy("to-delete-policy")
	require.NoError(t, err)

	_, err = b.GetPolicy("to-delete-policy")
	require.ErrorIs(t, err, iot.ErrPolicyNotFound)
}

func TestRefinement3_CreatePolicy_ARNFormat(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackendWithConfig("555566667777", "ca-central-1")
	out, err := b.CreatePolicy(&iot.CreatePolicyInput{
		PolicyName:     "arn-test-policy",
		PolicyDocument: `{}`,
	})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iot:ca-central-1:555566667777:policy/arn-test-policy", out.PolicyARN)
}

// -----------------------------------------------------------------------
// Additional coverage: TopicRule CRUD
// -----------------------------------------------------------------------

func TestRefinement3_CreateTopicRule_ARNFormat(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackendWithConfig("111100001111", "us-west-2")
	err := b.CreateTopicRule(&iot.CreateTopicRuleInput{
		RuleName: "arn-rule",
		TopicRulePayload: &iot.TopicRulePayload{
			SQL:     "SELECT * FROM 'topic'",
			Actions: []iot.RuleAction{},
		},
	})
	require.NoError(t, err)

	r, err := b.GetTopicRule("arn-rule")
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iot:us-west-2:111100001111:rule/arn-rule", r.ARN)
}

func TestRefinement3_GetTopicRule_EnabledByDefault(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	err := b.CreateTopicRule(&iot.CreateTopicRuleInput{
		RuleName: "enabled-rule",
		TopicRulePayload: &iot.TopicRulePayload{
			SQL:          "SELECT * FROM 'topic'",
			RuleDisabled: false,
			Actions:      []iot.RuleAction{},
		},
	})
	require.NoError(t, err)

	r, err := b.GetTopicRule("enabled-rule")
	require.NoError(t, err)
	assert.True(t, r.Enabled)
}

func TestRefinement3_DisableTopicRule_SetsEnabledFalse(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	err := b.CreateTopicRule(&iot.CreateTopicRuleInput{
		RuleName: "dis-rule",
		TopicRulePayload: &iot.TopicRulePayload{
			SQL:     "SELECT * FROM 'topic'",
			Actions: []iot.RuleAction{},
		},
	})
	require.NoError(t, err)

	err = b.DisableTopicRule("dis-rule")
	require.NoError(t, err)

	r, err := b.GetTopicRule("dis-rule")
	require.NoError(t, err)
	assert.False(t, r.Enabled)
}

func TestRefinement3_EnableTopicRule_SetsEnabledTrue(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	err := b.CreateTopicRule(&iot.CreateTopicRuleInput{
		RuleName: "en-rule",
		TopicRulePayload: &iot.TopicRulePayload{
			SQL:          "SELECT * FROM 'topic'",
			RuleDisabled: true,
			Actions:      []iot.RuleAction{},
		},
	})
	require.NoError(t, err)

	r, err := b.GetTopicRule("en-rule")
	require.NoError(t, err)
	assert.False(t, r.Enabled)

	err = b.EnableTopicRule("en-rule")
	require.NoError(t, err)

	r, err = b.GetTopicRule("en-rule")
	require.NoError(t, err)
	assert.True(t, r.Enabled)
}

// -----------------------------------------------------------------------
// Additional coverage: GetPolicy returns correct defaultVersionId after CreatePolicyVersion
// -----------------------------------------------------------------------

func TestRefinement3_GetPolicy_DefaultVersionID_AfterMultipleVersions(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreatePolicy(&iot.CreatePolicyInput{
		PolicyName:     "multi-default-policy",
		PolicyDocument: `{"v": 1}`,
	})
	require.NoError(t, err)

	// Add versions 2 and 3 without setting as default
	for range 2 {
		_, err = b.CreatePolicyVersion(&iot.CreatePolicyVersionInput{
			PolicyName:     "multi-default-policy",
			PolicyDocument: `{}`,
			SetAsDefault:   false,
		})
		require.NoError(t, err)
	}

	// Version 1 should still be default
	out, err := b.GetPolicy("multi-default-policy")
	require.NoError(t, err)
	assert.Equal(t, "1", out.DefaultVersionID)

	// Add version 4 and set as default
	_, err = b.CreatePolicyVersion(&iot.CreatePolicyVersionInput{
		PolicyName:     "multi-default-policy",
		PolicyDocument: `{"v": 4}`,
		SetAsDefault:   true,
	})
	require.NoError(t, err)

	out, err = b.GetPolicy("multi-default-policy")
	require.NoError(t, err)
	assert.Equal(t, "4", out.DefaultVersionID)
}

// -----------------------------------------------------------------------
// Additional coverage: ThingGroup member operations
// -----------------------------------------------------------------------

func TestRefinement3_ListThingsInThingGroup_EmptyGroup(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateThingGroup(&iot.CreateThingGroupInput{ThingGroupName: "empty-group"})
	require.NoError(t, err)

	members, err := b.ListThingsInThingGroup(&iot.ListThingsInThingGroupInput{ThingGroupName: "empty-group"})
	require.NoError(t, err)
	assert.Empty(t, members)
}

func TestRefinement3_AddThingToGroup_ListsCorrectly(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateThingGroup(&iot.CreateThingGroupInput{ThingGroupName: "member-group"})
	require.NoError(t, err)

	_, err = b.CreateThing(&iot.CreateThingInput{ThingName: "group-member-thing"})
	require.NoError(t, err)

	err = b.AddThingToThingGroup(&iot.AddThingToThingGroupInput{
		ThingGroupName: "member-group",
		ThingName:      "group-member-thing",
	})
	require.NoError(t, err)

	members, err := b.ListThingsInThingGroup(&iot.ListThingsInThingGroupInput{ThingGroupName: "member-group"})
	require.NoError(t, err)
	assert.Contains(t, members, "group-member-thing")
}

func TestRefinement3_RemoveThingFromGroup_UpdatesMembership(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateThingGroup(&iot.CreateThingGroupInput{ThingGroupName: "remove-test-group"})
	require.NoError(t, err)

	_, err = b.CreateThing(&iot.CreateThingInput{ThingName: "remove-member-thing"})
	require.NoError(t, err)

	err = b.AddThingToThingGroup(&iot.AddThingToThingGroupInput{
		ThingGroupName: "remove-test-group",
		ThingName:      "remove-member-thing",
	})
	require.NoError(t, err)

	err = b.RemoveThingFromThingGroup(&iot.RemoveThingFromThingGroupInput{
		ThingGroupName: "remove-test-group",
		ThingName:      "remove-member-thing",
	})
	require.NoError(t, err)

	members, err := b.ListThingsInThingGroup(&iot.ListThingsInThingGroupInput{ThingGroupName: "remove-test-group"})
	require.NoError(t, err)
	assert.NotContains(t, members, "remove-member-thing")
}

// -----------------------------------------------------------------------
// Additional coverage: DescribeThingType not found
// -----------------------------------------------------------------------

func TestRefinement3_DescribeThingType_NotFound_Error(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.DescribeThingType("ghost-type")
	require.ErrorIs(t, err, iot.ErrThingTypeNotFound)
}

// -----------------------------------------------------------------------
// Additional coverage: DescribeThingGroup not found
// -----------------------------------------------------------------------

func TestRefinement3_DescribeThingGroup_NotFound_Error(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.DescribeThingGroup("ghost-group")
	require.ErrorIs(t, err, iot.ErrThingGroupNotFound)
}

// -----------------------------------------------------------------------
// Additional coverage: Reset idempotency
// -----------------------------------------------------------------------

func TestRefinement3_Reset_MultipleTimesIsIdempotent(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateThing(&iot.CreateThingInput{ThingName: "multi-reset-thing"})
	require.NoError(t, err)

	b.Reset()
	b.Reset()
	b.Reset()

	assert.Empty(t, b.ListThings())
}

// -----------------------------------------------------------------------
// Additional coverage: DescribeEndpoint returns address
// -----------------------------------------------------------------------

func TestRefinement3_DescribeEndpoint_ReturnsAddress(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	out, err := b.DescribeEndpoint("")
	require.NoError(t, err)
	assert.NotEmpty(t, out.EndpointAddress)
}

func TestRefinement3_DescribeEndpoint_HTTP_ReturnsAddress(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	var out map[string]string
	code := r3JSON(t, h, http.MethodGet, "/endpoint", nil, &out)
	require.Equal(t, http.StatusOK, code)
	assert.NotEmpty(t, out["endpointAddress"])
}

// -----------------------------------------------------------------------
// Additional coverage: ThingPrincipal attachment
// -----------------------------------------------------------------------

func TestRefinement3_AttachThingPrincipal_Stored(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateThing(&iot.CreateThingInput{ThingName: "principal-thing"})
	require.NoError(t, err)

	err = b.AttachThingPrincipal(&iot.AttachThingPrincipalInput{
		ThingName: "principal-thing",
		Principal: "arn:aws:iot:us-east-1:123456789012:cert/" + strings.Repeat("a", 64),
	})
	require.NoError(t, err)

	principals, err := b.ListThingPrincipals("principal-thing")
	require.NoError(t, err)
	require.Len(t, principals, 1)
}

func TestRefinement3_ListThingPrincipals_ThingNotFound_Error(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.ListThingPrincipals("ghost-thing")
	require.ErrorIs(t, err, iot.ErrThingNotFound)
}
