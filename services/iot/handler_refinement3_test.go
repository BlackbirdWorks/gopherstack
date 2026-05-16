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

	for i := 0; i < 10; i++ {
		var out map[string]string
		code := r3JSON(t, h, http.MethodPost, "/certificates", map[string]any{
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

	for i := 0; i < 20; i++ {
		cert, err := b.RegisterCertificate(&iot.RegisterCertificateInput{
			CertificatePem: "PEM",
			Status:         "ACTIVE",
		})
		require.NoError(t, err)
		assert.False(t, seen[cert.CertificateID], "duplicate cert ID at iteration %d: %s", i, cert.CertificateID)
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

	assert.True(t, !after.LastModifiedAt.Before(before.LastModifiedAt),
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
	assert.Equal(t, float64(2), v)
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
		assert.Equal(t, expectedVersion, out["version"])
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
	assert.True(t, strings.Contains(tt.ThingTypeID, "-"), "ThingTypeID should be UUID-formatted")
}

func TestRefinement3_ThingType_IDsAreUnique(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	seen := make(map[string]bool)

	for i := 0; i < 5; i++ {
		name := "unique-type-" + string(rune('a'+i))
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
	code := r3JSON(t, h, http.MethodPost, "/certificates", map[string]any{
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
	code := r3JSON(t, h, http.MethodPost, "/certificates", map[string]any{
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
	r3JSON(t, h, http.MethodPost, "/certificates", map[string]any{
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
	r3Req(t, h, http.MethodPost, "/certificates", map[string]any{
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
	require.NoError(t, err)
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
	require.NoError(t, err)
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
	assert.True(t, !updated.LastModifiedAt.Before(initial),
		"LastModifiedAt should be updated after UpdateCertificate")
}

func TestRefinement3_UpdateCertificate_InvalidStatus_HTTP400(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	var createOut map[string]string
	r3JSON(t, h, http.MethodPost, "/certificates", map[string]any{
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

func TestRefinement3_DeletePolicyVersion_HTTP_DefaultVersion_Returns409(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	r3Req(t, h, http.MethodPost, "/policies/http-default-del", map[string]any{
		"policyDocument": `{}`,
	})

	var out map[string]string
	code := r3JSON(t, h, http.MethodDelete, "/policies/http-default-del/version/1", nil, &out)
	assert.Equal(t, http.StatusConflict, code)
	assert.Equal(t, "DeleteConflictException", out["__type"])
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
	assert.Equal(t, doc, pv.PolicyDocument)
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
	assert.Len(t, b.ListThingTypes(), 0)
}

func TestRefinement3_Reset_ClearsCertificates(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateCertificateFromCsr(&iot.CreateCertificateFromCsrInput{SetAsActive: true})
	require.NoError(t, err)

	b.Reset()
	assert.Len(t, b.ListCertificates(), 0)
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
	assert.Equal(t, doc, out["policyDocument"])
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
