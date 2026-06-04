package macie2_test

// Batch-2 AWS-accuracy tests for Macie2 (go-dl1m).
//
// Covers accuracy gaps not exercised by the batch-1 audit:
//
//  1. AllowList ARN shape: arn:aws:macie2:{region}:{account}:allow-list/{uuid}
//  2. Session timestamps: createdAt and updatedAt are RFC3339
//  3. Session updatedAt advances after UpdateMacieSession
//  4. Creation-time tags visible via ListTagsForResource
//  5. GetCustomDataIdentifier: no "deleted" field in response
//  6. ListAllowLists empty state: allowLists is [] not null
//  7. ListCustomDataIdentifiers empty state: items is [] not null
//  8. ListFindingsFilters empty state: findingsFilterListItems is [] not null
//  9. ListFindings empty state: findingIds is [] not null
// 10. GetFindings empty findingIds: findings is [] not null
// 11. UntagResource removes only the specified key
// 12. TagResource merges new tags with existing creation-time tags

import (
	"encoding/json"
	"net/http"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// 1. AllowList ARN shape
// ---------------------------------------------------------------------------

var reAllowListARN = regexp.MustCompile(
	`^arn:aws:macie2:[a-z0-9-]+:\d{12}:allow-list/[0-9a-f-]{36}$`,
)

func TestBatch2_AllowListARN_Shape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/allow-lists", map[string]any{
		"clientToken": "tok-arn",
		"name":        "arn-test",
		"criteria":    map[string]any{"regex": "\\d+"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	arn := resp["arn"]
	require.NotEmpty(t, arn, "CreateAllowList must return arn")
	assert.True(t, reAllowListARN.MatchString(arn),
		"AllowList ARN must match arn:aws:macie2:{region}:{account}:allow-list/{uuid}, got %q", arn)
}

// ---------------------------------------------------------------------------
// 2 & 3. Session timestamps present and updatedAt advances
// ---------------------------------------------------------------------------

func TestBatch2_Session_Timestamps_Present(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/macie", map[string]string{
		"findingPublishingFrequency": "ONE_HOUR",
	})

	rec := doRequest(t, h, http.MethodGet, "/macie", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	for _, field := range []string{"createdAt", "updatedAt"} {
		raw, ok := resp[field]
		require.True(t, ok, "GetMacieSession must include %s", field)
		ts, ok := raw.(string)
		require.True(t, ok, "%s must be a string", field)
		_, err := time.Parse(time.RFC3339Nano, ts)
		assert.NoError(t, err, "%s must be RFC3339, got %q", field, ts)
	}
}

func TestBatch2_Session_UpdatedAt_Advances(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/macie", nil)

	rec1 := doRequest(t, h, http.MethodGet, "/macie", nil)
	require.Equal(t, http.StatusOK, rec1.Code)

	var before map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &before))
	createdAt := before["createdAt"].(string)

	time.Sleep(2 * time.Millisecond)

	doRequest(t, h, http.MethodPatch, "/macie",
		map[string]string{"findingPublishingFrequency": "SIX_HOURS"})

	rec2 := doRequest(t, h, http.MethodGet, "/macie", nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var after map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &after))

	assert.Equal(t, createdAt, after["createdAt"].(string), "createdAt must not change after update")
	assert.NotEqual(t, after["createdAt"], after["updatedAt"],
		"updatedAt must differ from createdAt after UpdateMacieSession")
}

// ---------------------------------------------------------------------------
// 4. Creation-time tags visible via ListTagsForResource
// ---------------------------------------------------------------------------

func TestBatch2_CreationTags_VisibleViaTagAPI(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/allow-lists", map[string]any{
		"clientToken": "tag-create",
		"name":        "tagged-at-create",
		"criteria":    map[string]any{"regex": "\\w+"},
		"tags":        map[string]string{"env": "test", "team": "security"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var created map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	arn := created["arn"]

	rec2 := doRequest(t, h, http.MethodGet, "/tags/"+arn, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var tagResp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &tagResp))

	tags, ok := tagResp["tags"].(map[string]any)
	require.True(t, ok, "tags must be an object")
	assert.Equal(t, "test", tags["env"], "creation-time tag env must be visible via ListTagsForResource")
	assert.Equal(t, "security", tags["team"], "creation-time tag team must be visible via ListTagsForResource")
}

// ---------------------------------------------------------------------------
// 5. GetCustomDataIdentifier: no "deleted" field in response
// ---------------------------------------------------------------------------

func TestBatch2_CDI_NoDeletedField(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/custom-data-identifiers", map[string]any{
		"name":  "no-deleted-field",
		"regex": `\d+`,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var created map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	id := created["customDataIdentifierId"]

	rec2 := doRequest(t, h, http.MethodGet, "/custom-data-identifiers/"+id, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
	_, hasDeleted := resp["deleted"]
	assert.False(t, hasDeleted, "GetCustomDataIdentifier must not include a 'deleted' field in the response")
}

// ---------------------------------------------------------------------------
// 6-10. Empty-state: arrays must be [] not null
// ---------------------------------------------------------------------------

func TestBatch2_ListAllowLists_EmptyNotNull(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/allow-lists", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	v, ok := resp["allowLists"]
	require.True(t, ok, "response must contain allowLists key")
	assert.NotNil(t, v, "allowLists must be [] not null when empty")

	arr, isArr := v.([]any)
	require.True(t, isArr, "allowLists must be an array")
	assert.Empty(t, arr)
}

func TestBatch2_ListCustomDataIDs_EmptyNotNull(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/custom-data-identifiers/list", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	v, ok := resp["items"]
	require.True(t, ok, "response must contain items key")
	assert.NotNil(t, v, "items must be [] not null when empty")

	arr, isArr := v.([]any)
	require.True(t, isArr, "items must be an array")
	assert.Empty(t, arr)
}

func TestBatch2_ListFindingsFilters_EmptyNotNull(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/findingsfilters", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	v, ok := resp["findingsFilterListItems"]
	require.True(t, ok, "response must contain findingsFilterListItems key")
	assert.NotNil(t, v, "findingsFilterListItems must be [] not null when empty")

	arr, isArr := v.([]any)
	require.True(t, isArr, "findingsFilterListItems must be an array")
	assert.Empty(t, arr)
}

func TestBatch2_ListFindings_EmptyNotNull(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/findings", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	v, ok := resp["findingIds"]
	require.True(t, ok, "response must contain findingIds key")
	assert.NotNil(t, v, "findingIds must be [] not null when empty")

	arr, isArr := v.([]any)
	require.True(t, isArr, "findingIds must be an array")
	assert.Empty(t, arr)
}

func TestBatch2_GetFindings_EmptyInput_NotNull(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/findings/describe",
		map[string]any{"findingIds": []string{}})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	v, ok := resp["findings"]
	require.True(t, ok, "response must contain findings key")
	assert.NotNil(t, v, "findings must be [] not null for empty findingIds input")
}

// ---------------------------------------------------------------------------
// 11. UntagResource removes only the specified key
// ---------------------------------------------------------------------------

func TestBatch2_UntagResource_RemovesSpecificKey(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/allow-lists", map[string]any{
		"clientToken": "untag-test",
		"name":        "untag-list",
		"criteria":    map[string]any{"regex": "\\d+"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var created map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	arn := created["arn"]

	doRequest(t, h, http.MethodPost, "/tags/"+arn,
		map[string]any{"tags": map[string]string{"keep": "yes", "remove": "me"}})

	doRequest(t, h, http.MethodDelete, "/tags/"+arn+"?tagKeys=remove", nil)

	rec2 := doRequest(t, h, http.MethodGet, "/tags/"+arn, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var tagResp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &tagResp))
	tags, ok := tagResp["tags"].(map[string]any)
	require.True(t, ok)

	assert.Equal(t, "yes", tags["keep"], "key 'keep' must remain after untagging 'remove'")
	_, hasRemove := tags["remove"]
	assert.False(t, hasRemove, "key 'remove' must be absent after UntagResource")
}

// ---------------------------------------------------------------------------
// 12. TagResource merges with creation-time tags
// ---------------------------------------------------------------------------

func TestBatch2_TagResource_MergesWithCreationTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/allow-lists", map[string]any{
		"clientToken": "merge-test",
		"name":        "merge-list",
		"criteria":    map[string]any{"regex": "\\d+"},
		"tags":        map[string]string{"origin": "create"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var created map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	arn := created["arn"]

	doRequest(t, h, http.MethodPost, "/tags/"+arn,
		map[string]any{"tags": map[string]string{"added": "later"}})

	rec2 := doRequest(t, h, http.MethodGet, "/tags/"+arn, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var tagResp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &tagResp))
	tags, ok := tagResp["tags"].(map[string]any)
	require.True(t, ok)

	assert.Equal(t, "create", tags["origin"], "creation-time tag must be present after TagResource call")
	assert.Equal(t, "later", tags["added"], "explicitly-added tag must be present")
}
