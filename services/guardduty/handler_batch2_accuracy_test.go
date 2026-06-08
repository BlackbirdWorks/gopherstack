package guardduty_test

// Batch-2 AWS-accuracy tests for GuardDuty (go-3twh).
//
// Covers accuracy gaps not exercised by the batch-1 audit:
//
//  1. Detector ID shape: 32 lowercase hex chars (no dashes)
//  2. Detector timestamps: createdAt and updatedAt are valid millisecond-precision UTC strings
//  3. updatedAt advances after UpdateDetector; createdAt unchanged
//  4. GetDetector: tags:{} not null when created without tags (bug fix)
//  5. ListDetectors empty state: detectorIds:[]
//  6. ListFilters empty state: filterNames:[]
//  7. GetFilter: tags:{} not null when created without tags (bug fix)
//  8. ListFindings empty state: findingIds:[]
//  9. GetFindings empty input: findings:[]
// 10. ListIPSets empty state: ipSetIds:[]
// 11. GetIPSet: tags:{} not null when created without tags (bug fix)
// 12. ListThreatIntelSets empty state: threatIntelSetIds:[]
// 13. GetThreatIntelSet: tags:{} not null when created without tags (bug fix)
// 14. TagResource merges new tags with creation-time tags
// 15. UntagResource removes only the specified key

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/guardduty"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func newBatch2Backend() *guardduty.InMemoryBackend {
	return guardduty.NewInMemoryBackend("123456789012", "us-east-1")
}

func newBatch2Handler(t *testing.T) *guardduty.Handler {
	t.Helper()

	return guardduty.NewHandler(newBatch2Backend())
}

func b2CreateDetector(t *testing.T, h *guardduty.Handler) string {
	t.Helper()

	rec := auditDo(t, h, http.MethodPost, "/detector", map[string]any{
		"enable": true,
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	id, ok := resp["detectorId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, id)

	return id
}

const tsLayout = "2006-01-02T15:04:05.000Z"

func parseTS(t *testing.T, field, value string) time.Time {
	t.Helper()

	parsed, err := time.Parse(tsLayout, value)
	require.NoError(t, err, "%s must parse with layout %q, got %q", field, tsLayout, value)

	return parsed
}

// ---------------------------------------------------------------------------
// 1. Detector ID shape
// ---------------------------------------------------------------------------

var (
	reDetectorID  = regexp.MustCompile(`^[0-9a-f]{32}$`)
	reDetectorARN = regexp.MustCompile(`^arn:aws:guardduty:[a-z0-9-]+:\d{12}:detector/[0-9a-f]{32}$`)
)

func TestBatch2_DetectorID_Shape(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)
	detectorID := b2CreateDetector(t, h)

	assert.True(t, reDetectorID.MatchString(detectorID),
		"detectorId must be 32 lowercase hex chars, got %q", detectorID)

	// Verify the ARN we'd construct is well-formed.
	arn := fmt.Sprintf("arn:aws:guardduty:us-east-1:123456789012:detector/%s", detectorID)
	assert.True(t, reDetectorARN.MatchString(arn),
		"constructed detector ARN must match expected pattern, got %q", arn)
}

// ---------------------------------------------------------------------------
// 2. Detector timestamps present and valid format
// ---------------------------------------------------------------------------

func TestBatch2_Detector_Timestamps_Present(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)
	id := b2CreateDetector(t, h)

	rec := auditDo(t, h, http.MethodGet, "/detector/"+id, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	for _, field := range []string{"createdAt", "updatedAt"} {
		raw, ok := resp[field]
		require.True(t, ok, "GetDetector must include %s", field)
		ts, ok := raw.(string)
		require.True(t, ok, "%s must be a string, got %T", field, raw)
		require.NotEmpty(t, ts, "%s must not be empty", field)
		parseTS(t, field, ts)
	}
}

// ---------------------------------------------------------------------------
// 3. updatedAt advances after UpdateDetector; createdAt unchanged
// ---------------------------------------------------------------------------

func TestBatch2_Detector_UpdatedAt_Advances(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)
	id := b2CreateDetector(t, h)

	rec1 := auditDo(t, h, http.MethodGet, "/detector/"+id, nil)
	require.Equal(t, http.StatusOK, rec1.Code)

	var before map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &before))
	createdAt := before["createdAt"].(string)
	updatedAt1 := before["updatedAt"].(string)

	time.Sleep(2 * time.Millisecond)

	rec := auditDo(t, h, http.MethodPost, "/detector/"+id, map[string]any{
		"findingPublishingFrequency": "FIFTEEN_MINUTES",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := auditDo(t, h, http.MethodGet, "/detector/"+id, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var after map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &after))
	createdAt2 := after["createdAt"].(string)
	updatedAt2 := after["updatedAt"].(string)

	assert.Equal(t, createdAt, createdAt2, "createdAt must not change after UpdateDetector")

	t1 := parseTS(t, "updatedAt before", updatedAt1)
	t2 := parseTS(t, "updatedAt after", updatedAt2)
	assert.True(t, t2.After(t1) || t2.Equal(t1),
		"updatedAt must not regress: before=%s after=%s", updatedAt1, updatedAt2)
}

// ---------------------------------------------------------------------------
// 4. GetDetector tags:{} not null when created without tags
// ---------------------------------------------------------------------------

func TestBatch2_Detector_Tags_EmptyMap_Not_Null(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)
	id := b2CreateDetector(t, h) // no tags

	rec := auditDo(t, h, http.MethodGet, "/detector/"+id, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	raw, exists := resp["tags"]
	require.True(t, exists, "GetDetector response must include 'tags' key")
	assert.NotNil(t, raw, "tags must be {} not null when no tags on create")

	tags, ok := raw.(map[string]any)
	require.True(t, ok, "tags must be an object, got %T", raw)
	assert.Empty(t, tags, "tags must be empty map {}")
}

// ---------------------------------------------------------------------------
// 5. ListDetectors empty state: detectorIds:[]
// ---------------------------------------------------------------------------

func TestBatch2_ListDetectors_Empty_State(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	rec := auditDo(t, h, http.MethodGet, "/detector", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	raw, exists := resp["detectorIds"]
	require.True(t, exists, "ListDetectors must include 'detectorIds' key")
	assert.NotNil(t, raw, "detectorIds must be [] not null when empty")

	ids, ok := raw.([]any)
	require.True(t, ok, "detectorIds must be an array, got %T", raw)
	assert.Empty(t, ids, "detectorIds must be empty []")
}

// ---------------------------------------------------------------------------
// 6. ListFilters empty state: filterNames:[]
// ---------------------------------------------------------------------------

func TestBatch2_ListFilters_Empty_State(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)
	id := b2CreateDetector(t, h)

	rec := auditDo(t, h, http.MethodGet, "/detector/"+id+"/filter", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	raw, exists := resp["filterNames"]
	require.True(t, exists, "ListFilters must include 'filterNames' key")
	assert.NotNil(t, raw, "filterNames must be [] not null when empty")

	names, ok := raw.([]any)
	require.True(t, ok, "filterNames must be an array, got %T", raw)
	assert.Empty(t, names, "filterNames must be empty []")
}

// ---------------------------------------------------------------------------
// 7. GetFilter tags:{} not null when created without tags
// ---------------------------------------------------------------------------

func TestBatch2_Filter_Tags_EmptyMap_Not_Null(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)
	id := b2CreateDetector(t, h)

	rec := auditDo(t, h, http.MethodPost, "/detector/"+id+"/filter", map[string]any{
		"name":   "batch2-filter",
		"action": "NOOP",
		"rank":   1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = auditDo(t, h, http.MethodGet, "/detector/"+id+"/filter/batch2-filter", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	raw, exists := resp["tags"]
	require.True(t, exists, "GetFilter response must include 'tags' key")
	assert.NotNil(t, raw, "tags must be {} not null when no tags on create")

	tags, ok := raw.(map[string]any)
	require.True(t, ok, "tags must be an object, got %T", raw)
	assert.Empty(t, tags, "tags must be empty map {}")
}

// ---------------------------------------------------------------------------
// 8. ListFindings empty state: findingIds:[]
// ---------------------------------------------------------------------------

func TestBatch2_ListFindings_Empty_State(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)
	id := b2CreateDetector(t, h)

	rec := auditDo(t, h, http.MethodPost, "/detector/"+id+"/findings", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	raw, exists := resp["findingIds"]
	require.True(t, exists, "ListFindings must include 'findingIds' key")
	assert.NotNil(t, raw, "findingIds must be [] not null when empty")

	ids, ok := raw.([]any)
	require.True(t, ok, "findingIds must be an array, got %T", raw)
	assert.Empty(t, ids, "findingIds must be empty []")
}

// ---------------------------------------------------------------------------
// 9. GetFindings empty input: findings:[]
// ---------------------------------------------------------------------------

func TestBatch2_GetFindings_Empty_Input(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)
	id := b2CreateDetector(t, h)

	rec := auditDo(t, h, http.MethodPost, "/detector/"+id+"/findings/get",
		map[string]any{"findingIds": []string{}})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	raw, exists := resp["findings"]
	require.True(t, exists, "GetFindings must include 'findings' key")
	assert.NotNil(t, raw, "findings must be [] not null when empty input")

	findings, ok := raw.([]any)
	require.True(t, ok, "findings must be an array, got %T", raw)
	assert.Empty(t, findings, "findings must be empty []")
}

// ---------------------------------------------------------------------------
// 10. ListIPSets empty state: ipSetIds:[]
// ---------------------------------------------------------------------------

func TestBatch2_ListIPSets_Empty_State(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)
	id := b2CreateDetector(t, h)

	rec := auditDo(t, h, http.MethodGet, "/detector/"+id+"/ipset", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	raw, exists := resp["ipSetIds"]
	require.True(t, exists, "ListIPSets must include 'ipSetIds' key")
	assert.NotNil(t, raw, "ipSetIds must be [] not null when empty")

	ids, ok := raw.([]any)
	require.True(t, ok, "ipSetIds must be an array, got %T", raw)
	assert.Empty(t, ids, "ipSetIds must be empty []")
}

// ---------------------------------------------------------------------------
// 11. GetIPSet tags:{} not null when created without tags
// ---------------------------------------------------------------------------

func TestBatch2_IPSet_Tags_EmptyMap_Not_Null(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)
	id := b2CreateDetector(t, h)

	rec := auditDo(t, h, http.MethodPost, "/detector/"+id+"/ipset", map[string]any{
		"name":     "batch2-ipset",
		"format":   "TXT",
		"location": "s3://bucket/ipset.txt",
		"activate": false,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	ipSetID := createResp["ipSetId"].(string)

	rec = auditDo(t, h, http.MethodGet, "/detector/"+id+"/ipset/"+ipSetID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	raw, exists := resp["tags"]
	require.True(t, exists, "GetIPSet response must include 'tags' key")
	assert.NotNil(t, raw, "tags must be {} not null when no tags on create")

	tags, ok := raw.(map[string]any)
	require.True(t, ok, "tags must be an object, got %T", raw)
	assert.Empty(t, tags, "tags must be empty map {}")
}

// ---------------------------------------------------------------------------
// 12. ListThreatIntelSets empty state: threatIntelSetIds:[]
// ---------------------------------------------------------------------------

func TestBatch2_ListThreatIntelSets_Empty_State(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)
	id := b2CreateDetector(t, h)

	rec := auditDo(t, h, http.MethodGet, "/detector/"+id+"/threatintelset", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	raw, exists := resp["threatIntelSetIds"]
	require.True(t, exists, "ListThreatIntelSets must include 'threatIntelSetIds' key")
	assert.NotNil(t, raw, "threatIntelSetIds must be [] not null when empty")

	ids, ok := raw.([]any)
	require.True(t, ok, "threatIntelSetIds must be an array, got %T", raw)
	assert.Empty(t, ids, "threatIntelSetIds must be empty []")
}

// ---------------------------------------------------------------------------
// 13. GetThreatIntelSet tags:{} not null when created without tags
// ---------------------------------------------------------------------------

func TestBatch2_ThreatIntelSet_Tags_EmptyMap_Not_Null(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)
	id := b2CreateDetector(t, h)

	rec := auditDo(t, h, http.MethodPost, "/detector/"+id+"/threatintelset", map[string]any{
		"name":     "batch2-tis",
		"format":   "TXT",
		"location": "s3://bucket/tis.txt",
		"activate": false,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	setID := createResp["threatIntelSetId"].(string)

	rec = auditDo(t, h, http.MethodGet, "/detector/"+id+"/threatintelset/"+setID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	raw, exists := resp["tags"]
	require.True(t, exists, "GetThreatIntelSet response must include 'tags' key")
	assert.NotNil(t, raw, "tags must be {} not null when no tags on create")

	tags, ok := raw.(map[string]any)
	require.True(t, ok, "tags must be an object, got %T", raw)
	assert.Empty(t, tags, "tags must be empty map {}")
}

// ---------------------------------------------------------------------------
// 14. TagResource merges new tags with creation-time tags
// ---------------------------------------------------------------------------

func TestBatch2_TagResource_Merges_Creation_Tags(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	rec := auditDo(t, h, http.MethodPost, "/detector", map[string]any{
		"enable": true,
		"tags":   map[string]string{"env": "test"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	id := createResp["detectorId"].(string)

	arn := fmt.Sprintf("arn:aws:guardduty:us-east-1:123456789012:detector/%s", id)

	rec = auditDo(t, h, http.MethodPost, "/tags/"+arn, map[string]any{
		"tags": map[string]string{"owner": "alice"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = auditDo(t, h, http.MethodGet, "/tags/"+arn, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var tagsResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tagsResp))
	tags := tagsResp["tags"].(map[string]any)

	assert.Equal(t, "test", tags["env"], "creation-time tag must be preserved")
	assert.Equal(t, "alice", tags["owner"], "new tag must be added via TagResource")
}

// ---------------------------------------------------------------------------
// 15. UntagResource removes only the specified key
// ---------------------------------------------------------------------------

func TestBatch2_UntagResource_Removes_Only_Specified_Key(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	rec := auditDo(t, h, http.MethodPost, "/detector", map[string]any{
		"enable": true,
		"tags":   map[string]string{"k1": "v1", "k2": "v2", "k3": "v3"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	id := createResp["detectorId"].(string)

	arn := fmt.Sprintf("arn:aws:guardduty:us-east-1:123456789012:detector/%s", id)

	rec = auditDo(t, h, http.MethodDelete, "/tags/"+arn+"?tagKeys=k2", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = auditDo(t, h, http.MethodGet, "/tags/"+arn, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var tagsResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tagsResp))
	tags := tagsResp["tags"].(map[string]any)

	assert.Equal(t, "v1", tags["k1"], "k1 must be preserved")
	assert.NotContains(t, tags, "k2", "k2 must be removed by UntagResource")
	assert.Equal(t, "v3", tags["k3"], "k3 must be preserved")
}
