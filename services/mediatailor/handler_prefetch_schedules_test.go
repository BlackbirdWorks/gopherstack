package mediatailor_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrefetchSchedule_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestPlaybackConfig(t, h, "pc1")

	tests := []struct {
		check    func(t *testing.T, rec *httptest.ResponseRecorder)
		name     string
		wantCode int
	}{
		{
			name:     "create prefetch schedule",
			wantCode: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "sched1", resp["Name"])
				assert.Equal(t, "pc1", resp["PlaybackConfigurationName"])
				assert.Contains(t, resp["Arn"], ":mediatailor:")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h2 := newTestHandler(t)
			createTestPlaybackConfig(t, h2, "pc1")

			rec := doRequest(t, h2, http.MethodPost, "/prefetchSchedule/pc1/sched1", nil)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec)
			}
		})
	}

	// get
	doRequest(t, h, http.MethodPost, "/prefetchSchedule/pc1/sched1", nil)
	rec := doRequest(t, h, http.MethodGet, "/prefetchSchedule/pc1/sched1", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "sched1", resp["Name"])

	// list
	rec = doRequest(t, h, http.MethodPost, "/prefetchSchedule/pc1", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	items, _ := listResp["Items"].([]any)
	assert.Len(t, items, 1)

	// delete
	rec = doRequest(t, h, http.MethodDelete, "/prefetchSchedule/pc1/sched1", nil)
	require.Equal(t, http.StatusNoContent, rec.Code)

	// get after delete returns 404
	rec = doRequest(t, h, http.MethodGet, "/prefetchSchedule/pc1/sched1", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestPrefetchSchedule_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		method   string
		path     string
		name     string
		wantCode int
	}{
		{
			name:     "create under missing playback config returns 404",
			method:   http.MethodPost,
			path:     "/prefetchSchedule/nope/sched1",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "get missing schedule returns 404",
			method:   http.MethodGet,
			path:     "/prefetchSchedule/pc1/missing",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "delete missing schedule returns 404",
			method:   http.MethodDelete,
			path:     "/prefetchSchedule/pc1/missing",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createTestPlaybackConfig(t, h, "pc1")

			rec := doRequest(t, h, tc.method, tc.path, nil)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestListPrefetchSchedules_WithItems(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestPlaybackConfig(t, h, "pc1")

	for i := range 2 {
		name := "sched-" + string(rune('a'+i))
		doRequest(t, h, http.MethodPost, "/prefetchSchedule/pc1/"+name, nil)
	}

	rec := doRequest(t, h, http.MethodPost, "/prefetchSchedule/pc1", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items, _ := resp["Items"].([]any)
	assert.Len(t, items, 2)
}

// TestListPrefetchSchedules_PostWithBodyPagination verifies
// ListPrefetchSchedules is routed as POST (not GET) on the bare
// /prefetchSchedule/{PlaybackConfigurationName} path, and that MaxResults/
// NextToken are read from the JSON request body rather than the query
// string — confirmed against aws-sdk-go-v2's serializer for
// ListPrefetchSchedulesInput.
func TestListPrefetchSchedules_PostWithBodyPagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPut, "/playbackConfiguration", map[string]any{"Name": "pc1"})

	for _, name := range []string{"sched-a", "sched-b", "sched-c"} {
		doRequest(t, h, http.MethodPost, "/prefetchSchedule/pc1/"+name, nil)
	}

	// A GET on the list path must not be routed to ListPrefetchSchedules.
	getRec := doRequest(t, h, http.MethodGet, "/prefetchSchedule/pc1", nil)
	assert.NotEqual(t, http.StatusOK, getRec.Code, "GET must not serve ListPrefetchSchedules")

	rec := doRequest(t, h, http.MethodPost, "/prefetchSchedule/pc1", map[string]any{
		"MaxResults": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items, _ := resp["Items"].([]any)
	assert.Len(t, items, 1, "body MaxResults must limit the page size")
	assert.NotEmpty(t, resp["NextToken"], "a NextToken must be returned when more pages remain")
}

// TestCreatePrefetchSchedule_RetrievalAndConsumption verifies
// CreatePrefetchSchedule stores and returns Retrieval/Consumption.
func TestCreatePrefetchSchedule_RetrievalAndConsumption(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestPlaybackConfig(t, h, "pc1")

	// StartTime/EndTime are unixTimestamp shapes on the wire (JSON number of
	// seconds since epoch), not RFC3339 strings — confirmed against
	// aws-sdk-go-v2's (de)serializers and botocore's service-2.json.
	const (
		retrievalStart  float64 = 1_767_225_600 // 2026-01-01T00:00:00Z
		retrievalEnd    float64 = 1_767_229_200 // 2026-01-01T01:00:00Z
		consumptionEnd  float64 = 1_767_232_800 // 2026-01-01T02:00:00Z
		consumptionSame         = retrievalEnd
	)

	rec := doRequest(t, h, http.MethodPost, "/prefetchSchedule/pc1/sched1", map[string]any{
		"Retrieval": map[string]any{
			"StartTime": retrievalStart,
			"EndTime":   retrievalEnd,
		},
		"Consumption": map[string]any{
			"StartTime": consumptionSame,
			"EndTime":   consumptionEnd,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	retrieval, ok := resp["Retrieval"].(map[string]any)
	require.True(t, ok, "Retrieval must be present in response")
	assert.InEpsilon(t, retrievalStart, retrieval["StartTime"], 0.001)
	assert.InEpsilon(t, retrievalEnd, retrieval["EndTime"], 0.001)

	consumption, ok := resp["Consumption"].(map[string]any)
	require.True(t, ok, "Consumption must be present in response")
	assert.InEpsilon(t, consumptionSame, consumption["StartTime"], 0.001)
	assert.InEpsilon(t, consumptionEnd, consumption["EndTime"], 0.001)
}

// TestListPrefetchSchedules_FiltersByScheduleTypeAndStreamID verifies
// ListPrefetchSchedules honors its ScheduleType/StreamId request filters
// (deferred item: routing + pagination were fixed in a prior pass, but the
// filters themselves were never implemented).
func TestListPrefetchSchedules_FiltersByScheduleTypeAndStreamID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestPlaybackConfig(t, h, "pc1")

	doRequest(t, h, http.MethodPost, "/prefetchSchedule/pc1/single-a", map[string]any{
		"ScheduleType": "SINGLE",
		"StreamId":     "stream-1",
	})
	doRequest(t, h, http.MethodPost, "/prefetchSchedule/pc1/single-b", map[string]any{
		"ScheduleType": "SINGLE",
		"StreamId":     "stream-2",
	})
	doRequest(t, h, http.MethodPost, "/prefetchSchedule/pc1/recurring-a", map[string]any{
		"ScheduleType": "RECURRING",
		"StreamId":     "stream-1",
	})

	rec := doRequest(t, h, http.MethodPost, "/prefetchSchedule/pc1", map[string]any{
		"ScheduleType": "SINGLE",
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items, _ := resp["Items"].([]any)
	assert.Len(t, items, 2, "ScheduleType=SINGLE must exclude the RECURRING schedule")

	rec = doRequest(t, h, http.MethodPost, "/prefetchSchedule/pc1", map[string]any{
		"StreamId": "stream-1",
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items, _ = resp["Items"].([]any)
	assert.Len(t, items, 2, "StreamId=stream-1 must match single-a and recurring-a only")
}

// TestPrefetchSchedule_TagsAndScheduleTypeRoundTrip verifies CreatePrefetchSchedule
// accepts and returns Tags and ScheduleType (ScheduleType/StreamId were entirely
// unmodeled before this pass; Tags support did not exist on PrefetchSchedule at
// all).
func TestPrefetchSchedule_TagsAndScheduleTypeRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestPlaybackConfig(t, h, "pc1")

	rec := doRequest(t, h, http.MethodPost, "/prefetchSchedule/pc1/sched1", map[string]any{
		"ScheduleType": "RECURRING",
		"StreamId":     "stream-9",
		"tags":         map[string]any{"env": "prod"},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "RECURRING", resp["ScheduleType"])
	assert.Equal(t, "stream-9", resp["StreamId"])
	tags, _ := resp["tags"].(map[string]any)
	assert.Equal(t, "prod", tags["env"])
	assert.NotNil(t, resp["CreationTime"])
}

// TestCreatePrefetchSchedule_InvalidScheduleType verifies an unrecognized
// ScheduleType is rejected as BadRequestException, matching the real enum's
// only two members (SINGLE, RECURRING).
func TestCreatePrefetchSchedule_InvalidScheduleType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestPlaybackConfig(t, h, "pc1")

	rec := doRequest(t, h, http.MethodPost, "/prefetchSchedule/pc1/sched1", map[string]any{
		"ScheduleType": "BOGUS",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
