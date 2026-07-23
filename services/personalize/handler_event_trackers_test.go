package personalize_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPersonalize_EventTracker_TrackingID(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)
	dgArn := personalizeCreateDatasetGroup(t, h, "my-tracker-dg")

	rec := personalizeDo(t, h, "CreateEventTracker", map[string]any{
		"name":            "my-tracker",
		"datasetGroupArn": dgArn,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	m := personalizeUnmarshal(t, rec)
	etArn, _ := m["eventTrackerArn"].(string)
	trackingID, _ := m["trackingId"].(string)
	assert.NotEmpty(t, etArn)
	assert.NotEmpty(t, trackingID, "trackingId must be non-empty on create")

	// Describe returns same trackingId
	rec = personalizeDo(t, h, "DescribeEventTracker", map[string]any{"eventTrackerArn": etArn})
	require.Equal(t, http.StatusOK, rec.Code)
	et := personalizeUnmarshal(t, rec)["eventTracker"].(map[string]any)
	assert.Equal(t, trackingID, et["trackingId"])
	assert.Equal(t, "ACTIVE", et["status"])
	assert.Equal(t, dgArn, et["datasetGroupArn"])
}

func TestPersonalize_EventTracker_ListDelete(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)
	dgArn := personalizeCreateDatasetGroup(t, h, "trackers-dg")
	personalizeDo(t, h, "CreateEventTracker", map[string]any{"name": "t1", "datasetGroupArn": dgArn})
	personalizeDo(t, h, "CreateEventTracker", map[string]any{"name": "t2", "datasetGroupArn": dgArn})

	rec := personalizeDo(t, h, "ListEventTrackers", map[string]any{"datasetGroupArn": dgArn})
	listed := personalizeUnmarshal(t, rec)
	assert.Len(t, listed["eventTrackers"].([]any), 2)

	// Find the first tracker and delete it
	trackers := listed["eventTrackers"].([]any)
	first := trackers[0].(map[string]any)
	firstArn := first["eventTrackerArn"].(string)
	personalizeDo(t, h, "DeleteEventTracker", map[string]any{"eventTrackerArn": firstArn})

	rec = personalizeDo(t, h, "ListEventTrackers", map[string]any{})
	listed = personalizeUnmarshal(t, rec)
	assert.Len(t, listed["eventTrackers"].([]any), 1)
}

// --- Filter ---
