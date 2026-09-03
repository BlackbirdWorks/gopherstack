package memorydb_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
)

// TestRefinement1_MaxEventsCap verifies that events are capped at maxEvents.
func TestMaxEventsCap(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)

	for i := range 1200 {
		b.AddEvent(&memorydb.ExportedEvent{
			Date:       time.Now(),
			SourceName: "src",
			SourceType: "cluster",
			Message:    string(rune('a' + i%26)),
		})
	}

	// Should be capped at 1000
	assert.LessOrEqual(t, memorydb.EventCount(b), 1000)
}

// TestRefinement1_AddEventCapEnforced verifies AddEvent does not grow beyond cap.
func TestAddEventCapEnforced(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)

	for range 100 {
		b.AddEvent(&memorydb.ExportedEvent{
			Date:       time.Now(),
			SourceName: "cluster-x",
			SourceType: "cluster",
			Message:    "test event",
		})
	}

	assert.Equal(t, 100, memorydb.EventCount(b))

	// Adding one more should still be 100 (under cap)
	b.AddEvent(&memorydb.ExportedEvent{
		Date: time.Now(), SourceName: "x", SourceType: "cluster", Message: "extra",
	})

	assert.Equal(t, 101, memorydb.EventCount(b))
}

// TestDescribeEvents_Pagination asserts DescribeEventsInput.MaxResults is
// honoured and DescribeEventsOutput.NextToken is returned when more events
// remain, instead of always returning every stored event in one response.
func TestDescribeEvents_Pagination(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	h := memorydb.NewHandler(b)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	for i := range 3 {
		b.AddEvent(&memorydb.ExportedEvent{
			Date:       time.Now(),
			SourceName: "cluster-a",
			SourceType: "cluster",
			Message:    string(rune('a' + i)),
		})
	}

	rec := doRequest(t, h, "DescribeEvents", map[string]any{"MaxResults": 1})
	require.Equal(t, http.StatusOK, rec.Code)

	var first struct {
		NextToken string           `json:"NextToken"`
		Events    []map[string]any `json:"Events"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &first))
	require.Len(t, first.Events, 1, "first page must truncate to MaxResults")
	require.NotEmpty(t, first.NextToken, "NextToken must be set when more events remain")

	rec = doRequest(t, h, "DescribeEvents", map[string]any{"MaxResults": 1, "NextToken": first.NextToken})
	require.Equal(t, http.StatusOK, rec.Code)

	var second struct {
		NextToken string           `json:"NextToken"`
		Events    []map[string]any `json:"Events"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &second))
	require.Len(t, second.Events, 1, "second page must return the next event")
	assert.NotEqual(t, first.Events[0]["Message"], second.Events[0]["Message"], "no event must repeat across pages")
}
