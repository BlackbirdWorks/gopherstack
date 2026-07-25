package neptune_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDescribeEvents_ReflectsRealLifecycleActivity locks the core fix: there
// was no event log backing this backend at all, so DescribeEvents always
// answered empty regardless of what a caller had actually done. Creating and
// deleting a cluster must now produce real, retrievable events.
func TestDescribeEvents_ReflectsRealLifecycleActivity(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "events-cluster")

	rr := doRequest(t, h, url.Values{
		"Action":  {"DescribeEvents"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "events-cluster")
	assert.Contains(t, body, "DB cluster created")
	assert.Contains(t, body, "db-cluster")

	doRequest(t, h, url.Values{
		"Action":              {"DeleteDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"events-cluster"},
		"SkipFinalSnapshot":   {"true"},
	})

	rr = doRequest(t, h, url.Values{
		"Action":  {"DescribeEvents"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "DB cluster deleted")
}

// TestDescribeEvents_FiltersBySourceIdentifierAndType verifies DescribeEvents
// narrows results to a single resource/source-type combination rather than
// dumping the whole account log indiscriminately.
func TestDescribeEvents_FiltersBySourceIdentifierAndType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "events-filter-a")
	createCluster(t, h, "events-filter-b")

	rr := doRequest(t, h, url.Values{
		"Action":           {"DescribeEvents"},
		"Version":          {"2014-10-31"},
		"SourceIdentifier": {"events-filter-a"},
		"SourceType":       {"db-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "events-filter-a")
	assert.NotContains(t, body, "events-filter-b")
}

// TestDescribeEvents_NoActivityIsGenuinelyEmpty verifies a fresh backend with
// no recorded activity still returns a valid (empty) response, so the fix
// doesn't turn DescribeEvents into a suspiciously-always-populated stub in
// the other direction.
func TestDescribeEvents_NoActivityIsGenuinelyEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":  {"DescribeEvents"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "DescribeEventsResponse")
	assert.NotContains(t, rr.Body.String(), "<Event>")
}
