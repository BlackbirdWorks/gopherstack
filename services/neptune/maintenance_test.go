package neptune_test

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune"
)

// TestPendingMaintenanceActions_QueueRoundTrip locks the core fix: there was
// no backing pending-action queue at all, so DescribePendingMaintenanceActions
// always answered empty and ApplyPendingMaintenanceAction never had anything
// real to mutate. AddPendingMaintenanceActionInternal seeds the queue the way
// real AWS's own system-side upgrade/patch-availability data would, after
// which Describe/Apply operate on genuine state.
func TestPendingMaintenanceActions_QueueRoundTrip(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	h := neptune.NewHandler(backend)
	const resourceARN = "arn:aws:rds:us-east-1:000000000000:cluster:pma-cluster"
	backend.AddPendingMaintenanceActionInternal(resourceARN, "system-update", "a system update is available")

	rr := doRequest(t, h, url.Values{
		"Action":  {"DescribePendingMaintenanceActions"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, resourceARN)
	assert.Contains(t, body, "system-update")
	assert.Contains(t, body, "a system update is available")
}

// TestDescribePendingMaintenanceActions_MaxRecordsPaginates verifies
// DescribePendingMaintenanceActions.MaxRecords (bd gopherstack-xhu2t tier-1
// finding) narrows the outer per-resource list, not individual actions.
func TestDescribePendingMaintenanceActions_MaxRecordsPaginates(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	h := neptune.NewHandler(backend)
	backend.AddPendingMaintenanceActionInternal(
		"arn:aws:rds:us-east-1:000000000000:cluster:page-a", "system-update", "a",
	)
	backend.AddPendingMaintenanceActionInternal(
		"arn:aws:rds:us-east-1:000000000000:cluster:page-b", "db-upgrade", "b",
	)

	full := doRequest(t, h, url.Values{
		"Action":  {"DescribePendingMaintenanceActions"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, full.Code)
	assert.Contains(t, full.Body.String(), "page-a")
	assert.Contains(t, full.Body.String(), "page-b")

	paged := doRequest(t, h, url.Values{
		"Action":     {"DescribePendingMaintenanceActions"},
		"Version":    {"2014-10-31"},
		"MaxRecords": {"1"},
	})
	require.Equal(t, http.StatusOK, paged.Code)
	body := paged.Body.String()
	// Outer resource order is map-iteration order (non-deterministic), so
	// assert cardinality rather than which of the two resources survives.
	hasA := strings.Contains(body, "page-a")
	hasB := strings.Contains(body, "page-b")
	assert.NotEqual(t, hasA, hasB, "expected exactly one resource in a MaxRecords=1 page, got a=%v b=%v", hasA, hasB)
	assert.Contains(t, body, "<Marker>")
}

// TestPendingMaintenanceActions_ApplyImmediateSetsCurrentApplyDate verifies
// ApplyPendingMaintenanceAction with OptInType=immediate actually stamps
// CurrentApplyDate on the queued action, and that undo-opt-in clears it back
// out -- real mutation, not a disguised no-op.
func TestPendingMaintenanceActions_ApplyImmediateSetsCurrentApplyDate(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	h := neptune.NewHandler(backend)
	const resourceARN = "arn:aws:rds:us-east-1:000000000000:db:pma-instance"
	backend.AddPendingMaintenanceActionInternal(resourceARN, "db-upgrade", "engine upgrade available")

	rr := doRequest(t, h, url.Values{
		"Action":             {"ApplyPendingMaintenanceAction"},
		"Version":            {"2014-10-31"},
		"ResourceIdentifier": {resourceARN},
		"ApplyAction":        {"db-upgrade"},
		"OptInType":          {"immediate"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "<CurrentApplyDate>")

	rr = doRequest(t, h, url.Values{
		"Action":             {"ApplyPendingMaintenanceAction"},
		"Version":            {"2014-10-31"},
		"ResourceIdentifier": {resourceARN},
		"ApplyAction":        {"db-upgrade"},
		"OptInType":          {"undo-opt-in"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.NotContains(t, rr.Body.String(), "<CurrentApplyDate>")
}

// TestPendingMaintenanceActions_InvalidOptInType verifies OptInType is
// validated against AWS's three accepted values.
func TestPendingMaintenanceActions_InvalidOptInType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":             {"ApplyPendingMaintenanceAction"},
		"Version":            {"2014-10-31"},
		"ResourceIdentifier": {"arn:aws:rds:us-east-1:000000000000:cluster:whatever"},
		"ApplyAction":        {"system-update"},
		"OptInType":          {"not-a-real-opt-in-type"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "InvalidParameterValue")
}

// TestPendingMaintenanceActions_DescribeFiltersByResource verifies
// DescribePendingMaintenanceActions' db-cluster-id filter narrows results to
// a single resource, and that resources with no queued actions never appear
// (matching AWS, which never emits an empty
// ResourcePendingMaintenanceActions entry).
func TestPendingMaintenanceActions_DescribeFiltersByResource(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	h := neptune.NewHandler(backend)
	backend.AddPendingMaintenanceActionInternal("arn:aws:rds:us-east-1:000000000000:cluster:a", "system-update", "a")
	backend.AddPendingMaintenanceActionInternal("arn:aws:rds:us-east-1:000000000000:cluster:b", "db-upgrade", "b")

	rr := doRequest(t, h, url.Values{
		"Action":                          {"DescribePendingMaintenanceActions"},
		"Version":                         {"2014-10-31"},
		"Filters.Filter.1.Name":           {"db-cluster-id"},
		"Filters.Filter.1.Values.Value.1": {"arn:aws:rds:us-east-1:000000000000:cluster:a"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "cluster:a")
	assert.NotContains(t, body, "cluster:b")
}
