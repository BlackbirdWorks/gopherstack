package cloudfront_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMonitoringSubscription_NotFound verifies Get and Delete both 404 when no
// subscription has been created for a distribution, and that Get succeeds once one has.
func TestMonitoringSubscription_NotFound(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)
	const prefix = "/2020-05-31/"
	const distID = "ENOSUBSCRIPTION"
	// Real Create/Get/DeleteMonitoringSubscription use the PLURAL "distributions/"
	// prefix (cloudfront@v1.67.4 serializers.go), unlike every other distribution
	// sub-path which is singular.
	path := prefix + "distributions/" + distID + "/monitoring-subscription"

	getRec := doXML(t, h, http.MethodGet, path, nil)
	assert.Equal(t, http.StatusNotFound, getRec.Code)
	assert.Contains(t, getRec.Body.String(), "NoSuchMonitoringSubscription")

	deleteRec := doXML(t, h, http.MethodDelete, path, nil)
	assert.Equal(t, http.StatusNotFound, deleteRec.Code)

	createRec := doXML(t, h, http.MethodPost, path,
		[]byte(`<MonitoringSubscription><RealtimeMetricsSubscriptionConfig>`+
			`<RealtimeMetricsSubscriptionStatus>Enabled</RealtimeMetricsSubscriptionStatus>`+
			`</RealtimeMetricsSubscriptionConfig></MonitoringSubscription>`))
	require.Equal(t, http.StatusOK, createRec.Code)

	getAfterCreate := doXML(t, h, http.MethodGet, path, nil)
	assert.Equal(t, http.StatusOK, getAfterCreate.Code)
	assert.Contains(t, getAfterCreate.Body.String(), "Enabled")

	deleteAfterCreate := doXML(t, h, http.MethodDelete, path, nil)
	assert.Equal(t, http.StatusNoContent, deleteAfterCreate.Code)

	// Deleted again -> 404.
	deleteAgain := doXML(t, h, http.MethodDelete, path, nil)
	assert.Equal(t, http.StatusNotFound, deleteAgain.Code)
}

// ---------------------------------------------------------------------------
// ResourcePolicy
// ---------------------------------------------------------------------------

// TestMonitoringSubscription_CRUD tests monitoring subscription Create/Get/Delete.
func TestMonitoringSubscription_CRUD(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const distID = "E1DIST123456"
	const prefix = "/2020-05-31/"
	// Real Create/Get/DeleteMonitoringSubscription use the PLURAL "distributions/"
	// prefix (cloudfront@v1.67.4 serializers.go), unlike every other distribution
	// sub-path which is singular.
	path := prefix + "distributions/" + distID + "/monitoring-subscription"

	// Create
	body := `<MonitoringSubscription><RealtimeMetricsSubscriptionConfig>` +
		`<RealtimeMetricsSubscriptionStatus>Enabled</RealtimeMetricsSubscriptionStatus>` +
		`</RealtimeMetricsSubscriptionConfig></MonitoringSubscription>`
	cfOK(t, h, http.MethodPost, path, body)

	// Get
	out := cfOK(t, h, http.MethodGet, path, "")
	if !strings.Contains(out, "MonitoringSubscription") {
		t.Errorf("unexpected response: %s", out)
	}

	// Delete
	cfOK(t, h, http.MethodDelete, path, "")
}
