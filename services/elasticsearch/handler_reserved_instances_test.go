package elasticsearch_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestElasticsearchHandler_ReservedInstances_Lifecycle drives
// DescribeReservedElasticsearchInstanceOfferings,
// PurchaseReservedElasticsearchInstanceOffering, and
// DescribeReservedElasticsearchInstances through the HTTP handler.
func TestElasticsearchHandler_ReservedInstances_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodGet, "/2015-01-01/es/reservedInstanceOfferings", nil)
	require.Len(t, readJSONBody(t, resp)["ReservedElasticsearchInstanceOfferings"], 1)

	resp = doRequest(t, h, http.MethodPost, "/2015-01-01/es/purchaseReservedInstanceOffering", map[string]any{
		"ReservedElasticsearchInstanceOfferingId": "offer-t3-small-1y",
		"ReservationName":                         "reserved-state",
	})
	require.NotEmpty(t, readJSONBody(t, resp)["ReservedElasticsearchInstanceId"])

	resp = doRequest(t, h, http.MethodGet, "/2015-01-01/es/reservedInstances", nil)
	require.Len(t, readJSONBody(t, resp)["ReservedElasticsearchInstances"], 1)
}
