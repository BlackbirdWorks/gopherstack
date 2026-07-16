package opensearch_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReservedInstances_ListOfferingsAndPurchase(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// List offerings — must be non-empty.
	lor := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/reservedInstances/offerings", nil)
	defer lor.Body.Close()
	require.Equal(t, http.StatusOK, lor.StatusCode)

	var loOut map[string]any
	require.NoError(t, json.NewDecoder(lor.Body).Decode(&loOut))
	offerings, ok := loOut["ReservedInstanceOfferings"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, offerings, "must have at least one offering")

	offeringID := offerings[0].(map[string]any)["ReservedInstanceOfferingId"].(string)

	// Purchase the first offering.
	pr := doRequest(t, h, http.MethodPost,
		"/2021-01-01/opensearch/reservedInstances/offerings/"+offeringID,
		map[string]any{"ReservationName": "my-reservation", "InstanceCount": 2})
	defer pr.Body.Close()
	require.Equal(t, http.StatusOK, pr.StatusCode)

	var pOut map[string]any
	require.NoError(t, json.NewDecoder(pr.Body).Decode(&pOut))
	assert.NotEmpty(t, pOut["ReservedInstanceId"])
	assert.Equal(t, "my-reservation", pOut["ReservationName"])

	// Purchased instance must appear in DescribeReservedInstances.
	dr := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/reservedInstances", nil)
	defer dr.Body.Close()
	require.Equal(t, http.StatusOK, dr.StatusCode)

	var dOut map[string]any
	require.NoError(t, json.NewDecoder(dr.Body).Decode(&dOut))
	instances, ok := dOut["ReservedInstances"].([]any)
	require.True(t, ok)
	assert.Len(t, instances, 1)
}

func TestReservedInstances_PurchaseNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodPost,
		"/2021-01-01/opensearch/reservedInstances/offerings/nonexistent-offering",
		map[string]any{"ReservationName": "r1", "InstanceCount": 1})
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}
