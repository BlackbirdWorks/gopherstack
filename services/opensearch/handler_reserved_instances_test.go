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
		"/2021-01-01/opensearch/reservedInstanceOfferings", nil)
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
		"/2021-01-01/opensearch/purchaseReservedInstanceOffering",
		map[string]any{
			"ReservedInstanceOfferingId": offeringID,
			"ReservationName":            "my-reservation",
			"InstanceCount":              2,
		})
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
	require.Len(t, instances, 1)
	inst := instances[0].(map[string]any)
	assert.Equal(t, "active", inst["State"])

	reservationID := inst["ReservedInstanceId"].(string)

	// reservationId query filter narrows DescribeReservedInstances to one entry.
	fr := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/reservedInstances?reservationId="+reservationID, nil)
	defer fr.Body.Close()
	require.Equal(t, http.StatusOK, fr.StatusCode)

	var fOut map[string]any
	require.NoError(t, json.NewDecoder(fr.Body).Decode(&fOut))
	filtered := fOut["ReservedInstances"].([]any)
	require.Len(t, filtered, 1)
	assert.Equal(t, reservationID, filtered[0].(map[string]any)["ReservedInstanceId"])

	// offeringId query filter narrows DescribeReservedInstanceOfferings to one entry.
	ofr := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/reservedInstanceOfferings?offeringId="+offeringID, nil)
	defer ofr.Body.Close()
	require.Equal(t, http.StatusOK, ofr.StatusCode)

	var ofOut map[string]any
	require.NoError(t, json.NewDecoder(ofr.Body).Decode(&ofOut))
	filteredOfferings := ofOut["ReservedInstanceOfferings"].([]any)
	require.Len(t, filteredOfferings, 1)
	assert.Equal(t, offeringID, filteredOfferings[0].(map[string]any)["ReservedInstanceOfferingId"])
}

func TestReservedInstances_PurchaseNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodPost,
		"/2021-01-01/opensearch/purchaseReservedInstanceOffering",
		map[string]any{
			"ReservedInstanceOfferingId": "nonexistent-offering",
			"ReservationName":            "r1",
			"InstanceCount":              1,
		})
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}
