package medialive_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOfferings_ListDescribe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check    func(t *testing.T, body []byte)
		name     string
		wantCode int
	}{
		{
			name:     "list returns seeded offerings",
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				offerings := resp["offerings"].([]any)
				assert.GreaterOrEqual(t, len(offerings), 3)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodGet, "/prod/offerings", nil)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestOfferings_Describe(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/prod/offerings/87654321", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "87654321", resp["offeringId"])
	assert.NotEmpty(t, resp["region"])
	_, hasName := resp["name"]
	assert.False(t, hasName, "real DescribeOfferingOutput has no name field")

	// Unknown offering
	rec = doRequest(t, h, http.MethodGet, "/prod/offerings/99999999", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestReservations_PurchaseListDescribeDeleteUpdate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Purchase
	rec := doRequest(t, h, http.MethodPost, "/prod/offerings/87654321/purchase", map[string]any{
		"name":  "test-reservation",
		"count": 2.0,
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	var purchaseResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &purchaseResp))
	resv := purchaseResp["reservation"].(map[string]any)
	reservationID := resv["reservationId"].(string)
	assert.NotEmpty(t, reservationID)
	assert.Equal(t, "ACTIVE", resv["state"])
	assert.InDelta(t, float64(2), resv["count"], 0.001)

	// Describe
	rec = doRequest(t, h, http.MethodGet, "/prod/reservations/"+reservationID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// List
	rec = doRequest(t, h, http.MethodGet, "/prod/reservations", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp["reservations"].([]any), 1)

	// Update name
	rec = doRequest(t, h, http.MethodPut, "/prod/reservations/"+reservationID, map[string]any{
		"name": "renamed-reservation",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var updatedResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updatedResp))
	updatedResv := updatedResp["reservation"].(map[string]any)
	assert.Equal(t, "renamed-reservation", updatedResv["name"])

	// Delete (cancel) -- DeleteReservationOutput is NOT wrapped in "reservation".
	rec = doRequest(t, h, http.MethodDelete, "/prod/reservations/"+reservationID, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	var deletedResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &deletedResp))
	assert.Equal(t, "CANCELED", deletedResp["state"])

	// Describe after delete returns 404
	rec = doRequest(t, h, http.MethodGet, "/prod/reservations/"+reservationID, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestReservations_RenewalSettings locks in a fix for a gap where
// gopherstack didn't track "renewalSettings" at all (a real field on
// DescribeReservationOutput/Reservation -- verified against
// aws-sdk-go-v2/service/medialive's Reservation/RenewalSettings types):
// PurchaseOffering/UpdateReservation silently discarded any renewalSettings
// a caller sent, and it was never echoed back.
func TestReservations_RenewalSettings(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/prod/offerings/87654321/purchase", map[string]any{
		"name":  "renewal-reservation",
		"count": 1.0,
		"renewalSettings": map[string]any{
			"automaticRenewal": "ENABLED",
			"renewalCount":     3.0,
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	purchased := decodeBody(t, rec.Body.Bytes())["reservation"].(map[string]any)
	reservationID := purchased["reservationId"].(string)

	rs := purchased["renewalSettings"].(map[string]any)
	assert.Equal(t, "ENABLED", rs["automaticRenewal"])
	assert.InDelta(t, float64(3), rs["renewalCount"], 0)

	// Describe echoes the same renewalSettings back.
	rec = doRequest(t, h, http.MethodGet, "/prod/reservations/"+reservationID, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	described := decodeBody(t, rec.Body.Bytes())
	rs = described["renewalSettings"].(map[string]any)
	assert.Equal(t, "ENABLED", rs["automaticRenewal"])

	// Update with a new renewalSettings object overwrites it.
	rec = doRequest(t, h, http.MethodPut, "/prod/reservations/"+reservationID, map[string]any{
		"renewalSettings": map[string]any{"automaticRenewal": "DISABLED"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	updated := decodeBody(t, rec.Body.Bytes())["reservation"].(map[string]any)
	rs = updated["renewalSettings"].(map[string]any)
	assert.Equal(t, "DISABLED", rs["automaticRenewal"])

	// A reservation purchased without renewalSettings omits the key
	// entirely, matching a real never-configured reservation.
	rec = doRequest(t, h, http.MethodPost, "/prod/offerings/87654321/purchase", map[string]any{
		"name": "no-renewal",
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	noRenewal := decodeBody(t, rec.Body.Bytes())["reservation"].(map[string]any)
	_, hasRenewal := noRenewal["renewalSettings"]
	assert.False(t, hasRenewal)
}
