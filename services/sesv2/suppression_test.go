package sesv2_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestPutSuppressedDestination tests the PutSuppressedDestination operation.
func TestPutSuppressedDestination(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodPut, "/v2/email/suppression/addresses", map[string]any{
		"EmailAddress": "suppress@example.com",
		"Reason":       "BOUNCE",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestGetSuppressedDestination tests the GetSuppressedDestination operation.
func TestGetSuppressedDestination(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPut, "/v2/email/suppression/addresses", map[string]any{
		"EmailAddress": "get-supp@example.com",
		"Reason":       "COMPLAINT",
	})

	rec := doRequest(
		t,
		h,
		http.MethodGet,
		"/v2/email/suppression/addresses/get-supp%40example.com",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestDeleteSuppressedDestination tests the DeleteSuppressedDestination operation.
func TestDeleteSuppressedDestination(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPut, "/v2/email/suppression/addresses", map[string]any{
		"EmailAddress": "del-supp@example.com",
		"Reason":       "BOUNCE",
	})

	rec := doRequest(
		t,
		h,
		http.MethodDelete,
		"/v2/email/suppression/addresses/del-supp%40example.com",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestListSuppressedDestinations tests the ListSuppressedDestinations operation.
func TestListSuppressedDestinations(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodGet, "/v2/email/suppression/addresses", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}
