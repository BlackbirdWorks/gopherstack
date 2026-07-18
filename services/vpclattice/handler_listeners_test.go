package vpclattice_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/vpclattice"
)

// TestListener_CRUD tests listeners.
func TestListener_CRUD(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	// create service
	recSvc := doRequest(t, h, http.MethodPost, "/services", map[string]any{"name": "svc-l"})
	require.Equal(t, http.StatusCreated, recSvc.Code)
	svcID, _ := parseBody(t, recSvc)["id"].(string)

	// create listener
	rec := doRequest(t, h, http.MethodPost, "/services/"+svcID+"/listeners", map[string]any{
		"name":     "my-listener",
		"protocol": "HTTP",
		"port":     80,
		"defaultAction": map[string]any{
			"fixedResponse": map[string]any{"statusCode": 404},
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	l := parseBody(t, rec)
	listenerID, _ := l["id"].(string)
	require.NotEmpty(t, listenerID)
	assert.Equal(t, "my-listener", l["name"])
	assert.InDelta(t, float64(80), l["port"], 0)

	// get
	rec = doRequest(t, h, http.MethodGet, "/services/"+svcID+"/listeners/"+listenerID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// update
	rec = doRequest(
		t,
		h,
		http.MethodPatch,
		"/services/"+svcID+"/listeners/"+listenerID,
		map[string]any{
			"defaultAction": map[string]any{
				"fixedResponse": map[string]any{"statusCode": 200},
			},
		},
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, vpclattice.ListenerCount(h.Backend.(*vpclattice.InMemoryBackend)))

	// list
	rec = doRequest(t, h, http.MethodGet, "/services/"+svcID+"/listeners", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	list := parseBody(t, rec)
	items, _ := list["items"].([]any)
	assert.Len(t, items, 1)

	// delete
	rec = doRequest(t, h, http.MethodDelete, "/services/"+svcID+"/listeners/"+listenerID, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)
	assert.Equal(t, 0, vpclattice.ListenerCount(h.Backend.(*vpclattice.InMemoryBackend)))
}
