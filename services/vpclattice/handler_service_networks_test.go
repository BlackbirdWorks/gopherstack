package vpclattice_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/vpclattice"
)

// TestServiceNetwork_CRUD tests service networks.
func TestServiceNetwork_CRUD(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	// create
	rec := doRequest(t, h, http.MethodPost, "/servicenetworks", map[string]any{"name": "sn1"})
	require.Equal(t, http.StatusCreated, rec.Code)
	created := parseBody(t, rec)
	id, _ := created["id"].(string)
	require.NotEmpty(t, id)
	assert.Contains(
		t,
		created["arn"],
		"arn:aws:vpc-lattice:us-east-1:000000000000:servicenetwork/sn-",
	)

	// get
	rec = doRequest(t, h, http.MethodGet, "/servicenetworks/"+id, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	got := parseBody(t, rec)
	assert.Equal(t, "sn1", got["name"])

	// update
	rec = doRequest(
		t,
		h,
		http.MethodPatch,
		"/servicenetworks/"+id,
		map[string]any{"authType": "AWS_IAM"},
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// list
	rec = doRequest(t, h, http.MethodGet, "/servicenetworks", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	list := parseBody(t, rec)
	items, _ := list["items"].([]any)
	assert.Len(t, items, 1)
	assert.Equal(t, 1, vpclattice.ServiceNetworkCount(h.Backend.(*vpclattice.InMemoryBackend)))

	// delete
	rec = doRequest(t, h, http.MethodDelete, "/servicenetworks/"+id, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)
	assert.Equal(t, 0, vpclattice.ServiceNetworkCount(h.Backend.(*vpclattice.InMemoryBackend)))
}
