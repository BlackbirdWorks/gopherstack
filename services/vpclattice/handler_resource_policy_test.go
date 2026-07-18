package vpclattice_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestResourcePolicy tests put/get/delete resource policy.
func TestResourcePolicy(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	resArn := "arn:aws:vpc-lattice:us-east-1:000000000000:servicenetwork/sn-abc123"
	policy := `{"Version":"2012-10-17","Statement":[]}`

	// put
	rec := doRequest(
		t,
		h,
		http.MethodPut,
		"/resourcepolicy/"+resArn,
		map[string]any{"policy": policy},
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// get
	rec = doRequest(t, h, http.MethodGet, "/resourcepolicy/"+resArn, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	resp := parseBody(t, rec)
	assert.Equal(t, policy, resp["policy"])

	// delete
	rec = doRequest(t, h, http.MethodDelete, "/resourcepolicy/"+resArn, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// get after delete returns 404
	rec = doRequest(t, h, http.MethodGet, "/resourcepolicy/"+resArn, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
