package vpclattice_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAccessLogSubscription_CRUD tests access log subscriptions.
func TestAccessLogSubscription_CRUD(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	// create service for resource
	recSvc := doRequest(t, h, http.MethodPost, "/services", map[string]any{"name": "svc-als"})
	require.Equal(t, http.StatusCreated, recSvc.Code)
	svcID, _ := parseBody(t, recSvc)["id"].(string)

	// create ALS
	destArn := "arn:aws:logs:us-east-1:000000000000:log-group:/vpc-lattice/test"
	rec := doRequest(t, h, http.MethodPost, "/accesslogsubscriptions", map[string]any{
		"resourceIdentifier": svcID,
		"destinationArn":     destArn,
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	als := parseBody(t, rec)
	alsID, _ := als["id"].(string)
	require.NotEmpty(t, alsID)
	assert.Equal(t, destArn, als["destinationArn"])

	// get
	rec = doRequest(t, h, http.MethodGet, "/accesslogsubscriptions/"+alsID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// update
	newDest := "arn:aws:logs:us-east-1:000000000000:log-group:/vpc-lattice/new"
	rec = doRequest(t, h, http.MethodPatch, "/accesslogsubscriptions/"+alsID, map[string]any{
		"destinationArn": newDest,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	updated := parseBody(t, rec)
	assert.Equal(t, newDest, updated["destinationArn"])

	// list
	rec = doRequest(t, h, http.MethodGet, "/accesslogsubscriptions", nil)
	list := parseBody(t, rec)
	items, _ := list["items"].([]any)
	assert.Len(t, items, 1)

	// delete
	rec = doRequest(t, h, http.MethodDelete, "/accesslogsubscriptions/"+alsID, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}
