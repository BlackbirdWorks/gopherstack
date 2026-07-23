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

// TestServiceNetworkDelete_ConflictWhileAssociated verifies that
// DeleteServiceNetwork is rejected with 409 while the service network still
// has a service or VPC association, matching real AWS's
// DeleteServiceNetwork doc comment ("You can only delete the service
// network if there is no service or VPC associated with it").
func TestServiceNetworkDelete_ConflictWhileAssociated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		associate func(t *testing.T, h *vpclattice.Handler, snID string)
		name      string
	}{
		{
			name: "service association",
			associate: func(t *testing.T, h *vpclattice.Handler, snID string) {
				t.Helper()

				svcRec := doRequest(t, h, http.MethodPost, "/services", map[string]any{"name": "svc-sn-conflict"})
				require.Equal(t, http.StatusCreated, svcRec.Code)
				svcID, _ := parseBody(t, svcRec)["id"].(string)

				assocRec := doRequest(t, h, http.MethodPost, "/servicenetworkserviceassociations", map[string]any{
					"serviceNetworkIdentifier": snID,
					"serviceIdentifier":        svcID,
				})
				require.Equal(t, http.StatusCreated, assocRec.Code)
			},
		},
		{
			name: "vpc association",
			associate: func(t *testing.T, h *vpclattice.Handler, snID string) {
				t.Helper()

				assocRec := doRequest(t, h, http.MethodPost, "/servicenetworkvpcassociations", map[string]any{
					"serviceNetworkIdentifier": snID,
					"vpcIdentifier":            "vpc-1",
				})
				require.Equal(t, http.StatusCreated, assocRec.Code)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			snRec := doRequest(t, h, http.MethodPost, "/servicenetworks", map[string]any{"name": "sn-" + tc.name})
			require.Equal(t, http.StatusCreated, snRec.Code)
			snID, _ := parseBody(t, snRec)["id"].(string)

			tc.associate(t, h, snID)

			rec := doRequest(t, h, http.MethodDelete, "/servicenetworks/"+snID, nil)
			assert.Equal(t, http.StatusConflict, rec.Code)
			assert.Equal(t, 1, vpclattice.ServiceNetworkCount(h.Backend.(*vpclattice.InMemoryBackend)))
		})
	}
}

// TestServiceNetworkDelete_CascadesDependents verifies that once a service
// network has no associations, deleting it also removes its resource
// policy, auth policy, and access log subscriptions -- the cascade real AWS
// documents on DeleteServiceNetwork -- leaving no ghost rows behind.
func TestServiceNetworkDelete_CascadesDependents(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	snRec := doRequest(t, h, http.MethodPost, "/servicenetworks", map[string]any{"name": "sn-cascade"})
	require.Equal(t, http.StatusCreated, snRec.Code)
	sn := parseBody(t, snRec)
	snID, _ := sn["id"].(string)
	snARN, _ := sn["arn"].(string)

	require.Equal(t, http.StatusOK,
		doRequest(t, h, http.MethodPut, "/authpolicy/"+snARN, map[string]any{"policy": `{}`}).Code)
	require.Equal(t, http.StatusOK,
		doRequest(t, h, http.MethodPut, "/resourcepolicy/"+snARN, map[string]any{"policy": `{}`}).Code)
	require.Equal(t, http.StatusCreated,
		doRequest(t, h, http.MethodPost, "/accesslogsubscriptions", map[string]any{
			"resourceIdentifier": snARN, "destinationArn": "arn:aws:s3:::bucket",
		}).Code)

	rec := doRequest(t, h, http.MethodDelete, "/servicenetworks/"+snID, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	assert.Equal(t, http.StatusNotFound,
		doRequest(t, h, http.MethodGet, "/authpolicy/"+snARN, nil).Code, "auth policy must be cascade-deleted")
	assert.Equal(t, http.StatusNotFound,
		doRequest(t, h, http.MethodGet, "/resourcepolicy/"+snARN, nil).Code, "resource policy must be cascade-deleted")

	alsRec := doRequest(t, h, http.MethodGet, "/accesslogsubscriptions?resourceIdentifier="+snARN, nil)
	require.Equal(t, http.StatusOK, alsRec.Code)
	alsItems, _ := parseBody(t, alsRec)["items"].([]any)
	assert.Empty(t, alsItems, "access log subscriptions must be cascade-deleted")
}
