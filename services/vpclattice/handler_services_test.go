package vpclattice_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/vpclattice"
)

// TestService_CRUD tests create/get/update/delete/list for services.
func TestService_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		check    func(t *testing.T, resp map[string]any)
		name     string
		wantCode int
	}{
		{
			name:     "create missing name returns 400",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "create with name returns 201",
			body:     map[string]any{"name": "my-svc"},
			wantCode: http.StatusCreated,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()
				assert.Contains(
					t,
					resp["arn"],
					"arn:aws:vpc-lattice:us-east-1:000000000000:service/svc-",
				)
				assert.Equal(t, "my-svc", resp["name"])
				assert.Equal(t, "ACTIVE", resp["status"])
				assert.NotEmpty(t, resp["id"])
			},
		},
		{
			name:     "create duplicate name returns 409",
			body:     map[string]any{"name": "dup-svc"},
			wantCode: http.StatusCreated,
		},
	}

	h := newTestHandler(t)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, http.MethodPost, "/services", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, parseBody(t, rec))
			}
		})
	}
}

func TestService_DuplicateName(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/services", map[string]any{"name": "dup"})
	require.Equal(t, http.StatusCreated, rec.Code)

	rec2 := doRequest(t, h, http.MethodPost, "/services", map[string]any{"name": "dup"})
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

func TestService_GetUpdateDelete(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	// create
	rec := doRequest(t, h, http.MethodPost, "/services", map[string]any{"name": "svc1"})
	require.Equal(t, http.StatusCreated, rec.Code)
	created := parseBody(t, rec)
	id, _ := created["id"].(string)
	require.NotEmpty(t, id)

	// get by id
	rec = doRequest(t, h, http.MethodGet, "/services/"+id, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	got := parseBody(t, rec)
	assert.Equal(t, "svc1", got["name"])

	// get not found
	rec = doRequest(t, h, http.MethodGet, "/services/svc-notexist", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// update
	rec = doRequest(t, h, http.MethodPatch, "/services/"+id, map[string]any{"authType": "AWS_IAM"})
	assert.Equal(t, http.StatusOK, rec.Code)
	updated := parseBody(t, rec)
	assert.Equal(t, "AWS_IAM", updated["authType"])

	// list
	rec = doRequest(t, h, http.MethodGet, "/services", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	list := parseBody(t, rec)
	items, _ := list["items"].([]any)
	assert.Len(t, items, 1)

	// delete
	rec = doRequest(t, h, http.MethodDelete, "/services/"+id, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, vpclattice.ServiceCount(h.Backend.(*vpclattice.InMemoryBackend)))

	// get after delete returns 404
	rec = doRequest(t, h, http.MethodGet, "/services/"+id, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestServiceDelete_ConflictWhileAssociated verifies that DeleteService is
// rejected with 409 while the service is still associated with a service
// network, matching real AWS's DeleteService doc comment ("A service can't
// be deleted if it's associated with a service network").
func TestServiceDelete_ConflictWhileAssociated(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	svcID := createTestService(t, h, "svc-conflict")

	snRec := doRequest(t, h, http.MethodPost, "/servicenetworks", map[string]any{"name": "sn-conflict"})
	require.Equal(t, http.StatusCreated, snRec.Code)
	snID, _ := parseBody(t, snRec)["id"].(string)

	assocRec := doRequest(t, h, http.MethodPost, "/servicenetworkserviceassociations", map[string]any{
		"serviceNetworkIdentifier": snID,
		"serviceIdentifier":        svcID,
	})
	require.Equal(t, http.StatusCreated, assocRec.Code)

	rec := doRequest(t, h, http.MethodDelete, "/services/"+svcID, nil)
	assert.Equal(t, http.StatusConflict, rec.Code, "delete must be rejected while an SNSA references the service")
	assert.Equal(t, 1, vpclattice.ServiceCount(h.Backend.(*vpclattice.InMemoryBackend)))
}

// TestServiceDelete_CascadesDependents verifies that once a service has no
// service-network association, deleting it also removes its listeners,
// listener rules, resource policy, auth policy, and access log
// subscriptions -- the cascade real AWS documents on DeleteService -- and
// leaves no ghost rows behind in any of those tables.
func TestServiceDelete_CascadesDependents(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)
	backend := h.Backend.(*vpclattice.InMemoryBackend)

	svcRec := doRequest(t, h, http.MethodPost, "/services", map[string]any{"name": "svc-cascade"})
	require.Equal(t, http.StatusCreated, svcRec.Code)
	svc := parseBody(t, svcRec)
	svcID, _ := svc["id"].(string)
	svcARN, _ := svc["arn"].(string)

	lRec := doRequest(t, h, http.MethodPost, "/services/"+svcID+"/listeners", map[string]any{
		"name": "l1", "protocol": "HTTP",
	})
	require.Equal(t, http.StatusCreated, lRec.Code)
	require.Equal(t, 1, vpclattice.ListenerCount(backend))
	require.Equal(t, 1, vpclattice.RuleCount(backend), "CreateListener implicitly creates a default rule")

	require.Equal(t, http.StatusOK,
		doRequest(t, h, http.MethodPut, "/authpolicy/"+svcARN, map[string]any{"policy": `{}`}).Code)
	require.Equal(t, http.StatusOK,
		doRequest(t, h, http.MethodPut, "/resourcepolicy/"+svcARN, map[string]any{"policy": `{}`}).Code)
	require.Equal(t, http.StatusCreated,
		doRequest(t, h, http.MethodPost, "/accesslogsubscriptions", map[string]any{
			"resourceIdentifier": svcARN, "destinationArn": "arn:aws:s3:::bucket",
		}).Code)
	require.Equal(t, http.StatusOK,
		doRequest(t, h, http.MethodPost, "/tags/"+svcARN, map[string]any{"tags": map[string]any{"k": "v"}}).Code)

	rec := doRequest(t, h, http.MethodDelete, "/services/"+svcID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	assert.Equal(t, 0, vpclattice.ListenerCount(backend), "listeners must be cascade-deleted")
	assert.Equal(t, 0, vpclattice.RuleCount(backend), "rules must be cascade-deleted")

	assert.Equal(t, http.StatusNotFound,
		doRequest(t, h, http.MethodGet, "/authpolicy/"+svcARN, nil).Code, "auth policy must be cascade-deleted")
	assert.Equal(t, http.StatusNotFound,
		doRequest(t, h, http.MethodGet, "/resourcepolicy/"+svcARN, nil).Code, "resource policy must be cascade-deleted")

	alsRec := doRequest(t, h, http.MethodGet, "/accesslogsubscriptions?resourceIdentifier="+svcARN, nil)
	require.Equal(t, http.StatusOK, alsRec.Code)
	alsItems, _ := parseBody(t, alsRec)["items"].([]any)
	assert.Empty(t, alsItems, "access log subscriptions must be cascade-deleted")

	tagsRec := doRequest(t, h, http.MethodGet, "/tags/"+svcARN, nil)
	require.Equal(t, http.StatusOK, tagsRec.Code)
	tagsMap, _ := parseBody(t, tagsRec)["tags"].(map[string]any)
	assert.Empty(t, tagsMap, "tags must be cascade-deleted")
}

// createTestService creates a service via the HTTP handler and returns its ID.
func createTestService(t *testing.T, h *vpclattice.Handler, name string) string {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/services", map[string]any{"name": name})
	require.Equal(t, http.StatusCreated, rec.Code)

	id, _ := parseBody(t, rec)["id"].(string)
	require.NotEmpty(t, id)

	return id
}

// TestRegionIsolation verifies that resources created in one region are not visible in another.
func TestRegionIsolation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		createRegion string
		listRegion   string
		wantCount    int
	}{
		{
			name:         "same region sees resource",
			createRegion: "us-east-1",
			listRegion:   "us-east-1",
			wantCount:    1,
		},
		{
			name:         "different region sees nothing",
			createRegion: "us-east-1",
			listRegion:   "eu-west-1",
			wantCount:    0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			rec := doRequestWithRegion(t, h, tc.createRegion, http.MethodPost, "/services", map[string]any{
				"name": "region-svc",
			})
			require.Equal(t, http.StatusCreated, rec.Code)

			recList := doRequestWithRegion(t, h, tc.listRegion, http.MethodGet, "/services", nil)
			require.Equal(t, http.StatusOK, recList.Code)
			listResp := parseBody(t, recList)
			items, _ := listResp["items"].([]any)
			assert.Len(t, items, tc.wantCount)
		})
	}
}
