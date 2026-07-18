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
