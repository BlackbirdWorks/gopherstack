package cleanrooms_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createProtectedQuery(t *testing.T, e *echo.Echo, mID string) string {
	t.Helper()
	startRec := doRequest(t, e, http.MethodPost, "/memberships/"+mID+"/protectedQueries", map[string]any{
		"sqlParameters": map[string]any{
			"queryString": "SELECT * FROM t",
		},
		"resultConfiguration": map[string]any{},
	})
	require.Equal(t, http.StatusOK, startRec.Code)

	var startResp map[string]map[string]any
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startResp))

	return startResp["protectedQuery"]["id"].(string)
}

func TestProtectedQueries_Create(t *testing.T) {
	t.Parallel()

	type args struct {
		body map[string]any
	}
	type wants struct {
		status int
	}

	tests := []struct {
		args  args
		name  string
		wants wants
	}{
		{
			name: "valid_create",
			args: args{
				body: map[string]any{
					"sqlParameters":       map[string]any{"queryString": "SELECT 1"},
					"resultConfiguration": map[string]any{},
				},
			},
			wants: wants{status: http.StatusOK},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newTestServer(t)
			_, mID := setupTestEnvironment(t, e)

			rec := doRequest(t, e, "POST", "/memberships/"+mID+"/protectedQueries", tt.args.body)
			require.Equal(t, tt.wants.status, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			pq := resp["protectedQuery"].(map[string]any)

			assert.Equal(t, "SUBMITTED", pq["status"],
				"newly started protected query must have status SUBMITTED, not STARTED")
			assert.Contains(t, pq, "membershipId", "protectedQuery must have 'membershipId' key (AWS canonical)")
			assert.Contains(t, pq, "membershipIdentifier", "protectedQuery must have legacy 'membershipIdentifier'")
			assert.Equal(t, mID, pq["membershipId"])
			assert.Equal(t, pq["membershipId"], pq["membershipIdentifier"])
		})
	}
}

func TestProtectedQueries_Get(t *testing.T) {
	t.Parallel()

	type args struct {
		setupID bool
	}
	type wants struct {
		status int
	}

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name:  "get_existing",
			args:  args{setupID: true},
			wants: wants{status: http.StatusOK},
		},
		{
			name:  "get_missing",
			args:  args{setupID: false},
			wants: wants{status: http.StatusNotFound},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newTestServer(t)
			_, mID := setupTestEnvironment(t, e)

			id := "invalid-id"
			if tt.args.setupID {
				id = createProtectedQuery(t, e, mID)
			}

			rec := doRequest(t, e, http.MethodGet, "/memberships/"+mID+"/protectedQueries/"+id, nil)
			if tt.args.setupID {
				require.Equal(t, tt.wants.status, rec.Code)
			} else {
				require.NotEqual(t, http.StatusOK, rec.Code)
			}
		})
	}
}

func TestProtectedQueries_List(t *testing.T) {
	t.Parallel()

	type args struct{}
	type wants struct {
		status int
	}

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name:  "list_all",
			args:  args{},
			wants: wants{status: http.StatusOK},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newTestServer(t)
			_, mID := setupTestEnvironment(t, e)
			createProtectedQuery(t, e, mID)

			rec := doRequest(t, e, http.MethodGet, "/memberships/"+mID+"/protectedQueries", nil)
			require.Equal(t, tt.wants.status, rec.Code)
		})
	}
}

func TestProtectedQueries_Update(t *testing.T) {
	t.Parallel()

	type args struct {
		body map[string]any
	}
	type wants struct {
		statuses []int
	}

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name: "valid_update",
			args: args{
				body: map[string]any{
					"targetStatus": "CANCELLED",
				},
			},
			wants: wants{statuses: []int{http.StatusOK, http.StatusNotFound, http.StatusConflict}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newTestServer(t)
			_, mID := setupTestEnvironment(t, e)
			id := createProtectedQuery(t, e, mID)

			rec := doRequest(t, e, http.MethodPatch, "/memberships/"+mID+"/protectedQueries/"+id, tt.args.body)
			assert.Contains(t, tt.wants.statuses, rec.Code)
		})
	}
}
