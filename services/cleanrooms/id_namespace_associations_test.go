package cleanrooms_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createIDNamespaceAssociation(t *testing.T, e *echo.Echo, mID string) string {
	t.Helper()
	createRec := doRequest(t, e, http.MethodPost, "/memberships/"+mID+"/idnamespaceassociations", map[string]any{
		"name": "test-ns",
		"inputReferenceConfig": map[string]any{
			"inputReferenceArn":      "arn:aws:cleanrooms:us-east-1:123456789012:membership/" + mID,
			"manageResourcePolicies": true,
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	var createResp map[string]map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

	return createResp["idNamespaceAssociation"]["id"].(string)
}

func TestIDNamespaceAssociations_Create(t *testing.T) {
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
					"name": "test-ns",
					"inputReferenceConfig": map[string]any{
						"inputReferenceArn":      "arn:aws:cleanrooms:us-east-1:123456789012:membership/",
						"manageResourcePolicies": true,
					},
				},
			},
			wants: wants{
				status: http.StatusOK,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newTestServer(t)
			_, mID := setupTestEnvironment(t, e)

			// Fix up ARN with mID
			body := tt.args.body
			if config, ok := body["inputReferenceConfig"].(map[string]any); ok {
				if arn, ok2 := config["inputReferenceArn"].(string); ok2 &&
					arn == "arn:aws:cleanrooms:us-east-1:123456789012:membership/" {
					config["inputReferenceArn"] = arn + mID
				}
			}

			rec := doRequest(t, e, http.MethodPost, "/memberships/"+mID+"/idnamespaceassociations", body)
			require.Equal(t, tt.wants.status, rec.Code)
		})
	}
}

func TestIDNamespaceAssociations_Get(t *testing.T) {
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
			name: "get_existing",
			args: args{
				setupID: true,
			},
			wants: wants{
				status: http.StatusOK,
			},
		},
		{
			name: "get_missing",
			args: args{
				setupID: false,
			},
			wants: wants{
				status: http.StatusNotFound,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newTestServer(t)
			_, mID := setupTestEnvironment(t, e)

			id := "invalid-id"
			if tt.args.setupID {
				id = createIDNamespaceAssociation(t, e, mID)
			}

			rec := doRequest(t, e, http.MethodGet, "/memberships/"+mID+"/idnamespaceassociations/"+id, nil)
			if tt.args.setupID {
				require.Equal(t, tt.wants.status, rec.Code)
			} else {
				require.NotEqual(t, http.StatusOK, rec.Code)
			}
		})
	}
}

func TestIDNamespaceAssociations_List(t *testing.T) {
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
			createIDNamespaceAssociation(t, e, mID)

			rec := doRequest(t, e, http.MethodGet, "/memberships/"+mID+"/idnamespaceassociations", nil)
			require.Equal(t, tt.wants.status, rec.Code)
		})
	}
}

func TestIDNamespaceAssociations_Update(t *testing.T) {
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
			name: "valid_update",
			args: args{
				body: map[string]any{
					"description": "updated desc",
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
			id := createIDNamespaceAssociation(t, e, mID)

			rec := doRequest(t, e, http.MethodPatch, "/memberships/"+mID+"/idnamespaceassociations/"+id, tt.args.body)
			require.Equal(t, tt.wants.status, rec.Code)
		})
	}
}

func TestIDNamespaceAssociations_Delete(t *testing.T) {
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
			name:  "valid_delete",
			args:  args{},
			wants: wants{status: http.StatusOK},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newTestServer(t)
			_, mID := setupTestEnvironment(t, e)
			id := createIDNamespaceAssociation(t, e, mID)

			rec := doRequest(t, e, http.MethodDelete, "/memberships/"+mID+"/idnamespaceassociations/"+id, nil)
			require.Equal(t, tt.wants.status, rec.Code)
		})
	}
}

func TestCollaborationIDNamespaceAssociations(t *testing.T) {
	t.Parallel()

	type args struct {
		urlSuffix string
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
			name:  "get",
			args:  args{urlSuffix: "/idnamespaceassociations/{id}"},
			wants: wants{statuses: []int{http.StatusOK, http.StatusNotFound}},
		},
		{
			name:  "list",
			args:  args{urlSuffix: "/idnamespaceassociations"},
			wants: wants{statuses: []int{http.StatusOK, http.StatusNotFound}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newTestServer(t)
			colID, mID := setupTestEnvironment(t, e)
			id := createIDNamespaceAssociation(t, e, mID)

			url := "/collaborations/" + colID + tt.args.urlSuffix
			if tt.args.urlSuffix == "/idnamespaceassociations/{id}" {
				url = "/collaborations/" + colID + "/idnamespaceassociations/" + id
			}

			rec := doRequest(t, e, http.MethodGet, url, nil)
			assert.Contains(t, tt.wants.statuses, rec.Code)
		})
	}
}
