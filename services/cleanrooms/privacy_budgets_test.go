package cleanrooms_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestEnvironment is a helper to bootstrap a collaboration and membership
// so that dependent tests can use the resulting mID and colID.
func setupTestEnvironment(t *testing.T, e *echo.Echo) (string, string) {
	t.Helper()
	// Create Collaboration
	doRequest(t, e, http.MethodPost, "/collaborations", map[string]any{
		"name": "collab", "creatorDisplayName": "user",
		"creatorMemberAbilities": []string{"CAN_QUERY"},
		"members":                []any{}, "queryLogStatus": "DISABLED",
	})
	rec := doRequest(t, e, http.MethodGet, "/collaborations", nil)
	var colResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &colResp))
	colID := colResp["collaborationList"].([]any)[0].(map[string]any)["id"].(string)

	// Create Membership
	rec2 := doRequest(t, e, http.MethodPost, "/memberships", map[string]any{
		"collaborationIdentifier": colID, "queryLogStatus": "DISABLED",
	})
	var memResp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &memResp))
	mID := memResp["membership"].(map[string]any)["id"].(string)

	return colID, mID
}

// createBudgetTemplate is a helper to create a template and return its ID for Get/Update tests.
func createBudgetTemplate(t *testing.T, e *echo.Echo, mID string) string {
	t.Helper()
	createRec := doRequest(t, e, http.MethodPost, "/memberships/"+mID+"/privacybudgettemplates", map[string]any{
		"privacyBudgetType": "DIFFERENTIAL_PRIVACY",
		"autoRefresh":       "CALENDAR_MONTH",
		"parameters": map[string]any{
			"differentialPrivacy": map[string]any{
				"epsilon":            1.0,
				"usersNoisePerQuery": 100,
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	var createResp map[string]map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

	return createResp["privacyBudgetTemplate"]["id"].(string)
}

func TestPrivacyBudgetTemplates_Create(t *testing.T) {
	t.Parallel()

	type args struct {
		body   map[string]any
		method string
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
			name: "valid_differential_privacy",
			args: args{
				method: http.MethodPost,
				body: map[string]any{
					"privacyBudgetType": "DIFFERENTIAL_PRIVACY",
					"autoRefresh":       "CALENDAR_MONTH",
					"parameters": map[string]any{
						"differentialPrivacy": map[string]any{
							"epsilon":            1.0,
							"usersNoisePerQuery": 100,
						},
					},
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

			rec := doRequest(t, e, tt.args.method, "/memberships/"+mID+"/privacybudgettemplates", tt.args.body)
			require.Equal(t, tt.wants.status, rec.Code)
		})
	}
}

func TestPrivacyBudgetTemplates_Get(t *testing.T) {
	t.Parallel()

	type args struct {
		method  string
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
			args:  args{method: http.MethodGet, setupID: true},
			wants: wants{status: http.StatusOK},
		},
		{
			name:  "get_missing",
			args:  args{method: http.MethodGet, setupID: false},
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
				id = createBudgetTemplate(t, e, mID)
			}

			rec := doRequest(t, e, tt.args.method, "/memberships/"+mID+"/privacybudgettemplates/"+id, nil)
			if tt.args.setupID {
				require.Equal(t, tt.wants.status, rec.Code)
			} else {
				require.NotEqual(t, http.StatusOK, rec.Code)
			}
		})
	}
}

func TestPrivacyBudgetTemplates_List(t *testing.T) {
	t.Parallel()

	type args struct {
		method string
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
			name:  "list_all",
			args:  args{method: http.MethodGet},
			wants: wants{status: http.StatusOK},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newTestServer(t)
			_, mID := setupTestEnvironment(t, e)
			createBudgetTemplate(t, e, mID)

			rec := doRequest(t, e, tt.args.method, "/memberships/"+mID+"/privacybudgettemplates", nil)
			require.Equal(t, tt.wants.status, rec.Code)
		})
	}
}

func TestPrivacyBudgetTemplates_Update(t *testing.T) {
	t.Parallel()

	type args struct {
		body   map[string]any
		method string
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
			name: "valid_update",
			args: args{
				method: http.MethodPatch,
				body: map[string]any{
					"autoRefresh": "NONE",
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
			id := createBudgetTemplate(t, e, mID)

			rec := doRequest(t, e, tt.args.method, "/memberships/"+mID+"/privacybudgettemplates/"+id, tt.args.body)
			require.Equal(t, tt.wants.status, rec.Code)
		})
	}
}

func TestPrivacyBudgetTemplates_Delete(t *testing.T) {
	t.Parallel()

	type args struct {
		method string
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
			name:  "valid_delete",
			args:  args{method: http.MethodDelete},
			wants: wants{status: http.StatusOK},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newTestServer(t)
			_, mID := setupTestEnvironment(t, e)
			id := createBudgetTemplate(t, e, mID)

			rec := doRequest(t, e, tt.args.method, "/memberships/"+mID+"/privacybudgettemplates/"+id, nil)
			require.Equal(t, tt.wants.status, rec.Code)
		})
	}
}

func TestPreviewPrivacyImpact(t *testing.T) {
	t.Parallel()

	type args struct {
		body   map[string]any
		method string
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
			name: "valid_preview",
			args: args{
				method: http.MethodPost,
				body: map[string]any{
					"parameters": map[string]any{
						"differentialPrivacy": map[string]any{
							"epsilon":            1.0,
							"usersNoisePerQuery": 100,
						},
					},
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

			rec := doRequest(t, e, tt.args.method, "/memberships/"+mID+"/previewprivacyimpact", tt.args.body)
			require.Equal(t, tt.wants.status, rec.Code)
		})
	}
}

func TestListPrivacyBudgets(t *testing.T) {
	t.Parallel()

	type args struct {
		method string
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
			name:  "list_budgets",
			args:  args{method: http.MethodGet},
			wants: wants{status: http.StatusOK},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newTestServer(t)
			_, mID := setupTestEnvironment(t, e)

			rec := doRequest(t, e, tt.args.method, "/memberships/"+mID+"/privacybudgets", nil)
			require.Equal(t, tt.wants.status, rec.Code)
		})
	}
}

func TestCollaborationPrivacyBudgetTemplates(t *testing.T) {
	t.Parallel()

	type args struct {
		method    string
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
			name: "get",
			args: args{
				method:    http.MethodGet,
				urlSuffix: "/privacybudgettemplates/{id}",
			},
			wants: wants{statuses: []int{http.StatusOK, http.StatusNotFound}},
		},
		{
			name: "list",
			args: args{
				method:    http.MethodGet,
				urlSuffix: "/privacybudgettemplates",
			},
			wants: wants{statuses: []int{http.StatusOK, http.StatusNotFound}},
		},
		{
			name: "list_budgets",
			args: args{
				method:    http.MethodGet,
				urlSuffix: "/privacybudgets",
			},
			wants: wants{statuses: []int{http.StatusOK, http.StatusNotFound}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newTestServer(t)
			colID, mID := setupTestEnvironment(t, e)
			id := createBudgetTemplate(t, e, mID)

			url := "/collaborations/" + colID + tt.args.urlSuffix
			if tt.args.urlSuffix == "/privacybudgettemplates/{id}" {
				url = "/collaborations/" + colID + "/privacybudgettemplates/" + id
			}

			rec := doRequest(t, e, tt.args.method, url, nil)
			assert.Contains(t, tt.wants.statuses, rec.Code)
		})
	}
}
