package cleanrooms_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createProtectedJob(t *testing.T, e *echo.Echo, mID string) string {
	t.Helper()
	startRec := doRequest(t, e, http.MethodPost, "/memberships/"+mID+"/protectedJobs", map[string]any{
		"type": "SQL",
		"sqlParameters": map[string]any{
			"queryString":         "SELECT * FROM t",
			"analysisTemplateArn": "arn:aws:cleanrooms:us-east-1:123456789012:membership/" + mID + "/analysistemplate/at-1",
		},
	})
	require.Equal(t, http.StatusOK, startRec.Code)
	var startResp map[string]map[string]any
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startResp))

	return startResp["protectedJob"]["id"].(string)
}

func TestProtectedJobs_Create(t *testing.T) {
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
					"type": "SQL",
					"sqlParameters": map[string]any{
						"queryString":         "SELECT * FROM t",
						"analysisTemplateArn": "arn:aws:cleanrooms:us-east-1:123456789012:membership/at-1",
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

			rec := doRequest(t, e, http.MethodPost, "/memberships/"+mID+"/protectedJobs", tt.args.body)
			require.Equal(t, tt.wants.status, rec.Code)
		})
	}
}

func TestProtectedJobs_Update(t *testing.T) {
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
					"description": "updated desc",
				},
			},
			wants: wants{statuses: []int{http.StatusOK, http.StatusNotFound}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newTestServer(t)
			_, mID := setupTestEnvironment(t, e)
			id := createProtectedJob(t, e, mID)

			rec := doRequest(t, e, http.MethodPatch, "/memberships/"+mID+"/protectedJobs/"+id, tt.args.body)
			assert.Contains(t, tt.wants.statuses, rec.Code)
		})
	}
}
