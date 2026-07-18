package ssoadmin_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateApplicationAssignment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		principalID   string
		principalType string
		wantStatus    int
		useInvalidApp bool
	}{
		{
			name:          "assign user to application",
			principalID:   "user-001",
			principalType: "USER",
			wantStatus:    http.StatusOK,
		},
		{
			name:          "assign group to application",
			principalID:   "group-001",
			principalType: "GROUP",
			wantStatus:    http.StatusOK,
		},
		{
			name:          "assign to nonexistent application",
			principalID:   "user-001",
			principalType: "USER",
			wantStatus:    http.StatusNotFound,
			useInvalidApp: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			var appArn string
			if tt.useInvalidApp {
				appArn = "arn:aws:sso::123456789012:application/ssoins-bad/apl-notexist"
			} else {
				instanceArn := createInstance(t, h, "assign-app-instance")
				rec := doRequest(t, h, "CreateApplication", map[string]any{
					"InstanceArn":            instanceArn,
					"ApplicationProviderArn": "arn:aws:sso::123456789012:applicationProvider/custom",
					"Name":                   "AssignApp",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseResponse(t, rec)
				appArn = resp["ApplicationArn"].(string)
			}
			rec := doRequest(t, h, "CreateApplicationAssignment", map[string]any{
				"ApplicationArn": appArn,
				"PrincipalId":    tt.principalID,
				"PrincipalType":  tt.principalType,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestDeleteApplicationAssignment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		createAssign bool
		wantStatus   int
	}{
		{
			name:         "delete existing assignment",
			createAssign: true,
			wantStatus:   http.StatusOK,
		},
		{
			name:         "delete nonexistent assignment",
			createAssign: false,
			wantStatus:   http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			instanceArn := createInstance(t, h, "del-assign-instance")
			rec := doRequest(t, h, "CreateApplication", map[string]any{
				"InstanceArn":            instanceArn,
				"ApplicationProviderArn": "arn:aws:sso::123456789012:applicationProvider/custom",
				"Name":                   "DelAssignApp",
			})
			require.Equal(t, http.StatusOK, rec.Code)
			resp := parseResponse(t, rec)
			appArn := resp["ApplicationArn"].(string)

			if tt.createAssign {
				rec2 := doRequest(t, h, "CreateApplicationAssignment", map[string]any{
					"ApplicationArn": appArn,
					"PrincipalId":    "user-del",
					"PrincipalType":  "USER",
				})
				require.Equal(t, http.StatusOK, rec2.Code)
			}

			rec3 := doRequest(t, h, "DeleteApplicationAssignment", map[string]any{
				"ApplicationArn": appArn,
				"PrincipalId":    "user-del",
				"PrincipalType":  "USER",
			})
			assert.Equal(t, tt.wantStatus, rec3.Code)
		})
	}
}

// TestListApplicationAssignmentsForPrincipal verifies the new operation.
func TestListApplicationAssignmentsForPrincipal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		principalID   string
		principalType string
		wantStatus    int
		wantCount     int
	}{
		{
			name:          "principal_with_app_assignments",
			principalID:   "user-abc",
			principalType: "USER",
			wantStatus:    http.StatusOK,
			wantCount:     2,
		},
		{
			name:          "principal_with_no_assignments",
			principalID:   "user-nobody",
			principalType: "USER",
			wantStatus:    http.StatusOK,
			wantCount:     0,
		},
		{
			name:          "missing_principal_id",
			principalID:   "",
			principalType: "USER",
			wantStatus:    http.StatusBadRequest,
		},
		{
			name:          "missing_principal_type",
			principalID:   "user-abc",
			principalType: "",
			wantStatus:    http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "r3-laafp-inst")

			// Create 2 apps and assign user-abc to both.
			for i := range 2 {
				appRec := doRequest(t, h, "CreateApplication", map[string]any{
					"InstanceArn":            instanceArn,
					"ApplicationProviderArn": "arn:aws:sso::123456789012:applicationProvider/custom",
					"Name":                   "r3-laafp-app-" + string(rune('A'+i)),
				})
				require.Equal(t, http.StatusOK, appRec.Code)
				appArn := parseResponse(t, appRec)["ApplicationArn"].(string)

				assignRec := doRequest(t, h, "CreateApplicationAssignment", map[string]any{
					"ApplicationArn": appArn,
					"PrincipalId":    "user-abc",
					"PrincipalType":  "USER",
				})
				require.Equal(t, http.StatusOK, assignRec.Code)
			}

			rec := doRequest(t, h, "ListApplicationAssignmentsForPrincipal", map[string]any{
				"InstanceArn":   instanceArn,
				"PrincipalId":   tt.principalID,
				"PrincipalType": tt.principalType,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResponse(t, rec)
				assignments, ok := resp["ApplicationAssignments"].([]any)
				require.True(t, ok)
				assert.Len(t, assignments, tt.wantCount)
			}
		})
	}
}
