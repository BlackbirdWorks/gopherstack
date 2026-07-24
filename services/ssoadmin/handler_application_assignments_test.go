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
			wantStatus:    http.StatusBadRequest,
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
			wantStatus:   http.StatusBadRequest,
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

// TestDescribeApplicationAssignmentWireShape locks in the real
// DescribeApplicationAssignmentOutput wire shape: flat top-level fields
// (ApplicationArn, PrincipalId, PrincipalType) with NO nested
// "ApplicationAssignment" wrapper. gopherstack previously nested these under
// an invented "ApplicationAssignment" key that doesn't exist on the real
// wire -- a real aws-sdk-go-v2 client parsing that response would find every
// DescribeApplicationAssignmentOutput field nil/empty.
func TestDescribeApplicationAssignmentWireShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "daa-wire-shape-inst")
	appArn := createApplication(t, h, instanceArn, "DAAWireShapeApp")

	assignRec := doRequest(t, h, "CreateApplicationAssignment", map[string]any{
		"ApplicationArn": appArn,
		"PrincipalId":    "user-wire-shape",
		"PrincipalType":  "USER",
	})
	require.Equal(t, http.StatusOK, assignRec.Code)

	rec := doRequest(t, h, "DescribeApplicationAssignment", map[string]any{
		"ApplicationArn": appArn,
		"PrincipalId":    "user-wire-shape",
		"PrincipalType":  "USER",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)

	assert.NotContains(t, resp, "ApplicationAssignment",
		`DescribeApplicationAssignmentOutput has no nested "ApplicationAssignment" member`)
	assert.Equal(t, appArn, resp["ApplicationArn"])
	assert.Equal(t, "user-wire-shape", resp["PrincipalId"])
	assert.Equal(t, "USER", resp["PrincipalType"])
}

// TestApplicationSessionConfiguration_UserBackgroundSessionApplicationStatus
// locks in the real Put/GetApplicationSessionConfiguration wire shape: the
// member is "UserBackgroundSessionApplicationStatus" (ENABLED/DISABLED), not
// "SessionDuration" -- gopherstack previously modeled a fabricated
// SessionDuration concept for this operation pair that doesn't exist
// anywhere on the real API (confused with the unrelated
// PermissionSet.SessionDuration field). GetApplicationSessionConfigurationOutput
// is also flat -- no nested "ApplicationSessionConfiguration" wrapper.
func TestApplicationSessionConfiguration_UserBackgroundSessionApplicationStatus(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "session-config-inst")
	appArn := createApplication(t, h, instanceArn, "SessionConfigApp")

	putRec := doRequest(t, h, "PutApplicationSessionConfiguration", map[string]any{
		"ApplicationArn":                         appArn,
		"UserBackgroundSessionApplicationStatus": "ENABLED",
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doRequest(t, h, "GetApplicationSessionConfiguration", map[string]any{
		"ApplicationArn": appArn,
	})
	require.Equal(t, http.StatusOK, getRec.Code)
	resp := parseResponse(t, getRec)

	assert.NotContains(t, resp, "ApplicationSessionConfiguration",
		`GetApplicationSessionConfigurationOutput has no nested wrapper member`)
	assert.NotContains(t, resp, "SessionDuration", "there is no SessionDuration member on this operation")
	assert.Equal(t, "ENABLED", resp["UserBackgroundSessionApplicationStatus"])

	// Invalid value rejected.
	badRec := doRequest(t, h, "PutApplicationSessionConfiguration", map[string]any{
		"ApplicationArn":                         appArn,
		"UserBackgroundSessionApplicationStatus": "PT4H",
	})
	assert.Equal(t, http.StatusBadRequest, badRec.Code)
}
