package iam_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

// TestParity_CreateRole_MaxSessionDurationBounds verifies that CreateRole rejects
// MaxSessionDuration values outside the AWS-allowed range [3600, 43200].
// Real AWS returns ValidationError for out-of-range values; the emulator previously
// accepted any value without validation.
func TestParity_CreateRole_MaxSessionDurationBounds(t *testing.T) {
	t.Parallel()

	const validPolicy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}`

	tests := []struct {
		wantErr  string
		name     string
		msd      string
		wantCode int
	}{
		{
			name:     "below_minimum_rejected",
			msd:      "3599",
			wantCode: http.StatusBadRequest,
			wantErr:  "ValidationError",
		},
		{
			name:     "zero_rejected",
			msd:      "0",
			wantCode: http.StatusBadRequest,
			wantErr:  "ValidationError",
		},
		{
			name:     "above_maximum_rejected",
			msd:      "43201",
			wantCode: http.StatusBadRequest,
			wantErr:  "ValidationError",
		},
		{
			name:     "minimum_boundary_accepted",
			msd:      "3600",
			wantCode: http.StatusOK,
		},
		{
			name:     "maximum_boundary_accepted",
			msd:      "43200",
			wantCode: http.StatusOK,
		},
		{
			name:     "mid_range_accepted",
			msd:      "7200",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			e := echo.New()

			req := iamRequest("CreateRole", map[string]string{
				"RoleName":                 "test-role-" + tt.name,
				"AssumeRolePolicyDocument": validPolicy,
				"MaxSessionDuration":       tt.msd,
			})
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantCode, rec.Code,
				"CreateRole MaxSessionDuration=%s", tt.msd)

			if tt.wantErr != "" {
				var errResp iam.ErrorResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, tt.wantErr, errResp.Error.Code,
					"expected error code %q for MaxSessionDuration=%s", tt.wantErr, tt.msd)
			}
		})
	}
}

// TestParity_CreateRole_MaxSessionDurationPersisted verifies that a valid
// MaxSessionDuration is stored and returned by GetRole.
func TestParity_CreateRole_MaxSessionDurationPersisted(t *testing.T) {
	t.Parallel()

	const validPolicy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}`

	h, _ := newTestHandler(t)
	e := echo.New()

	req := iamRequest("CreateRole", map[string]string{
		"RoleName":                 "msd-persist-role",
		"AssumeRolePolicyDocument": validPolicy,
		"MaxSessionDuration":       "7200",
	})
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	require.NoError(t, h.Handler()(c))
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp iam.CreateRoleResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &createResp))
	assert.Equal(t, int32(7200), createResp.CreateRoleResult.Role.MaxSessionDuration)

	getReq := iamRequest("GetRole", map[string]string{"RoleName": "msd-persist-role"})
	getRec := httptest.NewRecorder()
	getC := e.NewContext(getReq, getRec)

	require.NoError(t, h.Handler()(getC))
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp iam.GetRoleResponse
	require.NoError(t, xml.Unmarshal(getRec.Body.Bytes(), &getResp))
	assert.Equal(t, int32(7200), getResp.GetRoleResult.Role.MaxSessionDuration)
}
