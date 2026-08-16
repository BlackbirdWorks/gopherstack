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

// TestCreateRole_AssumeRolePolicyDocument_Validation asserts MalformedPolicyDocument for invalid JSON.
func TestCreateRole_AssumeRolePolicyDocument_Validation(t *testing.T) {
	t.Parallel()

	validPolicyDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}`

	tests := []struct {
		name        string
		wantErrCode string
		policyDoc   string
		wantCode    int
	}{
		{
			name:      "valid_json_policy_accepted",
			policyDoc: validPolicyDoc,
			wantCode:  http.StatusOK,
		},
		{
			name:        "invalid_json_rejected",
			policyDoc:   `not-valid-json`,
			wantCode:    http.StatusBadRequest,
			wantErrCode: "MalformedPolicyDocument",
		},
		{
			name:        "truncated_json_rejected",
			policyDoc:   `{"Version":"2012-10-17"`,
			wantCode:    http.StatusBadRequest,
			wantErrCode: "MalformedPolicyDocument",
		},
		{
			name:      "empty_policy_doc_allowed",
			policyDoc: "",
			wantCode:  http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			e := echo.New()

			req := iamRequest("CreateRole", map[string]string{
				"RoleName":                 "test-role-" + tt.name,
				"AssumeRolePolicyDocument": tt.policyDoc,
			})
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantErrCode != "" {
				var errResp iam.ErrorResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, tt.wantErrCode, errResp.Error.Code)
			}
		})
	}
}

// TestCreateRole_MaxSessionDurationBounds verifies that CreateRole rejects
// MaxSessionDuration values outside the AWS-allowed range [3600, 43200].
// Real AWS returns ValidationError for out-of-range values; the emulator previously
// accepted any value without validation.
func TestCreateRole_MaxSessionDurationBounds(t *testing.T) {
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

// TestCreateRole_MaxSessionDurationPersisted verifies that a valid
// MaxSessionDuration is stored and returned by GetRole.
func TestCreateRole_MaxSessionDurationPersisted(t *testing.T) {
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

func TestHandler_UpdateAssumeRolePolicy(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	_, _ = b.CreateRole("MyRole", "/", doc, "")

	newDoc := `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},` +
		`"Action":"sts:AssumeRole"}]}`
	req := iamRequest("UpdateAssumeRolePolicy", map[string]string{
		"RoleName":       "MyRole",
		"PolicyDocument": newDoc,
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_TagRole_RoundTrip(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	_, _ = b.CreateRole("MyRole", "/", doc, "")

	req := iamRequest("TagRole", map[string]string{
		"RoleName":            "MyRole",
		"Tags.member.1.Key":   "team",
		"Tags.member.1.Value": "platform",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	req2 := iamRequest("ListRoleTags", map[string]string{"RoleName": "MyRole"})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Contains(t, rec2.Body.String(), "platform")

	req3 := iamRequest("UntagRole", map[string]string{
		"RoleName":         "MyRole",
		"TagKeys.member.1": "team",
	})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code)
}

func TestHandler_UpdateRole(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	_, _ = b.CreateRole("MyRole", "/", doc, "")

	req := iamRequest("UpdateRole", map[string]string{
		"RoleName":    "MyRole",
		"Description": "Updated description",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestIAMHandler_Roles(t *testing.T) {
	t.Parallel()

	t.Run("CreateRole", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, _ := newTestHandler(t)

		req := iamRequest("CreateRole", map[string]string{
			"RoleName":                 "MyRole",
			"AssumeRolePolicyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
		})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp iam.CreateRoleResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Equal(t, "MyRole", resp.CreateRoleResult.Role.RoleName)
	})

	t.Run("GetRole", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, b := newTestHandler(t)
		_, _ = b.CreateRole("MyRole", "/", "", "")

		req := iamRequest("GetRole", map[string]string{"RoleName": "MyRole"})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("DeleteRole", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, b := newTestHandler(t)
		_, _ = b.CreateRole("MyRole", "/", "", "")

		req := iamRequest("DeleteRole", map[string]string{"RoleName": "MyRole"})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("ListRoles", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, b := newTestHandler(t)
		_, _ = b.CreateRole("RoleA", "/", "", "")
		_, _ = b.CreateRole("RoleB", "/", "", "")

		req := iamRequest("ListRoles", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp iam.ListRolesResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Len(t, resp.ListRolesResult.Roles, 2)
	})

	t.Run("CreateRole_WithMaxSessionDuration", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, _ := newTestHandler(t)

		req := iamRequest("CreateRole", map[string]string{
			"RoleName":                 "MyRoleWithDuration",
			"AssumeRolePolicyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
			"MaxSessionDuration":       "7200",
		})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp iam.CreateRoleResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Equal(t, "MyRoleWithDuration", resp.CreateRoleResult.Role.RoleName)
		assert.Equal(t, int32(7200), resp.CreateRoleResult.Role.MaxSessionDuration)
	})
}

func TestIAMHandler_UpdateAssumeRolePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*iam.InMemoryBackend)
		params      map[string]string
		name        string
		wantContain string
		wantCode    int
	}{
		{
			name: "UpdateAssumeRolePolicy_Success",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole("MyRole", "/", "{}", "")
			},
			params: map[string]string{
				"RoleName":       "MyRole",
				"PolicyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
			},
			wantCode:    http.StatusOK,
			wantContain: "UpdateAssumeRolePolicyResponse",
		},
		{
			name:  "UpdateAssumeRolePolicy_RoleNotFound",
			setup: func(_ *iam.InMemoryBackend) {},
			params: map[string]string{
				"RoleName":       "Ghost",
				"PolicyDocument": "{}",
			},
			wantCode:    http.StatusNotFound, // NoSuchEntity is 404 on real AWS IAM.
			wantContain: "NoSuchEntity",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, b := newTestHandler(t)
			tt.setup(b)

			req := iamRequest("UpdateAssumeRolePolicy", tt.params)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContain)
		})
	}
}
