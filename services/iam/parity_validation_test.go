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

	validPolicyDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}`

	tests := []struct {
		name        string
		policyDoc   string
		wantCode    int
		wantErrCode string
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
