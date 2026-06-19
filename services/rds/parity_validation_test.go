package rds_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRDS_FormParseFail_Returns400 asserts that a malformed form body returns 400 ValidationException.
func TestRDS_FormParseFail_Returns400(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()

	// Send a body that causes ParseForm to fail by using a semicolon-only content type
	// that triggers an invalid percent-encoding error.
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("%zz=bad"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ValidationException")
}

// TestRDS_CreateDBInstance_IdentifierValidation asserts that invalid identifiers are rejected.
func TestRDS_CreateDBInstance_IdentifierValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		dbInstanceID     string
		dbInstanceClass  string
		engine           string
		wantErrorContain string
		wantCode         int
	}{
		{
			name:            "valid_identifier",
			dbInstanceID:    "my-db-instance",
			dbInstanceClass: "db.t3.micro",
			engine:          "mysql",
			wantCode:        http.StatusOK,
		},
		{
			name:             "empty_identifier",
			dbInstanceID:     "",
			dbInstanceClass:  "db.t3.micro",
			engine:           "mysql",
			wantCode:         http.StatusBadRequest,
			wantErrorContain: "Error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRDSHandler()
			body := "Action=CreateDBInstance" +
				"&DBInstanceIdentifier=" + tt.dbInstanceID +
				"&DBInstanceClass=" + tt.dbInstanceClass +
				"&Engine=" + tt.engine

			rec := postRDSForm(t, h, body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantErrorContain != "" {
				assert.Contains(t, rec.Body.String(), tt.wantErrorContain)
			}
		})
	}
}
