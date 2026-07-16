package eventbridge_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
)

func TestHandler_ConnectionCRUD(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	rec := auditMakeRequest(t, h, e, "CreateConnection", map[string]any{
		"Name":              "h-conn",
		"AuthorizationType": "BASIC",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "DescribeConnection", map[string]any{"Name": "h-conn"})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "AUTHORIZED")

	rec = auditMakeRequest(t, h, e, "ListConnections", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "UpdateConnection", map[string]any{
		"Name":              "h-conn",
		"AuthorizationType": "API_KEY",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "DeauthorizeConnection", map[string]any{"Name": "h-conn"})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DEAUTHORIZED")

	rec = auditMakeRequest(t, h, e, "DeleteConnection", map[string]any{"Name": "h-conn"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_Connection_AuthParametersRoundtrip(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	rec := auditMakeRequest(t, h, e, "CreateConnection", map[string]any{
		"Name":              "handler-auth-conn",
		"AuthorizationType": "BASIC",
		"AuthParameters": map[string]any{
			"BasicAuthParameters": map[string]any{
				"Username": "testuser",
				"Password": "testpass",
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "DescribeConnection", map[string]any{"Name": "handler-auth-conn"})
	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "testuser")
	// Password must not appear in describe output.
	assert.NotContains(t, body, "testpass")
}
