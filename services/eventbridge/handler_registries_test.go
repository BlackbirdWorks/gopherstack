package eventbridge_test

import (
	"net/http"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

func TestHandler_SchemaRegistry_CRUD(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	rec := auditMakeRequest(t, h, e, "CreateRegistry", map[string]any{
		"RegistryName": "h-registry",
		"Description":  "handler test",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "h-registry")

	rec = auditMakeRequest(t, h, e, "DescribeRegistry", map[string]any{
		"RegistryName": "h-registry",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "ListRegistries", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "h-registry")

	rec = auditMakeRequest(t, h, e, "UpdateRegistry", map[string]any{
		"RegistryName": "h-registry",
		"Description":  "updated",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "DeleteRegistry", map[string]any{
		"RegistryName": "h-registry",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "DescribeRegistry", map[string]any{
		"RegistryName": "h-registry",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_BuiltinRegistry_Returns403(t *testing.T) {
	t.Parallel()

	builtinNames := []string{"aws.events", "discovered-schemas"}

	for _, name := range builtinNames {
		t.Run("delete-"+name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			b := newBackend()
			h := eventbridge.NewHandler(b)

			rec := auditMakeRequest(t, h, e, "DeleteRegistry", map[string]any{
				"RegistryName": name,
			})
			assert.Equal(t, http.StatusForbidden, rec.Code)
			assert.Contains(t, rec.Body.String(), "ForbiddenException")
		})

		t.Run("create-"+name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			b := newBackend()
			h := eventbridge.NewHandler(b)

			rec := auditMakeRequest(t, h, e, "CreateRegistry", map[string]any{
				"RegistryName": name,
			})
			assert.Equal(t, http.StatusForbidden, rec.Code)
			assert.Contains(t, rec.Body.String(), "ForbiddenException")
		})
	}
}
