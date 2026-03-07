package dynamodb_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

func TestDashboardProvider(t *testing.T) {
	t.Parallel()

	t.Run("NewDashboardProvider creates provider", func(t *testing.T) {
		t.Parallel()

		p := dynamodb.NewDashboardProvider()
		require.NotNil(t, p)
	})

	t.Run("DashboardName returns DynamoDB", func(t *testing.T) {
		t.Parallel()

		p := dynamodb.NewDashboardProvider()
		assert.Equal(t, "DynamoDB", p.DashboardName())
	})

	t.Run("DashboardRoutePrefix returns dynamodb", func(t *testing.T) {
		t.Parallel()

		p := dynamodb.NewDashboardProvider()
		assert.Equal(t, "dynamodb", p.DashboardRoutePrefix())
	})

	t.Run("RegisterDashboardRoutes registers routes", func(t *testing.T) {
		t.Parallel()

		p := dynamodb.NewDashboardProvider()
		called := false
		p.Handlers.HandleDynamoDB = func(_ http.ResponseWriter, _ *http.Request, _ string) {
			called = true
		}

		e := echo.New()
		group := e.Group("/dynamodb")
		p.RegisterDashboardRoutes(group, nil, "")

		// Invoke the catch-all route
		req := httptest.NewRequest(http.MethodGet, "/dynamodb/tables", nil)
		w := httptest.NewRecorder()
		e.ServeHTTP(w, req)

		assert.True(t, called, "HandleDynamoDB should have been called")
	})
}
