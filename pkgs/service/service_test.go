package service_test

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type MockService struct {
	name     string
	priority int
	matched  bool
}

func (m *MockService) Name() string { return m.name }
func (m *MockService) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return c.String(http.StatusOK, m.name)
	}
}
func (m *MockService) RouteMatcher() service.Matcher {
	return func(_ *echo.Context) bool {
		return m.matched
	}
}
func (m *MockService) GetSupportedOperations() []string        { return nil }
func (m *MockService) ExtractOperation(_ *echo.Context) string { return "op" }
func (m *MockService) ExtractResource(_ *echo.Context) string  { return "res" }
func (m *MockService) MatchPriority() int                      { return m.priority }

func TestRegistry(t *testing.T) {
	t.Parallel()
	logger := slog.Default()
	reg := service.NewRegistry(logger)

	svc := &MockService{name: "S1", priority: 10}
	err := reg.Register(svc)
	require.NoError(t, err)

	assert.Equal(t, 1, reg.Count())
	assert.NotNil(t, reg.GetByName("S1"))

	// Duplicate registration
	err = reg.Register(svc)
	assert.ErrorIs(t, err, service.ErrServiceAlreadyRegistered)
}

func TestRegistryMiddleware(t *testing.T) {
	t.Parallel()
	reg := service.NewRegistry(slog.Default())

	var called bool
	mw := func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			called = true

			return next(c)
		}
	}

	reg.Use(mw)
	svc := &MockService{name: "S1", matched: true}
	_ = reg.Register(svc)

	entry := reg.GetByName("S1")
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := entry.WrappedHandler(c)
	require.NoError(t, err)
	assert.True(t, called)
}

func TestRouter(t *testing.T) {
	t.Parallel()
	reg := service.NewRegistry(slog.Default())

	s1 := &MockService{name: "S1", priority: 10, matched: false}
	s2 := &MockService{name: "S2", priority: 20, matched: true}
	s3 := &MockService{name: "S3", priority: 5, matched: true}

	_ = reg.Register(s1)
	_ = reg.Register(s2)
	_ = reg.Register(s3)

	router := service.NewServiceRouter(reg)

	// Verify routing
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	handler := router.RouteHandler()(func(c *echo.Context) error {
		return c.String(http.StatusOK, "fallback")
	})

	err := handler(c)
	require.NoError(t, err)
	assert.Equal(t, "S2", rec.Body.String()) // S2 matched first by priority
}

func TestRouterFallback(t *testing.T) {
	t.Parallel()
	reg := service.NewRegistry(slog.Default())
	s1 := &MockService{name: "S1", matched: false}
	_ = reg.Register(s1)

	router := service.NewServiceRouter(reg)

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	handler := router.RouteHandler()(func(c *echo.Context) error {
		return c.String(http.StatusOK, "fallback")
	})

	_ = handler(c)
	assert.Equal(t, "fallback", rec.Body.String())
}
