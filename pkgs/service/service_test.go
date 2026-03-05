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

	tests := []struct {
		wantErr    error
		setup      func(*service.Registry)
		register   *MockService
		name       string
		wantByName string
		wantCount  int
		wantNonNil bool
	}{
		{
			name:       "register new service",
			register:   &MockService{name: "S1", priority: 10},
			wantCount:  1,
			wantByName: "S1",
			wantNonNil: true,
		},
		{
			name: "duplicate registration returns error",
			setup: func(reg *service.Registry) {
				_ = reg.Register(&MockService{name: "S1", priority: 10})
			},
			register:   &MockService{name: "S1", priority: 10},
			wantCount:  1,
			wantByName: "S1",
			wantErr:    service.ErrServiceAlreadyRegistered,
			wantNonNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			reg := service.NewRegistry(slog.Default())

			if tt.setup != nil {
				tt.setup(reg)
			}

			err := reg.Register(tt.register)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantCount, reg.Count())

			got := reg.GetByName(tt.wantByName)
			if tt.wantNonNil {
				assert.NotNil(t, got)
			} else {
				assert.Nil(t, got)
			}
		})
	}
}

func TestRegistryMiddleware(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		middlewares []service.Middleware
		wantCalled  bool
	}{
		{
			name: "middleware is invoked on wrapped handler",
			middlewares: []service.Middleware{
				func(next echo.HandlerFunc) echo.HandlerFunc {
					return func(c *echo.Context) error {
						c.Set("called", true)

						return next(c)
					}
				},
			},
			wantCalled: true,
		},
		{
			name:        "no middleware still executes handler",
			middlewares: nil,
			wantCalled:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			reg := service.NewRegistry(slog.Default())

			for _, mw := range tt.middlewares {
				reg.Use(mw)
			}

			svc := &MockService{name: "S1", matched: true}
			require.NoError(t, reg.Register(svc))

			entry := reg.GetByName("S1")
			require.NotNil(t, entry)

			e := echo.New()
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := entry.WrappedHandler(c)
			require.NoError(t, err)

			if tt.wantCalled {
				assert.Equal(t, true, c.Get("called"))
			} else {
				assert.Nil(t, c.Get("called"))
			}
		})
	}
}

func TestServiceRouter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantBody string
		services []*MockService
	}{
		{
			name: "routes to highest priority matching service",
			services: []*MockService{
				{name: "S1", priority: 10, matched: false},
				{name: "S2", priority: 20, matched: true},
				{name: "S3", priority: 5, matched: true},
			},
			wantBody: "S2",
		},
		{
			name: "falls back when no service matches",
			services: []*MockService{
				{name: "S1", matched: false},
			},
			wantBody: "fallback",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			reg := service.NewRegistry(slog.Default())

			for _, svc := range tt.services {
				require.NoError(t, reg.Register(svc))
			}

			router := service.NewServiceRouter(reg)

			e := echo.New()
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			handler := router.RouteHandler()(func(c *echo.Context) error {
				return c.String(http.StatusOK, "fallback")
			})

			err := handler(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantBody, rec.Body.String())
		})
	}
}
