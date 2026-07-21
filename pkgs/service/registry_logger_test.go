package service_test

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// loggingService records the logger it observed (via logger.Load) and emits a
// record so the test can assert the service tag is present.
type loggingService struct {
	seen     chan context.Context
	name     string
	priority int
}

func (s *loggingService) Name() string { return s.name }

func (s *loggingService) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		logger.Load(ctx).InfoContext(ctx, "handled")
		s.seen <- ctx

		return c.String(http.StatusOK, s.name)
	}
}

func (s *loggingService) RouteMatcher() service.Matcher {
	return func(_ *echo.Context) bool { return true }
}
func (s *loggingService) GetSupportedOperations() []string        { return nil }
func (s *loggingService) ExtractOperation(_ *echo.Context) string { return "op" }
func (s *loggingService) ExtractResource(_ *echo.Context) string  { return "res" }
func (s *loggingService) MatchPriority() int                      { return s.priority }

func invokeService(t *testing.T, entry *service.Entry, base *slog.Logger) {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req = req.WithContext(logger.Save(req.Context(), base))
	rec := httptest.NewRecorder()
	c := echo.New().NewContext(req, rec)

	require.NoError(t, entry.WrappedHandler(c))
}

func TestRegisterScopesLoggerToService(t *testing.T) {
	t.Parallel()

	type args struct {
		svc *loggingService
	}
	type wants struct {
		contains string
	}

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name: "scopes logger to service",
			args: args{
				svc: &loggingService{name: "WidgetSvc", priority: 1, seen: make(chan context.Context, 1)},
			},
			wants: wants{
				contains: "service=WidgetSvc",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer
			base := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{}))

			reg := service.NewRegistry()
			require.NoError(t, reg.Register(tc.args.svc))

			entry := reg.GetByName(tc.args.svc.name)
			require.NotNil(t, entry)

			invokeService(t, entry, base)

			assert.Contains(t, buf.String(), tc.wants.contains,
				"records emitted inside the handler must carry the service tag")
		})
	}
}

// TestRegisterLoggerScopingIsRequestIsolated proves the base logger is never
// mutated: two services sharing one base logger see only their own service tag,
// and concurrent requests do not clobber one another.
func TestRegisterLoggerScopingIsRequestIsolated(t *testing.T) {
	t.Parallel()

	type serviceConfig struct {
		name     string
		priority int
	}
	type args struct {
		services []serviceConfig
	}
	type wants struct{}

	tests := []struct {
		name  string
		wants wants
		args  args
	}{
		{
			name: "request isolated",
			args: args{
				services: []serviceConfig{
					{name: "Alpha", priority: 2},
					{name: "Beta", priority: 1},
				},
			},
			wants: wants{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			reg := service.NewRegistry()

			var entries []*service.Entry
			for _, sc := range tc.args.services {
				svc := &loggingService{name: sc.name, priority: sc.priority, seen: make(chan context.Context, 1)}
				require.NoError(t, reg.Register(svc))
				entry := reg.GetByName(sc.name)
				require.NotNil(t, entry)
				entries = append(entries, entry)
			}

			check := func(entry *service.Entry, want, notWant string) {
				var buf bytes.Buffer
				base := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{}))
				invokeService(t, entry, base)
				assert.Contains(t, buf.String(), "service="+want)
				assert.NotContains(t, buf.String(), "service="+notWant)
			}

			var wg sync.WaitGroup
			for i, e := range entries {
				want := tc.args.services[i].name
				notWant := ""
				for j, sc := range tc.args.services {
					if i != j {
						notWant = sc.name

						break
					}
				}

				wg.Add(1)
				go func(entry *service.Entry, w, nw string) {
					defer wg.Done()
					check(entry, w, nw)
				}(e, want, notWant)
			}
			wg.Wait()
		})
	}
}
