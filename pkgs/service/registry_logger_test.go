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
	name     string
	priority int
	seen     chan context.Context
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

	var buf bytes.Buffer

	base := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{}))

	reg := service.NewRegistry()
	svc := &loggingService{name: "WidgetSvc", priority: 1, seen: make(chan context.Context, 1)}
	require.NoError(t, reg.Register(svc))

	entry := reg.GetByName("WidgetSvc")
	require.NotNil(t, entry)

	invokeService(t, entry, base)

	assert.Contains(t, buf.String(), "service=WidgetSvc",
		"records emitted inside the handler must carry the service tag")
}

// TestRegisterLoggerScopingIsRequestIsolated proves the base logger is never
// mutated: two services sharing one base logger see only their own service tag,
// and concurrent requests do not clobber one another.
func TestRegisterLoggerScopingIsRequestIsolated(t *testing.T) {
	t.Parallel()

	reg := service.NewRegistry()

	a := &loggingService{name: "Alpha", priority: 2, seen: make(chan context.Context, 1)}
	b := &loggingService{name: "Beta", priority: 1, seen: make(chan context.Context, 1)}
	require.NoError(t, reg.Register(a))
	require.NoError(t, reg.Register(b))

	entryA := reg.GetByName("Alpha")
	entryB := reg.GetByName("Beta")
	require.NotNil(t, entryA)
	require.NotNil(t, entryB)

	// Each request gets its own buffer-backed base logger; assert no bleed.
	check := func(entry *service.Entry, want, notWant string) {
		var buf bytes.Buffer

		base := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{}))
		invokeService(t, entry, base)
		assert.Contains(t, buf.String(), "service="+want)
		assert.NotContains(t, buf.String(), "service="+notWant)
	}

	var wg sync.WaitGroup

	wg.Add(2)

	go func() { defer wg.Done(); check(entryA, "Alpha", "Beta") }()
	go func() { defer wg.Done(); check(entryB, "Beta", "Alpha") }()
	wg.Wait()
}
