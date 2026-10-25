// Package azurefunctions emulates Azure Functions: function apps whose HTTP-triggered functions run in the
// official Azure Functions host containers behind <app>.<domain>/api/<function>, with the host's own
// authLevel key enforcement, and the Kudu zip-deploy API on <app>.scm.<domain>. Apps are created through the ARM
// Microsoft.Web provider, which shares their settings, keys and packages. See PARITY.md.
package azurefunctions

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	httpReadHeaderTimeout = 10 * time.Second
	httpIdleTimeout       = 120 * time.Second
)

// Handler owns the function-app listener.
type Handler struct {
	Backend  *InMemoryBackend
	srvMu    *lockmetrics.RWMutex
	srv      *http.Server
	done     *sync.WaitGroup
	Settings Settings
	Port     int
}

// NewHandler returns a Handler over backend.
func NewHandler(backend *InMemoryBackend, settings Settings) *Handler {
	if settings.Port == 0 {
		settings.Port = DefaultPort
	}

	return &Handler{
		Backend: backend, Settings: settings, Port: settings.Port,
		srvMu: lockmetrics.New("azurefunctions.server"), done: &sync.WaitGroup{},
	}
}

var (
	_ service.BackgroundWorker = (*Handler)(nil)
	_ service.Shutdowner       = (*Handler)(nil)
	_ service.Resettable       = (*Handler)(nil)
)

// Name implements service.Service.
func (h *Handler) Name() string { return "AzureFunctions" }

// GetSupportedOperations implements service.Service.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"InvokeHTTPFunction", "ZipDeploy", "OneDeploy", "ListDeployments", "GetDeployment", "DownloadSite",
	}
}

// RouteMatcher never matches: function apps listen on their own port.
func (h *Handler) RouteMatcher() service.Matcher { return func(*echo.Context) bool { return false } }

// MatchPriority implements service.Registerable.
func (h *Handler) MatchPriority() int { return 0 }

// ExtractOperation implements service.Service.
func (h *Handler) ExtractOperation(*echo.Context) string { return "Unknown" }

// ExtractResource implements service.Service.
func (h *Handler) ExtractResource(*echo.Context) string { return "" }

// Reset implements service.Resettable.
func (h *Handler) Reset() { h.Backend.Reset() }

// Handler returns a 404 handler; the real API is on the dedicated port.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return c.String(http.StatusNotFound, "azurefunctions serves on its dedicated port")
	}
}

// ServeHTTP serves the dedicated listener.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) { h.Backend.ServeHTTP(w, r) }

// StartWorker binds the listener synchronously so a port conflict is returned to the caller.
func (h *Handler) StartWorker(ctx context.Context) error {
	var lc net.ListenConfig

	ln, err := lc.Listen(ctx, "tcp", fmt.Sprintf(":%d", h.Port))
	if err != nil {
		return fmt.Errorf("azurefunctions: bind port %d: %w", h.Port, err)
	}

	h.Backend.setRunCtx(context.WithoutCancel(ctx))

	srv := &http.Server{
		Handler: h, ReadHeaderTimeout: httpReadHeaderTimeout, IdleTimeout: httpIdleTimeout,
		BaseContext: func(net.Listener) context.Context { return context.WithoutCancel(ctx) },
	}

	h.srvMu.Lock("StartWorker")
	h.srv = srv
	h.srvMu.Unlock()

	log := logger.Load(ctx)
	log.InfoContext(ctx, "azurefunctions: starting dedicated listener", "port", h.Port)

	h.done.Go(func() {
		if serveErr := srv.Serve(ln); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.ErrorContext(ctx, "azurefunctions: listener stopped", "error", serveErr)
		}
	})

	return nil
}

// Shutdown implements service.Shutdowner.
func (h *Handler) Shutdown(ctx context.Context) {
	h.srvMu.Lock("Shutdown")
	srv := h.srv
	h.srv = nil
	h.srvMu.Unlock()

	if srv != nil {
		if err := srv.Shutdown(ctx); err != nil {
			logger.Load(ctx).ErrorContext(ctx, "azurefunctions: graceful shutdown failed", "error", err)
			_ = srv.Close()
		}

		h.done.Wait()
	}

	h.Backend.Close()
}
