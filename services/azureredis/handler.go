package azureredis

import (
	"context"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Handler owns the cache servers. Azure Cache for Redis has no HTTP data plane: ARM manages caches and clients
// speak Redis to each cache's own ports, so the handler never matches a request.
type Handler struct {
	Backend *InMemoryBackend
}

var (
	_ service.Shutdowner = (*Handler)(nil)
	_ service.Resettable = (*Handler)(nil)
)

// NewHandler wraps backend.
func NewHandler(backend *InMemoryBackend) *Handler { return &Handler{Backend: backend} }

// Name returns the service name.
func (h *Handler) Name() string { return "AzureRedis" }

// GetSupportedOperations lists the operations ARM's Microsoft.Cache provider drives.
func (h *Handler) GetSupportedOperations() []string {
	return []string{"Ensure", "Delete", "Keys", "RegenerateKey", "Reboot", "Import", "Export"}
}

// RouteMatcher never matches.
func (h *Handler) RouteMatcher() service.Matcher { return func(*echo.Context) bool { return false } }

// MatchPriority is irrelevant because RouteMatcher never matches.
func (h *Handler) MatchPriority() int { return 0 }

// ExtractOperation is unused: requests never reach the shared router.
func (h *Handler) ExtractOperation(*echo.Context) string { return "Unknown" }

// ExtractResource is unused: requests never reach the shared router.
func (h *Handler) ExtractResource(*echo.Context) string { return "" }

// Handler returns a 404 handler.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return c.String(http.StatusNotFound, "azureredis is managed through Microsoft.Cache on the ARM listener")
	}
}

// Reset stops every cache.
func (h *Handler) Reset() { h.Backend.Reset() }

// Shutdown stops every cache.
func (h *Handler) Shutdown(context.Context) { h.Backend.Close() }

// Snapshot implements persistence.Persistable.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("azureredis: restore snapshot: %w", err)
	}

	return nil
}
