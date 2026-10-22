package azuresql

import (
	"context"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Handler owns the SQL servers. Azure SQL has no HTTP data plane: ARM manages servers and clients speak TDS to
// each server's own port, so the handler never matches a request.
type Handler struct {
	Backend  *InMemoryBackend
	launcher *dockerLauncher
}

var (
	_ service.Shutdowner = (*Handler)(nil)
	_ service.Resettable = (*Handler)(nil)
)

// NewHandler wraps backend.
func NewHandler(backend *InMemoryBackend) *Handler { return &Handler{Backend: backend} }

// Name returns the service name.
func (h *Handler) Name() string { return "AzureSQL" }

// GetSupportedOperations lists the operations ARM's Microsoft.Sql provider drives.
func (h *Handler) GetSupportedOperations() []string {
	return []string{"Ensure", "Delete", "EnsureDatabase", "DropDatabase"}
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
		return c.String(http.StatusNotFound, "azuresql is managed through Microsoft.Sql on the ARM listener")
	}
}

// Reset stops every server.
func (h *Handler) Reset() { h.Backend.Reset() }

// Shutdown stops every server and releases the runtime client.
func (h *Handler) Shutdown(context.Context) {
	h.Backend.Close()

	if h.launcher != nil {
		h.launcher.Close()
	}
}

// Snapshot implements persistence.Persistable.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("azuresql: restore snapshot: %w", err)
	}

	return nil
}
