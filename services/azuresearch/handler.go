// Package azuresearch emulates the Azure AI Search data plane: indexes, documents, full-text,
// filter, facet, vector and hybrid queries, suggesters, synonym maps, data sources, indexers
// and skillsets, authenticated with admin and query api-keys that the ARM plane manages.
// See PARITY.md.
package azuresearch

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/devtls"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

const (
	httpReadHeaderTimeout = 10 * time.Second
	httpReadTimeout       = 60 * time.Second
	httpIdleTimeout       = 120 * time.Second
)

// Handler owns the HTTPS listener and the indexer scheduler.
type Handler struct {
	Backend  *InMemoryBackend
	srvMu    *lockmetrics.RWMutex
	srv      *http.Server
	cancel   context.CancelFunc
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
		srvMu: lockmetrics.New("azuresearch.server"), done: &sync.WaitGroup{},
	}
}

var (
	_ service.BackgroundWorker = (*Handler)(nil)
	_ service.Shutdowner       = (*Handler)(nil)
	_ service.Resettable       = (*Handler)(nil)
)

// Name implements service.Service.
func (h *Handler) Name() string { return "AzureSearch" }

// GetSupportedOperations implements service.Service.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opCreateIndex, opGetIndex, opListIndexes, opDeleteIndex, opIndexStatistics, opAnalyzeText,
		opSearch, opIndexDocuments, opGetDocument, opCountDocuments, opSuggest, opAutocomplete,
		opSynonymMap, opDataSource, opIndexer, opRunIndexer, opResetIndexer, opIndexerStatus,
		opSkillset, opServiceStatistics,
	}
}

// RouteMatcher never matches: Azure AI Search listens on its own port.
func (h *Handler) RouteMatcher() service.Matcher { return func(*echo.Context) bool { return false } }

// MatchPriority implements service.Registerable.
func (h *Handler) MatchPriority() int { return 0 }

// Reset implements service.Resettable.
func (h *Handler) Reset() { h.Backend.Reset() }

// ExtractOperation implements service.Service.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	return operationOf(c.Request().Method, parseSegments(c.Request().URL.Path))
}

// ExtractResource implements service.Service.
func (h *Handler) ExtractResource(c *echo.Context) string { return c.Request().URL.Path }

func (h *Handler) generateCert() (tls.Certificate, error) {
	cert, err := devtls.GenerateSelfSignedCert()
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("azuresearch: generate self-signed certificate: %w", err)
	}

	return cert, nil
}

// StartWorker binds the HTTPS listener and starts the indexer scheduler.
func (h *Handler) StartWorker(ctx context.Context) error {
	var lc net.ListenConfig

	ln, err := lc.Listen(ctx, "tcp", fmt.Sprintf(":%d", h.Port))
	if err != nil {
		return fmt.Errorf("azuresearch: bind port %d: %w", h.Port, err)
	}

	cert, err := h.generateCert()
	if err != nil {
		_ = ln.Close()

		return err
	}

	e := echo.New()
	e.Use(logger.EchoMiddleware(logger.Load(ctx)))
	e.Any("/*", telemetry.WrapEchoHandler("AzureSearch", h.Handler(), h))

	srv := &http.Server{
		Handler:           e,
		ReadHeaderTimeout: httpReadHeaderTimeout,
		ReadTimeout:       httpReadTimeout,
		IdleTimeout:       httpIdleTimeout,
	}

	schedCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))

	h.srvMu.Lock("StartWorker")
	h.srv, h.cancel = srv, cancel
	h.srvMu.Unlock()

	log := logger.Load(ctx)
	log.InfoContext(ctx, "azuresearch: starting dedicated HTTPS listener", "port", h.Port)

	tlsLn := tls.NewListener(ln, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})

	h.done.Go(func() {
		if serveErr := srv.Serve(tlsLn); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.ErrorContext(ctx, "azuresearch: listener stopped", "error", serveErr)
		}
	})
	h.done.Go(func() { h.schedule(schedCtx) })

	return nil
}

func (h *Handler) schedule(ctx context.Context) {
	t := time.NewTicker(DefaultPollInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			h.Backend.RunDue()
		}
	}
}

// Shutdown implements service.Shutdowner.
func (h *Handler) Shutdown(ctx context.Context) {
	h.srvMu.Lock("Shutdown")
	srv, cancel := h.srv, h.cancel
	h.srv, h.cancel = nil, nil
	h.srvMu.Unlock()

	if cancel != nil {
		cancel()
	}

	if srv == nil {
		return
	}

	if err := srv.Shutdown(ctx); err != nil {
		logger.Load(ctx).ErrorContext(ctx, "azuresearch: graceful shutdown failed, forcing close", "error", err)
		_ = srv.Close()
	}

	h.done.Wait()
}
