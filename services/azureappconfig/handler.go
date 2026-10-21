// Package azureappconfig emulates Azure App Configuration: key-values with labels,
// ETags and sync tokens, revisions and point-in-time reads, locks, snapshots and
// feature flags behind the HMAC-SHA256 authenticated data plane, sharing its
// stores and access keys with the ARM plane. See PARITY.md.
package azureappconfig

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

var (
	errTLSKeyMissing  = errors.New("azureappconfig: --azure-appconfig-tls-cert set without --azure-appconfig-tls-key")
	errTLSCertMissing = errors.New("azureappconfig: --azure-appconfig-tls-key set without --azure-appconfig-tls-cert")
)

// Handler owns the HTTPS listener.
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
		srvMu: lockmetrics.New("azureappconfig.server"), done: &sync.WaitGroup{},
	}
}

var (
	_ service.BackgroundWorker = (*Handler)(nil)
	_ service.Shutdowner       = (*Handler)(nil)
	_ service.Resettable       = (*Handler)(nil)
)

// Name implements service.Service.
func (h *Handler) Name() string { return "AzureAppConfig" }

// GetSupportedOperations implements service.Service.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opGetKeyValue, opPutKeyValue, opDeleteKeyValue, opListKeyValues, opListKeys, opListLabels,
		opListRevisions, opPutLock, opDeleteLock, opCreateSnapshot, opGetSnapshot, opUpdateSnapshot,
		opListSnapshots, opGetOperation,
	}
}

// RouteMatcher never matches: App Configuration listens on its own port.
func (h *Handler) RouteMatcher() service.Matcher { return func(*echo.Context) bool { return false } }

// MatchPriority implements service.Registerable.
func (h *Handler) MatchPriority() int { return 0 }

// Reset implements service.Resettable.
func (h *Handler) Reset() { h.Backend.Reset() }

// ExtractOperation implements service.Service.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	_, path := splitStorePrefix(c.Request().URL.EscapedPath())

	return operationOf(c.Request().Method, routeSegments(path))
}

// ExtractResource implements service.Service.
func (h *Handler) ExtractResource(c *echo.Context) string { return c.Request().URL.Path }

func (h *Handler) loadOrGenerateCert() (tls.Certificate, error) {
	certFile, keyFile := h.Settings.TLSCertFile, h.Settings.TLSKeyFile

	switch {
	case certFile != "" && keyFile == "":
		return tls.Certificate{}, errTLSKeyMissing
	case certFile == "" && keyFile != "":
		return tls.Certificate{}, errTLSCertMissing
	case certFile != "":
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return tls.Certificate{}, fmt.Errorf("azureappconfig: load TLS certificate: %w", err)
		}

		return cert, nil
	}

	cert, err := devtls.GenerateSelfSignedCert()
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("azureappconfig: generate self-signed certificate: %w", err)
	}

	return cert, nil
}

// StartWorker binds the HTTPS listener.
func (h *Handler) StartWorker(ctx context.Context) error {
	var lc net.ListenConfig

	ln, err := lc.Listen(ctx, "tcp", fmt.Sprintf(":%d", h.Port))
	if err != nil {
		return fmt.Errorf("azureappconfig: bind port %d: %w", h.Port, err)
	}

	cert, err := h.loadOrGenerateCert()
	if err != nil {
		_ = ln.Close()

		return err
	}

	e := echo.New()
	e.Use(logger.EchoMiddleware(logger.Load(ctx)))
	e.Any("/*", telemetry.WrapEchoHandler("AzureAppConfig", h.Handler(), h))

	srv := &http.Server{
		Handler:           e,
		ReadHeaderTimeout: httpReadHeaderTimeout,
		ReadTimeout:       httpReadTimeout,
		IdleTimeout:       httpIdleTimeout,
	}

	h.srvMu.Lock("StartWorker")
	h.srv = srv
	h.srvMu.Unlock()

	log := logger.Load(ctx)
	log.InfoContext(ctx, "azureappconfig: starting dedicated HTTPS listener", "port", h.Port)

	tlsLn := tls.NewListener(ln, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})

	h.done.Go(func() {
		if serveErr := srv.Serve(tlsLn); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.ErrorContext(ctx, "azureappconfig: listener stopped", "error", serveErr)
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

	if srv == nil {
		return
	}

	if err := srv.Shutdown(ctx); err != nil {
		logger.Load(ctx).ErrorContext(ctx, "azureappconfig: graceful shutdown failed, forcing close", "error", err)
		_ = srv.Close()
	}

	h.done.Wait()
}
