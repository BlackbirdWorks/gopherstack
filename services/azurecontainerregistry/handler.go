// Package azurecontainerregistry emulates the Azure Container Registry data plane: a Docker Registry v2 API
// per registry, the OAuth2 refresh and access token endpoints az acr login uses, admin-user Basic
// authentication and the /acr/v1 repository, tag and manifest metadata API. Registries are created through
// the ARM Microsoft.ContainerRegistry provider, which shares their credentials. See PARITY.md.
package azurecontainerregistry

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
)

const (
	httpReadHeaderTimeout = 10 * time.Second
	httpIdleTimeout       = 120 * time.Second
)

// Handler owns the registry listener.
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
		srvMu: lockmetrics.New("azurecontainerregistry.server"), done: &sync.WaitGroup{},
	}
}

var (
	_ service.BackgroundWorker = (*Handler)(nil)
	_ service.Shutdowner       = (*Handler)(nil)
	_ service.Resettable       = (*Handler)(nil)
)

// Name implements service.Service.
func (h *Handler) Name() string { return "AzureContainerRegistry" }

// GetSupportedOperations implements service.Service.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CheckAPIVersion", "GetCatalog", "ListTags", "GetManifest", "PutManifest", "DeleteManifest",
		"StartBlobUpload", "UploadBlobChunk", "CompleteBlobUpload", "GetBlob", "DeleteBlob",
		"ExchangeToken", "GetAccessToken",
		"ListRepositories", "GetRepositoryAttributes", "UpdateRepositoryAttributes", "DeleteRepository",
		"ListTagAttributes", "GetTagAttributes", "UpdateTagAttributes", "DeleteTag",
		"ListManifestAttributes", "GetManifestAttributes", "UpdateManifestAttributes",
	}
}

// RouteMatcher never matches: the registry listens on its own port.
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
		return c.String(http.StatusNotFound, "azurecontainerregistry serves on its dedicated port")
	}
}

func (h *Handler) tlsConfig() (*tls.Config, error) {
	certFile, keyFile := h.Settings.TLSCertFile, h.Settings.TLSKeyFile

	var (
		cert tls.Certificate
		err  error
	)

	switch {
	case certFile == "" && keyFile == "":
		return nil, nil //nolint:nilnil // plain HTTP
	case certFile != "" && keyFile == "":
		return nil, ErrTLSKeyMissing
	case certFile == "":
		return nil, ErrTLSCertMissing
	default:
		cert, err = tls.LoadX509KeyPair(certFile, keyFile)
	}

	if err != nil {
		return nil, fmt.Errorf("azurecontainerregistry: load TLS certificate: %w", err)
	}

	return &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}, nil
}

// SelfSignedConfig returns a TLS config with a generated certificate, for callers that want HTTPS in tests.
func SelfSignedConfig() (*tls.Config, error) {
	cert, err := devtls.GenerateSelfSignedCert()
	if err != nil {
		return nil, fmt.Errorf("azurecontainerregistry: generate self-signed certificate: %w", err)
	}

	return &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}, nil
}

// StartWorker binds the listener synchronously so a port conflict is returned to the caller.
func (h *Handler) StartWorker(ctx context.Context) error {
	tlsCfg, err := h.tlsConfig()
	if err != nil {
		return err
	}

	var lc net.ListenConfig

	ln, err := lc.Listen(ctx, "tcp", fmt.Sprintf(":%d", h.Port))
	if err != nil {
		return fmt.Errorf("azurecontainerregistry: bind port %d: %w", h.Port, err)
	}

	if tlsCfg != nil {
		ln = tls.NewListener(ln, tlsCfg)
	}

	srv := &http.Server{
		Handler: h, ReadHeaderTimeout: httpReadHeaderTimeout, IdleTimeout: httpIdleTimeout,
		BaseContext: func(net.Listener) context.Context { return context.WithoutCancel(ctx) },
	}

	h.srvMu.Lock("StartWorker")
	h.srv = srv
	h.srvMu.Unlock()

	log := logger.Load(ctx)
	log.InfoContext(ctx, "azurecontainerregistry: starting dedicated listener", "port", h.Port, "tls", tlsCfg != nil)

	h.done.Go(func() {
		if serveErr := srv.Serve(ln); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.ErrorContext(ctx, "azurecontainerregistry: listener stopped", "error", serveErr)
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
			logger.Load(ctx).ErrorContext(ctx, "azurecontainerregistry: graceful shutdown failed", "error", err)
			_ = srv.Close()
		}

		h.done.Wait()
	}

	h.Backend.Close()
}
