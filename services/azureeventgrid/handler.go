// Package azureeventgrid emulates Azure Event Grid: custom topics with event
// subscriptions delivering to webhooks, Service Bus, Event Hubs and Storage
// queues (with retries and blob dead-lettering), and namespace topics with
// pull delivery. State is shared with the ARM plane; the HTTPS data plane
// serves the official publisher and namespace SDKs. See PARITY.md.
package azureeventgrid

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
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
	deliveryTick          = time.Second
)

var (
	errTLSKeyMissing  = errors.New("azureeventgrid: --azure-eventgrid-tls-cert set without --azure-eventgrid-tls-key")
	errTLSCertMissing = errors.New("azureeventgrid: --azure-eventgrid-tls-key set without --azure-eventgrid-tls-cert")
)

// Handler owns the HTTPS listener and the delivery worker.
type Handler struct {
	Backend  *InMemoryBackend
	srvMu    *lockmetrics.RWMutex
	srv      *http.Server
	stop     context.CancelFunc
	done     *sync.WaitGroup
	Settings Settings
	Port     int
}

// NewHandler returns a Handler over backend.
func NewHandler(backend *InMemoryBackend, settings Settings) *Handler {
	if settings.Port == 0 {
		settings.Port = DefaultPort
	}

	base := settings.Advertise
	if base == "" {
		base = "https://localhost:" + strconv.Itoa(settings.Port)
	}

	backend.SetValidationBase(base)

	return &Handler{
		Backend: backend, Settings: settings, Port: settings.Port,
		srvMu: lockmetrics.New("azureeventgrid.server"), done: &sync.WaitGroup{},
	}
}

var (
	_ service.BackgroundWorker = (*Handler)(nil)
	_ service.Shutdowner       = (*Handler)(nil)
	_ service.Resettable       = (*Handler)(nil)
)

// Name implements service.Service.
func (h *Handler) Name() string { return "AzureEventGrid" }

// GetSupportedOperations implements service.Service.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opPublishEvents, opPublishCloudEvents, opSendEvents, opReceiveEvents, opAcknowledgeEvents,
		opReleaseEvents, opRejectEvents, opRenewEventLocks, opValidateSubscription,
	}
}

// RouteMatcher never matches: Event Grid listens on its own port.
func (h *Handler) RouteMatcher() service.Matcher { return func(*echo.Context) bool { return false } }

// MatchPriority implements service.Registerable.
func (h *Handler) MatchPriority() int { return 0 }

// Reset implements service.Resettable.
func (h *Handler) Reset() { h.Backend.Reset() }

// ExtractOperation implements service.Service.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	return operationOf(c.Request())
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
			return tls.Certificate{}, fmt.Errorf("azureeventgrid: load TLS certificate: %w", err)
		}

		return cert, nil
	}

	cert, err := devtls.GenerateSelfSignedCert()
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("azureeventgrid: generate self-signed certificate: %w", err)
	}

	return cert, nil
}

// StartWorker binds the HTTPS listener and starts the delivery worker.
func (h *Handler) StartWorker(ctx context.Context) error {
	var lc net.ListenConfig

	ln, err := lc.Listen(ctx, "tcp", fmt.Sprintf(":%d", h.Port))
	if err != nil {
		return fmt.Errorf("azureeventgrid: bind port %d: %w", h.Port, err)
	}

	cert, err := h.loadOrGenerateCert()
	if err != nil {
		_ = ln.Close()

		return err
	}

	e := echo.New()
	e.Use(logger.EchoMiddleware(logger.Load(ctx)))
	e.Any("/*", telemetry.WrapEchoHandler("AzureEventGrid", h.Handler(), h))

	srv := &http.Server{
		Handler:           e,
		ReadHeaderTimeout: httpReadHeaderTimeout,
		ReadTimeout:       httpReadTimeout,
		IdleTimeout:       httpIdleTimeout,
	}
	wctx, cancel := context.WithCancel(logger.WithWorker(ctx, "azureeventgrid", "listener"))

	h.srvMu.Lock("StartWorker")
	h.srv, h.stop = srv, cancel
	h.srvMu.Unlock()

	log := logger.Load(wctx)
	log.InfoContext(wctx, "azureeventgrid: starting dedicated HTTPS listener", "port", h.Port)

	tlsLn := tls.NewListener(ln, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})

	go func() {
		if serveErr := srv.Serve(tlsLn); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.ErrorContext(wctx, "azureeventgrid: listener stopped", "error", serveErr)
		}
	}()

	h.done.Add(1)

	go h.runDelivery(wctx)

	return nil
}

func (h *Handler) runDelivery(ctx context.Context) {
	defer h.done.Done()

	tick := time.NewTicker(deliveryTick)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-h.Backend.kick:
		case <-tick.C:
		}

		h.Backend.RunDue(ctx)
	}
}

// Shutdown implements service.Shutdowner.
func (h *Handler) Shutdown(ctx context.Context) {
	h.srvMu.Lock("Shutdown")
	srv, stop := h.srv, h.stop
	h.srv, h.stop = nil, nil
	h.srvMu.Unlock()

	if stop != nil {
		stop()
	}

	h.done.Wait()

	if srv == nil {
		return
	}

	if err := srv.Shutdown(ctx); err != nil {
		logger.Load(ctx).ErrorContext(ctx, "azureeventgrid: graceful shutdown failed, forcing close", "error", err)
		_ = srv.Close()
	}
}
