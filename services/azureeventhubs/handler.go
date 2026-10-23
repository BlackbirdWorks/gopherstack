// Package azureeventhubs emulates Azure Event Hubs: namespaces, event hubs,
// consumer groups and authorization rules shared with the ARM plane, an AMQP
// 1.0 data plane (pkgs/amqp1) for the official SDKs, and a small REST/Atom
// surface. The Kafka endpoint is not implemented. See PARITY.md.
package azureeventhubs

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

const (
	opCreateHub           = "CreateEventHub"
	opGetHub              = "GetEventHub"
	opDeleteHub           = "DeleteEventHub"
	opListHubs            = "ListEventHubs"
	opCreateConsumerGroup = "CreateConsumerGroup"
	opGetConsumerGroup    = "GetConsumerGroup"
	opDeleteConsumerGroup = "DeleteConsumerGroup"
	opListConsumerGroups  = "ListConsumerGroups"
	opSendEvent           = "SendEvent"
	unknownOperation      = "Unknown"

	httpReadHeaderTimeout = 10 * time.Second
	httpReadTimeout       = 60 * time.Second
	httpIdleTimeout       = 120 * time.Second
	janitorInterval       = 30 * time.Second
)

// Handler owns the HTTP listener and the AMQP servers of the Event Hubs emulator.
type Handler struct {
	Backend     *InMemoryBackend
	srv         *http.Server
	AMQPCert    *tls.Certificate
	amqp        *amqpService
	amqpMu      *lockmetrics.RWMutex
	Port        int
	AMQPPort    int
	AMQPTLSPort int
	ValidateSAS bool
}

// NewHandler returns a Handler over backend with default ports.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{
		Backend:     backend,
		Port:        DefaultPort,
		AMQPPort:    DefaultAMQPPort,
		AMQPTLSPort: DefaultAMQPTLSPort,
		amqpMu:      lockmetrics.New("azureeventhubs.amqp"),
	}
}

var (
	_ service.BackgroundWorker = (*Handler)(nil)
	_ service.Shutdowner       = (*Handler)(nil)
	_ service.Resettable       = (*Handler)(nil)
)

// Name implements service.Service.
func (h *Handler) Name() string { return "AzureEventHubs" }

// GetSupportedOperations implements service.Service.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opCreateHub, opGetHub, opDeleteHub, opListHubs,
		opCreateConsumerGroup, opGetConsumerGroup, opDeleteConsumerGroup, opListConsumerGroups, opSendEvent,
	}
}

// RouteMatcher never matches: Event Hubs listens on dedicated ports.
func (h *Handler) RouteMatcher() service.Matcher { return func(*echo.Context) bool { return false } }

// MatchPriority implements service.Registerable.
func (h *Handler) MatchPriority() int { return 0 }

// Reset implements service.Resettable.
func (h *Handler) Reset() { h.Backend.Reset() }

// ExtractOperation implements service.Service.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	req, err := parsePath(c.Request().URL.Path)
	if err != nil {
		return unknownOperation
	}

	return operationFor(c.Request().Method, req)
}

// ExtractResource implements service.Service.
func (h *Handler) ExtractResource(c *echo.Context) string {
	req, err := parsePath(c.Request().URL.Path)
	if err != nil {
		return ""
	}

	return req.hub
}

// StartWorker binds the HTTP listener and the AMQP listeners.
func (h *Handler) StartWorker(ctx context.Context) error {
	var lc net.ListenConfig

	ln, err := lc.Listen(ctx, "tcp", fmt.Sprintf(":%d", h.Port))
	if err != nil {
		return fmt.Errorf("azureeventhubs: bind port %d: %w", h.Port, err)
	}

	e := echo.New()
	e.Use(logger.EchoMiddleware(logger.Load(ctx)))
	e.Any("/*", telemetry.WrapEchoHandler("AzureEventHubs", h.Handler(), h))

	h.srv = &http.Server{
		Handler:           e,
		ReadHeaderTimeout: httpReadHeaderTimeout,
		ReadTimeout:       httpReadTimeout,
		IdleTimeout:       httpIdleTimeout,
	}

	srv := h.srv
	wctx := logger.WithWorker(ctx, "azureeventhubs", "listener")
	log := logger.Load(wctx)
	log.InfoContext(wctx, "azureeventhubs: starting dedicated listener", "port", h.Port)

	go func() {
		if serveErr := srv.Serve(ln); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.ErrorContext(wctx, "azureeventhubs: listener stopped", "error", serveErr)
		}
	}()

	go h.runJanitor(ctx)

	return h.startAMQP(ctx)
}

func (h *Handler) runJanitor(ctx context.Context) {
	g := worker.NewGroup(ctx, "azureeventhubs")
	g.Ticker("RetentionSweeper", janitorInterval, 0, func(context.Context) {
		h.Backend.Sweep(h.Backend.now())
	})

	<-ctx.Done()
	g.Stop()
}

// Shutdown implements service.Shutdowner.
func (h *Handler) Shutdown(ctx context.Context) {
	h.stopAMQP()

	srv := h.srv
	h.srv = nil

	if srv == nil {
		return
	}

	if err := srv.Shutdown(ctx); err != nil {
		logger.Load(ctx).ErrorContext(ctx, "azureeventhubs: graceful shutdown failed, forcing close", "error", err)
		_ = srv.Close()
	}
}

func splitPath(p string) []string {
	var out []string

	for s := range strings.SplitSeq(p, "/") {
		if s != "" {
			out = append(out, s)
		}
	}

	return out
}
