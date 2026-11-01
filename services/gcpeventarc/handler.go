package gcpeventarc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/labstack/echo/v5"
	"google.golang.org/genproto/googleapis/cloud/location"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpenforce"
	"github.com/blackbirdworks/gopherstack/pkgs/gcprest"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const gracefulStopWait = 2 * time.Second

// Handler serves the Eventarc API on a dedicated gRPC and REST listener.
type Handler struct {
	Backend *InMemoryBackend
	srvMu   *lockmetrics.RWMutex
	srv     *grpc.Server
	wg      *sync.WaitGroup
	addr    string
	Port    int
}

var (
	_ service.BackgroundWorker = (*Handler)(nil)
	_ service.Shutdowner       = (*Handler)(nil)
	_ service.Resettable       = (*Handler)(nil)
)

// NewHandler creates a Handler; callers override Port from Settings.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{
		Backend: backend,
		Port:    DefaultPort,
		srvMu:   lockmetrics.New("gcpeventarc.server"),
		wg:      &sync.WaitGroup{},
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "GCPEventarc" }

// GetSupportedOperations lists the supported gRPC methods.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateTrigger", "GetTrigger", "ListTriggers", "UpdateTrigger", "DeleteTrigger",
		"CreateChannel", "GetChannel", "ListChannels", "UpdateChannel", "DeleteChannel",
		"CreateChannelConnection", "GetChannelConnection", "ListChannelConnections", "DeleteChannelConnection",
		"GetProvider", "ListProviders", "GetGoogleChannelConfig", "UpdateGoogleChannelConfig",
		"GetLocation", "ListLocations", "GetIamPolicy", "SetIamPolicy", "TestIamPermissions",
		"GetOperation", "ListOperations", "DeleteOperation", "CancelOperation", "WaitOperation",
	}
}

// RouteMatcher never matches: Eventarc runs on its own listener.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(*echo.Context) bool { return false }
}

// MatchPriority is irrelevant because RouteMatcher never matches.
func (h *Handler) MatchPriority() int { return 0 }

// ExtractOperation is unused: requests never reach the shared router.
func (h *Handler) ExtractOperation(*echo.Context) string { return "Unknown" }

// ExtractResource is unused: requests never reach the shared router.
func (h *Handler) ExtractResource(*echo.Context) string { return "" }

// Addr returns the bound listener address, or "" before StartWorker.
func (h *Handler) Addr() string {
	h.srvMu.RLock("Addr")
	defer h.srvMu.RUnlock()

	return h.addr
}

// Reset clears all state.
func (h *Handler) Reset() { h.Backend.Reset() }

// Handler returns a 404 handler; the real API is on the dedicated port.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return c.String(http.StatusNotFound, "gcpeventarc serves gRPC and REST on its dedicated port")
	}
}

type debugLogger interface {
	DebugContext(ctx context.Context, msg string, args ...any)
}

func logUnary(log debugLogger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		resp, err := next(ctx, req)
		if err != nil && status.Code(err) != codes.OK {
			log.DebugContext(ctx, "gcpeventarc: rpc failed", "method", info.FullMethod, "error", err)
		}

		return resp, err
	}
}

func (h *Handler) registerServers(srv *grpc.Server) {
	eventarcpb.RegisterEventarcServer(srv, &eventarcServer{b: h.Backend})
	iampb.RegisterIAMPolicyServer(srv, &iamServer{b: h.Backend})
	location.RegisterLocationsServer(srv, locationsServer{b: h.Backend})
	longrunningpb.RegisterOperationsServer(srv, h.Backend.ops)
}

// StartWorker binds the dedicated listener synchronously so a port conflict is returned to the caller.
func (h *Handler) StartWorker(ctx context.Context) error {
	var lc net.ListenConfig

	listener, err := lc.Listen(ctx, "tcp", fmt.Sprintf(":%d", h.Port))
	if err != nil {
		return fmt.Errorf("gcpeventarc: bind port %d: %w", h.Port, err)
	}

	workerCtx := logger.WithWorker(ctx, "gcpeventarc", "listener")
	log := logger.Load(workerCtx)

	srv := gcpenforce.NewServer(ctx, "eventarc.googleapis.com", grpc.UnaryInterceptor(logUnary(log)))
	h.registerServers(srv)

	_, port, _ := net.SplitHostPort(listener.Addr().String())
	h.Backend.SetPushBase("http://" + net.JoinHostPort("127.0.0.1", port))

	h.srvMu.Lock("StartWorker")
	h.srv, h.addr = srv, listener.Addr().String()
	h.srvMu.Unlock()

	log.InfoContext(workerCtx, "gcpeventarc: starting dedicated listener", "port", h.Port)

	push := gcprest.Extra{
		Match:   func(path string) bool { return strings.HasPrefix(path, pushPath) },
		Handler: h.Backend.PushHandler(),
	}

	h.wg.Go(func() {
		if serveErr := gcprest.ServeWithExtras(srv, listener, nil, push); serveErr != nil &&
			!errors.Is(serveErr, grpc.ErrServerStopped) {
			log.ErrorContext(workerCtx, "gcpeventarc: listener stopped", "error", serveErr)
		}
	})

	return nil
}

// Shutdown stops the listener, forcing a close after a short grace period.
func (h *Handler) Shutdown(ctx context.Context) {
	h.srvMu.Lock("Shutdown")
	srv := h.srv
	h.srv = nil
	h.srvMu.Unlock()

	if srv == nil {
		return
	}

	done := make(chan struct{})

	go func() {
		srv.GracefulStop()
		close(done)
	}()

	grace := time.NewTimer(gracefulStopWait)
	defer grace.Stop()

	select {
	case <-done:
	case <-grace.C:
		srv.Stop()
		<-done
	case <-ctx.Done():
		srv.Stop()
		<-done
	}

	h.wg.Wait()
	h.Backend.CloseIdleConnections()
}
