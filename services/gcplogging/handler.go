package gcplogging

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/labstack/echo/v5"
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

// Handler serves the Cloud Logging gRPC API (and its REST transcoding) on a dedicated listener.
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
		srvMu:   lockmetrics.New("gcplogging.server"),
		wg:      &sync.WaitGroup{},
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "GCPLogging" }

// GetSupportedOperations lists the supported gRPC methods.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"WriteLogEntries", "ListLogEntries", "ListLogs", "DeleteLog", "TailLogEntries",
		"ListMonitoredResourceDescriptors",
		"CreateSink", "GetSink", "ListSinks", "UpdateSink", "DeleteSink",
		"CreateExclusion", "GetExclusion", "ListExclusions", "UpdateExclusion", "DeleteExclusion",
		"CreateBucket", "GetBucket", "ListBuckets", "UpdateBucket", "DeleteBucket", "UndeleteBucket",
		"CreateBucketAsync", "UpdateBucketAsync",
		"CreateView", "GetView", "ListViews", "UpdateView", "DeleteView",
		"GetCmekSettings", "UpdateCmekSettings", "GetSettings", "UpdateSettings",
		"CreateLogMetric", "GetLogMetric", "ListLogMetrics", "UpdateLogMetric", "DeleteLogMetric",
	}
}

// RouteMatcher never matches: Cloud Logging runs on its own listener.
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

// Reset clears all in-memory state.
func (h *Handler) Reset() { h.Backend.Reset() }

// Handler returns a 404 handler; the real API is on the dedicated port.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return c.String(http.StatusNotFound, "gcplogging serves gRPC and REST on its dedicated port")
	}
}

type debugLogger interface {
	DebugContext(ctx context.Context, msg string, args ...any)
}

func logInterceptor(log debugLogger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		resp, err := next(ctx, req)
		if err != nil && status.Code(err) != codes.OK {
			log.DebugContext(ctx, "gcplogging: rpc failed", "method", info.FullMethod, "error", err)
		}

		return resp, err
	}
}

func registerServers(srv *grpc.Server, b *InMemoryBackend) {
	loggingpb.RegisterLoggingServiceV2Server(srv, &loggingServer{b: b})
	loggingpb.RegisterConfigServiceV2Server(srv, &configServer{b: b})
	loggingpb.RegisterMetricsServiceV2Server(srv, &metricsServer{b: b})
	longrunningpb.RegisterOperationsServer(srv, b.ops)
}

// StartWorker binds the dedicated listener synchronously so a port conflict is returned to the caller.
func (h *Handler) StartWorker(ctx context.Context) error {
	var listenConfig net.ListenConfig

	listener, err := listenConfig.Listen(ctx, "tcp", fmt.Sprintf(":%d", h.Port))
	if err != nil {
		return fmt.Errorf("gcplogging: bind port %d: %w", h.Port, err)
	}

	workerCtx := logger.WithWorker(ctx, "gcplogging", "listener")
	log := logger.Load(workerCtx)

	srv := gcpenforce.NewServer(ctx, "logging.googleapis.com", grpc.UnaryInterceptor(logInterceptor(log)))
	registerServers(srv, h.Backend)

	h.srvMu.Lock("StartWorker")
	h.srv = srv
	h.addr = listener.Addr().String()
	h.srvMu.Unlock()

	log.InfoContext(workerCtx, "gcplogging: starting dedicated gRPC listener", "port", h.Port)

	h.wg.Go(func() {
		if serveErr := gcprest.Serve(srv, listener); serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			log.ErrorContext(workerCtx, "gcplogging: listener stopped", "error", serveErr)
		}
	})

	return nil
}

// Shutdown stops the gRPC server, forcing a close after a short grace period.
func (h *Handler) Shutdown(ctx context.Context) {
	h.srvMu.Lock("Shutdown")
	srv := h.srv
	h.srv = nil
	h.srvMu.Unlock()

	if srv == nil {
		return
	}

	h.Backend.closeTails()

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
}
