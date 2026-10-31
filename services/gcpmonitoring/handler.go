package gcpmonitoring

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
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

const (
	// DefaultEvalInterval is how often alert policies and uptime checks are evaluated.
	DefaultEvalInterval = 5 * time.Second
	gracefulStopWait    = 2 * time.Second
)

// Handler serves the Cloud Monitoring gRPC API (and its REST transcoding) on a dedicated listener.
type Handler struct {
	Backend *InMemoryBackend
	srvMu   *lockmetrics.RWMutex
	srv     *grpc.Server
	wg      *sync.WaitGroup
	cancel  context.CancelFunc
	addr    string
	// EvalInterval is the alert and uptime tick; zero disables the background loop.
	EvalInterval time.Duration
	Port         int
}

var (
	_ service.BackgroundWorker = (*Handler)(nil)
	_ service.Shutdowner       = (*Handler)(nil)
	_ service.Resettable       = (*Handler)(nil)
)

// NewHandler creates a Handler; callers override Port from Settings.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{
		Backend:      backend,
		Port:         DefaultPort,
		EvalInterval: DefaultEvalInterval,
		srvMu:        lockmetrics.New("gcpmonitoring.server"),
		wg:           &sync.WaitGroup{},
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "GCPMonitoring" }

// GetSupportedOperations lists the supported gRPC methods.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"ListMonitoredResourceDescriptors", "GetMonitoredResourceDescriptor",
		"ListMetricDescriptors", "GetMetricDescriptor", "CreateMetricDescriptor", "DeleteMetricDescriptor",
		"ListTimeSeries", "CreateTimeSeries", "CreateServiceTimeSeries",
		"ListAlertPolicies", "GetAlertPolicy", "CreateAlertPolicy", "UpdateAlertPolicy", "DeleteAlertPolicy",
		"ListNotificationChannelDescriptors", "GetNotificationChannelDescriptor",
		"ListNotificationChannels", "GetNotificationChannel", "CreateNotificationChannel",
		"UpdateNotificationChannel", "DeleteNotificationChannel",
		"SendNotificationChannelVerificationCode", "GetNotificationChannelVerificationCode",
		"VerifyNotificationChannel",
		"ListUptimeCheckConfigs", "GetUptimeCheckConfig", "CreateUptimeCheckConfig", "UpdateUptimeCheckConfig",
		"DeleteUptimeCheckConfig", "ListUptimeCheckIps",
		"ListGroups", "GetGroup", "CreateGroup", "UpdateGroup", "DeleteGroup", "ListGroupMembers",
		"CreateSnooze", "GetSnooze", "ListSnoozes", "UpdateSnooze",
		"CreateService", "GetService", "ListServices", "UpdateService", "DeleteService",
		"CreateServiceLevelObjective", "GetServiceLevelObjective", "ListServiceLevelObjectives",
		"UpdateServiceLevelObjective", "DeleteServiceLevelObjective",
	}
}

// RouteMatcher never matches: Cloud Monitoring runs on its own listener.
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
		return c.String(http.StatusNotFound, "gcpmonitoring serves gRPC and REST on its dedicated port")
	}
}

type debugLogger interface {
	DebugContext(ctx context.Context, msg string, args ...any)
}

func logInterceptor(log debugLogger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		resp, err := next(ctx, req)
		if err != nil && status.Code(err) != codes.OK {
			log.DebugContext(ctx, "gcpmonitoring: rpc failed", "method", info.FullMethod, "error", err)
		}

		return resp, err
	}
}

func registerServers(srv *grpc.Server, b *InMemoryBackend) {
	monitoringpb.RegisterMetricServiceServer(srv, &metricServer{b: b})
	monitoringpb.RegisterAlertPolicyServiceServer(srv, &alertServer{b: b})
	monitoringpb.RegisterNotificationChannelServiceServer(srv, &channelServer{b: b})
	monitoringpb.RegisterUptimeCheckServiceServer(srv, &uptimeServer{b: b})
	monitoringpb.RegisterGroupServiceServer(srv, &groupServer{b: b})
	monitoringpb.RegisterSnoozeServiceServer(srv, &snoozeServer{b: b})
	monitoringpb.RegisterServiceMonitoringServiceServer(srv, &serviceServer{b: b})
}

// StartWorker binds the dedicated listener synchronously so a port conflict is returned to the caller.
func (h *Handler) StartWorker(ctx context.Context) error {
	var listenConfig net.ListenConfig

	listener, err := listenConfig.Listen(ctx, "tcp", fmt.Sprintf(":%d", h.Port))
	if err != nil {
		return fmt.Errorf("gcpmonitoring: bind port %d: %w", h.Port, err)
	}

	workerCtx := logger.WithWorker(ctx, "gcpmonitoring", "listener")
	log := logger.Load(workerCtx)

	srv := gcpenforce.NewServer(ctx, "monitoring.googleapis.com", grpc.UnaryInterceptor(logInterceptor(log)))
	registerServers(srv, h.Backend)

	loopCtx, cancel := context.WithCancel(context.WithoutCancel(workerCtx))

	h.srvMu.Lock("StartWorker")
	h.srv = srv
	h.cancel = cancel
	h.addr = listener.Addr().String()
	h.srvMu.Unlock()

	if h.EvalInterval > 0 {
		h.wg.Go(func() { h.Backend.RunLoop(loopCtx, h.EvalInterval) })
	}

	log.InfoContext(workerCtx, "gcpmonitoring: starting dedicated gRPC listener", "port", h.Port)

	h.wg.Go(func() {
		if serveErr := gcprest.ServeWithExtras(srv, listener, nil, gcprest.Extra{
			Match: IsPrometheusPath, Handler: h.Backend.PrometheusHandler(),
		}); serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			log.ErrorContext(workerCtx, "gcpmonitoring: listener stopped", "error", serveErr)
		}
	})

	return nil
}

// Shutdown stops the gRPC server, forcing a close after a short grace period.
func (h *Handler) Shutdown(ctx context.Context) {
	h.srvMu.Lock("Shutdown")
	srv, cancel := h.srv, h.cancel
	h.srv, h.cancel = nil, nil
	h.srvMu.Unlock()

	if srv == nil {
		return
	}

	cancel()

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
