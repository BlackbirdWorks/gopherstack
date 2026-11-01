package gcprun

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/run/apiv2/runpb"
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
	gracefulStopWait  = 2 * time.Second
	readHeaderTimeout = 10 * time.Second
)

// Handler serves the Cloud Run Admin API (v2) on a dedicated gRPC listener and an HTTP ingress for services.
type Handler struct {
	Backend *InMemoryBackend
	srvMu   *lockmetrics.RWMutex
	srv     *grpc.Server
	ingress *http.Server
	wg      *sync.WaitGroup
	cancel  context.CancelFunc
	addr    string
	ingAddr string
	// Port is the gRPC port; IngressPort the HTTP ingress port. Zero binds an ephemeral port, negative disables ingress.
	Port        int
	IngressPort int
}

var (
	_ service.BackgroundWorker = (*Handler)(nil)
	_ service.Shutdowner       = (*Handler)(nil)
	_ service.Resettable       = (*Handler)(nil)
)

// NewHandler creates a Handler; callers override the ports from Settings.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{
		Backend:     backend,
		Port:        DefaultPort,
		IngressPort: DefaultIngressPort,
		srvMu:       lockmetrics.New("gcprun.server"),
		wg:          &sync.WaitGroup{},
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "GCPRun" }

// GetSupportedOperations lists the supported gRPC methods.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateService", "GetService", "ListServices", "UpdateService", "DeleteService",
		"GetRevision", "ListRevisions", "DeleteRevision",
		"CreateJob", "GetJob", "ListJobs", "UpdateJob", "DeleteJob", "RunJob",
		"GetExecution", "ListExecutions", "DeleteExecution", "CancelExecution",
		"GetTask", "ListTasks",
		"GetIamPolicy", "SetIamPolicy", "TestIamPermissions",
	}
}

// RouteMatcher never matches: Cloud Run runs on its own listeners.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(*echo.Context) bool { return false }
}

// MatchPriority is irrelevant because RouteMatcher never matches.
func (h *Handler) MatchPriority() int { return 0 }

// ExtractOperation is unused: requests never reach the shared router.
func (h *Handler) ExtractOperation(*echo.Context) string { return "Unknown" }

// ExtractResource is unused: requests never reach the shared router.
func (h *Handler) ExtractResource(*echo.Context) string { return "" }

// Addr returns the bound gRPC address, or "" before StartWorker.
func (h *Handler) Addr() string {
	h.srvMu.RLock("Addr")
	defer h.srvMu.RUnlock()

	return h.addr
}

// IngressAddr returns the bound ingress address, or "" when ingress is off.
func (h *Handler) IngressAddr() string {
	h.srvMu.RLock("IngressAddr")
	defer h.srvMu.RUnlock()

	return h.ingAddr
}

// Reset clears all in-memory state.
func (h *Handler) Reset() { h.Backend.Reset() }

// Handler returns a 404 handler; the real API is gRPC on the dedicated port.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return c.String(http.StatusNotFound, "gcprun serves gRPC on its dedicated port")
	}
}

func logInterceptor(log interface {
	DebugContext(ctx context.Context, msg string, args ...any)
},
) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		resp, err := next(ctx, req)
		if err != nil && status.Code(err) != codes.OK {
			log.DebugContext(ctx, "gcprun: rpc failed", "method", info.FullMethod, "error", err)
		}

		return resp, err
	}
}

func (h *Handler) registerServers(srv *grpc.Server) {
	runpb.RegisterServicesServer(srv, &servicesServer{b: h.Backend})
	runpb.RegisterRevisionsServer(srv, &revisionsServer{b: h.Backend})
	runpb.RegisterJobsServer(srv, &jobsServer{b: h.Backend})
	runpb.RegisterExecutionsServer(srv, &executionsServer{b: h.Backend})
	runpb.RegisterTasksServer(srv, &tasksServer{b: h.Backend})
	longrunningpb.RegisterOperationsServer(srv, h.Backend.ops)
}

func listen(ctx context.Context, port int) (net.Listener, error) {
	var lc net.ListenConfig

	l, err := lc.Listen(ctx, "tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, fmt.Errorf("gcprun: bind port %d: %w", port, err)
	}

	return l, nil
}

// StartWorker binds both listeners synchronously so a port conflict is returned to the caller.
func (h *Handler) StartWorker(ctx context.Context) error {
	grpcL, err := listen(ctx, h.Port)
	if err != nil {
		return err
	}

	var ingL net.Listener

	if h.IngressPort >= 0 {
		if ingL, err = listen(ctx, h.IngressPort); err != nil {
			_ = grpcL.Close()

			return err
		}

		if tcp, ok := ingL.Addr().(*net.TCPAddr); ok {
			h.Backend.SetIngressPort(tcp.Port)
		}
	}

	workerCtx := logger.WithWorker(ctx, "gcprun", "listener")
	log := logger.Load(workerCtx)

	srv := gcpenforce.NewServer(ctx, "run.googleapis.com", grpc.UnaryInterceptor(logInterceptor(log)))
	h.registerServers(srv)

	runCtx, cancel := context.WithCancel(context.WithoutCancel(workerCtx))
	h.Backend.SetRunContext(runCtx)

	h.srvMu.Lock("StartWorker")
	h.srv, h.cancel, h.addr = srv, cancel, grpcL.Addr().String()

	if ingL != nil {
		h.ingress = &http.Server{Handler: h.Backend.IngressHandler(), ReadHeaderTimeout: readHeaderTimeout}
		h.ingAddr = ingL.Addr().String()
	}
	ingress := h.ingress
	h.srvMu.Unlock()

	log.InfoContext(workerCtx, "gcprun: starting dedicated gRPC listener", "port", h.Port, "ingressPort", h.IngressPort)

	h.wg.Go(func() {
		if serveErr := gcprest.Serve(srv, grpcL); serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			log.ErrorContext(workerCtx, "gcprun: listener stopped", "error", serveErr)
		}
	})

	if ingress != nil {
		h.wg.Go(func() {
			if serveErr := ingress.Serve(ingL); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
				log.ErrorContext(workerCtx, "gcprun: ingress stopped", "error", serveErr)
			}
		})
	}

	return nil
}

// Shutdown stops both listeners, aborts running workloads and removes their containers.
func (h *Handler) Shutdown(ctx context.Context) {
	h.srvMu.Lock("Shutdown")
	srv, ingress, cancel := h.srv, h.ingress, h.cancel
	h.srv, h.ingress, h.cancel = nil, nil, nil
	h.srvMu.Unlock()

	if srv == nil {
		return
	}

	cancel()

	if ingress != nil {
		_ = ingress.Close()
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
	h.Backend.Close()
}
