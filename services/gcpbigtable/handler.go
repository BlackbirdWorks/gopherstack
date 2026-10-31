package gcpbigtable

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"cloud.google.com/go/bigtable/admin/apiv2/adminpb"
	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
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

const (
	gracefulStopWait = 2 * time.Second
	serverMaxMessage = 256 << 20
)

// Handler serves the Cloud Bigtable admin and data APIs on a dedicated gRPC and REST listener.
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
		srvMu:   lockmetrics.New("gcpbigtable.server"),
		wg:      &sync.WaitGroup{},
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "GCPBigtable" }

// GetSupportedOperations lists the supported gRPC methods.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateInstance", "GetInstance", "ListInstances", "UpdateInstance", "PartialUpdateInstance", "DeleteInstance",
		"CreateCluster", "GetCluster", "ListClusters", "UpdateCluster", "PartialUpdateCluster", "DeleteCluster",
		"CreateAppProfile", "GetAppProfile", "ListAppProfiles", "UpdateAppProfile", "DeleteAppProfile",
		"GetIamPolicy", "SetIamPolicy", "TestIamPermissions",
		"CreateTable", "ListTables", "GetTable", "UpdateTable", "DeleteTable", "ModifyColumnFamilies", "DropRowRange",
		"GenerateConsistencyToken", "CheckConsistency",
		"CreateBackup", "GetBackup", "UpdateBackup", "DeleteBackup", "ListBackups", "RestoreTable",
		"ReadRows", "SampleRowKeys", "MutateRow", "MutateRows", "CheckAndMutateRow", "ReadModifyWriteRow",
		"PingAndWarm",
		"GetOperation", "ListOperations", "DeleteOperation", "CancelOperation", "WaitOperation",
	}
}

// RouteMatcher never matches: Cloud Bigtable runs on its own listener.
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
		return c.String(http.StatusNotFound, "gcpbigtable serves gRPC and REST on its dedicated port")
	}
}

type debugLogger interface {
	DebugContext(ctx context.Context, msg string, args ...any)
}

func logUnary(log debugLogger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		resp, err := next(ctx, req)
		if err != nil && status.Code(err) != codes.OK {
			log.DebugContext(ctx, "gcpbigtable: rpc failed", "method", info.FullMethod, "error", err)
		}

		return resp, err
	}
}

func logStream(log debugLogger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, next grpc.StreamHandler) error {
		err := next(srv, ss)
		if err != nil && status.Code(err) != codes.OK {
			log.DebugContext(ss.Context(), "gcpbigtable: rpc failed", "method", info.FullMethod, "error", err)
		}

		return err
	}
}

func (h *Handler) registerServers(srv *grpc.Server) {
	adminpb.RegisterBigtableInstanceAdminServer(srv, &instanceAdminServer{b: h.Backend})
	adminpb.RegisterBigtableTableAdminServer(srv, &tableAdminServer{b: h.Backend})
	btpb.RegisterBigtableServer(srv, &dataServer{b: h.Backend})
	longrunningpb.RegisterOperationsServer(srv, h.Backend.ops)
}

// StartWorker binds the dedicated listener synchronously so a port conflict is returned to the caller.
func (h *Handler) StartWorker(ctx context.Context) error {
	var lc net.ListenConfig

	listener, err := lc.Listen(ctx, "tcp", fmt.Sprintf(":%d", h.Port))
	if err != nil {
		return fmt.Errorf("gcpbigtable: bind port %d: %w", h.Port, err)
	}

	workerCtx := logger.WithWorker(ctx, "gcpbigtable", "listener")
	log := logger.Load(workerCtx)

	srv := gcpenforce.NewServer(
		ctx, "bigtable.googleapis.com",
		grpc.UnaryInterceptor(logUnary(log)),
		grpc.StreamInterceptor(logStream(log)),
		grpc.MaxRecvMsgSize(serverMaxMessage),
	)
	h.registerServers(srv)

	h.srvMu.Lock("StartWorker")
	h.srv, h.addr = srv, listener.Addr().String()
	h.srvMu.Unlock()

	log.InfoContext(workerCtx, "gcpbigtable: starting dedicated listener", "port", h.Port)

	h.wg.Go(func() {
		if serveErr := gcprest.Serve(srv, listener); serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			log.ErrorContext(workerCtx, "gcpbigtable: listener stopped", "error", serveErr)
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
}
