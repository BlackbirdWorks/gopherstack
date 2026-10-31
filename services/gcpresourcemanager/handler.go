package gcpresourcemanager

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	"cloud.google.com/go/serviceusage/apiv1/serviceusagepb"
	"github.com/labstack/echo/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/pkgs/gcprest"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const gracefulStopWait = 2 * time.Second

// Handler serves Resource Manager (v3 gRPC and REST, v1 REST) and Service Usage on a dedicated listener.
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
		srvMu:   lockmetrics.New("gcpresourcemanager.server"),
		wg:      &sync.WaitGroup{},
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "GCPResourceManager" }

// GetSupportedOperations lists the supported gRPC methods.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateProject", "GetProject", "ListProjects", "SearchProjects", "UpdateProject", "MoveProject",
		"DeleteProject", "UndeleteProject",
		"CreateFolder", "GetFolder", "ListFolders", "SearchFolders", "UpdateFolder", "MoveFolder",
		"DeleteFolder", "UndeleteFolder",
		"GetOrganization", "SearchOrganizations",
		"GetIamPolicy", "SetIamPolicy", "TestIamPermissions",
		"CreateTagKey", "GetTagKey", "GetNamespacedTagKey", "ListTagKeys", "UpdateTagKey", "DeleteTagKey",
		"CreateTagValue", "GetTagValue", "GetNamespacedTagValue", "ListTagValues", "UpdateTagValue", "DeleteTagValue",
		"CreateTagBinding", "ListTagBindings", "DeleteTagBinding",
		"EnableService", "DisableService", "GetService", "ListServices", "BatchEnableServices", "BatchGetServices",
		"GetOperation", "ListOperations", "DeleteOperation", "WaitOperation", "CancelOperation",
	}
}

// RouteMatcher never matches: the service runs on its own listener.
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

// Reset restores the seeded state.
func (h *Handler) Reset() { h.Backend.Reset() }

// Handler returns a 404 handler; the real API is on the dedicated port.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return c.String(http.StatusNotFound, "gcpresourcemanager serves gRPC and REST on its dedicated port")
	}
}

type debugLogger interface {
	DebugContext(ctx context.Context, msg string, args ...any)
}

func logUnary(log debugLogger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		resp, err := next(ctx, req)
		if err != nil && status.Code(err) != codes.OK {
			log.DebugContext(ctx, "gcpresourcemanager: rpc failed", "method", info.FullMethod, "error", err)
		}

		return resp, err
	}
}

func (h *Handler) registerServers(srv *grpc.Server) {
	iam := iamMethods{b: h.Backend}

	resourcemanagerpb.RegisterProjectsServer(srv, projectsServer{iamMethods: iam})
	resourcemanagerpb.RegisterFoldersServer(srv, foldersServer{iamMethods: iam})
	resourcemanagerpb.RegisterOrganizationsServer(srv, organizationsServer{iamMethods: iam})
	resourcemanagerpb.RegisterTagKeysServer(srv, tagKeysServer{iamMethods: iam})
	resourcemanagerpb.RegisterTagValuesServer(srv, tagValuesServer{iamMethods: iam})
	resourcemanagerpb.RegisterTagBindingsServer(srv, tagBindingsServer{b: h.Backend})
	serviceusagepb.RegisterServiceUsageServer(srv, serviceUsageServer{b: h.Backend})
	longrunningpb.RegisterOperationsServer(srv, h.Backend.lro)
}

// StartWorker binds the dedicated listener synchronously so a port conflict is returned to the caller.
func (h *Handler) StartWorker(ctx context.Context) error {
	var lc net.ListenConfig

	listener, err := lc.Listen(ctx, "tcp", fmt.Sprintf(":%d", h.Port))
	if err != nil {
		return fmt.Errorf("gcpresourcemanager: bind port %d: %w", h.Port, err)
	}

	workerCtx := logger.WithWorker(ctx, "gcpresourcemanager", "listener")
	log := logger.Load(workerCtx)

	srv := grpc.NewServer(grpc.UnaryInterceptor(logUnary(log)))
	h.registerServers(srv)

	h.srvMu.Lock("StartWorker")
	h.srv, h.addr = srv, listener.Addr().String()
	h.srvMu.Unlock()

	log.InfoContext(workerCtx, "gcpresourcemanager: starting dedicated listener", "port", h.Port)

	h.wg.Go(func() {
		v1 := gcprest.Extra{MatchRequest: matchV1, Handler: v1Handler{b: h.Backend}}
		if serveErr := gcprest.ServeWithExtras(srv, listener, nil, v1); serveErr != nil &&
			!errors.Is(serveErr, grpc.ErrServerStopped) {
			log.ErrorContext(workerCtx, "gcpresourcemanager: listener stopped", "error", serveErr)
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
