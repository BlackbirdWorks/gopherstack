package gcpkms

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/kms/apiv1/kmspb"
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
	sweepInterval    = 30 * time.Second
	gracefulStopWait = 2 * time.Second
)

// Handler serves the Cloud KMS gRPC API on a dedicated listener.
type Handler struct {
	Backend *InMemoryBackend
	srvMu   *lockmetrics.RWMutex
	srv     *grpc.Server
	wg      *sync.WaitGroup
	cancel  context.CancelFunc
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
		srvMu:   lockmetrics.New("gcpkms.server"),
		wg:      &sync.WaitGroup{},
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "GCPKMS" }

// GetSupportedOperations lists the supported gRPC methods.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateKeyRing", "GetKeyRing", "ListKeyRings",
		"CreateCryptoKey", "GetCryptoKey", "ListCryptoKeys", "UpdateCryptoKey", "UpdateCryptoKeyPrimaryVersion",
		"CreateCryptoKeyVersion", "GetCryptoKeyVersion", "ListCryptoKeyVersions", "UpdateCryptoKeyVersion",
		"DestroyCryptoKeyVersion", "RestoreCryptoKeyVersion", "DeleteCryptoKey", "DeleteCryptoKeyVersion",
		"GetRetiredResource", "ListRetiredResources",
		"CreateImportJob", "GetImportJob", "ListImportJobs", "ImportCryptoKeyVersion",
		"Encrypt", "Decrypt", "RawEncrypt", "RawDecrypt", "AsymmetricSign", "AsymmetricDecrypt",
		"GetPublicKey", "MacSign", "MacVerify", "GenerateRandomBytes",
		"SetIamPolicy", "GetIamPolicy", "TestIamPermissions",
	}
}

// RouteMatcher never matches: Cloud KMS runs on its own gRPC listener.
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

// Handler returns a 404 handler; the real API is gRPC on the dedicated port.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return c.String(http.StatusNotFound, "gcpkms serves gRPC on its dedicated port")
	}
}

func logInterceptor(log interface {
	DebugContext(ctx context.Context, msg string, args ...any)
},
) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		resp, err := next(ctx, req)
		if err != nil && status.Code(err) != codes.OK {
			log.DebugContext(ctx, "gcpkms: rpc failed", "method", info.FullMethod, "error", err)
		}

		return resp, err
	}
}

// StartWorker binds the dedicated listener synchronously so a port conflict is returned to the caller.
func (h *Handler) StartWorker(ctx context.Context) error {
	var listenConfig net.ListenConfig

	listener, err := listenConfig.Listen(ctx, "tcp", fmt.Sprintf(":%d", h.Port))
	if err != nil {
		return fmt.Errorf("gcpkms: bind port %d: %w", h.Port, err)
	}

	workerCtx := logger.WithWorker(ctx, "gcpkms", "listener")
	log := logger.Load(workerCtx)

	srv := gcpenforce.NewServer(ctx, "cloudkms.googleapis.com", grpc.UnaryInterceptor(logInterceptor(log)))
	kmspb.RegisterKeyManagementServiceServer(srv, &server{b: h.Backend})
	iampb.RegisterIAMPolicyServer(srv, &iamServer{b: h.Backend})
	longrunningpb.RegisterOperationsServer(srv, h.Backend.ops)

	sweepCtx, cancel := context.WithCancel(context.WithoutCancel(workerCtx))

	h.srvMu.Lock("StartWorker")
	h.srv = srv
	h.addr = listener.Addr().String()
	h.cancel = cancel
	h.srvMu.Unlock()

	log.InfoContext(workerCtx, "gcpkms: starting dedicated gRPC listener", "port", h.Port)

	h.wg.Add(2) //nolint:mnd // rotation janitor and gRPC serve loop

	go func() {
		defer h.wg.Done()

		h.Backend.RunJanitor(sweepCtx, sweepInterval)
	}()

	go func() {
		defer h.wg.Done()

		if serveErr := gcprest.Serve(srv, listener); serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			log.ErrorContext(workerCtx, "gcpkms: listener stopped", "error", serveErr)
		}
	}()

	return nil
}

// Shutdown stops the gRPC server and the janitor, forcing a close after a short grace period.
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
