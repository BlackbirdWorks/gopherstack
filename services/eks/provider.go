package eks

import (
	"errors"
	"fmt"
	"os"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned by Init when appCtx is nil.
var ErrNilAppContext = errors.New("AppContext is required")

// EngineConfig exposes the EKS engine mode (stub or docker).
type EngineConfig interface {
	GetEKSEngine() string
}

func enableDockerClusters(ctx *service.AppContext, backend *InMemoryBackend) {
	rt, err := container.NewRuntime(container.Config{Logger: ctx.Logger})
	if err != nil {
		ctx.Logger.Warn("EKS: container runtime unavailable; clusters stay metadata-only", "error", err)

		return
	}

	err = backend.EnableClusters(ClusterEngineConfig{
		Runtime: rt,
		Ports:   ctx.PortAlloc,
		Logger:  ctx.Logger,
		Host:    os.Getenv("EKS_CLUSTER_HOST"),
		Token:   os.Getenv("EKS_CLUSTER_TOKEN"),
		Image:   os.Getenv("EKS_K3S_IMAGE"),
	})
	if err != nil {
		ctx.Logger.Warn("EKS: invalid cluster engine configuration; clusters stay metadata-only", "error", err)
		_ = rt.Close()
	}
}

// Provider implements service.Provider for AWS EKS.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "EKS" }

// Init initializes the EKS service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w", ErrNilAppContext)
	}

	accountID, region := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackend(ctx.JanitorCtx, accountID, region)

	if ec, ok := ctx.Config.(EngineConfig); ok && ClusterEngineEnabled(ec.GetEKSEngine()) {
		enableDockerClusters(ctx, backend)
	}

	handler := NewHandler(backend)
	handler.EnableRegions()

	return handler, nil
}
