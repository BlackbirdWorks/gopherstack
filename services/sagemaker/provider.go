package sagemaker

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned by Init when a nil AppContext is passed.
var ErrNilAppContext = errors.New("sagemaker: nil AppContext")

// EngineModeConfig exposes the SageMaker engine mode (stub or docker).
type EngineModeConfig interface {
	GetSageMakerEngine() string
}

// Provider implements service.Provider for AWS SageMaker.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "SageMaker" }

// Init initializes the SageMaker service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	accountID, region := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackendWithContext(ctx.JanitorCtx, accountID, region)

	if ec, ok := ctx.Config.(EngineModeConfig); ok && ec.GetSageMakerEngine() == EngineDocker {
		cfg := EngineConfig{}

		rt, err := container.NewRuntime(container.Config{Logger: ctx.Logger})
		if err != nil {
			ctx.Logger.Warn("SageMaker: container runtime unavailable; endpoints and jobs will fail", "error", err)

			cfg.Reason = "no container runtime is available: " + err.Error()
		} else {
			cfg.Runtime = rt
		}

		backend.EnableEngine(cfg)
	}

	handler := NewHandler(backend)

	return handler, nil
}
