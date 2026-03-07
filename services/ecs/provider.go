package ecs

import (
	"fmt"
	"log/slog"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for Amazon ECS.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "ECS" }

// Init initializes the ECS service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(appCtx *service.AppContext) (service.Registerable, error) {
	accountID := config.DefaultAccountID
	region := config.DefaultRegion

	if cfgProvider, ok := appCtx.Config.(config.Provider); ok {
		cfg := cfgProvider.GetGlobalConfig()
		if cfg.AccountID != "" {
			accountID = cfg.AccountID
		}

		if cfg.Region != "" {
			region = cfg.Region
		}
	}

	log := appCtx.Logger
	if log == nil {
		log = slog.Default()
	}

	runner, err := newTaskRunner()
	if err != nil {
		return nil, fmt.Errorf("init ECS task runner: %w", err)
	}

	backend := NewInMemoryBackend(accountID, region, runner)
	reconciler := NewReconciler(backend)

	if appCtx.JanitorCtx != nil {
		go reconciler.Start(appCtx.JanitorCtx)
	}

	log.Info("ECS service initialized")

	return NewHandler(backend), nil
}

// compile-time assertion that Provider implements service.Provider.
var _ service.Provider = (*Provider)(nil)
