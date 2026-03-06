package ecr

import (
	"log/slog"
	"os"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const enableLocalRegistryEnv = "GOPHERSTACK_ENABLE_LOCAL_REGISTRY"

// Provider implements service.Provider for Amazon ECR.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "ECR" }

// Init initializes the ECR service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(appCtx *service.AppContext) (service.Registerable, error) {
	var globalCfg config.GlobalConfig
	if cfgProvider, ok := appCtx.Config.(config.Provider); ok {
		globalCfg = cfgProvider.GetGlobalConfig()
	}

	if globalCfg.AccountID == "" {
		globalCfg.AccountID = config.DefaultAccountID
	}

	if globalCfg.Region == "" {
		globalCfg.Region = config.DefaultRegion
	}

	log := appCtx.Logger
	if log == nil {
		log = slog.Default()
	}

	localRegistryEnabled := os.Getenv(enableLocalRegistryEnv) == "1"

	// The endpoint for repository URIs is set to the Gopherstack server address.
	// At init time we don't know the actual port; the CLI sets this after startup.
	// For now use an empty string; SetEndpoint() can be called later.
	backend := NewInMemoryBackend(globalCfg.AccountID, globalCfg.Region, "")

	if localRegistryEnabled {
		log.Info("ECR local registry enabled; starting embedded Docker registry v2")

		rh := newDistributionRegistry(appCtx.JanitorCtx)

		return NewHandler(backend, rh), nil
	}

	log.Warn(
		"ECR local registry is disabled; docker push/pull will not work. Set GOPHERSTACK_ENABLE_LOCAL_REGISTRY=1 to enable",
	)

	return NewHandler(backend, nil), nil
}
