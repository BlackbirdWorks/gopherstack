package ecr

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const enableLocalRegistryEnv = "GOPHERSTACK_ENABLE_LOCAL_REGISTRY"

// ErrNilAppContext is returned by Init when appCtx is nil.
var ErrNilAppContext = errors.New("AppContext is required")

// Provider implements service.Provider for Amazon ECR.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "ECR" }

// Init initializes the ECR service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(appCtx *service.AppContext) (service.Registerable, error) {
	if appCtx == nil {
		return nil, fmt.Errorf("%w", ErrNilAppContext)
	}

	var globalCfg *config.GlobalConfig
	if cfgProvider, ok := appCtx.Config.(config.Provider); ok {
		globalCfg = cfgProvider.GetGlobalConfig()
	} else {
		globalCfg = config.NewGlobalConfig(config.DefaultAccountID, config.DefaultRegion, 0, 0, false, 0)
	}

	accountID := globalCfg.GetAccountID()
	region := globalCfg.GetRegion()

	if accountID == "" {
		accountID = config.DefaultAccountID
	}

	if region == "" {
		region = config.DefaultRegion
	}

	localRegistryEnabled := os.Getenv(enableLocalRegistryEnv) == "1"

	// The endpoint for repository URIs is set to the Gopherstack server address.
	// At init time we don't know the actual port; the CLI sets this after startup.
	// For now use an empty string; SetEndpoint() can be called later.
	backend := NewInMemoryBackend(accountID, region, "")

	var registryHandler http.Handler

	if localRegistryEnabled {
		appCtx.Logger.Info("ECR local registry enabled; starting embedded Docker registry v2")

		janitorCtx := appCtx.JanitorCtx
		if janitorCtx == nil {
			janitorCtx = context.Background()
		}

		registryHandler = newDistributionRegistry(janitorCtx)
	} else {
		appCtx.Logger.Warn(
			"ECR local registry is disabled; docker push/pull will not work. Set GOPHERSTACK_ENABLE_LOCAL_REGISTRY=1 to enable",
		)
	}

	// Attach the background lifecycle-expiry janitor. StartWorker runs it against
	// appCtx.JanitorCtx once the service is registered.
	return NewHandler(backend, registryHandler).WithJanitor(0, appCtx.JanitorTimeout), nil
}
