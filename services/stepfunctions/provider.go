package stepfunctions

import (
	"errors"
	"fmt"
	"os"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/stepfunctions/asl"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("stepfunctions: nil app context")

// Provider implements service.Provider for the Step Functions service.
type Provider struct{}

// Name returns the logical name of the provider.
func (p *Provider) Name() string { return "StepFunctions" }

// Init initializes the Step Functions service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	var backend *InMemoryBackend

	if cp, ok := ctx.Config.(config.Provider); ok {
		cfg := cp.GetGlobalConfig()
		backend = NewInMemoryBackendWithContext(ctx.JanitorCtx, cfg.GetAccountID(), cfg.GetRegion())
	} else {
		backend = NewInMemoryBackend()
	}

	if sp, ok := ctx.Config.(SettingsProvider); ok {
		backend.SetSettings(sp.GetStepFunctionsSettings())
	}

	if err := loadMockConfigFromEnv(ctx, backend); err != nil {
		return nil, err
	}

	handler := NewHandler(backend)

	return handler, nil
}

// SettingsProvider is implemented by config objects that supply Step Functions settings.
type SettingsProvider interface {
	GetStepFunctionsSettings() Settings
}

// loadMockConfigFromEnv loads the file named by SFN_MOCK_CONFIG (or the LocalStack-prefixed form).
func loadMockConfigFromEnv(ctx *service.AppContext, backend *InMemoryBackend) error {
	path := os.Getenv("SFN_MOCK_CONFIG")
	if path == "" {
		path = os.Getenv("LOCALSTACK_SFN_MOCK_CONFIG")
	}

	if path == "" {
		return nil
	}

	mockCfg, err := asl.LoadMockConfig(path)
	if err != nil {
		return fmt.Errorf("stepfunctions: SFN_MOCK_CONFIG %q: %w", path, err)
	}

	backend.SetMockConfig(mockCfg)
	logger.Load(ctx.JanitorCtx).InfoContext(ctx.JanitorCtx, "Step Functions mock config loaded", "path", path)

	return nil
}
