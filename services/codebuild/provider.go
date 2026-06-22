package codebuild

import (
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Provider.Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("AppContext is required")

// ConfigProvider is a private interface to extract CodeBuild configuration
// from the abstract AppContext Config.
type ConfigProvider interface {
	GetCodeBuildSettings() Settings
}

// Provider implements service.Provider for AWS CodeBuild.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "CodeBuild" }

// Init initializes the CodeBuild service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w", ErrNilAppContext)
	}

	accountID, region := service.AccountRegionOrDefault(ctx)

	var settings Settings

	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetCodeBuildSettings()
	}

	backend := NewInMemoryBackend(accountID, region)
	handler := NewHandler(backend)
	handler.WithJanitor(settings.JanitorInterval, settings.BuildTTL, ctx.JanitorTimeout)

	return handler, nil
}
