package backup

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ConfigProvider is a private interface to extract Backup configuration
// from the abstract AppContext Config.
type ConfigProvider interface {
	GetBackupSettings() Settings
}

// Provider implements service.Provider for AWS Backup.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "Backup" }

// Init initializes the Backup service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	accountID, region := service.AccountRegionOrDefault(ctx)

	var settings Settings

	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetBackupSettings()
	}

	backend := NewInMemoryBackend(accountID, region)
	handler := NewHandler(backend)
	handler.WithJanitor(settings.JanitorInterval, settings.JobTTL, ctx.JanitorTimeout)

	return handler, nil
}
