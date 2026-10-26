package azurefile

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("azurefile: nil app context")

// ConfigProvider extracts Azure Files configuration from the AppContext Config.
type ConfigProvider interface {
	GetAzureFileSettings() Settings
}

// Provider implements service.Provider for Azure Files. Like the other Azure
// data-plane services it never joins the shared AWS router: the Handler runs
// its own listener (see Handler.StartWorker).
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "AzureFile" }

// Init builds the backend and handler; the port is bound later in StartWorker.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	settings := DefaultSettings()
	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetAzureFileSettings()
	}

	h := NewHandler(NewInMemoryBackend())
	h.Port = settings.Port
	h.VerifyAuth = settings.VerifyAuth

	return h, nil
}
