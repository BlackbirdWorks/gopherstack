package s3control

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned by Init when a nil AppContext is passed.
var ErrNilAppContext = errors.New("nil AppContext passed to S3Control Provider.Init")

// Provider implements service.Provider for S3 Control.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "S3Control" }

// Init initializes the S3 Control service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	accountID := config.DefaultAccountID
	region := config.DefaultRegion

	if cp, ok := ctx.Config.(config.Provider); ok {
		gc := cp.GetGlobalConfig()
		if gc != nil {
			accountID = gc.GetAccountID()
			region = gc.GetRegion()
		}
	}

	backend := NewInMemoryBackendWithConfig(accountID, region)
	handler := NewHandler(backend)

	return handler, nil
}
