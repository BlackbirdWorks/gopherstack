package kinesisanalytics

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned by Provider.Init when a nil AppContext is supplied.
var ErrNilAppContext = errors.New("kinesisanalytics: AppContext must not be nil")

// Provider implements service.Provider for the Kinesis Analytics service.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "KinesisAnalytics" }

// Init initializes the Kinesis Analytics service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	accountID, region := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackend(region, accountID)
	handler := NewHandler(backend)
	handler.AccountID = accountID
	handler.DefaultRegion = region

	return handler, nil
}
