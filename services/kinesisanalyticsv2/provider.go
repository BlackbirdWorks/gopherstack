// Package kinesisanalyticsv2 provides an in-memory stub of AWS Kinesis Data Analytics v2.
package kinesisanalyticsv2

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned by Provider.Init when a nil AppContext is supplied.
var ErrNilAppContext = errors.New("kinesisanalyticsv2: AppContext must not be nil")

// Provider implements service.Provider for Kinesis Data Analytics v2.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "KinesisAnalyticsV2" }

// Init initializes the Kinesis Data Analytics v2 backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	accountID, region := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackend(accountID, region)
	handler := NewHandler(backend)

	return handler, nil
}
