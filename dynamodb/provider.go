package dynamodb

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ConfigProvider is a private interface to extract DynamoDB configuration
// from the abstract AppContext Config.
type ConfigProvider interface {
	GetDynamoDBSettings() Settings
}

// Provider implements service.Provider for the DynamoDB service.
type Provider struct{}

// Name returns the logical name of the provider.
func (p *Provider) Name() string {
	return "DynamoDB"
}

// Init initializes the DynamoDB service backend, janitor, and handler.
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	var settings Settings

	// Try to extract configuration if the config implements the extractor interface
	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetDynamoDBSettings()
	}

	backend := NewInMemoryDB()
	handler := NewHandler(backend, ctx.Logger).WithJanitor(settings)

	return handler, nil
}
