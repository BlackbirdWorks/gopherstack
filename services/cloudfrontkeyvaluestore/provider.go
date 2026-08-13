package cloudfrontkeyvaluestore

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for CloudFront KeyValueStore.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "CloudFront KeyValueStore" }

// Init initializes the CloudFront KeyValueStore handler with a nil backend.
// The backend is wired later in cli.go via wireCloudFrontKeyValueStore(),
// which points it at the CloudFront service's *InMemoryBackend -- see
// handler.go's Handler doc comment for why this service owns no state of
// its own.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(_ *service.AppContext) (service.Registerable, error) {
	return NewHandler(nil), nil
}
