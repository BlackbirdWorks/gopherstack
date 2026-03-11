package dynamodbstreams

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for DynamoDB Streams.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "DynamoDBStreams" }

// Init initializes the DynamoDB Streams handler with a nil backend.
// The backend is wired later in cli.go via wireDynamoDBStreams().
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(_ *service.AppContext) (service.Registerable, error) {
	return NewHandler(nil), nil
}
