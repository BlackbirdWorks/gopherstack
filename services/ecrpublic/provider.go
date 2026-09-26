package ecrpublic

import "github.com/blackbirdworks/gopherstack/pkgs/service"

// Provider implements service.Provider for Amazon ECR Public.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "ECRPublic" }

// Init initializes the Amazon ECR Public service backend and handler. Public
// repositories are a us-east-1-only service in real AWS, so the backend is
// always constructed for that region regardless of the configured default.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	accountID, _ := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackend(accountID, "us-east-1")
	handler := NewHandler(backend)
	handler.AccountID = accountID
	handler.DefaultRegion = "us-east-1"

	return handler, nil
}
