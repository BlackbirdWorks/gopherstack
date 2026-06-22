package bedrock

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for Amazon Bedrock.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "Bedrock" }

// Init initializes the Bedrock backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	accountID, region := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackend(accountID, region)
	handler := NewHandler(backend)

	return handler, nil
}

// AgentsProvider implements service.Provider for Amazon Bedrock Agents.
type AgentsProvider struct{}

// Name returns the provider name.
func (p *AgentsProvider) Name() string { return "BedrockAgents" }

// Init initializes the Bedrock Agents backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *AgentsProvider) Init(ctx *service.AppContext) (service.Registerable, error) {
	accountID, region := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackend(accountID, region)
	handler := NewAgentsHandler(backend)

	return handler, nil
}
