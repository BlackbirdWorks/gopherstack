package resourcegroupstaggingapi

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/organizations"
)

const tagPolicyType = "TAG_POLICY"

type organizationsHandlerProvider interface {
	GetOrganizationsHandler() service.Registerable
}

// organizationsTagPolicy resolves the Organizations backend lazily, so
// provider init order does not matter.
func organizationsTagPolicy(p organizationsHandlerProvider) TagPolicyProvider {
	return func() (string, bool) {
		h, ok := p.GetOrganizationsHandler().(*organizations.Handler)
		if !ok || h == nil || h.Backend == nil {
			return "", false
		}

		policy, err := h.Backend.DescribeEffectivePolicy(tagPolicyType, "")
		if err != nil || policy == nil {
			return "", false
		}

		return policy.PolicyContent, true
	}
}

// ErrNilAppContext is returned by Init when a nil AppContext is passed.
var ErrNilAppContext = errors.New("nil AppContext passed to ResourceGroupsTaggingAPI Provider.Init")

// Provider implements service.Provider for the Resource Groups Tagging API.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "ResourceGroupsTaggingAPI" }

// Init initializes the Resource Groups Tagging API backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	accountID, region := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackend(accountID, region)
	if op, ok := ctx.Config.(organizationsHandlerProvider); ok {
		backend.RegisterTagPolicyProvider(organizationsTagPolicy(op))
	}

	handler := NewHandler(backend)

	return handler, nil
}
