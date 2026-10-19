// Package codegurureviewer emulates Amazon CodeGuru Reviewer: repository associations, code reviews that run a
// deterministic static rule set over the repository's real source, recommendations and feedback.
package codegurureviewer

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned by Init when no AppContext is supplied.
var ErrNilAppContext = errors.New("codegurureviewer: AppContext must not be nil")

// Provider implements service.Provider.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "CodeGuruReviewer" }

// Init builds the backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	accountID, region := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackend(accountID, region)
	backend.SetAppConfig(ctx.Config)

	h := NewHandler(backend)
	h.DefaultRegion = region

	return h, nil
}
